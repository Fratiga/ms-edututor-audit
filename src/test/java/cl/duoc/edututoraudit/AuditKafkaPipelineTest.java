package cl.duoc.edututoraudit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.validation.autoconfigure.ValidationAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

// El flujo completo de audit contra un broker Kafka real y con la MISMA
// configuración de application.properties (consumidor, deserializer tolerante a
// errores, reintentos, DLT): lo que se prueba aquí es lo que corre en producción.
@SpringBootTest(classes = AuditKafkaPipelineTest.Config.class, properties = {
	// Reintentos rápidos para que la prueba no espere 1 s, 2 s, 4 s.
	"edututor.kafka.retry.initial-interval-ms=100",
	"edututor.kafka.retry.max-interval-ms=400"
})
class AuditKafkaPipelineTest {

	private static final String EVENTOS = "sessions.events";
	private static final String DLT = "sessions.events.DLT";
	private static final String ALERTAS = "sessions.alerts";

	private static final EmbeddedKafkaKraftBroker BROKER = new EmbeddedKafkaKraftBroker(1, 3, EVENTOS, DLT, ALERTAS);
	private static final EventoAuditoriaRepository REPOSITORIO = mock(EventoAuditoriaRepository.class);

	static {
		BROKER.afterPropertiesSet();
	}

	@DynamicPropertySource
	static void propiedades(DynamicPropertyRegistry registry) {
		registry.add("spring.kafka.bootstrap-servers", BROKER::getBrokersAsString);
	}

	@Configuration
	@ImportAutoConfiguration({ KafkaAutoConfiguration.class, ValidationAutoConfiguration.class })
	@Import({ KafkaClientConfig.class, AuditEventListener.class, EventValidator.class, AuditService.class,
		AlertDetector.class, AlertPublisher.class })
	static class Config {

		@Bean
		EventoAuditoriaRepository repositorio() {
			return REPOSITORIO;
		}
	}

	@Autowired
	KafkaTemplate<String, byte[]> bytes;

	private static String json(String eventId, long sessionId, String estudiante, String estadoNuevo) {
		return ("{'eventId':'" + eventId + "','type':'SESSION_STATE_CHANGED','timestamp':'2026-11-05T12:00:00Z',"
			+ "'traceId':'t','correlationId':'c','payload':{'sessionId':" + sessionId + ",'estudianteId':'" + estudiante
			+ "','servicioId':2,'tutorId':null,'estadoAnterior':'CONFIRMADA','estadoNuevo':'" + estadoNuevo + "'}}")
			.replace('\'', '"');
	}

	private SendResult<String, byte[]> enviar(String key, String valor) throws Exception {
		return bytes.send(EVENTOS, key, valor.getBytes(StandardCharsets.UTF_8)).get();
	}

	private static boolean conEventId(EventoAuditoria e, String eventId) {
		return e != null && eventId.equals(e.getEventId());
	}

	// Lee un tópico desde el inicio hasta encontrar un mensaje que cumpla la condición.
	private static Optional<ConsumerRecord<String, String>> buscar(String topico, Predicate<ConsumerRecord<String, String>> condicion) {
		try (KafkaConsumer<String, String> consumidor = new KafkaConsumer<>(Map.of(
				ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER.getBrokersAsString(),
				ConsumerConfig.GROUP_ID_CONFIG, "prueba-" + UUID.randomUUID(),
				ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
				ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
				ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
			consumidor.subscribe(List.of(topico));
			long limite = System.currentTimeMillis() + 25_000;
			while (System.currentTimeMillis() < limite) {
				for (ConsumerRecord<String, String> registro : consumidor.poll(Duration.ofMillis(500))) {
					if (condicion.test(registro)) {
						return Optional.of(registro);
					}
				}
			}
		}
		return Optional.empty();
	}

	private static String cabecera(ConsumerRecord<String, String> registro, String nombre) {
		Header h = registro.headers().lastHeader(nombre);
		return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
	}

	// ------------------------------------------------------------------ camino feliz

	@Test
	void auditaUnEventoValidoConSuPosicionEnKafka() throws Exception {
		when(REPOSITORIO.findByEventId("ok-1")).thenReturn(Optional.empty());

		SendResult<String, byte[]> enviado = enviar("7", json("ok-1", 7, "STU-1", "ASIGNADA"));

		var guardado = org.mockito.ArgumentCaptor.forClass(EventoAuditoria.class);
		verify(REPOSITORIO, timeout(20_000)).save(argThat(e -> conEventId(e, "ok-1")));
		verify(REPOSITORIO, timeout(20_000).atLeastOnce()).save(guardado.capture());
		EventoAuditoria registro = guardado.getAllValues().stream().filter(e -> conEventId(e, "ok-1")).findFirst().orElseThrow();
		assertThat(registro.getKafkaKey()).isEqualTo("7");
		assertThat(registro.getKafkaParticion()).isEqualTo(enviado.getRecordMetadata().partition());
		assertThat(registro.getKafkaOffset()).isEqualTo(enviado.getRecordMetadata().offset());
		assertThat(registro.getSessionId()).isEqualTo(7L);
	}

	@Test
	void unMensajeSinKeyTambienSeAudita() throws Exception {
		when(REPOSITORIO.findByEventId("ok-sin-key")).thenReturn(Optional.empty());

		enviar(null, json("ok-sin-key", 8, "STU-1", "ASIGNADA"));

		verify(REPOSITORIO, timeout(20_000)).save(argThat(e -> conEventId(e, "ok-sin-key") && e.getKafkaKey() == null));
	}

	// ------------------------------------------------------------- mensajes defectuosos

	@Test
	void unTextoQueNoEsJsonVaALaDltYElConsumoSigue() throws Exception {
		when(REPOSITORIO.findByEventId("tras-hola")).thenReturn(Optional.empty());

		enviar(null, "hola");
		enviar("9", json("tras-hola", 9, "STU-1", "ASIGNADA"));

		// el mensaje malo llegó a la DLT, byte a byte y con su origen
		var enDlt = buscar(DLT, r -> "hola".equals(r.value()));
		assertThat(enDlt).as("«hola» debe estar en " + DLT).isPresent();
		assertThat(cabecera(enDlt.get(), "kafka_dlt-original-topic")).isEqualTo(EVENTOS);

		// y el siguiente evento válido se procesó con normalidad
		verify(REPOSITORIO, timeout(20_000)).save(argThat(e -> conEventId(e, "tras-hola")));
	}

	@Test
	void unEventoInvalidoVaALaDltSinReintentosNiAuditoria() throws Exception {
		// sin eventId y con un sessionId marcador para reconocerlo
		String sinId = json("x", 777, "STU-1", "ASIGNADA").replace("\"eventId\":\"x\",", "");
		enviar("777", sinId);

		var enDlt = buscar(DLT, r -> r.value() != null && r.value().contains("\"sessionId\":777"));
		assertThat(enDlt).as("el evento inválido debe estar en " + DLT).isPresent();
		assertThat(cabecera(enDlt.get(), "kafka_dlt-exception-message")).contains("eventId");
		// la validación lo rechazó antes de tocar la base de datos
		verify(REPOSITORIO, after(1_000).never()).save(argThat(e -> e != null && Long.valueOf(777).equals(e.getSessionId())));
	}

	// ---------------------------------------------------------------- reintentos

	@Test
	void unFalloTransitorioSeReintentaYSeProcesa() throws Exception {
		when(REPOSITORIO.findByEventId("trans-1")).thenReturn(Optional.empty());
		when(REPOSITORIO.save(argThat(e -> conEventId(e, "trans-1"))))
			.thenThrow(new RuntimeException("base de datos no disponible"))
			.thenThrow(new RuntimeException("base de datos no disponible"))
			.thenAnswer(invocacion -> invocacion.getArgument(0));

		enviar("10", json("trans-1", 10, "STU-1", "ASIGNADA"));

		// 2 fallos + 1 éxito = 3 intentos, sin pasar por la DLT
		verify(REPOSITORIO, timeout(20_000).times(3)).save(argThat(e -> conEventId(e, "trans-1")));
		assertThat(buscar(DLT, r -> r.value() != null && r.value().contains("trans-1"))).isEmpty();
	}

	@Test
	void siElFalloNoSeArreglaTerminaEnLaDltDespuesDeLosReintentos() throws Exception {
		when(REPOSITORIO.findByEventId("siempre-falla")).thenReturn(Optional.empty());
		when(REPOSITORIO.save(argThat(e -> conEventId(e, "siempre-falla"))))
			.thenThrow(new RuntimeException("base de datos no disponible"));

		enviar("11", json("siempre-falla", 11, "STU-1", "ASIGNADA"));

		// 1 intento + 3 reintentos (edututor.kafka.retry.max-attempts=3)
		verify(REPOSITORIO, timeout(20_000).times(4)).save(argThat(e -> conEventId(e, "siempre-falla")));
		var enDlt = buscar(DLT, r -> r.value() != null && r.value().contains("siempre-falla"));
		assertThat(enDlt).as("tras los reintentos debe estar en " + DLT).isPresent();
		assertThat(cabecera(enDlt.get(), "kafka_dlt-exception-message")).contains("base de datos no disponible");
	}

	// ------------------------------------------------------------- segundo tópico

	@Test
	void treCancelacionesDeUnEstudiantePublicanUnaAlertaEnElSegundoTopico() throws Exception {
		when(REPOSITORIO.findByEventId("alerta-1")).thenReturn(Optional.empty());
		when(REPOSITORIO.countByEstudianteIdAndEstadoNuevoAndOcurridoEnGreaterThanEqual(eq("STU-ALERTA"), eq("CANCELADA"), any()))
			.thenReturn(3L);

		enviar("12", json("alerta-1", 12, "STU-ALERTA", "CANCELADA"));

		var alerta = buscar(ALERTAS, r -> "STU-ALERTA:CANCELACIONES_REITERADAS".equals(r.key()));
		assertThat(alerta).as("la alerta debe publicarse en " + ALERTAS).isPresent();
		assertThat(alerta.get().value())
			.contains("\"alertId\":\"alerta-1\"")
			.contains("\"regla\":\"CANCELACIONES_REITERADAS\"")
			.contains("\"severidad\":\"MEDIA\"")
			.contains("\"estudianteId\":\"STU-ALERTA\"")
			.contains("\"valor\":3");
		// la fecha viaja como texto ISO-8601, no como número
		assertThat(alerta.get().value()).containsPattern("\"detectadaEn\":\"\\d{4}-\\d{2}-\\d{2}T");
	}

	@Test
	void sinAlertaNoSePublicaNada() throws Exception {
		when(REPOSITORIO.findByEventId("sin-alerta-1")).thenReturn(Optional.empty());

		enviar("13", json("sin-alerta-1", 13, "STU-TRANQUILO", "CONFIRMADA"));

		verify(REPOSITORIO, timeout(20_000)).save(argThat(e -> conEventId(e, "sin-alerta-1")));
		verify(REPOSITORIO, never()).countByEstudianteIdAndEstadoNuevoAndOcurridoEnGreaterThanEqual(eq("STU-TRANQUILO"), anyString(), any());
		assertThat(buscar(ALERTAS, r -> r.key() != null && r.key().startsWith("STU-TRANQUILO"))).isEmpty();
	}
}
