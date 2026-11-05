package cl.duoc.edututoraudit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

// Con el contenedor real de @KafkaListener y un broker embebido: comprueba que
// Spring inyecta partición, offset y key en los parámetros @Header, también
// cuando el mensaje llega SIN key (publicado a mano desde la consola).
@SpringJUnitConfig(AuditEventListenerKafkaTest.Config.class)
class AuditEventListenerKafkaTest {

	static final EventoAuditoriaRepository REPOSITORIO = mock(EventoAuditoriaRepository.class);

	@Configuration
	@EnableKafka
	static class Config {

		@Bean(destroyMethod = "destroy")
		EmbeddedKafkaKraftBroker broker() {
			return new EmbeddedKafkaKraftBroker(1, 3, "sessions.events");
		}

		@Bean
		EventoAuditoriaRepository repositorio() {
			return REPOSITORIO;
		}

		@Bean
		AuditEventListener listener(EventoAuditoriaRepository repositorio) {
			return new AuditEventListener(repositorio);
		}

		// Misma configuración de deserialización que application.properties.
		@Bean
		ConsumerFactory<String, Object> consumerFactory(EmbeddedKafkaKraftBroker broker) {
			Map<String, Object> props = new HashMap<>();
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
			props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
			props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "java.util.LinkedHashMap");
			props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
			return new DefaultKafkaConsumerFactory<>(props);
		}

		@Bean
		ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
				ConsumerFactory<String, Object> consumerFactory) {
			ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory);
			return factory;
		}

		@Bean
		KafkaTemplate<String, Object> plantilla(EmbeddedKafkaKraftBroker broker) {
			Map<String, Object> props = new HashMap<>();
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
			props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
			return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
		}
	}

	@Autowired
	KafkaTemplate<String, Object> plantilla;

	private Map<String, Object> evento(String eventId) {
		return Map.of(
			"eventId", eventId,
			"type", "SESSION_CREATED",
			"timestamp", Instant.now().toString(),
			"payload", Map.of("sessionId", 7, "estudianteId", "STU-204", "estadoNuevo", "SOLICITADA"));
	}

	@Test
	void elListenerRecibeParticionOffsetYKeyReales() throws Exception {
		when(REPOSITORIO.findByEventId("con-key")).thenReturn(Optional.empty());

		SendResult<String, Object> enviado = plantilla.send("sessions.events", "7", evento("con-key")).get();

		ArgumentCaptor<EventoAuditoria> guardado = ArgumentCaptor.forClass(EventoAuditoria.class);
		verify(REPOSITORIO, timeout(20_000)).save(guardado.capture());
		assertThat(guardado.getValue().getEventId()).isEqualTo("con-key");
		assertThat(guardado.getValue().getKafkaKey()).isEqualTo("7");
		assertThat(guardado.getValue().getKafkaParticion()).isEqualTo(enviado.getRecordMetadata().partition());
		assertThat(guardado.getValue().getKafkaOffset()).isEqualTo(enviado.getRecordMetadata().offset());
	}

	@Test
	void unMensajeSinKeyLlegaIgualYQuedaSinKey() throws Exception {
		when(REPOSITORIO.findByEventId("sin-key")).thenReturn(Optional.empty());

		SendResult<String, Object> enviado = plantilla.send("sessions.events", evento("sin-key")).get();

		ArgumentCaptor<EventoAuditoria> guardado = ArgumentCaptor.forClass(EventoAuditoria.class);
		verify(REPOSITORIO, timeout(20_000)).save(org.mockito.ArgumentMatchers.argThat(e -> e != null && "sin-key".equals(e.getEventId())));
		verify(REPOSITORIO, timeout(20_000).atLeastOnce()).save(guardado.capture());
		EventoAuditoria registro = guardado.getAllValues().stream()
			.filter(e -> "sin-key".equals(e.getEventId())).findFirst().orElseThrow();
		assertThat(registro.getKafkaKey()).isNull();
		assertThat(registro.getKafkaParticion()).isEqualTo(enviado.getRecordMetadata().partition());
		assertThat(registro.getKafkaOffset()).isEqualTo(enviado.getRecordMetadata().offset());
	}
}
