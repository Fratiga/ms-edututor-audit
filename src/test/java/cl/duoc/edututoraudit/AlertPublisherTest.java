package cl.duoc.edututoraudit;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

// La alerta se publica con la key "estudiante:regla" y un fallo de Kafka no
// propaga excepciones (la auditoría ya quedó guardada).
class AlertPublisherTest {

	@SuppressWarnings("unchecked")
	private final KafkaTemplate<String, Object> kafka = mock(KafkaTemplate.class);
	private final AlertPublisher publisher = new AlertPublisher(kafka, "sessions.alerts");

	private SessionAlertDto alerta() {
		return new SessionAlertDto("e-1", SessionAlertDto.CANCELACIONES_REITERADAS, "MEDIA", "STU-9", 7L, 3, 3, 7,
			"STU-9 canceló 3 sesiones en los últimos 7 días", Instant.now());
	}

	@Test
	void usaEstudianteYReglaComoKey() {
		when(kafka.send(eq("sessions.alerts"), any(String.class), any())).thenReturn(new CompletableFuture<>());

		publisher.publicar(alerta());

		verify(kafka).send(eq("sessions.alerts"), eq("STU-9:CANCELACIONES_REITERADAS"), any());
	}

	@Test
	void unKafkaQueFallaNoLanzaExcepcion() {
		when(kafka.send(eq("sessions.alerts"), any(String.class), any()))
			.thenThrow(new org.apache.kafka.common.errors.TimeoutException("metadata no disponible"));

		assertThatCode(() -> publisher.publicar(alerta())).doesNotThrowAnyException();
	}

	@Test
	void unEnvioFallidoNoLanzaExcepcion() {
		CompletableFuture<org.springframework.kafka.support.SendResult<String, Object>> fallido = new CompletableFuture<>();
		fallido.completeExceptionally(new RuntimeException("broker caido"));
		when(kafka.send(eq("sessions.alerts"), any(String.class), any())).thenReturn(fallido);

		assertThatCode(() -> publisher.publicar(alerta())).doesNotThrowAnyException();
	}
}
