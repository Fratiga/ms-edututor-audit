package cl.duoc.edututoraudit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

// El listener debe guardar de dónde salió cada evento en Kafka (partición,
// offset y key) y tolerar mensajes sin key.
class AuditEventListenerTest {

	private final EventoAuditoriaRepository repository = mock(EventoAuditoriaRepository.class);
	private final AuditEventListener listener = new AuditEventListener(repository);

	private Map<String, Object> evento(String eventId) {
		return Map.of(
			"eventId", eventId,
			"type", "SESSION_STATE_CHANGED",
			"timestamp", Instant.now().toString(),
			"payload", Map.of("sessionId", 7, "estudianteId", "STU-204", "estadoAnterior", "SOLICITADA", "estadoNuevo", "CONFIRMADA"));
	}

	@Test
	void guardaParticionOffsetYKey() {
		when(repository.findByEventId("e-1")).thenReturn(Optional.empty());

		listener.onSessionEvent(evento("e-1"), 2, 41L, "7");

		ArgumentCaptor<EventoAuditoria> guardado = ArgumentCaptor.forClass(EventoAuditoria.class);
		verify(repository).save(guardado.capture());
		assertThat(guardado.getValue().getKafkaParticion()).isEqualTo(2);
		assertThat(guardado.getValue().getKafkaOffset()).isEqualTo(41L);
		assertThat(guardado.getValue().getKafkaKey()).isEqualTo("7");
		assertThat(guardado.getValue().getSessionId()).isEqualTo(7L);
	}

	@Test
	void unMensajeSinKeyNoRompeElListener() {
		when(repository.findByEventId("e-2")).thenReturn(Optional.empty());

		listener.onSessionEvent(evento("e-2"), 0, 3L, null);

		ArgumentCaptor<EventoAuditoria> guardado = ArgumentCaptor.forClass(EventoAuditoria.class);
		verify(repository).save(guardado.capture());
		assertThat(guardado.getValue().getKafkaKey()).isNull();
		assertThat(guardado.getValue().getKafkaOffset()).isEqualTo(3L);
	}

	@Test
	void unEventoYaAuditadoNoSeDuplica() {
		when(repository.findByEventId("e-3")).thenReturn(Optional.of(mock(EventoAuditoria.class)));

		listener.onSessionEvent(evento("e-3"), 1, 9L, "7");

		verify(repository, never()).save(any());
	}
}
