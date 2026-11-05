package cl.duoc.edututoraudit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import jakarta.validation.Validation;

// Lógica de negocio sin Kafka: validación del DTO, auditoría idempotente con la
// posición de origen y la regla de cancelaciones reiteradas.
class AuditDomainTest {

	private final EventoAuditoriaRepository repository = mock(EventoAuditoriaRepository.class);
	private final AlertPublisher publisher = mock(AlertPublisher.class);
	private final AlertDetector detector = new AlertDetector(repository, new AlertProperties(3, 7));
	private final AuditService servicio = new AuditService(repository, detector, publisher);
	private final EventValidator validador = new EventValidator(
		Validation.buildDefaultValidatorFactory().getValidator());

	private SessionEventDto evento(String eventId, String estudiante, String estadoNuevo) {
		return new SessionEventDto(eventId, "SESSION_STATE_CHANGED", Instant.parse("2026-11-05T12:00:00Z"), null, null,
			new SessionEventDto.Payload(7L, estudiante, 2L, null, "CONFIRMADA", estadoNuevo));
	}

	// ------------------------------------------------------------- validación

	@Test
	void unEventoCompletoEsValido() {
		assertThatCode(() -> validador.validar(evento("e-1", "STU-1", "ASIGNADA"))).doesNotThrowAnyException();
	}

	@Test
	void unEventoSinCamposObligatoriosSeRechaza() {
		SessionEventDto sinId = new SessionEventDto(" ", "SESSION_CREATED", Instant.now(), null, null,
			new SessionEventDto.Payload(1L, "STU-1", 1L, null, null, "SOLICITADA"));
		SessionEventDto sinPayload = new SessionEventDto("e-2", "SESSION_CREATED", Instant.now(), null, null, null);
		SessionEventDto sinEstado = new SessionEventDto("e-3", "SESSION_CREATED", Instant.now(), null, null,
			new SessionEventDto.Payload(1L, "STU-1", 1L, null, null, ""));

		assertThatThrownBy(() -> validador.validar(sinId)).isInstanceOf(InvalidEventException.class)
			.hasMessageContaining("eventId");
		assertThatThrownBy(() -> validador.validar(sinPayload)).isInstanceOf(InvalidEventException.class)
			.hasMessageContaining("payload");
		assertThatThrownBy(() -> validador.validar(sinEstado)).isInstanceOf(InvalidEventException.class)
			.hasMessageContaining("estadoNuevo");
		assertThatThrownBy(() -> validador.validar(null)).isInstanceOf(InvalidEventException.class);
	}

	// -------------------------------------------------------------- auditoría

	@Test
	void guardaElEventoConSuPosicionEnKafka() {
		when(repository.findByEventId("e-1")).thenReturn(Optional.empty());

		servicio.registrar(evento("e-1", "STU-1", "ASIGNADA"), new AuditService.OrigenKafka(2, 41L, "7"));

		ArgumentCaptor<EventoAuditoria> guardado = ArgumentCaptor.forClass(EventoAuditoria.class);
		verify(repository).save(guardado.capture());
		assertThat(guardado.getValue().getEventId()).isEqualTo("e-1");
		assertThat(guardado.getValue().getSessionId()).isEqualTo(7L);
		assertThat(guardado.getValue().getKafkaParticion()).isEqualTo(2);
		assertThat(guardado.getValue().getKafkaOffset()).isEqualTo(41L);
		assertThat(guardado.getValue().getKafkaKey()).isEqualTo("7");
	}

	@Test
	void unMensajeSinKeyQuedaSinKey() {
		when(repository.findByEventId("e-2")).thenReturn(Optional.empty());

		servicio.registrar(evento("e-2", "STU-1", "ASIGNADA"), new AuditService.OrigenKafka(0, 3L, null));

		ArgumentCaptor<EventoAuditoria> guardado = ArgumentCaptor.forClass(EventoAuditoria.class);
		verify(repository).save(guardado.capture());
		assertThat(guardado.getValue().getKafkaKey()).isNull();
	}

	@Test
	void unEventoYaAuditadoNoSeDuplica() {
		when(repository.findByEventId("e-3")).thenReturn(Optional.of(mock(EventoAuditoria.class)));

		servicio.registrar(evento("e-3", "STU-1", "CANCELADA"), new AuditService.OrigenKafka(1, 9L, "7"));

		verify(repository, never()).save(any());
		verify(publisher, never()).publicar(any());
	}

	// ----------------------------------------------------------------- alertas

	private void conCancelaciones(long cuantas) {
		when(repository.countByEstudianteIdAndEstadoNuevoAndOcurridoEnGreaterThanEqual(eq("STU-9"), eq("CANCELADA"), any()))
			.thenReturn(cuantas);
	}

	@Test
	void bajoElUmbralNoHayAlerta() {
		conCancelaciones(2);

		servicio.registrar(evento("c-1", "STU-9", "CANCELADA"), new AuditService.OrigenKafka(0, 1L, "7"));

		verify(publisher, never()).publicar(any());
	}

	@Test
	void alLlegarAlUmbralSePublicaUnaAlertaMedia() {
		conCancelaciones(3);

		servicio.registrar(evento("c-2", "STU-9", "CANCELADA"), new AuditService.OrigenKafka(0, 2L, "7"));

		ArgumentCaptor<SessionAlertDto> alerta = ArgumentCaptor.forClass(SessionAlertDto.class);
		verify(publisher).publicar(alerta.capture());
		assertThat(alerta.getValue().alertId()).isEqualTo("c-2");
		assertThat(alerta.getValue().regla()).isEqualTo("CANCELACIONES_REITERADAS");
		assertThat(alerta.getValue().severidad()).isEqualTo("MEDIA");
		assertThat(alerta.getValue().estudianteId()).isEqualTo("STU-9");
		assertThat(alerta.getValue().valor()).isEqualTo(3);
		assertThat(alerta.getValue().umbral()).isEqualTo(3);
	}

	@Test
	void conElDobleDelUmbralLaAlertaEsAlta() {
		conCancelaciones(6);

		servicio.registrar(evento("c-3", "STU-9", "CANCELADA"), new AuditService.OrigenKafka(0, 3L, "7"));

		ArgumentCaptor<SessionAlertDto> alerta = ArgumentCaptor.forClass(SessionAlertDto.class);
		verify(publisher).publicar(alerta.capture());
		assertThat(alerta.getValue().severidad()).isEqualTo("ALTA");
	}

	@Test
	void soloLasCancelacionesDisparanLaRegla() {
		conCancelaciones(10);

		servicio.registrar(evento("c-4", "STU-9", "CONFIRMADA"), new AuditService.OrigenKafka(0, 4L, "7"));

		verify(repository, never()).countByEstudianteIdAndEstadoNuevoAndOcurridoEnGreaterThanEqual(any(), any(), any());
		verify(publisher, never()).publicar(any());
	}
}
