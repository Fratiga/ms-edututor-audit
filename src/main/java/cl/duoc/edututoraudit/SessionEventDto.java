package cl.duoc.edututoraudit;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

// Evento que publica ms-edututor-sessions en sessions.events. Cada servicio
// declara su propio DTO (no comparten código de dominio): este es el contrato
// JSON que audit espera. Las propiedades desconocidas se ignoran para que
// sessions pueda agregar campos sin romper a este consumidor.
@JsonIgnoreProperties(ignoreUnknown = true)
public record SessionEventDto(
	@NotBlank(message = "eventId es obligatorio") String eventId,
	@NotBlank(message = "type es obligatorio") String type,
	@NotNull(message = "timestamp es obligatorio") Instant timestamp,
	String traceId,
	String correlationId,
	@NotNull(message = "payload es obligatorio") @Valid Payload payload
) {

	@JsonIgnoreProperties(ignoreUnknown = true)
	public record Payload(
		@NotNull(message = "payload.sessionId es obligatorio") Long sessionId,
		String estudianteId,
		Long servicioId,
		String tutorId,
		String estadoAnterior,
		@NotBlank(message = "payload.estadoNuevo es obligatorio") String estadoNuevo
	) {
	}
}
