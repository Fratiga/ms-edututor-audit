package cl.duoc.edututoraudit;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import org.springframework.stereotype.Component;

// Regla de analítica sobre el historial: un estudiante que cancela sesiones
// con frecuencia merece atención. Es lógica de negocio pura: no sabe de Kafka,
// solo recibe el evento ya auditado y decide si hay alerta.
@Component
public class AlertDetector {

	static final String CANCELADA = "CANCELADA";

	private final EventoAuditoriaRepository repository;
	private final AlertProperties propiedades;

	public AlertDetector(EventoAuditoriaRepository repository, AlertProperties propiedades) {
		this.repository = repository;
		this.propiedades = propiedades;
	}

	public Optional<SessionAlertDto> evaluar(EventoAuditoria evento) {
		if (!CANCELADA.equals(evento.getEstadoNuevo()) || evento.getEstudianteId() == null) {
			return Optional.empty();
		}
		int umbral = propiedades.umbralOrDefault();
		int ventana = propiedades.ventanaDiasOrDefault();
		long cancelaciones = repository.countByEstudianteIdAndEstadoNuevoAndOcurridoEnGreaterThanEqual(
			evento.getEstudianteId(), CANCELADA, evento.getOcurridoEn().minus(ventana, ChronoUnit.DAYS));
		if (cancelaciones < umbral) {
			return Optional.empty();
		}
		String severidad = cancelaciones >= 2L * umbral ? "ALTA" : "MEDIA";
		return Optional.of(new SessionAlertDto(
			evento.getEventId(),
			SessionAlertDto.CANCELACIONES_REITERADAS,
			severidad,
			evento.getEstudianteId(),
			evento.getSessionId(),
			cancelaciones,
			umbral,
			ventana,
			evento.getEstudianteId() + " canceló " + cancelaciones + " sesiones en los últimos " + ventana + " días",
			Instant.now()));
	}
}
