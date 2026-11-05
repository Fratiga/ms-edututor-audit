package cl.duoc.edututoraudit;

import java.time.Instant;
import java.util.List;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

// Solo lectura, tal como pide el caso: "Consulta el timeline. Solo lectura."
@RestController
@RequestMapping("/api/audit")
public class AuditController {

	private final EventoAuditoriaRepository repository;

	public AuditController(EventoAuditoriaRepository repository) {
		this.repository = repository;
	}

	// GET /api/audit/timeline — filtros: usuario (estudianteId), fechas, tipo de evento.
	@GetMapping("/timeline")
	public List<EventoAuditoriaResponse> timeline(
			@RequestParam(required = false) String usuario,
			@RequestParam(required = false) String tipo,
			@RequestParam(required = false) Instant from,
			@RequestParam(required = false) Instant to) {
		return repository.buscar(usuario, tipo, from, to).stream().map(EventoAuditoriaResponse::from).toList();
	}

	@GetMapping("/session/{id}")
	public List<EventoAuditoriaResponse> porSesion(@PathVariable Long id) {
		return repository.findBySessionIdOrderByOcurridoEnAsc(id).stream().map(EventoAuditoriaResponse::from).toList();
	}


	// DTO externo alineado al contrato del frontend. payloadJson reconstruye el
	// detalle de la transicion y, si el registro lo tiene, la posicion exacta
	// del evento en Kafka (kafkaParticion / kafkaOffset / kafkaKey).
	public record EventoAuditoriaResponse(String id, String eventoTipo, String origen, String usuario,
			java.time.Instant fechaTimestamp, java.util.Map<String, Object> payloadJson, String resultado) {
		static EventoAuditoriaResponse from(EventoAuditoria e) {
			java.util.Map<String, Object> payload = new java.util.LinkedHashMap<>();
			payload.put("sessionId", String.valueOf(e.getSessionId()));
			payload.put("tutorId", e.getTutorId() == null ? "" : e.getTutorId());
			payload.put("estadoAnterior", e.getEstadoAnterior() == null ? "" : e.getEstadoAnterior());
			payload.put("estadoNuevo", e.getEstadoNuevo() == null ? "" : e.getEstadoNuevo());
			if (e.getKafkaParticion() != null) {
				payload.put("kafkaParticion", String.valueOf(e.getKafkaParticion()));
				payload.put("kafkaOffset", String.valueOf(e.getKafkaOffset()));
				payload.put("kafkaKey", e.getKafkaKey() == null ? "" : e.getKafkaKey());
			}
			return new EventoAuditoriaResponse(String.valueOf(e.getId()), e.getTipo(), e.getOrigen(),
				e.getEstudianteId(), e.getOcurridoEn(), payload, e.getResultado());
		}
	}
}
