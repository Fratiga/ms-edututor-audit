package cl.duoc.edututoraudit;

import java.time.Instant;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

// Cada microservicio deserializa el JSON del tópico a su propio tipo
// (Map genérico aquí), sin depender de la clase Java del productor — son
// servicios independientes, no comparten código de dominio.
@Component
public class AuditEventListener {

	private static final Logger log = LoggerFactory.getLogger(AuditEventListener.class);

	private final EventoAuditoriaRepository repository;

	public AuditEventListener(EventoAuditoriaRepository repository) {
		this.repository = repository;
	}

	@KafkaListener(topics = "sessions.events", groupId = "audit-service-v1")
	public void onSessionEvent(@Payload Map<String, Object> evento) {
		String eventId = (String) evento.get("eventId");

		// Idempotencia: si ya se procesó este eventId, no se duplica el registro.
		if (repository.findByEventId(eventId).isPresent()) {
			log.info("Evento {} ya auditado, se ignora", eventId);
			return;
		}

		@SuppressWarnings("unchecked")
		Map<String, Object> payload = (Map<String, Object>) evento.get("payload");

		EventoAuditoria registro = new EventoAuditoria(
			eventId,
			payload.get("sessionId") == null ? null : Long.valueOf(payload.get("sessionId").toString()),
			(String) evento.get("type"),
			(String) payload.get("estudianteId"),
			(String) payload.get("tutorId"),
			(String) payload.get("estadoAnterior"),
			(String) payload.get("estadoNuevo"),
			Instant.parse((String) evento.get("timestamp")));

		repository.save(registro);
		log.info("Auditado eventId={} tipo={} sessionId={}", eventId, registro.getTipo(), registro.getSessionId());
	}
}
