package cl.duoc.edututoraudit;

import java.time.Instant;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
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
	public void onSessionEvent(
			@Payload Map<String, Object> evento,
			@Header(KafkaHeaders.RECEIVED_PARTITION) int particion,
			@Header(KafkaHeaders.OFFSET) long offset,
			// required = false: un mensaje sin key (por ejemplo, uno publicado a
			// mano desde la consola) no trae este header y no debe romper el listener.
			@Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key) {
		String eventId = (String) evento.get("eventId");

		// Idempotencia: si ya se procesó este eventId, no se duplica el registro.
		if (repository.findByEventId(eventId).isPresent()) {
			log.info("Evento {} ya auditado (kafka p{} o{}), se ignora", eventId, particion, offset);
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

		registro.registrarOrigenKafka(particion, offset, key);
		repository.save(registro);
		log.info("Auditado kafka p{} o{} key={} eventId={} tipo={} sessionId={}", particion, offset,
			key != null ? key : "(sin key)", eventId, registro.getTipo(), registro.getSessionId());
	}
}
