package cl.duoc.edututoraudit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

// Lógica de negocio de la auditoría: registrar cada evento una sola vez y, si
// el historial muestra un patrón, generar la alerta. No depende de Kafka salvo
// por los datos de origen (partición, offset, key) que guarda como trazabilidad:
// el listener solo traduce el mensaje y llama aquí.
@Service
public class AuditService {

	private static final Logger log = LoggerFactory.getLogger(AuditService.class);

	// Dónde estaba el mensaje en Kafka.
	public record OrigenKafka(int particion, long offset, String key) {
	}

	private final EventoAuditoriaRepository repository;
	private final AlertDetector detector;
	private final AlertPublisher alertas;

	public AuditService(EventoAuditoriaRepository repository, AlertDetector detector, AlertPublisher alertas) {
		this.repository = repository;
		this.detector = detector;
		this.alertas = alertas;
	}

	public void registrar(SessionEventDto evento, OrigenKafka origen) {
		// Idempotencia: Kafka entrega "al menos una vez". Si este eventId ya se
		// procesó (reentrega, reposicionamiento del grupo), no se duplica el registro.
		if (repository.findByEventId(evento.eventId()).isPresent()) {
			log.info("Evento {} ya auditado (kafka p{} o{}), se ignora", evento.eventId(), origen.particion(), origen.offset());
			return;
		}

		SessionEventDto.Payload payload = evento.payload();
		EventoAuditoria registro = new EventoAuditoria(
			evento.eventId(),
			payload.sessionId(),
			evento.type(),
			payload.estudianteId(),
			payload.tutorId(),
			payload.estadoAnterior(),
			payload.estadoNuevo(),
			evento.timestamp());
		registro.registrarOrigenKafka(origen.particion(), origen.offset(), origen.key());
		repository.save(registro);

		log.info("Auditado kafka p{} o{} key={} eventId={} tipo={} sessionId={}", origen.particion(), origen.offset(),
			origen.key() != null ? origen.key() : "(sin key)", evento.eventId(), registro.getTipo(), registro.getSessionId());

		detector.evaluar(registro).ifPresent(alertas::publicar);
	}
}
