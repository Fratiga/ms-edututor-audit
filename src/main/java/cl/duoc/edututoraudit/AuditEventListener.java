package cl.duoc.edututoraudit;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

// Capa de mensajería: recibe el evento de Kafka, lo valida y se lo entrega a
// AuditService. No tiene lógica de negocio.
//
// Si algo falla aquí, el manejo de errores del contenedor (KafkaClientConfig)
// reintenta con espera creciente y, agotados los reintentos o ante un mensaje
// inválido, lo envía a sessions.events.DLT y lo registra en el log.
//
// Grupo audit-service-v1: lee TODOS los eventos (cada grupo tiene su propio
// avance, report lee los mismos con otro grupo). La concurrencia reparte las
// particiones entre hilos de este servicio, y si se levanta otra instancia con el
// mismo grupo Kafka reparte las particiones entre las dos.
@Component
public class AuditEventListener {

	private final EventValidator validador;
	private final AuditService servicio;

	public AuditEventListener(EventValidator validador, AuditService servicio) {
		this.validador = validador;
		this.servicio = servicio;
	}

	@KafkaListener(topics = "${edututor.kafka.topic.events}", groupId = "${spring.kafka.consumer.group-id}")
	public void onSessionEvent(
			@Payload(required = false) SessionEventDto evento,
			@Header(KafkaHeaders.RECEIVED_PARTITION) int particion,
			@Header(KafkaHeaders.OFFSET) long offset,
			// required = false: un mensaje sin key (por ejemplo, uno publicado a
			// mano desde la consola) no trae este header y no debe romper el listener.
			@Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key) {
		validador.validar(evento);
		servicio.registrar(evento, new AuditService.OrigenKafka(particion, offset, key));
	}
}
