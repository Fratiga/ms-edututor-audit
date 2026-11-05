package cl.duoc.edututoraudit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

// Publica las alertas en sessions.alerts. La key es "estudiante:regla": las
// alertas de un mismo estudiante y regla van a la misma partición (en orden) y,
// como el tópico se compacta, Kafka conserva solo la última de cada key.
//
// Una alerta que no se pudo publicar no debe deshacer la auditoría (el evento
// ya quedó guardado): se deja registro en el log.
@Component
public class AlertPublisher {

	private static final Logger log = LoggerFactory.getLogger(AlertPublisher.class);

	private final KafkaTemplate<String, Object> kafkaTemplate;
	private final String topic;

	public AlertPublisher(KafkaTemplate<String, Object> kafkaTemplate,
			@Value("${edututor.kafka.topic.alerts}") String topic) {
		this.kafkaTemplate = kafkaTemplate;
		this.topic = topic;
	}

	public void publicar(SessionAlertDto alerta) {
		String key = alerta.estudianteId() + ":" + alerta.regla();
		try {
			kafkaTemplate.send(topic, key, alerta).whenComplete((resultado, error) -> {
				if (error != null) {
					log.warn("Alerta {} ({}) NO se publicó en {}: {}", alerta.alertId(), key, topic, error.getMessage());
				}
				else {
					log.info("Alerta {} ({}) publicada en {} p{} o{}", alerta.regla(), alerta.estudianteId(), topic,
						resultado.getRecordMetadata().partition(), resultado.getRecordMetadata().offset());
				}
			});
		}
		catch (RuntimeException e) {
			log.warn("Alerta {} ({}) NO se publicó en {}: {}", alerta.alertId(), key, topic, e.getMessage());
		}
	}
}
