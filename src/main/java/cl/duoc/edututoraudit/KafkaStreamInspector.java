package cl.duoc.edututoraudit;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

// Lee de Kafka el estado del tópico de eventos: cuántos mensajes tiene cada
// partición (offset final) y hasta dónde ha leído cada grupo de consumo
// (offset actual). La diferencia es el LAG: lo que un grupo aún no procesa.
// Es lo mismo que muestran kafka-consumer-groups --describe y Kafka UI, pero
// consultable desde la propia aplicación.
@Component
public class KafkaStreamInspector {

	private static final long TIMEOUT_SEGUNDOS = 5;

	private final KafkaAdmin kafkaAdmin;
	private final String topic;
	private final List<String> grupos;

	public KafkaStreamInspector(KafkaAdmin kafkaAdmin,
			@Value("${edututor.kafka.topic:sessions.events}") String topic,
			@Value("${edututor.kafka.groups:audit-service-v1,report-service-v1}") List<String> grupos) {
		this.kafkaAdmin = kafkaAdmin;
		this.topic = topic;
		this.grupos = grupos;
	}

	public record ParticionOffsets(int particion, long offsetFinal) {
	}

	public record ParticionGrupo(int particion, long offsetActual, long offsetFinal, long lag) {
	}

	public record GrupoEstado(String grupo, long lagTotal, List<ParticionGrupo> particiones) {
	}

	public record StreamEstado(String topic, int particiones, long totalEventos,
			List<ParticionOffsets> detalle, List<GrupoEstado> grupos) {
	}

	public StreamEstado inspeccionar() {
		Map<String, Object> config = new HashMap<>(kafkaAdmin.getConfigurationProperties());
		// Con Kafka caído el cliente reintenta por defecto durante minutos: se acota
		// para que la pantalla de auditoría responda un error en pocos segundos.
		config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 4000);
		config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5000);

		try (Admin admin = Admin.create(config)) {
			TopicDescription descripcion = admin.describeTopics(List.of(topic))
				.allTopicNames().get(TIMEOUT_SEGUNDOS, TimeUnit.SECONDS).get(topic);

			List<TopicPartition> particiones = descripcion.partitions().stream()
				.map(p -> new TopicPartition(topic, p.partition()))
				.sorted(Comparator.comparingInt(TopicPartition::partition))
				.toList();

			Map<TopicPartition, OffsetSpec> pedido = new HashMap<>();
			particiones.forEach(tp -> pedido.put(tp, OffsetSpec.latest()));
			Map<TopicPartition, ListOffsetsResultInfo> finales = admin.listOffsets(pedido)
				.all().get(TIMEOUT_SEGUNDOS, TimeUnit.SECONDS);

			List<ParticionOffsets> detalle = particiones.stream()
				.map(tp -> new ParticionOffsets(tp.partition(), finales.get(tp).offset()))
				.toList();

			List<GrupoEstado> estados = grupos.stream().map(grupo -> {
				try {
					Map<TopicPartition, OffsetAndMetadata> confirmados = admin.listConsumerGroupOffsets(grupo)
						.partitionsToOffsetAndMetadata().get(TIMEOUT_SEGUNDOS, TimeUnit.SECONDS);
					List<ParticionGrupo> filas = particiones.stream().map(tp -> {
						OffsetAndMetadata c = confirmados.get(tp);
						// Un grupo que nunca confirmó en la partición no leyó nada: va en 0.
						long actual = c == null ? 0 : c.offset();
						long fin = finales.get(tp).offset();
						return new ParticionGrupo(tp.partition(), actual, fin, Math.max(0, fin - actual));
					}).toList();
					return new GrupoEstado(grupo, filas.stream().mapToLong(ParticionGrupo::lag).sum(), filas);
				} catch (Exception e) {
					throw new IllegalStateException("No se pudo leer el grupo " + grupo, e);
				}
			}).toList();

			return new StreamEstado(topic, particiones.size(),
				detalle.stream().mapToLong(ParticionOffsets::offsetFinal).sum(), detalle, estados);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Consulta a Kafka interrumpida");
		} catch (Exception e) {
			throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
				"Kafka no disponible: " + e.getClass().getSimpleName());
		}
	}
}
