package cl.duoc.edututoraudit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.springframework.web.server.ResponseStatusException;

// Prueba contra un broker Kafka real (embebido, sin Docker): el inspector debe
// informar el offset final de cada partición y el avance y LAG de cada grupo.
class KafkaStreamInspectorTest {

	private static final String TOPIC = "sessions.events";
	private static final String GRUPO_AL_DIA = "audit-service-v1";
	private static final String GRUPO_SIN_LEER = "report-service-v1";

	private static EmbeddedKafkaKraftBroker broker;

	@BeforeAll
	static void levantarBroker() throws Exception {
		broker = new EmbeddedKafkaKraftBroker(1, 3, TOPIC);
		broker.afterPropertiesSet();

		// 6 mensajes con key (el id de la sesión): una misma key cae siempre en la misma partición.
		try (KafkaProducer<String, String> productor = new KafkaProducer<>(Map.of(
				ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString(),
				ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
				ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class))) {
			for (String key : List.of("s1", "s1", "s2", "s3", "s1", "s2")) {
				productor.send(new ProducerRecord<>(TOPIC, key, "evento-" + key)).get();
			}
		}

		// El grupo "audit-service-v1" lee y confirma 4 de los 6 mensajes; el otro no lee nada.
		try (KafkaConsumer<String, String> consumidor = new KafkaConsumer<>(Map.of(
				ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString(),
				ConsumerConfig.GROUP_ID_CONFIG, GRUPO_AL_DIA,
				ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
				ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 4,
				ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
				ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
			consumidor.subscribe(List.of(TOPIC));
			int leidos = 0;
			long limite = System.currentTimeMillis() + 15_000;
			while (leidos < 4 && System.currentTimeMillis() < limite) {
				ConsumerRecords<String, String> registros = consumidor.poll(Duration.ofMillis(500));
				leidos += registros.count();
			}
			assertThat(leidos).as("el grupo debe alcanzar a leer 4 mensajes").isGreaterThanOrEqualTo(4);
			consumidor.commitSync();
		}
	}

	@AfterAll
	static void apagarBroker() {
		broker.destroy();
	}

	private KafkaStreamInspector inspector(String bootstrap) {
		KafkaAdmin admin = new KafkaAdmin(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap));
		return new KafkaStreamInspector(admin, TOPIC, List.of(GRUPO_AL_DIA, GRUPO_SIN_LEER));
	}

	@Test
	void informaOffsetsFinalesPorParticion() {
		KafkaStreamInspector.StreamEstado estado = inspector(broker.getBrokersAsString()).inspeccionar();

		assertThat(estado.topic()).isEqualTo(TOPIC);
		assertThat(estado.particiones()).isEqualTo(3);
		assertThat(estado.detalle()).hasSize(3);
		assertThat(estado.totalEventos()).isEqualTo(6);
		assertThat(estado.detalle().stream().mapToLong(KafkaStreamInspector.ParticionOffsets::offsetFinal).sum()).isEqualTo(6);
	}

	@Test
	void calculaElLagDeCadaGrupo() {
		KafkaStreamInspector.StreamEstado estado = inspector(broker.getBrokersAsString()).inspeccionar();

		KafkaStreamInspector.GrupoEstado alDia = estado.grupos().stream()
			.filter(g -> g.grupo().equals(GRUPO_AL_DIA)).findFirst().orElseThrow();
		KafkaStreamInspector.GrupoEstado sinLeer = estado.grupos().stream()
			.filter(g -> g.grupo().equals(GRUPO_SIN_LEER)).findFirst().orElseThrow();

		// Leyó 4 de 6: le quedan 2. Y el avance por partición nunca supera el final.
		assertThat(alDia.lagTotal()).isEqualTo(2);
		assertThat(alDia.particiones().stream().mapToLong(KafkaStreamInspector.ParticionGrupo::offsetActual).sum()).isEqualTo(4);
		assertThat(alDia.particiones()).allSatisfy(p -> assertThat(p.offsetActual()).isLessThanOrEqualTo(p.offsetFinal()));

		// Un grupo que nunca confirmó nada tiene todo el tópico pendiente.
		assertThat(sinLeer.lagTotal()).isEqualTo(6);
		assertThat(sinLeer.particiones()).allSatisfy(p -> assertThat(p.offsetActual()).isZero());
	}

	@Test
	void conKafkaCaidoResponde503SinColgarse() {
		long inicio = System.currentTimeMillis();

		assertThatThrownBy(() -> inspector("localhost:1").inspeccionar())
			.isInstanceOf(ResponseStatusException.class)
			.hasMessageContaining("503");

		assertThat(System.currentTimeMillis() - inicio).as("debe fallar en pocos segundos").isLessThan(15_000);
	}
}
