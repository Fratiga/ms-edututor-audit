package cl.duoc.edututoraudit;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.core.KafkaAdmin;

// La aplicación completa necesita Oracle para arrancar, así que esto comprueba
// solo el cableado nuevo: que Spring Boot provea el KafkaAdmin con la
// configuración de spring.kafka.* y que el inspector se construya con él y con
// sus valores por defecto (tópico y grupos).
class KafkaStreamInspectorWiringTest {

	private final ApplicationContextRunner contexto = new ApplicationContextRunner()
		.withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration.class))
		.withUserConfiguration(KafkaStreamInspector.class, KafkaStreamController.class);

	@Test
	void elInspectorSeConstruyeConElKafkaAdminDeSpringBoot() {
		contexto.withPropertyValues("spring.kafka.bootstrap-servers=broker.ejemplo:9092").run(ctx -> {
			assertThat(ctx).hasNotFailed();
			assertThat(ctx).hasSingleBean(KafkaStreamInspector.class);
			assertThat(ctx).hasSingleBean(KafkaStreamController.class);
			assertThat(ctx.getBean(KafkaAdmin.class).getConfigurationProperties().get("bootstrap.servers"))
				.asString().contains("broker.ejemplo:9092");
		});
	}

	@Test
	void aceptaOtroTopicoYOtrosGruposPorConfiguracion() {
		contexto.withPropertyValues(
				"spring.kafka.bootstrap-servers=localhost:9092",
				"edututor.kafka.topic=otro.topico",
				"edututor.kafka.groups=g1,g2,g3")
			.run(ctx -> assertThat(ctx).hasNotFailed());
	}
}
