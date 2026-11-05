package cl.duoc.edututoraudit;

import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.JsonSerializer;

// Lo que no se puede expresar solo con propiedades del cliente Kafka:
//  - el productor JSON (alertas) y el de bytes (reenvío a la DLT),
//  - el manejo de errores del consumidor.
// Bootstrap, consumer group, deserializers, commits, timeouts y el resto de la
// configuración del cliente están en application.properties (spring.kafka.*).
@Configuration
@EnableConfigurationProperties({ AlertProperties.class, KafkaRetryProperties.class })
public class KafkaClientConfig {

	private static final Logger log = LoggerFactory.getLogger(KafkaClientConfig.class);

	// Productor de DTO (alertas). Fechas como texto ISO-8601 y sin header con el
	// nombre de la clase Java: los consumidores son servicios independientes.
	@Bean
	ProducerFactory<String, Object> producerFactory(KafkaProperties propiedades) {
		ObjectMapper mapper = new ObjectMapper()
			.registerModule(new JavaTimeModule())
			.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
		JsonSerializer<Object> valor = new JsonSerializer<>(mapper);
		valor.setAddTypeInfo(false);
		return new DefaultKafkaProducerFactory<>(propiedades.buildProducerProperties(), new StringSerializer(), valor);
	}

	@Bean
	KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
		return new KafkaTemplate<>(producerFactory);
	}

	// Para reenviar a la DLT, byte a byte, un mensaje que ni siquiera se pudo
	// deserializar (por ejemplo, el texto "hola" publicado a mano).
	@Bean
	KafkaTemplate<String, byte[]> dltBytesTemplate(KafkaProperties propiedades) {
		return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(propiedades.buildProducerProperties(),
			new StringSerializer(), new ByteArraySerializer()));
	}

	// Manejo de errores del consumidor (el contenedor lo toma solo):
	//  1. Un mensaje inválido (InvalidEventException) o ilegible (no es JSON) no
	//     se reintenta: va directo a <tópico>.DLT.
	//  2. Un fallo transitorio (por ejemplo, la base de datos) se reintenta con
	//     espera creciente; si sigue fallando, también va a la DLT.
	//  3. Cada paso queda en el log, y el consumo sigue con el mensaje siguiente:
	//     un mensaje defectuoso nunca detiene el procesamiento.
	@Bean
	DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, Object> jsonTemplate,
			KafkaTemplate<String, byte[]> dltBytesTemplate, KafkaRetryProperties retry,
			@Value("${edututor.kafka.dlt-suffix:.DLT}") String sufijoDlt) {

		// La DLT usa la misma partición que el mensaje original (por eso tiene las mismas particiones).
		Map<Class<?>, KafkaOperations<?, ?>> plantillas = new LinkedHashMap<>();
		plantillas.put(byte[].class, dltBytesTemplate);
		plantillas.put(Object.class, jsonTemplate);
		DeadLetterPublishingRecoverer dlt = new DeadLetterPublishingRecoverer(plantillas,
			(registro, error) -> new TopicPartition(registro.topic() + sufijoDlt, registro.partition()));

		ConsumerRecordRecoverer conLog = (registro, error) -> {
			log.error("Mensaje enviado a la DLT {}{} (origen p{} o{} key={}): {}", registro.topic(), sufijoDlt,
				registro.partition(), registro.offset(), registro.key(), causa(error));
			dlt.accept(registro, error);
		};

		ExponentialBackOffWithMaxRetries espera = new ExponentialBackOffWithMaxRetries(retry.maxAttemptsOrDefault());
		espera.setInitialInterval(retry.initialIntervalMsOrDefault());
		espera.setMultiplier(retry.multiplierOrDefault());
		espera.setMaxInterval(retry.maxIntervalMsOrDefault());

		DefaultErrorHandler manejador = new DefaultErrorHandler(conLog, espera);
		manejador.addNotRetryableExceptions(InvalidEventException.class);
		manejador.setCommitRecovered(true);
		manejador.setRetryListeners(new RetryListener() {
			@Override
			public void failedDelivery(org.apache.kafka.clients.consumer.ConsumerRecord<?, ?> registro, Exception error,
					int intento) {
				log.warn("Fallo procesando p{} o{} (intento {}): {}", registro.partition(), registro.offset(), intento,
					causa(error));
			}
		});
		return manejador;
	}

	private static String causa(Throwable error) {
		Throwable raiz = error;
		while (raiz.getCause() != null && raiz.getCause() != raiz) {
			raiz = raiz.getCause();
		}
		return raiz.getClass().getSimpleName() + ": " + raiz.getMessage();
	}
}
