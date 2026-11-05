package cl.duoc.edututoraudit;

import org.springframework.boot.context.properties.ConfigurationProperties;

// Reintentos antes de enviar un mensaje a la DLT: se espera initialIntervalMs,
// luego el doble (multiplier), hasta maxIntervalMs, un máximo de maxAttempts veces.
@ConfigurationProperties(prefix = "edututor.kafka.retry")
public record KafkaRetryProperties(Integer maxAttempts, Long initialIntervalMs, Double multiplier, Long maxIntervalMs) {

	public int maxAttemptsOrDefault() {
		return maxAttempts == null ? 3 : maxAttempts;
	}

	public long initialIntervalMsOrDefault() {
		return initialIntervalMs == null ? 1000L : initialIntervalMs;
	}

	public double multiplierOrDefault() {
		return multiplier == null ? 2.0 : multiplier;
	}

	public long maxIntervalMsOrDefault() {
		return maxIntervalMs == null ? 8000L : maxIntervalMs;
	}
}
