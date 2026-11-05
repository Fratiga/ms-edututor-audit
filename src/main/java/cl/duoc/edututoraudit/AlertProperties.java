package cl.duoc.edututoraudit;

import org.springframework.boot.context.properties.ConfigurationProperties;

// Umbral de la regla de cancelaciones: a partir de `umbral` cancelaciones de un
// mismo estudiante dentro de `ventanaDias`, se publica una alerta.
@ConfigurationProperties(prefix = "edututor.alerts.cancelaciones")
public record AlertProperties(Integer umbral, Integer ventanaDias) {

	public int umbralOrDefault() {
		return umbral == null ? 3 : umbral;
	}

	public int ventanaDiasOrDefault() {
		return ventanaDias == null ? 7 : ventanaDias;
	}
}
