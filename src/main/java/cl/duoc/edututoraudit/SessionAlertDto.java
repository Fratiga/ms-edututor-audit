package cl.duoc.edututoraudit;

import java.time.Instant;

// Alerta que audit publica en sessions.alerts cuando un patrón del historial
// lo amerita. Key del mensaje: "estudianteId:regla". alertId es el eventId del
// evento que disparó la regla: si se reenvía la misma alerta, el consumidor la
// reconoce y no la duplica.
public record SessionAlertDto(
	String alertId,
	String regla,
	String severidad,
	String estudianteId,
	Long sessionId,
	long valor,
	long umbral,
	int ventanaDias,
	String mensaje,
	Instant detectadaEn
) {

	public static final String CANCELACIONES_REITERADAS = "CANCELACIONES_REITERADAS";
}
