package cl.duoc.edututoraudit;

// El mensaje llegó pero no sirve (le faltan campos obligatorios): reintentarlo
// no lo arregla, así que va directo al tópico DLT sin pasar por los reintentos.
public class InvalidEventException extends RuntimeException {

	public InvalidEventException(String mensaje) {
		super(mensaje);
	}
}
