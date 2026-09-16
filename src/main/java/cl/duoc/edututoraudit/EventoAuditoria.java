package cl.duoc.edututoraudit;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;

// Timeline de eventos académicos: quién solicitó, confirmó, asignó o cerró
// una sesión. Esquema propio, desacoplado del transaccional de sessions.
@Entity
public class EventoAuditoria {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(unique = true)
	private String eventId;

	private Long sessionId;

	private String tipo;

	private String estudianteId;

	private String tutorId;

	private String estadoAnterior;

	private String estadoNuevo;

	private Instant ocurridoEn;

	private Instant registradoEn;

	protected EventoAuditoria() {
	}

	public EventoAuditoria(String eventId, Long sessionId, String tipo, String estudianteId, String tutorId,
			String estadoAnterior, String estadoNuevo, Instant ocurridoEn) {
		this.eventId = eventId;
		this.sessionId = sessionId;
		this.tipo = tipo;
		this.estudianteId = estudianteId;
		this.tutorId = tutorId;
		this.estadoAnterior = estadoAnterior;
		this.estadoNuevo = estadoNuevo;
		this.ocurridoEn = ocurridoEn;
		this.registradoEn = Instant.now();
	}

	public Long getId() {
		return id;
	}

	public String getEventId() {
		return eventId;
	}

	public Long getSessionId() {
		return sessionId;
	}

	public String getTipo() {
		return tipo;
	}

	public String getEstudianteId() {
		return estudianteId;
	}

	public String getTutorId() {
		return tutorId;
	}

	public String getEstadoAnterior() {
		return estadoAnterior;
	}

	public String getEstadoNuevo() {
		return estadoNuevo;
	}

	public Instant getOcurridoEn() {
		return ocurridoEn;
	}

	public Instant getRegistradoEn() {
		return registradoEn;
	}
}
