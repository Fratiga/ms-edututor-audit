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

	// Complementarios para el frontend oficial: todo evento que llega a esta
	// tabla ya se publicó y consumió con éxito (si Kafka hubiera fallado, el
	// evento ni siquiera existiría acá), y siempre viene del mismo tópico.
	private String origen = "ms-edututor-sessions";

	private String resultado = "EXITOSO";

	// De dónde salió el evento en Kafka (tópico sessions.events): partición y
	// offset del mensaje, y su key (sessionId). Permite rastrear cada registro
	// hasta su posición exacta en el log. Nulos en los registros anteriores a
	// esta columna.
	private Integer kafkaParticion;

	private Long kafkaOffset;

	private String kafkaKey;

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

	public void registrarOrigenKafka(int particion, long offset, String key) {
		this.kafkaParticion = particion;
		this.kafkaOffset = offset;
		this.kafkaKey = key;
	}

	public Integer getKafkaParticion() {
		return kafkaParticion;
	}

	public Long getKafkaOffset() {
		return kafkaOffset;
	}

	public String getKafkaKey() {
		return kafkaKey;
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

	public String getOrigen() {
		return origen;
	}

	public String getResultado() {
		return resultado;
	}
}
