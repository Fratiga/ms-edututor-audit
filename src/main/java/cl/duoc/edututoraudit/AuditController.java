package cl.duoc.edututoraudit;

import java.time.Instant;
import java.util.List;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

// Solo lectura, tal como pide el caso: "Consulta el timeline. Solo lectura."
@RestController
@RequestMapping("/api/audit")
public class AuditController {

	private final EventoAuditoriaRepository repository;

	public AuditController(EventoAuditoriaRepository repository) {
		this.repository = repository;
	}

	// GET /api/audit/timeline — filtros: usuario (estudianteId), fechas, tipo de evento.
	@GetMapping("/timeline")
	public List<EventoAuditoria> timeline(
			@RequestParam(required = false) String usuario,
			@RequestParam(required = false) String tipo,
			@RequestParam(required = false) Instant from,
			@RequestParam(required = false) Instant to) {
		return repository.buscar(usuario, tipo, from, to);
	}

	@GetMapping("/session/{id}")
	public List<EventoAuditoria> porSesion(@PathVariable Long id) {
		return repository.findBySessionIdOrderByOcurridoEnAsc(id);
	}
}
