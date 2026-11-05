package cl.duoc.edututoraudit;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

// Solo lectura: estado del tópico de eventos y del avance de cada grupo.
@RestController
@RequestMapping("/api/audit/kafka")
public class KafkaStreamController {

	private final KafkaStreamInspector inspector;

	public KafkaStreamController(KafkaStreamInspector inspector) {
		this.inspector = inspector;
	}

	// GET /api/audit/kafka/stream
	@GetMapping("/stream")
	public KafkaStreamInspector.StreamEstado stream() {
		return inspector.inspeccionar();
	}
}
