package cl.duoc.edututoraudit;

import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;

// Valida el DTO recibido de Kafka con las anotaciones de Bean Validation.
@Component
public class EventValidator {

	private final Validator validator;

	public EventValidator(Validator validator) {
		this.validator = validator;
	}

	public void validar(Object evento) {
		if (evento == null) {
			throw new InvalidEventException("el mensaje no tiene contenido");
		}
		var violaciones = validator.validate(evento);
		if (!violaciones.isEmpty()) {
			String detalle = violaciones.stream()
				.map(ConstraintViolation::getMessage)
				.sorted()
				.collect(Collectors.joining("; "));
			throw new InvalidEventException(detalle);
		}
	}
}
