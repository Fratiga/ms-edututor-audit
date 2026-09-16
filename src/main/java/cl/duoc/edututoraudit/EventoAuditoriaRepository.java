package cl.duoc.edututoraudit;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface EventoAuditoriaRepository extends JpaRepository<EventoAuditoria, Long> {

	Optional<EventoAuditoria> findByEventId(String eventId);

	List<EventoAuditoria> findBySessionIdOrderByOcurridoEnAsc(Long sessionId);

	@Query("""
		SELECT e FROM EventoAuditoria e
		WHERE (:estudianteId IS NULL OR e.estudianteId = :estudianteId)
		AND (:tipo IS NULL OR e.tipo = :tipo)
		AND (:desde IS NULL OR e.ocurridoEn >= :desde)
		AND (:hasta IS NULL OR e.ocurridoEn <= :hasta)
		ORDER BY e.ocurridoEn DESC
		""")
	List<EventoAuditoria> buscar(
		@Param("estudianteId") String estudianteId,
		@Param("tipo") String tipo,
		@Param("desde") Instant desde,
		@Param("hasta") Instant hasta);
}
