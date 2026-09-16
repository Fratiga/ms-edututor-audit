package cl.duoc.edututoraudit;

import java.io.IOException;

import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

@Component
public class RequireBffContextFilter extends OncePerRequestFilter {

	@Override
	protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
			throws ServletException, IOException {
		if (request.getHeader("X-User-Id") == null) {
			response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Falta contexto de identidad del BFF (X-User-Id)");
			return;
		}
		chain.doFilter(request, response);
	}
}
