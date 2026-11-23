package portfolio.ads;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.validation.Validator;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/events")
final class EventApi {
    private static final int MAX_BYTES = 256 * 1024;
    private final ObjectMapper json;
    private final Validator validator;
    private final Transport transport;
    private final Clock clock;
    private final int rate;
    private double tokens;
    private long lastRefill = System.nanoTime();

    EventApi(ObjectMapper json, Validator validator, Transport transport, Clock clock,
             @Value("${ads.requests-per-second}") int rate) {
        if (rate < 1) throw new IllegalArgumentException("Request rate must be positive");
        this.json = json;
        this.validator = validator;
        this.transport = transport;
        this.clock = clock;
        this.rate = rate;
        this.tokens = rate;
    }

    private synchronized boolean permit() {
        long now = System.nanoTime();
        tokens = Math.min(rate, tokens + (now - lastRefill) / 1e9 * rate);
        lastRefill = now;
        if (tokens < 1) return false;
        tokens--;
        return true;
    }

    @PostMapping(consumes = "application/json")
    ResponseEntity<?> single(HttpServletRequest request, HttpServletResponse response) throws IOException {
        return ingest(request, response, false);
    }

    @PostMapping(path = "/batch", consumes = "application/json")
    ResponseEntity<?> batch(HttpServletRequest request, HttpServletResponse response) throws IOException {
        return ingest(request, response, true);
    }

    private ResponseEntity<?> ingest(HttpServletRequest req, HttpServletResponse res, boolean batch)
            throws IOException {
        String supplied = req.getHeader("X-Correlation-ID");
        String correlation = supplied != null && supplied.matches("[A-Za-z0-9_.-]{1,96}")
            ? supplied : UUID.randomUUID().toString();
        res.setHeader("X-Correlation-ID", correlation);
        try (var ignored = MDC.putCloseable("correlation_id", correlation)) {
            if (!permit()) {
                res.setHeader("Retry-After", "1");
                throw new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS);
            }
            byte[] body = req.getInputStream().readNBytes(MAX_BYTES + 1);
            if (body.length > MAX_BYTES) throw new ResponseStatusException(HttpStatus.PAYLOAD_TOO_LARGE);
            List<AdsEvent> incoming;
            try {
                var reader = json.reader().with(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
                if (batch) {
                    AdsEvent[] values = reader.forType(AdsEvent[].class).readValue(body);
                    incoming = values == null ? List.of() : java.util.Arrays.asList(values);
                } else {
                    incoming = java.util.Collections.singletonList(reader.forType(AdsEvent.class).readValue(body));
                }
            } catch (com.fasterxml.jackson.core.JsonProcessingException malformed) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Malformed event JSON");
            }
            if (incoming.isEmpty() || incoming.size() > 500)
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Batch must contain 1 to 500 events");

            Instant now = clock.instant();
            var events = new ArrayList<AdsEvent>(incoming.size());
            for (var event : incoming) {
                if (event == null || !validator.validate(event).isEmpty() || !validTime(event.eventTime(), now))
                    throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid event fields or timestamp");
                events.add(event.received(now));
            }
            try {
                transport.publish(events, correlation);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Retry with the same event IDs");
            } catch (Exception unavailable) {
                throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Retry with the same event IDs");
            }
            return ResponseEntity.accepted().body(Map.of("acknowledged", events.size(), "correlationId", correlation));
        }
    }

    static boolean validTime(Instant event, Instant received) {
        return !event.isBefore(Instant.parse("2000-01-01T00:00:00Z"))
            && !event.isAfter(received.plus(Duration.ofMinutes(5)));
    }
}
