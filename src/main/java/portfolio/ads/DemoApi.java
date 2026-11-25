package portfolio.ads;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Validator;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.time.Clock;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/demo")
@Profile("public-demo")
class DemoApi {
    private static final int MAX_BYTES = 16 * 1024;
    private final DemoSessionStore sessions;
    private final ObjectMapper json;
    private final Validator validator;
    private final Clock clock;

    DemoApi(DemoSessionStore sessions, ObjectMapper json, Validator validator, Clock clock) {
        this.sessions = sessions;
        this.json = json;
        this.validator = validator;
        this.clock = clock;
    }

    @PostMapping("/sessions")
    DemoSessionStore.SpaceView create() {
        return sessions.create();
    }

    @GetMapping("/state")
    DemoSessionStore.SpaceView state(@RequestHeader("X-Demo-Session") UUID sessionId) {
        return sessions.state(sessionId);
    }

    @PostMapping(path = "/events", consumes = "application/json")
    ResponseEntity<?> ingest(@RequestHeader("X-Demo-Session") UUID sessionId,
                             HttpServletRequest request) throws IOException {
        long length = request.getContentLengthLong();
        if (length > MAX_BYTES) throw new ResponseStatusException(HttpStatus.PAYLOAD_TOO_LARGE);
        byte[] body = request.getInputStream().readNBytes(MAX_BYTES + 1);
        if (body.length > MAX_BYTES) throw new ResponseStatusException(HttpStatus.PAYLOAD_TOO_LARGE);

        DemoEventRequest payload;
        try {
            payload = json.reader()
                .with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                .forType(DemoEventRequest.class)
                .readValue(body);
        } catch (JacksonException malformed) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Malformed event JSON");
        }
        AdsEvent event = payload == null ? null : payload.event();
        if (event == null || !validator.validate(event).isEmpty()
                || !EventApi.validTime(event.eventTime(), clock.instant())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid synthetic event");
        }
        var simulatedReceivedAt = payload.simulatedReceivedAt();
        if (simulatedReceivedAt == null || simulatedReceivedAt.isBefore(event.eventTime())
                || simulatedReceivedAt.isAfter(clock.instant().plus(java.time.Duration.ofMinutes(5)))) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid simulated receive timestamp");
        }

        var stored = sessions.ingest(sessionId, event, simulatedReceivedAt);
        return ResponseEntity.ok(Map.of("outcome", stored.outcome(), "state", stored.state()));
    }

    @PostMapping("/reset")
    DemoSessionStore.SpaceView reset(@RequestHeader("X-Demo-Session") UUID sessionId) {
        return sessions.reset(sessionId);
    }

    @GetMapping("/health")
    Map<String, String> health() {
        return Map.of("status", "UP", "mode", "public-demo",
            "transport", "synchronous-in-process", "storage", "ephemeral-in-memory");
    }
}
