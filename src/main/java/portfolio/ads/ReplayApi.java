package portfolio.ads;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/replay/jobs")
@Profile("!public-demo")
class ReplayApi {
    private final Replay replay;
    private final String secret;

    ReplayApi(Replay replay, @Value("${ads.replay-token}") String secret) {
        this.replay = replay;
        this.secret = secret;
    }

    @PostMapping
    ResponseEntity<?> create(@RequestHeader(value = "Authorization", required = false) String auth,
        @RequestParam Instant from, @RequestParam Instant to, @RequestParam(required = false) UUID campaignId) {
        authorize(auth);
        MetricsApi.requireRange(from, to);
        var id = replay.create(from, to, campaignId);
        return ResponseEntity.accepted().body(Map.of("id", id));
    }

    @GetMapping("/{id}")
    Map<String, Object> get(@RequestHeader(value = "Authorization", required = false) String auth,
                            @PathVariable UUID id) {
        authorize(auth);
        return replay.get(id);
    }

    @PostMapping("/{id}/resume")
    ResponseEntity<?> resume(@RequestHeader(value = "Authorization", required = false) String auth,
                             @PathVariable UUID id) {
        authorize(auth);
        if (!replay.resume(id)) throw new ResponseStatusException(HttpStatus.CONFLICT);
        return ResponseEntity.accepted().build();
    }

    private void authorize(String auth) {
        if (secret.isBlank()) throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        if (auth == null || !MessageDigest.isEqual(("Bearer " + secret).getBytes(StandardCharsets.UTF_8),
                auth.getBytes(StandardCharsets.UTF_8))) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED);
        }
    }
}
