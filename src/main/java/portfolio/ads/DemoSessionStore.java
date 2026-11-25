package portfolio.ads;

import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

@Component
@Profile("public-demo")
class DemoSessionStore {
    static final Duration SESSION_TTL = Duration.ofHours(2);
    static final int MAX_SESSIONS = 200;
    static final int MAX_EVENTS_PER_SESSION = 100;
    private static final Duration CREATE_REFILL = Duration.ofSeconds(6);
    private static final int CREATE_BURST = 10;
    private final Map<UUID, Space> spaces = new LinkedHashMap<>();
    private final Clock clock;
    private final TokenBucket createLimit = new TokenBucket(CREATE_BURST, CREATE_REFILL);
    private final TokenBucket requestLimit = new TokenBucket(120, Duration.ofSeconds(2));
    private final TokenBucket eventLimit = new TokenBucket(120, Duration.ofMillis(500));

    DemoSessionStore(Clock clock) {
        this.clock = clock;
    }

    synchronized SpaceView create() {
        purgeExpired();
        if (!createLimit.take()) {
            throw new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS, "Please wait before creating another demo space");
        }
        if (!requestLimit.take()) {
            throw new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS, "Public demo request capacity reached; retry shortly");
        }
        if (spaces.size() >= MAX_SESSIONS) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Demo spaces are at capacity; try again later");
        }
        var space = new Space(UUID.randomUUID(), UUID.randomUUID(), clock);
        spaces.put(space.id, space);
        return space.view();
    }

    SpaceView state(UUID id) {
        return require(id).view();
    }

    EventResult ingest(UUID id, AdsEvent event, Instant simulatedReceivedAt) {
        return require(id).ingest(event, simulatedReceivedAt);
    }

    SpaceView reset(UUID id) {
        return require(id).reset();
    }

    private synchronized Space require(UUID id) {
        var space = spaces.get(id);
        if (space == null || space.expired(clock.instant())) {
            if (space != null) spaces.remove(id, space);
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Demo space expired; create a new one");
        }
        if (!requestLimit.take()) {
            throw new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS, "Public demo request capacity reached; retry shortly");
        }
        if (!space.requestLimit.take()) {
            throw new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS, "This demo space is limited to 30 requests per minute, with a small initial burst");
        }
        space.touch(clock.instant());
        return space;
    }

    @Scheduled(fixedDelay = 60_000)
    synchronized void purgeExpired() {
        Instant now = clock.instant();
        spaces.values().removeIf(space -> space.expired(now));
    }

    record SpaceView(UUID sessionId, UUID userId, Instant expiresAt, int eventCount,
                     int conversionCount, Map<String, Object> metrics,
                     List<AttributionView> attributions, List<EventView> events) {}

    record AttributionView(UUID conversionId, UUID eventId, Instant eventTime, UUID touchId,
                           UUID campaignId, String status, long revision,
                           BigDecimal value, String currency) {}

    record EventView(UUID eventId, AdsEvent.Kind type, UUID campaignId, UUID conversionId,
                     Instant eventTime, Instant receivedAt, Instant processedAt,
                     boolean eligible, String outcome) {}

    record EventResult(String outcome, SpaceView state) {}

    private static final class Space {
        private final UUID id;
        private final UUID userId;
        private final Clock clock;
        private final LinkedHashMap<UUID, StoredEvent> events = new LinkedHashMap<>();
        private final LinkedHashMap<UUID, Conversion> conversions = new LinkedHashMap<>();
        private final TokenBucket requestLimit = new TokenBucket(MAX_EVENTS_PER_SESSION + 10, Duration.ofSeconds(2));
        private Instant lastActivity;

        private Space(UUID id, UUID userId, Clock clock) {
            this.id = id;
            this.userId = userId;
            this.clock = clock;
            this.lastActivity = clock.instant();
        }

        private synchronized boolean expired(Instant now) {
            return !lastActivity.plus(SESSION_TTL).isAfter(now);
        }

        private synchronized void touch(Instant now) {
            lastActivity = now;
        }

        private synchronized SpaceView view() {
            var metrics = new TreeMap<String, Map<String, Object>>();
            for (var entry : events.values()) {
                var event = entry.event();
                if (event.type() == AdsEvent.Kind.IMPRESSION || event.type() == AdsEvent.Kind.CLICK) {
                    var row = metrics.computeIfAbsent(event.campaignId().toString(), ignored ->
                        new TreeMap<>(Map.of("campaignId", event.campaignId().toString(),
                            "impressions", 0L, "clicks", 0L, "conversions", 0L,
                            "unattributed", 0L, "revenue", new TreeMap<String, BigDecimal>())));
                    String field = event.type() == AdsEvent.Kind.CLICK ? "clicks" : "impressions";
                    row.put(field, (Long) row.get(field) + 1);
                }
            }

            var attributionViews = new ArrayList<AttributionView>();
            for (var conversion : conversions.values()) {
                var winningTouch = conversion.event().eligible() ? winner(conversion.event()) : null;
                String status = !conversion.event().eligible() ? "LATE"
                    : winningTouch == null ? "UNATTRIBUTED" : "ATTRIBUTED";
                attributionViews.add(new AttributionView(conversion.event().conversionId(),
                    conversion.event().eventId(), conversion.event().eventTime(),
                    winningTouch == null ? null : winningTouch.eventId(),
                    winningTouch == null ? null : winningTouch.campaignId(), status,
                    conversion.revision(), conversion.event().conversionValue(),
                    conversion.event().currency()));
                if (status.equals("UNATTRIBUTED")) {
                    continue;
                }
                if (status.equals("ATTRIBUTED")) {
                    var campaign = metrics.computeIfAbsent(winningTouch.campaignId().toString(), ignored ->
                        new TreeMap<>(Map.of("campaignId", winningTouch.campaignId().toString(),
                            "impressions", 0L, "clicks", 0L, "conversions", 0L,
                            "unattributed", 0L, "revenue", new TreeMap<String, BigDecimal>())));
                    campaign.put("conversions", (Long) campaign.get("conversions") + 1);
                    @SuppressWarnings("unchecked")
                    var revenue = (Map<String, BigDecimal>) campaign.get("revenue");
                    revenue.merge(conversion.event().currency(), conversion.event().conversionValue(), BigDecimal::add);
                }
            }
            var eventViews = events.values().stream()
                .map(item -> new EventView(item.event().eventId(), item.event().type(),
                    item.event().campaignId(), item.event().conversionId(), item.event().eventTime(),
                    item.event().receivedAt(), item.processedAt(), item.event().eligible(), item.outcome()))
                .toList();
            var campaignMetrics = new TreeMap<String, Map<String, Object>>();
            metrics.forEach(campaignMetrics::put);
            return new SpaceView(id, userId, lastActivity.plus(SESSION_TTL), events.size(),
                conversions.size(), Map.of("events", events.size(), "conversions", conversions.size(),
                    "impressions", eventViews.stream().filter(e -> e.type() == AdsEvent.Kind.IMPRESSION).count(),
                    "clicks", eventViews.stream().filter(e -> e.type() == AdsEvent.Kind.CLICK).count(),
                    "unattributedConversions", attributionViews.stream()
                        .filter(a -> a.status().equals("UNATTRIBUTED") || a.status().equals("LATE")).count(),
                    "campaigns", campaignMetrics),
                attributionViews, eventViews);
        }

        private synchronized EventResult ingest(AdsEvent incoming, Instant simulatedReceivedAt) {
            if (!incoming.userId().equals(userId)) {
                throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Event user does not belong to this demo space");
            }
            if (events.size() >= MAX_EVENTS_PER_SESSION && !events.containsKey(incoming.eventId())) {
                throw new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS, "Demo space event limit reached; reset it to continue");
            }

            var existing = events.get(incoming.eventId());
            if (existing != null) {
                String outcome = samePayload(existing.event(), incoming) ? "DUPLICATE" : "CONFLICT";
                return new EventResult(outcome, view());
            }
            if (!eventLimit.take()) {
                throw new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS, "Demo request capacity reached; retry shortly");
            }

            Instant processedAt = clock.instant();
            var received = incoming.received(simulatedReceivedAt);
            String outcome = received.eligible() ? "ACCEPTED" : "LATE";
            if (received.type() == AdsEvent.Kind.CONVERSION && conversions.containsKey(received.conversionId())) {
                outcome = "DUPLICATE_CONVERSION";
            }
            events.put(received.eventId(), new StoredEvent(received, processedAt, outcome));
            if (received.type() == AdsEvent.Kind.CONVERSION && !conversions.containsKey(received.conversionId())) {
                conversions.put(received.conversionId(), new Conversion(received));
            }
            if (received.eligible() && (received.type() == AdsEvent.Kind.CONVERSION
                    || received.type() == AdsEvent.Kind.CLICK || received.type() == AdsEvent.Kind.IMPRESSION)) {
                refreshRevisions();
            }
            lastActivity = processedAt;
            return new EventResult(outcome, view());
        }

        private synchronized SpaceView reset() {
            events.clear();
            conversions.clear();
            lastActivity = clock.instant();
            return view();
        }

        private void refreshRevisions() {
            for (var conversion : conversions.values()) {
                var touch = winner(conversion.event());
                UUID newTouch = touch == null ? null : touch.eventId();
                if (!java.util.Objects.equals(conversion.touchId(), newTouch)) {
                    conversion.setTouchId(newTouch);
                    conversion.incrementRevision();
                }
            }
        }

        private AdsEvent winner(AdsEvent conversion) {
            return Attribution.winner(userId, conversion.eventTime(),
                events.values().stream().map(StoredEvent::event).toList()).orElse(null);
        }

        private static boolean samePayload(AdsEvent first, AdsEvent second) {
            return first.eventId().equals(second.eventId()) && first.userId().equals(second.userId())
                && first.type() == second.type() && java.util.Objects.equals(first.campaignId(), second.campaignId())
                && java.util.Objects.equals(first.adId(), second.adId())
                && java.util.Objects.equals(first.conversionId(), second.conversionId())
                && java.util.Objects.equals(first.conversionValue(), second.conversionValue())
                && java.util.Objects.equals(first.currency(), second.currency())
                && first.eventTime().equals(second.eventTime()) && first.source().equals(second.source());
        }
    }

    private record StoredEvent(AdsEvent event, Instant processedAt, String outcome) {}

    private static final class Conversion {
        private final AdsEvent event;
        private UUID touchId;
        private long revision;

        private Conversion(AdsEvent event) {
            this.event = event;
        }

        private AdsEvent event() {
            return event;
        }

        private UUID touchId() {
            return touchId;
        }

        private long revision() {
            return revision;
        }

        private void setTouchId(UUID touchId) {
            this.touchId = touchId;
        }

        private void incrementRevision() {
            revision++;
        }
    }

    private static final class TokenBucket {
        private final int capacity;
        private final double tokensPerNano;
        private double tokens;
        private long lastRefill = System.nanoTime();

        private TokenBucket(int capacity, Duration refillDuration) {
            this.capacity = capacity;
            this.tokensPerNano = 1.0 / refillDuration.toNanos();
            this.tokens = capacity;
        }

        private synchronized boolean take() {
            long now = System.nanoTime();
            tokens = Math.min(capacity, tokens + (now - lastRefill) * tokensPerNano);
            lastRefill = now;
            if (tokens < 1) return false;
            tokens--;
            return true;
        }
    }
}
