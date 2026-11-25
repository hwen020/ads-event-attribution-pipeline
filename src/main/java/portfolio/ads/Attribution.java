package portfolio.ads;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

final class Attribution {
    static final Duration LATENESS = Duration.ofDays(2);
    static final Duration CLICK_WINDOW = Duration.ofDays(7);
    static final Duration VIEW_WINDOW = Duration.ofDays(1);

    static Instant windowStart(Instant conversion, AdsEvent.Kind kind) {
        return conversion.minus(kind == AdsEvent.Kind.CLICK ? CLICK_WINDOW : VIEW_WINDOW);
    }

    static Optional<AdsEvent> winner(UUID userId, Instant conversionTime, List<AdsEvent> events) {
        return events.stream()
            .filter(AdsEvent::eligible)
            .filter(event -> event.userId().equals(userId))
            .filter(event -> event.type() == AdsEvent.Kind.CLICK || event.type() == AdsEvent.Kind.IMPRESSION)
            .filter(event -> !event.eventTime().isAfter(conversionTime))
            .filter(event -> !event.eventTime().isBefore(windowStart(conversionTime, event.type())))
            .sorted(Comparator
                .comparing((AdsEvent event) -> event.type() == AdsEvent.Kind.CLICK)
                .thenComparing(AdsEvent::eventTime)
                .thenComparing(event -> event.eventId().toString()))
            .reduce((first, second) -> second);
    }

    private Attribution() {}
}
