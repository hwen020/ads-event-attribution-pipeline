package portfolio.ads;

import java.time.Duration;
import java.time.Instant;

final class Attribution {
    static final Duration LATENESS = Duration.ofDays(2);
    static final Duration CLICK_WINDOW = Duration.ofDays(7);
    static final Duration VIEW_WINDOW = Duration.ofDays(1);

    static Instant windowStart(Instant conversion, AdsEvent.Kind kind) {
        return conversion.minus(kind == AdsEvent.Kind.CLICK ? CLICK_WINDOW : VIEW_WINDOW);
    }

    private Attribution() {}
}
