package portfolio.ads;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import static org.assertj.core.api.Assertions.assertThat;

class AttributionTest {
    @Test
    void clickAndViewWindowsDiffer() {
        var now = Instant.parse("2026-01-09T00:00:00Z");
        assertThat(Attribution.windowStart(now, AdsEvent.Kind.CLICK)).isEqualTo(now.minus(7, ChronoUnit.DAYS));
        assertThat(Attribution.windowStart(now, AdsEvent.Kind.IMPRESSION)).isEqualTo(now.minus(1, ChronoUnit.DAYS));
    }

    @Test
    void timestampsRejectPastAndFutureOutsidePolicy() {
        var now = Instant.parse("2026-01-09T00:00:00Z");
        assertThat(EventApi.validTime(now.plusSeconds(300), now)).isTrue();
        assertThat(EventApi.validTime(now.plusSeconds(301), now)).isFalse();
        assertThat(EventApi.validTime(Instant.EPOCH, now)).isFalse();
    }
}
