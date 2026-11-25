package portfolio.ads;

import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
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

    @Test
    void lastEligibleClickWinsAndWindowBoundariesAreInclusive() {
        var user = UUID.randomUUID();
        var conversionTime = Instant.parse("2026-01-09T00:00:00Z");
        var impression = touch(UUID.randomUUID(), user, AdsEvent.Kind.IMPRESSION,
            conversionTime.minus(1, ChronoUnit.HOURS), UUID.randomUUID());
        var earlierClick = touch(UUID.randomUUID(), user, AdsEvent.Kind.CLICK,
            conversionTime.minus(2, ChronoUnit.HOURS), UUID.randomUUID());
        var latestClick = touch(UUID.randomUUID(), user, AdsEvent.Kind.CLICK,
            conversionTime.minus(1, ChronoUnit.HOURS), UUID.randomUUID());
        var outsideWindow = touch(UUID.randomUUID(), user, AdsEvent.Kind.CLICK,
            conversionTime.minus(7, ChronoUnit.DAYS).minusNanos(1), UUID.randomUUID());
        var futureClick = touch(UUID.randomUUID(), user, AdsEvent.Kind.CLICK,
            conversionTime.plusSeconds(1), UUID.randomUUID());

        assertThat(Attribution.winner(user, conversionTime,
            List.of(impression, earlierClick, latestClick, outsideWindow, futureClick)))
            .contains(latestClick);
        assertThat(Attribution.winner(user, conversionTime, List.of(outsideWindow))).isEmpty();
    }

    private static AdsEvent touch(UUID eventId, UUID userId, AdsEvent.Kind kind, Instant time, UUID campaign) {
        return new AdsEvent(eventId, userId, kind, campaign, UUID.randomUUID(), null,
            null, null, time, time, "unit-test");
    }
}
