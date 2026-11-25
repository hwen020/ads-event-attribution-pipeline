package portfolio.ads;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.context.annotation.Profile;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

@Service
@Profile("!public-demo")
class EventStore {
    private final JdbcTemplate db;
    private final MeterRegistry meters;

    EventStore(JdbcTemplate db, MeterRegistry meters) {
        this.db = db;
        this.meters = meters;
    }

    static OffsetDateTime sql(Instant time) {
        return time.atOffset(ZoneOffset.UTC);
    }

    @Transactional
    public boolean apply(AdsEvent event, String payload) {
        var sample = io.micrometer.core.instrument.Timer.start(meters);
        try {
            lock(event.userId());
            boolean eligible = event.eligible();
            int inserted = db.update("""
                INSERT INTO raw_events(event_id,user_id,kind,campaign_id,event_time,received_at,
                                       conversion_id,eligible,payload)
                VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT (event_id) DO NOTHING
                """, event.eventId(), event.userId(), event.type().name(), event.campaignId(),
                sql(event.eventTime()), sql(event.receivedAt()), event.conversionId(), eligible, payload);
            if (inserted == 0) return false;
            if (event.type() == AdsEvent.Kind.CONVERSION) {
                int created = db.update("""
                    INSERT INTO attribution_results(conversion_id,event_id,user_id,event_time,
                           finalize_at,eligible,value,currency)
                    VALUES (?,?,?,?,?,?,?,?) ON CONFLICT (conversion_id) DO NOTHING
                    """, event.conversionId(), event.eventId(), event.userId(), sql(event.eventTime()),
                    sql(event.eventTime().plus(Attribution.LATENESS)), eligible,
                    event.conversionValue(), event.currency());
                if (created != 0 && eligible) recalculate(event.userId(), event.eventTime(), event.eventTime());
            } else if (eligible) {
                touch(event.userId(), event.eventTime(), event.type());
            }
            return true;
        } finally {
            sample.stop(meters.timer("ads.database.write"));
        }
    }

    void lock(UUID user) {
        db.query("SELECT pg_advisory_xact_lock(?)", rs -> {}, user.getMostSignificantBits() ^ user.getLeastSignificantBits());
    }

    int touch(UUID user, Instant instant, AdsEvent.Kind kind) {
        var window = kind == AdsEvent.Kind.CLICK ? Attribution.CLICK_WINDOW : Attribution.VIEW_WINDOW;
        return recalculate(user, instant, instant.plus(window));
    }

    int recalculate(UUID user, Instant from, Instant to) {
        var sample = io.micrometer.core.instrument.Timer.start(meters);
        try {
            return db.update("""
                WITH winners AS (
                    SELECT a.conversion_id, winner.event_id, winner.campaign_id
                    FROM attribution_results a
                    LEFT JOIN LATERAL (
                        SELECT t.event_id, t.campaign_id FROM raw_events t
                        WHERE t.user_id = a.user_id AND t.eligible AND t.event_time <= a.event_time
                          AND ((t.kind = 'CLICK' AND t.event_time >= a.event_time - interval '168 hours')
                            OR (t.kind = 'IMPRESSION' AND t.event_time >= a.event_time - interval '24 hours'))
                        ORDER BY (t.kind = 'CLICK') DESC, t.event_time DESC, t.event_id DESC LIMIT 1
                    ) winner ON true
                    WHERE a.user_id = ? AND a.eligible AND a.event_time BETWEEN ? AND ?
                )
                UPDATE attribution_results a SET touch_id = w.event_id, campaign_id = w.campaign_id,
                       version = a.version + 1, updated_at = clock_timestamp()
                FROM winners w WHERE a.conversion_id = w.conversion_id
                  AND (a.touch_id, a.campaign_id) IS DISTINCT FROM (w.event_id, w.campaign_id)
                """, user, sql(from), sql(to));
        } finally {
            sample.stop(meters.timer("ads.attribution"));
        }
    }
}
