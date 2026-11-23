package portfolio.ads;

import org.springframework.boot.actuate.health.HealthEndpoint;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1")
class MetricsApi {
    private static final String FROM = "2000-01-01T00:00:00Z";
    private static final String TO = "9999-01-01T00:00:00Z";
    private static final String SUMS = """
        coalesce(sum(impressions),0) AS impressions, coalesce(sum(clicks),0) AS clicks,
        coalesce(sum(conversions),0) AS conversions, coalesce(sum(unattributed),0) AS unattributed_conversions,
        coalesce(sum(clicks)::numeric/nullif(sum(impressions),0),0) AS ctr,
        coalesce(sum(conversions-unattributed)::numeric/nullif(sum(clicks),0),0) AS cvr
        """;
    private static final String FILTER =
        "event_time >= ? AND event_time < ? AND (?::uuid IS NULL OR campaign_id = ?)";
    private final JdbcTemplate db;
    private final HealthEndpoint health;

    MetricsApi(JdbcTemplate db, HealthEndpoint health) {
        this.db = db;
        this.health = health;
    }

    @GetMapping("/metrics/overview")
    Map<String, Object> overview(@RequestParam(defaultValue = FROM) Instant from,
                                 @RequestParam(defaultValue = TO) Instant to,
                                 @RequestParam(required = false) UUID campaignId) {
        requireRange(from, to);
        var result = db.queryForMap("SELECT " + SUMS + " FROM campaign_metrics WHERE " + FILTER,
            EventStore.sql(from), EventStore.sql(to), campaignId, campaignId);
        result.put("attributed_revenue", revenue(from, to, campaignId));
        result.put("processing_latency_ms", db.queryForObject(
            "SELECT avg(extract(epoch FROM processed_at-received_at)*1000) FROM raw_events WHERE " + FILTER,
            java.math.BigDecimal.class, EventStore.sql(from), EventStore.sql(to), campaignId, campaignId));
        return result;
    }

    @GetMapping("/metrics/campaigns")
    List<Map<String, Object>> campaigns(@RequestParam(defaultValue = FROM) Instant from,
        @RequestParam(defaultValue = TO) Instant to, @RequestParam(required = false) UUID campaignId,
        @RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "50") int size) {
        requireRange(from, to);
        long offset = offset(page, size);
        return db.queryForList("SELECT campaign_id, " + SUMS + """
            , coalesce(jsonb_object_agg(currency, total) FILTER (WHERE currency IS NOT NULL),
                       '{}'::jsonb)::text AS revenue_by_currency
            FROM (
                SELECT campaign_id, currency, sum(impressions) AS impressions, sum(clicks) AS clicks,
                    sum(conversions) AS conversions, sum(unattributed) AS unattributed, sum(revenue) AS total
                FROM campaign_metrics WHERE campaign_id IS NOT NULL AND
            """ + FILTER + """
                GROUP BY campaign_id, currency
            ) grouped GROUP BY campaign_id ORDER BY campaign_id LIMIT ? OFFSET ?
            """, EventStore.sql(from), EventStore.sql(to), campaignId, campaignId, size, offset);
    }

    @GetMapping("/metrics/campaigns/{campaignId}")
    Map<String, Object> campaign(@PathVariable UUID campaignId,
        @RequestParam(defaultValue = FROM) Instant from, @RequestParam(defaultValue = TO) Instant to) {
        return overview(from, to, campaignId);
    }

    @GetMapping("/attributions")
    List<Map<String, Object>> attributions(@RequestParam(defaultValue = FROM) Instant from,
        @RequestParam(defaultValue = TO) Instant to, @RequestParam(required = false) UUID campaignId,
        @RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "50") int size) {
        requireRange(from, to);
        return db.queryForList("""
            SELECT conversion_id, event_id, event_time, touch_id, campaign_id, version, value, currency,
                CASE WHEN NOT eligible THEN 'LATE'
                     WHEN touch_id IS NOT NULL THEN 'ATTRIBUTED'
                     WHEN clock_timestamp() < finalize_at THEN 'PENDING'
                     ELSE 'UNATTRIBUTED' END AS status
            FROM attribution_results WHERE
            """ + FILTER + " ORDER BY event_time DESC, conversion_id LIMIT ? OFFSET ?",
            EventStore.sql(from), EventStore.sql(to), campaignId, campaignId, size, offset(page, size));
    }

    @GetMapping("/health")
    ResponseEntity<?> health() {
        var report = health.health();
        return ResponseEntity.status("UP".equals(report.getStatus().getCode()) ? 200 : 503)
            .body(Map.of("status", report.getStatus().getCode()));
    }

    static void requireRange(Instant from, Instant to) {
        if (!from.isBefore(to)) throw new org.springframework.web.server.ResponseStatusException(
            org.springframework.http.HttpStatus.BAD_REQUEST, "from must precede to");
    }

    private List<Map<String, Object>> revenue(Instant from, Instant to, UUID campaign) {
        return db.queryForList("""
            SELECT currency, sum(revenue) AS revenue FROM campaign_metrics WHERE currency IS NOT NULL
                AND campaign_id IS NOT NULL AND
            """ + FILTER + " GROUP BY currency ORDER BY currency",
            EventStore.sql(from), EventStore.sql(to), campaign, campaign);
    }

    private static long offset(int page, int size) {
        if (page < 0 || size < 1 || size > 200)
            throw new org.springframework.web.server.ResponseStatusException(
                org.springframework.http.HttpStatus.BAD_REQUEST, "Invalid pagination");
        return (long) page * size;
    }
}
