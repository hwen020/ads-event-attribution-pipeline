package portfolio.ads;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.context.annotation.Profile;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
@Profile("!public-demo")
class Replay {
    private final JdbcTemplate db;
    private final TransactionTemplate tx;
    private final EventStore store;
    private final MeterRegistry meters;

    Replay(JdbcTemplate db, TransactionTemplate tx, EventStore store, MeterRegistry meters) {
        this.db = db;
        this.tx = tx;
        this.store = store;
        this.meters = meters;
    }

    UUID create(Instant from, Instant to, UUID campaign) {
        return tx.execute(status -> {
            UUID id = UUID.randomUUID();
            db.update("INSERT INTO replay_jobs(id,from_time,to_time,campaign_id) VALUES (?,?,?,?)",
                id, EventStore.sql(from), EventStore.sql(to), campaign);
            int total = db.update("""
                INSERT INTO replay_items(job_id,sequence_id)
                SELECT ?, r.sequence_id FROM raw_events r
                LEFT JOIN attribution_results a ON a.event_id=r.event_id
                WHERE r.event_time >= ? AND r.event_time < ?
                    AND (?::uuid IS NULL OR coalesce(r.campaign_id,a.campaign_id) = ?)
                """, id, EventStore.sql(from), EventStore.sql(to), campaign, campaign);
            db.update("UPDATE replay_jobs SET total=? WHERE id=?", total, id);
            log(id, "Snapshot queued");
            return id;
        });
    }

    @Scheduled(fixedDelay = 750)
    void poll() {
        var jobs = db.queryForList("""
            SELECT id FROM replay_jobs WHERE status IN ('QUEUED','RUNNING') ORDER BY created_at LIMIT 5
            """, UUID.class);
        for (UUID id : jobs) {
            try {
                for (int i = 0; i < 100; i++) {
                    if (!Boolean.TRUE.equals(tx.execute(status -> step(id)))) break;
                }
            } catch (RuntimeException failure) {
                meters.counter("ads.replay.jobs", "status", "failed").increment();
                tx.executeWithoutResult(status -> {
                    int changed = db.update("""
                        UPDATE replay_jobs SET status='FAILED',last_error=?
                        WHERE id=? AND status IN ('QUEUED','RUNNING')
                        """, failure.getClass().getSimpleName(), id);
                    if (changed != 0) log(id, "Transaction rolled back. Resume from the saved cursor.");
                });
            }
        }
    }

    private boolean step(UUID id) {
        var cursor = db.queryForList("""
            SELECT cursor_id FROM replay_jobs WHERE id=? AND status IN ('QUEUED','RUNNING')
            FOR UPDATE SKIP LOCKED
            """, Long.class, id);
        if (cursor.isEmpty()) return false;
        var rows = db.query("""
            SELECT r.sequence_id,r.user_id,r.kind,r.event_time,r.eligible
            FROM replay_items i JOIN raw_events r USING(sequence_id)
            WHERE i.job_id=? AND i.sequence_id>? ORDER BY i.sequence_id LIMIT 1
            """, (rs, row) -> new Item(rs.getLong(1), rs.getObject(2, UUID.class),
                AdsEvent.Kind.valueOf(rs.getString(3)), rs.getObject(4, OffsetDateTime.class).toInstant(),
                rs.getBoolean(5)), id, cursor.getFirst());
        if (rows.isEmpty()) {
            db.update("UPDATE replay_jobs SET status='COMPLETE' WHERE id=?", id);
            log(id, "Snapshot complete");
            meters.counter("ads.replay.jobs", "status", "complete").increment();
            return false;
        }
        var item = rows.getFirst();
        int corrected = 0;
        if (item.eligible()) {
            store.lock(item.user());
            corrected = item.kind() == AdsEvent.Kind.CONVERSION
                ? store.recalculate(item.user(), item.time(), item.time())
                : store.touch(item.user(), item.time(), item.kind());
        }
        db.update("""
            UPDATE replay_jobs SET status='RUNNING',cursor_id=?,scanned=scanned+1,corrected=corrected+?
            WHERE id=?
            """, item.sequence(), corrected, id);
        return true;
    }

    Map<String, Object> get(UUID id) {
        var jobs = db.queryForList("SELECT * FROM replay_jobs WHERE id=?", id);
        if (jobs.isEmpty()) throw new org.springframework.web.server.ResponseStatusException(
            org.springframework.http.HttpStatus.NOT_FOUND);
        jobs.getFirst().put("logs", db.queryForList(
            "SELECT at,message FROM replay_job_log WHERE job_id=? ORDER BY id DESC LIMIT 100", id));
        return jobs.getFirst();
    }

    boolean resume(UUID id) {
        return Boolean.TRUE.equals(tx.execute(status -> {
            int changed = db.update("""
                UPDATE replay_jobs SET status='QUEUED',last_error=NULL WHERE id=? AND status='FAILED'
                """, id);
            if (changed != 0) log(id, "Resume requested");
            return changed != 0;
        }));
    }

    private void log(UUID id, String message) {
        db.update("INSERT INTO replay_job_log(job_id,message) VALUES (?,?)", id, message);
    }

    private record Item(long sequence, UUID user, AdsEvent.Kind kind, Instant time, boolean eligible) {}
}
