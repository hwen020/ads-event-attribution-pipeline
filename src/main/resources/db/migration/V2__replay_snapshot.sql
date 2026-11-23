ALTER TABLE replay_jobs DROP COLUMN snapshot_max;
ALTER TABLE replay_jobs ADD COLUMN total bigint NOT NULL DEFAULT 0;
CREATE TABLE replay_items (
    job_id uuid NOT NULL REFERENCES replay_jobs,
    sequence_id bigint NOT NULL REFERENCES raw_events(sequence_id),
    PRIMARY KEY (job_id,sequence_id)
);
