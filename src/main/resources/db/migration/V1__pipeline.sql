CREATE TABLE raw_events (
    sequence_id bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
    event_id uuid PRIMARY KEY,
    user_id uuid NOT NULL,
    kind text NOT NULL CHECK (kind IN ('IMPRESSION','CLICK','CONVERSION')),
    campaign_id uuid,
    event_time timestamptz NOT NULL,
    received_at timestamptz NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    conversion_id uuid,
    eligible boolean NOT NULL,
    payload text NOT NULL
);
CREATE INDEX raw_touch ON raw_events (user_id, kind, event_time DESC, event_id DESC)
    WHERE eligible AND kind IN ('CLICK','IMPRESSION');
CREATE INDEX raw_replay ON raw_events (event_time, sequence_id);
CREATE INDEX raw_campaign ON raw_events (campaign_id, event_time);
CREATE TABLE attribution_results (
    conversion_id uuid PRIMARY KEY,
    event_id uuid UNIQUE NOT NULL REFERENCES raw_events,
    user_id uuid NOT NULL,
    event_time timestamptz NOT NULL,
    finalize_at timestamptz NOT NULL,
    eligible boolean NOT NULL,
    value numeric(20,4) NOT NULL,
    currency char(3) NOT NULL,
    touch_id uuid REFERENCES raw_events,
    campaign_id uuid,
    version bigint NOT NULL DEFAULT 1,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX attribution_user ON attribution_results (user_id, event_time) WHERE eligible;
CREATE INDEX attribution_period ON attribution_results (event_time, campaign_id);
CREATE VIEW campaign_metrics AS
    SELECT campaign_id, event_time, (kind = 'IMPRESSION')::int AS impressions,
           (kind = 'CLICK')::int AS clicks, 0 AS conversions, 0 AS unattributed,
           NULL::char(3) AS currency, 0::numeric AS revenue
    FROM raw_events WHERE kind IN ('IMPRESSION','CLICK')
    UNION ALL
    SELECT campaign_id, event_time, 0, 0, 1, (touch_id IS NULL)::int,
           currency, CASE WHEN touch_id IS NULL THEN 0 ELSE value END
    FROM attribution_results;
CREATE TABLE replay_jobs (
    id uuid PRIMARY KEY,
    from_time timestamptz NOT NULL,
    to_time timestamptz NOT NULL,
    campaign_id uuid,
    status text NOT NULL DEFAULT 'QUEUED',
    snapshot_max bigint NOT NULL,
    cursor_id bigint NOT NULL DEFAULT 0,
    scanned bigint NOT NULL DEFAULT 0,
    corrected bigint NOT NULL DEFAULT 0,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE replay_job_log (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id uuid NOT NULL REFERENCES replay_jobs,
    at timestamptz NOT NULL DEFAULT clock_timestamp(),
    message text NOT NULL
);
