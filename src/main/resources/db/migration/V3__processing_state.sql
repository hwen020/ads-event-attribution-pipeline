CREATE VIEW event_processing_state AS
    SELECT event_id, received_at, processed_at, eligible
    FROM raw_events;
