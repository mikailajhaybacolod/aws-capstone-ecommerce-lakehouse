-- Quality Test evidence — the 4 injected bad rows with the rule each broke.
-- Row test-both-fail-003 lists TWO rules, proving metadata captures every failure.
-- dq_source_path traces each row back to its Bronze partition.
SELECT
  user_session,
  event_type,
  price,
  user_id,
  dq_failed_rules,
  dq_timestamp,
  dq_source_path
FROM mikaila_quarantine.quarantine_events;