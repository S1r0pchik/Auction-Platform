DROP INDEX outbox_auction_pending;
ALTER TABLE outbox_events DROP COLUMN last_error;
ALTER TABLE outbox_events DROP COLUMN last_attempt_at;
DELETE FROM schema_migrations WHERE version=2;
