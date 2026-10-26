ALTER TABLE outbox_events ADD COLUMN last_error text;
ALTER TABLE outbox_events ADD COLUMN last_attempt_at timestamptz;
CREATE INDEX outbox_auction_pending ON outbox_events(auction_id,seq)
 WHERE published_at IS NULL;
