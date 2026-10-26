CREATE INDEX outbox_published_retention ON outbox_events(published_at, seq) WHERE published_at IS NOT NULL;
