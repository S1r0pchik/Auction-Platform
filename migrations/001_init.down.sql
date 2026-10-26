DROP TABLE IF EXISTS outbox_events;
ALTER TABLE auctions DROP CONSTRAINT IF EXISTS last_bid_belongs_to_auction;
DROP TABLE IF EXISTS bids, auctions, sessions, users;
DELETE FROM schema_migrations WHERE version=1;
