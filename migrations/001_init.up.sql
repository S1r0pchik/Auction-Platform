CREATE TABLE users (
 id uuid PRIMARY KEY,
 email text NOT NULL UNIQUE,
 password_hash text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE sessions (
 token_hash bytea PRIMARY KEY,
 user_id uuid NOT NULL REFERENCES users(id),
 expires_at timestamptz NOT NULL
);
CREATE INDEX ON sessions(expires_at);
CREATE TABLE auctions (
 id uuid PRIMARY KEY,
 owner_id uuid NOT NULL REFERENCES users(id),
 title text NOT NULL CHECK (length(title) BETWEEN 1 AND 200),
 description text NOT NULL DEFAULT '',
 currency text NOT NULL DEFAULT 'RUB' CHECK (currency = 'RUB'),
 starting_price bigint NOT NULL CHECK (starting_price BETWEEN 0 AND 9000000000000000),
 current_price bigint NOT NULL CHECK (current_price BETWEEN starting_price AND 9000000000000000),
 min_increment bigint NOT NULL CHECK (min_increment BETWEEN 1 AND 9000000000000000),
 starts_at timestamptz NOT NULL,
 ends_at timestamptz NOT NULL CHECK (ends_at > starts_at),
 status text NOT NULL CHECK (status IN ('scheduled','active','finished','cancelled')),
 last_bid_id uuid,
 version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
 finished_at timestamptz,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX ON auctions(status, starts_at);
CREATE INDEX ON auctions(status, ends_at);
CREATE INDEX ON auctions(created_at DESC, id DESC);
CREATE TABLE bids (
 id uuid PRIMARY KEY,
 auction_id uuid NOT NULL REFERENCES auctions(id),
 bidder_id uuid NOT NULL REFERENCES users(id),
 amount bigint NOT NULL CHECK (amount BETWEEN 1 AND 9000000000000000),
 accepted_at timestamptz NOT NULL,
 auction_version bigint NOT NULL,
 idempotency_key text NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 128),
 UNIQUE (auction_id, bidder_id, idempotency_key),
 UNIQUE (auction_id, auction_version),
 UNIQUE (auction_id, id)
);
ALTER TABLE auctions ADD CONSTRAINT last_bid_belongs_to_auction
 FOREIGN KEY (id, last_bid_id) REFERENCES bids(auction_id, id);
CREATE TABLE outbox_events (
 seq bigserial PRIMARY KEY,
 id uuid NOT NULL UNIQUE,
 auction_id uuid NOT NULL REFERENCES auctions(id),
 aggregate_version bigint NOT NULL,
 event_type text NOT NULL,
 schema_version integer NOT NULL DEFAULT 1,
 payload jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 published_at timestamptz,
 attempts integer NOT NULL DEFAULT 0,
 next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 UNIQUE (auction_id, aggregate_version)
);
CREATE INDEX outbox_pending ON outbox_events(seq) WHERE published_at IS NULL;
