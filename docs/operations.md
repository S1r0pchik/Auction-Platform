# Running and checking the app

## Status

`/health/live` checks that HTTP responds. `/health` and `/health/ready` check
PostgreSQL. Kafka failures do not change REST readiness.

`/status` adds database pool use, pending events, overdue auctions, WebSocket
connections, bid limits, and consumer state. Polling every 5–10 seconds is enough
for local checks. Queue sizes come from database queries, not worker heartbeats.

| Consumer state | Meaning |
|---|---|
| `starting` / `joining` | Starting or waiting for Kafka partitions |
| `unknown` | No recent lag reading |
| `running` | Assigned partitions and a recent zero-lag reading |
| `catching_up` | Events are waiting to be processed |
| `stalled` | A commit or progress has taken too long |
| `retrying` | A temporary failure; the consumer will restart |
| `blocked` | An invalid event needs attention |
| `stopped` | The consumer has stopped |

Broker access alone does not show progress. Check lag, recent commits, and errors
too. The readings are taken at different times.

## Kafka outage

Run this against the local Docker setup:

```powershell
./scripts/check-kafka-recovery.ps1
```

It creates demo data, stops Kafka, checks that a bid is still accepted, and starts
Kafka again in a `finally` block. It waits for the outbox to drain and checks that
the API process did not restart. Demo data remains in PostgreSQL.

During the outage, the consumer becomes `retrying`. REST stays ready while the
database is available, and new events wait in the outbox. Clients can use REST
while live updates are delayed. Recovery needs broker startup, group assignment,
retry delays, and backlog processing, so it has no fixed completion time.

For `blocked`, inspect `last_error` for the topic, partition, and offset. Fix the
event source or add support for its format, then restart the API. Invalid events
are not skipped. Restarting without fixing the cause encounters the same event.
Validation checks event structure and matching values, not who sent it. Kafka
write access must stay limited to trusted producers.

## Limits and cleanup

- Login and registration: 30 attempts per minute per direct IP and 10 per email,
  with four requests at once. Counters reset on API restart.
- Bids: eight requests at once, at most two per auction. Row lock waits are
  limited to 500 ms. Extra requests get 429; a lock timeout returns 503.
- WebSocket: `WS_MAX_CONNECTIONS` defaults to 1000, `WS_MAX_PER_AUCTION` to 100.
  Slots stay reserved until the connection handler finishes.
- Publisher: `OUTBOX_WORKERS` defaults to 4 and accepts 1–16. Failed events retry
  without letting later events of the same auction pass them.

API limits are local to one process. Forwarded IP headers are ignored. Public
hosting also needs TLS, private credentials, and request limits at the proxy.

Published events are kept for seven days by default. `OUTBOX_RETENTION_HOURS`
accepts 1–8760 hours. Age is measured from publication, not creation. Pending
events and bid history are never removed by this cleanup. Kafka retention is separate.

Outbox cleanup runs at startup and once a minute, for up to 100 batches of 1000
rows within five seconds. Session cleanup removes up to 1000 expired sessions
per minute. Failures are logged and retried on the next pass. Expired tokens are
rejected even before their rows are removed.

## Failure tests

Full tests with PostgreSQL, Kafka, and the race detector:

```sh
docker compose run --rm tests
```

Publisher crash after Kafka confirms a send:

```sh
docker compose run --rm tests go test -race -v -count=1 -timeout=60s ./tests/integration -run TestPublisherKilledAfterKafkaACK
```

This kills a child process before it marks the event as published. It checks
rollback, repeated Kafka delivery, ordering, and duplicate filtering. It covers
a process crash, not disk loss or a power failure.

`TestHotAuctionDoesNotStarveReads` holds one auction lock and sends competing bids.
It checks that extra bids are rejected while another auction can still be read.
Kafka tests use temporary topics; database tests use separate schemas.

On shutdown, the API cancels background work. HTTP handlers and background workers
share a ten-second shutdown budget. Leaving the Kafka group has a two-second timeout.

## Load tests

Use a local test database:

```powershell
go run ./cmd/loadtest -profiles reads,bids -rates 50,100,200 -duration 10s -confirm 60s -output load-results.json
```

The generator creates two users and one auction per worker, with 32 workers by
default. Data remains in the database; sessions are closed on normal completion.
Each worker bids on its own auction. This does not measure competition for one
auction or WebSocket delivery.

Reports include response codes, successful requests per second, p95/p99 latency,
dropped generator requests, and `/status` before and after each stage. A stage
passes with no dropped requests, at least 99% success, p99 at most 250 ms, and
successful RPS at least 98% of the target.

Check the outbox separately: passing HTTP checks does not prove that the event
queue can keep up. Docker and the generator share the machine. See the
[saved measurements](performance.md) for the limits of the existing results.
