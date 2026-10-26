# Local load tests

These saved measurements are from October 22, 2026. They describe that local run,
not production capacity or the performance of later code changes.

## Setup

Windows, Intel Core i5-12500H, 16 logical CPUs. Docker Desktop had 16 CPUs and
about 7.6 GiB of memory. The Go 1.27.0 generator ran on Windows; Linux containers
used Go 1.27.3, PostgreSQL 18, and Kafka 4.1.1.

There was one API, four publisher workers, and an API database pool of 20.
Bid limits were eight requests total and two per auction. The database already
contained demo data. HTTP logging and background workers stayed enabled.

The generator had 32 workers with separate auctions. Reads fetched one auction;
bids wrote to PostgreSQL and the outbox. The runs did not test a single popular
auction or WebSocket latency. See [the method](operations.md#load-tests).

## Results

Each row is a separate 60-second run without tests or builds running alongside it.

| Profile | Target RPS | Successful RPS | p95, ms | p99, ms | HTTP 429 | Passed |
|---|---:|---:|---:|---:|---:|---|
| Reads | 400 | 400.0 | 3.28 | 3.93 | 0% | Yes |
| Bids | 200 | 197.0 | 17.23 | 38.62 | 1.50% | No |
| Bids | 100 | 97.6 | 40.70 | 99.45 | 2.33% | No |
| Bids | 50 | 49.9 | 11.05 | 14.18 | 0.17% | Yes |

Reports: [reads](load-reads-2026-10-22.json),
[200 RPS bids](load-bids-2026-10-22.json),
[100 RPS bids](load-bids-100-2026-10-22.json),
[50 RPS bids](load-bids-50-2026-10-22.json).

The 50 RPS bid run had 2995 accepted bids and five 429 responses. It met the
99% success rule; it was not a run with zero failures. The 100 and 200 RPS runs
failed that rule. Database latency and the shared environment varied, so these
results do not show a fixed maximum rate.

An earlier [exploratory run](load-results-2026-10-22.json) tried up to 800 RPS.
At 800 RPS for bids, pending events grew from about 9 to 2177. That rate was not
sustainable for the full pipeline. Its final bid stage overlapped with tests and
a build, so it is not an independent capacity measurement.

That early run also exposed a generator startup delay. Worker readiness and a
short startup wait were added before the separate runs above. The original
reports are kept as recorded, including dropped requests.

## Repeat a run

With the local app running:

```powershell
go run ./cmd/loadtest -profiles reads -rates 400 -duration 60s -confirm 0 -output load-reads.json
go run ./cmd/loadtest -profiles bids -rates 50 -duration 60s -confirm 0 -output load-bids.json
```

Run one command at a time and keep new reports separate from the old ones. The
generator leaves its data in the database. Check outbox growth and consumer lag
alongside HTTP results. Longer runs are needed to assess table growth, cleanup,
and recovery from a backlog.
