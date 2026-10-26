# Auction Platform

A Go backend for timed auctions. Users can create an auction, place bids, and get
live updates over WebSocket. This is a learning project focused on database
transactions, concurrent requests, and event delivery.

The stack is Go, PostgreSQL, and Kafka. There are no payments or money transfers.

## Run it

You need Docker with Compose and a running Docker Engine.

```sh
docker compose up --build -d
```

The API runs at `http://localhost:8080`. Database migrations and the Kafka topic
are set up automatically. The ports are only open on localhost.

- `GET /health/ready` checks the database connection.
- `GET /status` shows the state of the event queue and background workers.
- [API guide](docs/api.md) lists the requests and examples.

To watch the logs:

```sh
docker compose logs -f api publisher lifecycle
```

To try the main flow in PowerShell:

```powershell
./scripts/demo.ps1
```

The script creates two users, an auction, and a bid. It then repeats the bid
request and checks that the API returns the same bid. The demo data stays in the database.

To stop the services and keep the data:

```sh
docker compose down
```

## How it works

The application runs as three processes:

- `api` handles HTTP requests and sends WebSocket updates.
- `publisher` sends saved events from PostgreSQL to Kafka.
- `lifecycle` starts and closes auctions when their time comes.

Bids on the same auction use a PostgreSQL row lock. Each bid is checked against
the latest price and the current time after getting the lock. A request key lets
the client retry a bid without creating it twice.

A bid and its event are saved in the same transaction. The event waits in an
outbox table until the publisher sends it to Kafka. Events can arrive more than
once, so WebSocket subscriptions ignore versions they have already received.

## Tests

Run the full test suite with PostgreSQL, Kafka, and the Go race detector:

```sh
docker compose run --rm tests
```

The tests cover concurrent bids, repeated requests, auction deadlines, failed
event delivery, login, and WebSocket updates. Database tests use separate schemas
and remove them when finished.

For a quick local check, use the Go version in [go.mod](go.mod):

```sh
go test ./...
go vet ./...
```

Database and Kafka tests are skipped unless `TEST_DATABASE_URL` and
`KAFKA_TEST_BROKERS` are set. The Docker command sets both for you.

## Run Go locally

Start just PostgreSQL and Kafka:

```sh
docker compose up -d postgres kafka kafka-init
```

If the app is already running in Docker, stop its processes first:

```sh
docker compose stop api publisher lifecycle
```

Set these variables in PowerShell, then apply migrations:

```powershell
$env:DATABASE_URL = 'postgres://auction:auction@localhost:5432/auction?sslmode=disable'
$env:KAFKA_BROKERS = 'localhost:9092'
go run ./cmd/auction migrate
```

Run each command below in a separate terminal with the same variables:

```sh
go run ./cmd/auction api
go run ./cmd/auction publisher
go run ./cmd/auction lifecycle
```

Other settings are listed in [.env.example](.env.example).
The app reads environment variables; it does not load `.env` files itself.

## Limits

This setup uses one API instance and one Kafka broker. WebSocket updates can be
missed, so clients should fetch the current auction after reconnecting. A Kafka
outage delays updates, but the REST API can still accept bids while the database is available.

There is no auction editing, cancellation, password reset, or payment flow.
The Docker settings and database credentials are for local development.

More detailed notes: [design](docs/decisions.md),
[operations](docs/operations.md), [migrations](docs/migrations.md),
and [local load tests](docs/performance.md).
