# HTTP API

Base URL: `http://localhost:8080`.

Send request bodies as JSON with `Content-Type: application/json`. The size limit
is 16 KiB, and unknown fields are rejected. IDs are UUIDs. Dates use RFC 3339 with
a timezone, such as `2026-10-26T12:00:00Z`.

All amounts are whole kopecks in RUB: `10000` means 100 rubles.
The largest allowed amount is `9000000000000000`.

## Sign up and log in

| Method | Path | Response |
|---|---|---|
| POST | `/api/v1/auth/register` | 201: `{id, email}` |
| POST | `/api/v1/auth/login` | 200: `{token, expires_at}` |
| POST | `/api/v1/auth/logout` | 204, no response body |

Use the same body for registration and login:

```json
{
  "email": "user@example.com",
  "password": "my-password-123"
}
```

Passwords must be 12–72 bytes long. Emails are stored in lowercase.
A login token lasts 24 hours. Send it in this header:

```http
Authorization: Bearer <token>
```

Creating an auction, placing a bid, and logging out require a token.
Reading auctions, bid history, and WebSocket updates does not.

Logout takes no body and removes only the current session. Repeating logout with
a token in the correct format also returns 204.

Login and registration share a limit of 30 attempts per minute per IP and 10 per
email, including successful attempts. Up to 4 requests can run at once. Exceeding
a limit returns 429. IP limits use the direct connection, not forwarded headers.

## Create an auction

Send `POST /api/v1/auctions` with your token and this body:

```json
{
  "title": "Mechanical keyboard",
  "description": "New, sealed",
  "starting_price": 1000000,
  "min_increment": 50000,
  "starts_at": "2026-10-26T12:00:00Z",
  "ends_at": "2026-10-27T12:00:00Z"
}
```

Replace the dates before sending. The end must be in the future and after the
start. The start can be in the past if you want bidding to open right away.
Dates are stored with microsecond precision; the start and end must still differ at that precision.

The title must be 1–200 characters after trimming spaces. The description can be
up to 10000 bytes. The starting price can be zero; the minimum increment must be positive.

The response is 201 with the auction object. It includes `id`, `owner_id`,
`current_price`, `status`, `version`, and the dates. `leader_id` is the current
leader, or the winner once the auction has ended. It is `null` when there are no bids.

Repeating this request creates another auction.

## Place a bid

```http
POST /api/v1/auctions/{id}/bids
Authorization: Bearer <token>
Idempotency-Key: unique-bid-key
Content-Type: application/json

{"amount":1100000}
```

The first bid must meet the starting price. Later bids must reach the current
price plus `min_increment`. Every bid must be positive. The owner cannot bid on
their own auction. Bidding opens at `starts_at` and closes at `ends_at`.

Use a new `Idempotency-Key` for each new bid. It must contain 1–128 printable ASCII
characters, with no spaces at either end. A key belongs to one user and auction.

- New bid: 201 with `id`, `auction_id`, `bidder_id`, `amount`, `accepted_at`, and `auction_version`.
- Same key and amount: 200 with the original accepted bid and `Idempotency-Replayed: true`.
- Same key with a different amount: 409. Rejected bids do not use up the key.

If the connection fails or you get a 5xx response, retry with the same key.
Repeating an accepted bid also works after the auction ends. After a 429 response,
wait before retrying and follow `Retry-After` when it is present.

## Read auctions and bids

| Method | Path | Response |
|---|---|---|
| GET | `/api/v1/auctions/{id}` | Auction object |
| GET | `/api/v1/auctions?limit=50` | `{items, next_cursor}`, newest first |
| GET | `/api/v1/auctions/{id}/bids?limit=50&after_version=0` | `{items, next_after_version}`, oldest first |

`limit` can be 1–100 and defaults to 50.

For the next page of auctions, pass `next_cursor` as `cursor`. An empty string
means there are no more results. For bids, pass `next_after_version` as
`after_version`. A `null` value means you have reached the end of the current history.

To check for new bids later, use the last `auction_version` you received.
Bid versions may have gaps because creating and closing an auction also increases its version.

## Live updates

Connect to `ws://localhost:8080/api/v1/auctions/{id}/ws`.

1. Open the connection and keep incoming events in a buffer.
2. Fetch the auction through REST.
3. Drop buffered events whose `version` is no newer than the REST response, then process newer events.
4. Fetch the auction again after reconnecting or seeing a gap in versions.

Each event has `event_id`, `type`, `schema_version`, `auction_id`, `version`, and a
full `auction` object. Event types are `auction.created`, `bid.placed`, and
`auction.finished`. A `bid.placed` event also contains `bid`.

WebSocket delivery is not guaranteed. Slow clients are disconnected and should
fetch the current state through REST. The connection does not accept client messages.
Browser connections must use the same host by default.

The default limits are 1000 connections per API instance and 100 per auction.
Extra connections get 429 before the WebSocket connection is opened.

## Errors

```json
{
  "code": "bid_too_low",
  "message": "The amount is below the minimum acceptable bid.",
  "request_id": "...",
  "fields": {"amount": "must be at least minimum_amount (in kopecks)"},
  "details": {"minimum_amount": 1150000}
}
```

Use `code` to handle errors. `fields` and `details` are included when there is
extra information. `_body` refers to the whole request body.

| Status | Meaning |
|---|---|
| 400 | Invalid JSON, fields, or parameters |
| 401 | Missing or invalid token, or incorrect login details |
| 403 | The owner tried to bid on their own auction |
| 404 / 405 | Auction or route not found / method not allowed |
| 408 / 413 | Reading the request took too long / body too large |
| 409 | Auction closed or not started, bid too low, key conflict, or email already registered |
| 429 | Too many requests or WebSocket connections |
| 500 / 503 | Internal error / service temporarily unavailable |

## Service status

| Path | What it checks |
|---|---|
| `/health/live` | The HTTP process is running |
| `/health` and `/health/ready` | PostgreSQL is reachable; otherwise returns 503 |
| `/status` | Database, Kafka consumer, pending events, workers, and limits |

The health checks do not require Kafka to be available. See [operations.md](operations.md)
for more details about failures and limits.
