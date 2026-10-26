# Design notes

## Bids

Each bid locks its auction row with `SELECT FOR UPDATE`. At `READ COMMITTED`, a
waiting request sees the price left by the previous transaction before checking
its own amount. A Go mutex would not protect the data across separate processes.

The order is auction lock, bid insert, then outbox insert. The closing worker uses
the same auction lock. The bid, new price, and event commit together or all roll back.

The request key is checked before the current auction rules. This lets a client
repeat an accepted bid even after the auction closes. A different amount with the
same key returns a conflict.

The deadline check uses `clock_timestamp()` after getting the lock. PostgreSQL
`now()` would return the transaction start time, which may be before a lock wait.
A bid is allowed when `starts_at <= checked time < ends_at`. Its commit can finish
later. If the client loses the response, it should retry with the same key.

Closing an auction saves its final state and event in one transaction. The last
bidder becomes the winner; no bids means no winner. Moving from `scheduled` to
`active` does not create an event or change the version.

Database constraints protect IDs, prices, and links between rows. The remaining
rules depend on the store methods; direct SQL can still break them.

## Events

The handler writes events to an outbox table in PostgreSQL. A separate publisher
sends them to Kafka and marks them as published after Kafka confirms the send.
A crash between that confirmation and the database commit causes a repeat.

Each worker takes the first pending event of an auction. `SKIP LOCKED` lets other
workers serve other auctions. A `NOT EXISTS` check prevents a later event from
passing one that is locked, waiting for retry, or failing. The Kafka key is the
auction ID, so events for one auction use one partition.

There are four workers by default. Sending to Kafka holds an outbox transaction
open, with a 15-second timeout. Failed sends retry with a delay of up to 30 seconds.
They do not block unrelated auctions unless Kafka itself is down. Pending events
are never dropped just because they have failed many times.

The consumer validates events, passes them to the WebSocket hub, then commits
Kafka offsets. Automatic commits are disabled. Group rebalancing waits until the
batch is finished. Offset commits have a three-second timeout.

Temporary errors restart the consumer with a fresh client and a delay of up to
30 seconds. Invalid events stop consumption until the source or code is fixed
and the API is restarted. REST keeps working during these failures.

## WebSocket

Each subscription remembers its last event version. Repeated or older versions
are ignored. A slow client is disconnected so it cannot block other clients.

Updates are not stored for each client. After connecting, the client fetches the
current auction through REST and ignores older buffered events. It fetches again
after reconnecting or seeing a gap in versions.

This is not exactly-once delivery. A future consumer that changes database records
would need its own stored event IDs and transaction.

## Limits

Bids on different auctions run in parallel; bids on one auction wait for the same
row lock. The API allows eight bid requests at once, at most two per auction.
Extra requests get 429. Waiting for a database lock is limited to 500 ms.

The local setup has one API instance and one Kafka broker. More API instances
would need a way to send each event to all their WebSocket hubs. One shared Kafka
group would split events between instances instead.

Consumer status separates broker access, assigned partitions, offset lag, and
recent progress. These readings help with debugging but are not an atomic snapshot
or proof that every client received an update.

## References

- [PostgreSQL transaction isolation](https://www.postgresql.org/docs/current/transaction-iso.html)
- [PostgreSQL row locks](https://www.postgresql.org/docs/current/explicit-locking.html)
- [PostgreSQL time functions](https://www.postgresql.org/docs/current/functions-datetime.html)
- [franz-go client](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo)
