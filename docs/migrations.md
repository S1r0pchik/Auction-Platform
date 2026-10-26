# Database migrations

Run migrations before starting the app:

```sh
go run ./cmd/auction migrate
```

For Docker, rebuild the image if the migration files changed:

```sh
docker compose build migrate
docker compose run --rm migrate
```

Files named `NNN_name.up.sql` are included in the binary. Add a new file with a
new positive number for each change. Names can contain letters, digits, and
underscores. Files run in numeric order, so 2 comes before 10.

The runner locks the current schema, checks the history, and applies all new files
in one transaction. If any file fails, the whole new batch rolls back. Running the
command again does not repeat completed migrations.

`schema_migrations` stores each version and its SHA-256 checksum. The runner rejects
changed files that were already applied, unknown applied versions, duplicate
numbers, and gaps in the applied history. Windows line endings are normalized
before calculating checksums. Do not edit an applied file; add a new migration.

The first runner stored no checksum. When upgrading that database, the new runner
accepts migration 001 as the starting point and saves its checksum. This cannot
prove that the old SQL was never changed. Missing checksums for other versions are errors.

| Version | Change |
|---|---|
| 001 | Users, sessions, auctions, bids, and outbox |
| 002 | Outbox error details and an index for pending events per auction |
| 003 | Index for removing old published events |

SQL must work inside a transaction. `VACUUM` and `CREATE INDEX CONCURRENTLY` cannot
be used here. The current index migrations may delay writes while they run;
a large live database needs a separate plan.

The `.down.sql` files are for manual use on a disposable database, in reverse
order. The `migrate` command only runs `.up.sql` files.
