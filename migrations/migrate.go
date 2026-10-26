package migrations

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed *.up.sql
var files embed.FS

type migration struct {
	version             int
	name, sql, checksum string
}

var filename = regexp.MustCompile(`^([0-9]+)_[A-Za-z0-9_]+\.up\.sql$`)

func catalog(source fs.FS) ([]migration, error) {
	names, err := fs.Glob(source, "*.up.sql")
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("no up migrations found")
	}
	result := make([]migration, 0, len(names))
	seen := map[int]bool{}
	for _, name := range names {
		match := filename.FindStringSubmatch(name)
		if match == nil {
			return nil, fmt.Errorf("invalid migration filename %q", name)
		}
		version, err := strconv.ParseInt(match[1], 10, 32)
		if err != nil || version < 1 {
			return nil, fmt.Errorf("invalid migration version %q", name)
		}
		if seen[int(version)] {
			return nil, fmt.Errorf("duplicate migration version %d", version)
		}
		seen[int(version)] = true
		data, err := fs.ReadFile(source, name)
		if err != nil {
			return nil, err
		}
		sql := strings.ReplaceAll(string(data), "\r\n", "\n")
		sum := sha256.Sum256([]byte(sql))
		result = append(result, migration{version: int(version), name: name, sql: sql, checksum: hex.EncodeToString(sum[:])})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].version < result[j].version })
	return result, nil
}

func Up(ctx context.Context, db *pgxpool.Pool) error {
	return apply(ctx, db, files)
}

func apply(ctx context.Context, db *pgxpool.Pool, source fs.FS) error {
	list, err := catalog(source)
	if err != nil {
		return err
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended(current_schema(),715201))`); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version integer PRIMARY KEY,checksum text)`); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS checksum text`); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `SELECT version,checksum FROM schema_migrations ORDER BY version`)
	if err != nil {
		return err
	}
	applied := map[int]*string{}
	for rows.Next() {
		var version int
		var sum *string
		if err = rows.Scan(&version, &sum); err != nil {
			rows.Close()
			return err
		}
		applied[version] = sum
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return err
	}
	known := map[int]bool{}
	pending := false
	for _, m := range list {
		known[m.version] = true
		sum, exists := applied[m.version]
		if !exists {
			pending = true
			continue
		}
		if pending {
			return fmt.Errorf("migration history has a gap before version %d", m.version)
		}
		// The original migration runner did not store a checksum for version 1.
		if sum == nil {
			if m.version != 1 {
				return fmt.Errorf("missing checksum for migration %d", m.version)
			}
			if _, err = tx.Exec(ctx, `UPDATE schema_migrations SET checksum=$2 WHERE version=$1`, m.version, m.checksum); err != nil {
				return err
			}
		} else if *sum != m.checksum {
			return fmt.Errorf("checksum mismatch for applied migration %s", m.name)
		}
	}
	for version := range applied {
		if !known[version] {
			return fmt.Errorf("database contains unknown migration version %d", version)
		}
	}
	for _, m := range list {
		if _, exists := applied[m.version]; exists {
			continue
		}
		if _, err = tx.Exec(ctx, m.sql); err != nil {
			return fmt.Errorf("apply migration %s: %w", m.name, err)
		}
		if _, err = tx.Exec(ctx, `INSERT INTO schema_migrations(version,checksum) VALUES($1,$2)`, m.version, m.checksum); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
