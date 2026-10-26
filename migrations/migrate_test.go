package migrations

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestCatalogNumericOrderAndChecksums(t *testing.T) {
	source := fstest.MapFS{"10_last.up.sql": {Data: []byte("SELECT 10;\r\n")}, "2_first.up.sql": {Data: []byte("SELECT 2;\n")}}
	list, err := catalog(source)
	if err != nil || list[0].version != 2 || list[1].version != 10 {
		t.Fatal(list, err)
	}
	source["10_last.up.sql"].Data = []byte("SELECT 10;\n")
	normalized, err := catalog(source)
	if err != nil || normalized[1].checksum != list[1].checksum {
		t.Fatal("line endings changed checksum", err)
	}
	source["02_duplicate.up.sql"] = &fstest.MapFile{Data: []byte("SELECT 1;")}
	if _, err = catalog(source); err == nil {
		t.Fatal("duplicate version accepted")
	}
}

func migrationDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	schema := "migration_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = 5
	db, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error(err)
		}
		admin.Close()
	})
	return db
}

func fixture() fstest.MapFS {
	return fstest.MapFS{
		"001_create.up.sql": {Data: []byte("CREATE TABLE sample (value integer); INSERT INTO sample VALUES(7);")},
		"002_extend.up.sql": {Data: []byte("ALTER TABLE sample ADD COLUMN note text NOT NULL DEFAULT 'kept';")},
	}
}

func TestApplyFreshConcurrentAndIdempotent(t *testing.T) {
	db := migrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	source := fixture()
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- apply(ctx, db, source) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM sample WHERE value=7 AND note='kept'`).Scan(&count); err != nil || count != 1 {
		t.Fatal(count, err)
	}
	if err := db.QueryRow(ctx, `SELECT count(*) FROM schema_migrations WHERE length(checksum)=64`).Scan(&count); err != nil || count != 2 {
		t.Fatal(count, err)
	}
}

func TestApplyUpgradesLegacyVersionOneWithoutDataLoss(t *testing.T) {
	db := migrationDB(t)
	ctx := context.Background()
	source := fixture()
	if _, err := db.Exec(ctx, `CREATE TABLE schema_migrations(version integer PRIMARY KEY); INSERT INTO schema_migrations VALUES(1);`+string(source["001_create.up.sql"].Data)); err != nil {
		t.Fatal(err)
	}
	if err := apply(ctx, db, source); err != nil {
		t.Fatal(err)
	}
	var value int
	var note string
	if err := db.QueryRow(ctx, `SELECT value,note FROM sample`).Scan(&value, &note); err != nil || value != 7 || note != "kept" {
		t.Fatal(value, note, err)
	}
	if err := apply(ctx, db, source); err != nil {
		t.Fatal("upgrade not repeatable", err)
	}
}

func TestApplyRollsBackFailedBatchAndCanRetry(t *testing.T) {
	db := migrationDB(t)
	ctx := context.Background()
	source := fixture()
	source["003_broken.up.sql"] = &fstest.MapFile{Data: []byte("ALTER TABLE table_that_does_not_exist ADD COLUMN x int;")}
	if err := apply(ctx, db, source); err == nil {
		t.Fatal("broken migration accepted")
	}
	var absent bool
	if err := db.QueryRow(ctx, `SELECT to_regclass('sample') IS NULL AND to_regclass('schema_migrations') IS NULL`).Scan(&absent); err != nil || !absent {
		t.Fatal("partial migration survived", err)
	}
	delete(source, "003_broken.up.sql")
	if err := apply(ctx, db, source); err != nil {
		t.Fatal(err)
	}
}

func TestApplyRejectsChangedUnknownAndGappedHistory(t *testing.T) {
	for _, scenario := range []string{"changed", "unknown", "gap"} {
		t.Run(scenario, func(t *testing.T) {
			db := migrationDB(t)
			ctx := context.Background()
			source := fixture()
			if err := apply(ctx, db, source); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "changed":
				source["001_create.up.sql"].Data = []byte("DROP TABLE sample;")
			case "unknown":
				if _, err := db.Exec(ctx, `INSERT INTO schema_migrations(version,checksum) VALUES(99,'unknown')`); err != nil {
					t.Fatal(err)
				}
			case "gap":
				if _, err := db.Exec(ctx, `DELETE FROM schema_migrations WHERE version=1`); err != nil {
					t.Fatal(err)
				}
			}
			if err := apply(ctx, db, source); err == nil {
				t.Fatal("invalid migration history accepted", scenario)
			}
			var count int
			if err := db.QueryRow(ctx, `SELECT count(*) FROM sample`).Scan(&count); err != nil || count != 1 {
				t.Fatal("data changed on rejected migration", err)
			}
		})
	}
}
