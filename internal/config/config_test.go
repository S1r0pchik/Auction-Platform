package config

import (
	"testing"
	"time"
)

func TestOutboxRetention(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://unused")
	t.Setenv("OUTBOX_RETENTION_HOURS", "")
	c, err := Load()
	if err != nil || c.OutboxRetention != 168*time.Hour {
		t.Fatal(c, err)
	}
	for _, v := range []string{"0", "-1", "8761", "abc"} {
		t.Setenv("OUTBOX_RETENTION_HOURS", v)
		if _, err := Load(); err == nil {
			t.Fatal(v)
		}
	}
}

func TestWebSocketLimits(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://unused")
	t.Setenv("WS_MAX_CONNECTIONS", "")
	t.Setenv("WS_MAX_PER_AUCTION", "")
	cfg, err := Load()
	if err != nil || cfg.WSMaxConnections != 1000 || cfg.WSMaxPerAuction != 100 {
		t.Fatal(cfg, err)
	}
	for _, key := range []string{"WS_MAX_CONNECTIONS", "WS_MAX_PER_AUCTION"} {
		t.Run(key, func(t *testing.T) {
			for _, value := range []string{"0", "-1", "abc"} {
				t.Setenv(key, value)
				if _, err := Load(); err == nil {
					t.Fatal("invalid limit accepted", key, value)
				}
			}
		})
	}
}

func TestOutboxWorkerLimits(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://unused")
	t.Setenv("OUTBOX_WORKERS", "")
	cfg, err := Load()
	if err != nil || cfg.OutboxWorkers != 4 {
		t.Fatal(cfg, err)
	}
	for _, value := range []string{"0", "-1", "17", "abc"} {
		t.Setenv("OUTBOX_WORKERS", value)
		if _, err = Load(); err == nil {
			t.Fatal("invalid worker limit accepted", value)
		}
	}
}
