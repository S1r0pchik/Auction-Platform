package config

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	DatabaseURL, HTTPAddr, Topic, Group string
	Brokers                             []string
	WSMaxConnections, WSMaxPerAuction   int
	OutboxWorkers                       int
	OutboxRetention                     time.Duration
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func Load() (Config, error) {
	c := Config{
		DatabaseURL: os.Getenv("DATABASE_URL"),
		HTTPAddr:    env("HTTP_ADDR", ":8080"),
		Topic:       env("KAFKA_TOPIC", "auction.events"),
		Group:       env("KAFKA_GROUP", "auction-api"),
		Brokers:     strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ","),
	}
	if c.DatabaseURL == "" {
		return c, fmt.Errorf("DATABASE_URL is required")
	}
	var err error
	hours, err := strconv.Atoi(env("OUTBOX_RETENTION_HOURS", "168"))
	if err != nil || hours < 1 || hours > 8760 {
		return c, fmt.Errorf("OUTBOX_RETENTION_HOURS must be between 1 and 8760")
	}
	c.OutboxRetention = time.Duration(hours) * time.Hour
	if c.OutboxWorkers, err = strconv.Atoi(env("OUTBOX_WORKERS", "4")); err != nil || c.OutboxWorkers < 1 || c.OutboxWorkers > 16 {
		return c, fmt.Errorf("OUTBOX_WORKERS must be between 1 and 16")
	}
	if c.WSMaxConnections, err = strconv.Atoi(env("WS_MAX_CONNECTIONS", "1000")); err != nil || c.WSMaxConnections < 1 {
		return c, fmt.Errorf("WS_MAX_CONNECTIONS must be positive")
	}
	if c.WSMaxPerAuction, err = strconv.Atoi(env("WS_MAX_PER_AUCTION", "100")); err != nil || c.WSMaxPerAuction < 1 {
		return c, fmt.Errorf("WS_MAX_PER_AUCTION must be positive")
	}
	return c, nil
}

func OpenDB(ctx context.Context, url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 20
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	db, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err = db.Ping(pingCtx); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}
