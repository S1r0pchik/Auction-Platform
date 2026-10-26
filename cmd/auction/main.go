package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"auction-platform/internal/auction"
	"auction-platform/internal/auth"
	"auction-platform/internal/config"
	"auction-platform/internal/events"
	"auction-platform/internal/httpapi"
	"auction-platform/internal/monitor"
	"auction-platform/internal/outbox"
	"auction-platform/internal/realtime"
	"auction-platform/migrations"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if err := run(); err != nil {
		slog.Error("process stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) != 2 {
		return fmt.Errorf("usage: auction [api|lifecycle|publisher|migrate]")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	db, err := config.OpenDB(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	store := &auction.Store{DB: db}
	switch os.Args[1] {
	case "migrate":
		return migrations.Up(ctx, db)
	case "lifecycle":
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				for i := 0; i < 100; i++ {
					op, cancel := context.WithTimeout(ctx, 5*time.Second)
					worked, e := store.Tick(op)
					cancel()
					if e != nil {
						slog.Error("lifecycle tick", "error", e)
						break
					}
					if !worked {
						break
					}
				}
			}
		}
	case "publisher":
		client, err := events.Producer(cfg.Brokers)
		if err != nil {
			return err
		}
		defer client.Close()
		p := &outbox.Publisher{DB: db, Sender: outbox.KafkaSender{Client: client, Topic: cfg.Topic}}
		cleanupCtx, stopCleanup := context.WithCancel(ctx)
		cleanupDone := make(chan struct{})
		go func() {
			defer close(cleanupDone)
			p.RunCleanup(cleanupCtx, cfg.OutboxRetention)
		}()
		p.Run(ctx, cfg.OutboxWorkers)
		stopCleanup()
		<-cleanupDone
		return nil
	case "api":
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		hub := realtime.NewHubWithLimits(cfg.WSMaxConnections, cfg.WSMaxPerAuction)
		state := monitor.NewConsumer()
		api := &httpapi.API{Auctions: store, Auth: &auth.Store{DB: db}, Hub: hub, Consumer: state}
		server := &http.Server{
			Addr:              cfg.HTTPAddr,
			Handler:           api.Router(),
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			IdleTimeout:       60 * time.Second,
			MaxHeaderBytes:    16 << 10,
			BaseContext:       func(net.Listener) context.Context { return ctx },
		}
		errs := make(chan error, 1)
		consumerDone := make(chan struct{})
		cleanupDone := make(chan struct{})
		go func() {
			defer close(cleanupDone)
			api.Auth.RunSessionCleanup(ctx)
		}()
		go func() {
			defer close(consumerDone)
			events.Supervise(ctx, func(sessionCtx context.Context) error {
				client, e := events.ConsumerObserved(cfg.Brokers, cfg.Topic, cfg.Group, state)
				if e != nil {
					return e
				}
				defer events.CloseConsumer(client)
				return events.RunConsumerObserved(sessionCtx, client, hub, state)
			}, state)
		}()
		go func() {
			slog.Info("HTTP listening", "address", cfg.HTTPAddr)
			errs <- server.ListenAndServe()
		}()
		select {
		case <-ctx.Done():
		case err = <-errs:
		}
		cancel()
		shutdown, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelShutdown()
		closeErr := server.Shutdown(shutdown)
		select {
		case <-consumerDone:
		case <-shutdown.Done():
			closeErr = errors.Join(closeErr, fmt.Errorf("consumer shutdown: %w", shutdown.Err()))
		}
		select {
		case <-cleanupDone:
		case <-shutdown.Done():
			closeErr = errors.Join(closeErr, fmt.Errorf("session cleanup shutdown: %w", shutdown.Err()))
		}
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		if err != nil {
			return err
		}
		return closeErr
	default:
		return fmt.Errorf("unknown command %q", os.Args[1])
	}
}
