package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"auction-platform/internal/auction"
	"auction-platform/internal/events"
	"auction-platform/internal/outbox"
	"auction-platform/internal/realtime"
	"auction-platform/internal/testsupport"
)

// The helper uses the real Publisher transaction and KafkaSender. It never
// returns from Send after the ACK, leaving UPDATE/COMMIT strictly after the kill.
func TestPublisherCrashHelper(t *testing.T) {
	if os.Getenv("AUCTION_CRASH_CHILD") != "1" {
		t.Skip("subprocess only")
	}
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(os.Getenv("AUCTION_CRASH_DB"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = os.Getenv("AUCTION_CRASH_SCHEMA")
	db, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	client, err := events.Producer(strings.Split(os.Getenv("KAFKA_TEST_BROKERS"), ","))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	sender := outbox.KafkaSender{Client: client, Topic: os.Getenv("AUCTION_CRASH_TOPIC")}
	p := &outbox.Publisher{DB: db, Sender: senderFunc(func(ctx context.Context, k, v []byte) error {
		if err := sender.Send(ctx, k, v); err != nil {
			return err
		}
		fmt.Println("KAFKA_ACK_BEFORE_COMMIT")
		select {}
	})}
	if _, err := p.PublishOne(ctx); err != nil {
		t.Fatal(err)
	}
	t.Fatal("helper unexpectedly returned")
}

func TestPublisherKilledAfterKafkaACK(t *testing.T) {
	raw := os.Getenv("KAFKA_TEST_BROKERS")
	if raw == "" {
		t.Skip("KAFKA_TEST_BROKERS not set")
	}
	db := database(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store := &auction.Store{DB: db}
	a := lot(t, store)
	if _, _, err := store.PlaceBid(ctx, a.ID, user(t, db), 1000, "crash"); err != nil {
		t.Fatal(err)
	}
	brokers := strings.Split(raw, ",")
	topic := testsupport.KafkaTopic(t, brokers)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestPublisherCrashHelper$", "-test.v")
	cmd.Env = append(os.Environ(), "AUCTION_CRASH_CHILD=1", "AUCTION_CRASH_DB="+db.Config().ConnString(), "AUCTION_CRASH_SCHEMA="+db.Config().ConnConfig.RuntimeParams["search_path"], "AUCTION_CRASH_TOPIC="+topic)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill() }()
	ack := make(chan bool, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if scanner.Text() == "KAFKA_ACK_BEFORE_COMMIT" {
				ack <- true
				return
			}
		}
		ack <- false
	}()
	select {
	case ok := <-ack:
		if !ok {
			t.Fatal("child exited before Kafka ACK")
		}
	case <-ctx.Done():
		t.Fatal("ACK timeout")
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("expected forced process death")
	}
	var pending int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL AND attempts=0`).Scan(&pending); err != nil || pending != 2 {
		t.Fatal("uncommitted transaction survived", pending, err)
	}
	producer, err := events.Producer(brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	p := &outbox.Publisher{DB: db, Sender: outbox.KafkaSender{Client: producer, Topic: topic}}
	for range 2 {
		if worked, err := p.PublishOne(ctx); err != nil || !worked {
			t.Fatal(worked, err)
		}
	}
	consumer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().AtStart()}}))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	hub := realtime.NewHub()
	sub, unsubscribe, err := hub.Subscribe(a.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer unsubscribe()
	var delivered []auction.Event
	for len(delivered) < 3 {
		fetched := consumer.PollRecords(ctx, 10)
		if len(fetched.Errors()) > 0 {
			t.Fatal(fetched.Errors())
		}
		for _, r := range fetched.Records() {
			var e auction.Event
			if err := json.Unmarshal(r.Value, &e); err != nil {
				t.Fatal(err)
			}
			delivered = append(delivered, e)
			if err := events.Handle(hub, r.Key, r.Value); err != nil {
				t.Fatal(err)
			}
		}
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
	}
	if len(delivered) != 3 || delivered[0].ID != delivered[1].ID || delivered[0].Version != 1 || delivered[2].Version != 2 || len(sub.Events) != 2 {
		t.Fatal("loss/order/dedup failure", delivered, len(sub.Events))
	}
	if err := db.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`).Scan(&pending); err != nil || pending != 0 {
		t.Fatal(pending, err)
	}
	t.Log("forced kill after real Kafka ACK: versions [1 1 2], 2 hub notifications, no pending events")
}
