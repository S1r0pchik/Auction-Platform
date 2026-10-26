package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"auction-platform/internal/auction"
	"auction-platform/internal/auth"
	"auction-platform/internal/httpapi"
)

func TestLogoutRevokesOnlyCurrentSession(t *testing.T) {
	db := database(t)
	s := &auth.Store{DB: db}
	ctx := context.Background()
	c := auth.Credentials{Email: "logout@example.com", Password: "strong-password-123"}
	if _, err := s.Register(ctx, c); err != nil {
		t.Fatal(err)
	}
	first, err := s.Login(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.Login(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	a := &httpapi.API{Auth: s, Auctions: &auction.Store{DB: db}}
	router := a.Router()
	call := func(path, header string, body []byte, status int) {
		t.Helper()
		r := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
		r.Header.Set("Authorization", header)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		if w.Code != status {
			t.Fatalf("%s: got %d want %d: %s", path, w.Code, status, w.Body.String())
		}
		if status == 204 && w.Body.Len() != 0 {
			t.Fatal("logout must have empty body")
		}
	}
	call("/api/v1/auth/logout", "", nil, 401)
	call("/api/v1/auth/logout", "Bearer invalid", nil, 401)
	call("/api/v1/auth/logout", "Bearer "+first.Token, nil, 204)
	call("/api/v1/auth/logout", "Bearer "+first.Token, nil, 204)
	if _, err := s.Authenticate(ctx, first.Token); !errors.Is(err, auth.ErrCredentials) {
		t.Fatal("revoked session accepted", err)
	}
	input, _ := json.Marshal(auction.CreateInput{Title: "After logout", StartingPrice: 100, MinIncrement: 10, StartsAt: time.Now().Add(-time.Minute), EndsAt: time.Now().Add(time.Hour)})
	call("/api/v1/auctions", "Bearer "+first.Token, input, 401)
	call("/api/v1/auctions", "Bearer "+second.Token, input, 201)
	if _, err := db.Exec(ctx, `UPDATE sessions SET expires_at=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Authenticate(ctx, second.Token); !errors.Is(err, auth.ErrCredentials) {
		t.Fatal("expired session accepted", err)
	}
	call("/api/v1/auth/logout", "Bearer "+second.Token, nil, 204)
	call("/api/v1/auth/logout", "Bearer "+strings.Repeat("x", 43), nil, 204)
}

func TestSessionCleanupBoundedAndConcurrent(t *testing.T) {
	db := database(t)
	s := &auth.Store{DB: db}
	ctx := context.Background()
	id := user(t, db)
	_, err := db.Exec(ctx, `INSERT INTO sessions(token_hash,user_id,expires_at)
		SELECT decode(md5(i::text),'hex'),$1,clock_timestamp()-interval '1 hour' FROM generate_series(1,2501) i`, id)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(ctx, `INSERT INTO sessions VALUES(decode(md5('active'),'hex'),$1,clock_timestamp()+interval '1 hour')`, id)
	if err != nil {
		t.Fatal(err)
	}
	locked, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer locked.Rollback(ctx)
	if _, err = locked.Exec(ctx, `SELECT 1 FROM sessions WHERE token_hash=decode(md5('1'),'hex') FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if n, err := s.CleanupExpired(bounded); err != nil || n != 1000 {
		t.Fatal("batch/lock skip", n, err)
	}
	var wg sync.WaitGroup
	for range 3 {
		wg.Go(func() {
			if n, err := s.CleanupExpired(bounded); err != nil || n > 1000 {
				t.Error("concurrent cleanup", n, err)
			}
		})
	}
	wg.Wait()
	var remaining int
	if err = db.QueryRow(ctx, `SELECT count(*) FROM sessions`).Scan(&remaining); err != nil || remaining != 2 {
		t.Fatal("active and locked rows must remain", remaining, err)
	}
	if err = locked.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if n, err := s.CleanupExpired(ctx); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	if n, err := s.CleanupExpired(ctx); err != nil || n != 0 {
		t.Fatal(n, err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); s.RunSessionCleanup(workerCtx) }()
	stop()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not stop")
	}
}
