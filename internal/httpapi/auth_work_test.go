package httpapi

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestAuthWorkSharesCapacityAndReleasesIt(t *testing.T) {
	limit := limitAuthWork(make(chan struct{}, 1))
	entered := make(chan struct{})
	release := make(chan struct{})
	var unblock sync.Once
	defer unblock.Do(func() { close(release) })
	register := limit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(http.StatusBadRequest)
	}))
	login := limit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	done := make(chan struct{})
	go func() {
		defer close(done)
		register.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/auth/register", nil))
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("register handler did not start")
	}
	w := httptest.NewRecorder()
	login.ServeHTTP(w, httptest.NewRequest("POST", "/auth/login", nil))
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("busy: got %d, want 429", w.Code)
	}
	unblock.Do(func() { close(release) })
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("register handler did not finish")
	}
	w = httptest.NewRecorder()
	login.ServeHTTP(w, httptest.NewRequest("POST", "/auth/login", nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("after failed registration: got %d, want 401", w.Code)
	}
}
