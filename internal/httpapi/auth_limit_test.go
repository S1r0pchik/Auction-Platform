package httpapi

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"auction-platform/internal/auth"
)

func TestAuthLimitsAndRecovery(t *testing.T) {
	l := newAuthLimiter()
	now := time.Now()
	l.now = func() time.Time { return now }
	var accepted atomic.Int32
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			if l.allow("account:user@example.com", 10) == 0 {
				accepted.Add(1)
			}
		})
	}
	wg.Wait()
	if accepted.Load() != 10 {
		t.Fatal(accepted.Load())
	}
	if l.allow("account:other@example.com", 10) != 0 {
		t.Fatal("unrelated account blocked")
	}
	now = now.Add(time.Minute)
	if l.allow("account:user@example.com", 10) != 0 {
		t.Fatal("window did not recover")
	}
	for i := 0; i < 11000; i++ {
		l.allow(fmt.Sprint(i), 1)
	}
	if len(l.windows) > 10000 {
		t.Fatal("unbounded table")
	}
	now = now.Add(time.Minute)
	if l.allow("new", 1) != 0 {
		t.Fatal("table did not recover")
	}
}

func TestAuthRoutesEnforceSharedAccountAndIPLimits(t *testing.T) {
	for _, mode := range []string{"account", "ip"} {
		t.Run(mode, func(t *testing.T) {
			api := &API{Auth: &auth.Store{}}
			h := api.Router()
			limit := 10
			if mode == "ip" {
				limit = 30
			}
			for i := 0; i <= limit; i++ {
				email := "user@example.com"
				if mode == "ip" {
					email = fmt.Sprintf("user%d@example.com", i)
				}
				path := "/api/v1/auth/login"
				if i%2 == 0 {
					path = "/api/v1/auth/register"
				}
				req := httptest.NewRequest("POST", path, strings.NewReader(fmt.Sprintf(`{"email":%q,"password":"short"}`, email)))
				req.RemoteAddr = "192.0.2.1:1234"
				if mode == "account" {
					req.RemoteAddr = fmt.Sprintf("192.0.2.%d:1234", i+1)
				}
				w := httptest.NewRecorder()
				h.ServeHTTP(w, req)
				if i == limit {
					if w.Code != 429 || w.Header().Get("Retry-After") == "" {
						t.Fatal(i, w.Code)
					}
				} else if w.Code != 400 && w.Code != 401 {
					t.Fatal(i, w.Code)
				}
			}
		})
	}
}

func TestAuthIPIgnoresForwardedAndAccountNormalization(t *testing.T) {
	l := newAuthLimiter()
	h := l.ip(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	for i := 0; i < 31; i++ {
		r := httptest.NewRequest("POST", "/", nil)
		r.RemoteAddr = "192.0.2.1:1234"
		r.Header.Set("X-Forwarded-For", fmt.Sprintf("192.0.2.%d", i+2))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if i < 30 && w.Code != 204 || i == 30 && (w.Code != 429 || w.Header().Get("Retry-After") == "") {
			t.Fatal(i, w.Code)
		}
	}
	for i := 0; i < 11; i++ {
		email := "user@example.com"
		if i%2 == 0 {
			email = " USER@EXAMPLE.COM "
		}
		w := httptest.NewRecorder()
		ok := l.account(w, httptest.NewRequest("POST", "/", nil), email)
		if ok != (i < 10) {
			t.Fatal("normalization bypass", i)
		}
	}
}
