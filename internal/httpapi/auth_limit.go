package httpapi

import (
	"crypto/sha256"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type authWindow struct {
	until time.Time
	count int
}

type AuthLimiter struct {
	mu        sync.Mutex
	windows   map[[32]byte]authWindow
	now       func() time.Time
	nextSweep time.Time
}

func newAuthLimiter() *AuthLimiter {
	return &AuthLimiter{windows: make(map[[32]byte]authWindow), now: time.Now}
}

func (l *AuthLimiter) allow(key string, limit int) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	hash := sha256.Sum256([]byte(key))
	w := l.windows[hash]
	if !now.Before(w.until) {
		if len(l.windows) >= 10000 {
			if !now.Before(l.nextSweep) {
				for k, v := range l.windows {
					if !now.Before(v.until) {
						delete(l.windows, k)
					}
				}
				l.nextSweep = now.Add(time.Minute)
			}
			if len(l.windows) >= 10000 {
				return 60
			}
		}
		w = authWindow{until: now.Add(time.Minute)}
	}
	if w.count >= limit {
		return max(1, int(w.until.Sub(now).Seconds()+1))
	}
	w.count++
	l.windows[hash] = w
	return 0
}

func authLimited(w http.ResponseWriter, r *http.Request, seconds int) bool {
	if seconds == 0 {
		return false
	}
	w.Header().Set("Retry-After", strconv.Itoa(seconds))
	writeError(w, r, 429, "auth_rate_limited")
	return true
}

func (l *AuthLimiter) ip(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			host = r.RemoteAddr
		}
		if ip := net.ParseIP(host); ip != nil {
			host = ip.String()
		}
		if authLimited(w, r, l.allow("ip:"+host, 30)) {
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (l *AuthLimiter) account(w http.ResponseWriter, r *http.Request, email string) bool {
	return !authLimited(w, r, l.allow("account:"+strings.ToLower(strings.TrimSpace(email)), 10))
}
