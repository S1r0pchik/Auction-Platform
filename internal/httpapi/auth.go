package httpapi

import (
	"net/http"
	"strings"

	"github.com/google/uuid"

	"auction-platform/internal/auth"
)

func limitAuthWork(slots chan struct{}) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
			default:
				writeError(w, r, 429, "too_many_requests")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func (a *API) register(w http.ResponseWriter, r *http.Request) {
	var credentials auth.Credentials
	if !decode(w, r, &credentials) || !a.authLimits.account(w, r, credentials.Email) {
		return
	}
	r, cancel := timeout(r)
	defer cancel()
	user, err := a.Auth.Register(r.Context(), credentials)
	if err != nil {
		fail(w, r, err)
		return
	}
	respond(w, 201, user)
}

func (a *API) login(w http.ResponseWriter, r *http.Request) {
	var credentials auth.Credentials
	if !decode(w, r, &credentials) || !a.authLimits.account(w, r, credentials.Email) {
		return
	}
	r, cancel := timeout(r)
	defer cancel()
	session, err := a.Auth.Login(r.Context(), credentials)
	if err != nil {
		fail(w, r, err)
		return
	}
	respond(w, 200, session)
}

func (a *API) user(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	h := r.Header.Get("Authorization")
	if !strings.HasPrefix(h, "Bearer ") {
		writeError(w, r, 401, "invalid_credentials")
		return uuid.Nil, false
	}
	id, err := a.Auth.Authenticate(r.Context(), strings.TrimPrefix(h, "Bearer "))
	if err != nil {
		fail(w, r, err)
		return uuid.Nil, false
	}
	return id, true
}

func (a *API) logout(w http.ResponseWriter, r *http.Request) {
	r, cancel := timeout(r)
	defer cancel()
	h := r.Header.Get("Authorization")
	if !strings.HasPrefix(h, "Bearer ") {
		writeError(w, r, 401, "invalid_credentials")
		return
	}
	if err := a.Auth.Logout(r.Context(), strings.TrimPrefix(h, "Bearer ")); err != nil {
		fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
