package httpapi

import (
	"context"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"auction-platform/internal/auction"
	"auction-platform/internal/auth"
	"auction-platform/internal/monitor"
	"auction-platform/internal/realtime"
)

type API struct {
	Auctions   *auction.Store
	Auth       *auth.Store
	Hub        *realtime.Hub
	Consumer   *monitor.Consumer
	BidGate    *BidGate
	authLimits *AuthLimiter
}

func (a *API) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID, requestLog, middleware.Recoverer)
	r.NotFound(func(w http.ResponseWriter, r *http.Request) { writeError(w, r, 404, "route_not_found") })
	r.MethodNotAllowed(func(w http.ResponseWriter, r *http.Request) { writeError(w, r, 405, "method_not_allowed") })
	if a.BidGate == nil {
		a.BidGate = NewBidGate(8, 2)
	}
	r.Get("/health/live", func(w http.ResponseWriter, r *http.Request) { respond(w, 200, map[string]string{"status": "alive"}) })
	r.Get("/health/ready", a.ready)
	r.Get("/status", a.status)
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := a.Auctions.DB.Ping(ctx); err != nil {
			writeError(w, r, 503, "database_unavailable")
			return
		}
		respond(w, 200, map[string]string{"status": "ok"})
	})
	if a.authLimits == nil {
		a.authLimits = newAuthLimiter()
	}
	passwordWork := limitAuthWork(make(chan struct{}, 4))
	r.Route("/api/v1", func(r chi.Router) {
		r.With(a.authLimits.ip, passwordWork).Post("/auth/register", a.register)
		r.With(a.authLimits.ip, passwordWork).Post("/auth/login", a.login)
		r.Post("/auth/logout", a.logout)
		r.Post("/auctions", a.create)
		r.Get("/auctions", a.list)
		r.Get("/auctions/{id}", a.get)
		r.With(a.BidGate.middleware).Post("/auctions/{id}/bids", a.bid)
		r.Get("/auctions/{id}/bids", a.bids)
		r.Get("/auctions/{id}/ws", a.ws)
	})
	return r
}
