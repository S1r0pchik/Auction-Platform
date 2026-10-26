package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
)

func requestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrapped := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		defer func() {
			slog.Info("HTTP request", "method", r.Method, "path", r.URL.Path, "status", wrapped.Status(), "duration", time.Since(start), "request_id", middleware.GetReqID(r.Context()))
		}()
		next.ServeHTTP(wrapped, r)
	})
}

func respond(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func pathID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeProblem(w, r, 400, "invalid_id", map[string]string{"id": "must be a UUID"}, nil)
		return uuid.Nil, false
	}
	return id, true
}

func timeout(r *http.Request) (*http.Request, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	return r.WithContext(ctx), cancel
}
