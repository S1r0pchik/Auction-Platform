package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgconn"

	"auction-platform/internal/auction"
	"auction-platform/internal/auth"
	"auction-platform/internal/validation"
)

type errorResponse struct {
	Code      string            `json:"code"`
	Message   string            `json:"message"`
	RequestID string            `json:"request_id"`
	Fields    map[string]string `json:"fields,omitempty"`
	Details   map[string]any    `json:"details,omitempty"`
}

var messages = map[string]string{
	"auth_rate_limited":           "Too many authentication attempts. Try again after Retry-After seconds.",
	"invalid_json":                "Request body must be one valid JSON object.",
	"invalid_request":             "Some request fields are invalid.",
	"invalid_id":                  "The auction ID must be a UUID.",
	"invalid_limit":               "The page size must be between 1 and 100.",
	"invalid_cursor":              "The pagination cursor is invalid.",
	"invalid_credentials_format":  "Email or password does not meet registration requirements.",
	"invalid_credentials":         "Credentials are missing, invalid, or expired.",
	"email_already_registered":    "An account with this email already exists.",
	"owner_cannot_bid":            "The auction owner cannot bid on their own lot.",
	"not_found":                   "The requested auction does not exist.",
	"route_not_found":             "The requested API route does not exist.",
	"method_not_allowed":          "This HTTP method is not supported for this route.",
	"auction_unavailable":         "The auction has not started, has ended, or is unavailable for bidding.",
	"bid_too_low":                 "The amount is below the minimum acceptable bid.",
	"idempotency_conflict":        "This Idempotency-Key was already used with a different amount.",
	"too_many_requests":           "Too many authentication requests are in progress. Try again later.",
	"bid_capacity_exceeded":       "Bid processing capacity is full. Retry with the same key after a delay.",
	"websocket_capacity_exceeded": "The WebSocket connection limit has been reached.",
	"request_unavailable":         "The request could not complete in time. Retry bids with the same key.",
	"retry_request":               "The database operation could not acquire a lock or complete concurrently. Retry bids with the same key.",
	"database_unavailable":        "The database is temporarily unavailable.",
	"payload_too_large":           "The request body must not exceed 16384 bytes.",
	"request_timeout":             "The request body could not be read in time.",
	"internal_error":              "An unexpected server error occurred.",
}

func writeProblem(w http.ResponseWriter, r *http.Request, status int, code string, fields map[string]string, details map[string]any) {
	message := messages[code]
	if message == "" {
		message = "The request could not be completed."
	}
	respond(w, status, errorResponse{Code: code, Message: message, RequestID: middleware.GetReqID(r.Context()), Fields: fields, Details: details})
}

func writeError(w http.ResponseWriter, r *http.Request, status int, code string) {
	writeProblem(w, r, status, code, nil, nil)
}

func fail(w http.ResponseWriter, r *http.Request, err error) {
	status, code := 500, "internal_error"
	switch {
	case errors.Is(err, auction.ErrInvalid):
		status, code = 400, auction.ErrInvalid.Error()
	case errors.Is(err, auth.ErrInvalid):
		status, code = 400, auth.ErrInvalid.Error()
	case errors.Is(err, auth.ErrCredentials):
		status, code = 401, "invalid_credentials"
	case errors.Is(err, auth.ErrEmail):
		status, code = 409, auth.ErrEmail.Error()
	case errors.Is(err, auction.ErrNotFound):
		status, code = 404, auction.ErrNotFound.Error()
	case errors.Is(err, auction.ErrForbidden):
		status, code = 403, auction.ErrForbidden.Error()
	case errors.Is(err, auction.ErrClosed):
		status, code = 409, auction.ErrClosed.Error()
	case errors.Is(err, auction.ErrLowBid):
		status, code = 409, auction.ErrLowBid.Error()
	case errors.Is(err, auction.ErrIdempotency):
		status, code = 409, auction.ErrIdempotency.Error()
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		status, code = 503, "request_unavailable"
	default:
		var pe *pgconn.PgError
		if errors.As(err, &pe) && (pe.Code == "40P01" || pe.Code == "40001" || pe.Code == "55P03") {
			status, code = 503, "retry_request"
		}
	}
	var fields map[string]string
	var details map[string]any
	var invalid *validation.Error
	if status == 400 && errors.As(err, &invalid) {
		fields = invalid.Fields
	}
	var low *auction.BidTooLowError
	if status == 409 && errors.As(err, &low) {
		details = map[string]any{"minimum_amount": low.Minimum}
		fields = map[string]string{"amount": "must be at least minimum_amount (in kopecks)"}
	}
	if status >= 500 {
		slog.Error("request failed", "request_id", middleware.GetReqID(r.Context()), "error", err)
	}
	writeProblem(w, r, status, code, fields, details)
}
