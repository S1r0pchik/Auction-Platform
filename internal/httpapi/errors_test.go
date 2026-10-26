package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5/middleware"

	"auction-platform/internal/auction"
	"auction-platform/internal/validation"
)

func TestDecodeExplainsInvalidFields(t *testing.T) {
	cases := []struct {
		name, body, field string
		status            int
	}{
		{"invalid start date", `{"starts_at":"tomorrow"}`, "starts_at", 400},
		{"numeric end date", `{"ends_at":123}`, "ends_at", 400},
		{"fractional price", `{"starting_price":1.5}`, "starting_price", 400},
		{"string increment", `{"min_increment":"100"}`, "min_increment", 400},
		{"numeric title", `{"title":12}`, "title", 400},
		{"unknown field", `{"unexpected":"secret-must-not-appear"}`, "unexpected", 400},
		{"malformed JSON", `{"title":`, "_body", 400},
		{"two objects", `{} {}`, "_body", 400},
		{"null object", `null`, "_body", 400},
		{"array", `[]`, "_body", 400},
		{"oversize", `{"title":"` + strings.Repeat("x", 17000) + `"}`, "", 413},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/", strings.NewReader(tc.body))
			rec := httptest.NewRecorder()
			var target auction.CreateInput
			if decode(rec, req, &target) {
				t.Fatal("invalid request accepted")
			}
			var result errorResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if rec.Code != tc.status || result.Message == "" || tc.field != "" && result.Fields[tc.field] == "" {
				t.Fatal(rec.Code, result)
			}
			if strings.Contains(rec.Body.String(), "secret-must-not-appear") {
				t.Fatal("raw request value exposed")
			}
		})
	}
}

func TestDomainErrorsHaveSafeDetailsAndRequestID(t *testing.T) {
	cases := []struct {
		name        string
		err         error
		status      int
		code, field string
		minimum     int64
	}{
		{"validation", validation.Field(auction.ErrInvalid, "ends_at", "must be in the future"), 400, "invalid_request", "ends_at", 0},
		{"minimum", &auction.BidTooLowError{Minimum: 1150000}, 409, "bid_too_low", "amount", 1150000},
		{"internal", errors.New("database driver private-detail"), 500, "internal_error", "", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest("POST", "/", nil)
			middleware.RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fail(w, r, tc.err) })).ServeHTTP(rec, req)
			var result errorResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if rec.Code != tc.status || result.Code != tc.code || result.RequestID == "" || tc.field != "" && result.Fields[tc.field] == "" {
				t.Fatal(result)
			}
			if tc.minimum > 0 && result.Details["minimum_amount"] != float64(tc.minimum) {
				t.Fatal(result)
			}
			if strings.Contains(rec.Body.String(), "private-detail") {
				t.Fatal("internal error exposed")
			}
		})
	}
}
