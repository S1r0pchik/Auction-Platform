package httpapi

import (
	"encoding/base64"
	"encoding/json"
	"net/http/httptest"
	"testing"
)

func TestListRejectsIncompleteCursor(t *testing.T) {
	handler := (&API{}).Router()
	for _, body := range []string{
		`{}`,
		`null`,
		`{"id":"00000000-0000-0000-0000-000000000001"}`,
		`{"at":"2026-10-25T12:00:00Z"}`,
		`{"at":"2026-10-25T12:00:00Z","id":"00000000-0000-0000-0000-000000000000"}`,
	} {
		t.Run(body, func(t *testing.T) {
			encoded := base64.RawURLEncoding.EncodeToString([]byte(body))
			r := httptest.NewRequest("GET", "/api/v1/auctions?cursor="+encoded, nil)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			var response errorResponse
			if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
				t.Fatalf("expected a JSON error, got status %d and body %q", w.Code, w.Body.String())
			}
			if w.Code != 400 || response.Code != "invalid_cursor" {
				t.Fatalf("got %d %s, want 400 invalid_cursor", w.Code, response.Code)
			}
		})
	}
}
