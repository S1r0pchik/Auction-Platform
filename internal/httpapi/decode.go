package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 16<<10))
	if err != nil {
		var large *http.MaxBytesError
		var timeout net.Error
		if errors.As(err, &large) {
			writeError(w, r, 413, "payload_too_large")
		} else if errors.As(err, &timeout) && timeout.Timeout() {
			writeError(w, r, 408, "request_timeout")
		} else {
			writeProblem(w, r, 400, "invalid_json", map[string]string{"_body": "could not read the request body"}, nil)
		}
		return false
	}
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		writeProblem(w, r, 400, "invalid_json", map[string]string{"_body": "must be a JSON object"}, nil)
		return false
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if err = d.Decode(v); err != nil {
		writeProblem(w, r, 400, "invalid_json", jsonFields(body, err), nil)
		return false
	}
	if err = d.Decode(new(any)); err != io.EOF {
		writeProblem(w, r, 400, "invalid_json", map[string]string{"_body": "must contain exactly one JSON object"}, nil)
		return false
	}
	return true
}

func jsonFields(body []byte, err error) map[string]string {
	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) {
		field := typeErr.Field
		if field == "" {
			field = "_body"
		}
		expected := "the expected JSON type"
		switch typeErr.Type.String() {
		case "int64", "int":
			expected = "a 64-bit integer"
		case "string":
			expected = "a string"
		}
		return map[string]string{field: "must be " + expected}
	}
	var dateErr *time.ParseError
	if errors.As(err, &dateErr) || strings.HasPrefix(err.Error(), "Time.UnmarshalJSON:") {
		var raw map[string]json.RawMessage
		_ = json.Unmarshal(body, &raw)
		fields := map[string]string{}
		for _, name := range []string{"starts_at", "ends_at"} {
			if value, ok := raw[name]; ok {
				var date time.Time
				if json.Unmarshal(value, &date) != nil {
					fields[name] = "must be an RFC 3339 timestamp with a timezone"
				}
			}
		}
		if len(fields) > 0 {
			return fields
		}
	}
	if quoted, ok := strings.CutPrefix(err.Error(), "json: unknown field "); ok {
		if field, e := strconv.Unquote(quoted); e == nil {
			return map[string]string{field: "unknown field"}
		}
	}
	var syntax *json.SyntaxError
	if errors.As(err, &syntax) {
		return map[string]string{"_body": fmt.Sprintf("malformed JSON at byte %d", syntax.Offset)}
	}
	return map[string]string{"_body": "malformed or incomplete JSON"}
}
