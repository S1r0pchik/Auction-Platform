package validation

type Error struct {
	Cause  error
	Fields map[string]string
}

func (e *Error) Error() string { return e.Cause.Error() }

func (e *Error) Unwrap() error { return e.Cause }

func Field(cause error, field, message string) error {
	return &Error{Cause: cause, Fields: map[string]string{field: message}}
}
