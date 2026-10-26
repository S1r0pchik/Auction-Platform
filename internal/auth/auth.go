package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net/mail"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"

	"auction-platform/internal/validation"
)

var (
	ErrInvalid     = errors.New("invalid_credentials_format")
	ErrCredentials = errors.New("invalid_credentials")
	ErrEmail       = errors.New("email_already_registered")
)

type Store struct{ DB *pgxpool.Pool }

type Credentials struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type User struct {
	ID    uuid.UUID `json:"id"`
	Email string    `json:"email"`
}

type Session struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
}

func normalize(c Credentials) (Credentials, error) {
	c.Email = strings.ToLower(strings.TrimSpace(c.Email))
	m, e := mail.ParseAddress(c.Email)
	fields := map[string]string{}
	if e != nil || m.Address != c.Email || len(c.Email) > 254 {
		fields["email"] = "must be a valid email address of at most 254 bytes"
	}
	if len(c.Password) < 12 || len(c.Password) > 72 {
		fields["password"] = "must contain 12 to 72 bytes"
	}
	if len(fields) > 0 {
		return c, &validation.Error{Cause: ErrInvalid, Fields: fields}
	}
	return c, nil
}

func (s *Store) Register(ctx context.Context, c Credentials) (User, error) {
	c, err := normalize(c)
	if err != nil {
		return User{}, err
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(c.Password), bcrypt.DefaultCost)
	if err != nil {
		return User{}, err
	}
	u := User{ID: uuid.New(), Email: c.Email}
	_, err = s.DB.Exec(ctx, `INSERT INTO users(id,email,password_hash) VALUES($1,$2,$3)`, u.ID, u.Email, string(hash))
	var pe *pgconn.PgError
	if errors.As(err, &pe) && pe.Code == "23505" {
		return User{}, ErrEmail
	}
	return u, err
}

func (s *Store) Login(ctx context.Context, c Credentials) (Session, error) {
	c, err := normalize(c)
	if err != nil {
		return Session{}, ErrCredentials
	}
	var id uuid.UUID
	var hash string
	err = s.DB.QueryRow(ctx, `SELECT id,password_hash FROM users WHERE email=$1`, c.Email).Scan(&id, &hash)
	if errors.Is(err, pgx.ErrNoRows) {
		// Keep unknown-account logins comparable in cost.
		bcrypt.CompareHashAndPassword([]byte("$2a$10$7EqJtq98hPqEX7fNZaFWoO5ZIVQY3k6a5cOa4fRZfBfKMKkKBjzHy"), []byte(c.Password))
		return Session{}, ErrCredentials
	}
	if err != nil {
		return Session{}, err
	}
	if bcrypt.CompareHashAndPassword([]byte(hash), []byte(c.Password)) != nil {
		return Session{}, ErrCredentials
	}
	var secret [32]byte
	if _, err = rand.Read(secret[:]); err != nil {
		return Session{}, err
	}
	token := base64.RawURLEncoding.EncodeToString(secret[:])
	sum := sha256.Sum256([]byte(token))
	var expires time.Time
	err = s.DB.QueryRow(ctx, `INSERT INTO sessions(token_hash, user_id, expires_at)
	VALUES($1, $2, clock_timestamp()+interval '24 hours')
	RETURNING expires_at`, sum[:], id).Scan(&expires)
	return Session{Token: token, ExpiresAt: expires}, err
}

func (s *Store) Authenticate(ctx context.Context, token string) (uuid.UUID, error) {
	if len(token) != 43 {
		return uuid.Nil, ErrCredentials
	}
	sum := sha256.Sum256([]byte(token))
	var id uuid.UUID
	err := s.DB.QueryRow(ctx, `SELECT user_id FROM sessions WHERE token_hash=$1 AND expires_at>clock_timestamp()`, sum[:]).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrCredentials
	}
	return id, err
}
