package store

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

// Store holds the connection pool and exposes all database operations.
type Store struct {
	pool *pgxpool.Pool
}

// Endpoint is a subscriber URL that receives webhook deliveries.
type Endpoint struct {
	ID        string    `json:"id"`
	URL       string    `json:"url"`
	CreatedAt time.Time `json:"created_at"`
}

// Event is an immutable record of something that happened in the application.
// One event fans out to one Delivery per registered Endpoint.
type Event struct {
	ID        string          `json:"id"`
	EventType string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload"` // stored as JSONB, returned as-is in HTTP responses
	CreatedAt time.Time       `json:"created_at"`
}

// Delivery is one unit of work in the queue, one event destined for one endpoint.
// The dispatcher polls this table and marks rows delivered or failed.
type Delivery struct {
	ID          string
	EventID     string
	EndpointID  string
	EndpointURL string
	Payload     []byte
}

// Attempt is an append-only record of a single HTTP send.
// StatusCode is nil when the request never reached the server (network error).
type Attempt struct {
	DeliveryID   string
	StatusCode   *int
	ResponseBody string
	Error        string
	DurationMs   int64
}

// Connect creates a connection pool. Connections are opened lazily as needed.
func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	return pgxpool.New(ctx, dsn)
}

// Migrate runs any pending goose migrations from the embedded SQL files.
// goose requires a *sql.DB, so we wrap the pgx pool with a stdlib adapter just for this call.
func Migrate(pool *pgxpool.Pool, migrations fs.FS) error {
	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()
	goose.SetBaseFS(migrations)
	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}
	return goose.Up(db, ".")
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// BeginTx opens a transaction. The caller is responsible for Commit or Rollback.
func (s *Store) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return s.pool.Begin(ctx)
}

func (s *Store) CreateEndpoint(ctx context.Context, url string) (Endpoint, error) {
	var e Endpoint
	err := s.pool.QueryRow(ctx,
		`INSERT INTO endpoints (url) VALUES ($1) RETURNING id, url, created_at`,
		url,
	).Scan(&e.ID, &e.URL, &e.CreatedAt)
	return e, err
}

func (s *Store) ListEndpoints(ctx context.Context) ([]Endpoint, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, url FROM endpoints`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Endpoint
	for rows.Next() {
		var e Endpoint
		if err := rows.Scan(&e.ID, &e.URL); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	// rows.Err() catches any error that occurred mid-iteration, not just at query time.
	return out, rows.Err()
}

// CreateEvent inserts a new event inside an existing transaction.
// Must be called in the same tx as CreateDelivery so both succeed or fail together.
func (s *Store) CreateEvent(ctx context.Context, tx pgx.Tx, eventType string, payload json.RawMessage) (Event, error) {
	var e Event
	err := tx.QueryRow(ctx,
		`INSERT INTO events (event_type, payload) VALUES ($1, $2)
		 RETURNING id, event_type, payload, created_at`,
		eventType, []byte(payload),
	).Scan(&e.ID, &e.EventType, &e.Payload, &e.CreatedAt)
	return e, err
}

// CreateDelivery creates one delivery job for a given event+endpoint pair.
// Called once per endpoint inside the same tx as CreateEvent.
func (s *Store) CreateDelivery(ctx context.Context, tx pgx.Tx, eventID, endpointID string) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO deliveries (event_id, endpoint_id) VALUES ($1, $2)`,
		eventID, endpointID,
	)
	return err
}

// claimQuery selects the next pending delivery and locks its row.
// FOR UPDATE OF d SKIP LOCKED means: lock only the deliveries row, and if another
// worker already holds the lock, skip it instead of waiting.
const claimQuery = `
SELECT d.id, d.event_id, d.endpoint_id, e.url, ev.payload
FROM   deliveries d
JOIN   endpoints  e  ON e.id  = d.endpoint_id
JOIN   events     ev ON ev.id = d.event_id
WHERE  d.status          = 'pending'
  AND  d.next_attempt_at <= NOW()
ORDER BY d.created_at
FOR UPDATE OF d SKIP LOCKED
LIMIT 1`

// ClaimDelivery grabs the next pending delivery inside an open transaction.
// Returns (delivery, true, nil) if one was found, (zero, false, nil) if the queue is empty.
// The row stays locked until the transaction is committed or rolled back.
func (s *Store) ClaimDelivery(ctx context.Context, tx pgx.Tx) (Delivery, bool, error) {
	var d Delivery
	err := tx.QueryRow(ctx, claimQuery).Scan(
		&d.ID, &d.EventID, &d.EndpointID, &d.EndpointURL, &d.Payload,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Delivery{}, false, nil
	}
	if err != nil {
		return Delivery{}, false, err
	}
	return d, true, nil
}

func (s *Store) MarkDelivered(ctx context.Context, tx pgx.Tx, id string) error {
	_, err := tx.Exec(ctx, `UPDATE deliveries SET status = 'delivered' WHERE id = $1`, id)
	return err
}

func (s *Store) MarkFailed(ctx context.Context, tx pgx.Tx, id string) error {
	_, err := tx.Exec(ctx, `UPDATE deliveries SET status = 'failed' WHERE id = $1`, id)
	return err
}

// RecordAttempt writes the result of an HTTP send to the attempts table.
// NULLIF converts an empty error string to NULL so the column is NULL on success.
func (s *Store) RecordAttempt(ctx context.Context, tx pgx.Tx, a Attempt) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO attempts (delivery_id, status_code, response_body, error, duration_ms)
		 VALUES ($1, $2, $3, NULLIF($4, ''), $5)`,
		a.DeliveryID, a.StatusCode, a.ResponseBody, a.Error, a.DurationMs,
	)
	return err
}
