package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"practiceNATSJetstream/internal/model"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrIdempotencyConflict = errors.New("Idempotency-Key already used for another request")
	ErrRequestInProgress   = errors.New("request for this key or order is in progress; retry the original key")
)

type IdempotencyRecord struct {
	RequestHash       string
	OrderID           string
	UpdatedAt         *time.Time
	ExpectedSequence  *uint64
	PublishedSequence *uint64
	Completed         bool
}

// IdempotencyClaim owns a PostgreSQL session lock for the duration of one attempt.
// Prepare is committed before publishing; Complete saves the order and result atomically.
// Close releases the attempt lock, but retains prepared/published work for a retry.
// A claim is used by one goroutine only.
type IdempotencyClaim interface {
	Record() IdempotencyRecord
	Prepare(context.Context, time.Time, uint64) error
	MarkPublished(context.Context, uint64) error
	Complete(context.Context, model.Order) error
	Close() error
}

type idempotencyClaim struct {
	conn   *pgxpool.Conn
	key    string
	record IdempotencyRecord
}

func (r *OrderRepository) ClaimIdempotency(ctx context.Context, key, requestHash, orderID string) (IdempotencyClaim, error) {
	conn, err := r.db.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	var locked bool
	err = conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtextextended('orders-idempotency:' || $1, 0))`, key).Scan(&locked)
	if err != nil {
		// An interrupted query may have acquired the session lock. Never pool it.
		closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = conn.Conn().Close(closeCtx)
		conn.Release()
		return nil, err
	}
	if !locked {
		conn.Release()
		return nil, ErrRequestInProgress
	}
	claim := &idempotencyClaim{conn: conn, key: key}
	// The PK prevents duplicate keys; the partial unique index also prevents a
	// different key from overwriting the latest event of an unfinished order.
	_, err = conn.Exec(ctx, `
		INSERT INTO idempotency_keys (idempotency_key, request_hash, order_id)
		VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, key, requestHash, orderID)
	if err == nil {
		err = conn.QueryRow(ctx, `
			SELECT request_hash, order_id, updated_at, expected_sequence, published_sequence, completed
			FROM idempotency_keys WHERE idempotency_key = $1`, key).Scan(
			&claim.record.RequestHash, &claim.record.OrderID, &claim.record.UpdatedAt,
			&claim.record.ExpectedSequence, &claim.record.PublishedSequence, &claim.record.Completed)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrRequestInProgress
	}
	if err == nil && (claim.record.RequestHash != requestHash || claim.record.OrderID != orderID) {
		err = ErrIdempotencyConflict
	}
	if err != nil {
		// Do not delete another request's unprepared claim on a hash conflict.
		return nil, errors.Join(err, claim.unlock())
	}
	return claim, nil
}

func (c *idempotencyClaim) Record() IdempotencyRecord { return c.record }

func (c *idempotencyClaim) Prepare(ctx context.Context, updatedAt time.Time, expectedSequence uint64) error {
	updatedAt = updatedAt.UTC().Truncate(time.Microsecond)
	tag, err := c.conn.Exec(ctx, `
		UPDATE idempotency_keys SET updated_at = $2, expected_sequence = $3
		WHERE idempotency_key = $1 AND NOT completed AND expected_sequence IS NULL`,
		c.key, updatedAt, expectedSequence)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("idempotency preparation did not update exactly one record")
	}
	c.record.UpdatedAt = &updatedAt
	c.record.ExpectedSequence = &expectedSequence
	return nil
}

func (c *idempotencyClaim) MarkPublished(ctx context.Context, sequence uint64) error {
	tag, err := c.conn.Exec(ctx, `
		UPDATE idempotency_keys SET published_sequence = $2
		WHERE idempotency_key = $1 AND NOT completed AND expected_sequence IS NOT NULL`, c.key, sequence)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("idempotency publish did not update exactly one record")
	}
	c.record.PublishedSequence = &sequence
	return nil
}

func (c *idempotencyClaim) Complete(ctx context.Context, order model.Order) error {
	if c.record.PublishedSequence == nil || c.record.UpdatedAt == nil ||
		order.OrderID != c.record.OrderID || !order.UpdatedAt.Equal(*c.record.UpdatedAt) {
		return errors.New("cannot complete an unconfirmed or mismatched order")
	}
	tx, err := c.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()
	if _, err := saveOrder(ctx, tx, order); err != nil {
		return fmt.Errorf("save order: %w", err)
	}
	tag, err := tx.Exec(ctx, `UPDATE idempotency_keys SET completed = TRUE
		WHERE idempotency_key = $1 AND NOT completed AND published_sequence IS NOT NULL`, c.key)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("idempotency completion did not update exactly one record")
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	c.record.Completed = true
	return nil
}

func (c *idempotencyClaim) Close() error {
	if c.conn == nil {
		return nil
	}
	// Independent of a canceled HTTP context. Only work that cannot have been
	// published is deleted. Prepared records can always be resumed with the key.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := c.conn.Exec(ctx, `DELETE FROM idempotency_keys
		WHERE idempotency_key = $1 AND NOT completed AND expected_sequence IS NULL`, c.key)
	return errors.Join(err, c.unlock())
}

func (c *idempotencyClaim) unlock() error {
	if c.conn == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var unlocked bool
	err := c.conn.QueryRow(ctx, `SELECT pg_advisory_unlock(hashtextextended('orders-idempotency:' || $1, 0))`, c.key).Scan(&unlocked)
	if err != nil || !unlocked {
		// Discard rather than return a potentially locked session to the pool.
		_ = c.conn.Conn().Close(ctx)
		if err == nil {
			err = errors.New("idempotency session lock was lost")
		}
	}
	c.conn.Release()
	c.conn = nil
	return err
}
