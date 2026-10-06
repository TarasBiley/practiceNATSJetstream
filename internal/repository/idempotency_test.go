package repository

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"practiceNATSJetstream/internal/model"
	"practiceNATSJetstream/internal/testutil"
)

func TestClaimIdempotencyAtomicAndRecoverable(t *testing.T) {
	db := testutil.Postgres(t)
	repo := NewOrderRepository(db)
	ctx := context.Background()
	const attempts = 12
	start := make(chan struct{})
	results := make(chan IdempotencyClaim, attempts)
	errs := make(chan error, attempts)
	var wg sync.WaitGroup
	for range attempts {
		wg.Go(func() {
			<-start
			claim, err := repo.ClaimIdempotency(ctx, "same-key", "hash", "order-atomic")
			results <- claim
			errs <- err
		})
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	winners := 0
	var winner IdempotencyClaim
	for claim := range results {
		if claim != nil {
			winners++
			winner = claim
		}
	}
	for err := range errs {
		if err != nil && !errors.Is(err, ErrRequestInProgress) {
			t.Fatal(err)
		}
	}
	if winners != 1 {
		t.Fatalf("got %d claims, want one", winners)
	}
	defer winner.Close()
	if err := winner.Prepare(ctx, time.Now(), 0); err != nil {
		t.Fatal(err)
	}
	if err := winner.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ClaimIdempotency(ctx, "another-key", "hash", "order-atomic"); !errors.Is(err, ErrRequestInProgress) {
		t.Fatalf("another key overwrote pending order: %v", err)
	}
	if _, err := repo.ClaimIdempotency(ctx, "same-key", "other-body", "order-atomic"); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("body mismatch: %v", err)
	}
	resumed, err := repo.ClaimIdempotency(ctx, "same-key", "hash", "order-atomic")
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.Close()
	if resumed.Record().ExpectedSequence == nil {
		t.Fatal("lost durable intent")
	}
}

func TestUnpreparedClaimReleasedOnCanceledContext(t *testing.T) {
	repo := NewOrderRepository(testutil.Postgres(t))
	ctx, cancel := context.WithCancel(context.Background())
	claim, err := repo.ClaimIdempotency(ctx, "key", "hash", "order-1")
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := repo.ClaimIdempotency(context.Background(), "new-key", "hash", "order-1")
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
}

func TestCompleteRollsBackOrderAndPreservesTimestamp(t *testing.T) {
	db := testutil.Postgres(t)
	repo := NewOrderRepository(db)
	ctx := context.Background()
	claim, err := repo.ClaimIdempotency(ctx, "key", "hash", "order-1")
	if err != nil {
		t.Fatal(err)
	}
	defer claim.Close()
	timestamp := time.Now().UTC().Truncate(time.Microsecond)
	if err := claim.Prepare(ctx, timestamp, 0); err != nil {
		t.Fatal(err)
	}
	if err := claim.MarkPublished(ctx, 1); err != nil {
		t.Fatal(err)
	}
	// Fail after the orders UPSERT to prove the two writes share a transaction.
	_, err = db.Exec(ctx, `CREATE FUNCTION reject_completion() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN IF NEW.completed THEN RAISE EXCEPTION 'injected completion failure'; END IF; RETURN NEW; END $$;
 CREATE TRIGGER reject_completion BEFORE UPDATE ON idempotency_keys FOR EACH ROW EXECUTE FUNCTION reject_completion()`)
	if err != nil {
		t.Fatal(err)
	}
	order := model.Order{OrderID: "order-1", Status: "paid", UpdatedAt: timestamp}
	if err := claim.Complete(ctx, order); err == nil {
		t.Fatal("expected injected failure")
	}
	var count int
	if err := db.QueryRow(ctx, "SELECT count(*) FROM orders").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("orders UPSERT was not rolled back")
	}
	if _, err := db.Exec(ctx, "DROP TRIGGER reject_completion ON idempotency_keys"); err != nil {
		t.Fatal(err)
	}
	if err := claim.Complete(ctx, order); err != nil {
		t.Fatal(err)
	}
	got, err := repo.GetByID(ctx, order.OrderID)
	if err != nil || !got.UpdatedAt.Equal(timestamp) {
		t.Fatalf("order=%+v err=%v", got, err)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	replay, err := repo.ClaimIdempotency(ctx, "key", "hash", "order-1")
	if err != nil {
		t.Fatal(err)
	}
	defer replay.Close()
	if !replay.Record().Completed || !replay.Record().UpdatedAt.Equal(timestamp) {
		t.Fatal("result was not saved")
	}
	// The next logical update also preserves the timestamp supplied by Go.
	order.UpdatedAt = timestamp.Add(time.Second)
	saved, err := repo.Save(ctx, order)
	if err != nil || !saved.Equal(order.UpdatedAt) {
		t.Fatalf("Save UPDATE timestamp=%v err=%v", saved, err)
	}
}
