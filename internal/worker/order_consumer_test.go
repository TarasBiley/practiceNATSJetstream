package worker

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestConsumerStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w := &OrderConsumer{}
	if err := w.ProcessOnce(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("ProcessOnce returned %v, want context.Canceled", err)
	}
	// No dependencies should be accessed after cancellation.
	w.processMessage(ctx, nil)
	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("consumer did not stop")
	}
}
