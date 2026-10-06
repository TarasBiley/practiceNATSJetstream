package worker

import (
	"context"
	"errors"
	"github.com/nats-io/nats.go/jetstream"
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

type testMessage struct {
	jetstream.Msg
	data              []byte
	delivery          uint64
	acks, terms, naks int
	delay             time.Duration
}

func (m *testMessage) Data() []byte    { return m.data }
func (m *testMessage) Subject() string { return "order.status.order-1" }
func (m *testMessage) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{Stream: "ORDERS", NumDelivered: m.delivery}, nil
}
func (m *testMessage) Ack() error                         { m.acks++; return nil }
func (m *testMessage) Term() error                        { m.terms++; return nil }
func (m *testMessage) NakWithDelay(d time.Duration) error { m.naks++; m.delay = d; return nil }

type testCache struct {
	calls  int
	err    error
	cancel context.CancelFunc
}

func (c *testCache) SetOrderStatus(context.Context, string, []byte) error {
	c.calls++
	if c.cancel != nil {
		c.cancel()
	}
	return c.err
}

func TestMessageRetrySemantics(t *testing.T) {
	for _, tc := range []struct {
		name              string
		delivery          uint64
		data              string
		failed            bool
		acks, terms, naks int
	}{
		{"success", 1, `{"order_id":"order-1"}`, false, 1, 0, 0},
		{"retry", 1, `{"order_id":"order-1"}`, true, 0, 0, 1},
		{"last_delivery", 3, `{"order_id":"order-1"}`, true, 0, 1, 0},
		{"invalid_json", 1, `{`, false, 0, 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache := &testCache{}
			if tc.failed {
				cache.err = errors.New("Redis unavailable")
			}
			w := &OrderConsumer{redisClient: cache}
			msg := &testMessage{data: []byte(tc.data), delivery: tc.delivery}
			w.processMessage(context.Background(), msg)
			if msg.acks != tc.acks || msg.terms != tc.terms || msg.naks != tc.naks {
				t.Fatalf("ack/term/nak=%d/%d/%d", msg.acks, msg.terms, msg.naks)
			}
			if tc.naks > 0 && msg.delay != 10*time.Second {
				t.Fatalf("delay=%v", msg.delay)
			}
			if cache.calls > 1 {
				t.Fatal("manual Redis retry loop")
			}
		})
	}
}

type testBatch struct{ messages chan jetstream.Msg }

func (b testBatch) Messages() <-chan jetstream.Msg { return b.messages }
func (b testBatch) Error() error                   { return nil }

type testConsumer struct {
	remaining, calls int
	messages         []*testMessage
}

func (c *testConsumer) FetchNoWait(batch int) (jetstream.MessageBatch, error) {
	c.calls++
	ch := make(chan jetstream.Msg, batch)
	for range min(c.remaining, batch) {
		msg := &testMessage{data: []byte(`{"order_id":"order-1"}`), delivery: 1}
		c.messages = append(c.messages, msg)
		ch <- msg
		c.remaining--
	}
	close(ch)
	return testBatch{ch}, nil
}
func TestProcessOnceDrainsAllBatches(t *testing.T) {
	consumer := &testConsumer{remaining: 205}
	cache := &testCache{}
	w := &OrderConsumer{consumer: consumer, redisClient: cache}
	if err := w.ProcessOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if consumer.calls != 4 || cache.calls != 205 {
		t.Fatalf("fetches=%d processed=%d", consumer.calls, cache.calls)
	}
	for _, msg := range consumer.messages {
		if msg.acks != 1 {
			t.Fatal("message not acknowledged")
		}
	}
}
func TestCancellationDoesNotTerminateMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache := &testCache{err: context.Canceled, cancel: cancel}
	msg := &testMessage{data: []byte(`{"order_id":"order-1"}`), delivery: 3}
	w := &OrderConsumer{redisClient: cache}
	w.processMessage(ctx, msg)
	if msg.terms != 0 || msg.acks != 0 || msg.naks != 0 {
		t.Fatal("shutdown finalized an unprocessed message")
	}
}
