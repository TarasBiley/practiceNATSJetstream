package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"practiceNATSJetstream/internal/model"
	"practiceNATSJetstream/internal/repository"
	"practiceNATSJetstream/internal/testutil"

	"github.com/nats-io/nats.go/jetstream"
)

func post(handler http.HandlerFunc, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/api/orders", strings.NewReader(body))
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	w := httptest.NewRecorder()
	handler(w, req)
	return w
}

func TestOrderValidation(t *testing.T) {
	for _, id := range []string{"order-100", "order_100", "ABC123"} {
		if !validOrderID(id) {
			t.Errorf("valid id rejected: %s", id)
		}
	}
	for _, id := range []string{"*", ">", "order.*", "a.b", "a b", "", "заказ", strings.Repeat("a", 65)} {
		if validOrderID(id) {
			t.Errorf("invalid id accepted: %s", id)
		}
		body, _ := json.Marshal(model.UpdateOrderRequest{OrderID: id, Status: "paid"})
		if got := post(UpdateOrder(nil, nil), "key", string(body)); got.Code != 400 {
			t.Fatalf("POST id=%q: %d", id, got.Code)
		}
		for _, h := range []http.HandlerFunc{GetOrder(nil), GetOrderFromStream(nil)} {
			r := httptest.NewRequest("GET", "/", nil)
			r.SetPathValue("order_id", id)
			w := httptest.NewRecorder()
			h(w, r)
			if w.Code != 400 {
				t.Fatalf("GET id=%q: %d", id, w.Code)
			}
		}
	}
	for _, tc := range []struct{ key, body string }{
		{"", `{"order_id":"order-1","status":"paid"}`},
		{" ", `{"order_id":"order-1","status":"paid"}`},
		{"key", `{`},
		{"key", `{"order_id":"order-1","status":"paid"} {}`},
		{"key", `{"order_id":"order-1","status":"paid","extra":1}`},
		{"key", `{"order_id":"order-1","status":"invalid"}`},
	} {
		if got := post(UpdateOrder(nil, nil), tc.key, tc.body); got.Code != 400 {
			t.Fatalf("body=%s: %d", tc.body, got.Code)
		}
	}
}

type fakePublisher struct {
	mu                    sync.Mutex
	calls, events         int
	last                  *jetstream.RawStreamMsg
	failBefore, failAfter bool
	entered               chan struct{}
	proceed               chan struct{}
}

func (p *fakePublisher) GetLastOrderStatus(context.Context, string) (*jetstream.RawStreamMsg, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.last == nil {
		return nil, jetstream.ErrMsgNotFound
	}
	return p.last, nil
}
func (p *fakePublisher) PrintStreamInfo(context.Context) error { return nil }
func (p *fakePublisher) PublishOrderStatus(_ context.Context, _ string, data []byte, _ string, _ uint64) (*jetstream.PubAck, error) {
	if p.entered != nil {
		close(p.entered)
		<-p.proceed
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if p.failBefore {
		p.failBefore = false
		return nil, errors.New("not published")
	}
	if p.last != nil && string(p.last.Data) == string(data) {
		return &jetstream.PubAck{Stream: "ORDERS", Sequence: 1, Duplicate: true}, nil
	}
	p.events++
	p.last = &jetstream.RawStreamMsg{Data: append([]byte(nil), data...), Sequence: 1}
	if p.failAfter {
		p.failAfter = false
		return nil, context.DeadlineExceeded
	}
	return &jetstream.PubAck{Stream: "ORDERS", Sequence: 1}, nil
}

func TestIdempotentPOST(t *testing.T) {
	db := testutil.Postgres(t)
	repo := repository.NewOrderRepository(db)
	publisher := &fakePublisher{}
	h := UpdateOrder(repo, publisher)
	body := `{"order_id":"order-1","status":"paid"}`
	first := post(h, "key", body)
	if first.Code != 200 {
		t.Fatalf("first: %d %s", first.Code, first.Body)
	}
	second := post(h, "key", `{ "status":"paid", "order_id":"order-1" }`)
	if second.Code != 200 || second.Body.String() != first.Body.String() {
		t.Fatalf("replay: %d %s", second.Code, second.Body)
	}
	if publisher.calls != 1 || publisher.events != 1 {
		t.Fatal("replay performed side effects")
	}
	mismatch := post(h, "key", `{"order_id":"order-1","status":"shipped"}`)
	if mismatch.Code != 409 {
		t.Fatalf("mismatch: %d", mismatch.Code)
	}
	var response model.UpdateOrderResponse
	if err := json.Unmarshal(first.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	saved, err := repo.GetByID(context.Background(), "order-1")
	if err != nil {
		t.Fatal(err)
	}
	var event model.Order
	if err := json.Unmarshal(publisher.last.Data, &event); err != nil {
		t.Fatal(err)
	}
	if !saved.UpdatedAt.Equal(response.UpdatedAt) || !event.UpdatedAt.Equal(response.UpdatedAt) {
		t.Fatal("timestamps differ")
	}
}

func TestPOSTRecoversFailures(t *testing.T) {
	for _, scenario := range []string{"before_publish", "lost_ack", "mark_published", "save_order"} {
		t.Run(scenario, func(t *testing.T) {
			db := testutil.Postgres(t)
			repo := repository.NewOrderRepository(db)
			p := &fakePublisher{failBefore: scenario == "before_publish", failAfter: scenario == "lost_ack"}
			ctx := context.Background()
			if scenario == "save_order" {
				_, err := db.Exec(ctx, `ALTER TABLE orders ADD CONSTRAINT injected_failure CHECK (status <> 'paid')`)
				if err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "mark_published" {
				_, err := db.Exec(ctx, `ALTER TABLE idempotency_keys ADD CONSTRAINT injected_failure CHECK (published_sequence IS NULL)`)
				if err != nil {
					t.Fatal(err)
				}
			}
			h := UpdateOrder(repo, p)
			body := `{"order_id":"order-retry","status":"paid"}`
			first := post(h, "retry-key", body)
			if first.Code != 500 {
				t.Fatalf("expected failure: %d %s", first.Code, first.Body)
			}
			if scenario == "save_order" {
				if _, err := db.Exec(ctx, `ALTER TABLE orders DROP CONSTRAINT injected_failure`); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "mark_published" {
				if _, err := db.Exec(ctx, `ALTER TABLE idempotency_keys DROP CONSTRAINT injected_failure`); err != nil {
					t.Fatal(err)
				}
			}
			next := post(h, "retry-key", body)
			if next.Code != 200 {
				t.Fatalf("retry: %d %s", next.Code, next.Body)
			}
			if p.events != 1 {
				t.Fatalf("logical events=%d", p.events)
			}
			if scenario == "save_order" && p.calls != 1 {
				t.Fatal("published twice after SQL failure")
			}
			replay := post(h, "retry-key", body)
			if replay.Body.String() != next.Body.String() {
				t.Fatal("replay result changed")
			}
		})
	}
}

func TestParallelPOSTDoesNotPublishTwice(t *testing.T) {
	repo := repository.NewOrderRepository(testutil.Postgres(t))
	p := &fakePublisher{entered: make(chan struct{}), proceed: make(chan struct{})}
	h := UpdateOrder(repo, p)
	body := `{"order_id":"order-concurrent","status":"paid"}`
	first := make(chan *httptest.ResponseRecorder, 1)
	go func() { first <- post(h, "parallel", body) }()
	select {
	case <-p.entered:
	case <-time.After(3 * time.Second):
		close(p.proceed)
		t.Fatal("publish did not start")
	}
	second := post(h, "parallel", body)
	otherKey := post(h, "different-key", body)
	close(p.proceed)
	result := <-first
	if second.Code != 409 || otherKey.Code != 409 || result.Code != 200 {
		t.Fatalf("statuses: %d %d %d", second.Code, otherKey.Code, result.Code)
	}
	if p.events != 1 {
		t.Fatalf("published %d events", p.events)
	}
}

func TestPreparationFailureReleasesClaim(t *testing.T) {
	db := testutil.Postgres(t)
	req := model.UpdateOrderRequest{OrderID: "order-preparation", Status: "paid"}
	hash, err := makeRequestHash(req)
	if err != nil {
		t.Fatal(err)
	}
	// Go's time.Time JSON marshaler rejects years outside [0,9999].
	_, err = db.Exec(context.Background(), `INSERT INTO idempotency_keys (idempotency_key,request_hash,order_id,updated_at)
 VALUES ('prepare-key',$1,$2,$3)`, hash, req.OrderID, time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	p := &fakePublisher{}
	h := UpdateOrder(repository.NewOrderRepository(db), p)
	body := `{"order_id":"order-preparation","status":"paid"}`
	if got := post(h, "prepare-key", body); got.Code != 500 {
		t.Fatalf("got %d", got.Code)
	}
	var count int
	if err := db.QueryRow(context.Background(), "SELECT count(*) FROM idempotency_keys").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 || p.events != 0 {
		t.Fatal("failed preparation left a claim or an event")
	}
	if got := post(h, "prepare-key", body); got.Code != 200 {
		t.Fatalf("retry: %d %s", got.Code, got.Body)
	}
}
