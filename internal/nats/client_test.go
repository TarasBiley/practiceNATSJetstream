package nats

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"net"
	"os"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"
)

// Delay the drain's PONG to verify Close waits for the actual connection close.
func TestCloseWaitsForDrain(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	drainPing := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		fmt.Fprint(conn, "INFO {}\r\n")
		scanner := bufio.NewScanner(conn)
		pings := 0
		for scanner.Scan() {
			if scanner.Text() == "PING" {
				pings++
				if pings == 2 {
					close(drainPing)
					<-release
				}
				fmt.Fprint(conn, "PONG\r\n")
			}
		}
		if err := scanner.Err(); err != nil {
			t.Errorf("scanner error: %v", err)
		}

	}()
	closed := make(chan struct{})
	nc, err := natsgo.Connect("nats://"+listener.Addr().String(),
		natsgo.NoReconnect(),
		natsgo.ClosedHandler(func(*natsgo.Conn) { close(closed) }),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	client := &Client{Conn: nc, closed: closed}
	done := make(chan error, 1)
	go func() { done <- client.Close() }()
	select {
	case <-drainPing:
	case <-time.After(2 * time.Second):
		t.Fatal("drain did not flush the connection")
	}
	select {
	case err := <-done:
		t.Fatalf("Close returned before drain completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	release <- struct{}{}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not finish after drain")
	}
	if !nc.IsClosed() {
		t.Fatal("connection is still open")
	}
	if err := client.Close(); err != nil {
		t.Fatalf("repeated Close failed: %v", err)
	}
}

type lastMessageStream struct {
	jetstream.Stream
	message *jetstream.RawStreamMsg
}

func (s lastMessageStream) GetLastMsgForSubject(context.Context, string) (*jetstream.RawStreamMsg, error) {
	return s.message, nil
}

func TestPublishRecoversWithoutRepublishing(t *testing.T) {
	data := []byte(`{"order_id":"order-1","status":"paid"}`)
	client := &Client{Stream: lastMessageStream{message: &jetstream.RawStreamMsg{
		Data: data, Sequence: 42, Header: natsgo.Header{jetstream.MsgIDHeader: []string{"request-id"}},
	}}}
	// JS is nil: any actual publish would panic. Recovery is independent of the dedup window.
	ack, err := client.PublishOrderStatus(context.Background(), "order-1", data, "request-id", 0)
	if err != nil || ack.Sequence != 42 || !ack.Duplicate {
		t.Fatalf("ack=%+v err=%v", ack, err)
	}
	_, err = client.PublishOrderStatus(context.Background(), "order-1", []byte("different"), "request-id", 0)
	if !errors.Is(err, ErrPublishConflict) {
		t.Fatalf("mismatched recovered event: %v", err)
	}
}

func TestPublishCASIntegration(t *testing.T) {
	url := os.Getenv("TEST_NATS_URL")
	if url == "" {
		t.Skip("set TEST_NATS_URL to an isolated JetStream server")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	id := fmt.Sprintf("cas-%d", time.Now().UnixNano())
	first, err := client.PublishOrderStatus(ctx, id, []byte("first"), id+"-1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.PublishOrderStatus(ctx, id, []byte("stale"), id+"-2", 0); !errors.Is(err, ErrPublishConflict) {
		t.Fatalf("CAS failed to reject stale sequence: %v", err)
	}
	replay, err := client.PublishOrderStatus(ctx, id, []byte("first"), id+"-1", 0)
	if err != nil || replay.Sequence != first.Sequence || !replay.Duplicate {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	next, err := client.PublishOrderStatus(ctx, id, []byte("next"), id+"-3", first.Sequence)
	if err != nil || next.Sequence <= first.Sequence {
		t.Fatalf("next=%+v err=%v", next, err)
	}
}
