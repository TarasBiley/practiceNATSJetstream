package nats

import (
	"bufio"
	"fmt"
	"net"
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
