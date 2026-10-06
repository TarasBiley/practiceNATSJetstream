package nats

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var ErrPublishConflict = errors.New("order changed while publishing")

type Client struct {
	Conn   *natsgo.Conn
	JS     jetstream.JetStream
	Stream jetstream.Stream
	closed <-chan struct{}
}

func New(
	ctx context.Context,
	natsURL string,
) (*Client, error) {

	closed := make(chan struct{})

	nc, err := natsgo.Connect(
		natsURL,
		natsgo.DrainTimeout(5*time.Second),
		natsgo.ClosedHandler(func(*natsgo.Conn) {
			close(closed)
		}),
	)

	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}

	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:              "ORDERS",
		Subjects:          []string{"order.status.*"},
		Storage:           jetstream.FileStorage,
		MaxMsgsPerSubject: 1,
		AllowDirect:       true,
	})
	if err != nil {
		nc.Close()
		return nil, err
	}

	return &Client{
		Conn:   nc,
		JS:     js,
		Stream: stream,
		closed: closed,
	}, nil
}

func (c *Client) Close() error {
	if c.Conn.IsClosed() {
		return nil
	}
	if err := c.Conn.Drain(); err != nil {
		c.Conn.Close()
		return err
	}
	timer := time.NewTimer(6 * time.Second)
	defer timer.Stop()
	select {
	case <-c.closed:
		return c.Conn.LastError()
	case <-timer.C:
		c.Conn.Close()
		slog.Error("NATS drain timeout")

		return errors.New("NATS drain timeout")
	}
}

// PublishOrderStatus uses the durable expected sequence from the idempotency
// record. Never refresh it after an ambiguous timeout: that could create a second
// event once the JetStream deduplication window expires.
func (c *Client) PublishOrderStatus(
	ctx context.Context, orderID string, data []byte, msgID string, expectedSeq uint64,
) (*jetstream.PubAck, error) {
	subject := "order.status." + orderID
	lastMsg, err := c.Stream.GetLastMsgForSubject(ctx, subject)
	if err != nil && !errors.Is(err, jetstream.ErrMsgNotFound) {
		return nil, err
	}
	if err == nil && lastMsg.Header.Get(jetstream.MsgIDHeader) == msgID {
		if !bytes.Equal(lastMsg.Data, data) {
			return nil, ErrPublishConflict
		}
		return &jetstream.PubAck{Stream: "ORDERS", Sequence: lastMsg.Sequence, Duplicate: true}, nil
	}
	ack, err := c.JS.Publish(ctx, subject, data,
		jetstream.WithMsgID(msgID),
		jetstream.WithExpectLastSequencePerSubject(expectedSeq),
	)
	if err != nil {
		var apiErr *jetstream.APIError
		if errors.As(err, &apiErr) && (apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence ||
			apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequenceConstant) {
			return nil, ErrPublishConflict
		}
		return nil, err
	}
	return ack, nil
}

func (c *Client) PrintStreamInfo(ctx context.Context) error {
	info, err := c.Stream.Info(ctx)
	if err != nil {
		return err
	}

	slog.Info(
		"NATS stream info",
		"stream", info.Config.Name,
		"messages", info.State.Msgs,
		"subjects", info.State.NumSubjects,
	)

	return nil
}

func (c *Client) GetLastOrderStatus(
	ctx context.Context,
	orderID string,
) (*jetstream.RawStreamMsg, error) {

	subject := "order.status." + orderID

	msg, err := c.Stream.GetLastMsgForSubject(ctx, subject)
	if err != nil {
		return nil, err
	}

	return msg, nil
}
