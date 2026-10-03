package nats

import (
	"context"
	"fmt"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type Client struct {
	Conn   *natsgo.Conn
	JS     jetstream.JetStream
	Stream jetstream.Stream
}

func New(ctx context.Context) (*Client, error) {
	nc, err := natsgo.Connect("nats://localhost:4222")
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
	}, nil
}

func (c *Client) Close() error {
	return c.Conn.Drain()
}

func (c *Client) PublishOrderStatus(
	ctx context.Context,
	orderID string,
	data []byte,
	msgID string,
) (*jetstream.PubAck, error) {

	subject := "order.status." + orderID

	ack, err := c.JS.Publish(
		ctx,
		subject,
		data,
		jetstream.WithMsgID(msgID),
	)
	if err != nil {
		return nil, err
	}

	return ack, nil
}

func (c *Client) PrintStreamInfo(ctx context.Context) error {
	info, err := c.Stream.Info(ctx)
	if err != nil {
		return err
	}

	fmt.Printf(
		"stream=%s messages=%d subjects=%d\n",
		info.Config.Name,
		info.State.Msgs,
		info.State.NumSubjects,
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
