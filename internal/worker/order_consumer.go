package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"practiceNATSJetstream/internal/cache"
	"practiceNATSJetstream/internal/model"
	natsclient "practiceNATSJetstream/internal/nats"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const maxDeliveries = 3
const retryDelay = 10 * time.Second

type orderCache interface {
	SetOrderStatus(context.Context, string, []byte) error
}

type batchConsumer interface {
	FetchNoWait(int) (jetstream.MessageBatch, error)
}

type OrderConsumer struct {
	natsClient  *natsclient.Client
	redisClient orderCache
	consumer    batchConsumer
}

func NewOrderConsumer(
	ctx context.Context,
	natsClient *natsclient.Client,
	redisClient *cache.RedisClient,
) (*OrderConsumer, error) {

	consumer, err := natsClient.JS.CreateOrUpdateConsumer(
		ctx,
		"ORDERS",
		jetstream.ConsumerConfig{
			Durable:       "ORDERS_CACHE",
			AckPolicy:     jetstream.AckExplicitPolicy,
			FilterSubject: "order.status.*",
			MaxDeliver:    maxDeliveries,
		},
	)
	if err != nil {
		return nil, err
	}

	return &OrderConsumer{
		natsClient:  natsClient,
		redisClient: redisClient,
		consumer:    consumer,
	}, nil
}

func (w *OrderConsumer) ProcessOnce(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		batch, err := w.consumer.FetchNoWait(100)
		if err != nil {
			return err
		}

		n := 0

		messages := batch.Messages()
	batchLoop:
		for {
			var msg jetstream.Msg
			select {
			case <-ctx.Done():
				return ctx.Err()
			case next, ok := <-messages:
				if !ok {
					break batchLoop
				}
				msg = next
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			n++

			metadata, err := msg.Metadata()
			if err == nil {
				slog.Info(
					"JetStream message received",
					"stream", metadata.Stream,
					"subject", msg.Subject(),
					"seq", metadata.Sequence.Stream,
					"delivery", metadata.NumDelivered,
				)
			}

			processCtx, cancel := context.WithTimeout(
				context.WithoutCancel(ctx),
				25*time.Second,
			)

			w.processMessage(processCtx, msg)
			cancel()
		}

		if err := batch.Error(); err != nil {
			return err
		}

		if n == 0 {
			break
		}
	}

	return nil
}

func (w *OrderConsumer) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	slog.Info("order consumer started")
	defer slog.Info("order consumer stopped")
	for {
		// Drain the entire available backlog immediately, then poll for new/redelivered messages.
		if err := w.ProcessOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("consumer processing error", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *OrderConsumer) SyncCache(ctx context.Context) (int, error) {
	info, err := w.natsClient.Stream.Info(
		ctx,
		jetstream.WithSubjectFilter("order.status.*"),
	)
	if err != nil {
		return 0, err
	}

	synced := 0

	for subject := range info.State.Subjects {
		msg, err := w.natsClient.Stream.GetLastMsgForSubject(ctx, subject)
		if err != nil {
			return synced, err
		}

		var order model.Order

		if err := json.Unmarshal(msg.Data, &order); err != nil {
			return synced, err
		}

		err = w.redisClient.SetOrderStatus(
			ctx,
			order.OrderID,
			msg.Data,
		)
		if err != nil {
			return synced, err
		}

		slog.Info(
			"cache synchronized",
			"stream", "ORDERS",
			"subject", msg.Subject,
			"seq", msg.Sequence,
		)

		synced++
	}

	return synced, nil
}

func (w *OrderConsumer) processMessage(
	ctx context.Context,
	msg jetstream.Msg,
) {
	if ctx.Err() != nil {
		return
	}

	var order model.Order

	if err := json.Unmarshal(msg.Data(), &order); err != nil {
		slog.Error(
			"failed to decode message",
			"error", err,
		)

		if err := msg.Term(); err != nil {
			slog.Error(
				"failed to terminate message",
				"error", err,
			)
		}

		return
	}

	metadata, metadataErr := msg.Metadata()
	var delivery uint64
	if metadataErr == nil {
		delivery = metadata.NumDelivered
	}
	err := w.redisClient.SetOrderStatus(
		ctx,
		order.OrderID,
		msg.Data(),
	)

	if err == nil {
		if err := msg.Ack(); err != nil {
			slog.Error(
				"failed to ack message",
				"order_id", order.OrderID,
				"error", err,
			)
			return
		}

		slog.Info(
			"order cached and acked",
			"order_id", order.OrderID,
			"attempt", delivery,
		)

		return
	}

	if ctx.Err() != nil {
		return
	}
	if metadataErr != nil {
		slog.Error(
			"failed to get message metadata",
			"order_id", order.OrderID,
			"error", metadataErr,
		)
	}

	if metadataErr == nil && metadata.NumDelivered >= maxDeliveries {
		slog.Error(
			"order cache retries exhausted",
			"order_id", order.OrderID,
			"delivery", metadata.NumDelivered,
			"error", err,
		)

		if err := msg.Term(); err != nil {
			slog.Error(
				"failed to terminate message",
				"order_id", order.OrderID,
				"error", err,
			)
		}

		return
	}

	slog.Warn(
		"failed to cache order, scheduling retry",
		"order_id", order.OrderID,
		"attempt", delivery,
		"delay", retryDelay,
		"error", err,
	)

	if err := msg.NakWithDelay(retryDelay); err != nil {
		slog.Error(
			"failed to NAK message",
			"order_id", order.OrderID,
			"error", err,
		)
	}
}
