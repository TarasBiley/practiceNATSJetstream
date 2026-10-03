package worker

import (
	"context"
	"encoding/json"
	"log"
	"practiceNATSJetstream/internal/cache"
	"practiceNATSJetstream/internal/model"
	natsclient "practiceNATSJetstream/internal/nats"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type OrderConsumer struct {
	natsClient  *natsclient.Client
	redisClient *cache.RedisClient
	consumer    jetstream.Consumer
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
			MaxDeliver:    3,
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
	batch, err := w.consumer.Fetch(
		100,
		jetstream.FetchMaxWait(2*time.Second),
	)
	if err != nil {
		return err
	}

	for msg := range batch.Messages() {

		metadata, err := msg.Metadata()
		if err == nil {
			log.Printf(
				"stream=%s subject=%s seq=%d delivery=%d",
				metadata.Stream,
				msg.Subject(),
				metadata.Sequence.Stream,
				metadata.NumDelivered,
			)
		}

		w.processMessage(ctx, msg)
	}

	if err := batch.Error(); err != nil {
		return err
	}

	return nil
}

func (w *OrderConsumer) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	log.Println("order consumer started")

	for {
		select {
		case <-ticker.C:
			err := w.ProcessOnce(ctx)
			if err != nil {
				log.Printf("consumer processing error: %v", err)
			}

		case <-ctx.Done():
			log.Println("order consumer stopped")
			return
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

		log.Printf(
			"sync-cache stream=ORDERS subject=%s seq=%d",
			msg.Subject,
			msg.Sequence,
		)

		synced++
	}

	return synced, nil
}

func (w *OrderConsumer) processMessage(
	ctx context.Context,
	msg jetstream.Msg,
) {
	var order model.Order

	if err := json.Unmarshal(msg.Data(), &order); err != nil {
		log.Printf("failed to decode message: %v", err)

		if err := msg.Term(); err != nil {
			log.Printf("failed to terminate message: %v", err)
		}

		return
	}

	const maxAttempts = 3

	for attempt := 1; attempt <= maxAttempts; attempt++ {

		err := w.redisClient.SetOrderStatus(
			ctx,
			order.OrderID,
			msg.Data(),
		)

		if err == nil {
			if err := msg.Ack(); err != nil {
				log.Printf("failed to ack message: %v", err)
				return
			}

			log.Printf(
				"order=%s cached and acked attempt=%d",
				order.OrderID,
				attempt,
			)

			return
		}

		log.Printf(
			"failed to save order=%s to Redis attempt=%d/%d: %v",
			order.OrderID,
			attempt,
			maxAttempts,
			err,
		)

		if attempt == maxAttempts {
			if err := msg.Term(); err != nil {
				log.Printf("failed to terminate message: %v", err)
			}

			log.Printf(
				"order=%s stopped after %d attempts",
				order.OrderID,
				maxAttempts,
			)

			return
		}

		timer := time.NewTimer(10 * time.Second)

		select {
		case <-timer.C:
			// через 10 секунд следующая попытка

		case <-ctx.Done():
			timer.Stop()
			return
		}
	}
}
