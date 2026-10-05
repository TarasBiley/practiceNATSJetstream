package cache

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisClient struct {
	Client *redis.Client
}

func NewRedisClient(ctx context.Context) (*RedisClient, error) {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, err
	}

	return &RedisClient{
		Client: client,
	}, nil
}

func (r *RedisClient) Close() error {
	return r.Client.Close()
}

func (r *RedisClient) SetOrderStatus(
	ctx context.Context,
	orderID string,
	data []byte,
) error {

	key := "order:" + orderID + ":status"

	return r.Client.Set(
		ctx,
		key,
		data,
		time.Hour,
	).Err()
}
