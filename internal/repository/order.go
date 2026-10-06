package repository

import (
	"context"
	"time"

	"practiceNATSJetstream/internal/model"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type OrderRepository struct {
	db *pgxpool.Pool
}

func NewOrderRepository(db *pgxpool.Pool) *OrderRepository {
	return &OrderRepository{
		db: db,
	}
}

func (r *OrderRepository) Save(
	ctx context.Context,
	order model.Order,
) (time.Time, error) {
	return saveOrder(ctx, r.db, order)
}

type orderQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func saveOrder(ctx context.Context, db orderQuerier, order model.Order) (time.Time, error) {
	query := `
		INSERT INTO orders (order_id, status, comment, updated_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (order_id)
		DO UPDATE SET
			status = EXCLUDED.status,
			comment = EXCLUDED.comment,
			updated_at = EXCLUDED.updated_at
		RETURNING updated_at
	`

	var updatedAt time.Time

	err := db.QueryRow(
		ctx,
		query,
		order.OrderID,
		order.Status,
		order.Comment,
		order.UpdatedAt,
	).Scan(&updatedAt)

	return updatedAt, err
}

func (r *OrderRepository) GetByID(
	ctx context.Context,
	orderID string,
) (model.Order, error) {

	query := `
		SELECT order_id, status, comment, updated_at
		FROM orders
		WHERE order_id = $1
	`

	var order model.Order

	err := r.db.QueryRow(
		ctx,
		query,
		orderID,
	).Scan(
		&order.OrderID,
		&order.Status,
		&order.Comment,
		&order.UpdatedAt,
	)

	return order, err
}
