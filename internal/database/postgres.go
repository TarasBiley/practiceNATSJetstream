package database

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

func New(
	ctx context.Context,
	postgresURL string,
) (*pgxpool.Pool, error) {

	db, err := pgxpool.New(
		ctx,
		postgresURL,
	)
	if err != nil {
		return nil, err
	}

	return db, nil
}
