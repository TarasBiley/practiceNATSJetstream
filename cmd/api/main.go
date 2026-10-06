package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"practiceNATSJetstream/config"
	_ "practiceNATSJetstream/docs"

	"practiceNATSJetstream/internal/cache"
	"practiceNATSJetstream/internal/database"
	"practiceNATSJetstream/internal/handler"
	natsclient "practiceNATSJetstream/internal/nats"
	"practiceNATSJetstream/internal/repository"
	"practiceNATSJetstream/internal/worker"

	httpSwagger "github.com/swaggo/http-swagger/v2"
)

// @title Order Status Service API
// @version 1.0
// @description Microservice for tracking current order statuses with PostgreSQL, NATS JetStream and Redis.
// @host localhost:8080
// @BasePath /
func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if err := run(); err != nil {
		slog.Error("application stopped with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	// PostgreSQL
	db, err := database.New(
		ctx,
		cfg.PostgresURL,
	)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := db.Ping(ctx); err != nil {
		return err
	}

	slog.Info("PostgreSQL connected")

	// NATS JetStream
	natsClient, err := natsclient.New(
		ctx,
		cfg.NATSURL,
	)
	if err != nil {
		return err
	}

	defer func() {
		if err := natsClient.Close(); err != nil {
			slog.Error(
				"NATS shutdown error",
				"error", err,
			)
		}
	}()

	slog.Info("NATS JetStream connected")

	// Redis
	redisClient, err := cache.NewRedisClient(
		ctx,
		cfg.RedisAddr,
	)
	if err != nil {
		return err
	}
	defer redisClient.Close()

	slog.Info("Redis connected")

	// Worker
	workerCtx, cancelWorker := context.WithCancel(context.Background())
	defer cancelWorker()

	orderConsumer, err := worker.NewOrderConsumer(
		ctx,
		natsClient,
		redisClient,
	)
	if err != nil {
		return err
	}

	slog.Info("JetStream consumer created")

	workerDone := make(chan struct{})

	go func() {
		defer close(workerDone)
		orderConsumer.Run(workerCtx)
	}()

	// Repository
	repo := repository.NewOrderRepository(db)

	// Router
	mux := http.NewServeMux()

	mux.HandleFunc(
		"POST /api/orders",
		handler.UpdateOrder(repo, natsClient),
	)

	mux.HandleFunc(
		"GET /api/orders/{order_id}",
		handler.GetOrder(repo),
	)

	mux.HandleFunc(
		"GET /api/stream/orders/{order_id}",
		handler.GetOrderFromStream(natsClient),
	)

	mux.HandleFunc(
		"POST /api/admin/sync-cache",
		handler.SyncCache(orderConsumer),
	)

	mux.Handle(
		"/swagger/",
		httpSwagger.WrapHandler,
	)

	// HTTP Server
	server := &http.Server{
		Addr:    ":" + cfg.HTTPPort,
		Handler: mux,
	}

	serverErr := make(chan error, 1)

	go func() {
		slog.Info(
			"HTTP server started",
			"addr", server.Addr,
		)

		serverErr <- server.ListenAndServe()
	}()

	// Ждём Ctrl+C / SIGTERM или ошибку HTTP-сервера.
	var serveErr error

	select {
	case <-ctx.Done():
	case err := <-serverErr:
		if err != http.ErrServerClosed {
			serveErr = err
		}
	}

	stop()

	slog.Info("shutting down")

	// Даём HTTP-запросам до 5 секунд на завершение.
	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error(
			"HTTP shutdown error",
			"error", err,
		)

		if err := server.Close(); err != nil {
			slog.Error(
				"HTTP close error",
				"error", err,
			)
		}
	}

	// Сначала останавливаем consumer.
	cancelWorker()
	<-workerDone

	slog.Info("consumer stopped")
	slog.Info("server stopped")

	return serveErr
}
