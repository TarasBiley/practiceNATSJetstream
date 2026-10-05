package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "practiceNATSJetstream/docs"

	"practiceNATSJetstream/internal/cache"
	"practiceNATSJetstream/internal/handler"
	natsclient "practiceNATSJetstream/internal/nats"
	"practiceNATSJetstream/internal/repository"
	"practiceNATSJetstream/internal/worker"

	"github.com/jackc/pgx/v5/pgxpool"
	httpSwagger "github.com/swaggo/http-swagger/v2"
)

// @title Order Status Service API
// @version 1.0
// @description Microservice for tracking current order statuses with PostgreSQL, NATS JetStream and Redis.
// @host localhost:8080
// @BasePath /
func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	// Главный context приложения.
	// Отменится при Ctrl+C или SIGTERM.
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// PostgreSQL
	db, err := pgxpool.New(
		ctx,
		"postgres://orders:orders@localhost:5436/orders?sslmode=disable",
	)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := db.Ping(ctx); err != nil {
		return err
	}

	log.Println("PostgreSQL connected")

	// NATS JetStream
	natsClient, err := natsclient.New(ctx)
	if err != nil {
		return err
	}

	defer func() {
		if err := natsClient.Close(); err != nil {
			log.Printf("NATS shutdown error: %v", err)
		}
	}()

	log.Println("NATS JetStream connected")

	// Redis
	redisClient, err := cache.NewRedisClient(ctx)
	if err != nil {
		return err
	}
	defer redisClient.Close()

	log.Println("Redis connected")

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	defer cancelWorker()

	// Pull Consumer
	orderConsumer, err := worker.NewOrderConsumer(
		ctx,
		natsClient,
		redisClient,
	)
	if err != nil {
		return err
	}

	log.Println("JetStream consumer created")

	// Канал, через который узнаем,
	// что consumer полностью завершился.
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
		Addr:    ":8080",
		Handler: mux,
	}

	serverErr := make(chan error, 1)
	go func() {
		log.Println("server started on :8080")

		serverErr <- server.ListenAndServe()
	}()

	// Ждём Ctrl+C / SIGTERM
	var serveErr error
	select {
	case <-ctx.Done():
	case err := <-serverErr:
		if err != http.ErrServerClosed {
			serveErr = err
		}
	}
	stop()

	log.Println("shutting down...")

	// Даём HTTP-запросам до 5 секунд на завершение.
	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP shutdown error: %v", err)
		if err := server.Close(); err != nil {
			log.Printf("HTTP close error: %v", err)
		}
	}

	// Останавливаем consumer до закрытия используемых им подключений.
	cancelWorker()
	<-workerDone
	log.Println("consumer stopped")

	if err := natsClient.Close(); err != nil {
		log.Printf("NATS shutdown error: %v", err)
	}

	log.Println("server stopped")
	return serveErr
}
