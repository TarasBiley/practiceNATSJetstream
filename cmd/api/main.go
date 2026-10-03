package main

import (
	"context"
	"fmt"
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
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Ping(ctx); err != nil {
		log.Fatal(err)
	}

	fmt.Println("PostgreSQL connected")

	// NATS JetStream
	natsClient, err := natsclient.New(ctx)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("NATS JetStream connected")

	// Redis
	redisClient, err := cache.NewRedisClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer redisClient.Close()

	fmt.Println("Redis connected")

	// Pull Consumer
	orderConsumer, err := worker.NewOrderConsumer(
		ctx,
		natsClient,
		redisClient,
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("JetStream consumer created")

	// Канал, через который узнаем,
	// что consumer полностью завершился.
	workerDone := make(chan struct{})

	go func() {
		defer close(workerDone)

		orderConsumer.Run(ctx)
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

	go func() {
		fmt.Println("server started on :8080")

		err := server.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			log.Printf("server error: %v", err)
		}
	}()

	// Ждём Ctrl+C / SIGTERM
	<-ctx.Done()

	fmt.Println("shutting down...")

	// Даём HTTP-запросам до 5 секунд на завершение.
	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP shutdown error: %v", err)
	}

	// Ждём завершения consumer.
	select {
	case <-workerDone:
		fmt.Println("consumer stopped")

	case <-time.After(3 * time.Second):
		log.Println("consumer shutdown timeout")
	}

	// Корректно закрываем NATS.
	if err := natsClient.Close(); err != nil {
		log.Printf("NATS shutdown error: %v", err)
	}

	fmt.Println("server stopped")
}
