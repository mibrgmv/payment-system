package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/mibrgmv/payment-service/services/account/internal/config"
	"github.com/mibrgmv/payment-service/services/account/internal/server"
	"github.com/mibrgmv/payment-service/shared/postgres"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var cfg config.Config
	err := config.Load(&cfg)
	if err != nil {
		log.Fatal("Failed to load config:", err)
	}

	pool, err := postgres.NewPostgresPool(ctx, cfg.Postgres)
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer pool.Close()

	publisher := server.SetupKafkaPublisher(pool, cfg.Kafka)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Println("starting kafka outbox publisher...")
		publisher.Start(ctx)
	}()

	sig := <-sigCh
	log.Printf("Kafka publisher shutting down. Received signal: %v", sig)
	cancel()
}
