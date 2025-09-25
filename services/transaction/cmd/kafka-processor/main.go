package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/mibrgmv/payment-service/services/transaction/internal/config"
	"github.com/mibrgmv/payment-service/services/transaction/internal/server"
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

	processor := server.SetupKafkaProcessor(pool, cfg.Kafka)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Printf("starting kafka event processor...")
		if err := processor.Start(ctx); err != nil {
			log.Printf("Kafka processor error: %v", err)
		}
	}()

	sig := <-sigCh
	log.Printf("Kafka processor shutting down. Received signal: %v", sig)
	cancel()
}
