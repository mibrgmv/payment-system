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

	kafkaProcessor := server.SetupKafkaProcessor(pool, cfg.Kafka)
	if err := kafkaProcessor.Start(ctx); err != nil {
		log.Fatal("Failed to start Kafka processor:", err)
	}

	log.Println("Kafka event processor started successfully")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigCh
	log.Printf("Kafka processor shutting down. Received signal: %v", sig)
	cancel()
}
