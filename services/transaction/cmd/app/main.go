package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
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

	migrationPath := filepath.Join("internal", "migrations")
	if err := postgres.MigrateUp(pool, migrationPath); err != nil {
		log.Fatal("Failed to run migrations:", err)
	}

	//kafkaProcessor := server.SetupKafkaProcessor(pool, cfg.Kafka)
	//if err := kafkaProcessor.Start(ctx); err != nil {
	//	log.Fatal("Failed to start Kafka processor:", err)
	//}
	//
	//kafkaPublisher := server.SetupKafkaPublisher(pool, cfg.Kafka)
	//kafkaPublisher.Start(ctx)

	s := server.NewGrpcServer(pool)
	lis, err := net.Listen("tcp", cfg.Server.GetAddr())
	if err != nil {
		log.Fatal("Failed to listen:", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Printf("Transaction gRPC server starting on %s", cfg.Server.GetAddr())
		if err := s.Serve(lis); err != nil {
			log.Fatal("Failed to serve gRPC:", err)
		}
	}()

	sig := <-sigCh
	log.Printf("Transaction gRPC server shutting down. received signal: %v", sig)
	s.GracefulStop()
}
