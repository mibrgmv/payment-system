package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/mibrgmv/go-platform/postgres"
	"github.com/mibrgmv/payment-system/account/internal/config"
	"github.com/mibrgmv/payment-system/account/internal/server"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatal("Failed to load config:", err)
	}

	pool, err := postgres.NewPostgresPool(ctx, cfg.Postgres)
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer pool.Close()

	migrationPath := filepath.Join("migrations")
	if err := postgres.MigrateUp(cfg.Postgres.ConnectionStringURL(), migrationPath); err != nil {
		log.Fatal("Failed to run migrations:", err)
	}

	s := server.NewGrpcServer(pool)
	lis, err := net.Listen("tcp", cfg.Server.Addr())
	if err != nil {
		log.Fatal("Failed to listen:", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Printf("gRPC server starting on %s", cfg.Server.Addr())
		if err := s.Serve(lis); err != nil {
			log.Fatal("Failed to serve gRPC:", err)
		}
	}()

	sig := <-sigCh
	log.Printf("gRPC server shutting down. received signal: %v", sig)
	s.GracefulStop()
}
