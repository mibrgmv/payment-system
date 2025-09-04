package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	accountgrpc "github.com/mibrgmv/payment-service/services/account/internal/presentation/grpc"
	accountv1 "github.com/mibrgmv/payment-service/services/account/internal/protogen/account"
	"github.com/mibrgmv/payment-service/services/account/internal/repository"
	"github.com/mibrgmv/payment-service/services/account/internal/service"
	"github.com/mibrgmv/payment-service/shared/db/postgres"
)

func main() {
	ctx := context.Background()

	dbCfg := postgres.DefaultConfig("postgres://bill_clinton:2001@localhost:5432/account_service")
	pool, err := postgres.NewPostgresPool(ctx, dbCfg)
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer pool.Close()

	migrationPath := filepath.Join("migrations")
	if err := postgres.RunMigrations(ctx, pool, migrationPath); err != nil {
		log.Fatal("Failed to run migrations:", err)
	}

	accountRepo := repository.NewAccountRepository(pool)
	balanceRepo := repository.NewBalanceRepository(pool)

	accountService := service.NewAccountService(accountRepo, balanceRepo)

	grpcServer := accountgrpc.NewAccountServiceServer(accountService)
	server := grpc.NewServer()
	accountv1.RegisterAccountServiceServer(server, grpcServer)
	reflection.Register(server)

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal("Failed to listen:", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Println("gRPC server starting on :50051")
		if err := server.Serve(lis); err != nil {
			log.Fatal("Failed to serve gRPC:", err)
		}
	}()

	sig := <-sigCh
	log.Printf("gRPC server shutting down. received signal: %v", sig)
	server.GracefulStop()
}
