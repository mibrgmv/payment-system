package server

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mibrgmv/go-platform/outbox"
	transactiongrpc "github.com/mibrgmv/payment-system/transaction/internal/grpc"
	"github.com/mibrgmv/payment-system/transaction/internal/repository/postgres"
	"github.com/mibrgmv/payment-system/transaction/internal/service"
	transactionv1 "github.com/mibrgmv/payment-system/transaction/pkg/protogen/transaction/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func NewGrpcServer(pool *pgxpool.Pool) *grpc.Server {
	transactionRepo := postgres.NewTransactionRepository(pool)
	outboxRepo := outbox.NewPostgresRepository(pool)
	transactionService := service.NewTransactionService(transactionRepo, outboxRepo)

	server := grpc.NewServer()
	grpcServer := transactiongrpc.NewTransactionServer(transactionService)
	transactionv1.RegisterTransactionServiceServer(server, grpcServer)
	reflection.Register(server)

	return server
}
