package server

import (
	"github.com/jackc/pgx/v5/pgxpool"
	transactiongrpc "github.com/mibrgmv/payment-service/services/transaction/internal/grpc"
	transactionv1 "github.com/mibrgmv/payment-service/services/transaction/internal/protogen/transaction"
	"github.com/mibrgmv/payment-service/services/transaction/internal/repository/postgres"
	"github.com/mibrgmv/payment-service/services/transaction/internal/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func NewGrpcServer(pool *pgxpool.Pool) *grpc.Server {
	transactionRepo := postgres.NewTransactionRepository(pool)
	outboxRepo := postgres.NewOutboxRepository(pool)
	transactionService := service.NewTransactionService(transactionRepo, outboxRepo)

	server := grpc.NewServer()
	grpcServer := transactiongrpc.NewTransactionServer(transactionService)
	transactionv1.RegisterTransactionServiceServer(server, grpcServer)
	reflection.Register(server)

	return server
}
