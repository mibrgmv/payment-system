package server

import (
	"github.com/jackc/pgx/v5/pgxpool"
	transactiongrpc "github.com/mibrgmv/payment-system/services/transaction/internal/grpc"
	transactionv1 "github.com/mibrgmv/payment-system/services/transaction/internal/protogen/transaction/v1"
	"github.com/mibrgmv/payment-system/services/transaction/internal/repository/postgres"
	"github.com/mibrgmv/payment-system/services/transaction/internal/service"
	"github.com/mibrgmv/payment-system/shared/outbox"
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
