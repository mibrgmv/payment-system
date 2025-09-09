package server

import (
	"github.com/jackc/pgx/v5/pgxpool"
	accountgrpc "github.com/mibrgmv/payment-service/services/account/internal/grpc"
	accountv1 "github.com/mibrgmv/payment-service/services/account/internal/protogen/account"
	"github.com/mibrgmv/payment-service/services/account/internal/repository/postgres"
	"github.com/mibrgmv/payment-service/services/account/internal/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func NewGrpcServer(pool *pgxpool.Pool) *grpc.Server {
	accountRepo := postgres.NewAccountRepository(pool)
	balanceRepo := postgres.NewBalanceRepository(pool)

	accountService := service.NewAccountService(accountRepo, balanceRepo)

	server := grpc.NewServer()

	grpcServer := accountgrpc.NewAccountServiceServer(accountService)
	accountv1.RegisterAccountServiceServer(server, grpcServer)

	reflection.Register(server)

	return server
}
