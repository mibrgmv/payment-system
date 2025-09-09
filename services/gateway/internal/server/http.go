package server

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/mibrgmv/payment-service/services/gateway/internal/config"
	accountpb "github.com/mibrgmv/payment-service/services/gateway/internal/protogen/account"
	transactionpb "github.com/mibrgmv/payment-service/services/gateway/internal/protogen/transaction"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func NewHttpServer(ctx context.Context, config config.Config) (*http.Server, error) {
	mux := runtime.NewServeMux()

	if err := accountpb.RegisterAccountServiceHandlerFromEndpoint(
		ctx,
		mux,
		config.Services.Account.GetAddr(),
		[]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	); err != nil {
		return nil, fmt.Errorf("failed to register account service: %w", err)
	}

	if err := transactionpb.RegisterTransactionServiceHandlerFromEndpoint(
		ctx,
		mux,
		config.Services.Transaction.GetAddr(),
		[]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	); err != nil {
		return nil, fmt.Errorf("failed to register transaction service: %w", err)
	}

	router := gin.Default()
	router.NoRoute(gin.WrapH(mux))

	return &http.Server{
		Addr:    config.Server.GetAddr(),
		Handler: router,
	}, nil
}
