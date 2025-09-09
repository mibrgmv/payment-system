package server

import (
	"context"
	_ "embed"
	"fmt"
	"net/http"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/mibrgmv/payment-service/services/gateway/internal/config"
	accountpb "github.com/mibrgmv/payment-service/services/gateway/internal/protogen/account"
	transactionpb "github.com/mibrgmv/payment-service/services/gateway/internal/protogen/transaction"
	"github.com/swaggo/http-swagger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

//go:embed gateway.swagger.json
var swaggerJSON []byte

func NewHttpServer(ctx context.Context, config config.Config) (*http.Server, error) {
	gwmux := runtime.NewServeMux()

	if err := accountpb.RegisterAccountServiceHandlerFromEndpoint(
		ctx,
		gwmux,
		config.Services.Account.GetAddr(),
		[]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	); err != nil {
		return nil, fmt.Errorf("failed to register account service: %w", err)
	}

	if err := transactionpb.RegisterTransactionServiceHandlerFromEndpoint(
		ctx,
		gwmux,
		config.Services.Transaction.GetAddr(),
		[]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	); err != nil {
		return nil, fmt.Errorf("failed to register transaction service: %w", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/", gwmux)

	mux.HandleFunc("/swagger.json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(swaggerJSON)
	})

	mux.Handle("/swagger/", httpSwagger.Handler(
		httpSwagger.URL("/swagger.json"),
	))

	return &http.Server{
		Addr:    config.Server.GetAddr(),
		Handler: mux,
	}, nil
}
