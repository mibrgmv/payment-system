package server

import (
	"context"
	"fmt"
	"net/http"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/mibrgmv/payment-system/gateway/internal/config"
	accountv1 "github.com/mibrgmv/payment-system/gateway/internal/protogen/account/v1"
	transactionv1 "github.com/mibrgmv/payment-system/gateway/internal/protogen/transaction/v1"
	"github.com/swaggo/http-swagger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func NewHttpServer(ctx context.Context, config config.Config) (*http.Server, error) {
	gwmux := runtime.NewServeMux()

	if err := accountv1.RegisterAccountServiceHandlerFromEndpoint(
		ctx,
		gwmux,
		config.Services.Account.GetAddr(),
		[]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	); err != nil {
		return nil, fmt.Errorf("failed to register account service: %w", err)
	}

	if err := transactionv1.RegisterTransactionServiceHandlerFromEndpoint(
		ctx,
		gwmux,
		config.Services.Transaction.GetAddr(),
		[]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	); err != nil {
		return nil, fmt.Errorf("failed to register transaction service: %w", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/", gwmux)

	mux.HandleFunc("/api/v1/swagger.json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		http.ServeFile(w, r, "./api/v1/gateway.swagger.json")
	})

	mux.Handle("/swagger/", httpSwagger.Handler(
		httpSwagger.URL("/api/v1/swagger.json"),
	))

	return &http.Server{
		Addr:    config.Server.GetAddr(),
		Handler: mux,
	}, nil
}
