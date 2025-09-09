SERVICES := account transaction gateway

.PHONY: help proto-all proto-clean $(addprefix proto-,$(SERVICES))

help:
	@echo "Available targets:"
	@echo "  make deps              - Install dependencies"
	@echo "  make proto-all         - Generate all protobuf code"
	@echo "  make proto-account     - Generate account service protobuf"
	@echo "  make proto-transaction - Generate transaction service protobuf"

deps:
	@echo "Installing Go protobuf tools..."
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@latest
	go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@latest

proto-all: proto-account proto-transaction proto-gateway

proto-account:
	@echo "Generating account service protobuf..."
	@mkdir -p services/account/internal/protogen/account
	protoc -I=services/account/api -I=third_party \
		--go_out=services/account/internal/protogen/account \
		--go_opt=paths=source_relative \
		--go-grpc_out=services/account/internal/protogen/account \
		--go-grpc_opt=paths=source_relative \
		services/account/api/*.proto

proto-transaction:
	@echo "Generating transaction service protobuf..."
	@mkdir -p services/transaction/internal/protogen/transaction
	protoc -I=services/transaction/api -I=third_party \
		--go_out=services/transaction/internal/protogen/transaction \
		--go_opt=paths=source_relative \
		--go-grpc_out=services/transaction/internal/protogen/transaction \
		--go-grpc_opt=paths=source_relative \
		services/transaction/api/*.proto

proto-gateway:
	@echo "Generating gateway service protobuf..."
	@mkdir -p services/gateway/internal/protogen
	protoc -I=services/gateway/api -I=third_party \
		--go_out=services/gateway/internal/protogen \
		--go_opt=paths=source_relative \
		--go-grpc_out=services/gateway/internal/protogen \
		--go-grpc_opt=paths=source_relative \
		--grpc-gateway_out=services/gateway/internal/protogen \
		--grpc-gateway_opt=paths=source_relative \
		--openapiv2_out=services/gateway/api \
		--openapiv2_opt=allow_merge=true,merge_file_name=gateway \
		services/gateway/api/*.proto