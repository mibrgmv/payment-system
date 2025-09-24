SERVICES := account transaction gateway

.PHONY: \
	help deps \
	proto-all proto-clean $(addprefix proto-,$(SERVICES)) \
	test-account test-account-kafka \
	test-transaction test-transaction-kafka \
	test-gateway \
	test-shared test-shared-events \
	test

help:
	@echo "Available targets:"
	@echo "  make deps                    - Install dependencies"
	@echo "  make proto                   - Generate all protobuf code"
	@echo "  make proto-account           - Generate account service protobuf"
	@echo "  make proto-transaction       - Generate transaction service protobuf"
	@echo "  make test                    - Run all tests"
	@echo "  make test-account            - Run account service tests"
	@echo "  make test-account-kafka      - Run account Kafka handler tests"
	@echo "  make test-transaction        - Run transaction service tests"
	@echo "  make test-transaction-kafka  - Run transaction Kafka handler tests"
	@echo "  make test-gateway            - Run gateway service tests"
	@echo "  make test-shared             - Run shared component tests"
	@echo "  make test-shared-events      - Run shared events tests"

deps:
	@echo "Installing Go protobuf tools..."
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@latest
	go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@latest

proto: $(addprefix proto-,$(SERVICES))

proto-account:
	@echo "Generating account service protobuf..."
	@mkdir -p services/account/internal/protogen/account
	protoc -I=services/account/api -I=shared/third_party \
		--go_out=services/account/internal/protogen/account \
		--go_opt=paths=source_relative \
		--go-grpc_out=services/account/internal/protogen/account \
		--go-grpc_opt=paths=source_relative \
		services/account/api/*.proto

proto-transaction:
	@echo "Generating transaction service protobuf..."
	@mkdir -p services/transaction/internal/protogen/transaction
	protoc -I=services/transaction/api -I=shared/third_party \
		--go_out=services/transaction/internal/protogen/transaction \
		--go_opt=paths=source_relative \
		--go-grpc_out=services/transaction/internal/protogen/transaction \
		--go-grpc_opt=paths=source_relative \
		services/transaction/api/*.proto

proto-gateway:
	@echo "Generating gateway service protobuf..."
	@mkdir -p services/gateway/internal/protogen/account
	@mkdir -p services/gateway/internal/protogen/transaction
	protoc -I=services/gateway/api -I=shared/third_party \
		--go_out=services/gateway/internal/protogen/account \
		--go_opt=paths=source_relative \
		--go-grpc_out=services/gateway/internal/protogen/account \
		--go-grpc_opt=paths=source_relative \
		--grpc-gateway_out=services/gateway/internal/protogen/account \
		--grpc-gateway_opt=paths=source_relative \
		services/gateway/api/account.proto
	protoc -I=services/gateway/api -I=shared/third_party \
		--go_out=services/gateway/internal/protogen/transaction \
		--go_opt=paths=source_relative \
		--go-grpc_out=services/gateway/internal/protogen/transaction \
		--go-grpc_opt=paths=source_relative \
		--grpc-gateway_out=services/gateway/internal/protogen/transaction \
		--grpc-gateway_opt=paths=source_relative \
		services/gateway/api/transaction.proto
	protoc -I=services/gateway/api -I=shared/third_party \
		--openapiv2_out=services/gateway/api \
		--openapiv2_opt=allow_merge=true,merge_file_name=gateway \
		services/gateway/api/*.proto

test-account-kafka:
	@echo "Running account service /kafka tests..."
	cd services/account/internal/kafka/consumer_handlers && go test -v ./...
	cd services/account/internal/kafka/producer_handlers && go test -v ./...

test-account: test-account-kafka

test-transaction-kafka:
	@echo "Running transaction service /kafka tests..."
	cd services/transaction/internal/kafka/consumer_handlers && go test -v ./...
	cd services/transaction/internal/kafka/producer_handlers && go test -v ./...

test-transaction: test-transaction-kafka

test-gateway:
	@echo "Nothing to run for 'make test-gateway'..."

test-shared-events:
	@echo "Running shared /events tests..."
	cd shared/events && go test -v ./...

test-shared: test-shared-events

test: $(addprefix test-,$(SERVICES)) test-shared