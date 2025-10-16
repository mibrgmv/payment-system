.PHONY: ling gen test-kafka test

lint:
	golangci-lint run

gen:
	@echo "Generating transaction service protobuf..."
	@mkdir -p services/transaction/internal/protogen/transaction
	protoc -I=services/transaction/api -I=shared/third_party \
		--go_out=services/transaction/internal/protogen/transaction \
		--go_opt=paths=source_relative \
		--go-grpc_out=services/transaction/internal/protogen/transaction \
		--go-grpc_opt=paths=source_relative \
		services/transaction/api/*.proto

test-kafka:
	@echo "Running transaction service /kafka tests..."
	cd services/transaction/internal/kafka/consumer_handlers && go test -v ./...
	cd services/transaction/internal/kafka/producer_handlers && go test -v ./...

test: test-kafka
