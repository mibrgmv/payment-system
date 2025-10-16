.PHONY: ling gen test-kafka test

lint:
	golangci-lint run

gen:
	@echo "Generating account service protobuf..."
	@mkdir -p services/account/internal/protogen/account
	protoc -I=services/account/api -I=shared/third_party \
		--go_out=services/account/internal/protogen/account \
		--go_opt=paths=source_relative \
		--go-grpc_out=services/account/internal/protogen/account \
		--go-grpc_opt=paths=source_relative \
		services/account/api/*.proto

test-kafka:
	@echo "Running account service /kafka tests..."
	cd services/account/internal/kafka/consumer_handlers && go test -v ./...
	cd services/account/internal/kafka/producer_handlers && go test -v ./...

test: test-kafka
