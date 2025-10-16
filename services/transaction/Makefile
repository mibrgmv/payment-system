.PHONY: ling gen test-kafka test

lint:
	golangci-lint run

gen:
	@echo "Generating transaction service protobuf..."
	protoc -I=api/v1 -I=../../shared/third_party \
		--go_out=. \
		--go_opt=module=github.com/mibrgmv/payment-system/transaction \
		--go-grpc_out=. \
		--go-grpc_opt=module=github.com/mibrgmv/payment-system/transaction \
		api/v1/*.proto

test-kafka:
	@echo "Running transaction service /kafka tests..."
	cd internal/kafka/consumer_handlers && go test -v ./...
	cd internal/kafka/producer_handlers && go test -v ./...

test: test-kafka
