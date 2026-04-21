.PHONY: ling gen test-kafka test

lint:
	golangci-lint run

gen:
	@echo "Generating account service protobuf..."
	protoc -I=api/v1 -I=../../third_party \
		--go_out=. \
		--go_opt=module=github.com/mibrgmv/payment-system/account \
		--go-grpc_out=. \
		--go-grpc_opt=module=github.com/mibrgmv/payment-system/account \
		api/v1/*.proto

test-kafka:
	@echo "Running account service /kafka tests..."
	cd internal/kafka/consumer_handlers && go test -v ./...
	cd internal/kafka/producer_handlers && go test -v ./...

test: test-kafka
