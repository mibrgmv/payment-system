.PHONY: ling gen test-kafka test

lint:
	golangci-lint run

gen:
	@echo "Generating account service protobuf..."
	protoc -I=api/v1 -I=../../shared/third_party \
		--go_out=. \
		--go_opt=module=github.com/mibrgmv/payment-system/services/account \
		--go-grpc_out=. \
		--go-grpc_opt=module=github.com/mibrgmv/payment-system/services/account \
		api/v1/*.proto

test-kafka:
	@echo "Running account service /kafka tests..."
	cd services/account/internal/kafka/consumer_handlers && go test -v ./...
	cd services/account/internal/kafka/producer_handlers && go test -v ./...

test: test-kafka
