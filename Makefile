.PHONY: \
	help deps \
	gen gen-gateway gen-account gen-transaction \
	test test-account test-transaction test-shared

help:
	@echo "Available targets:"
	@echo "  make deps                    - Install dependencies"
	@echo "  make gen                     - Generate all protobuf code"
	@echo "  make gen-gateway             - Generate gateway protobuf"
	@echo "  make gen-account             - Generate account service protobuf"
	@echo "  make gen-transaction         - Generate transaction service protobuf"
	@echo "  make test                    - Run all tests"
	@echo "  make test-account            - Run account service tests"
	@echo "  make test-transaction        - Run transaction service tests"
	@echo "  make test-shared             - Run shared component tests"

deps:
	@echo "Installing Go protobuf tools..."
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@latest
	go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@latest

gen: gen-gateway gen-account gen-transaction

gen-gateway:
	@echo "Generating gateway service protobuf..."
	cd gateway && make gen

gen-account:
	@echo "Generating account service protobuf..."
	cd account && make gen

gen-transaction:
	@echo "Generating transaction service protobuf..."
	cd transaction && make gen

test: test-account test-transaction test-shared

test-account:
	@echo "Running account service tests..."
	cd account && make test

test-transaction:
	@echo "Running transaction service tests..."
	cd transaction && make test

test-shared:
	@echo "Running shared module tests..."
	cd shared && make test
