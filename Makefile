SERVICES := account transaction gateway
PROTO_DIRS := $(foreach service,$(SERVICES),services/$(service)/proto)

.PHONY: help proto-all proto-clean proto-lint $(addprefix proto-,$(PROTO_SERVICES))

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

proto-all: $(addprefix proto-,$(PROTO_SERVICES))

proto-account:
	@echo "Generating account service protobuf..."
	@mkdir -p services/account/internal/protogen/account/v1
	protoc -I=services/account/api/v1 -I=third_party \
		--go_out=services/account/internal/protogen/account/v1 \
		--go_opt=paths=source_relative \
		--go-grpc_out=services/account/internal/protogen/account/v1 \
		--go-grpc_opt=paths=source_relative \
		--grpc-gateway_out=services/account/internal/protogen/account/v1 \
		--grpc-gateway_opt=paths=source_relative \
		--openapiv2_out=services/account/api/v1 \
		--openapiv2_opt=allow_merge=true,merge_file_name=account \
		services/account/api/v1/*.proto