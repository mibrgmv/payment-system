.PHONY: lint gen

GO_MODULE=github.com/mibrgmv/payment-system/gateway
PROTO_DIR=api/v1

lint:
	golangci-lint run

gen:
	protoc -I=$(PROTO_DIR) -I=../third_party \
		--go_out=. \
		--go_opt=module=$(GO_MODULE) \
		--go-grpc_out=. \
		--go-grpc_opt=module=$(GO_MODULE) \
		--grpc-gateway_out=. \
		--grpc-gateway_opt=module=$(GO_MODULE) \
		--openapiv2_out=$(PROTO_DIR) \
		--openapiv2_opt=allow_merge=true,merge_file_name=gateway \
		$(PROTO_DIR)/*.proto
