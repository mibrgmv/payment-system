module github.com/mibrgmv/payment-service/services/gateway

go 1.24.0

replace github.com/mibrgmv/payment-service/shared => ../../shared

require (
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.27.2
	github.com/joho/godotenv v1.5.1
	github.com/mibrgmv/payment-service/shared v0.0.0-00010101000000-000000000000
	github.com/swaggo/http-swagger v1.3.4
	google.golang.org/genproto/googleapis/api v0.0.0-20250908214217-97024824d090
	google.golang.org/grpc v1.75.0
	google.golang.org/protobuf v1.36.9
)

require (
	github.com/KyleBanks/depth v1.2.1 // indirect
	github.com/go-openapi/jsonpointer v0.19.5 // indirect
	github.com/go-openapi/jsonreference v0.20.0 // indirect
	github.com/go-openapi/spec v0.20.6 // indirect
	github.com/go-openapi/swag v0.19.15 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/mailru/easyjson v0.7.6 // indirect
	github.com/stretchr/testify v1.11.1 // indirect
	github.com/swaggo/files v1.0.1 // indirect
	github.com/swaggo/swag v1.8.12 // indirect
	golang.org/x/net v0.44.0 // indirect
	golang.org/x/sys v0.36.0 // indirect
	golang.org/x/text v0.29.0 // indirect
	golang.org/x/tools v0.36.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250826171959-ef028d996bc1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
