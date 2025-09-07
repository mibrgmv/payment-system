module github.com/mibrgmv/payment-service/services/account

go 1.24.0

require (
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.27.2
	github.com/jackc/pgx/v5 v5.7.5
	github.com/joho/godotenv v1.5.1
	github.com/mibrgmv/payment-service/shared v0.0.0-00010101000000-000000000000
	google.golang.org/genproto/googleapis/api v0.0.0-20250826171959-ef028d996bc1
	google.golang.org/grpc v1.75.0
	google.golang.org/protobuf v1.36.8
)

require (
	github.com/golang-migrate/migrate/v4 v4.19.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/lib/pq v1.10.9 // indirect
	golang.org/x/crypto v0.39.0 // indirect
	golang.org/x/net v0.41.0 // indirect
	golang.org/x/sync v0.16.0 // indirect
	golang.org/x/sys v0.33.0 // indirect
	golang.org/x/text v0.28.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250818200422-3122310a409c // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/mibrgmv/payment-service/shared => ../../shared
