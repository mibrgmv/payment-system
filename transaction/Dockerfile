FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o transaction-service ./cmd/app
RUN CGO_ENABLED=0 GOOS=linux go build -o transaction-kafka-processor ./cmd/kafka-processor
RUN CGO_ENABLED=0 GOOS=linux go build -o transaction-kafka-publisher ./cmd/kafka-publisher

FROM alpine:latest AS transaction-service
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /app/transaction-service .
COPY --from=builder /app/config.yaml ./config.yaml
COPY --from=builder /app/migrations ./migrations
EXPOSE 50052
CMD ["./transaction-service"]

FROM alpine:latest AS transaction-kafka-processor
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /app/transaction-kafka-processor .
COPY --from=builder /app/config.yaml ./config.yaml
CMD ["./transaction-kafka-processor"]

FROM alpine:latest AS transaction-kafka-publisher
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /app/transaction-kafka-publisher .
COPY --from=builder /app/config.yaml ./config.yaml
CMD ["./transaction-kafka-publisher"]
