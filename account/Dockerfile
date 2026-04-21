FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o account-service ./cmd/app
RUN CGO_ENABLED=0 GOOS=linux go build -o account-kafka-processor ./cmd/kafka-processor
RUN CGO_ENABLED=0 GOOS=linux go build -o account-kafka-publisher ./cmd/kafka-publisher

FROM alpine:latest AS account-service
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /app/account-service .
COPY --from=builder /app/config.yaml ./config.yaml
COPY --from=builder /app/migrations ./migrations
EXPOSE 50052
CMD ["./account-service"]

FROM alpine:latest AS account-kafka-processor
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /app/account-kafka-processor .
COPY --from=builder /app/config.yaml ./config.yaml
CMD ["./account-kafka-processor"]

FROM alpine:latest AS account-kafka-publisher
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /app/account-kafka-publisher .
COPY --from=builder /app/config.yaml ./config.yaml
CMD ["./account-kafka-publisher"]
