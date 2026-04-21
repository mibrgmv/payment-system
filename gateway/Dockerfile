FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o gateway ./cmd/gateway

FROM alpine:latest AS gateway
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /app/gateway .
COPY --from=builder /app/config.yaml ./config.yaml
COPY --from=builder /app/api/v1/gateway.swagger.json ./api/v1
EXPOSE 8080
CMD ["./gateway"]
