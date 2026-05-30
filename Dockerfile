FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/uni-logium-svc ./cmd/uni-logium-svc

FROM alpine:3.21

COPY --from=builder /bin/uni-logium-svc /bin/uni-logium-svc
COPY --from=builder /app/config.yaml /config.yaml

ENV KV_VIPER_FILE=/config.yaml

ENTRYPOINT ["/bin/uni-logium-svc"]
CMD ["run", "service"]
