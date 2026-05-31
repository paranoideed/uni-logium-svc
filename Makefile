CONFIG_FILE := ./config.yaml

build:
	KV_VIPER_FILE=$(CONFIG_FILE) go build -o ./cmd/uni-logium-svc/main ./cmd/uni-logium-svc/main.go

run-server:
	KV_VIPER_FILE=$(CONFIG_FILE) go build -o ./cmd/uni-logium-svc/main ./cmd/uni-logium-svc/main.go
	set -a && . ./.env && set +a && KV_VIPER_FILE=$(CONFIG_FILE) ./cmd/uni-logium-svc/main run service
