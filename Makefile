.PHONY: build test check-connections
build:
	@test "$$(go env GOVERSION)" = "go1.27.3" || (echo 'Use Go 1.27.3'; exit 1)
	go build -o bin/ethvis-collector ./cmd/ethvis-collector

test:
	@test "$$(go env GOVERSION)" = "go1.27.3" || (echo 'Use Go 1.27.3'; exit 1)
	@test "$$(node --version)" = "v24.22.1" || (echo 'Use Node.js 24.22.1'; exit 1)
	go test -race ./...
	go vet ./...
	node --test analysis/test/*.test.mjs

check-connections:
	scripts/check_connections.sh
