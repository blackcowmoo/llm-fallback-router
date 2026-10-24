BINARY_DIR := bin
ROUTER_BIN := $(BINARY_DIR)/llm-router
MOCK_BIN := $(BINARY_DIR)/mock-llm

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -s -w \
	-X github.com/blackcowmoo/llm-fallback-router/internal/version.Version=$(VERSION) \
	-X github.com/blackcowmoo/llm-fallback-router/internal/version.Commit=$(COMMIT) \
	-X github.com/blackcowmoo/llm-fallback-router/internal/version.BuildDate=$(BUILD_DATE)

.PHONY: all build mock test race vet fmt fmt-check lint cover docker-build compose-up compose-down e2e clean

all: build

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o $(ROUTER_BIN) ./cmd/llm-router

mock:
	CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o $(MOCK_BIN) ./cmd/mock-llm

test:
	go test -count=1 ./...

race:
	go test -race -shuffle=on -count=1 ./...

vet:
	go vet ./...

fmt:
	gofmt -w cmd internal

fmt-check:
	@unformatted="$$(gofmt -l cmd internal)"; \
	if [ -n "$$unformatted" ]; then \
		echo "gofmt required for:"; echo "$$unformatted"; exit 1; \
	fi

lint:
	golangci-lint run

cover:
	go test -race -coverprofile=coverage.out -covermode=atomic ./...
	go tool cover -func=coverage.out

docker-build:
	docker build --build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) --build-arg BUILD_DATE=$(BUILD_DATE) -t llm-fallback-router:local .
	docker build -t llm-fallback-router-mock:local -f Dockerfile.mock .

compose-up:
	docker compose up -d --build

compose-down:
	docker compose down -v --remove-orphans

e2e:
	scripts/e2e.sh

clean:
	rm -rf $(BINARY_DIR) dist coverage.out
