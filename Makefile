BINARY        := dragon-cluster-resp-proxy
MODULE        := github.com/barestack/dragon-cluster-resp-proxy
VERSION       ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
GO            ?= go
GOFLAGS       ?=
LDFLAGS       := -s -w -X $(MODULE)/internal/version.Version=$(VERSION)
BIN_DIR       := bin

.PHONY: build test test-race test-integration lint vet fmt bench run release-snapshot ci

build:
	@mkdir -p $(BIN_DIR)
	$(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/$(BINARY) ./cmd/$(BINARY)

test:
	$(GO) test $(GOFLAGS) ./...

test-race:
	$(GO) test $(GOFLAGS) -race ./...

test-integration:
	$(GO) test $(GOFLAGS) -tags=integration -count=1 -timeout 10m ./test/integration/...

lint:
	golangci-lint run ./...

vet:
	$(GO) vet ./...

fmt:
	$(GO) fmt ./...

bench:
	$(GO) test $(GOFLAGS) -bench=. -benchmem -count=1 ./internal/resp ./internal/cluster ./internal/command

run: build
	$(BIN_DIR)/$(BINARY) --config configs/config.yaml

release-snapshot:
	goreleaser release --snapshot --clean

ci: vet test-race
