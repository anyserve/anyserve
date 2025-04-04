.DEFAULT_GOAL := help

# Get module path
MODULE_PATH := $(shell go list -m)
# Determine the package path for version variables
VERSION_PKG := $(MODULE_PATH)/internal/version

# Get version information (adjust VERSION logic if needed, e.g., from a file)
VERSION ?= $(shell git describe --tags --always --dirty --match='v*' 2>/dev/null || echo "dev")
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_DATE := $(shell date -u +'%Y-%m-%dT%H:%M:%SZ')
GO_VERSION := $(shell go version | awk '{print $$3}')

# Build flags
LDFLAGS := -s -w \
           -X '$(VERSION_PKG).Version=$(VERSION)' \
           -X '$(VERSION_PKG).GitCommit=$(GIT_COMMIT)' \
           -X '$(VERSION_PKG).BuildDate=$(BUILD_DATE)' \
           -X '$(VERSION_PKG).GoVersion=$(GO_VERSION)'

BINARY_NAME := anyserve
BINARY_PATH := bin/$(BINARY_NAME)
CMD_PATH := cmd/anyserve/main.go

build:
	@echo ">>> Building $(BINARY_NAME) version $(VERSION) ($(GIT_COMMIT))..."
	@mkdir -p bin
	go build -ldflags="$(LDFLAGS)" -v -o $(BINARY_PATH) $(CMD_PATH)

clean:
	rm -rf $(BINARY_PATH)

lint:
	golangci-lint run --config=../.golangci.yml ./...

test:
ifdef COVERAGE
	go test -v -race -coverprofile=coverage.txt -covermode=atomic ./...
else
	go test -v -race ./...
endif

fmt:
	go fmt ./...

upgrade:
	go get -u ./... && go mod tidy

dev: build
	$(BINARY_PATH)

help:
	@echo "Available commands:"
	@echo "  build    - Build $(BINARY_NAME) with version information"
	@echo "  dev      - Build and run $(BINARY_NAME)"
	@echo "  clean    - Clean build files"
	@echo "  fmt      - Format code"
	@echo "  test     - Run tests"
	@echo "  upgrade  - Upgrade dependencies"
	@echo "  lint     - Run lint"

.PHONY: dev build clean lint test fmt upgrade help
