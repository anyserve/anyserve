.DEFAULT_GOAL := help

build:
	@go version
	@echo "GOPATH: $(GOPATH)"
	@echo "Building anyserve with verbose output..."
	go build -v -o bin/anyserve cmd/anyserve/main.go

clean:
	rm -rf bin/anyserve

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
	bin/anyserve

help:
	@echo "Available commands:"
	@echo "  build - Build anyserve"
	@echo "  dev   - Build and run anyserve"
	@echo "  clean - Clean build files"
	@echo "  fmt   - Format code"
	@echo "  test  - Run tests"
	@echo "  upgrade - Upgrade dependencies"
	@echo "  lint  - Run lint"

.PHONY: dev build clean lint test fmt upgrade help
