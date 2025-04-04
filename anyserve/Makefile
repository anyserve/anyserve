.DEFAULT_GOAL := help

anyserve-build:
	@go version
	@echo "GOPATH: $(GOPATH)"
	@echo "Building anyserve with verbose output..."
	go build -v -o bin/anyserve cmd/anyserve/main.go

clean:
	rm -f bin/servelet bin/anyserve

build-all: anyserve-build

lint:
	golangci-lint run --config=.golangci.yml ./...

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

anyserve-dev: anyserve-build
	bin/anyserve


help:
	@echo "Available commands:"
	@echo "  build-all      - Build all programs"
	@echo "  anyserve-build - Build anyserve"
	@echo "  anyserve-dev   - Build and run anyserve"
	@echo "  clean          - Clean build files"
	@echo "  fmt            - Format code"
	@echo "  test           - Run tests"
	@echo "  upgrade        - Upgrade dependencies"
	@echo "  lint           - Run lint"

.PHONY: anyserve-dev anyserve-build clean build-all test fmt help
