.DEFAULT_GOAL := help

servelet-build:
	@go version
	@echo "GOPATH: $(GOPATH)"
	@echo "Building servelet with verbose output..."
	go build -v -o bin/servelet cmd/servelet/main.go

anyserve-build:
	@go version
	@echo "GOPATH: $(GOPATH)"
	@echo "Building anyserve with verbose output..."
	go build -v -o bin/anyserve cmd/anyserve/main.go

clean:
	rm -f bin/servelet bin/anyserve

build-all: servelet-build anyserve-build

test:
	go test -v ./...

fmt:
	go fmt ./...

upgrade:
	go get -u ./... && go mod tidy

servelet-dev: servelet-build
	bin/servelet

anyserve-dev: anyserve-build
	bin/anyserve


help:
	@echo "Available commands:"
	@echo "  build-all      - Build all programs"
	@echo "  anyserve-build - Build anyserve"
	@echo "  anyserve-dev   - Build and run anyserve"
	@echo "  servelet-build - Build servelet"
	@echo "  servelet-dev   - Build and run servelet"
	@echo "  clean          - Clean build files"
	@echo "  fmt            - Format code"
	@echo "  test           - Run tests"
	@echo "  upgrade        - Upgrade dependencies"

.PHONY: servelet-dev servelet-build anyserve-dev anyserve-build clean build-all test fmt help
