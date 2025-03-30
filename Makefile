servelet-dev: servelet-build
	bin/servelet

servelet-build:
	go build -o bin/servelet cmd/servelet/main.go

anyserve-dev: anyserve-build
	bin/anyserve

anyserve-build:
	go build -o bin/anyserve cmd/anyserve/main.go

clean:
	rm -f bin/servelet bin/anyserve

build-all: servelet-build anyserve-build

test:
	go test -v ./...

fmt:
	go fmt ./...

help:
	@echo "Available commands:"
	@echo "  servelet-dev   - Build and run servelet"
	@echo "  anyserve-dev   - Build and run anyserve"
	@echo "  build-all      - Build all programs"
	@echo "  clean          - Clean build files"
	@echo "  test           - Run tests"
	@echo "  fmt            - Format code"

.PHONY: servelet-dev servelet-build anyserve-dev anyserve-build clean build-all test fmt help
