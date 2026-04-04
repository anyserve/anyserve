.DEFAULT_GOAL := help

MISE := $(HOME)/.local/bin/mise
BASE_ENV := export PATH="$(HOME)/.local/bin:$(HOME)/.cargo/bin:$$PATH" && . "$(HOME)/.cargo/env"

setup:
	@$(BASE_ENV) && $(MISE) trust --all && $(MISE) install

build:
	@$(BASE_ENV) && $(MISE) run build

check:
	@$(BASE_ENV) && $(MISE) run check

test:
	@$(BASE_ENV) && $(MISE) run test

e2e:
	@$(BASE_ENV) && $(MISE) run e2e

clippy:
	@$(BASE_ENV) && $(MISE) run clippy

python-sdk:
	@$(BASE_ENV) && $(MISE) run python-sdk

python-sdk-smoke:
	@$(BASE_ENV) && $(MISE) run python-sdk-smoke

python-sdk-test:
	@$(BASE_ENV) && $(MISE) run python-sdk-test

python-sdk-e2e:
	@$(BASE_ENV) && $(MISE) run python-sdk-e2e

docs-build:
	@$(BASE_ENV) && $(MISE) run docs-build

fmt:
	@$(BASE_ENV) && $(MISE) run fmt

help:
	@echo "Available commands:"
	@echo "  setup  - trust mise config and install tools"
	@echo "  build  - build the Rust workspace"
	@echo "  check  - run cargo check through mise"
	@echo "  test   - run workspace tests"
	@echo "  e2e    - run the gRPC end-to-end test"
	@echo "  clippy - lint the workspace"
	@echo "  python-sdk - build the Rust-exported Python bindings wheel"
	@echo "  python-sdk-smoke - install the built wheel and verify bindings imports"
	@echo "  python-sdk-test - run Python unit tests for the high-level bindings API"
	@echo "  python-sdk-e2e - run Python SDK live end-to-end tests"
	@echo "  docs-build - build the mdBook documentation"
	@echo "  fmt    - format the workspace"

.PHONY: setup build check test e2e clippy python-sdk python-sdk-smoke python-sdk-test python-sdk-e2e docs-build fmt help
