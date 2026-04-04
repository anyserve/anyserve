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

python-sdk:
	@$(BASE_ENV) && $(MISE) run python-sdk

fmt:
	@$(BASE_ENV) && cargo fmt --all

help:
	@echo "Available commands:"
	@echo "  setup  - trust mise config and install tools"
	@echo "  build  - build the Rust workspace"
	@echo "  check  - run cargo check through mise"
	@echo "  test   - run workspace tests"
	@echo "  python-sdk - build the Rust-exported Python wheel"
	@echo "  fmt    - format the workspace"

.PHONY: setup build check test python-sdk fmt help
