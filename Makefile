.PHONY: help install lint format check-format mypy test unit-tests integration-tests build ci clean jupyter activate

# Default pytest options (can be overridden):
# - Parallelize integration suite with xdist using file-based distribution
# - Emit live logs from workers (INFO level) with consistent formatting
PYTEST_ADDOPTS ?= -n auto --dist loadfile --log-cli-level=INFO --log-cli-format="%(levelname)s:%(name)s:%(message)s"

# Default target
help:
	@echo "Available targets:"
	@echo "  install           Install all dependencies with uv"
	@echo "  lint              Run ruff linting checks"
	@echo "  format            Format code with ruff"
	@echo "  check-format      Check if code formatting is correct"
	@echo "  mypy              Run mypy type checking"
	@echo "  test              Run all tests with pytest"
	@echo "  unit-tests        Run unit tests only (exclude integration tests)"
	@echo "  integration-tests Run all integration tests for the adapter"
	@echo "  integration-test  Run a single integration test file (use TEST_FILE=path/to/test.py)"
	@echo "  build             Build the package"
	@echo "  ci                Run all CI checks (lint, format, mypy, unit-tests, build)"
	@echo "  clean             Clean build artifacts"
	@echo "  jupyter           Run Jupyter Lab"
	@echo "  activate          Activate the uv virtual environment"

# Install dependencies
install:
	uv sync --all-groups

# Linting
lint:
	uv run ruff check

# Format code
format:
	uv run ruff format

# Check formatting
check-format:
	uv run ruff format --check

# Type checking
mypy:
	uv run mypy

# Unit tests
test:
	uv run pytest

# Unit tests only (exclude integration tests)
unit-tests:
	uv run pytest -m "not integration"

# Integration tests
integration-tests:
	@if [ -f .env ]; then \
		echo "Loading environment variables from .env file..."; \
		eval $$(cat .env | grep -v '^#' | grep -v '^$$' | sed 's/=/="/; s/$$/"/; s/^/export /' | tr '\n' ';') && uv run pytest $(PYTEST_ADDOPTS) tests/functional/adapter -s -m integration; \
	else \
		uv run pytest $(PYTEST_ADDOPTS) tests/functional/adapter -s -m integration; \
	fi

# Run a single integration test file
integration-test:
	@if [ -z "$(TEST_FILE)" ]; then \
		echo "Usage: make integration-test TEST_FILE=path/to/test_file.py"; \
		exit 1; \
	fi; \
	if [ -f .env ]; then \
		echo "Loading environment variables from .env file..."; \
		eval $$(cat .env | grep -v '^#' | grep -v '^$$' | sed 's/=/="/; s/$$/"/; s/^/export /' | tr '\n' ';') && uv run pytest $(PYTEST_ADDOPTS) $(TEST_FILE) -s -m integration; \
	else \
		uv run pytest $(PYTEST_ADDOPTS) $(TEST_FILE) -s -m integration; \
	fi

# Build package
build:
	uv build

# Run all CI checks
ci: lint check-format mypy unit-tests build
	@echo "All CI checks passed!"

# Clean build artifacts
clean:
	rm -rf dist/
	rm -rf build/
	rm -rf *.egg-info/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

jupyter:
	uv run --with jupyter jupyter lab

activate:
	@echo "To activate the uv virtual environment, run:"
	@echo "  source .venv/bin/activate"
	@echo ""
	@echo "After activation, you can use commands like:"
	@echo "  python, pytest, ruff, mypy, etc. without 'uv run'"