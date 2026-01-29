.PHONY: help install sync clean test lint format build run dev-setup check all pre-commit-install pre-commit-run pre-commit-autoupdate

# Emojis and Icons
ROCKET=🚀
CHECK=✓
INFO=ℹ️
TEST=🧪
BUILD=🔨
CLEAN=🧹
SETUP=⚙️

# Default Python interpreter
UV := ~/.local/bin/uv
PYTHON := $(UV) run python

help:
	@echo "$(ROCKET) DeltaWarehouse360 - Available Commands"
	@echo ""
	@echo "$(SETUP) Setup Commands:"
	@echo "  make install          Install uv package manager"
	@echo "  make sync             Sync dependencies and create virtual environment"
	@echo "  make dev-setup        Complete development setup (install + sync)"
	@echo "  make pre-commit-install Install pre-commit hooks"
	@echo ""
	@echo "$(TEST) Development Commands:"
	@echo "  make test             Run pytest test suite"
	@echo "  make lint             Run ruff linter"
	@echo "  make format           Format code with ruff"
	@echo "  make check            Run all checks (lint + test)"
	@echo "  make pre-commit-run   Run all pre-commit hooks"
	@echo ""
	@echo "$(BUILD) Build Commands:"
	@echo "  make build            Build wheel package"
	@echo "  make build-clean      Clean build artifacts and rebuild"
	@echo ""
	@echo "🛠️  Utility Commands:"
	@echo "  make clean            Remove virtual environment and cache"
	@echo "  make run              Run main application"
	@echo "  make shell            Open Python shell in project environment"
	@echo "  make all              Run all checks and build"
	@echo ""

# Setup targets
install:
	@echo "$(ROCKET) Installing uv..."
	curl -LsSf https://astral.sh/uv/install.sh | sh
	@echo "$(CHECK) uv installed successfully"

sync:
	@echo "$(ROCKET) Syncing dependencies..."
	$(UV) sync
	@echo "$(CHECK) Dependencies synced"

dev-setup: install sync
	@echo "$(CHECK) Development setup complete"

# Test targets
test:
	@echo "$(TEST) Running tests..."
	$(UV) run pytest -v

test-watch:
	@echo "$(TEST) Running tests in watch mode..."
	$(UV) run pytest -v --tb=short

# Lint and format targets
lint:
	@echo "🔍 Checking code with ruff..."
	$(UV) run ruff check .

format:
	@echo "✨ Formatting code with ruff..."
	$(UV) run ruff format .

check: lint test
	@echo "$(CHECK) All checks passed"

# Build targets
build:
	@echo "$(BUILD) Building wheel package..."
	$(UV) build --wheel
	@echo "$(CHECK) Build complete"

build-clean: clean build
	@echo "$(CHECK) Clean build complete"

# Run targets
run:
	@echo "$(ROCKET) Running application..."
	$(UV) run python -m DeltaWarehouse360.main

shell:
	@echo "🐍 Opening Python shell..."
	$(UV) run python

# Utility targets
clean:
	@echo "$(CLEAN) Cleaning up..."
	rm -rf .venv
	rm -rf .pytest_cache
	rm -rf build dist *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name ".coverage" -delete
	@echo "$(CHECK) Cleanup complete"

# Comprehensive targets
all: check build
	@echo "$(ROCKET) $(CHECK) All tasks completed successfully"

# Add a dependency
add-dep:
	@read -p "Enter package name: " pkg; \
	$(UV) add $$pkg

add-dev-dep:
	@read -p "Enter package name: " pkg; \
	$(UV) add --dev $$pkg

# Export requirements for pip
export-requirements:
	@echo "📋 Exporting requirements..."
	$(UV) export --format requirements-txt > requirements.txt
	$(UV) export --format requirements-txt --dev > requirements-dev.txt
	@echo "$(CHECK) Requirements exported to requirements.txt and requirements-dev.txt"

# Show project info
info:
	@echo "$(INFO) Project Information:"
	@echo "Name: DeltaWarehouse360"
	@echo "Version: 0.0.1"
	@echo "Python: >=3.10,<3.13"
	@echo "Build System: hatchling"
	@echo ""
	@echo "$(ROCKET) uv Version:"
	@$(UV) --version
	@echo ""
	@echo "$(SETUP) Environment:"
	@echo "Virtual Environment: .venv"

# Databricks bundle targets
DATABRICKS := databricks
DB_TARGET ?= prod

.PHONY: databricks-build databricks-validate databricks-deploy

databricks-build:
	@echo "$(ROCKET) Building Python wheel and Databricks bundle"
	$(UV) build --wheel
	@echo "$(BUILD) Building Databricks bundle from databricks.yml"
	$(DATABRICKS) bundle build
	@echo "$(CHECK) Databricks bundle build step finished"

databricks-validate:
	@echo "$(INFO) Validating databricks.yml"
	$(DATABRICKS) bundle validate
	@echo "$(CHECK) Validation finished"

databricks-deploy:
	@echo "$(ROCKET) Deploying Databricks bundle to target=$(DB_TARGET)"
	$(DATABRICKS) bundle deploy --target $(DB_TARGET)
	@echo "$(CHECK) Deploy finished (target=$(DB_TARGET))"

# Pre-commit hooks targets
pre-commit-install:
	@echo "🪝 Installing pre-commit hooks..."
	pip install pre-commit
	pre-commit install
	@echo "$(CHECK) Pre-commit hooks installed successfully"

pre-commit-run:
	@echo "🪝 Running pre-commit hooks on all files..."
	pre-commit run --all-files
	@echo "$(CHECK) Pre-commit hooks check complete"

pre-commit-autoupdate:
	@echo "🔄 Updating pre-commit hooks..."
	pre-commit autoupdate
	@echo "$(CHECK) Pre-commit hooks updated"
