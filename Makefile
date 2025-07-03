# Makefile for redisbench-admin

.PHONY: compliance compliance-fix test integration-tests help

# Code quality and compliance checks
compliance:
	@echo "🔍 Running compliance checks..."
	@if poetry run black --check redisbench_admin; then \
		echo "📝 Black formatting: ✅ PASSED"; \
	else \
		echo "📝 Black formatting: ❌ FAILED"; \
		echo "💡 Run 'make format' to fix formatting issues"; \
		exit 1; \
	fi
	@if poetry run flake8 redisbench_admin; then \
		echo "🔍 Flake8 linting: ✅ PASSED"; \
	else \
		echo "🔍 Flake8 linting: ❌ FAILED"; \
		exit 1; \
	fi
	@echo "✅ All compliance checks passed!"

# Fix code formatting issues
format:
	@echo "🔧 Fixing code formatting..."
	poetry run black redisbench_admin
	@echo "✅ Code formatting fixed!"

# Alias for format
compliance-fix: format

# Run tests with coverage
test:
	@echo "🧪 Running tests..."
	poetry run coverage erase
	poetry run pytest --cov=redisbench_admin --cov-report=term-missing -ra
	poetry run coverage xml
	@echo "✅ Tests completed!"

# Run integration tests (alias for test)
integration-tests: test

# Run both compliance and tests
all: compliance test

# Show help
help:
	@echo "Available targets:"
	@echo "  compliance       - Run code quality checks (black, flake8)"
	@echo "  format          - Fix code formatting with black"
	@echo "  compliance-fix   - Alias for format"
	@echo "  test            - Run tests with coverage"
	@echo "  integration-tests - Alias for test"
	@echo "  all             - Run compliance checks and tests"
	@echo "  help            - Show this help message"

# Default target
.DEFAULT_GOAL := help
