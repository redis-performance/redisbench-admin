#!/bin/bash

# Compliance checks script for redisbench-admin
set -e

echo "🔍 Running compliance checks..."

echo "📝 Checking code formatting with black..."
poetry run black --check redisbench_admin

echo "🔍 Running linting with flake8..."
poetry run flake8 redisbench_admin

echo "✅ All compliance checks passed!"
