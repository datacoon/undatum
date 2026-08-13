.PHONY: help install install-dev test lint format type-check docs man clean build

help: ## Show this help message
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

install: ## Install package in production mode
	pip install -e .

install-dev: ## Install package with development dependencies
	pip install -e .
	pip install black ruff mypy pylint pytest pytest-cov pre-commit sphinx sphinx-rtd-theme

test: ## Run tests
	pytest

test-cov: ## Run tests with coverage
	pytest --cov=undatum --cov-report=html --cov-report=term

lint: ## Run linters
	ruff check undatum/
	pylint undatum/

format: ## Format code with black
	black undatum/ tests/

format-check: ## Check code formatting without making changes
	black --check undatum/ tests/

type-check: ## Run type checker
	mypy undatum/

man: ## Generate man/undatum.1 from the CLI
	python scripts/generate_manpage.py

docs: ## Build documentation
	cd docs && make html

docs-serve: ## Serve documentation locally (requires sphinx-autobuild)
	cd docs && sphinx-autobuild . _build/html

clean: ## Clean build artifacts
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf htmlcov/
	rm -rf docs/_build/
	find . -type d -name __pycache__ -exec rm -r {} +
	find . -type f -name "*.pyc" -delete

build: ## Build distribution packages
	python -m build

pre-commit-install: ## Install pre-commit hooks
	pre-commit install

pre-commit-run: ## Run pre-commit hooks on all files
	pre-commit run --all-files

check-all: format-check lint type-check test ## Run all checks

ci: check-all ## Run all CI checks (alias for check-all)
