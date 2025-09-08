.PHONY: help install test lint format check clean

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@egrep '^(.+)\s*:.*?##\s*(.+)' $(MAKEFILE_LIST) | column -t -c 2 -s ':#'

install: ## Install dependencies
	poetry install

test: ## Run tests
	poetry run pytest

lint: ## Run linting
	poetry run ruff check .
	poetry run mypy src/

format: ## Format code
	poetry run black .
	poetry run ruff --fix .

check: ## Run all checks (format, lint, test)
	make format
	make lint  
	make test

clean: ## Clean up cache files
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf .coverage
	rm -rf htmlcov/