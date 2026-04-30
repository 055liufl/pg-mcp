.PHONY: install test test-unit test-integration lint format typecheck coverage clean docker-build docker-run

install:
	pip install -e ".[dev]"

test: test-unit

test-unit:
	pytest tests/unit/ -v

test-integration:
	pytest tests/integration/ -v -m integration

test-e2e:
	pytest tests/e2e/ -v -m e2e

test-all:
	pytest tests/ -v

lint:
	ruff check pg_mcp/ tests/

format:
	ruff format pg_mcp/ tests/

typecheck:
	mypy pg_mcp/

coverage:
	pytest --cov=pg_mcp --cov-report=term-missing tests/unit/

quality: lint typecheck test-unit

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache .coverage htmlcov/ build/ dist/

docker-build:
	docker build -t pg-mcp:latest .

docker-run:
	docker run --rm -it --env-file .env -p 8000:8000 pg-mcp:latest
