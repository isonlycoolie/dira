.PHONY: install lint typecheck test test-integration docker-up docker-down migrate bootstrap-roads

install:
	python -m pip install --upgrade pip
	python -m pip install hatch ruff mypy pytest pytest-cov alembic

lint:
	python -m ruff check .

typecheck:
	python -m mypy libs

test:
	python -m pytest

test-integration:
	python -m pytest tests/integration

docker-up:
	docker compose up -d

docker-down:
	docker compose down

migrate:
	python -m alembic upgrade head

bootstrap-roads:
	python scripts/bootstrap_road_network.py