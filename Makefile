# Makefile

# Variables
HOST = 0.0.0.0
PORT = 8000
LOG_LEVEL = info
WORKERS = 4

# Help command
.PHONY: help
help: ## Show this help
	@egrep -h '\s##\s' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# Run FastAPI with uvicorn
.PHONY: run-uvicorn
run-uvicorn: ## Run FastAPI with uvicorn
	uv run uvicorn main:app --host $(HOST) --port $(PORT) --reload --loop uvloop --http httptools --log-level $(LOG_LEVEL) --runtime-threads 2 --runtime-blocking-threads 1

# Run FastAPI with granian
.PHONY: run-granian
run-granian: ## Run FastAPI with granian
	uv run granian --interface asgi main:app --host $(HOST) --port $(PORT) --log-level $(LOG_LEVEL) --workers $(WORKERS) --no-ws --loop uvloop --interface asgi --pid-file .pid

.PHONY: run-granian-dev
run-granian-dev: ## Run FastAPI with granian
	uv run granian --interface asgi main:app --host $(HOST) --port $(PORT) --log-level $(LOG_LEVEL) --reload

# Create new alembic database migration
.PHONY: create-db-migration
create-db-migration: ## Create new alembic database migration aka database revision.
	uv run alembic revision --autogenerate -m "$(msg)"

.PHONY: apply-db-migrations
apply-db-migrations: ## apply alembic migrations to database/schema
	uv run alembic upgrade head