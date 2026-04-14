.PHONY: help up down restart logs db-migrate db-seed shell-api shell-db reset pull-model deploy-frontend models-restore models-snapshot

DOCKER_COMPOSE = docker compose
COLIMA_CPU     = 4
COLIMA_MEMORY  = 8
COLIMA_DISK    = 60

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

colima-start: ## Start Colima container runtime (run this first!)
	colima start --cpu $(COLIMA_CPU) --memory $(COLIMA_MEMORY) --disk $(COLIMA_DISK)

colima-stop: ## Stop Colima
	colima stop

up: ## Start all services
	@cp -n .env.example .env 2>/dev/null || true
	$(DOCKER_COMPOSE) up -d

down: ## Stop all services
	$(DOCKER_COMPOSE) down

restart: ## Restart all services
	$(DOCKER_COMPOSE) restart

logs: ## Tail logs (all services)
	$(DOCKER_COMPOSE) logs -f

logs-api: ## Tail API logs
	$(DOCKER_COMPOSE) logs -f api

logs-worker: ## Tail Celery worker logs
	$(DOCKER_COMPOSE) logs -f celery-worker

db-migrate: ## Run database migrations
	$(DOCKER_COMPOSE) exec api alembic upgrade head

db-seed: ## Seed database with initial data
	$(DOCKER_COMPOSE) exec api python scripts/seed.py

db-reset: ## Drop and recreate database (DESTRUCTIVE)
	$(DOCKER_COMPOSE) exec api alembic downgrade base
	$(DOCKER_COMPOSE) exec api alembic upgrade head
	$(DOCKER_COMPOSE) exec api python scripts/seed.py

models-restore: ## Restore champion/challenger models + history from database/models_snapshot.sql
	@test -f database/models_snapshot.sql || (echo "database/models_snapshot.sql not found — nothing to restore"; exit 1)
	docker exec -i jobharvest-postgres psql -U jobharvest -d jobharvest < database/models_snapshot.sql
	@mkdir -p storage
	@test -f database/auto_improve_memory.json && cp database/auto_improve_memory.json storage/auto_improve_memory.json || echo "(no memory mirror to restore)"
	@test -f database/auto_improve_history.json && cp database/auto_improve_history.json storage/auto_improve_history.json || echo "(no history mirror to restore)"
	@echo "✅ Models + memory restored from database/models_snapshot.sql"

models-snapshot: ## Manually regenerate + commit + push models snapshot (daemon does this automatically per iteration)
	bash database/dump_models.sh
	@mkdir -p database
	@test -f storage/auto_improve_memory.json && cp storage/auto_improve_memory.json database/auto_improve_memory.json || true
	@test -f storage/auto_improve_history.json && cp storage/auto_improve_history.json database/auto_improve_history.json || true
	git add database/models_snapshot.sql database/auto_improve_memory.json database/auto_improve_history.json backend/app/crawlers/ || true
	@if git diff --cached --quiet; then echo "No model changes to commit"; else \
		git commit -m "chore(models): manual snapshot via make models-snapshot" && \
		git push origin main || echo "⚠️ push deferred (commit stays local)"; \
	fi

shell-api: ## Open shell in API container
	$(DOCKER_COMPOSE) exec api bash

shell-db: ## Open psql shell
	$(DOCKER_COMPOSE) exec postgres psql -U jobharvest -d jobharvest

pull-model: ## Pull Ollama model (llama3.1:8b by default)
	$(DOCKER_COMPOSE) exec ollama ollama pull $${OLLAMA_MODEL:-llama3.1:8b}

reset: ## Full reset — stop, remove volumes, restart (DESTRUCTIVE)
	$(DOCKER_COMPOSE) down -v
	$(DOCKER_COMPOSE) up -d
	sleep 10
	$(MAKE) db-migrate
	$(MAKE) db-seed

deploy-frontend: ## Rebuild frontend dev image (only needed after package.json changes — source changes are live via HMR)
	$(DOCKER_COMPOSE) build --no-cache frontend
	$(DOCKER_COMPOSE) up -d --force-recreate frontend

build-frontend-prod: ## Build production nginx image (for stable releases)
	$(DOCKER_COMPOSE) build --no-cache --build-arg DOCKERFILE=Dockerfile frontend

crawl-trigger: ## Trigger a full crawl cycle
	curl -s -X POST http://localhost:8000/api/v1/crawl/trigger-full | python3 -m json.tool

health: ## Check system health
	curl -s http://localhost:8000/api/v1/health | python3 -m json.tool
