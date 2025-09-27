.PHONY: help build up down restart logs clean test

help:
	@echo "Available commands:"
	@echo "  make build    - Build Docker images"
	@echo "  make up       - Start all services"
	@echo "  make down     - Stop all services"
	@echo "  make restart  - Restart all services"
	@echo "  make logs     - View logs"
	@echo "  make clean    - Clean up volumes and images"
	@echo "  make test     - Run tests"
	@echo "  make db-shell - Connect to PostgreSQL"
	@echo "  make redis-cli- Connect to Redis"
	@echo "  make recovery - Trigger manual recovery"

build:
	cd infrastructure/docker && docker-compose build

up:
	cd infrastructure/docker && docker-compose up -d

down:
	cd infrastructure/docker && docker-compose down

restart:
	cd infrastructure/docker && docker-compose restart

logs:
	cd infrastructure/docker && docker-compose logs -f

logs-app:
	cd infrastructure/docker && docker-compose logs -f app

logs-worker:
	cd infrastructure/docker && docker-compose logs -f celery-worker

logs-beat:
	cd infrastructure/docker && docker-compose logs -f celery-beat

clean:
	cd infrastructure/docker && docker-compose down -v
	docker system prune -f

test:
	cd infrastructure/docker && docker-compose exec app pytest

db-shell:
	cd infrastructure/docker && docker-compose exec postgres psql -U irp_user -d irp_db

redis-cli:
	cd infrastructure/docker && docker-compose exec redis redis-cli

recovery:
	cd infrastructure/docker && docker-compose exec app python -m src.services.recovery_service

flower:
	@echo "Opening Flower UI at http://localhost:5555"
	@open http://localhost:5555 || xdg-open http://localhost:5555

ps:
	cd infrastructure/docker && docker-compose ps

health:
	@curl -s http://localhost:8000/health/ready | python -m json.tool

check-postgres:
	@docker exec -it irp-postgres psql -U irp_user -d irp_db -c "\dt"
	@echo "PostgreSQL is ready."

check-redis:
	@docker exec -it irp-redis redis-cli ping
	@echo "Redis is ready."