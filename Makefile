.PHONY: help dev-up dev-down topics test verify run clean

help:
	@echo "TransitFlow - Phase 1 Commands"
	@echo ""
	@echo "Infrastructure:"
	@echo "  make dev-up      Start local services (Kafka, PostgreSQL, Redis)"
	@echo "  make dev-down    Stop local services"
	@echo "  make topics      Apply topic configuration (idempotent)"
	@echo ""
	@echo "Development:"
	@echo "  make test        Run unit tests"
	@echo "  make verify      Run ingestion smoke tests"
	@echo "  make run         Run ingestion bridge (Line 600)"
	@echo "  make run-full    Run ingestion bridge (All traffic)"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean       Remove temp files"

dev-up:
	cd infra/local && docker-compose up -d
	@echo ""
	@echo "Services starting..."
	@sleep 5
	@echo ""
	@echo "Access:"
	@echo "  Redpanda Console: http://localhost:8080"
	@echo "  Grafana:          http://localhost:3000 (admin/admin)"
	@echo "  Prometheus:       http://localhost:9090"

dev-down:
	cd infra/local && docker-compose down

topics:
	docker cp infra/local/init/redpanda/topics.sh redpanda:/tmp/topics.sh
	docker exec -u 0 redpanda chmod +x /tmp/topics.sh
	docker exec redpanda /tmp/topics.sh

test:
	PYTHONPATH=. pytest tests/unit/ -v

verify:
	python scripts/verify_ingestion.py --full

run:
	python scripts/run_bridge.py --line 600

run-full:
	python scripts/run_bridge.py

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true