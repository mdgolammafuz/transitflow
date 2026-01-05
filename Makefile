.PHONY: help dev-up dev-down topics test verify run clean flink-build flink-submit flink-cancel flink-verify

help:
	@echo "TransitFlow Commands"
	@echo ""
	@echo "Infrastructure:"
	@echo "  make dev-up        Start local services (Kafka, Flink, Postgres, Redis)"
	@echo "  make dev-down      Stop local services"
	@echo "  make topics        Apply topic configuration (idempotent)"
	@echo ""
	@echo "Phase 1 - Ingestion:"
	@echo "  make test          Run Python unit tests"
	@echo "  make verify        Run ingestion smoke tests"
	@echo "  make run           Run ingestion bridge (Line 600)"
	@echo "  make run-full      Run ingestion bridge (All traffic)"
	@echo ""
	@echo "Phase 2 - Flink:"
	@echo "  make flink-build   Build Flink job JAR"
	@echo "  make flink-test    Run Flink unit tests"
	@echo "  make flink-submit  Submit job to Flink cluster"
	@echo "  make flink-cancel  Cancel all running Flink jobs"
	@echo "  make flink-verify  Run Phase 2 verification"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean         Remove temp files"

dev-up:
	@if [ ! -f infra/local/.env ]; then \
		echo "Creating .env from .env.example..."; \
		cp infra/local/.env.example infra/local/.env; \
	fi
	cd infra/local && docker-compose up -d
	@echo ""
	@echo "Services starting..."
	@sleep 10
	@echo ""
	@echo "Access:"
	@echo "  Redpanda Console:  http://localhost:8080"
	@echo "  Flink Dashboard:   http://localhost:8082"
	@echo "  MinIO Console:     http://localhost:9001"
	@echo "  Grafana:           http://localhost:3000 (admin/admin)"
	@echo "  Prometheus:        http://localhost:9090"

dev-down:
	cd infra/local && docker-compose down

# Executes topics.sh inside the container.
# This is better than inline commands because it handles retention policies/configs centralized in one file.
topics:
	docker cp infra/local/init/redpanda/topics.sh redpanda:/tmp/topics.sh
	docker exec -u 0 redpanda chmod +x /tmp/topics.sh
	docker exec redpanda /tmp/topics.sh

test:
	PYTHONPATH=. pytest tests/unit/ -v

verify:
	PYTHONPATH=. python scripts/verify_ingestion.py --full

run:
	PYTHONPATH=. python scripts/run_bridge.py --line 600

run-full:
	PYTHONPATH=. python scripts/run_bridge.py

# --- Phase 2: Flink ---

flink-build:
	cd flink && mvn clean package -DskipTests
	@mkdir -p infra/local/flink-jobs
	cp flink/target/transitflow-flink-1.0.0.jar infra/local/flink-jobs/

flink-test:
	cd flink && mvn test

flink-submit:
	@echo "Submitting Flink job..."
	docker exec -d flink-jobmanager flink run \
		-d -c fi.transitflow.TransitFlinkApp \
		/opt/flink/jobs/transitflow-flink-1.0.0.jar
	@echo ""
	@echo "Job submitted. Check status at http://localhost:8082"

flink-cancel:
	@echo "Cancelling all running jobs..."
	-docker exec flink-jobmanager flink list | grep RUNNING | awk '{print $$4}' | xargs -I {} docker exec flink-jobmanager flink cancel {}

flink-verify:
	PYTHONPATH=. python scripts/verify_phase2.py --check-all

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type d -name target -exec rm -rf {} + 2>/dev/null || true