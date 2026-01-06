# TransitFlow Unified Makefile
# Industry-standard automation for Stream Processing Pipeline

# Load environment variables for local scripts from the infra directory
# The '-' prefix prevents make from failing if the .env file doesn't exist yet
-include infra/local/.env
export

.PHONY: help dev-up dev-down topics test verify run clean flink-build flink-test flink-submit flink-cancel flink-verify

# Default target
help:
	@echo "TransitFlow Commands"
	@echo ""
	@echo "Infrastructure:"
	@echo "  make dev-up        Start local services (Kafka, Flink, Postgres, Redis)"
	@echo "  make dev-down      Stop local services and clear volumes"
	@echo "  make topics        Apply topic configuration (idempotent)"
	@echo ""
	@echo "Ingestion Bridge:"
	@echo "  make test          Run Python unit tests"
	@echo "  make verify        Run ingestion smoke tests"
	@echo "  make run           Run ingestion bridge (Line 600)"
	@echo "  make run-full      Run ingestion bridge (All traffic)"
	@echo ""
	@echo "Flink Stream Processing:"
	@echo "  make flink-build   Build Flink job JAR"
	@echo "  make flink-test    Run Flink unit tests"
	@echo "  make flink-submit  Submit job to Flink cluster"
	@echo "  make flink-cancel  Cancel all running Flink jobs"
	@echo "  make flink-verify  Run Stream Pipeline verification suite"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean         Remove temp files and build artifacts"

# --- Infrastructure ---

dev-up:
	@if [ ! -f infra/local/.env ]; then \
		echo "Creating .env from .env.example..."; \
		cp infra/local/.env.example infra/local/.env; \
	fi
	cd infra/local && docker-compose up -d
	@echo ""
	@echo "Waiting for services to reach healthy state..."
	@sleep 15
	@echo ""
	@echo "Access:"
	@echo "  Redpanda Console:  http://localhost:8080"
	@echo "  Flink Dashboard:   http://localhost:8082"
	@echo "  MinIO Console:     http://localhost:9001"
	@echo "  Grafana:           http://localhost:3000"
	@echo "  Prometheus:        http://localhost:9090"

dev-down:
	cd infra/local && docker-compose down --volumes --remove-orphans

# Centralized topic management inside the container
topics:
	@echo "Applying Kafka topic configurations..."
	docker cp infra/local/init/redpanda/topics.sh redpanda:/tmp/topics.sh
	docker exec -u 0 redpanda chmod +x /tmp/topics.sh
	docker exec redpanda /tmp/topics.sh

# --- Ingestion Bridge ---

test:
	PYTHONPATH=. pytest tests/unit/ -v

verify:
	PYTHONPATH=. python scripts/verify_ingestion.py --full

run:
	PYTHONPATH=. python scripts/run_bridge.py --line 600

run-full:
	PYTHONPATH=. python scripts/run_bridge.py

# --- Flink Stream Processing ---

flink-build:
	@echo "Building Flink JAR..."
	cd flink && mvn clean package -DskipTests
	@mkdir -p infra/local/flink-jobs
	cp flink/target/transitflow-flink-1.0.0.jar infra/local/flink-jobs/

flink-test:
	cd flink && mvn test

flink-submit:
	@echo "Submitting TransitFlow Vehicle Processor to Flink..."
	docker exec flink-jobmanager flink run \
		-d -c fi.transitflow.TransitFlinkApp \
		/opt/flink/jobs/transitflow-flink-1.0.0.jar
	@echo ""
	@echo "Job submitted. Check status at http://localhost:8082"

flink-cancel:
	@echo "Cancelling all running Flink jobs..."
	-docker exec flink-jobmanager flink list | grep RUNNING | awk '{print $$4}' | xargs -I {} docker exec flink-jobmanager flink cancel {}

flink-verify:
	@echo "Executing Stream Pipeline Verification Suite..."
	# The script now sees variables like REDIS_PASSWORD directly from the exported .env
	PYTHONPATH=. python scripts/verify_stream_pipeline.py --check-all

# --- Cleanup ---

clean:
	@echo "Cleaning workspace..."
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type d -name target -exec rm -rf {} + 2>/dev/null || true