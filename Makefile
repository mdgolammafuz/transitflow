.PHONY: help dev-up dev-down test verify run run-full clean flink-build flink-submit spark-bronze spark-silver flink-test flink-cancel flink-verify topics

# Configuration
SPARK_PKGS := "io.delta:delta-spark_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4"

help:
	@echo "TransitFlow Commands"
	@echo ""
	@echo "Infrastructure:"
	@echo "  make dev-up        Start all local services"
	@echo "  make dev-down      Stop local services"
	@echo "  make topics        Create all Kafka topics"
	@echo ""
	@echo "Phase 1 - Ingestion:"
	@echo "  make test          Run Python unit tests"
	@echo "  make verify        Run Phase 1 verification"
	@echo "  make run           Run ingestion bridge (line 600)"
	@echo "  make run-full      Run ingestion bridge (all traffic)"
	@echo ""
	@echo "Phase 2 - Flink:"
	@echo "  make flink-build   Build Flink job JAR"
	@echo "  make flink-test    Run Flink unit tests"
	@echo "  make flink-submit  Submit job to Flink cluster"
	@echo "  make flink-verify  Run Phase 2 verification"
	@echo ""
	@echo "Phase 3 - Spark & Delta Lake:"
	@echo "  make spark-bronze  Run Kafka -> Bronze (Streaming)"
	@echo "  make spark-silver  Run Bronze -> Silver (Batch/Transform)"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean         Remove temp files"

dev-up:
	@if [ ! -f infra/local/.env ]; then \
		echo "Creating .env from .env.example..."; \
		cp infra/local/.env.example infra/local/.env; \
	fi
	cd infra/local && docker compose up -d
	@echo "Services starting..."
	@sleep 15
	@make topics
	@echo "Spark Master UI: http://localhost:8083"

dev-down:
	cd infra/local && docker compose down

topics:
	@echo "Creating Kafka topics..."
	docker exec -it redpanda rpk topic create fleet.telemetry.raw --partitions 8 2>/dev/null || true
	docker exec -it redpanda rpk topic create fleet.telemetry.dlq --partitions 1 2>/dev/null || true
	docker exec -it redpanda rpk topic create fleet.enriched --partitions 8 2>/dev/null || true
	docker exec -it redpanda rpk topic create fleet.stop_events --partitions 8 2>/dev/null || true
	docker exec -it redpanda rpk topic list

test:
	PYTHONPATH=. pytest tests/unit/ -v

verify:
	PYTHONPATH=. python scripts/verify_phase1.py --full

run:
	PYTHONPATH=. python scripts/run_bridge.py --line 600

run-full:
	PYTHONPATH=. python scripts/run_bridge.py

flink-build:
	cd flink && mvn clean package -DskipTests
	@mkdir -p infra/local/flink-jobs
	cp flink/target/transitflow-flink-1.0.0.jar infra/local/flink-jobs/

flink-test:
	cd flink && mvn test

flink-submit:
	docker exec -it flink-jobmanager flink run -d /opt/flink/jobs/transitflow-flink-1.0.0.jar

spark-bronze:
	@echo "Starting Spark Bronze Streaming Writer..."
	docker exec -it spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--packages $(SPARK_PKGS) \
		/opt/spark/jobs/bronze_writer.py --table all

spark-silver:
	@echo "Running Silver Transformation..."
	docker exec -it spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--packages $(SPARK_PKGS) \
		/opt/spark/jobs/silver_transform.py

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	rm -rf flink/target