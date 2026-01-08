.PHONY: help dev-up dev-down clean topics setup-test lint test-ingestion test-unit test-all \
        verify verify-stream run run-full flink-build flink-submit flink-verify \
        spark-bronze spark-silver spark-gold spark-reconcile spark-maintenance

# --- Configuration ---
SPARK_PKGS := "io.delta:delta-spark_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4"

# SPARK_SUBMIT: Configured for the /opt/spark/jobs/spark volume mount
# PYTHONPATH allows absolute imports like 'from spark.config' to find the package
# We set spark.driver.host to spark-master for consistent container networking
SPARK_SUBMIT := docker exec -it spark-master /usr/bin/env PYTHONPATH=/opt/spark/jobs /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=spark-master \
  --total-executor-cores 1 \
  --executor-memory 1G \
  --packages $(SPARK_PKGS)

help:
	@echo "TransitFlow - Principal Engineer Data Pipeline Control"
	@echo ""
	@echo "1. Quality & Environment (Run First):"
	@echo "  make setup-test    Install local Python dependencies for testing"
	@echo "  make lint          AUTO-FIX formatting and check for PEP8 errors"
	@echo "  make test-ingestion Run Ingestion/Bridge unit tests"
	@echo "  make test-unit     Run Spark/Medallion logic tests with coverage"
	@echo "  make test-all      Run every unit test in the project"
	@echo ""
	@echo "2. Infrastructure Management:"
	@echo "  make dev-up        Start all Docker containers"
	@echo "  make dev-down      Stop and remove containers"
	@echo "  make topics        Initialize Kafka topics"
	@echo ""
	@echo "3. Ingestion & Streaming Execution:"
	@echo "  make run           Run Ingestion Bridge (Line 600)"
	@echo "  make verify        Verify Raw Telemetry landing (verify_ingestion.py)"
	@echo "  make flink-build   Build Flink Java JAR"
	@echo "  make flink-submit  Submit Enrichment Job to Flink"
	@echo "  make verify-stream Verify Enriched stream (verify_stream_pipeline.py)"
	@echo ""
	@echo "4. Lakehouse & Spark Operations:"
	@echo "  make spark-bronze  Stream: Kafka -> Bronze Delta"
	@echo "  make spark-silver  Batch: Bronze -> Silver Delta (Deduplication)"
	@echo "  make spark-gold    Batch: Silver -> Gold Delta (KPIs)"
	@echo "  make spark-reconcile Audit: Verify Bronze vs Silver counts in Postgres"
	@echo "  make spark-maintenance Run Delta VACUUM and OPTIMIZE"
	@echo ""
	@echo "5. Cleanup:"
	@echo "  make clean         Remove caches and build artifacts"

# --- Infrastructure ---
dev-up:
	@if [ ! -f infra/local/.env ]; then cp infra/local/.env.example infra/local/.env; fi
	cd infra/local && docker compose up -d
	@sleep 15
	@make topics

dev-down:
	cd infra/local && docker compose down

topics:
	docker exec -it redpanda rpk topic create fleet.telemetry.raw fleet.enriched fleet.stop_events --partitions 8 2>/dev/null || true
	docker exec -it redpanda rpk topic create fleet.telemetry.dlq --partitions 1 2>/dev/null || true

# --- Development, Quality & Testing ---
setup-test:
	pip install --upgrade pip
	pip install -r requirements-test.txt

lint:
	@echo "Auto-formatting code with isort and black..."
	isort spark/ tests/
	black spark/ tests/
	@echo "Checking for remaining PEP8 violations..."
	flake8 spark/ tests/

test-ingestion:
	PYTHONPATH=. pytest tests/unit/ingestion/ -v

test-unit:
	PYTHONPATH=. pytest tests/unit/spark/ --cov=spark --cov-report=term-missing

test-all:
	PYTHONPATH=. pytest tests/unit/ -v

# --- Ingestion & Streaming ---
verify:
	PYTHONPATH=. python scripts/verify_ingestion.py

verify-stream:
	PYTHONPATH=. python scripts/verify_stream_pipeline.py

run:
	PYTHONPATH=. python scripts/run_bridge.py --line 600

run-full:
	PYTHONPATH=. python scripts/run_bridge.py

flink-build:
	cd flink && mvn clean package -DskipTests
	@mkdir -p infra/local/flink-jobs
	cp flink/target/transitflow-flink-1.0.0.jar infra/local/flink-jobs/

flink-submit:
	docker exec -it flink-jobmanager flink run -d /opt/flink/jobs/transitflow-flink-1.0.0.jar

flink-verify:
	docker exec -it redpanda rpk topic consume fleet.enriched -n 5

# --- Spark & Delta Operations ---
# Note: Path corresponds to the volume mount /opt/spark/jobs/spark/
spark-bronze:
	$(SPARK_SUBMIT) /opt/spark/jobs/spark/bronze_writer.py --table all

spark-silver:
	$(SPARK_SUBMIT) /opt/spark/jobs/spark/silver_transform.py

spark-gold:
	$(SPARK_SUBMIT) /opt/spark/jobs/spark/gold_aggregation.py

spark-maintenance:
	$(SPARK_SUBMIT) /opt/spark/jobs/spark/maintenance.py

spark-reconcile:
	$(SPARK_SUBMIT) /opt/spark/jobs/spark/reconciliation.py --save

# Lakehouse Verification
verify-lakehouse:
	@echo "Running Final Lakehouse Integrity Check..."
	PYTHONPATH=. python scripts/verify_lakehouse.py

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	rm -rf flink/target .pytest_cache .coverage .flake8_cache