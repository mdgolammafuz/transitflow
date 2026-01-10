.PHONY: help dev-up dev-down clean topics setup-test lint test-ingestion test-unit test-all \
        verify-ingestion verify-stream verify-pipeline run run-full flink-build flink-submit \
        spark-bronze spark-silver spark-gold spark-reconcile dbt-deps dbt-seed dbt-snapshot \
        dbt-run dbt-test dbt-docs schema-register

# --- Configuration ---
SPARK_PKGS := "io.delta:delta-spark_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4"
SPARK_SUBMIT := docker exec -it spark-master /usr/bin/env PYTHONPATH=/opt/spark/jobs /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=spark-master \
  --total-executor-cores 1 \
  --executor-memory 1G \
  --packages $(SPARK_PKGS)

help:
	@echo "TransitFlow - Pipeline Control (Security & Clean Code Enforced)"
	@echo ""
	@echo "1. Quality & Validation:"
	@echo "  make lint          Auto-format and check PEP8 (isort, black, flake8)"
	@echo "  make test-all      Run all Python unit tests (Registry, dbt-contracts, Spark)"
	@echo "  make verify-pipeline Run the full pipeline integrity check script"
	@echo ""
	@echo "2. Infrastructure:"
	@echo "  make dev-up        Start containers and initialize Kafka topics"
	@echo "  make dev-down      Stop and remove containers"
	@echo ""
	@echo "3. Data Contracts & Registry:"
	@echo "  make schema-register Register Avro schemas with Schema Registry"
	@echo "  make dbt-deps        Install dbt dependencies"
	@echo ""
	@echo "4. Execution Layers:"
	@echo "  make run             Run Ingestion Bridge (Line 600)"
	@echo "  make flink-submit    Submit Enrichment Job to Flink"
	@echo "  make spark-bronze    Stream: Kafka -> Bronze Delta"
	@echo "  make dbt-snapshot    Capture SCD Type 2 History (Snapshots)"
	@echo "  make dbt-run         Execute Transformation Lineage (Staging -> Marts)"
	@echo "  make dbt-test        Verify Data Contracts (dbt Tests)"
	@echo ""
	@echo "5. Documentation & Cleanup:"
	@echo "  make dbt-docs        Generate and serve data lineage docs"
	@echo "  make clean           Remove caches, pycache, and build artifacts"

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

# --- Quality & Testing (Existing tests preserved) ---
setup-test:
	pip install -r requirements-test.txt

lint:
	isort spark/ tests/ src/ scripts/
	black spark/ tests/ src/ scripts/
	flake8 spark/ tests/ src/ scripts/

test-ingestion:
	PYTHONPATH=. pytest tests/unit/ingestion/ -v

test-unit:
	PYTHONPATH=. pytest tests/unit/spark/ --cov=spark --cov-report=term-missing

test-all:
	PYTHONPATH=. pytest tests/unit/ -v

# --- Ingestion & Streaming ---
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

# --- Schema Registry & dbt (The Restructure) ---
schema-register:
	@echo "Enforcing Data Contracts..."
	PYTHONPATH=. python scripts/register_schemas.py

dbt-deps:
	cd dbt && dbt deps --profiles-dir .

dbt-seed:
	cd dbt && dbt seed --profiles-dir .

dbt-snapshot:
	@echo "Capturing SCD Type 2 History..."
	cd dbt && dbt snapshot --profiles-dir .

dbt-run:
	@echo "Executing Transformation Lineage..."
	cd dbt && dbt run --profiles-dir .

dbt-test:
	@echo "Validating Data Contracts..."
	cd dbt && dbt test --profiles-dir .

dbt-docs:
	cd dbt && dbt docs generate --profiles-dir .
	cd dbt && dbt docs serve --port 8085 --profiles-dir .

# --- Verification ---
verify-pipeline:
	@echo "Running Pipeline Integrity Check..."
	PYTHONPATH=. python scripts/verify_pipeline.py --check-all

# --- Spark & Delta ---
spark-bronze:
	$(SPARK_SUBMIT) /opt/spark/jobs/spark/bronze_writer.py --table all

spark-silver:
	$(SPARK_SUBMIT) /opt/spark/jobs/spark/silver_transform.py

spark-gold:
	$(SPARK_SUBMIT) /opt/spark/jobs/spark/gold_aggregation.py

spark-reconcile:
	$(SPARK_SUBMIT) /opt/spark/jobs/spark/reconciliation.py --save

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name .pytest_cache -exec rm -rf {} +
	rm -rf flink/target .coverage dbt/dbt_packages dbt/target