.PHONY: help dev-up dev-down clean topics setup-test lint test-unit test-all \
        verify-pipeline run run-full flink-build flink-submit \
        spark-bronze spark-silver spark-gold spark-reconcile dbt-deps dbt-seed dbt-snapshot \
        dbt-run dbt-test dbt-docs schema-register schema-check schema-list \
        feature-api feature-sync feature-verify feature-test \
        serving-api train-model serving-verify serving-test

# --- Configuration ---
# Loads credentials and service names from infra/local/.env
-include infra/local/.env

# Local connectivity constants
REGISTRY_URL := $(if $(SCHEMA_REGISTRY_URL),$(SCHEMA_REGISTRY_URL),http://localhost:8081)
SPARK_PKGS := "io.delta:delta-spark_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.6.0"

# --- Environment Contexts ---

# LOCAL_ENV: For tools running directly on your Mac (dbt, API, Scripts, Serving)
LOCAL_ENV := POSTGRES_USER=$(POSTGRES_USER) \
             POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) \
             POSTGRES_DB=$(POSTGRES_DB) \
             POSTGRES_HOST=127.0.0.1 \
             POSTGRES_PORT=$(POSTGRES_PORT) \
             POSTGRES_SCHEMA=marts \
             REDIS_HOST=127.0.0.1 \
             REDIS_PORT=$(REDIS_PORT) \
             REDIS_PASSWORD=$(REDIS_PASSWORD) \
             SCHEMA_REGISTRY_URL=$(REGISTRY_URL) \
             FEATURE_API_URL=http://localhost:8000 \
             SERVING_API_URL=http://localhost:8001 \
             OTLP_ENDPOINT=http://localhost:4317

# DB_ENV: For tools running inside Docker (Spark)
DB_ENV := POSTGRES_USER=$(POSTGRES_USER) \
          POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) \
          POSTGRES_DB=$(POSTGRES_DB) \
          POSTGRES_HOST=$(POSTGRES_HOST) \
          POSTGRES_PORT=$(POSTGRES_PORT) \
          SCHEMA_REGISTRY_URL=$(REGISTRY_URL)

# dbt execution context (Mac-based)
DBT := cd dbt && DBT_PROFILES_DIR=. $(LOCAL_ENV) dbt

# Spark execution wrapper (Container-based)
SPARK_SUBMIT := docker exec -it --env-file infra/local/.env spark-master /usr/bin/env PYTHONPATH=/opt/spark/jobs /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=spark-master \
  --total-executor-cores 1 \
  --executor-memory 512M \
  --packages $(SPARK_PKGS)
  
help:
	@echo "TransitFlow - Unified Pipeline Control"
	@echo ""
	@echo "Quality & Validation:"
	@echo "  make lint               Auto-format and check PEP8 compliance"
	@echo "  make test-all           Run all unit tests (Registry, Contracts, Spark, Feature Store, ML)"
	@echo "  make verify-pipeline    Run complete end-to-end integrity suite"
	@echo ""
	@echo "Infrastructure:"
	@echo "  make dev-up             Launch containers and initialize environment"
	@echo "  make dev-down           Stop and remove all local services"
	@echo "  make topics             Initialize Kafka topics and partitions"
	@echo ""
	@echo "Data Contracts & Registry:"
	@echo "  make schema-register    Sync local schemas with Registry"
	@echo "  make schema-check       Verify remote schema compatibility"
	@echo "  make schema-list        List currently registered subjects"
	@echo ""
	@echo "Streaming & Ingestion:"
	@echo "  make run                Run Ingestion Bridge (filtered line 600)"
	@echo "  make run-full           Run Ingestion Bridge (all traffic)"
	@echo "  make flink-submit       Deploy enrichment job to Flink cluster"
	@echo ""
	@echo "Batch Processing (Spark):"
	@echo "  make spark-bronze       Stream Kafka data to Delta Lake Bronze"
	@echo "  make spark-silver       Run Silver-layer cleaning transformations"
	@echo "  make spark-gold         Execute Gold-layer business aggregations"
	@echo "  make spark-reconcile    Run data reconciliation between Bronze and Silver"
	@echo ""
	@echo "Warehouse & Transformation (dbt):"
	@echo "  make dbt-seed           Load static reference data (GTFS)"
	@echo "  make dbt-snapshot       Execute SCD Type 2 history capture"
	@echo "  make dbt-run            Run transformation lineage (Staging to Marts)"
	@echo "  make dbt-test           Execute data contract validation tests"
	@echo "  make dbt-docs           Generate and serve data catalog/lineage"
	@echo ""
	@echo "Feature Store & ML Serving (Phase 5 & 6):"
	@echo "  make feature-api        Launch FastAPI Feature Serving API (Port 8000)"
	@echo "  make feature-sync       Sync Delta Lake Gold to PostgreSQL Marts"
	@echo "  make feature-verify     Verify Feature Store integration"
	@echo "  make train-model        Execute ML Training Pipeline (XGBoost)"
	@echo "  make serving-api        Launch ML Prediction API (Port 8001)"
	@echo "  make serving-verify     Run comprehensive Phase 6 verification"
	@echo "  make serving-test       Run unit tests for ML and Serving"

# --- Infrastructure Management ---
dev-up:
	@if [ ! -f infra/local/.env ]; then cp infra/local/.env.example infra/local/.env; fi
	cd infra/local && docker compose up -d
	@echo "Initializing services..."
	@sleep 15
	@$(MAKE) topics

dev-down:
	cd infra/local && docker compose down

topics:
	@echo "Creating Kafka topics..."
	docker exec -it redpanda rpk topic create fleet.telemetry.raw fleet.enriched fleet.stop_events --partitions 8 2>/dev/null || true
	docker exec -it redpanda rpk topic create fleet.telemetry.dlq --partitions 1 2>/dev/null || true

# --- Quality Assurance ---
.PHONY: lint
lint:
	@echo "Running Import Sort (isort)..."
	isort feature_store/ serving/ ml_pipeline/ scripts/ tests/ src/
	
	@echo "Running Formatter (black)..."
	black feature_store/ serving/ ml_pipeline/ scripts/ tests/ src/
	
	@echo "Running Linter (ruff)..."
	ruff check feature_store/ serving/ ml_pipeline/ scripts/ tests/ src/ --fix
	
	@echo "Running Security Scan (bandit)..."
	# -r: recursive, -ll: only high severity, -c: use config from pyproject.toml
	bandit -r feature_store/ serving/ ml_pipeline/ scripts/ src/ -c pyproject.toml
	
	@echo "Running Static Type Checker (mypy)..."
	mypy feature_store/ serving/ ml_pipeline/ scripts/ src/

test-all:
	PYTHONPATH=$(CURDIR) pytest tests/unit/ -v

# --- Ingestion Control ---
run:
	PYTHONPATH=$(CURDIR) python3 scripts/run_bridge.py --line 600

run-full:
	PYTHONPATH=$(CURDIR) python3 scripts/run_bridge.py

# --- Streaming Enrichment (Flink) ---
flink-build:
	cd flink && mvn clean package -DskipTests
	@mkdir -p infra/local/flink-jobs
	cp flink/target/transitflow-flink-1.0.0.jar infra/local/flink-jobs/

flink-submit:
	docker exec -it flink-jobmanager flink run -d /opt/flink/jobs/transitflow-flink-1.0.0.jar

# --- Delta Lake Processing (Spark) ---
spark-bronze:
	$(SPARK_SUBMIT) /opt/spark/jobs/spark/bronze_writer.py --table all

spark-silver:
	$(SPARK_SUBMIT) /opt/spark/jobs/spark/silver_transform.py

spark-gold:
	$(SPARK_SUBMIT) /opt/spark/jobs/spark/gold_aggregation.py

spark-reconcile:
	$(SPARK_SUBMIT) /opt/spark/jobs/spark/reconciliation.py --save

# --- Data Contracts & Warehouse (dbt) ---
schema-register:
	@echo "Registering Avro definitions..."
	PYTHONPATH=$(CURDIR) $(LOCAL_ENV) python3 scripts/register_schemas.py

schema-check:
	@echo "Performing compatibility check..."
	PYTHONPATH=$(CURDIR) $(LOCAL_ENV) python3 scripts/register_schemas.py --check-only

schema-list:
	@curl -s $(REGISTRY_URL)/subjects | python3 -m json.tool

dbt-deps:
	$(DBT) deps

dbt-seed:
	$(DBT) seed

dbt-snapshot:
	$(DBT) snapshot

dbt-run:
	$(DBT) run

dbt-test:
	$(DBT) test

dbt-docs:
	$(DBT) docs generate
	@echo "Serving documentation at http://localhost:8085"
	$(DBT) docs serve --port 8085

# --- Feature Store & ML Serving (Phase 5 & 6) ---
feature-api:
	PYTHONPATH=$(CURDIR) $(LOCAL_ENV) python3 feature_store/api.py

feature-sync:
	$(SPARK_SUBMIT) /opt/spark/jobs/feature_store/feature_sync.py

feature-verify:
	PYTHONPATH=$(CURDIR) $(LOCAL_ENV) python3 scripts/verify_feature_store.py --check-all

feature-test:
	PYTHONPATH=$(CURDIR) pytest tests/unit/feature_store/ -v

train-model:
	@echo "Starting ML Training Pipeline..."
	$(SPARK_SUBMIT) /opt/spark/jobs/scripts/train_model.py

serving-api:
	PYTHONPATH=$(CURDIR) $(LOCAL_ENV) python3 serving/api.py

serving-verify:
	@echo "Running Phase 6 Smoke Tests..."
	PYTHONPATH=$(CURDIR) $(LOCAL_ENV) python3 scripts/smoke_test_ml_serving.py --check-all

serving-test:
	PYTHONPATH=$(CURDIR) pytest tests/unit/ml_pipeline/ tests/unit/serving/ -v

# --- Orchestrated Verification ---
verify-pipeline:
	@echo "Running end-to-end integrity suite..."
	PYTHONPATH=$(CURDIR) $(LOCAL_ENV) python3 scripts/verify_pipeline.py --check-all

# --- Storage & Lakehouse Utilities ---
lakehouse-ls:
	@docker exec -it minio sh -c "mc alias set myminio http://localhost:9000 minioadmin minioadmin > /dev/null && mc ls -r myminio/transitflow-lakehouse/"

# --- Maintenance ---
clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name .pytest_cache -exec rm -rf {} +
	rm -rf flink/target .coverage dbt/dbt_packages dbt/target