.PHONY: help dev-up dev-down clean topics setup-test lint test-unit test-all \
	      verify-pipeline run run-full flink-build flink-submit \
	      spark-bronze spark-silver spark-gold spark-reconcile dbt-deps dbt-seed dbt-snapshot \
	      dbt-run dbt-test dbt-docs schema-register schema-check schema-list \
	      feature-api feature-sync feature-verify feature-test \
	      serving-api train-model serving-verify serving-test lakehouse-ls metadata-init

# --- Configuration ---
# Loads credentials and service names from infra/local/.env
-include infra/local/.env
# Target date for manual verification and OCI Cron context
DATE ?= $(shell date +%Y-%m-%d)
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
# Explicitly overrides HOST environment variables for the internal Docker network context.
# This prevents 'Connection Refused' by ensuring Spark looks for 'minio' and 'postgres' containers, not itself.
SPARK_SUBMIT := docker exec -it --env-file infra/local/.env \
	-e KAFKA_BOOTSTRAP_SERVERS=redpanda:29092 \
	-e POSTGRES_HOST=postgres \
	-e MINIO_ENDPOINT=http://minio:9000 \
	spark-master /usr/bin/env PYTHONPATH=/opt/spark/jobs /opt/spark/bin/spark-submit \
	--master spark://spark-master:7077 \
	--conf spark.driver.host=spark-master \
	--total-executor-cores 1 \
	--executor-memory 2G \
	--driver-memory 1G \
	--packages $(SPARK_PKGS)

# --- The S3A Bridge ---
# We use the variable names as they appear inside our .env/container
S3A_CONF := --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
	          --conf spark.hadoop.fs.s3a.access.key=$${MINIO_ROOT_USER} \
	          --conf spark.hadoop.fs.s3a.secret.key=$${MINIO_ROOT_PASSWORD} \
	          --conf spark.hadoop.fs.s3a.path.style.access=true \
	          --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
	          --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false

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
	@echo "  make metadata-init      Initialize Gold Metadata (Ingredient B) from Postgres"
	@echo "  make spark-gold         Execute Gold-layer business aggregations (depends on metadata)"
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
	@echo "Ensuring Lakehouse bucket exists..."
	@docker exec -it minio mc alias set local http://localhost:9000 minioadmin minioadmin > /dev/null
	@docker exec -it minio mc mb local/$(LAKEHOUSE_BUCKET) || true

dev-down:
	cd infra/local && docker compose down

topics:
	@echo "Creating Kafka topics..."
	docker exec -it redpanda rpk topic create fleet.telemetry.raw fleet.enriched fleet.stop_events --partitions 8 2>/dev/null || true
	docker exec -it redpanda rpk topic create fleet.telemetry.dlq --partitions 1 2>/dev/null || true

# --- Quality Assurance ---
lint:
	@echo "Running Import Sort (isort)..."
	isort feature_store/ serving/ ml_pipeline/ scripts/ tests/ src/
	@echo "Running Formatter (black)..."
	black feature_store/ serving/ ml_pipeline/ scripts/ tests/ src/
	@echo "Running Linter (ruff)..."
	ruff check feature_store/ serving/ ml_pipeline/ scripts/ tests/ src/ --fix
	@echo "Running Security Scan (bandit)..."
	bandit -r feature_store/ serving/ ml_pipeline/ scripts/ src/ -c pyproject.toml
	@echo "Running Static Type Checker (mypy)..."
	mypy feature_store/ serving/ ml_pipeline/ scripts/ src/

test-all:
	PYTHONPATH=$(CURDIR) pytest tests/unit/ -v

# --- Ingestion Control ---
run:
	PYTHONPATH=$(CURDIR) $(LOCAL_ENV) python3 scripts/run_bridge.py --line 600

run-full:
	PYTHONPATH=$(CURDIR) $(LOCAL_ENV) python3 scripts/run_bridge.py

# Verification tool
verify-ingestion:
	PYTHONPATH=$(CURDIR) $(LOCAL_ENV) python3 scripts/verify_ingestion.py --full

# Run ONLY ingestion unit tests
test-ingestion:
	PYTHONPATH=$(CURDIR) pytest tests/unit/ingestion/ -v

# --- Streaming Enrichment (Flink) ---

# Build the JAR and move it to the shared volume path
flink-build:
	cd flink && mvn clean package -DskipTests
	@mkdir -p infra/local/flink-jobs
	cp flink/target/transitflow-flink-1.0.0.jar infra/local/flink-jobs/

# List all running jobs to the console
flink-list:
	docker exec -it flink-jobmanager flink list

# Stop all running jobs (DANGER: Stops everything in the local cluster)
flink-stop:
	@echo "Stopping all running Flink jobs..."
	docker exec -it flink-jobmanager bash -c "flink list | grep RUNNING | cut -d ' ' -f 4 | xargs -r flink cancel"

# Clean start: Build, Stop existing, and Submit new
flink-deploy: flink-build flink-stop flink-submit

# Submit the job to the JobManager
flink-submit:
	docker exec -it flink-jobmanager flink run -d /opt/flink/jobs/transitflow-flink-1.0.0.jar

# --- Delta Lake Processing (Spark) ---

# 1. Kafka to MinIO (Streaming)
spark-bronze:
	@echo "Starting Streaming Bronze Writer..."
	$(SPARK_SUBMIT) /opt/spark/jobs/spark/bronze_writer.py --table all

# 2. MinIO to Postgres (The Bridge)
spark-sync:
	@echo "Syncing Lakehouse (MinIO) to Postgres Bronze for $(DATE)..."
	$(SPARK_SUBMIT) $(S3A_CONF) /opt/spark/jobs/spark/sync_bronze_to_postgres.py --date $(DATE)

# 3. Bronze to Silver (Cleaning)
spark-silver:
	@echo "Transforming Bronze to Silver for $(DATE)..."
	$(SPARK_SUBMIT) $(S3A_CONF) /opt/spark/jobs/spark/silver_transform.py --date $(DATE)

# 4. Initialize Metadata
metadata-init:
	@echo "Initializing Gold Metadata Layer from Postgres..."
	$(SPARK_SUBMIT) $(S3A_CONF) /opt/spark/jobs/spark/initialize_metadata.py

# 5. Silver to Gold (Aggregation)
spark-gold:
	@echo "Running Gold Aggregations for $(DATE) ..."
	$(SPARK_SUBMIT) $(S3A_CONF) /opt/spark/jobs/spark/gold_aggregation.py --date $(DATE)

# 6. Quality & Maintenance
spark-reconcile:
	@echo "Running Reconciliation for $(DATE)..."
	$(SPARK_SUBMIT) $(S3A_CONF) /opt/spark/jobs/spark/reconciliation.py --date $(DATE) --save

spark-maintenance:
	@echo "Running Lakehouse Maintenance for $(DATE)..."
	$(SPARK_SUBMIT) $(S3A_CONF) /opt/spark/jobs/spark/maintenance.py --date $(DATE) --action all
	  
# 7. Targeted Unit Tests
spark-test:
	@echo "Running Spark Unit Tests inside container..."
	docker exec -it spark-master /usr/local/bin/pytest /opt/spark/jobs/tests/unit/spark/

# --- Data Contracts & Warehouse (dbt) ---
# --- Data Contracts & Warehouse (dbt) ---

# Variable for date-partitioned runs (e.g., make dbt-run DATE=2026-01-18)
# Defaults to today if not provided
DATE ?= $(shell date +%Y-%m-%d)

# Common environment variables for all dbt commands to ensure connectivity
DBT_ENV = cd dbt && DBT_PROFILES_DIR=. \
	POSTGRES_USER=$(POSTGRES_USER) \
	POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) \
	POSTGRES_DB=$(POSTGRES_DB) \
	POSTGRES_HOST=$(POSTGRES_HOST) \
	POSTGRES_PORT=$(POSTGRES_PORT) \
	POSTGRES_SCHEMA=$(POSTGRES_SCHEMA) \
	REDIS_HOST=$(REDIS_HOST) \
	REDIS_PORT=$(REDIS_PORT) \
	REDIS_PASSWORD=$(REDIS_PASSWORD) \
	SCHEMA_REGISTRY_URL=$(SCHEMA_REGISTRY_URL) \
	FEATURE_API_URL=$(FEATURE_API_URL) \
	SERVING_API_URL=$(SERVING_API_URL) \
	OTLP_ENDPOINT=$(OTLP_ENDPOINT)

# --- Quality Gates (Contracts & Registry) ---

# 1. Unit Tests (Code Logic for Registry & dbt Contracts)
test-contracts:
	@echo "Running Contract Unit Tests..."
	PYTHONPATH=$(CURDIR) pytest tests/unit/schema_registry/ tests/unit/dbt/ -v

# 2. Schema Registry Controls
schema-register:
	@echo "Registering Avro definitions..."
	PYTHONPATH=$(CURDIR) $(LOCAL_ENV) python3 scripts/register_schemas.py --schema-dir schemas/avro

schema-check:
	@echo "Performing compatibility check..."
	PYTHONPATH=$(CURDIR) $(LOCAL_ENV) python3 scripts/register_schemas.py --schema-dir schemas/avro --check-only

schema-list:
	@curl -s $(REGISTRY_URL)/subjects | python3 -m json.tool

# --- dbt Execution ---

dbt-deps:
	$(DBT_ENV) dbt deps

dbt-seed:
	$(DBT_ENV) dbt seed

dbt-snapshot:
	$(DBT_ENV) dbt snapshot

# Professional Run: Injecting the target_date into the dbt variable context
dbt-run:
	$(DBT_ENV) dbt run --vars "{'target_date': '$(DATE)'}"

# Full Refresh: Used when logic in Discovery Dimensions (dim_stops) changes
dbt-refresh:
	$(DBT_ENV) dbt run --full-refresh --vars "{'target_date': '$(DATE)'}"

dbt-test:
	$(DBT_ENV) dbt test --vars "{'target_date': '$(DATE)'}"

dbt-docs:
	$(DBT_ENV) dbt docs generate
	@echo "Serving documentation at http://localhost:8085"
	$(DBT_ENV) dbt docs serve --port 8085

# --- Orchestrated Verification ---

# Verifies Spark, MinIO, and Delta Parquet files (Storage Layer)
# REQUIRES DATE: Checks for existence of specific partitions
verify-lakehouse:
	@echo "Verifying Lakehouse Storage layers for $(DATE)..."
	PYTHONPATH=$(CURDIR) $(LOCAL_ENV) python3 scripts/verify_lakehouse.py --date $(DATE)

# Verifies Schema Registry, dbt connectivity, and Model integrity (Logic Layer)
# NO DATE NEEDED: Checks system configuration and contracts
verify-pipeline:
	@echo "Verifying Pipeline Integrity..."
	PYTHONPATH=$(CURDIR) $(LOCAL_ENV) python3 scripts/verify_pipeline.py --check-all
	
# --- Storage & Lakehouse Utilities ---
lakehouse-ls:
	@docker exec -it minio /usr/bin/mc alias set myminio http://localhost:9000 $(MINIO_ROOT_USER) $(MINIO_ROOT_PASSWORD) > /dev/null
	@docker exec -it minio /usr/bin/mc ls -r myminio/$(LAKEHOUSE_BUCKET)/


# --- Feature Store & ML Serving ---
feature-api:
	PYTHONPATH=$(CURDIR) $(LOCAL_ENV) python3 feature_store/api.py

# Optimized: Pass $(DATE) as a CLI argument to the Python script inside Spark
feature-sync:
	$(SPARK_SUBMIT) \
		--master "local[*]" \
		--conf spark.driver.host=localhost \
		--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
		--conf spark.hadoop.fs.s3a.access.key=$(MINIO_ROOT_USER) \
		--conf spark.hadoop.fs.s3a.secret.key=$(MINIO_ROOT_PASSWORD) \
		--conf spark.hadoop.fs.s3a.path.style.access=true \
		--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
		--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
		/opt/spark/jobs/feature_store/feature_sync.py $(DATE)

feature-verify:
	PYTHONPATH=$(CURDIR) $(LOCAL_ENV) python3 scripts/verify_feature_store.py --check-all

feature-test:
	PYTHONPATH=$(CURDIR) pytest tests/unit/feature_store/ -v

# Runs locally with Postgres connection
train-model:
	@echo "Starting ML Training Pipeline (Postgres -> XGBoost)..."
	PYTHONPATH=$(CURDIR) \
	MLFLOW_TRACKING_URI=http://localhost:5001 \
	MLFLOW_S3_ENDPOINT_URL=http://localhost:9000 \
	MLFLOW_S3_IGNORE_TLS=true \
	AWS_ACCESS_KEY_ID=$(MINIO_ROOT_USER) \
	AWS_SECRET_ACCESS_KEY=$(MINIO_ROOT_PASSWORD) \
	AWS_DEFAULT_REGION=us-east-1 \
	NO_PROXY=localhost,127.0.0.1,0.0.0.0 \
	POSTGRES_HOST=127.0.0.1 \
	$(LOCAL_ENV) python3 scripts/verify_train_model.py
	
serving-api:
	@echo "Starting ML Serving API..."
	PYTHONPATH=$(CURDIR) \
	MLFLOW_TRACKING_URI=http://localhost:5001 \
	MLFLOW_S3_ENDPOINT_URL=http://localhost:9000 \
	MLFLOW_S3_IGNORE_TLS=true \
	AWS_ACCESS_KEY_ID=$(MINIO_ROOT_USER) \
	AWS_SECRET_ACCESS_KEY=$(MINIO_ROOT_PASSWORD) \
	AWS_DEFAULT_REGION=us-east-1 \
	$(LOCAL_ENV) python3 serving/api.py


# Points to verification script
serving-verify:
	@echo "Running Phase 6 Verification..."
	PYTHONPATH=$(CURDIR) \
	MLFLOW_TRACKING_URI=http://localhost:5001 \
	MLFLOW_S3_ENDPOINT_URL=http://localhost:9000 \
	MLFLOW_S3_IGNORE_TLS=true \
	AWS_ACCESS_KEY_ID=$(MINIO_ROOT_USER) \
	AWS_SECRET_ACCESS_KEY=$(MINIO_ROOT_PASSWORD) \
	AWS_DEFAULT_REGION=us-east-1 \
	SERVING_API_URL=http://localhost:8001 \
	$(LOCAL_ENV) python3 scripts/verify_ml_serving.py --check-all
	
serving-test:
	PYTHONPATH=$(CURDIR) pytest tests/unit/ml_pipeline/ tests/unit/serving/ -v

# --- Maintenance ---
clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name .pytest_cache -exec rm -rf {} +
	rm -rf flink/target .coverage dbt/dbt_packages dbt/target