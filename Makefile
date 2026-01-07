.PHONY: help dev-up dev-down test verify run run-full clean flink-build flink-submit spark-bronze spark-silver spark-gold spark-maintenance spark-reconcile flink-test flink-cancel flink-verify topics

# --- Configuration ---
SPARK_PKGS := "io.delta:delta-spark_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4"

# We limit each job to 1 core and 1GB RAM so multiple jobs (Bronze/Silver) can run simultaneously
SPARK_SUBMIT := docker exec -it spark-master /opt/spark/bin/spark-submit \
	--master spark://spark-master:7077 \
	--conf spark.driver.host=spark-master \
	--total-executor-cores 1 \
	--executor-memory 1G \
	--packages $(SPARK_PKGS)

help:
	@echo "TransitFlow - Full Project Commands"
	@echo ""
	@echo "Infrastructure:"
	@echo "  make dev-up        Start all local services (Redpanda, MinIO, Spark, Flink, PG)"
	@echo "  make dev-down      Stop and remove local containers"
	@echo "  make topics        Manually initialize Kafka topics"
	@echo ""
	@echo "Phase 1 - Ingestion & Bridge:"
	@echo "  make test          Run Python unit tests for ingestion logic"
	@echo "  make verify        Run Phase 1 verification script (Raw Telemetry)"
	@echo "  make run           Run ingestion bridge (Line 600 only)"
	@echo "  make run-full      Run ingestion bridge (All Helsinki traffic)"
	@echo ""
	@echo "Phase 2 - Flink Stream Processing:"
	@echo "  make flink-build   Build Java Flink JAR (Maven)"
	@echo "  make flink-test    Run Flink unit tests"
	@echo "  make flink-submit  Submit Enrichment Job to Flink cluster"
	@echo "  make flink-verify  Verify Flink output in Redpanda"
	@echo ""
	@echo "Phase 3 - Spark & Delta Lakehouse:"
	@echo "  make spark-bronze  Run Stream: Kafka -> Bronze (Raw Parquet/Delta)"
	@echo "  make spark-silver  Run Batch: Bronze -> Silver (Cleaned/Deduplicated)"
	@echo "  make spark-gold    Run Batch: Silver -> Gold (Business Aggregates)"
	@echo "  make spark-reconcile Run Audit: Bronze vs Silver count verification"
	@echo "  make spark-maintenance Run VACUUM/OPTIMIZE on Delta tables"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean         Remove __pycache__ and build artifacts"

# --- Infrastructure Management ---
dev-up:
	@if [ ! -f infra/local/.env ]; then \
		echo "Creating .env from template..."; \
		cp infra/local/.env.example infra/local/.env; \
	fi
	cd infra/local && docker compose up -d
	@echo "Waiting for services to stabilize..."
	@sleep 15
	@make topics
	@echo "Spark Master UI: http://localhost:8083"
	@echo "Flink UI: http://localhost:8082"

dev-down:
	cd infra/local && docker compose down

topics:
	@echo "Creating Kafka topics..."
	docker exec -it redpanda rpk topic create fleet.telemetry.raw fleet.enriched fleet.stop_events --partitions 8 2>/dev/null || true
	docker exec -it redpanda rpk topic create fleet.telemetry.dlq --partitions 1 2>/dev/null || true
	docker exec -it redpanda rpk topic list

# --- Phase 1: Ingestion ---
test:
	PYTHONPATH=. pytest tests/unit/ -v

verify:
	PYTHONPATH=. python scripts/verify_phase1.py --full

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
	docker exec -it flink-jobmanager flink run -d /opt/flink/jobs/transitflow-flink-1.0.0.jar

flink-verify:
	@echo "Checking Redpanda for enriched messages..."
	docker exec -it redpanda rpk topic consume fleet.enriched -n 5

# --- Phase 3: Spark & Delta Lake ---
spark-bronze:
	@echo "Starting Spark Bronze Streaming Writer..."
	$(SPARK_SUBMIT) /opt/spark/jobs/bronze_writer.py --table all

spark-silver:
	@echo "Running Silver Transformation..."
	$(SPARK_SUBMIT) /opt/spark/jobs/silver_transform.py

spark-gold:
	@echo "Running Gold Aggregation..."
	$(SPARK_SUBMIT) /opt/spark/jobs/gold_aggregation.py

spark-maintenance:
	@echo "Running Maintenance (VACUUM/OPTIMIZE)..."
	$(SPARK_SUBMIT) /opt/spark/jobs/maintenance.py

spark-reconcile:
	@echo "Running Data Reconciliation..."
	$(SPARK_SUBMIT) /opt/spark/jobs/reconciliation.py --save

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	rm -rf flink/target