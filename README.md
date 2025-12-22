# Berlin Air Quality Predictor

**Problem**: Predict tomorrow's PM2.5 category (Good / Moderate / Unhealthy) for Berlin.

**User Story**: Help Berlin residents know if tomorrow's air will be safe for outdoor activities.

## PM2.5 Categories (WHO Guidelines)

| Category | PM2.5 (µg/m³) | Advisory |
|----------|---------------|----------|
| Good | ≤ 15 | Safe for everyone |
| Moderate | 16 - 35 | Sensitive groups limit outdoor time |
| Unhealthy | > 35 | Everyone reduce outdoor activity |

## Architecture

```
Lakehouse (Delta Lake) + Streaming (Flink/Java) + Batch (Spark/PySpark)
```

## Tech Stack

- **Ingestion**: Redpanda (Kafka), Schema Registry, Pydantic
- **Streaming**: Flink (Java)
- **Batch**: Spark (PySpark), Delta Lake
- **Storage**: Delta Lake (Bronze/Silver/Gold), PostgreSQL, Redis
- **Transform**: dbt-spark, Great Expectations
- **Orchestration**: Airflow
- **ML**: XGBoost, PyTorch, MLflow
- **Serving**: FastAPI
- **Monitoring**: Prometheus, Grafana

## Phases

| Phase | Name | Status |
|-------|------|--------|
| 1 | Data Exploration | In Progress |
| 2 | Ingestion | Pending |
| 3 | Stream + Batch + Lakehouse | Pending |
| 4 | Transform + Quality | Pending |
| 5 | Orchestration | Pending |
| 6 | ML Pipeline | Pending |
| 7 | Serve + Monitor | Pending |

## Quick Start (Phase 1)

```bash
cd aerosense
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Test API clients
python -m pytest tests/ -v

# Run exploration
python scripts/explore_data.py
```

## Data Sources

- **EEA**: European Environment Agency - Berlin PM2.5 stations
- **Open-Meteo**: Weather observations and forecasts
