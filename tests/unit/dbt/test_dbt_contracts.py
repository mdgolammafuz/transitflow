"""
Unit tests for dbt data contracts.
Pattern: DE#3 - Quality as Firewall
Tests validate that dbt contract definitions are correct.
Run with: pytest tests/unit/dbt/test_dbt_contracts.py -v
"""

from pathlib import Path
import yaml
import pytest

# Path to dbt project
DBT_DIR = Path(__file__).parent.parent.parent.parent / "dbt"

class TestStagingContracts:
    """Tests for staging model contracts."""

    def test_staging_config_exists(self):
        config_path = DBT_DIR / "models" / "staging" / "_staging.yml"
        assert config_path.exists()

    def test_stg_enriched_renaming_fix(self):
        """
        Ensures we fixed the Renaming Hell.
        We expect 'distance_since_last_meters', NOT 'ms'.
        """
        config_path = DBT_DIR / "models" / "staging" / "_staging.yml"
        with open(config_path) as f:
            config = yaml.safe_load(f)

        models = config.get("models", [])
        stg_enriched = next((m for m in models if m["name"] == "stg_enriched"), None)
        columns = [c["name"] for c in stg_enriched.get("columns", [])]

        assert "distance_since_last_meters" in columns, "Renaming Regression: 'meters' unit missing"
        assert "distance_since_last_ms" not in columns, "Renaming Regression: 'ms' unit still present"
    

    def test_stg_stop_events_names(self):
        """Ensures stop events use Spark-native names."""
        config_path = DBT_DIR / "models" / "staging" / "_staging.yml"
        with open(config_path) as f:
            config = yaml.safe_load(f)

        models = config.get("models", [])
        stg_stop = next((m for m in models if m["name"] == "stg_stop_events"), None)
        columns = [c["name"] for c in stg_stop.get("columns", [])]

        assert "historical_arrival_count" in columns
        assert "stop_lat" in columns

class TestDimensionContracts:
    """Tests for dimension model contracts."""

    def test_dim_stops_has_scd_columns(self):
        config_path = DBT_DIR / "models" / "dimensions" / "_dimensions.yml"
        with open(config_path) as f:
            config = yaml.safe_load(f)

        models = config.get("models", [])
        dim_stops = next((m for m in models if m["name"] == "dim_stops"), None)
        column_names = [c["name"] for c in dim_stops.get("columns", [])]

        scd_columns = ["valid_from", "valid_to", "is_current", "stop_key"]
        for col in scd_columns:
            assert col in column_names, f"Missing SCD column: {col}"

    def test_dim_stops_spatial_names(self):
        config_path = DBT_DIR / "models" / "dimensions" / "_dimensions.yml"
        with open(config_path) as f:
            config = yaml.safe_load(f)

        models = config.get("models", [])
        dim_stops = next((m for m in models if m["name"] == "dim_stops"), None)
        column_names = [c["name"] for c in dim_stops.get("columns", [])]

        assert "stop_lat" in column_names, "Renaming Massacre: latitude found"

class TestMartContracts:
    """Tests for mart model contracts."""

    def test_fct_stop_arrivals_has_metric_contract(self):
        config_path = DBT_DIR / "models" / "marts" / "_marts.yml"
        with open(config_path) as f:
            config = yaml.safe_load(f)

        fct = next((m for m in config.get("models", []) if m["name"] == "fct_stop_arrivals"), None)
        columns = [c["name"] for c in fct.get("columns", [])]

        assert "historical_arrival_count" in columns
        assert "stop_lat" in columns

    def test_fct_daily_performance_restored_column(self):
        """Ensures we restored the missing speed column."""
        config_path = DBT_DIR / "models" / "marts" / "_marts.yml"
        with open(config_path) as f:
            config = yaml.safe_load(f)
        
        fct = next((m for m in config.get("models", []) if m["name"] == "fct_daily_performance"), None)
        columns = [c["name"] for c in fct.get("columns", [])]
        
        assert "avg_speed_kmh" in columns, "Data Loss: avg_speed_kmh missing from mart contract"

class TestProjectConfig:
    """Tests for dbt project configuration."""

    def test_project_has_correct_vars(self):
        project_path = DBT_DIR / "dbt_project.yml"
        with open(project_path) as f:
            config = yaml.safe_load(f)

        vars = config.get("vars", {})
        # Variable name matches classify_delay macro
        assert "on_time_threshold_seconds" in vars, "Variable Mismatch: on_time_threshold_seconds missing"