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

    def test_stg_enriched_mirrors_spark(self):
        """
        Ensures staging models mirror Spark Silver exactly.
        Hardening: delay_category IS allowed because it's a Spark-native field.
        """
        config_path = DBT_DIR / "models" / "staging" / "_staging.yml"
        with open(config_path) as f:
            config = yaml.safe_load(f)

        models = config.get("models", [])
        stg_enriched = next((m for m in models if m["name"] == "stg_enriched"), None)
        columns = [c["name"] for c in stg_enriched.get("columns", [])]

        # We now EXPECT delay_category because Spark provides it.
        assert "delay_category" in columns, "Mirroring Error: delay_category missing from staging"

    def test_stg_stop_events_names(self):
        """Ensures stop events use Spark-native names (No Jugglery)."""
        config_path = DBT_DIR / "models" / "staging" / "_staging.yml"
        with open(config_path) as f:
            config = yaml.safe_load(f)

        models = config.get("models", [])
        stg_stop = next((m for m in models if m["name"] == "stg_stop_events"), None)
        columns = [c["name"] for c in stg_stop.get("columns", [])]

        # Must use Spark name, not 'sample_count'
        assert "historical_arrival_count" in columns
        assert "stop_lat" in columns

class TestDimensionContracts:
    """Tests for dimension model contracts."""

    def test_dimension_config_exists(self):
        """Dimension configuration file exists."""
        config_path = DBT_DIR / "models" / "dimensions" / "_dimensions.yml"
        assert config_path.exists(), f"Missing: {config_path}"

    def test_dim_stops_has_scd_columns(self):
        """dim_stops has SCD Type 2 columns."""
        config_path = DBT_DIR / "models" / "dimensions" / "_dimensions.yml"
        with open(config_path) as f:
            config = yaml.safe_load(f)

        models = config.get("models", [])
        dim_stops = next((m for m in models if m["name"] == "dim_stops"), None)
        assert dim_stops is not None
        column_names = [c["name"] for c in dim_stops.get("columns", [])]

        scd_columns = ["valid_from", "valid_to", "is_current", "stop_key"]
        for col in scd_columns:
            assert col in column_names, f"Missing SCD column: {col}"

    def test_dim_stops_spatial_names(self):
        """Ensures dim_stops uses stop_lat/lon (No Renaming)."""
        config_path = DBT_DIR / "models" / "dimensions" / "_dimensions.yml"
        with open(config_path) as f:
            config = yaml.safe_load(f)

        models = config.get("models", [])
        dim_stops = next((m for m in models if m["name"] == "dim_stops"), None)
        column_names = [c["name"] for c in dim_stops.get("columns", [])]

        assert "stop_lat" in column_names, "Renaming Massacre: latitude found"
        assert "stop_lon" in column_names

    def test_dim_stops_key_unique(self):
        """dim_stops surrogate key is unique."""
        config_path = DBT_DIR / "models" / "dimensions" / "_dimensions.yml"
        with open(config_path) as f:
            config = yaml.safe_load(f)

        models = config.get("models", [])
        dim_stops = next((m for m in models if m["name"] == "dim_stops"), None)
        columns = dim_stops.get("columns", [])
        stop_key = next((c for c in columns if c["name"] == "stop_key"), None)
        tests = stop_key.get("data_tests", []) # Updated to 'data_tests' for dbt v1.8+

        assert "unique" in tests
        assert "not_null" in tests

class TestMartContracts:
    """Tests for mart model contracts (fact tables)."""

    def test_fct_stop_arrivals_has_metric_contract(self):
        """Ensure the mart uses Spark-native metric names."""
        config_path = DBT_DIR / "models" / "marts" / "_marts.yml"
        with open(config_path) as f:
            config = yaml.safe_load(f)

        fct = next((m for m in config.get("models", []) if m["name"] == "fct_stop_arrivals"), None)
        columns = [c["name"] for c in fct.get("columns", [])]

        assert "historical_arrival_count" in columns
        assert "stop_lat" in columns

    def test_fct_stop_arrivals_unique_grain(self):
        """Ensures the mart has a unique combination test for the grain."""
        config_path = DBT_DIR / "models" / "marts" / "_marts.yml"
        with open(config_path) as f:
            config = yaml.safe_load(f)

        fct = next((m for m in config.get("models", []) if m["name"] == "fct_stop_arrivals"), None)
        tests = fct.get("data_tests", [])
        
        unique_test = next((t for t in tests if "dbt_utils.unique_combination_of_columns" in str(t)), None)
        assert unique_test is not None

class TestSourceContracts:
    """Tests for source definitions."""

    def test_sources_file_exists(self):
        config_path = DBT_DIR / "models" / "sources.yml"
        assert config_path.exists()

    def test_lakehouse_source_has_freshness(self):
        """Lakehouse source has freshness SLO."""
        config_path = DBT_DIR / "models" / "sources.yml"
        with open(config_path) as f:
            config = yaml.safe_load(f)

        sources = config.get("sources", [])
        lakehouse = next((s for s in sources if s["name"] == "raw_lakehouse"), None)
        
        # Check table-level freshness from our sources.yml audit
        table = lakehouse["tables"][0]
        assert "freshness" in table

class TestProjectConfig:
    """Tests for dbt project configuration."""

    def test_project_has_vars(self):
        project_path = DBT_DIR / "dbt_project.yml"
        with open(project_path) as f:
            config = yaml.safe_load(f)

        vars = config.get("vars", {})
        assert "on_time_threshold" in vars
        assert "scd_end_date" in vars # UTC Lock awareness

    def test_models_have_materialization(self):
        project_path = DBT_DIR / "dbt_project.yml"
        with open(project_path) as f:
            config = yaml.safe_load(f)

        models = config.get("models", {}).get("transitflow", {})
        assert models.get("staging", {}).get("+materialized") == "view"
        assert models.get("marts", {}).get("+materialized") == "table"