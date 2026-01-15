"""
Unit tests for dbt data contracts.

Pattern: DE#3 - Quality as Firewall
Tests validate that dbt contract definitions are correct.

Run with: pytest tests/unit/dbt/test_dbt_contracts.py -v
"""

from pathlib import Path

import yaml

# Path to dbt project
DBT_DIR = Path(__file__).parent.parent.parent.parent / "dbt"


class TestStagingContracts:
    """Tests for staging model contracts."""

    def test_staging_config_exists(self):
        config_path = DBT_DIR / "models" / "staging" / "_staging.yml"
        assert config_path.exists()

    def test_stg_enriched_no_business_logic(self):
        """
        Ensures staging models do NOT contain business logic columns
        like delay_category (should be in intermediate/marts).
        """
        config_path = DBT_DIR / "models" / "staging" / "_staging.yml"
        with open(config_path) as f:
            config = yaml.safe_load(f)

        models = config.get("models", [])
        stg_enriched = next((m for m in models if m["name"] == "stg_enriched"), None)
        columns = [c["name"] for c in stg_enriched.get("columns", [])]

        # Consistent with our "Clean Staging" rule:
        assert "delay_category" not in columns, "Logic Leak: delay_category found in staging"


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

        assert dim_stops is not None, "dim_stops model not found"

        columns = dim_stops.get("columns", [])
        column_names = [c["name"] for c in columns]

        # SCD Type 2 required columns
        scd_columns = ["valid_from", "valid_to", "is_current", "stop_key"]
        for col in scd_columns:
            assert col in column_names, f"Missing SCD column: {col}"

    def test_dim_stops_key_unique(self):
        """dim_stops surrogate key is unique."""
        config_path = DBT_DIR / "models" / "dimensions" / "_dimensions.yml"

        with open(config_path) as f:
            config = yaml.safe_load(f)

        models = config.get("models", [])
        dim_stops = next((m for m in models if m["name"] == "dim_stops"), None)

        columns = dim_stops.get("columns", [])
        stop_key = next((c for c in columns if c["name"] == "stop_key"), None)

        tests = stop_key.get("tests", [])

        assert "unique" in tests, "stop_key must be unique"
        assert "not_null" in tests, "stop_key must be not null"


class TestMartContracts:
    """Tests for mart model contracts (fact tables)."""

    def test_fct_stop_arrivals_has_delay_contract(self):
        """delay_category logic should reside in the mart/contract layer."""
        config_path = DBT_DIR / "models" / "marts" / "_marts.yml"
        with open(config_path) as f:
            config = yaml.safe_load(f)

        fct = next((m for m in config.get("models", []) if m["name"] == "fct_stop_arrivals"), None)
        columns = fct.get("columns", [])
        delay_cat = next((c for c in columns if c["name"] == "delay_category"), None)

        assert delay_cat is not None, "delay_category must be defined in the Mart contract"

        tests = delay_cat.get("tests", [])
        accepted = next((t for t in tests if "accepted_values" in str(t)), None)
        assert accepted is not None, "Missing accepted_values validation in Mart"


class TestSourceContracts:
    """Tests for source definitions."""

    def test_sources_file_exists(self):
        """Sources configuration file exists."""
        config_path = DBT_DIR / "models" / "sources.yml"
        assert config_path.exists(), f"Missing: {config_path}"

    def test_bronze_source_has_freshness(self):
        """Bronze source has freshness SLO."""
        config_path = DBT_DIR / "models" / "sources.yml"

        with open(config_path) as f:
            config = yaml.safe_load(f)

        sources = config.get("sources", [])
        bronze = next((s for s in sources if s["name"] == "bronze"), None)

        assert bronze is not None, "bronze source not found"
        assert "freshness" in bronze, "Missing freshness SLO on bronze source"

        freshness = bronze["freshness"]
        assert "error_after" in freshness, "Missing error_after in freshness"


class TestProjectConfig:
    """Tests for dbt project configuration."""

    def test_project_file_exists(self):
        """dbt_project.yml exists."""
        project_path = DBT_DIR / "dbt_project.yml"
        assert project_path.exists(), f"Missing: {project_path}"

    def test_project_has_vars(self):
        """Project defines threshold variables."""
        project_path = DBT_DIR / "dbt_project.yml"

        with open(project_path) as f:
            config = yaml.safe_load(f)

        vars = config.get("vars", {})

        assert "on_time_threshold" in vars, "Missing on_time_threshold var"
        assert "delayed_threshold" in vars, "Missing delayed_threshold var"
        assert "max_delay_seconds" in vars, "Missing max_delay_seconds var"

    def test_models_have_materialization(self):
        """Models have appropriate materialization."""
        project_path = DBT_DIR / "dbt_project.yml"

        with open(project_path) as f:
            config = yaml.safe_load(f)

        models = config.get("models", {}).get("transitflow", {})

        # Staging should be views (fast, no storage)
        assert models.get("staging", {}).get("+materialized") == "view"

        # Marts should be tables (for query performance)
        assert models.get("marts", {}).get("+materialized") == "table"
