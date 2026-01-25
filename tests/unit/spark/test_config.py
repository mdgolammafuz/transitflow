import os
import pytest
from unittest.mock import patch, MagicMock
from spark.config import load_config, create_spark_session

# --- Configuration Tests ---

def test_load_config_valid_env():
    """Verify that required environment variables are correctly mapped to SparkConfig."""
    mock_env = {
        "MINIO_ROOT_USER": "admin_user",
        "MINIO_ROOT_PASSWORD": "admin_password",
        "POSTGRES_USER": "transit_app",
        "POSTGRES_PASSWORD": "secure_password",
        "POSTGRES_HOST": "localhost",
        "POSTGRES_DB": "test_db"
    }
    
    with patch.dict(os.environ, mock_env):
        config = load_config()
        
        # Verify Required Fields
        assert config.minio_access_key == "admin_user"
        assert config.postgres_user == "transit_app"
        
        # Verify Logic-Based Paths (S3A Protocol)
        assert config.bronze_path == "s3a://transitflow-lakehouse/bronze"
        assert config.gold_path == "s3a://transitflow-lakehouse/gold"
        
        # Verify JDBC URL Construction
        assert config.postgres_jdbc_url == "jdbc:postgresql://localhost:5432/test_db"

def test_load_config_missing_required():
    """Ensure load_config raises ValueError if critical credentials are missing."""
    # Clear environment to ensure no leakage from host machine
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ValueError, match="Required environment variable not set"):
            load_config()

def test_get_checkpoint_path_logic():
    """Verify consistent checkpoint pathing for streaming integrity."""
    mock_env = {
        "MINIO_ROOT_USER": "u", "MINIO_ROOT_PASSWORD": "p",
        "POSTGRES_USER": "u", "POSTGRES_PASSWORD": "p"
    }
    with patch.dict(os.environ, mock_env):
        config = load_config()
        path = config.get_checkpoint_path("test_job")
        assert path == "s3a://transitflow-lakehouse/_checkpoints/test_job"

# --- Spark Session Tests ---

@patch("pyspark.sql.SparkSession.builder")
def test_spark_session_hardenings(mock_builder):
    """
    Verify the Principal Contract: UTC Lock, S3A Timeouts, and Circuit Breakers.
    This test ensures we never deploy a session without our safety guardrails.
    """
    # Mocking the fluent API of SparkSession.builder
    mock_builder.appName.return_value = mock_builder
    mock_builder.config.return_value = mock_builder
    mock_session = MagicMock()
    mock_builder.getOrCreate.return_value = mock_session
    
    # Mock Hadoop Configuration check
    mock_hadoop_conf = MagicMock()
    mock_session.sparkContext._jsc.hadoopConfiguration.return_value = mock_hadoop_conf

    mock_env = {
        "MINIO_ROOT_USER": "u", "MINIO_ROOT_PASSWORD": "p",
        "POSTGRES_USER": "u", "POSTGRES_PASSWORD": "p"
    }
    
    with patch.dict(os.environ, mock_env):
        create_spark_session("AuditApp")

    # Capture all config calls into a dict for verification
    config_calls = {call[0][0]: call[0][1] for call in mock_builder.config.call_args_list}

    # 1. Verify Global Timezone Lock
    assert config_calls.get("spark.sql.session.timeZone") == "UTC"

    # 2. Verify S3A Anti-Hang Protections
    assert config_calls.get("spark.hadoop.fs.s3a.connection.timeout") == "5000"
    assert config_calls.get("spark.hadoop.fs.s3a.attempts.maximum") == "1"
    assert config_calls.get("spark.hadoop.fs.s3a.retry.limit") == "1"

    # 3. Verify Delta Storage Policy
    assert "S3SingleDriverLogStore" in config_calls.get("spark.delta.logStore.class")

    # 4. Verify Hard-Lock on Hadoop Context
    mock_hadoop_conf.set.assert_any_call("fs.s3a.connection.timeout", "5000")
    mock_hadoop_conf.set.assert_any_call("fs.s3a.attempts.maximum", "1")