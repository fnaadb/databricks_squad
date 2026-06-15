"""
Unit tests for configuration management.
"""

import os
import pytest
from unittest.mock import patch

from src.common.config import (
    Config,
    CatalogConfig,
    SchemaConfig,
    QualityConfig,
    load_config,
    get_config_for_environment,
)


class TestCatalogConfig:
    """Tests for CatalogConfig."""
    
    def test_default_values(self):
        """Test default configuration values."""
        config = CatalogConfig()
        assert config.name == "squad"
        assert config.use_unity_catalog is True
        assert config.fallback_to_hive is True
    
    def test_custom_values(self):
        """Test custom configuration values."""
        config = CatalogConfig(
            name="custom_catalog",
            use_unity_catalog=False,
            fallback_to_hive=False,
        )
        assert config.name == "custom_catalog"
        assert config.use_unity_catalog is False
        assert config.fallback_to_hive is False


class TestSchemaConfig:
    """Tests for SchemaConfig."""
    
    def test_default_values(self):
        """Test default schema names."""
        config = SchemaConfig()
        assert config.bronze == "bronze"
        assert config.silver == "silver"
        assert config.gold == "gold"
    
    def test_custom_values(self):
        """Test custom schema names."""
        config = SchemaConfig(
            bronze="raw",
            silver="curated",
            gold="analytics",
        )
        assert config.bronze == "raw"
        assert config.silver == "curated"
        assert config.gold == "analytics"


class TestQualityConfig:
    """Tests for QualityConfig."""
    
    def test_default_values(self):
        """Test default quality thresholds."""
        config = QualityConfig()
        assert config.null_threshold == 0.1
        assert config.duplicate_threshold == 0.01
        assert config.freshness_hours == 24
    
    def test_custom_values(self):
        """Test custom quality thresholds."""
        config = QualityConfig(
            null_threshold=0.05,
            duplicate_threshold=0.005,
            freshness_hours=12,
        )
        assert config.null_threshold == 0.05
        assert config.duplicate_threshold == 0.005
        assert config.freshness_hours == 12


class TestConfig:
    """Tests for main Config class."""
    
    def test_default_config(self):
        """Test default configuration."""
        config = Config()
        assert config.environment == "dev"
        assert config.debug is False
        assert isinstance(config.catalog, CatalogConfig)
        assert isinstance(config.schemas, SchemaConfig)
        assert isinstance(config.quality, QualityConfig)
    
    def test_config_with_custom_nested(self):
        """Test configuration with custom nested configs."""
        config = Config(
            environment="prod",
            catalog=CatalogConfig(name="prod_catalog"),
            quality=QualityConfig(null_threshold=0.01),
            debug=False,
        )
        assert config.environment == "prod"
        assert config.catalog.name == "prod_catalog"
        assert config.quality.null_threshold == 0.01
        assert config.debug is False
    
    def test_full_table_name(self):
        """Test generation of full table names."""
        config = Config(
            catalog=CatalogConfig(name="my_catalog"),
            schemas=SchemaConfig(bronze="bronze_schema"),
        )
        full_name = f"{config.catalog.name}.{config.schemas.bronze}.my_table"
        assert full_name == "my_catalog.bronze_schema.my_table"


class TestLoadConfig:
    """Tests for load_config function."""
    
    def test_load_default_config(self):
        """Test loading default configuration."""
        with patch.dict(os.environ, {"ENVIRONMENT": "dev"}, clear=False):
            config = load_config()
            assert config.environment == "dev"
    
    def test_load_test_config(self):
        """Test loading test configuration."""
        with patch.dict(os.environ, {"ENVIRONMENT": "test"}, clear=False):
            config = load_config()
            assert config.environment == "test"
    
    def test_load_prod_config(self):
        """Test loading production configuration."""
        with patch.dict(os.environ, {"ENVIRONMENT": "prod"}, clear=False):
            config = load_config()
            assert config.environment == "prod"
    
    def test_environment_override(self):
        """Test environment override."""
        config = load_config(environment="staging")
        assert config.environment == "staging"


class TestGetConfigForEnvironment:
    """Tests for get_config_for_environment function."""
    
    def test_dev_environment(self):
        """Test dev environment configuration."""
        config = get_config_for_environment("dev")
        assert config.environment == "dev"
        assert config.debug is True
    
    def test_test_environment(self):
        """Test test environment configuration."""
        config = get_config_for_environment("test")
        assert config.environment == "test"
        assert "test" in config.catalog.name.lower()
    
    def test_prod_environment(self):
        """Test prod environment configuration."""
        config = get_config_for_environment("prod")
        assert config.environment == "prod"
        assert config.debug is False
        # Stricter quality thresholds in prod
        assert config.quality.null_threshold <= 0.05
