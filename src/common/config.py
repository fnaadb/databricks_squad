"""
Configuration management for the medallion pipeline.

Loads environment-specific configuration from YAML files and provides
a typed interface for accessing configuration values.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional
import os
import yaml


@dataclass
class CatalogConfig:
    """Unity Catalog configuration."""
    name: str
    create_if_not_exists: bool = True


@dataclass
class SchemaConfig:
    """Schema names for each layer."""
    bronze: str = "bronze"
    silver: str = "silver"
    gold: str = "gold"


@dataclass
class StorageConfig:
    """Storage path configuration."""
    raw_landing: str = "/Volumes/squad/raw/landing"
    bronze_checkpoint: str = "/Volumes/squad/bronze/checkpoints"
    silver_checkpoint: str = "/Volumes/squad/silver/checkpoints"


@dataclass
class ComputeConfig:
    """Compute cluster configuration."""
    cluster_type: str = "job"
    num_workers: int = 2
    node_type: str = "Standard_DS3_v2"
    driver_node_type: Optional[str] = None
    spark_version: str = "14.3.x-scala2.12"
    autoscale: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ProcessingConfig:
    """Data processing configuration."""
    batch_size: int = 50000
    max_files_per_trigger: int = 50
    trigger_interval: str = "30 seconds"


@dataclass
class QualityConfig:
    """Data quality threshold configuration."""
    null_threshold_pct: float = 2.0
    duplicate_threshold_pct: float = 0.5
    quarantine_enabled: bool = True
    fail_on_threshold_breach: bool = False


@dataclass
class LoggingConfig:
    """Logging configuration."""
    level: str = "INFO"
    structured: bool = True
    include_spark_logs: bool = False


@dataclass
class FeatureConfig:
    """Feature flag configuration."""
    streaming_enabled: bool = True
    liquid_clustering: bool = False
    auto_optimize: bool = True


@dataclass
class Config:
    """
    Main configuration class that combines all sub-configurations.
    
    Example usage:
        config = load_config("dev")
        catalog_name = config.catalog.name
        bronze_schema = config.schemas.bronze
    """
    environment: str
    debug: bool
    catalog: CatalogConfig
    schemas: SchemaConfig
    storage: StorageConfig
    compute: ComputeConfig
    processing: ProcessingConfig
    quality: QualityConfig
    logging: LoggingConfig
    features: FeatureConfig
    
    @property
    def bronze_database(self) -> str:
        """Full path to bronze database/schema."""
        return f"{self.catalog.name}.{self.schemas.bronze}"
    
    @property
    def silver_database(self) -> str:
        """Full path to silver database/schema."""
        return f"{self.catalog.name}.{self.schemas.silver}"
    
    @property
    def gold_database(self) -> str:
        """Full path to gold database/schema."""
        return f"{self.catalog.name}.{self.schemas.gold}"
    
    def get_table_path(self, layer: str, table_name: str) -> str:
        """Get fully qualified table name."""
        schema = getattr(self.schemas, layer)
        return f"{self.catalog.name}.{schema}.{table_name}"


def load_config(environment: str = None, config_dir: str = None) -> Config:
    """
    Load configuration for the specified environment.
    
    Args:
        environment: Environment name (dev, test, prod). 
                    Defaults to ENV environment variable or 'dev'.
        config_dir: Directory containing config files.
                   Defaults to 'configs/' relative to project root.
    
    Returns:
        Config object with loaded configuration.
    
    Raises:
        FileNotFoundError: If config file doesn't exist.
        ValueError: If config file is invalid.
    """
    if environment is None:
        environment = os.getenv("ENV", "dev")
    
    if config_dir is None:
        # Find config directory relative to this file
        project_root = Path(__file__).parent.parent.parent
        config_dir = project_root / "configs"
    else:
        config_dir = Path(config_dir)
    
    config_file = config_dir / f"{environment}.yml"
    
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_file}")
    
    with open(config_file, "r") as f:
        raw_config = yaml.safe_load(f)
    
    return _parse_config(raw_config)


def _parse_config(raw: Dict[str, Any]) -> Config:
    """Parse raw YAML config into typed Config object."""
    
    catalog_raw = raw.get("catalog", {})
    catalog = CatalogConfig(
        name=catalog_raw.get("name", "squad"),
        create_if_not_exists=catalog_raw.get("create_if_not_exists", True),
    )
    
    schemas_raw = raw.get("schemas", {})
    schemas = SchemaConfig(
        bronze=schemas_raw.get("bronze", "bronze"),
        silver=schemas_raw.get("silver", "silver"),
        gold=schemas_raw.get("gold", "gold"),
    )
    
    storage_raw = raw.get("storage", {})
    storage = StorageConfig(
        raw_landing=storage_raw.get("raw_landing", "/Volumes/squad/raw/landing"),
        bronze_checkpoint=storage_raw.get("bronze_checkpoint", "/Volumes/squad/bronze/checkpoints"),
        silver_checkpoint=storage_raw.get("silver_checkpoint", "/Volumes/squad/silver/checkpoints"),
    )
    
    compute_raw = raw.get("compute", {})
    compute = ComputeConfig(
        cluster_type=compute_raw.get("cluster_type", "job"),
        num_workers=compute_raw.get("num_workers", 2),
        node_type=compute_raw.get("node_type", "Standard_DS3_v2"),
        driver_node_type=compute_raw.get("driver_node_type"),
        spark_version=compute_raw.get("spark_version", "14.3.x-scala2.12"),
        autoscale=compute_raw.get("autoscale", {}),
    )
    
    processing_raw = raw.get("processing", {})
    processing = ProcessingConfig(
        batch_size=processing_raw.get("batch_size", 50000),
        max_files_per_trigger=processing_raw.get("max_files_per_trigger", 50),
        trigger_interval=processing_raw.get("trigger_interval", "30 seconds"),
    )
    
    quality_raw = raw.get("quality", {})
    quality = QualityConfig(
        null_threshold_pct=quality_raw.get("null_threshold_pct", 2.0),
        duplicate_threshold_pct=quality_raw.get("duplicate_threshold_pct", 0.5),
        quarantine_enabled=quality_raw.get("quarantine_enabled", True),
        fail_on_threshold_breach=quality_raw.get("fail_on_threshold_breach", False),
    )
    
    logging_raw = raw.get("logging", {})
    logging_config = LoggingConfig(
        level=logging_raw.get("level", "INFO"),
        structured=logging_raw.get("structured", True),
        include_spark_logs=logging_raw.get("include_spark_logs", False),
    )
    
    features_raw = raw.get("features", {})
    features = FeatureConfig(
        streaming_enabled=features_raw.get("streaming_enabled", True),
        liquid_clustering=features_raw.get("liquid_clustering", False),
        auto_optimize=features_raw.get("auto_optimize", True),
    )
    
    return Config(
        environment=raw.get("environment", "dev"),
        debug=raw.get("debug", False),
        catalog=catalog,
        schemas=schemas,
        storage=storage,
        compute=compute,
        processing=processing,
        quality=quality,
        logging=logging_config,
        features=features,
    )
