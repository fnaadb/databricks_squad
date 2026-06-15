"""
Structured logging utilities for the medallion pipeline.

Provides consistent logging format with correlation IDs, run tracking,
and Spark-compatible output.
"""

import logging
import sys
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional
import json


@dataclass
class LogContext:
    """
    Context for tracking pipeline execution.
    
    Attributes:
        run_id: Unique identifier for the pipeline run
        batch_id: Identifier for the current batch
        layer: Current medallion layer (bronze, silver, gold)
        table: Current table being processed
        started_at: Run start timestamp
        metadata: Additional context metadata
    """
    run_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    batch_id: Optional[str] = None
    layer: Optional[str] = None
    table: Optional[str] = None
    started_at: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def with_table(self, table: str) -> "LogContext":
        """Create new context with specified table."""
        return LogContext(
            run_id=self.run_id,
            batch_id=self.batch_id,
            layer=self.layer,
            table=table,
            started_at=self.started_at,
            metadata=self.metadata.copy(),
        )
    
    def with_layer(self, layer: str) -> "LogContext":
        """Create new context with specified layer."""
        return LogContext(
            run_id=self.run_id,
            batch_id=self.batch_id,
            layer=layer,
            table=self.table,
            started_at=self.started_at,
            metadata=self.metadata.copy(),
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert context to dictionary for logging."""
        return {
            "run_id": self.run_id,
            "batch_id": self.batch_id,
            "layer": self.layer,
            "table": self.table,
            "started_at": self.started_at.isoformat(),
            **self.metadata,
        }


class StructuredFormatter(logging.Formatter):
    """JSON formatter for structured logging."""
    
    def __init__(self, context: Optional[LogContext] = None):
        super().__init__()
        self.context = context or LogContext()
    
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "run_id": self.context.run_id,
        }
        
        # Add optional context
        if self.context.layer:
            log_entry["layer"] = self.context.layer
        if self.context.table:
            log_entry["table"] = self.context.table
        if self.context.batch_id:
            log_entry["batch_id"] = self.context.batch_id
        
        # Add extra fields from record
        if hasattr(record, "extra"):
            log_entry.update(record.extra)
        
        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        
        return json.dumps(log_entry)


class PipelineLogger:
    """
    Logger wrapper with pipeline context support.
    
    Example usage:
        logger = get_logger("bronze.transactions")
        logger.info("Starting ingestion", row_count=1000)
        
        with logger.context(table="transactions_raw"):
            logger.info("Processing table")
    """
    
    def __init__(self, name: str, context: Optional[LogContext] = None, structured: bool = True):
        self.logger = logging.getLogger(name)
        self._context = context or LogContext()
        self.structured = structured
        
        if not self.logger.handlers:
            self._setup_handler()
    
    def _setup_handler(self):
        """Set up logging handler with appropriate formatter."""
        handler = logging.StreamHandler(sys.stdout)
        
        if self.structured:
            formatter = StructuredFormatter(self._context)
        else:
            formatter = logging.Formatter(
                "%(asctime)s | %(levelname)-8s | %(name)s | [%(run_id)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
    
    @property
    def context(self) -> LogContext:
        """Get current logging context."""
        return self._context
    
    @context.setter
    def context(self, ctx: LogContext):
        """Set logging context."""
        self._context = ctx
        # Update formatter with new context
        for handler in self.logger.handlers:
            if isinstance(handler.formatter, StructuredFormatter):
                handler.formatter.context = ctx
    
    def _log(self, level: int, msg: str, **kwargs):
        """Internal logging method with extra fields."""
        extra = {"extra": kwargs, "run_id": self._context.run_id}
        self.logger.log(level, msg, extra=extra)
    
    def debug(self, msg: str, **kwargs):
        """Log debug message."""
        self._log(logging.DEBUG, msg, **kwargs)
    
    def info(self, msg: str, **kwargs):
        """Log info message."""
        self._log(logging.INFO, msg, **kwargs)
    
    def warning(self, msg: str, **kwargs):
        """Log warning message."""
        self._log(logging.WARNING, msg, **kwargs)
    
    def error(self, msg: str, **kwargs):
        """Log error message."""
        self._log(logging.ERROR, msg, **kwargs)
    
    def exception(self, msg: str, **kwargs):
        """Log exception with traceback."""
        self.logger.exception(msg, extra={"extra": kwargs, "run_id": self._context.run_id})
    
    @contextmanager
    def layer_context(self, layer: str):
        """Context manager for layer-specific logging."""
        old_context = self._context
        self.context = self._context.with_layer(layer)
        try:
            yield self
        finally:
            self.context = old_context
    
    @contextmanager
    def table_context(self, table: str):
        """Context manager for table-specific logging."""
        old_context = self._context
        self.context = self._context.with_table(table)
        try:
            yield self
        finally:
            self.context = old_context
    
    def log_row_counts(
        self,
        input_count: int,
        output_count: int,
        rejected_count: int = 0,
        operation: str = "transform",
    ):
        """Log row count metrics for a processing step."""
        self.info(
            f"Row counts for {operation}",
            input_count=input_count,
            output_count=output_count,
            rejected_count=rejected_count,
            success_rate=round(output_count / max(input_count, 1) * 100, 2),
        )


# Module-level logger cache
_loggers: Dict[str, PipelineLogger] = {}


def get_logger(
    name: str,
    context: Optional[LogContext] = None,
    structured: bool = True,
) -> PipelineLogger:
    """
    Get or create a logger with the specified name.
    
    Args:
        name: Logger name (e.g., "bronze.transactions")
        context: Optional logging context
        structured: Use structured (JSON) logging format
    
    Returns:
        PipelineLogger instance
    """
    if name not in _loggers:
        _loggers[name] = PipelineLogger(name, context, structured)
    elif context:
        _loggers[name].context = context
    
    return _loggers[name]


def set_log_level(level: str):
    """Set log level for all pipeline loggers."""
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    for logger in _loggers.values():
        logger.logger.setLevel(numeric_level)
