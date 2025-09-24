"""
Logging utilities for Enterprise Job Orchestrator

Provides structured logging configuration and utilities for the job orchestrator system.
"""

import logging
import sys
import json
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path


class StructuredFormatter(logging.Formatter):
    """
    Custom formatter for structured JSON logging.

    Formats log records as JSON with additional context fields for better
    observability and log aggregation.
    """

    def __init__(self, include_extra: bool = True):
        super().__init__()
        self.include_extra = include_extra

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }

        # Add exception information if present
        if record.exc_info:
            log_entry["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else None,
                "message": str(record.exc_info[1]) if record.exc_info[1] else None,
                "traceback": self.formatException(record.exc_info)
            }

        # Add extra fields from record
        if self.include_extra:
            extra_fields = {}
            for key, value in record.__dict__.items():
                if key not in {
                    'name', 'msg', 'args', 'levelname', 'levelno', 'pathname',
                    'filename', 'module', 'lineno', 'funcName', 'created',
                    'msecs', 'relativeCreated', 'thread', 'threadName',
                    'processName', 'process', 'message', 'exc_info', 'exc_text',
                    'stack_info'
                }:
                    extra_fields[key] = value

            if extra_fields:
                log_entry["extra"] = extra_fields

        return json.dumps(log_entry, default=str, ensure_ascii=False)


class JobContextFilter(logging.Filter):
    """
    Filter to add job context to log records.

    Automatically adds job_id, worker_id, and other context information
    to log records when available.
    """

    def __init__(self):
        super().__init__()
        self.context = {}

    def set_context(self, **kwargs):
        """Set context variables for logging."""
        self.context.update(kwargs)

    def clear_context(self):
        """Clear all context variables."""
        self.context.clear()

    def filter(self, record: logging.LogRecord) -> bool:
        """Add context to log record."""
        for key, value in self.context.items():
            setattr(record, key, value)
        return True


def setup_logger(
    name: str,
    level: str = "INFO",
    structured: bool = True,
    log_file: Optional[str] = None
) -> logging.Logger:
    """
    Set up a logger with appropriate configuration.

    Args:
        name: Logger name
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        structured: Whether to use structured JSON logging
        log_file: Optional log file path

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)

    # Avoid adding handlers multiple times
    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, level.upper()))

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, level.upper()))

    if structured:
        console_formatter = StructuredFormatter()
    else:
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # File handler if specified
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(getattr(logging, level.upper()))

        if structured:
            file_formatter = StructuredFormatter()
        else:
            file_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )

        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    # Add job context filter
    context_filter = JobContextFilter()
    logger.addFilter(context_filter)

    # Store filter reference for context management
    logger.context_filter = context_filter

    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance.

    Args:
        name: Logger name

    Returns:
        Logger instance
    """
    return logging.getLogger(name)


def set_log_context(logger: logging.Logger, **kwargs):
    """
    Set context variables for a logger.

    Args:
        logger: Logger instance
        **kwargs: Context variables to set
    """
    if hasattr(logger, 'context_filter'):
        logger.context_filter.set_context(**kwargs)


def clear_log_context(logger: logging.Logger):
    """
    Clear context variables for a logger.

    Args:
        logger: Logger instance
    """
    if hasattr(logger, 'context_filter'):
        logger.context_filter.clear_context()


class LoggerContext:
    """
    Context manager for temporary log context.

    Automatically sets and clears log context variables.
    """

    def __init__(self, logger: logging.Logger, **kwargs):
        self.logger = logger
        self.context = kwargs
        self.old_context = {}

    def __enter__(self):
        """Set temporary context."""
        if hasattr(self.logger, 'context_filter'):
            # Save current context
            self.old_context = self.logger.context_filter.context.copy()
            # Set new context
            self.logger.context_filter.set_context(**self.context)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Restore old context."""
        if hasattr(self.logger, 'context_filter'):
            # Restore old context
            self.logger.context_filter.context = self.old_context


# Default logger instance for the orchestrator
orchestrator_logger = setup_logger(
    "enterprise_job_orchestrator",
    level="INFO",
    structured=True
)