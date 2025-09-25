"""
Execution engines for different job types.

This module contains execution engines for various distributed computing frameworks:
- Apache Spark for big data processing
- Local execution for smaller tasks
- Extensible interface for adding new engines
"""

from .base import BaseExecutionEngine
from .spark_engine import SparkExecutionEngine
from .local_engine import LocalExecutionEngine

__all__ = [
    'BaseExecutionEngine',
    'SparkExecutionEngine',
    'LocalExecutionEngine'
]