"""
Postprocessing module for simulation results.

This module provides functionality for reading and processing simulation
results from different analysis types (static, eigen, dynamic).
"""
__version__ = "0.1.0"

from .core import EigenResultsProcessor
from .readers import EigenResultsReader
from .core import DynamicResultsProcessor
from .readers import DynamicResultsReader

__all__ = [
    "EigenResultsProcessor",
    "EigenResultsReader",
    "DynamicResultsProcessor",
    "DynamicResultsReader",
]
