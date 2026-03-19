"""Schema engine package for detecting and handling schema evolution."""

from .schema_detector import (
    SchemaDetector,
    SchemaChange,
    SchemaChangeType,
    SchemaComparisonResult
)
from .schema_evolution import SchemaEvolutionHandler

__all__ = [
    "SchemaDetector",
    "SchemaChange",
    "SchemaChangeType",
    "SchemaComparisonResult",
    "SchemaEvolutionHandler"
]
