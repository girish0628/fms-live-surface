"""Custom exceptions for the FMS Live Surface application."""
from __future__ import annotations


class ConfigLoadError(Exception):
    """Raised when configuration loading fails."""


class ServiceExecutionError(Exception):
    """Raised when a service operation fails."""


class SnippetConversionError(ServiceExecutionError):
    """Raised when Minestar snippet file conversion fails."""


class ModularCsvError(ServiceExecutionError):
    """Raised when Modular CSV reprojection fails."""


class RasterGenerationError(ServiceExecutionError):
    """Raised when raster or TIN generation fails."""


class OutputHandlerError(ServiceExecutionError):
    """Raised when output folder management fails."""


class PublishingError(ServiceExecutionError):
    """Raised when handoff to publishing solution fails."""


class ArchiveError(ServiceExecutionError):
    """Raised when snippet file archival fails."""


class MonitoringError(ServiceExecutionError):
    """Raised when file monitoring check fails."""


class ArcPyExecutionError(ServiceExecutionError):
    """Raised when an arcpy operation fails."""


class ValidationError(ServiceExecutionError):
    """Raised when data validation fails."""
