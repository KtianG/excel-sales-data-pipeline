from __future__ import annotations


class CreateDatabaseError(Exception):
    """Base exception for the CREATE_DATABASE project."""


class DataValidationError(CreateDatabaseError):
    """Raised when critical data validation fails."""


class ConfigurationError(CreateDatabaseError):
    """Raised when project configuration is invalid."""


class SourceDataError(CreateDatabaseError):
    """Raised when source files or sheets do not match expected structure."""
