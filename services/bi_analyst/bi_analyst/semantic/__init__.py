"""Versioned semantic metadata; no model, database, or ETL startup."""

from .catalogue import Catalogue, load_catalogue

__all__ = ["Catalogue", "load_catalogue"]
