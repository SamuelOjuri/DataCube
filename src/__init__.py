"""
DataCube - Monday.com Data Analysis System
"""

__version__ = "1.0.0"
__author__ = "DataCube Team"

# Import only what exists and is needed
try:
    from .core.monday_client import MondayClient
except ImportError:
    try:
        from src.core.monday_client import MondayClient
    except ImportError:
        MondayClient = None

__all__ = [
    "MondayClient",
]
