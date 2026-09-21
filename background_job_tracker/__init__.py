"""
Background Job Tracker Python SDK.

Real-time reliability, observability, and incident detection for Celery,
RQ, and background task queues.
"""

from .client import Tracker

__version__ = "0.1.2"
__all__ = ["Tracker", "__version__"]
