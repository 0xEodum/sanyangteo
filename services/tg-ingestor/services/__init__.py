"""
Services layer for tg-ingestor.

Contains business logic and orchestration services that coordinate
between adapters and implement use cases.
"""

from .ingestion_service import TelegramIngestionService

__all__ = [
    "TelegramIngestionService"
]