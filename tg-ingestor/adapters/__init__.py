"""
Adapters layer for tg-ingestor service.

Contains implementations of domain interfaces using external systems
like Telegram API, HTTP clients, etc.
"""

from .telethon_client import TelethonClientAdapter
from .http_publisher import HTTPPublisher, TokenLoader
from .chat_resolver import TelegramChatResolver
from .filters import TelegramMessageFilter, create_default_filter
from .mapper import TelethonMessageMapper

__all__ = [
    "TelethonClientAdapter",
    "HTTPPublisher",
    "TokenLoader", 
    "TelegramChatResolver",
    "TelegramMessageFilter",
    "create_default_filter",
    "TelethonMessageMapper"
]