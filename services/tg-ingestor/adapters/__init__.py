"""
Adapters layer for tg-ingestor service.

Contains implementations of domain interfaces using external systems
like Telegram API, Queue libraries, etc.
"""

from .telethon_client import TelethonClientAdapter
from .queue_publisher import QueuePublisher
from .chat_resolver import TelegramChatResolver
from .filters import TelegramMessageFilter, create_default_filter
from .mapper import TelethonMessageMapper

__all__ = [
    "TelethonClientAdapter",
    "QueuePublisher",
    "TelegramChatResolver", 
    "TelegramMessageFilter",
    "create_default_filter",
    "TelethonMessageMapper"
]