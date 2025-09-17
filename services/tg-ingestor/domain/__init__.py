"""
Domain layer for tg-ingestor service.

Contains business logic, data models, and interfaces.
Following Domain-Driven Design principles.
"""

from .dto import (
    TelegramEventDTO,
    ChatInfo, 
    MessageInfo,
    SenderInfo,
    PublishResult,
    ChatConfig,
    IngestionStats,
    MessageContext
)
from .ports import (
    Publisher,
    TelegramClient,
    ChatResolver,
    MessageFilter,
    MessageMapper,
    IngestionService
)

__all__ = [
    # DTOs
    "TelegramEventDTO",
    "ChatInfo",
    "MessageInfo", 
    "SenderInfo",
    "PublishResult",
    "ChatConfig",
    "IngestionStats",
    "MessageContext",
    
    # Ports (Interfaces)
    "Publisher",
    "TelegramClient",
    "ChatResolver", 
    "MessageFilter",
    "MessageMapper",
    "IngestionService"
]
