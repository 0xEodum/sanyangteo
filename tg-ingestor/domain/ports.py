"""
Ports (interfaces) for tg-ingestor service.
Following Dependency Inversion Principle - high-level modules depend on abstractions.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Optional, AsyncGenerator

from .dto import (
    TelegramEventDTO, 
    PublishResult, 
    ChatConfig, 
    IngestionStats,
    MessageContext
)


class Publisher(ABC):
    """
    Interface for publishing events to downstream systems.
    Can be implemented for HTTP, Kafka, NATS, etc.
    """
    
    @abstractmethod
    async def publish_event(self, event: TelegramEventDTO) -> PublishResult:
        """
        Publish a telegram event to the downstream system.
        
        Args:
            event: The telegram event to publish
            
        Returns:
            PublishResult with success status and details
            
        Raises:
            PublishError: If publishing fails after retries
        """
        pass
    
    @abstractmethod
    async def check_health(self) -> bool:
        """
        Check if the publisher is healthy and can accept events.
        
        Returns:
            True if healthy, False otherwise
        """
        pass


class TelegramClient(ABC):
    """
    Interface for Telegram client operations.
    Abstracts away the specific Telegram library (Telethon, Pyrogram, etc.)
    """
    
    @abstractmethod
    async def connect(self) -> None:
        """
        Connect to Telegram and authenticate.
        
        Raises:
            ConnectionError: If connection or authentication fails
        """
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from Telegram and cleanup resources."""
        pass
    
    @abstractmethod
    async def start_monitoring(self, chat_ids: List[int]) -> None:
        """
        Start monitoring specified chats for new messages.
        
        Args:
            chat_ids: List of chat IDs to monitor
            
        Raises:
            MonitoringError: If monitoring setup fails
        """
        pass
    
    @abstractmethod
    async def stop_monitoring(self) -> None:
        """Stop monitoring all chats."""
        pass
    
    @abstractmethod
    async def get_chat_history(
        self,
        chat_id: int,
        since: datetime,
        limit: Optional[int] = None
    ) -> AsyncGenerator[object, None]:
        """
        Get chat history since a specific datetime.
        
        Args:
            chat_id: Chat ID to fetch history from
            since: Fetch messages since this datetime
            limit: Maximum number of messages to fetch
            
        Yields:
            Raw message objects from the Telegram client
            
        Raises:
            HistoryError: If fetching history fails
        """
        pass
    
    @abstractmethod
    async def resolve_chat(self, chat_id: int) -> Optional[dict]:
        """
        Resolve chat information by ID.
        
        Args:
            chat_id: Chat ID to resolve
            
        Returns:
            Chat information dict or None if not found/accessible
        """
        pass


class ChatResolver(ABC):
    """
    Interface for resolving and validating chat configurations.
    """
    
    @abstractmethod
    async def validate_chats(self, chat_configs: List[ChatConfig]) -> List[ChatConfig]:
        """
        Validate that chats are accessible and of correct type.
        
        Args:
            chat_configs: List of chat configurations to validate
            
        Returns:
            List of validated chat configurations (may be filtered)
            
        Raises:
            ValidationError: If validation fails
        """
        pass
    
    @abstractmethod
    async def get_chat_info(self, chat_id: int) -> Optional[dict]:
        """
        Get detailed information about a chat.
        
        Args:
            chat_id: Chat ID to get info for
            
        Returns:
            Chat information dict or None if not accessible
        """
        pass


class MessageFilter(ABC):
    """
    Interface for filtering incoming messages.
    """
    
    @abstractmethod
    def should_process(self, message: object) -> bool:
        """
        Determine if a message should be processed.
        
        Args:
            message: Raw message object from Telegram client
            
        Returns:
            True if message should be processed, False otherwise
        """
        pass
    
    @abstractmethod
    def get_filter_reason(self, message: object) -> Optional[str]:
        """
        Get reason why a message was filtered out.
        
        Args:
            message: Raw message object from Telegram client
            
        Returns:
            Filter reason string or None if message passed filters
        """
        pass


class MessageMapper(ABC):
    """
    Interface for mapping raw Telegram messages to DTOs.
    """
    
    @abstractmethod
    async def map_message(self, message: object, context: MessageContext) -> Optional[TelegramEventDTO]:
        """
        Map a raw Telegram message to a TelegramEventDTO.
        
        Args:
            message: Raw message object from Telegram client
            context: Message processing context
            
        Returns:
            Mapped TelegramEventDTO or None if mapping fails
            
        Raises:
            MappingError: If mapping fails
        """
        pass


class IngestionService(ABC):
    """
    Interface for the main ingestion service.
    Coordinates message processing, filtering, and publishing.
    """
    
    @abstractmethod
    async def start(self) -> None:
        """
        Start the ingestion service.
        
        This includes:
        - Connecting to Telegram
        - Validating chat configurations
        - Starting live monitoring
        - Performing bootstrap if configured
        """
        pass
    
    @abstractmethod
    async def stop(self) -> None:
        """Stop the ingestion service and cleanup resources."""
        pass
    
    @abstractmethod
    async def get_stats(self) -> IngestionStats:
        """
        Get current ingestion statistics.
        
        Returns:
            Current statistics
        """
        pass
    
    @abstractmethod
    async def perform_bootstrap(self, chat_id: int, since: datetime) -> int:
        """
        Perform historical message bootstrap for a chat.
        
        Args:
            chat_id: Chat ID to bootstrap
            since: Fetch messages since this datetime
            
        Returns:
            Number of messages processed during bootstrap
        """
        pass


# Custom exceptions
class PublishError(Exception):
    """Raised when event publishing fails."""
    pass


class ConnectionError(Exception):
    """Raised when Telegram connection fails."""
    pass


class MonitoringError(Exception):
    """Raised when chat monitoring setup fails."""
    pass


class HistoryError(Exception):
    """Raised when fetching chat history fails."""
    pass


class ValidationError(Exception):
    """Raised when chat validation fails."""
    pass


class MappingError(Exception):
    """Raised when message mapping fails."""
    pass


class IngestionError(Exception):
    """Raised when ingestion process fails."""
    pass