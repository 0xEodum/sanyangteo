"""
Ports (interfaces) for queue-writer service.
Following Dependency Inversion Principle - high-level modules depend on abstractions.
"""

from abc import ABC, abstractmethod
from typing import Optional

from .schema import TelegramEvent, QueueWriteResult, HealthStatus


class QueueWriter(ABC):
    """
    Interface for writing events to a queue with idempotency support.
    Can be implemented with Redis Streams, Kafka, NATS, etc.
    """
    
    @abstractmethod
    async def write_event(self, event: TelegramEvent) -> QueueWriteResult:
        """
        Write an event to the queue with idempotency protection.
        
        Args:
            event: The telegram event to write
            
        Returns:
            QueueWriteResult with stream_id and duplicate flag
            
        Raises:
            QueueWriteError: If writing fails
            DuplicateEventError: If event is duplicate (optional, can return is_duplicate=True)
        """
        pass
    
    @abstractmethod
    async def check_health(self) -> HealthStatus:
        """
        Check if the queue backend is healthy and accessible.
        
        Returns:
            HealthStatus with current state
        """
        pass


class TokenValidator(ABC):
    """
    Interface for validating authentication tokens.
    Can be implemented with different token types (JWT, shared secret, etc.)
    """
    
    @abstractmethod
    async def validate_token(self, token: Optional[str]) -> bool:
        """
        Validate the provided authentication token.
        
        Args:
            token: The token to validate (from Authorization header)
            
        Returns:
            True if token is valid, False otherwise
        """
        pass


class EventProcessor(ABC):
    """
    Interface for processing incoming events.
    Coordinates validation, deduplication, and queue writing.
    """
    
    @abstractmethod
    async def process_event(self, event: TelegramEvent) -> QueueWriteResult:
        """
        Process an incoming telegram event.
        
        Args:
            event: The validated telegram event
            
        Returns:
            QueueWriteResult with processing details
            
        Raises:
            ProcessingError: If processing fails
        """
        pass


# Custom exceptions
class QueueWriteError(Exception):
    """Raised when writing to queue fails."""
    pass


class DuplicateEventError(Exception):
    """Raised when trying to write duplicate event."""
    pass


class ProcessingError(Exception):
    """Raised when event processing fails."""
    pass


class AuthenticationError(Exception):
    """Raised when authentication fails."""
    pass