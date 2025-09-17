"""
Data Transfer Objects for tg-ingestor service.
Defines the structure of events sent to queue-writer.
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, ConfigDict


class ChatInfo(BaseModel):
    """Information about the Telegram chat."""
    model_config = ConfigDict(extra='forbid')
    
    id: int = Field(..., description="Chat ID (negative for groups/supergroups)")
    type: str = Field(..., description="Chat type: group, supergroup, etc.")
    title: str = Field(..., description="Chat title for debugging/logs")


class MessageInfo(BaseModel):
    """Information about the message."""
    model_config = ConfigDict(extra='forbid')
    
    id: int = Field(..., description="Message ID within the chat")
    text: str = Field(..., min_length=1, max_length=16384, description="Message text content")


class SenderInfo(BaseModel):
    """Information about the message sender."""
    model_config = ConfigDict(extra='forbid')
    
    id: int = Field(..., description="Sender user ID")
    username: Optional[str] = Field(None, description="Username without @")
    display_name: str = Field(..., description="Display name or first_name + last_name")
    is_bot: bool = Field(default=False, description="Whether sender is a bot")


class TelegramEventDTO(BaseModel):
    """
    Event DTO sent to queue-writer service.
    Must match the schema expected by queue-writer.
    """
    model_config = ConfigDict(extra='forbid')
    
    schema_version: str = Field(default="1.0", description="Event schema version")
    event_type: str = Field(default="message_new", description="Type of event")
    event_ts: datetime = Field(..., description="Event timestamp (ISO format)")
    
    chat: ChatInfo = Field(..., description="Chat information")
    message: MessageInfo = Field(..., description="Message information") 
    sender: SenderInfo = Field(..., description="Sender information")
    
    idempotency_key: str = Field(
        ...,
        pattern=r"^tg:-?\d+:\d+$", 
        description="Unique key for idempotency: tg:<chat_id>:<message_id>"
    )
    
    @classmethod
    def create_idempotency_key(cls, chat_id: int, message_id: int) -> str:
        """
        Create idempotency key for the event.
        
        Args:
            chat_id: Telegram chat ID
            message_id: Telegram message ID
            
        Returns:
            Idempotency key string
        """
        return f"tg:{chat_id}:{message_id}"


class PublishResult(BaseModel):
    """Result of publishing an event."""
    model_config = ConfigDict(extra='forbid')
    
    success: bool = Field(..., description="Whether publishing succeeded")
    stream_id: Optional[str] = Field(None, description="Stream ID if successful")
    is_duplicate: bool = Field(default=False, description="Whether event was duplicate")
    error: Optional[str] = Field(None, description="Error message if failed")
    retry_count: int = Field(default=0, description="Number of retries attempted")


class ChatConfig(BaseModel):
    """Configuration for a monitored chat."""
    model_config = ConfigDict(extra='forbid')
    
    id: int = Field(..., description="Chat ID to monitor")
    enabled: bool = Field(default=True, description="Whether monitoring is enabled")
    title: Optional[str] = Field(None, description="Chat title for reference")


class IngestionStats(BaseModel):
    """Statistics for ingestion process."""
    model_config = ConfigDict(extra='forbid')
    
    messages_processed: int = Field(default=0, description="Total messages processed")
    messages_published: int = Field(default=0, description="Messages successfully published")
    messages_filtered: int = Field(default=0, description="Messages filtered out")
    messages_failed: int = Field(default=0, description="Messages that failed to process")
    duplicates_detected: int = Field(default=0, description="Duplicate messages detected")
    
    # Per-chat stats
    chats_monitored: int = Field(default=0, description="Number of chats being monitored")
    last_activity: Optional[datetime] = Field(None, description="Last message timestamp")
    
    # Bootstrap stats
    bootstrap_completed: bool = Field(default=False, description="Whether bootstrap completed")
    bootstrap_messages: int = Field(default=0, description="Messages loaded during bootstrap")
    
    def increment_processed(self) -> None:
        """Increment processed message counter."""
        self.messages_processed += 1
        self.last_activity = datetime.utcnow()
    
    def increment_published(self) -> None:
        """Increment published message counter."""
        self.messages_published += 1
    
    def increment_filtered(self) -> None:
        """Increment filtered message counter."""
        self.messages_filtered += 1
    
    def increment_failed(self) -> None:
        """Increment failed message counter."""
        self.messages_failed += 1
    
    def increment_duplicate(self) -> None:
        """Increment duplicate message counter."""
        self.duplicates_detected += 1


class MessageContext(BaseModel):
    """Context information for message processing."""
    model_config = ConfigDict(extra='forbid')
    
    chat_id: int = Field(..., description="Chat ID")
    message_id: int = Field(..., description="Message ID")
    is_bootstrap: bool = Field(default=False, description="Whether from bootstrap process")
    retry_count: int = Field(default=0, description="Current retry count")
    processing_start: datetime = Field(default_factory=datetime.utcnow)
    
    def get_processing_time_ms(self) -> float:
        """
        Get processing time in milliseconds.
        
        Returns:
            Processing time in milliseconds
        """
        elapsed = datetime.utcnow() - self.processing_start
        return elapsed.total_seconds() * 1000