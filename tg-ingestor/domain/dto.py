"""
Domain Transfer Objects for tg-ingestor service.
Defines the event structure that will be sent to queue-writer.
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
    text: str = Field(..., min_length=1, description="Message text content")


class SenderInfo(BaseModel):
    """Information about the message sender."""
    model_config = ConfigDict(extra='forbid')
    
    id: int = Field(..., description="Sender user ID")
    username: Optional[str] = Field(None, description="Username without @")
    display_name: str = Field(..., description="Display name or first_name + last_name")
    is_bot: bool = Field(default=False, description="Whether sender is a bot")


class TelegramEventDTO(BaseModel):
    """
    Main event DTO for Telegram messages.
    This matches the contract expected by queue-writer service.
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
            Idempotency key in format: tg:<chat_id>:<message_id>
        """
        return f"tg:{chat_id}:{message_id}"


class PublishResult(BaseModel):
    """Result of publishing an event."""
    model_config = ConfigDict(extra='forbid')
    
    success: bool = Field(..., description="Whether publishing succeeded")
    stream_id: Optional[str] = Field(None, description="Stream ID from queue-writer")
    is_duplicate: bool = Field(default=False, description="Whether event was duplicate")
    error: Optional[str] = Field(None, description="Error message if failed")
    retry_count: int = Field(default=0, description="Number of retries attempted")


class IngestionStats(BaseModel):
    """Statistics for message ingestion."""
    model_config = ConfigDict(extra='forbid')
    
    total_processed: int = Field(default=0, description="Total messages processed")
    total_published: int = Field(default=0, description="Total messages published")
    total_duplicates: int = Field(default=0, description="Total duplicates detected")
    total_filtered: int = Field(default=0, description="Total messages filtered out")
    total_errors: int = Field(default=0, description="Total errors encountered")
    
    # Per chat statistics
    chat_stats: dict[int, dict[str, int]] = Field(
        default_factory=dict,
        description="Per-chat statistics"
    )
    
    # Bootstrap statistics
    bootstrap_complete: bool = Field(default=False, description="Whether bootstrap completed")
    bootstrap_messages: int = Field(default=0, description="Messages processed during bootstrap")
    
    def increment_processed(self, chat_id: int) -> None:
        """Increment processed counter."""
        self.total_processed += 1
        self._ensure_chat_stats(chat_id)
        self.chat_stats[chat_id]["processed"] += 1
    
    def increment_published(self, chat_id: int) -> None:
        """Increment published counter."""
        self.total_published += 1
        self._ensure_chat_stats(chat_id)
        self.chat_stats[chat_id]["published"] += 1
    
    def increment_duplicates(self, chat_id: int) -> None:
        """Increment duplicates counter."""
        self.total_duplicates += 1
        self._ensure_chat_stats(chat_id)
        self.chat_stats[chat_id]["duplicates"] += 1
    
    def increment_filtered(self, chat_id: int) -> None:
        """Increment filtered counter."""
        self.total_filtered += 1
        self._ensure_chat_stats(chat_id)
        self.chat_stats[chat_id]["filtered"] += 1
    
    def increment_errors(self, chat_id: int) -> None:
        """Increment errors counter."""
        self.total_errors += 1
        self._ensure_chat_stats(chat_id)
        self.chat_stats[chat_id]["errors"] += 1
    
    def _ensure_chat_stats(self, chat_id: int) -> None:
        """Ensure chat statistics entry exists."""
        if chat_id not in self.chat_stats:
            self.chat_stats[chat_id] = {
                "processed": 0,
                "published": 0,
                "duplicates": 0,
                "filtered": 0,
                "errors": 0
            }


class ChatMetadata(BaseModel):
    """Metadata about a Telegram chat."""
    model_config = ConfigDict(extra='forbid')
    
    id: int = Field(..., description="Chat ID")
    title: str = Field(..., description="Chat title")
    type: str = Field(..., description="Chat type")
    username: Optional[str] = Field(None, description="Chat username")
    participant_count: Optional[int] = Field(None, description="Number of participants")
    
    # Status information
    is_accessible: bool = Field(default=True, description="Whether chat is accessible")
    last_message_date: Optional[datetime] = Field(None, description="Last message timestamp")
    error: Optional[str] = Field(None, description="Error accessing chat")


class BootstrapProgress(BaseModel):
    """Progress information for bootstrap process."""
    model_config = ConfigDict(extra='forbid')
    
    chat_id: int = Field(..., description="Chat ID being processed")
    total_expected: Optional[int] = Field(None, description="Expected number of messages")
    processed: int = Field(default=0, description="Messages processed so far")
    published: int = Field(default=0, description="Messages published so far")
    filtered: int = Field(default=0, description="Messages filtered out")
    
    start_time: datetime = Field(default_factory=datetime.utcnow)
    end_time: Optional[datetime] = Field(None, description="Completion time")
    
    @property
    def is_complete(self) -> bool:
        """Check if bootstrap is complete."""
        return self.end_time is not None
    
    @property
    def duration_seconds(self) -> Optional[float]:
        """Get bootstrap duration in seconds."""
        if not self.end_time:
            return None
        return (self.end_time - self.start_time).total_seconds()
    
    @property
    def progress_percentage(self) -> Optional[float]:
        """Get progress as percentage (0-100)."""
        if not self.total_expected or self.total_expected == 0:
            return None
        return min(100.0, (self.processed / self.total_expected) * 100)
    
    def mark_complete(self) -> None:
        """Mark bootstrap as complete."""
        self.end_time = datetime.utcnow()