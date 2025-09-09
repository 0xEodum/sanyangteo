"""
Domain schemas for queue-writer service.
Defines the structure of incoming events from tg-ingestor.
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


class TelegramEvent(BaseModel):
    """
    Main event structure for Telegram messages.
    Versioned schema for backward compatibility.
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


class QueueWriteResult(BaseModel):
    """Result of writing to queue."""
    model_config = ConfigDict(extra='forbid')
    
    stream_id: str = Field(..., description="Redis Stream ID")
    is_duplicate: bool = Field(default=False, description="Whether this was a duplicate")


class HealthStatus(BaseModel):
    """Health check response."""
    model_config = ConfigDict(extra='forbid')
    
    status: str = Field(..., description="overall status: healthy, unhealthy")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    checks: dict[str, str] = Field(default_factory=dict, description="Individual check results")