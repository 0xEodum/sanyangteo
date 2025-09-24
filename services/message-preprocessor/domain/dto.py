"""
Data Transfer Objects for message-preprocessor service.
Defines the structure of input/output data and internal processing models.
"""

from datetime import datetime
from typing import Optional, List, Dict, Any, Union
from enum import Enum

from pydantic import BaseModel, Field, ConfigDict


class ClassificationType(Enum):
    """Message classification types from LLM"""
    ORDER = "ORDER"
    OFFER = "OFFER" 
    OTHER = "OTHER"
    FAILED = "FAILED"


class ProcessingMode(Enum):
    """Service processing modes"""
    PROD = "prod"
    TEST = "test"


# Input DTOs (from telegram_events queue)
class TelegramEventDTO(BaseModel):
    """
    Input message from tg-ingestor service.
    Must match the output format from tg-ingestor.
    """
    model_config = ConfigDict(extra='forbid')
    
    schema_version: str = Field(..., description="Event schema version")
    event_type: str = Field(..., description="Type of event")
    event_ts: datetime = Field(..., description="Event timestamp")
    
    chat: Dict[str, Any] = Field(..., description="Chat information")
    message: Dict[str, Any] = Field(..., description="Message information")
    sender: Dict[str, Any] = Field(..., description="Sender information")
    
    idempotency_key: str = Field(..., description="Unique key for idempotency")


# LLM Response DTOs
class LLMRequest(BaseModel):
    """Request to LLM API"""
    model_config = ConfigDict(extra='forbid')
    
    model_name: str = Field(..., description="Model name")
    messages: List[Dict[str, str]] = Field(..., description="Chat messages")
    max_tokens: int = Field(default=1000, description="Maximum tokens")
    temperature: float = Field(default=0.1, description="Temperature")


class LLMResponse(BaseModel):
    """Raw response from LLM API"""
    model_config = ConfigDict(extra='forbid')
    
    success: bool = Field(..., description="Whether request succeeded")
    content: Optional[str] = Field(None, description="Response text")
    error: Optional[str] = Field(None, description="Error message if failed")
    model_used: str = Field(..., description="Model that was used")
    provider: str = Field(..., description="API provider")
    processing_time_ms: int = Field(..., description="Processing time")
    retry_count: int = Field(default=0, description="Number of retries")


# Parsed Data DTOs
class OrderItem(BaseModel):
    """Single plant item in ORDER"""
    model_config = ConfigDict(extra='forbid')
    
    plant_name: str = Field(..., description="Plant name")
    height: Optional[Union[str, float]] = Field(None, description="Height value or range")
    height_unit: Optional[str] = Field(None, description="Height unit")
    quantity: int = Field(..., description="Quantity")
    quantity_unit: str = Field(..., description="Quantity unit")


class OrderData(BaseModel):
    """Parsed ORDER data"""
    model_config = ConfigDict(extra='forbid')
    
    city: Optional[str] = Field(None, description="City name")
    items: List[OrderItem] = Field(..., description="Plant items")


class ContactInfo(BaseModel):
    """Contact information for OFFER"""
    model_config = ConfigDict(extra='forbid')
    
    phone: Optional[str] = Field(None, description="Phone number")
    telegram: Optional[str] = Field(None, description="Telegram username")
    email: Optional[str] = Field(None, description="Email address")


class OfferData(BaseModel):
    """Parsed OFFER data"""
    model_config = ConfigDict(extra='forbid')
    
    city: Optional[str] = Field(None, description="City name")
    company_name: Optional[str] = Field(None, description="Company or person name")
    contact: ContactInfo = Field(..., description="Contact information")


# Classification Result
class ClassificationResult(BaseModel):
    """Result of message classification and parsing"""
    model_config = ConfigDict(extra='forbid')
    
    classification: ClassificationType = Field(..., description="Message classification")
    parsed_data: Optional[Union[OrderData, OfferData]] = Field(None, description="Parsed structured data")
    raw_llm_response: str = Field(..., description="Raw LLM response")
    validation_passed: bool = Field(..., description="Whether validation passed")
    validation_errors: List[str] = Field(default_factory=list, description="Validation error messages")


# Processing Metadata
class ProcessingMetadata(BaseModel):
    """Metadata about message processing"""
    model_config = ConfigDict(extra='forbid')
    
    model_used: str = Field(..., description="LLM model used")
    provider: str = Field(..., description="API provider")
    processing_time_ms: int = Field(..., description="Total processing time")
    retry_count: int = Field(default=0, description="Number of retries")
    validation_passed: bool = Field(..., description="Whether validation passed")
    telegram_filled_from_sender: bool = Field(default=False, description="Whether telegram was filled from sender")
    
    # Additional fields for FAILED classification
    failure_reason: Optional[str] = Field(None, description="Failure reason for FAILED messages")
    models_attempted: List[str] = Field(default_factory=list, description="Models attempted for FAILED messages")
    last_error: Optional[str] = Field(None, description="Last error for FAILED messages")


# Output DTO (to preprocessed_data queue)
class PreprocessedMessageDTO(BaseModel):
    """
    Final processed message sent to output queue.
    Contains classification, parsed data, and original message.
    
    For FAILED messages:
    - classification = "FAILED"
    - parsed_data = None
    - processing_metadata contains failure details
    """
    model_config = ConfigDict(extra='forbid')
    
    schema_version: str = Field(default="1.0", description="Output schema version")
    processing_timestamp: datetime = Field(default_factory=datetime.utcnow, description="When processed")
    parsing_schema_version: str = Field(..., description="Parsing schema version used")
    
    classification: ClassificationType = Field(..., description="Message classification")
    parsed_data: Optional[Union[OrderData, OfferData]] = Field(None, description="Structured data or null for OTHER/FAILED")
    
    original_message: TelegramEventDTO = Field(..., description="Original telegram message")
    processing_metadata: ProcessingMetadata = Field(..., description="Processing metadata")


# Service Statistics
class ProcessingStats(BaseModel):
    """Service processing statistics"""
    model_config = ConfigDict(extra='forbid')
    
    messages_processed: int = Field(default=0, description="Total messages processed")
    messages_classified: Dict[str, int] = Field(
        default_factory=lambda: {"ORDER": 0, "OFFER": 0, "OTHER": 0, "FAILED": 0},
        description="Messages by classification"
    )
    messages_failed: int = Field(default=0, description="Messages that failed processing")
    
    # Model usage stats
    models_used: Dict[str, int] = Field(default_factory=dict, description="Usage count by model")
    avg_processing_time_ms: float = Field(default=0.0, description="Average processing time")
    
    # Key usage for test mode
    key_usage_stats: Dict[str, KeyUsageStats] = Field(default_factory=dict, description="Key usage statistics")
    
    last_activity: Optional[datetime] = Field(None, description="Last message timestamp")
    
    def increment_processed(self) -> None:
        """Increment processed message counter."""
        self.messages_processed += 1
        self.last_activity = datetime.utcnow()
    
    def increment_published(self) -> None:
        """Increment published message counter."""
        # This method is kept for compatibility but messages_published
        # is now tracked via messages_classified
        pass
    
    def increment_failed(self) -> None:
        """Increment failed message counter."""
        self.messages_failed += 1
        self.messages_classified["FAILED"] = self.messages_classified.get("FAILED", 0) + 1
    """Single API key configuration"""
    model_config = ConfigDict(extra='forbid')
    
    key: str = Field(..., description="API key value")
    name: str = Field(..., description="Key identifier")
    limit_per_day: Optional[int] = Field(None, description="Daily limit for rotation keys")


class ModelConfig(BaseModel):
    """Single model configuration with optional extra parameters"""
    model_config = ConfigDict(extra='forbid')
    
    name: str = Field(..., description="Model name")
    url: str = Field(..., description="API URL")
    provider: str = Field(..., description="Provider name")
    
    # Новое опциональное поле для дополнительных параметров
    extra_body: Optional[Dict[str, Any]] = Field(
        None, 
        description="Extra parameters for API request (e.g., OpenRouter provider settings)"
    )
    
    # Альтернативно можно добавить более гибкое поле для любых дополнительных параметров к запросу
    request_params: Optional[Dict[str, Any]] = Field(
        None,
        description="Additional parameters to pass to API request"
    )
    
    def get_request_extras(self) -> Dict[str, Any]:
        """Get dictionary of extra parameters for API request"""
        extras = {}
        
        if self.extra_body:
            extras["extra_body"] = self.extra_body
            
        if self.request_params:
            # Объединяем request_params с extras (request_params приоритетнее)
            extras.update(self.request_params)
            
        return extras


class FieldConfig(BaseModel):
    """Configuration for a single field in parsing schema"""
    model_config = ConfigDict(extra='forbid')
    
    name: str = Field(..., description="Field name")
    type: str = Field(..., description="Field type")
    required: bool = Field(default=False, description="Whether field is required")


class ParsingSchema(BaseModel):
    """Schema configuration for parsing"""
    model_config = ConfigDict(extra='forbid')
    
    version: str = Field(..., description="Schema version")
    fields: List[FieldConfig] = Field(..., description="Field configurations")


# Key Usage Tracking
class KeyUsageStats(BaseModel):
    """Statistics for API key usage"""
    model_config = ConfigDict(extra='forbid')
    
    key_name: str = Field(..., description="Key identifier")
    requests_today: int = Field(default=0, description="Requests made today")
    limit_per_day: int = Field(..., description="Daily limit")
    last_used: Optional[datetime] = Field(None, description="Last usage timestamp")
    is_exhausted: bool = Field(default=False, description="Whether key reached limit")
    
    @property
    def requests_remaining(self) -> int:
        """Calculate remaining requests for today"""
        return max(0, self.limit_per_day - self.requests_today)
    
    @property
    def usage_percentage(self) -> float:
        """Calculate usage percentage"""
        if self.limit_per_day == 0:
            return 0.0
        return (self.requests_today / self.limit_per_day) * 100


# Service Statistics
class ProcessingStats(BaseModel):
    """Service processing statistics"""
    model_config = ConfigDict(extra='forbid')
    
    messages_processed: int = Field(default=0, description="Total messages processed")
    messages_classified: Dict[str, int] = Field(
        default_factory=lambda: {"ORDER": 0, "OFFER": 0, "OTHER": 0},
        description="Messages by classification"
    )
    messages_failed: int = Field(default=0, description="Messages that failed processing")
    
    # Model usage stats
    models_used: Dict[str, int] = Field(default_factory=dict, description="Usage count by model")
    avg_processing_time_ms: float = Field(default=0.0, description="Average processing time")
    
    # Key usage for test mode
    key_usage_stats: Dict[str, KeyUsageStats] = Field(default_factory=dict, description="Key usage statistics")
    
    last_activity: Optional[datetime] = Field(None, description="Last processing timestamp")


# Context for processing pipeline
class ProcessingContext(BaseModel):
    """Context for processing a single message"""
    model_config = ConfigDict(extra='forbid')
    
    idempotency_key: str = Field(..., description="Message idempotency key")
    processing_mode: ProcessingMode = Field(..., description="Processing mode")
    parsing_schema_version: str = Field(..., description="Schema version to use")
    
    start_time: datetime = Field(default_factory=datetime.utcnow)
    current_model_index: int = Field(default=0, description="Current model being tried")
    retry_count: int = Field(default=0, description="Current retry count")
    models_attempted: List[str] = Field(default_factory=list, description="Models already attempted")
    
    def get_processing_time_ms(self) -> int:
        """Get current processing time in milliseconds"""
        elapsed = datetime.utcnow() - self.start_time
        return int(elapsed.total_seconds() * 1000)