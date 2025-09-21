"""
Domain layer for message-preprocessor service.

Contains business logic, data models, and interfaces.
Following Domain-Driven Design principles.
"""

from .dto import (
    # Enums
    ClassificationType,
    ProcessingMode,
    
    # Input/Output DTOs
    TelegramEventDTO,
    PreprocessedMessageDTO,
    
    # LLM DTOs
    LLMRequest,
    LLMResponse,
    ClassificationResult,
    
    # Parsed Data DTOs
    OrderItem,
    OrderData,
    ContactInfo,
    OfferData,
    
    # Processing DTOs
    ProcessingMetadata,
    ProcessingContext,
    ProcessingStats,
    
    # Configuration DTOs
    APIKeyConfig,
    ModelConfig,
    FieldConfig,
    ParsingSchema,
    KeyUsageStats
)

from .ports import (
    # Core Interfaces
    LLMClient,
    KeyManager,
    ModelManager,
    MessageParser,
    MessageValidator,
    
    # I/O Interfaces
    InputMessageConsumer,
    OutputPublisher,
    
    # Main Service Interface
    MessagePreprocessor,
    
    # Exceptions
    LLMClientError,
    KeyExhaustedError,
    ParsingError,
    ValidationError,
    SchemaNotFoundError,
    PublishError,
    ProcessingError
)

__all__ = [
    # Enums
    "ClassificationType",
    "ProcessingMode",
    
    # Input/Output DTOs
    "TelegramEventDTO",
    "PreprocessedMessageDTO", 
    
    # LLM DTOs
    "LLMRequest",
    "LLMResponse",
    "ClassificationResult",
    
    # Parsed Data DTOs
    "OrderItem",
    "OrderData",
    "ContactInfo", 
    "OfferData",
    
    # Processing DTOs
    "ProcessingMetadata",
    "ProcessingContext",
    "ProcessingStats",
    
    # Configuration DTOs
    "APIKeyConfig",
    "ModelConfig",
    "FieldConfig",
    "ParsingSchema",
    "KeyUsageStats",
    
    # Core Interfaces
    "LLMClient",
    "KeyManager",
    "ModelManager", 
    "MessageParser",
    "MessageValidator",
    
    # I/O Interfaces
    "InputMessageConsumer",
    "OutputPublisher",
    
    # Main Service Interface
    "MessagePreprocessor",
    
    # Exceptions
    "LLMClientError",
    "KeyExhaustedError",
    "ParsingError",
    "ValidationError",
    "SchemaNotFoundError",
    "PublishError",
    "ProcessingError"
]