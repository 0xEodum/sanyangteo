"""
Ports (interfaces) for message-preprocessor service.
Following Dependency Inversion Principle - high-level modules depend on abstractions.
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any, AsyncGenerator

from .dto import (
    TelegramEventDTO,
    PreprocessedMessageDTO,
    LLMRequest,
    LLMResponse,
    ClassificationResult,
    ProcessingContext,
    ProcessingStats,
    KeyUsageStats,
    APIKeyConfig,
    ModelConfig,
    ParsingSchema,
    ProcessingMode
)


class LLMClient(ABC):
    """
    Interface for LLM API communication.
    Handles API calls to different LLM providers (OpenAI, Anthropic, Groq, etc.)
    """
    
    @abstractmethod
    async def send_request(
        self, 
        request: LLMRequest, 
        api_key: str,
        api_url: str,
        provider: str
    ) -> LLMResponse:
        """
        Send request to LLM API.
        
        Args:
            request: LLM request with model and messages
            api_key: API key for authentication
            api_url: API endpoint URL
            provider: Provider name (openai, anthropic, groq)
            
        Returns:
            LLM response with content or error
            
        Raises:
            LLMClientError: If request fails permanently
        """
        pass
    
    @abstractmethod
    async def check_health(self) -> bool:
        """
        Check if LLM client is healthy and can make requests.
        
        Returns:
            True if healthy, False otherwise
        """
        pass


class KeyManager(ABC):
    """
    Interface for managing API key rotation and usage tracking.
    Handles both rotation keys (with limits) and direct keys (unlimited).
    """
    
    @abstractmethod
    async def get_available_key(self, processing_mode: ProcessingMode) -> Optional[APIKeyConfig]:
        """
        Get next available API key based on processing mode.
        
        Args:
            processing_mode: PROD uses direct keys, TEST uses rotation keys
            
        Returns:
            Available API key or None if all exhausted
        """
        pass
    
    @abstractmethod
    async def record_key_usage(self, key_name: str, success: bool) -> None:
        """
        Record API key usage for tracking limits.
        
        Args:
            key_name: Key identifier
            success: Whether the request was successful
        """
        pass
    
    @abstractmethod
    async def mark_key_exhausted(self, key_name: str) -> None:
        """
        Mark key as exhausted (reached daily limit or rate limited).
        
        Args:
            key_name: Key identifier to mark as exhausted
        """
        pass
    
    @abstractmethod
    async def get_key_stats(self) -> Dict[str, KeyUsageStats]:
        """
        Get usage statistics for all keys.
        
        Returns:
            Dictionary mapping key names to usage statistics
        """
        pass
    
    @abstractmethod
    async def reset_daily_limits(self) -> None:
        """
        Reset daily usage counters (called at UTC midnight).
        """
        pass


class ModelManager(ABC):
    """
    Interface for managing LLM models and selection strategy.
    """
    
    @abstractmethod
    def get_models_for_mode(self, processing_mode: ProcessingMode) -> List[ModelConfig]:
        """
        Get list of models for processing mode.
        
        Args:
            processing_mode: PROD uses main+fallback, TEST uses free models
            
        Returns:
            Ordered list of models to try
        """
        pass
    
    @abstractmethod
    def get_next_model(
        self, 
        context: ProcessingContext,
        processing_mode: ProcessingMode
    ) -> Optional[ModelConfig]:
        """
        Get next model to try based on context.
        
        Args:
            context: Current processing context
            processing_mode: Processing mode
            
        Returns:
            Next model to try or None if all exhausted
        """
        pass


class MessageParser(ABC):
    """
    Interface for parsing LLM responses into structured data.
    """
    
    @abstractmethod
    async def parse_llm_response(
        self, 
        llm_response: str,
        schema_version: str
    ) -> ClassificationResult:
        """
        Parse LLM response into structured classification result.
        
        Args:
            llm_response: Raw response from LLM
            schema_version: Parsing schema version to use
            
        Returns:
            Classification result with parsed data
            
        Raises:
            ParsingError: If response cannot be parsed
        """
        pass
    
    @abstractmethod
    def get_parsing_schema(self, schema_version: str) -> ParsingSchema:
        """
        Get parsing schema configuration.
        
        Args:
            schema_version: Schema version identifier
            
        Returns:
            Schema configuration
            
        Raises:
            SchemaNotFoundError: If schema version doesn't exist
        """
        pass


class MessageValidator(ABC):
    """
    Interface for validating parsed message data.
    """
    
    @abstractmethod
    async def validate_order_data(self, order_data: Dict[str, Any]) -> List[str]:
        """
        Validate ORDER message data.
        
        Args:
            order_data: Parsed ORDER data
            
        Returns:
            List of validation errors (empty if valid)
        """
        pass
    
    @abstractmethod
    async def validate_offer_data(self, offer_data: Dict[str, Any]) -> List[str]:
        """
        Validate OFFER message data.
        
        Args:
            offer_data: Parsed OFFER data
            
        Returns:
            List of validation errors (empty if valid)
        """
        pass
    
    @abstractmethod
    async def enhance_offer_data(
        self, 
        offer_data: Dict[str, Any],
        original_message: TelegramEventDTO
    ) -> Dict[str, Any]:
        """
        Enhance OFFER data by filling missing telegram from sender.
        
        Args:
            offer_data: Parsed OFFER data
            original_message: Original telegram message
            
        Returns:
            Enhanced offer data
        """
        pass


class OutputPublisher(ABC):
    """
    Interface for publishing processed messages to output queue.
    """
    
    @abstractmethod
    async def publish_processed_message(
        self, 
        message: PreprocessedMessageDTO
    ) -> bool:
        """
        Publish processed message to output queue.
        
        Args:
            message: Processed message to publish
            
        Returns:
            True if successfully published
            
        Raises:
            PublishError: If publishing fails
        """
        pass
    
    @abstractmethod
    async def check_health(self) -> bool:
        """
        Check if publisher is healthy.
        
        Returns:
            True if healthy, False otherwise
        """
        pass


class MessagePreprocessor(ABC):
    """
    Main interface for message preprocessing service.
    Coordinates the entire processing pipeline.
    """
    
    @abstractmethod
    async def start(self) -> None:
        """
        Start the message preprocessing service.
        
        This includes:
        - Connecting to input queue
        - Starting message consumption
        - Initializing all dependencies
        """
        pass
    
    @abstractmethod
    async def stop(self) -> None:
        """Stop the preprocessing service and cleanup resources."""
        pass
    
    @abstractmethod
    async def process_message(
        self, 
        telegram_message: TelegramEventDTO
    ) -> Optional[PreprocessedMessageDTO]:
        """
        Process a single telegram message through the pipeline.
        
        Args:
            telegram_message: Input message from telegram
            
        Returns:
            Processed message or None if processing failed
        """
        pass
    
    @abstractmethod
    async def get_stats(self) -> ProcessingStats:
        """
        Get current processing statistics.
        
        Returns:
            Current service statistics
        """
        pass
    
    @abstractmethod
    async def health_check(self) -> Dict[str, Any]:
        """
        Perform comprehensive health check.
        
        Returns:
            Health status of all components
        """
        pass
    """
    Interface for publishing processed messages to output queue.
    """
    
    @abstractmethod
    async def publish_processed_message(
        self, 
        message: PreprocessedMessageDTO
    ) -> bool:
        """
        Publish processed message to output queue.
        
        Args:
            message: Processed message to publish
            
        Returns:
            True if successfully published
            
        Raises:
            PublishError: If publishing fails
        """
        pass
    
    @abstractmethod
    async def check_health(self) -> bool:
        """
        Check if publisher is healthy.
        
        Returns:
            True if healthy, False otherwise
        """
        pass


class MessagePreprocessor(ABC):
    """
    Main interface for message preprocessing service.
    Coordinates the entire processing pipeline.
    """
    
    @abstractmethod
    async def start(self) -> None:
        """
        Start the message preprocessing service.
        
        This includes:
        - Connecting to input queue
        - Starting message consumption
        - Initializing all dependencies
        """
        pass
    
    @abstractmethod
    async def stop(self) -> None:
        """Stop the preprocessing service and cleanup resources."""
        pass
    
    @abstractmethod
    async def process_message(
        self, 
        telegram_message: TelegramEventDTO
    ) -> Optional[PreprocessedMessageDTO]:
        """
        Process a single telegram message through the pipeline.
        
        Args:
            telegram_message: Input message from telegram
            
        Returns:
            Processed message or None if processing failed
        """
        pass
    
    @abstractmethod
    async def get_stats(self) -> ProcessingStats:
        """
        Get current processing statistics.
        
        Returns:
            Current service statistics
        """
        pass
    
    @abstractmethod
    async def health_check(self) -> Dict[str, Any]:
        """
        Perform comprehensive health check.
        
        Returns:
            Health status of all components
        """
        pass


class InputMessageConsumer(ABC):
    """
    Interface for consuming messages from input queue.
    """
    
    @abstractmethod
    async def start_consuming(
        self, 
        message_handler: callable
    ) -> None:
        """
        Start consuming messages from input queue.
        
        Args:
            message_handler: Async function to handle each message
        """
        pass
    
    @abstractmethod
    async def stop_consuming(self) -> None:
        """Stop consuming messages."""
        pass
    
    @abstractmethod
    async def acknowledge_message(self, message_id: str) -> bool:
        """
        Acknowledge successful message processing.
        
        Args:
            message_id: Message ID to acknowledge
            
        Returns:
            True if acknowledged successfully
        """
        pass


# Custom exceptions
class LLMClientError(Exception):
    """Raised when LLM client request fails."""
    pass


class KeyExhaustedError(Exception):
    """Raised when all API keys are exhausted."""
    pass


class ParsingError(Exception):
    """Raised when LLM response parsing fails."""
    pass


class ValidationError(Exception):
    """Raised when message validation fails."""
    pass


class SchemaNotFoundError(Exception):
    """Raised when parsing schema is not found."""
    pass


class PublishError(Exception):
    """Raised when message publishing fails."""
    pass


class ProcessingError(Exception):
    """Raised when message processing fails."""
    pass