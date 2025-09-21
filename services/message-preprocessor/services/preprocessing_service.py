"""
Main preprocessing service that orchestrates the entire message processing pipeline.
Coordinates LLM requests, model selection, key rotation, parsing, validation, and publishing.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

from domain.ports import (
    MessagePreprocessor,
    LLMClient,
    KeyManager,
    ModelManager,
    MessageParser,
    MessageValidator,
    OutputPublisher,
    LLMClientError,
    KeyExhaustedError,
    ParsingError,
    ValidationError,
    PublishError,
    ProcessingError
)
from domain.dto import (
    TelegramEventDTO,
    PreprocessedMessageDTO,
    ProcessingContext,
    ProcessingStats,
    ProcessingMode,
    LLMRequest,
    LLMResponse,
    ClassificationResult,
    ProcessingMetadata,
    ClassificationType
)
from telemetry.logger import MetricsLogger
from adapters.llm_client import create_llm_request


logger = logging.getLogger(__name__)


class TelegramPreprocessingService(MessagePreprocessor):
    """
    Main preprocessing service implementing the complete processing pipeline.
    
    Pipeline Flow:
    1. Receive message from telegram_events queue
    2. Select API key and model based on processing mode
    3. Send LLM request with system prompt
    4. Parse LLM response into structured data
    5. Validate parsed data
    6. Enhance data (fill missing telegram from sender)  
    7. Publish to preprocessed_data queue
    8. Handle failures and retries
    """
    
    def __init__(
        self,
        llm_client: LLMClient,
        key_manager: KeyManager,
        model_manager: ModelManager,
        message_parser: MessageParser,
        message_validator: MessageValidator,
        output_publisher: OutputPublisher,
        processing_mode: ProcessingMode,
        system_prompt: str,
        parsing_schema_version: str = "v1",
        retry_delay_seconds: int = 300,
        max_attempts_per_model: int = 2,
        max_concurrent_messages: int = 5
    ):
        """
        Initialize preprocessing service.
        
        Args:
            llm_client: LLM API client
            key_manager: API key rotation manager
            model_manager: Model selection manager
            message_parser: Message parsing service
            message_validator: Message validation service
            output_publisher: Output queue publisher
            processing_mode: PROD or TEST mode
            system_prompt: System prompt for LLM
            parsing_schema_version: Schema version for parsing
            retry_delay_seconds: Delay for test mode retry
            max_attempts_per_model: Max attempts per model
            max_concurrent_messages: Max concurrent processing
        """
        self.llm_client = llm_client
        self.key_manager = key_manager
        self.model_manager = model_manager
        self.message_parser = message_parser
        self.message_validator = message_validator
        self.output_publisher = output_publisher
        
        self.processing_mode = processing_mode
        self.system_prompt = system_prompt
        self.parsing_schema_version = parsing_schema_version
        self.retry_delay_seconds = retry_delay_seconds
        self.max_attempts_per_model = max_attempts_per_model
        
        # Concurrency control
        self.semaphore = asyncio.Semaphore(max_concurrent_messages)
        
        # Service state
        self._is_running = False
        self._stats = ProcessingStats()
        
        # Metrics logger
        self.metrics = MetricsLogger("preprocessing_service")
        
        # Background tasks
        self._stats_task: Optional[asyncio.Task] = None
    
    async def start(self) -> None:
        """
        Start the preprocessing service.
        """
        if self._is_running:
            logger.warning("Preprocessing service already running")
            return
        
        try:
            logger.info("Starting message preprocessing service")
            
            # Connect key manager
            if hasattr(self.key_manager, 'connect'):
                await self.key_manager.connect()
            
            # Start background tasks
            self._stats_task = asyncio.create_task(self._periodic_stats())
            
            self._is_running = True
            
            logger.info(
                "Preprocessing service started successfully",
                extra={
                    "component": "preprocessing_service",
                    "processing_mode": self.processing_mode.value,
                    "schema_version": self.parsing_schema_version,
                    "max_concurrent": self.semaphore._value
                }
            )
            
        except Exception as e:
            logger.error(f"Failed to start preprocessing service: {e}")
            await self.stop()
            raise ProcessingError(f"Service startup failed: {e}") from e
    
    async def stop(self) -> None:
        """Stop the preprocessing service and cleanup resources."""
        if not self._is_running:
            return
        
        logger.info("Stopping preprocessing service")
        
        try:
            # Cancel background tasks
            if self._stats_task and not self._stats_task.done():
                self._stats_task.cancel()
                try:
                    await self._stats_task
                except asyncio.CancelledError:
                    pass
            
            # Close key manager
            if hasattr(self.key_manager, 'close'):
                await self.key_manager.close()
            
            # Close LLM client
            if hasattr(self.llm_client, 'close'):
                await self.llm_client.close()
            
            self._is_running = False
            
            logger.info(
                "Preprocessing service stopped",
                extra={
                    "component": "preprocessing_service",
                    "final_stats": self._stats.model_dump()
                }
            )
            
        except Exception as e:
            logger.error(f"Error during service shutdown: {e}")
    
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
        # Create processing context
        context = ProcessingContext(
            idempotency_key=telegram_message.idempotency_key,
            processing_mode=self.processing_mode,
            parsing_schema_version=self.parsing_schema_version
        )
        
        async with self.semaphore:
            return await self._process_with_context(telegram_message, context)
    
    async def _process_with_context(
        self,
        telegram_message: TelegramEventDTO,
        context: ProcessingContext
    ) -> Optional[PreprocessedMessageDTO]:
        """Process message with context and full error handling."""
        self._stats.messages_processed += 1
        
        try:
            logger.info(
                f"Processing message",
                extra={
                    "component": "preprocessing_service",
                    "idempotency_key": context.idempotency_key,
                    "processing_mode": context.processing_mode.value
                }
            )
            
            # Try processing with model rotation and retry
            result = await self._process_with_retry(telegram_message, context)
            
            if result:
                # Update stats
                classification = result.classification.value
                self._stats.messages_classified[classification] = (
                    self._stats.messages_classified.get(classification, 0) + 1
                )
                self._stats.last_activity = datetime.utcnow()
                
                # Log success metrics
                processing_time = context.get_processing_time_ms()
                self.metrics.log_message_processed(
                    idempotency_key=context.idempotency_key,
                    classification=classification,
                    processing_time_ms=processing_time,
                    model_used=result.processing_metadata.model_used,
                    provider=result.processing_metadata.provider,
                    retry_count=context.retry_count,
                    success=True
                )
                
                return result
            else:
                # Processing failed after all retries
                await self._handle_failed_message(
                    telegram_message, 
                    context, 
                    "All models exhausted",
                    "No models were able to successfully process the message"
                )
                return None
                
        except Exception as e:
            logger.error(
                f"Unexpected error processing message: {e}",
                extra={
                    "component": "preprocessing_service",
                    "idempotency_key": context.idempotency_key,
                    "error": str(e)
                }
            )
            await self._handle_failed_message(
                telegram_message, 
                context, 
                f"Unexpected error: {str(e)}", 
                str(e)
            )
            return None
    
    async def _process_with_retry(
        self,
        telegram_message: TelegramEventDTO,
        context: ProcessingContext
    ) -> Optional[PreprocessedMessageDTO]:
        """Process message with model retry logic."""
        
        while True:
            # Get next model to try
            model_config = self.model_manager.get_next_model(context, context.processing_mode)
            
            if not model_config:
                # No more models available
                if context.processing_mode == ProcessingMode.TEST and context.models_attempted:
                    # In TEST mode, wait and retry all models
                    logger.info(
                        f"All models exhausted in TEST mode, waiting {self.retry_delay_seconds}s",
                        extra={
                            "component": "preprocessing_service",
                            "idempotency_key": context.idempotency_key,
                            "delay_seconds": self.retry_delay_seconds
                        }
                    )
                    
                    self.metrics.log_retry_attempt(
                        idempotency_key=context.idempotency_key,
                        attempt_number=context.retry_count + 1,
                        reason="all_models_exhausted",
                        delay_seconds=self.retry_delay_seconds
                    )
                    
                    await asyncio.sleep(self.retry_delay_seconds)
                    
                    # Reset models attempted for retry
                    context.models_attempted.clear()
                    context.retry_count += 1
                    continue
                else:
                    # PROD mode or no models attempted - fail
                    return None
            
            # Mark model as attempted
            context.models_attempted.append(model_config.name)
            
            # Try processing with this model
            result = await self._try_single_model(telegram_message, context, model_config)
            
            if result:
                return result
            
            # Model failed, continue to next model
            logger.debug(
                f"Model {model_config.name} failed, trying next model",
                extra={
                    "component": "preprocessing_service",
                    "idempotency_key": context.idempotency_key,
                    "failed_model": model_config.name
                }
            )
    
    async def _try_single_model(
        self,
        telegram_message: TelegramEventDTO,
        context: ProcessingContext,
        model_config
    ) -> Optional[PreprocessedMessageDTO]:
        """Try processing with a single model."""
        
        for attempt in range(self.max_attempts_per_model):
            try:
                # Get API key
                api_key_config = await self.key_manager.get_available_key(context.processing_mode)
                if not api_key_config:
                    logger.warning(
                        f"No API keys available for {context.processing_mode.value} mode",
                        extra={
                            "component": "preprocessing_service",
                            "idempotency_key": context.idempotency_key,
                            "model": model_config.name
                        }
                    )
                    return None
                
                # Create LLM request
                user_message = telegram_message.message.get('text', '')
                llm_request = create_llm_request(
                    model_name=model_config.name,
                    system_prompt=self.system_prompt,
                    user_message=user_message
                )
                
                # Send LLM request
                llm_response = await self.llm_client.send_request(
                    request=llm_request,
                    api_key=api_key_config.key,
                    api_url=model_config.url,
                    provider=model_config.provider
                )
                
                # Record key usage
                await self.key_manager.record_key_usage(
                    api_key_config.name,
                    llm_response.success
                )
                
                # Log LLM request metrics
                self.metrics.log_llm_request(
                    model_name=model_config.name,
                    provider=model_config.provider,
                    duration_ms=llm_response.processing_time_ms,
                    success=llm_response.success,
                    retry_count=attempt,
                    error_type=llm_response.error.split(':')[0] if llm_response.error else None,
                    key_name=api_key_config.name
                )
                
                # Handle LLM response
                if not llm_response.success:
                    if llm_response.error and llm_response.error.startswith('RATE_LIMIT'):
                        # Rate limit hit - exclude key
                        await self.key_manager.mark_key_exhausted(api_key_config.name)
                        logger.warning(f"Key {api_key_config.name} rate limited, excluding from rotation")
                    
                    # For other errors, continue to next attempt/model
                    continue
                
                # Parse LLM response
                if not llm_response.content:
                    logger.warning(f"Empty response from LLM")
                    continue
                
                classification_result = await self.message_parser.parse_llm_response(
                    llm_response.content,
                    context.parsing_schema_version
                )
                
                # Validate parsed data
                validation_passed = True
                validation_errors = []
                
                if classification_result.classification == ClassificationType.ORDER and classification_result.parsed_data:
                    validation_errors = await self.message_validator.validate_order_data(
                        classification_result.parsed_data.model_dump()
                    )
                    
                elif classification_result.classification == ClassificationType.OFFER and classification_result.parsed_data:
                    # Validate and enhance OFFER data
                    offer_dict = classification_result.parsed_data.model_dump()
                    validation_errors = await self.message_validator.validate_offer_data(offer_dict)
                    
                    # Enhance with telegram from sender if needed
                    enhanced_offer_dict = await self.message_validator.enhance_offer_data(
                        offer_dict, telegram_message
                    )
                    
                    # Update classification result with enhanced data
                    from domain.dto import OfferData
                    classification_result.parsed_data = OfferData(**enhanced_offer_dict)
                
                validation_passed = len(validation_errors) == 0
                
                # Log parsing metrics
                self.metrics.log_parsing_result(
                    idempotency_key=context.idempotency_key,
                    classification=classification_result.classification.value,
                    parsing_success=True,
                    validation_success=validation_passed,
                    schema_version=context.parsing_schema_version,
                    validation_errors=validation_errors
                )
                
                if not validation_passed:
                    logger.warning(
                        f"Validation failed, will retry",
                        extra={
                            "component": "preprocessing_service",
                            "idempotency_key": context.idempotency_key,
                            "validation_errors": validation_errors
                        }
                    )
                    continue  # Retry with same model
                
                # Create final result
                processing_metadata = ProcessingMetadata(
                    model_used=model_config.name,
                    provider=model_config.provider,
                    processing_time_ms=context.get_processing_time_ms(),
                    retry_count=context.retry_count,
                    validation_passed=validation_passed,
                    telegram_filled_from_sender=getattr(
                        classification_result.parsed_data, '_telegram_filled_from_sender', False
                    ) if classification_result.parsed_data else False
                )
                
                # Create preprocessed message
                preprocessed_message = PreprocessedMessageDTO(
                    parsing_schema_version=f"{classification_result.classification.value.lower()}_{context.parsing_schema_version}",
                    classification=classification_result.classification,
                    parsed_data=classification_result.parsed_data,
                    original_message=telegram_message,
                    processing_metadata=processing_metadata
                )
                
                # Publish to output queue
                publish_success = await self.output_publisher.publish_processed_message(preprocessed_message)
                
                if not publish_success:
                    logger.error(f"Failed to publish processed message")
                    continue  # Retry
                
                logger.info(
                    f"Successfully processed message",
                    extra={
                        "component": "preprocessing_service",
                        "idempotency_key": context.idempotency_key,
                        "classification": classification_result.classification.value,
                        "model_used": model_config.name,
                        "processing_time_ms": context.get_processing_time_ms()
                    }
                )
                
                return preprocessed_message
                
            except ParsingError as e:
                logger.warning(
                    f"Parsing error (will retry): {e}",
                    extra={
                        "component": "preprocessing_service",
                        "idempotency_key": context.idempotency_key,
                        "model": model_config.name,
                        "attempt": attempt + 1
                    }
                )
                continue  # Retry with same model
                
            except Exception as e:
                logger.error(
                    f"Unexpected error in model attempt: {e}",
                    extra={
                        "component": "preprocessing_service",
                        "idempotency_key": context.idempotency_key,
                        "model": model_config.name,
                        "attempt": attempt + 1,
                        "error": str(e)
                    }
                )
                continue  # Retry with same model
        
        # All attempts with this model failed
        return None
    
    async def _handle_failed_message(
        self,
        telegram_message: TelegramEventDTO,
        context: ProcessingContext,
        failure_reason: str,
        last_error: Optional[str] = None
    ) -> None:
        """Handle message that failed all processing attempts by sending to output queue with FAILED classification."""
        try:
            # Create processing metadata with failure details
            processing_metadata = ProcessingMetadata(
                model_used=context.models_attempted[-1] if context.models_attempted else "none",
                provider="unknown",
                processing_time_ms=context.get_processing_time_ms(),
                retry_count=context.retry_count,
                validation_passed=False,
                failure_reason=failure_reason,
                models_attempted=context.models_attempted.copy(),
                last_error=last_error
            )
            
            # Create failed message with FAILED classification
            failed_message = PreprocessedMessageDTO(
                parsing_schema_version="failed",  # Special schema version for failed messages
                classification=ClassificationType.FAILED,
                parsed_data=None,  # Always null for failed messages
                original_message=telegram_message,
                processing_metadata=processing_metadata
            )
            
            # Publish to output queue like any other message
            publish_success = await self.output_publisher.publish_processed_message(failed_message)
            
            if publish_success:
                self._stats.increment_failed()
                
                logger.info(
                    f"Published failed message to output queue",
                    extra={
                        "component": "preprocessing_service",
                        "idempotency_key": context.idempotency_key,
                        "failure_reason": failure_reason,
                        "models_attempted": context.models_attempted,
                        "retry_count": context.retry_count
                    }
                )
            else:
                logger.error(
                    f"Failed to publish failed message to output queue",
                    extra={
                        "component": "preprocessing_service",
                        "idempotency_key": context.idempotency_key,
                        "failure_reason": failure_reason
                    }
                )
            
        except Exception as e:
            logger.error(
                f"Error handling failed message: {e}",
                extra={
                    "component": "preprocessing_service",
                    "idempotency_key": context.idempotency_key,
                    "failure_reason": failure_reason,
                    "error": str(e)
                }
            )
    
    async def get_stats(self) -> ProcessingStats:
        """Get current processing statistics."""
        # Update model usage stats
        for model_name in self._stats.models_used:
            # This would be updated during processing
            pass
        
        # Get key usage stats
        key_stats = await self.key_manager.get_key_stats()
        self._stats.key_usage_stats = key_stats
        
        return self._stats.model_copy()
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform comprehensive health check."""
        health_status = {
            "service_healthy": self._is_running,
            "components": {}
        }
        
        try:
            # Check LLM client
            health_status["components"]["llm_client"] = await self.llm_client.check_health()
            
            # Check output publisher
            health_status["components"]["output_publisher"] = await self.output_publisher.check_health()
            
            # Check key manager (basic check)
            try:
                await self.key_manager.get_key_stats()
                health_status["components"]["key_manager"] = True
            except:
                health_status["components"]["key_manager"] = False
            
            # Overall health
            health_status["overall_healthy"] = (
                health_status["service_healthy"] and
                all(health_status["components"].values())
            )
            
        except Exception as e:
            logger.error(f"Health check error: {e}")
            health_status["overall_healthy"] = False
            health_status["error"] = str(e)
        
        return health_status
    
    async def _periodic_stats(self) -> None:
        """Background task for periodic statistics logging."""
        while self._is_running:
            try:
                await asyncio.sleep(300)  # Every 5 minutes
                
                if not self._is_running:
                    break
                
                stats = await self.get_stats()
                
                self.metrics.log_service_stats(
                    messages_processed=stats.messages_processed,
                    classifications=stats.messages_classified,
                    messages_failed=stats.messages_failed,
                    avg_processing_time_ms=stats.avg_processing_time_ms,
                    models_used=stats.models_used,
                    key_stats={name: stats.requests_today for name, stats in stats.key_usage_stats.items()}
                )
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic stats: {e}")
        
        logger.info("Periodic stats task stopped")