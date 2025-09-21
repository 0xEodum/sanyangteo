"""
Output publisher using shared queue library.
Publishes processed messages to preprocessed_data queue.
"""

import logging
from typing import Optional

from queue_client import create_queue_client, QueueMessage, QueueClient
from queue_client.interfaces import QueueConnectionError

from domain.ports import OutputPublisher, PublishError
from domain.dto import PreprocessedMessageDTO


logger = logging.getLogger(__name__)


class QueueOutputPublisher(OutputPublisher):
    """
    Output publisher using shared queue library.
    Publishes processed messages to preprocessed_data queue.
    """
    
    def __init__(
        self,
        queue_type: str,
        queue_config: dict,
        queue_name: str = "preprocessed_data"
    ):
        """
        Initialize output publisher.
        
        Args:
            queue_type: Type of queue ("redis")
            queue_config: Queue configuration
            queue_name: Name of output queue
        """
        self.queue_type = queue_type
        self.queue_config = queue_config
        self.queue_name = queue_name
        
        self.client: Optional[QueueClient] = None
        self._connected = False
    
    async def connect(self) -> None:
        """Connect to output queue."""
        try:
            self.client = await create_queue_client(
                self.queue_type,
                self.queue_config
            )
            self._connected = True
            
            logger.info(
                f"Connected to output queue",
                extra={
                    "component": "output_publisher",
                    "queue_type": self.queue_type,
                    "queue_name": self.queue_name
                }
            )
            
        except Exception as e:
            logger.error(f"Failed to connect to output queue: {e}")
            raise QueueConnectionError(f"Output queue connection failed: {e}") from e
    
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
        """
        if not self._connected or not self.client:
            raise PublishError("Publisher not connected")
        
        try:
            # Convert message to queue format
            queue_message = self._convert_to_queue_message(message)
            
            # Publish to queue
            result = await self.client.write(self.queue_name, queue_message)
            
            if result.success:
                logger.info(
                    f"Published processed message",
                    extra={
                        "component": "output_publisher",
                        "idempotency_key": message.original_message.idempotency_key,
                        "classification": message.classification.value,
                        "stream_id": result.message_id,
                        "is_duplicate": result.is_duplicate,
                        "queue_name": self.queue_name
                    }
                )
                return True
            else:
                error_msg = result.error or "Unknown queue error"
                logger.error(
                    f"Failed to publish processed message: {error_msg}",
                    extra={
                        "component": "output_publisher",
                        "idempotency_key": message.original_message.idempotency_key,
                        "classification": message.classification.value,
                        "error": error_msg
                    }
                )
                return False
                
        except QueueConnectionError as e:
            error_msg = f"Queue connection error: {e}"
            logger.error(error_msg)
            raise PublishError(error_msg) from e
        
        except Exception as e:
            error_msg = f"Unexpected error during publish: {e}"
            logger.error(
                error_msg,
                extra={
                    "component": "output_publisher",
                    "idempotency_key": message.original_message.idempotency_key,
                    "classification": message.classification.value,
                    "error": str(e)
                }
            )
            raise PublishError(error_msg) from e
    
    def _convert_to_queue_message(self, message: PreprocessedMessageDTO) -> QueueMessage:
        """
        Convert PreprocessedMessageDTO to QueueMessage.
        
        Args:
            message: Processed message
            
        Returns:
            Queue message ready to publish
        """
        # Serialize message to dict for data
        message_data = message.model_dump()
        
        # Create headers with metadata
        headers = {
            "service": "message-preprocessor",
            "schema_version": message.schema_version,
            "parsing_schema_version": message.parsing_schema_version,
            "classification": message.classification.value,
            "model_used": message.processing_metadata.model_used,
            "provider": message.processing_metadata.provider,
            "validation_passed": str(message.processing_metadata.validation_passed),
            "retry_count": str(message.processing_metadata.retry_count)
        }
        
        # Use original idempotency key for deduplication
        return QueueMessage(
            key=message.original_message.idempotency_key,
            data=message_data,
            headers=headers,
            timestamp=message.processing_timestamp
        )
    
    async def check_health(self) -> bool:
        """
        Check if publisher is healthy.
        
        Returns:
            True if healthy, False otherwise
        """
        try:
            if not self._connected or not self.client:
                return False
            
            health = await self.client.health_check()
            return health.get("overall_healthy", False)
            
        except Exception as e:
            logger.warning(
                f"Health check failed: {e}",
                extra={
                    "component": "output_publisher",
                    "error": str(e)
                }
            )
            return False
    
    async def close(self) -> None:
        """Close publisher and cleanup resources."""
        try:
            if self.client:
                await self.client.close()
                self.client = None
                self._connected = False
            
            logger.info("Output publisher closed")
            
        except Exception as e:
            logger.error(f"Error closing output publisher: {e}")


def create_output_publisher(
    queue_type: str,
    queue_config: dict,
    queue_name: str = "preprocessed_data"
) -> QueueOutputPublisher:
    """
    Create output publisher with configuration.
    
    Args:
        queue_type: Type of queue
        queue_config: Queue configuration
        queue_name: Output queue name
        
    Returns:
        Configured output publisher
    """
    return QueueOutputPublisher(
        queue_type=queue_type,
        queue_config=queue_config,
        queue_name=queue_name
    )