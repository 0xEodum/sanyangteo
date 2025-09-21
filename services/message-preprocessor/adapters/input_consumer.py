"""
Input message consumer using shared queue library.
Consumes messages from telegram_events queue.
"""

import asyncio
import logging
from typing import Callable, Optional
import json

from queue_client import create_queue_client, QueueClient
from queue_client.interfaces import QueueConnectionError

from domain.ports import InputMessageConsumer
from domain.dto import TelegramEventDTO


logger = logging.getLogger(__name__)


class QueueInputConsumer(InputMessageConsumer):
    """
    Input consumer using shared queue library.
    Reads messages from telegram_events queue and passes them to message handler.
    """
    
    def __init__(
        self,
        queue_type: str,
        queue_config: dict,
        queue_name: str,
        consumer_group: str,
        consumer_id: str = "default"
    ):
        """
        Initialize input consumer.
        
        Args:
            queue_type: Type of queue ("redis")
            queue_config: Queue configuration
            queue_name: Name of input queue
            consumer_group: Consumer group name
            consumer_id: Consumer ID within group
        """
        self.queue_type = queue_type
        self.queue_config = queue_config
        self.queue_name = queue_name
        self.consumer_group = consumer_group
        self.consumer_id = consumer_id
        
        self.client: Optional[QueueClient] = None
        self._consuming = False
        self._consume_task: Optional[asyncio.Task] = None
        self._message_handler: Optional[Callable] = None
    
    async def connect(self) -> None:
        """Connect to input queue."""
        try:
            self.client = await create_queue_client(
                self.queue_type,
                self.queue_config
            )
            
            logger.info(
                f"Connected to input queue",
                extra={
                    "component": "input_consumer",
                    "queue_type": self.queue_type,
                    "queue_name": self.queue_name,
                    "consumer_group": self.consumer_group
                }
            )
            
        except Exception as e:
            logger.error(f"Failed to connect to input queue: {e}")
            raise QueueConnectionError(f"Input queue connection failed: {e}") from e
    
    async def start_consuming(
        self, 
        message_handler: Callable
    ) -> None:
        """
        Start consuming messages from input queue.
        
        Args:
            message_handler: Async function to handle each message
        """
        if self._consuming:
            logger.warning("Consumer already running")
            return
        
        if not self.client:
            await self.connect()
        
        self._message_handler = message_handler
        self._consuming = True
        
        # Start consumption task
        self._consume_task = asyncio.create_task(self._consume_loop())
        
        logger.info(
            f"Started consuming messages",
            extra={
                "component": "input_consumer",
                "queue_name": self.queue_name,
                "consumer_group": self.consumer_group,
                "consumer_id": self.consumer_id
            }
        )
    
    async def stop_consuming(self) -> None:
        """Stop consuming messages."""
        if not self._consuming:
            return
        
        self._consuming = False
        
        if self._consume_task and not self._consume_task.done():
            self._consume_task.cancel()
            try:
                await self._consume_task
            except asyncio.CancelledError:
                pass
        
        logger.info("Stopped consuming messages")
    
    async def _consume_loop(self) -> None:
        """Main consumption loop."""
        try:
            if not self.client:
                raise QueueConnectionError("Client not connected")
            
            logger.info(
                f"Starting message consumption loop",
                extra={
                    "component": "input_consumer",
                    "queue_name": self.queue_name
                }
            )
            
            async for message in self.client.read(
                self.queue_name,
                self.consumer_group,
                self.consumer_id
            ):
                if not self._consuming:
                    break
                
                try:
                    # Parse message data
                    telegram_message = self._parse_message(message)
                    
                    if telegram_message:
                        logger.debug(
                            f"Received message",
                            extra={
                                "component": "input_consumer",
                                "idempotency_key": telegram_message.idempotency_key,
                                "chat_id": telegram_message.chat.get('id'),
                                "message_id": telegram_message.message.get('id')
                            }
                        )
                        
                        # Process message
                        if self._message_handler:
                            await self._message_handler(telegram_message)
                        
                        # Acknowledge message
                        await self.acknowledge_message(message.message_id)
                    else:
                        logger.warning(
                            f"Failed to parse message, acknowledging anyway",
                            extra={
                                "component": "input_consumer",
                                "message_id": message.message_id,
                                "queue_name": self.queue_name
                            }
                        )
                        # Still acknowledge to avoid reprocessing bad messages
                        await self.acknowledge_message(message.message_id)
                
                except Exception as e:
                    logger.error(
                        f"Error processing message: {e}",
                        extra={
                            "component": "input_consumer",
                            "message_id": message.message_id,
                            "error": str(e)
                        }
                    )
                    # Don't acknowledge failed messages - they will be retried
        
        except asyncio.CancelledError:
            logger.info("Consumption loop cancelled")
        except Exception as e:
            logger.error(f"Error in consumption loop: {e}")
            # TODO: Implement reconnection logic
    
    def _parse_message(self, message) -> Optional[TelegramEventDTO]:
        """
        Parse queue message into TelegramEventDTO.
        
        Args:
            message: ConsumerMessage from queue
            
        Returns:
            Parsed TelegramEventDTO or None if parsing fails
        """
        try:
            # Message data should contain the TelegramEventDTO
            message_data = message.data
            
            if not isinstance(message_data, dict):
                logger.warning(f"Message data is not a dict: {type(message_data)}")
                return None
            
            # Create TelegramEventDTO from message data
            telegram_message = TelegramEventDTO(**message_data)
            
            return telegram_message
            
        except Exception as e:
            logger.error(
                f"Failed to parse message: {e}",
                extra={
                    "component": "input_consumer",
                    "message_id": message.message_id,
                    "error": str(e),
                    "data_preview": str(message.data)[:200] if hasattr(message, 'data') else 'N/A'
                }
            )
            return None
    
    async def acknowledge_message(self, message_id: str) -> bool:
        """
        Acknowledge successful message processing.
        
        Args:
            message_id: Message ID to acknowledge
            
        Returns:
            True if acknowledged successfully
        """
        try:
            if not self.client:
                return False
            
            success = await self.client.ack(self.queue_name, message_id)
            
            if success:
                logger.debug(
                    f"Acknowledged message",
                    extra={
                        "component": "input_consumer",
                        "message_id": message_id,
                        "queue_name": self.queue_name
                    }
                )
            else:
                logger.warning(
                    f"Failed to acknowledge message",
                    extra={
                        "component": "input_consumer",
                        "message_id": message_id,
                        "queue_name": self.queue_name
                    }
                )
            
            return success
            
        except Exception as e:
            logger.error(f"Error acknowledging message {message_id}: {e}")
            return False
    
    async def close(self) -> None:
        """Close consumer and cleanup resources."""
        try:
            await self.stop_consuming()
            
            if self.client:
                await self.client.close()
                self.client = None
            
            logger.info("Input consumer closed")
            
        except Exception as e:
            logger.error(f"Error closing input consumer: {e}")


def create_input_consumer(
    queue_type: str,
    queue_config: dict,
    queue_name: str,
    consumer_group: str,
    consumer_id: str = "default"
) -> QueueInputConsumer:
    """
    Create input consumer with configuration.
    
    Args:
        queue_type: Type of queue
        queue_config: Queue configuration
        queue_name: Input queue name
        consumer_group: Consumer group name
        consumer_id: Consumer ID
        
    Returns:
        Configured input consumer
    """
    return QueueInputConsumer(
        queue_type=queue_type,
        queue_config=queue_config,
        queue_name=queue_name,
        consumer_group=consumer_group,
        consumer_id=consumer_id
    )