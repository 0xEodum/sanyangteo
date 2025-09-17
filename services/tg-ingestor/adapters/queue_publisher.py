"""
Queue publisher using shared queue library - CLEAN VERSION
Заменяет http_publisher.py
"""

import logging
from typing import Optional

from queue_client import create_queue_client, QueueMessage, QueueClient
from queue_client.interfaces import QueueConnectionError

from domain.ports import Publisher, PublishError
from domain.dto import TelegramEventDTO, PublishResult


logger = logging.getLogger(__name__)


class QueuePublisher(Publisher):
    """
    Publisher использующий shared queue library
    ТОЛЬКО Redis, без HTTP fallback
    """
    
    def __init__(
        self,
        queue_type: str,
        queue_config: dict,
        queue_name: str = "telegram_events"
    ):
        """
        Инициализировать publisher
        
        Args:
            queue_type: Тип очереди ("redis")
            queue_config: Конфигурация подключения к очереди
            queue_name: Имя очереди для событий
        """
        self.queue_type = queue_type
        self.queue_config = queue_config
        self.queue_name = queue_name
        
        self.client: Optional[QueueClient] = None
        self._connected = False
    
    async def connect(self) -> None:
        """Подключиться к очереди"""
        try:
            self.client = await create_queue_client(
                self.queue_type,
                self.queue_config
            )
            self._connected = True
            
            logger.info(
                f"Connected to {self.queue_type} queue",
                extra={
                    "component": "queue_publisher",
                    "queue_type": self.queue_type,
                    "queue_name": self.queue_name
                }
            )
            
        except Exception as e:
            logger.error(f"Failed to connect to queue: {e}")
            raise PublishError(f"Queue connection failed: {e}") from e
    
    async def publish_event(self, event: TelegramEventDTO) -> PublishResult:
        """
        Опубликовать событие в очередь
        
        Args:
            event: Событие для публикации
            
        Returns:
            Результат публикации
            
        Raises:
            PublishError: Если публикация не удалась
        """
        if not self._connected or not self.client:
            raise PublishError("Publisher not connected")
        
        try:
            # Конвертируем TelegramEventDTO в QueueMessage
            queue_message = self._convert_to_queue_message(event)
            
            # Записываем в очередь
            result = await self.client.write(self.queue_name, queue_message)
            
            if result.success:
                logger.info(
                    f"Event published successfully",
                    extra={
                        "component": "queue_publisher",
                        "chat_id": event.chat.id,
                        "message_id": event.message.id,
                        "stream_id": result.message_id,
                        "is_duplicate": result.is_duplicate,
                        "queue_name": self.queue_name
                    }
                )
                
                return PublishResult(
                    success=True,
                    stream_id=result.message_id,
                    is_duplicate=result.is_duplicate
                )
            else:
                error_msg = result.error or "Unknown queue error"
                logger.error(
                    f"Failed to publish event: {error_msg}",
                    extra={
                        "component": "queue_publisher",
                        "chat_id": event.chat.id,
                        "message_id": event.message.id,
                        "error": error_msg
                    }
                )
                
                return PublishResult(
                    success=False,
                    error=error_msg
                )
                
        except QueueConnectionError as e:
            error_msg = f"Queue connection error: {e}"
            logger.error(error_msg)
            raise PublishError(error_msg) from e
        
        except Exception as e:
            error_msg = f"Unexpected error during publish: {e}"
            logger.error(
                error_msg,
                extra={
                    "component": "queue_publisher",
                    "chat_id": event.chat.id,
                    "message_id": event.message.id,
                    "error": str(e)
                }
            )
            raise PublishError(error_msg) from e
    
    def _convert_to_queue_message(self, event: TelegramEventDTO) -> QueueMessage:
        """
        Конвертировать TelegramEventDTO в QueueMessage
        
        Args:
            event: Событие Telegram
            
        Returns:
            Сообщение для очереди
        """
        # Сериализуем event в словарь для data
        event_data = event.model_dump()
        
        # Создаем headers с метаинформацией
        headers = {
            "service": "tg-ingestor",
            "event_type": event.event_type,
            "schema_version": event.schema_version,
            "chat_type": event.chat.type,
            "sender_is_bot": str(event.sender.is_bot)
        }
        
        return QueueMessage(
            key=event.idempotency_key,
            data=event_data,
            headers=headers,
            timestamp=event.event_ts
        )
    
    async def check_health(self) -> bool:
        """
        Проверить здоровье соединения с очередью
        
        Returns:
            True если соединение здоровое
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
                    "component": "queue_publisher",
                    "error": str(e)
                }
            )
            return False
    
    async def close(self) -> None:
        """Закрыть соединение с очередью"""
        try:
            if self.client:
                await self.client.close()
                self.client = None
                self._connected = False
                
            logger.info("Queue publisher closed")
            
        except Exception as e:
            logger.error(f"Error closing queue publisher: {e}")