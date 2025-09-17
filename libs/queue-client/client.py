"""
Основной клиент для работы с очередями
Объединяет writer и reader в единый интерфейс
"""

import logging
from typing import AsyncIterator, Optional

from interfaces import (
    QueueWriter, 
    QueueReader, 
    QueueMessage, 
    QueueResult, 
    ConsumerMessage,
    QueueConnectionError
)


logger = logging.getLogger(__name__)


class QueueClient:
    """
    Унифицированный клиент для работы с очередями
    Заменяет HTTP gateway подход
    """
    
    def __init__(self, writer: QueueWriter, reader: Optional[QueueReader] = None):
        """
        Инициализировать клиент
        
        Args:
            writer: Writer для записи в очередь
            reader: Reader для чтения из очереди (опционально)
        """
        self.writer = writer
        self.reader = reader
        self._connected = False
    
    async def connect(self) -> None:
        """Подключиться к очереди"""
        try:
            # Подключаем writer
            await self.writer.connect()
            
            # Подключаем reader если есть
            if self.reader:
                await self.reader.connect()
            
            self._connected = True
            logger.info("Queue client connected successfully")
            
        except Exception as e:
            logger.error(f"Failed to connect queue client: {e}")
            await self.close()
            raise QueueConnectionError(f"Connection failed: {e}") from e
    
    async def write(self, queue_name: str, message: QueueMessage) -> QueueResult:
        """
        Записать сообщение в очередь
        
        Args:
            queue_name: Имя очереди
            message: Сообщение для записи
            
        Returns:
            Результат операции
            
        Raises:
            QueueConnectionError: Если нет подключения
        """
        if not self._connected:
            raise QueueConnectionError("Client not connected")
        
        return await self.writer.write(queue_name, message)
    
    async def read(
        self,
        queue_name: str,
        consumer_group: str,
        consumer_id: str = "default"
    ) -> AsyncIterator[ConsumerMessage]:
        """
        Читать сообщения из очереди
        
        Args:
            queue_name: Имя очереди
            consumer_group: Группа консьюмеров
            consumer_id: ID консьюмера
            
        Yields:
            Сообщения из очереди
            
        Raises:
            QueueConnectionError: Если нет подключения или reader
        """
        if not self._connected:
            raise QueueConnectionError("Client not connected")
        
        if not self.reader:
            raise QueueConnectionError("No reader configured")
        
        async for message in self.reader.read(queue_name, consumer_group, consumer_id):
            yield message
    
    async def ack(self, queue_name: str, message_id: str) -> bool:
        """
        Подтвердить обработку сообщения
        
        Args:
            queue_name: Имя очереди
            message_id: ID сообщения
            
        Returns:
            True если успешно подтверждено
        """
        if not self.reader:
            return False
        
        return await self.reader.ack(queue_name, message_id)
    
    async def health_check(self) -> dict:
        """
        Проверить здоровье подключений
        
        Returns:
            Словарь с результатами проверки
        """
        result = {
            "connected": self._connected,
            "writer_healthy": False,
            "reader_healthy": False
        }
        
        try:
            if self.writer:
                result["writer_healthy"] = await self.writer.health_check()
            
            if self.reader:
                # У reader нет метода health_check в интерфейсе, проверяем через подключение
                result["reader_healthy"] = self.reader is not None
                
        except Exception as e:
            logger.error(f"Health check error: {e}")
        
        result["overall_healthy"] = (
            result["connected"] and 
            result["writer_healthy"] and 
            (result["reader_healthy"] or self.reader is None)
        )
        
        return result
    
    async def close(self) -> None:
        """Закрыть все подключения"""
        try:
            if self.writer:
                await self.writer.close()
            
            if self.reader:
                await self.reader.close()
            
            self._connected = False
            logger.info("Queue client closed")
            
        except Exception as e:
            logger.error(f"Error closing queue client: {e}")
    
    # Context manager support
    async def __aenter__(self):
        """Async context manager entry"""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()


# Удобные helper функции
async def write_to_queue(
    queue_type: str,
    config: dict,
    queue_name: str,
    message: QueueMessage
) -> QueueResult:
    """
    Удобная функция для разовой записи в очередь
    
    Usage:
        result = await write_to_queue(
            "redis",
            {"url": "redis://localhost"},
            "events",
            QueueMessage("key", {"data": "value"})
        )
    """
    from . import create_queue_client
    
    async with await create_queue_client(queue_type, config) as client:
        return await client.write(queue_name, message)


async def read_from_queue(
    queue_type: str,
    config: dict,
    queue_name: str,
    consumer_group: str,
    consumer_id: str = "default"
) -> AsyncIterator[ConsumerMessage]:
    """
    Удобная функция для чтения из очереди
    
    Usage:
        async for message in read_from_queue(
            "redis",
            {"url": "redis://localhost"},
            "events",
            "my_consumer_group"
        ):
            print(message.data)
    """
    from . import create_queue_client
    
    async with await create_queue_client(queue_type, config) as client:
        async for message in client.read(queue_name, consumer_group, consumer_id):
            yield message