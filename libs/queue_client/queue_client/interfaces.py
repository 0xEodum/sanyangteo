"""
Интерфейсы для queue client library
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, AsyncIterator
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class QueueType(Enum):
    """Поддерживаемые типы очередей"""
    REDIS = "redis"
    KAFKA = "kafka" 
    RABBITMQ = "rabbitmq"


@dataclass
class QueueMessage:
    """Сообщение для очереди"""
    key: str
    data: Dict[str, Any]
    headers: Optional[Dict[str, str]] = None
    partition: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def __post_init__(self):
        if self.headers is None:
            self.headers = {}


@dataclass
class QueueResult:
    """Результат операции с очередью"""
    success: bool
    message_id: str
    is_duplicate: bool = False
    error: Optional[str] = None
    retry_count: int = 0


@dataclass
class ConsumerMessage:
    """Сообщение для консьюмера"""
    message_id: str
    queue_name: str
    key: str
    data: Dict[str, Any]
    headers: Dict[str, str]
    timestamp: datetime
    offset: Optional[str] = None


class QueueWriter(ABC):
    """Абстрактный интерфейс для записи в очередь"""
    
    @abstractmethod
    async def connect(self) -> None:
        """Подключиться к очереди"""
        pass
    
    @abstractmethod
    async def write(self, queue_name: str, message: QueueMessage) -> QueueResult:
        """
        Записать сообщение в очередь
        
        Args:
            queue_name: Имя очереди
            message: Сообщение для записи
            
        Returns:
            Результат операции
        """
        pass
    
    @abstractmethod
    async def health_check(self) -> bool:
        """Проверить здоровье подключения"""
        pass
    
    @abstractmethod
    async def close(self) -> None:
        """Закрыть подключение"""
        pass


class QueueReader(ABC):
    """Абстрактный интерфейс для чтения из очереди"""
    
    @abstractmethod
    async def connect(self) -> None:
        """Подключиться к очереди"""
        pass
    
    @abstractmethod
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
        """
        pass
    
    @abstractmethod
    async def ack(self, queue_name: str, message_id: str) -> bool:
        """Подтвердить обработку сообщения"""
        pass
    
    @abstractmethod
    async def close(self) -> None:
        """Закрыть подключение"""
        pass


# Кастомные исключения
class QueueConnectionError(Exception):
    """Ошибка подключения к очереди"""
    pass


class QueueWriteError(Exception):
    """Ошибка записи в очередь"""
    pass


class QueueReadError(Exception):
    """Ошибка чтения из очереди"""
    pass


class QueueConfigError(Exception):
    """Ошибка конфигурации"""
    pass