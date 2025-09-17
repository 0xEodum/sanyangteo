"""
Unified Queue Client Library

Поддерживает Redis Streams, Kafka, RabbitMQ через единый интерфейс.
Замена HTTP gateway подхода на прямое подключение к очередям.
"""

from .interfaces import QueueMessage, QueueResult, QueueType
from .factory import QueueFactory
from .client import QueueClient

# Удобная функция для быстрого создания
async def create_queue_client(queue_type: str, config: dict) -> 'QueueClient':
    """
    Создает и подключает queue client
    
    Usage:
        client = await create_queue_client("redis", {"url": "redis://localhost"})
        result = await client.write("events", QueueMessage("key", {"data": "value"}))
    """
    queue_enum = QueueType(queue_type)
    writer = QueueFactory.create_writer(queue_enum, config)
    reader = QueueFactory.create_reader(queue_enum, config)
    
    client = QueueClient(writer, reader)
    await client.connect()
    return client

__version__ = "1.0.0"
__all__ = [
    "QueueMessage", 
    "QueueResult", 
    "QueueType", 
    "QueueFactory", 
    "QueueClient",
    "create_queue_client"
]