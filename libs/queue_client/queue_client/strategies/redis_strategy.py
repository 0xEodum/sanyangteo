"""
Redis Streams стратегия для queue client
Реплицирует функциональность queue-writer сервиса
"""

import json
import logging
from datetime import datetime, timezone
from typing import AsyncIterator, Dict, Optional

import redis.asyncio as redis
from redis.exceptions import RedisError, ConnectionError

from ..interfaces import (
    QueueWriter, 
    QueueReader, 
    QueueMessage, 
    QueueResult, 
    ConsumerMessage,
    QueueConnectionError,
    QueueWriteError,
    QueueReadError
)


logger = logging.getLogger(__name__)


class RedisQueueWriter(QueueWriter):
    """
    Redis Streams writer с идемпотентностью
    Заменяет queue-writer сервис
    """
    
    def __init__(
        self,
        url: str,
        dedup_ttl_seconds: int = 604800,  # 7 дней
        maxlen_approx: int = 1_000_000,
        max_connections: int = 10,
        socket_timeout: float = 5.0,
        socket_connect_timeout: float = 5.0
    ):
        self.url = url
        self.dedup_ttl_seconds = dedup_ttl_seconds
        self.maxlen_approx = maxlen_approx
        
        # Настройки подключения
        self.connection_kwargs = {
            "max_connections": max_connections,
            "socket_timeout": socket_timeout,
            "socket_connect_timeout": socket_connect_timeout,
            "retry_on_timeout": True,
            "health_check_interval": 30
        }
        
        self.client: Optional[redis.Redis] = None
        self._connected = False
    
    async def connect(self) -> None:
        """Подключиться к Redis"""
        try:
            # Создаем connection pool
            pool = redis.ConnectionPool.from_url(
                self.url,
                **self.connection_kwargs
            )
            
            self.client = redis.Redis(connection_pool=pool)
            
            # Тестируем подключение
            await self.client.ping()
            self._connected = True
            
            logger.info(
                f"Connected to Redis: {self.url}",
                extra={"component": "redis_queue_writer"}
            )
            
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise QueueConnectionError(f"Redis connection failed: {e}") from e
    
    async def write(self, queue_name: str, message: QueueMessage) -> QueueResult:
        """
        Записать сообщение в Redis Stream с дедупликацией
        Точно такая же логика как в queue-writer
        """
        if not self._connected or not self.client:
            raise QueueConnectionError("Not connected to Redis")
        
        try:
            # Ключи для дедупликации и stream
            dedup_set_key = f"{queue_name}:dedup"
            stream_key = queue_name
            
            # Проверяем дедупликацию (атомарная операция)
            is_new = await self.client.sadd(dedup_set_key, message.key)
            
            if not is_new:
                # Дубликат найден
                logger.debug(
                    f"Duplicate message detected: {message.key}",
                    extra={
                        "component": "redis_queue_writer",
                        "queue_name": queue_name,
                        "message_key": message.key
                    }
                )
                return QueueResult(
                    success=True,
                    message_id="duplicate",
                    is_duplicate=True
                )
            
            # Устанавливаем TTL для ключа дедупликации
            await self.client.expire(
                f"{dedup_set_key}:{message.key}",
                self.dedup_ttl_seconds
            )
            
            # Подготавливаем данные для stream
            stream_data = self._prepare_stream_data(message)
            
            # Записываем в stream с обрезкой
            stream_id = await self.client.xadd(
                stream_key,
                stream_data,
                maxlen=self.maxlen_approx,
                approximate=True
            )
            
            logger.debug(
                f"Message written to stream: {stream_id}",
                extra={
                    "component": "redis_queue_writer",
                    "queue_name": queue_name,
                    "stream_id": stream_id,
                    "message_key": message.key
                }
            )
            
            return QueueResult(
                success=True,
                message_id=stream_id,
                is_duplicate=False
            )
            
        except RedisError as e:
            logger.error(
                f"Redis write error: {e}",
                extra={
                    "component": "redis_queue_writer",
                    "queue_name": queue_name,
                    "message_key": message.key,
                    "error": str(e)
                }
            )
            raise QueueWriteError(f"Redis write failed: {e}") from e
        
        except Exception as e:
            logger.error(f"Unexpected write error: {e}")
            raise QueueWriteError(f"Unexpected error: {e}") from e
    
    def _prepare_stream_data(self, message: QueueMessage) -> Dict[str, str]:
        """
        Подготовить данные для Redis Stream
        Redis требует строковые значения
        """
        # Базовые поля
        stream_data = {
            "key": message.key,
            "timestamp": message.timestamp.isoformat(),
            "data_json": json.dumps(message.data, default=str),
            "headers_json": json.dumps(message.headers or {}, default=str)
        }
        
        # Если в data есть структурированные поля (как в TelegramEventDTO)
        # раскладываем их для удобства чтения
        if isinstance(message.data, dict):
            for key, value in message.data.items():
                if isinstance(value, (str, int, float, bool)):
                    stream_data[f"data_{key}"] = str(value)
                elif isinstance(value, dict):
                    # Вложенные объекты как JSON
                    stream_data[f"data_{key}_json"] = json.dumps(value, default=str)
        
        return stream_data
    
    async def health_check(self) -> bool:
        """Проверить здоровье Redis подключения"""
        try:
            if not self.client:
                return False
            await self.client.ping()
            return True
        except Exception as e:
            logger.warning(f"Redis health check failed: {e}")
            return False
    
    async def close(self) -> None:
        """Закрыть подключение к Redis"""
        try:
            if self.client:
                await self.client.aclose()
                self.client = None
                self._connected = False
                
            logger.info("Redis connection closed")
            
        except Exception as e:
            logger.error(f"Error closing Redis connection: {e}")


class RedisQueueReader(QueueReader):
    """
    Redis Streams reader для чтения сообщений
    """
    
    def __init__(
        self,
        url: str,
        max_connections: int = 10,
        socket_timeout: float = 5.0
    ):
        self.url = url
        self.connection_kwargs = {
            "max_connections": max_connections,
            "socket_timeout": socket_timeout,
            "retry_on_timeout": True
        }
        
        self.client: Optional[redis.Redis] = None
        self._connected = False
    
    async def connect(self) -> None:
        """Подключиться к Redis"""
        try:
            pool = redis.ConnectionPool.from_url(
                self.url,
                **self.connection_kwargs
            )
            
            self.client = redis.Redis(connection_pool=pool)
            await self.client.ping()
            self._connected = True
            
            logger.info("Redis reader connected")
            
        except Exception as e:
            raise QueueConnectionError(f"Redis reader connection failed: {e}") from e
    
    async def read(
        self,
        queue_name: str,
        consumer_group: str,
        consumer_id: str = "default"
    ) -> AsyncIterator[ConsumerMessage]:
        """
        Читать сообщения из Redis Stream
        """
        if not self._connected or not self.client:
            raise QueueConnectionError("Not connected to Redis")
        
        try:
            # Создаем consumer group если не существует
            try:
                await self.client.xgroup_create(
                    queue_name, 
                    consumer_group, 
                    id="0", 
                    mkstream=True
                )
            except redis.ResponseError as e:
                if "BUSYGROUP" not in str(e):
                    raise
            
            while True:
                try:
                    # Читаем новые сообщения
                    messages = await self.client.xreadgroup(
                        consumer_group,
                        consumer_id,
                        {queue_name: ">"},
                        count=10,
                        block=1000  # 1 секунда
                    )
                    
                    for stream_name, stream_messages in messages:
                        for message_id, fields in stream_messages:
                            yield self._parse_message(
                                message_id, 
                                queue_name, 
                                fields
                            )
                            
                except redis.ConnectionError:
                    logger.warning("Redis connection lost, reconnecting...")
                    await self.connect()
                    
                except Exception as e:
                    logger.error(f"Error reading from stream: {e}")
                    raise QueueReadError(f"Read failed: {e}") from e
        
        except Exception as e:
            raise QueueReadError(f"Stream read error: {e}") from e
    
    def _parse_message(
        self, 
        message_id: str, 
        queue_name: str, 
        fields: Dict[bytes, bytes]
    ) -> ConsumerMessage:
        """Парсить сообщение из Redis Stream"""
        try:
            # Конвертируем bytes в strings
            str_fields = {
                k.decode('utf-8'): v.decode('utf-8') 
                for k, v in fields.items()
            }
            
            # Извлекаем основные поля
            key = str_fields.get("key", "")
            timestamp_str = str_fields.get("timestamp", "")
            data_json = str_fields.get("data_json", "{}")
            headers_json = str_fields.get("headers_json", "{}")
            
            # Парсим JSON
            data = json.loads(data_json)
            headers = json.loads(headers_json)
            
            # Парсим timestamp
            try:
                timestamp = datetime.fromisoformat(timestamp_str)
            except (ValueError, TypeError):
                timestamp = datetime.now(timezone.utc)
            
            return ConsumerMessage(
                message_id=message_id,
                queue_name=queue_name,
                key=key,
                data=data,
                headers=headers,
                timestamp=timestamp,
                offset=message_id
            )
            
        except Exception as e:
            logger.error(f"Failed to parse message: {e}")
            # Возвращаем сырые данные в случае ошибки
            return ConsumerMessage(
                message_id=message_id,
                queue_name=queue_name,
                key="parse_error",
                data={"raw_fields": str_fields, "error": str(e)},
                headers={},
                timestamp=datetime.now(timezone.utc)
            )
    
    async def ack(self, queue_name: str, message_id: str) -> bool:
        """Подтвердить обработку сообщения"""
        try:
            if not self.client:
                return False
                
            result = await self.client.xack(
                queue_name,
                "default_group",  # TODO: сделать configurable
                message_id
            )
            return result > 0
            
        except Exception as e:
            logger.error(f"Failed to ack message {message_id}: {e}")
            return False
    
    async def close(self) -> None:
        """Закрыть подключение"""
        try:
            if self.client:
                await self.client.aclose()
                self.client = None
                self._connected = False
                
            logger.info("Redis reader connection closed")
            
        except Exception as e:
            logger.error(f"Error closing Redis reader: {e}")