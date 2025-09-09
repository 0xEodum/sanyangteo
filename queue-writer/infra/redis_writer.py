"""
Redis Streams implementation of QueueWriter interface.
Provides idempotent event writing with deduplication support.
"""

import json
import logging
from datetime import datetime
from typing import Optional

from redis.exceptions import RedisError

from domain.ports import QueueWriter, QueueWriteError
from domain.schema import TelegramEvent, QueueWriteResult, HealthStatus
from .redis_client import RedisClient


logger = logging.getLogger(__name__)


class RedisStreamWriter(QueueWriter):
    """
    Redis Streams implementation of QueueWriter interface.
    Supports idempotent writes using Redis Sets for deduplication.
    """
    
    def __init__(
        self,
        redis_client: RedisClient,
        stream_key: str = "tg:messages",
        dedup_set_key: str = "tg:dedup",
        dedup_ttl_seconds: int = 604800,  # 7 days
        maxlen_approx: int = 1_000_000
    ):
        """
        Initialize Redis Stream writer.
        
        Args:
            redis_client: Redis client instance
            stream_key: Redis Stream key name
            dedup_set_key: Redis Set key for deduplication
            dedup_ttl_seconds: TTL for deduplication entries (seconds)
            maxlen_approx: Approximate max length for stream trimming
        """
        self.redis_client = redis_client
        self.stream_key = stream_key
        self.dedup_set_key = dedup_set_key
        self.dedup_ttl_seconds = dedup_ttl_seconds
        self.maxlen_approx = maxlen_approx
    
    async def write_event(self, event: TelegramEvent) -> QueueWriteResult:
        """
        Write event to Redis Stream with idempotency check.
        
        Uses Redis Set to track processed idempotency keys.
        If key already exists, returns duplicate result without writing.
        
        Args:
            event: The telegram event to write
            
        Returns:
            QueueWriteResult with stream_id and duplicate flag
            
        Raises:
            QueueWriteError: If Redis operations fail
        """
        try:
            # Check for duplicate using SADD (atomic operation)
            is_new = await self.redis_client.client.sadd(
                self.dedup_set_key, 
                event.idempotency_key
            )
            
            if not is_new:
                # Duplicate detected
                logger.info(
                    f"Duplicate event detected: {event.idempotency_key}",
                    extra={
                        "component": "redis_writer",
                        "chat_id": event.chat.id,
                        "message_id": event.message.id,
                        "idempotency_key": event.idempotency_key
                    }
                )
                return QueueWriteResult(
                    stream_id="duplicate",
                    is_duplicate=True
                )
            
            # Set TTL for deduplication key
            await self.redis_client.client.expire(
                f"{self.dedup_set_key}:{event.idempotency_key}",
                self.dedup_ttl_seconds
            )
            
            # Prepare event data for stream
            stream_data = self._prepare_stream_data(event)
            
            # Write to stream with trimming
            stream_id = await self.redis_client.client.xadd(
                self.stream_key,
                stream_data,
                maxlen=self.maxlen_approx,
                approximate=True
            )
            
            logger.info(
                f"Event written to stream: {stream_id}",
                extra={
                    "component": "redis_writer",
                    "chat_id": event.chat.id,
                    "message_id": event.message.id,
                    "stream_id": stream_id,
                    "idempotency_key": event.idempotency_key
                }
            )
            
            return QueueWriteResult(
                stream_id=stream_id,
                is_duplicate=False
            )
            
        except RedisError as e:
            logger.error(
                f"Failed to write event to Redis: {e}",
                extra={
                    "component": "redis_writer",
                    "chat_id": event.chat.id,
                    "message_id": event.message.id,
                    "error": str(e)
                }
            )
            raise QueueWriteError(f"Redis write failed: {e}") from e
        
        except Exception as e:
            logger.error(
                f"Unexpected error writing event: {e}",
                extra={
                    "component": "redis_writer",
                    "chat_id": event.chat.id,
                    "message_id": event.message.id,
                    "error": str(e)
                }
            )
            raise QueueWriteError(f"Unexpected error: {e}") from e
    
    async def check_health(self) -> HealthStatus:
        """
        Check Redis health by pinging and checking stream info.
        
        Returns:
            HealthStatus with current state
        """
        checks = {}
        overall_status = "healthy"
        
        try:
            # Test Redis connectivity
            ping_result = await self.redis_client.ping()
            checks["redis_ping"] = "ok" if ping_result else "failed"
            
            if not ping_result:
                overall_status = "unhealthy"
            
            # Test stream operations
            try:
                stream_info = await self.redis_client.client.xinfo_stream(
                    self.stream_key
                )
                checks["stream_accessible"] = "ok"
                checks["stream_length"] = str(stream_info.get("length", "unknown"))
                
            except RedisError:
                # Stream might not exist yet, which is OK
                checks["stream_accessible"] = "not_exists_ok"
            
            # Test deduplication set
            try:
                dedup_size = await self.redis_client.client.scard(self.dedup_set_key)
                checks["dedup_set"] = f"ok_size_{dedup_size}"
                
            except RedisError as e:
                checks["dedup_set"] = f"error_{e}"
                overall_status = "unhealthy"
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            overall_status = "unhealthy"
            checks["general"] = f"error_{e}"
        
        return HealthStatus(
            status=overall_status,
            timestamp=datetime.utcnow(),
            checks=checks
        )
    
    def _prepare_stream_data(self, event: TelegramEvent) -> dict[str, str]:
        """
        Prepare event data for Redis Stream storage.
        
        Redis Streams require string values, so we serialize complex data as JSON.
        
        Args:
            event: The telegram event
            
        Returns:
            Dictionary with string keys and values for stream
        """
        return {
            "schema_version": event.schema_version,
            "event_type": event.event_type,
            "event_ts": event.event_ts.isoformat(),
            "idempotency_key": event.idempotency_key,
            
            # Chat info
            "chat_id": str(event.chat.id),
            "chat_type": event.chat.type,
            "chat_title": event.chat.title,
            
            # Message info
            "message_id": str(event.message.id),
            "message_text": event.message.text,
            
            # Sender info
            "sender_id": str(event.sender.id),
            "sender_username": event.sender.username or "",
            "sender_display_name": event.sender.display_name,
            "sender_is_bot": str(event.sender.is_bot),
            
            # Full event as JSON for downstream consumers who want complete data
            "event_json": event.model_dump_json()
        }
    
    async def get_stream_info(self) -> Optional[dict]:
        """
        Get stream information for monitoring.
        
        Returns:
            Stream info dictionary or None if stream doesn't exist
        """
        try:
            return await self.redis_client.client.xinfo_stream(self.stream_key)
        except RedisError:
            return None