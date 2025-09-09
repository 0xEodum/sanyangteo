"""
Redis client wrapper for queue-writer service.
Provides connection management and basic Redis operations.
"""

import logging
from contextlib import asynccontextmanager
from typing import Optional, AsyncGenerator

import redis.asyncio as redis
from redis.exceptions import RedisError, ConnectionError

# ✅ корректные типы из подмодулей
from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.connection import ConnectionPool as AsyncConnectionPool
from redis.asyncio.client import Pipeline as AsyncPipeline

logger = logging.getLogger(__name__)


class RedisClient:
    """
    Async Redis client wrapper with connection pooling and error handling.
    """

    def __init__(
        self,
        url: str,
        max_connections: int = 10,
        socket_timeout: float = 5.0,
        socket_connect_timeout: float = 5.0,
        retry_on_timeout: bool = True,
        health_check_interval: int = 30
    ):
        self.url = url
        self._pool: Optional[AsyncConnectionPool] = None
        self._client: Optional[AsyncRedis] = None

        self._pool_config = {
            "max_connections": max_connections,
            "socket_timeout": socket_timeout,
            "socket_connect_timeout": socket_connect_timeout,
            "retry_on_timeout": retry_on_timeout,
            "health_check_interval": health_check_interval,
        }

    async def connect(self) -> None:
        try:
            self._pool = redis.ConnectionPool.from_url(
                self.url,
                **self._pool_config
            )
            self._client = redis.Redis(connection_pool=self._pool)

            # Test connection
            await self._client.ping()
            logger.info(f"Connected to Redis: {self.url}")

        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            await self.close()
            raise ConnectionError(f"Redis connection failed: {e}") from e

    async def close(self) -> None:
        """Close Redis connection and cleanup resources."""
        if self._client:
            await self._client.aclose()
            self._client = None

        if self._pool:
            await self._pool.aclose()
            self._pool = None

        logger.info("Redis connection closed")

    @property
    def client(self) -> AsyncRedis:
        if not self._client:
            raise RuntimeError("Redis client not connected. Call connect() first.")
        return self._client

    async def ping(self) -> bool:
        try:
            if not self._client:
                return False
            await self._client.ping()
            return True
        except (RedisError, ConnectionError) as e:
            logger.warning(f"Redis ping failed: {e}")
            return False

    async def info(self) -> dict:
        try:
            return await self.client.info()
        except RedisError as e:
            logger.error(f"Failed to get Redis info: {e}")
            raise

    @asynccontextmanager
    async def pipeline(self) -> AsyncGenerator[AsyncPipeline, None]:
        pipe: AsyncPipeline = self.client.pipeline()
        try:
            yield pipe
        finally:
            await pipe.reset()

    async def execute_pipeline(self, pipeline: AsyncPipeline) -> list:
        try:
            return await pipeline.execute()
        except RedisError as e:
            logger.error(f"Pipeline execution failed: {e}")
            raise
