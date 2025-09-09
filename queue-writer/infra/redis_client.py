"""
Redis client wrapper for queue-writer service.
Provides connection management and basic Redis operations.
"""

import logging
from contextlib import asynccontextmanager
from typing import Optional, AsyncGenerator

import redis.asyncio as redis
from redis.exceptions import RedisError, ConnectionError


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
        """
        Initialize Redis client.
        
        Args:
            url: Redis connection URL (redis://host:port/db)
            max_connections: Maximum connections in pool
            socket_timeout: Socket timeout in seconds
            socket_connect_timeout: Connection timeout in seconds
            retry_on_timeout: Whether to retry on timeout
            health_check_interval: Health check interval in seconds
        """
        self.url = url
        self._pool: Optional[redis.ConnectionPool] = None
        self._client: Optional[redis.Redis] = None
        
        self._pool_config = {
            "max_connections": max_connections,
            "socket_timeout": socket_timeout,
            "socket_connect_timeout": socket_connect_timeout,
            "retry_on_timeout": retry_on_timeout,
            "health_check_interval": health_check_interval
        }
    
    async def connect(self) -> None:
        """
        Establish connection to Redis.
        
        Raises:
            ConnectionError: If connection fails
        """
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
    def client(self) -> redis.Redis:
        """
        Get Redis client instance.
        
        Returns:
            Redis client
            
        Raises:
            RuntimeError: If not connected
        """
        if not self._client:
            raise RuntimeError("Redis client not connected. Call connect() first.")
        return self._client
    
    async def ping(self) -> bool:
        """
        Ping Redis to check connectivity.
        
        Returns:
            True if ping successful, False otherwise
        """
        try:
            if not self._client:
                return False
            
            await self._client.ping()
            return True
            
        except (RedisError, ConnectionError) as e:
            logger.warning(f"Redis ping failed: {e}")
            return False
    
    async def info(self) -> dict:
        """
        Get Redis server info.
        
        Returns:
            Dictionary with Redis server information
            
        Raises:
            RedisError: If operation fails
        """
        try:
            return await self.client.info()
        except RedisError as e:
            logger.error(f"Failed to get Redis info: {e}")
            raise
    
    @asynccontextmanager
    async def pipeline(self) -> AsyncGenerator[redis.Pipeline, None]:
        """
        Context manager for Redis pipeline operations.
        
        Yields:
            Redis pipeline instance
        """
        pipe = self.client.pipeline()
        try:
            yield pipe
        finally:
            await pipe.reset()
    
    async def execute_pipeline(self, pipeline: redis.Pipeline) -> list:
        """
        Execute pipeline and handle errors.
        
        Args:
            pipeline: Redis pipeline to execute
            
        Returns:
            List of results
            
        Raises:
            RedisError: If pipeline execution fails
        """
        try:
            return await pipeline.execute()
        except RedisError as e:
            logger.error(f"Pipeline execution failed: {e}")
            raise