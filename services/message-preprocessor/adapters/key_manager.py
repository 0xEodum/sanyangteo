"""
API key manager with Redis-based rotation and usage tracking.
Handles both rotation keys (with daily limits) and direct keys (unlimited).
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Set
from dataclasses import dataclass

import redis.asyncio as redis
from redis.exceptions import RedisError

from domain.ports import KeyManager, KeyExhaustedError
from domain.dto import ProcessingMode, APIKeyConfig, KeyUsageStats

logger = logging.getLogger(__name__)


@dataclass
class LoadedKeys:
    """Container for loaded API keys"""
    rotation: List[APIKeyConfig]
    direct: List[APIKeyConfig]


class RedisKeyManager(KeyManager):
    """
    Redis-based key manager with rotation and usage tracking.

    Features:
    - Daily usage limits with UTC reset
    - Key rotation in test mode
    - Persistent usage tracking in Redis
    - Automatic key exclusion on limits/rate limits
    """

    def __init__(
            self,
            redis_url: str,
            key_prefix: str = "llm_key_usage:",
            keys_config: LoadedKeys = None
    ):
        """
        Initialize key manager.

        Args:
            redis_url: Redis connection URL
            key_prefix: Prefix for Redis keys
            keys_config: Loaded API keys configuration
        """
        self.redis_url = redis_url
        self.key_prefix = key_prefix
        self.keys_config = keys_config or LoadedKeys([], [])

        # Redis client
        self.redis_client: Optional[redis.Redis] = None

        # Current rotation state for test mode
        self._rotation_index = 0
        self._exhausted_keys: Set[str] = set()

        # Background task for daily reset
        self._reset_task: Optional[asyncio.Task] = None
        self._should_stop = False

    async def connect(self) -> None:
        """Connect to Redis and start background tasks."""
        try:
            pool = redis.ConnectionPool.from_url(
                self.redis_url,
                max_connections=10,
                socket_timeout=5.0,
                socket_connect_timeout=5.0,
                retry_on_timeout=True
            )

            self.redis_client = redis.Redis(connection_pool=pool)

            # Test connection
            await self.redis_client.ping()

            # Load initial state
            await self._load_exhausted_keys()

            # Start daily reset task
            self._reset_task = asyncio.create_task(self._daily_reset_scheduler())

            logger.info(
                f"Key manager connected to Redis",
                extra={
                    "component": "key_manager",
                    "redis_url": self.redis_url,
                    "rotation_keys": len(self.keys_config.rotation),
                    "direct_keys": len(self.keys_config.direct)
                }
            )

        except Exception as e:
            logger.error(f"Failed to connect key manager to Redis: {e}")
            raise

    async def close(self) -> None:
        """Close Redis connection and stop background tasks."""
        try:
            self._should_stop = True

            if self._reset_task and not self._reset_task.done():
                self._reset_task.cancel()
                try:
                    await self._reset_task
                except asyncio.CancelledError:
                    pass

            if self.redis_client:
                await self.redis_client.aclose()
                self.redis_client = None

            logger.info("Key manager closed")

        except Exception as e:
            logger.error(f"Error closing key manager: {e}")

    async def get_available_key(self, processing_mode: ProcessingMode) -> Optional[APIKeyConfig]:
        """
        Get next available API key based on processing mode.

        Args:
            processing_mode: PROD uses direct keys, TEST uses rotation keys

        Returns:
            Available API key or None if all exhausted
        """
        if not self.redis_client:
            raise RuntimeError("Key manager not connected to Redis")

        if processing_mode == ProcessingMode.PROD:
            return await self._get_direct_key()
        else:  # TEST mode
            return await self._get_rotation_key()

    async def _get_direct_key(self) -> Optional[APIKeyConfig]:
        """Get direct key for production mode (no limits)."""
        if not self.keys_config.direct:
            logger.error("No direct keys available for production mode")
            return None

        # For direct keys, just cycle through them (no usage limits)
        for key_config in self.keys_config.direct:
            if key_config.name not in self._exhausted_keys:
                logger.debug(
                    f"Selected direct key: {key_config.name}",
                    extra={
                        "component": "key_manager",
                        "key_name": key_config.name,
                        "mode": "prod"
                    }
                )
                return key_config

        logger.warning("All direct keys are exhausted")
        return None

    async def _get_rotation_key(self) -> Optional[APIKeyConfig]:
        """Get rotation key for test mode (with daily limits)."""
        if not self.keys_config.rotation:
            logger.error("No rotation keys available for test mode")
            return None

        # Try all rotation keys starting from current index
        keys_tried = 0
        total_keys = len(self.keys_config.rotation)

        while keys_tried < total_keys:
            key_config = self.keys_config.rotation[self._rotation_index]

            # Move to next key for next request
            self._rotation_index = (self._rotation_index + 1) % total_keys
            keys_tried += 1

            # Skip exhausted keys
            if key_config.name in self._exhausted_keys:
                continue

            # Check usage limit
            usage_stats = await self._get_key_usage_stats(key_config.name)
            if usage_stats.requests_remaining > 0:
                logger.debug(
                    f"Selected rotation key: {key_config.name}",
                    extra={
                        "component": "key_manager",
                        "key_name": key_config.name,
                        "mode": "test",
                        "requests_remaining": usage_stats.requests_remaining,
                        "usage_percentage": usage_stats.usage_percentage
                    }
                )
                return key_config
            else:
                # Key reached daily limit
                await self._mark_key_exhausted_internal(key_config.name, "daily_limit_reached")

        logger.warning("All rotation keys are exhausted or at daily limit")
        return None

    async def record_key_usage(self, key_name: str, success: bool) -> None:
        """
        Record API key usage for tracking limits.

        Args:
            key_name: Key identifier
            success: Whether the request was successful (counts either way)
        """
        if not self.redis_client:
            return

        try:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            usage_key = f"{self.key_prefix}{key_name}:{today}"

            # Increment usage counter
            current_usage = await self.redis_client.incr(usage_key)

            # Set expiration for cleanup (2 days to be safe)
            await self.redis_client.expire(usage_key, 172800)

            # Update last used timestamp
            last_used_key = f"{self.key_prefix}{key_name}:last_used"
            await self.redis_client.set(
                last_used_key,
                datetime.now(timezone.utc).isoformat(),
                ex=172800
            )

            logger.debug(
                f"Recorded key usage",
                extra={
                    "component": "key_manager",
                    "key_name": key_name,
                    "success": success,
                    "current_usage": current_usage,
                    "date": today
                }
            )

        except RedisError as e:
            logger.error(f"Failed to record key usage: {e}")

    async def mark_key_exhausted(self, key_name: str) -> None:
        """
        Mark key as exhausted (reached daily limit or rate limited).

        Args:
            key_name: Key identifier to mark as exhausted
        """
        await self._mark_key_exhausted_internal(key_name, "manual_exclusion")

    async def _mark_key_exhausted_internal(self, key_name: str, reason: str) -> None:
        """Internal method to mark key as exhausted with reason."""
        try:
            self._exhausted_keys.add(key_name)

            # Store in Redis for persistence
            exhausted_key = f"{self.key_prefix}exhausted:{key_name}"
            await self.redis_client.set(
                exhausted_key,
                reason,
                ex=86400  # Expire at end of day
            )

            logger.warning(
                f"Key marked as exhausted",
                extra={
                    "component": "key_manager",
                    "key_name": key_name,
                    "reason": reason
                }
            )

        except RedisError as e:
            logger.error(f"Failed to mark key as exhausted: {e}")

    async def get_key_stats(self) -> Dict[str, KeyUsageStats]:
        """
        Get usage statistics for all keys.

        Returns:
            Dictionary mapping key names to usage statistics
        """
        if not self.redis_client:
            return {}

        stats = {}
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # Get stats for all keys (rotation + direct)
        all_keys = self.keys_config.rotation + self.keys_config.direct

        for key_config in all_keys:
            try:
                usage_key = f"{self.key_prefix}{key_config.name}:{today}"
                last_used_key = f"{self.key_prefix}{key_config.name}:last_used"

                # Get current usage
                current_usage = await self.redis_client.get(usage_key)
                requests_today = int(current_usage) if current_usage else 0

                # Get last used timestamp
                last_used_str = await self.redis_client.get(last_used_key)
                last_used = None
                if last_used_str:
                    try:
                        last_used = datetime.fromisoformat(last_used_str.decode())
                    except (ValueError, AttributeError):
                        pass

                # Determine limit (None for direct keys means unlimited)
                limit = key_config.limit_per_day or 999999  # Large number for display

                stats[key_config.name] = KeyUsageStats(
                    key_name=key_config.name,
                    requests_today=requests_today,
                    limit_per_day=limit,
                    last_used=last_used,
                    is_exhausted=key_config.name in self._exhausted_keys
                )

            except Exception as e:
                logger.error(f"Failed to get stats for key {key_config.name}: {e}")
                # Create minimal stats on error
                stats[key_config.name] = KeyUsageStats(
                    key_name=key_config.name,
                    requests_today=0,
                    limit_per_day=key_config.limit_per_day or 999999,
                    is_exhausted=True  # Assume exhausted on error
                )

        return stats

    async def _get_key_usage_stats(self, key_name: str) -> KeyUsageStats:
        """Get usage stats for a specific key."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        usage_key = f"{self.key_prefix}{key_name}:{today}"

        try:
            current_usage = await self.redis_client.get(usage_key)
            requests_today = int(current_usage) if current_usage else 0

            # Find key config to get limit
            key_config = None
            for config in self.keys_config.rotation + self.keys_config.direct:
                if config.name == key_name:
                    key_config = config
                    break

            if not key_config:
                # Unknown key, assume exhausted
                return KeyUsageStats(
                    key_name=key_name,
                    requests_today=999999,
                    limit_per_day=0,
                    is_exhausted=True
                )

            limit = key_config.limit_per_day or 999999

            return KeyUsageStats(
                key_name=key_name,
                requests_today=requests_today,
                limit_per_day=limit,
                is_exhausted=key_name in self._exhausted_keys
            )

        except Exception as e:
            logger.error(f"Failed to get usage stats for {key_name}: {e}")
            return KeyUsageStats(
                key_name=key_name,
                requests_today=999999,
                limit_per_day=0,
                is_exhausted=True
            )

    async def reset_daily_limits(self) -> None:
        """
        Reset daily usage counters (called at UTC midnight).
        """
        if not self.redis_client:
            return

        try:
            # Clear exhausted keys
            self._exhausted_keys.clear()

            # Remove exhausted keys from Redis
            exhausted_pattern = f"{self.key_prefix}exhausted:*"
            exhausted_keys = []

            async for key in self.redis_client.scan_iter(match=exhausted_pattern):
                exhausted_keys.append(key)

            if exhausted_keys:
                await self.redis_client.delete(*exhausted_keys)

            # Reset rotation index
            self._rotation_index = 0

            logger.info(
                f"Daily limits reset completed",
                extra={
                    "component": "key_manager",
                    "reset_time": datetime.now(timezone.utc).isoformat(),
                    "exhausted_keys_cleared": len(exhausted_keys)
                }
            )

        except Exception as e:
            logger.error(f"Failed to reset daily limits: {e}")

    async def _load_exhausted_keys(self) -> None:
        """Load exhausted keys from Redis on startup."""
        if not self.redis_client:
            return

        try:
            exhausted_pattern = f"{self.key_prefix}exhausted:*"
            async for key in self.redis_client.scan_iter(match=exhausted_pattern):
                # Extract key name from Redis key
                key_name = key.decode().replace(f"{self.key_prefix}exhausted:", "")
                self._exhausted_keys.add(key_name)

            if self._exhausted_keys:
                logger.info(
                    f"Loaded {len(self._exhausted_keys)} exhausted keys from Redis",
                    extra={
                        "component": "key_manager",
                        "exhausted_keys": list(self._exhausted_keys)
                    }
                )

        except Exception as e:
            logger.error(f"Failed to load exhausted keys: {e}")

    async def _daily_reset_scheduler(self) -> None:
        """Background task that schedules daily resets at UTC midnight."""
        while not self._should_stop:
            try:
                # Calculate time until next UTC midnight
                now = datetime.now(timezone.utc)
                next_midnight = (now + timedelta(days=1)).replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
                sleep_seconds = (next_midnight - now).total_seconds()

                logger.info(
                    f"Scheduled daily reset in {sleep_seconds:.0f} seconds",
                    extra={
                        "component": "key_manager",
                        "next_reset": next_midnight.isoformat()
                    }
                )

                # Sleep until midnight
                await asyncio.sleep(sleep_seconds)

                if not self._should_stop:
                    await self.reset_daily_limits()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in daily reset scheduler: {e}")
                # Sleep for 1 hour on error before retrying
                await asyncio.sleep(3600)

        logger.info("Daily reset scheduler stopped")


def create_key_manager(
        redis_url: str,
        key_prefix: str,
        rotation_keys: List[APIKeyConfig],
        direct_keys: List[APIKeyConfig]
) -> RedisKeyManager:
    """
    Create and configure key manager.

    Args:
        redis_url: Redis connection URL
        key_prefix: Prefix for Redis keys
        rotation_keys: Keys for test mode with limits
        direct_keys: Keys for prod mode without limits

    Returns:
        Configured key manager
    """
    keys_config = LoadedKeys(
        rotation=rotation_keys,
        direct=direct_keys
    )

    return RedisKeyManager(
        redis_url=redis_url,
        key_prefix=key_prefix,
        keys_config=keys_config
    )