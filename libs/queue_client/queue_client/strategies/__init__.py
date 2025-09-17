"""
Strategies for different queue implementations
"""

from .redis_strategy import RedisQueueWriter, RedisQueueReader

__all__ = [
    "RedisQueueWriter",
    "RedisQueueReader"
]