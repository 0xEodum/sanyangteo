# queue-writer/__init__.py
"""
Queue Writer Service

HTTP API service for writing Telegram events to Redis Streams
with idempotency support and structured logging.
"""

__version__ = "1.0.0"
__author__ = "YuDev"
__description__ = "Queue Writer Service for Telegram Event Processing"

# queue-writer/domain/__init__.py
"""
Domain layer for queue-writer service.

Contains business logic, data models, and interfaces.
Following Domain-Driven Design principles.
"""

from .schema import TelegramEvent, QueueWriteResult, HealthStatus
from .ports import QueueWriter, TokenValidator, EventProcessor

__all__ = [
    "TelegramEvent",
    "QueueWriteResult", 
    "HealthStatus",
    "QueueWriter",
    "TokenValidator",
    "EventProcessor"
]

# queue-writer/infra/__init__.py
"""
Infrastructure layer for queue-writer service.

Contains implementations of domain interfaces using external systems
like Redis, databases, message queues, etc.
"""

from .redis_client import RedisClient
from .redis_writer import RedisStreamWriter

__all__ = [
    "RedisClient",
    "RedisStreamWriter"
]

# queue-writer/api/__init__.py
"""
API layer for queue-writer service.

Contains HTTP endpoints, authentication, and external interfaces.
Adapts external requests to internal domain models.
"""

from .http_server import QueueWriterAPI
from .auth import SharedTokenValidator, AuthDependency

__all__ = [
    "QueueWriterAPI",
    "SharedTokenValidator", 
    "AuthDependency"
]

# queue-writer/telemetry/__init__.py
"""
Telemetry and observability for queue-writer service.

Contains logging, metrics, and monitoring utilities.
"""

from .logger import setup_logging, JSONFormatter, MetricsLogger

__all__ = [
    "setup_logging",
    "JSONFormatter",
    "MetricsLogger"
]