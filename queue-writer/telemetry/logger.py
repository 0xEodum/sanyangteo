"""
Logging configuration for queue-writer service.
Provides structured JSON logging with correlation IDs and metrics.
"""

import logging
import logging.config
import sys
import json
import time
from datetime import datetime
from typing import Any, Dict, Optional


class JSONFormatter(logging.Formatter):
    """
    Custom JSON formatter for structured logging.
    Formats log records as JSON with consistent fields.
    """
    
    def __init__(
        self,
        service_name: str = "queue-writer",
        include_extra: bool = True
    ):
        """
        Initialize JSON formatter.
        
        Args:
            service_name: Name of the service for log identification
            include_extra: Whether to include extra fields from log record
        """
        super().__init__()
        self.service_name = service_name
        self.include_extra = include_extra
    
    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record as JSON.
        
        Args:
            record: Log record to format
            
        Returns:
            JSON-formatted log string
        """
        # Base log entry
        log_entry = {
            "timestamp": datetime.utcfromtimestamp(record.created).isoformat() + "Z",
            "level": record.levelname,
            "service": self.service_name,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }
        
        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        
        # Add extra fields if enabled
        if self.include_extra and hasattr(record, '__dict__'):
            # Include extra fields, excluding standard log record attributes
            standard_fields = {
                'name', 'msg', 'args', 'levelname', 'levelno', 'pathname',
                'filename', 'module', 'exc_info', 'exc_text', 'stack_info',
                'lineno', 'funcName', 'created', 'msecs', 'relativeCreated',
                'thread', 'threadName', 'processName', 'process', 'getMessage'
            }
            
            for key, value in record.__dict__.items():
                if key not in standard_fields and not key.startswith('_'):
                    log_entry[key] = value
        
        return json.dumps(log_entry, default=self._json_default)
    
    @staticmethod
    def _json_default(obj: Any) -> str:
        """
        JSON serializer for objects not serializable by default.
        
        Args:
            obj: Object to serialize
            
        Returns:
            String representation of object
        """
        if isinstance(obj, datetime):
            return obj.isoformat()
        return str(obj)


class CorrelationFilter(logging.Filter):
    """
    Logging filter that adds correlation ID to log records.
    Useful for tracing requests across the system.
    """
    
    def __init__(self, correlation_id: Optional[str] = None):
        """
        Initialize correlation filter.
        
        Args:
            correlation_id: Static correlation ID, or None for dynamic
        """
        super().__init__()
        self.correlation_id = correlation_id
    
    def filter(self, record: logging.LogRecord) -> bool:
        """
        Add correlation ID to log record.
        
        Args:
            record: Log record to modify
            
        Returns:
            Always True (don't filter out)
        """
        # Use provided correlation ID or generate one
        if not hasattr(record, 'correlation_id'):
            record.correlation_id = self.correlation_id or self._generate_correlation_id()
        
        return True
    
    @staticmethod
    def _generate_correlation_id() -> str:
        """
        Generate a simple correlation ID based on timestamp.
        
        Returns:
            Correlation ID string
        """
        return f"qw-{int(time.time() * 1000)}"


def setup_logging(
    level: str = "INFO",
    service_name: str = "queue-writer",
    enable_json: bool = True,
    enable_correlation: bool = True
) -> None:
    """
    Setup logging configuration for the service.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
        service_name: Service name for log identification
        enable_json: Whether to use JSON formatting
        enable_correlation: Whether to add correlation IDs
    """
    
    # Validate log level
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {level}")
    
    # Choose formatter
    if enable_json:
        formatter = JSONFormatter(service_name=service_name)
    else:
        formatter = logging.Formatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    # Setup handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    
    # Add correlation filter if enabled
    if enable_correlation:
        handler.addFilter(CorrelationFilter())
    
    # Configure root logger
    logging.root.setLevel(numeric_level)
    logging.root.handlers.clear()
    logging.root.addHandler(handler)
    
    # Set levels for specific loggers
    logging.getLogger("uvicorn").setLevel(logging.INFO)
    logging.getLogger("uvicorn.access").setLevel(logging.INFO)
    logging.getLogger("redis").setLevel(logging.WARNING)
    logging.getLogger("fastapi").setLevel(logging.INFO)
    
    # Log setup completion
    logger = logging.getLogger(__name__)
    logger.info(
        f"Logging configured",
        extra={
            "component": "logger",
            "level": level,
            "json_enabled": enable_json,
            "correlation_enabled": enable_correlation
        }
    )


class MetricsLogger:
    """
    Helper class for logging metrics and performance data.
    """
    
    def __init__(self, logger_name: str = "metrics"):
        """
        Initialize metrics logger.
        
        Args:
            logger_name: Name of the logger to use
        """
        self.logger = logging.getLogger(logger_name)
    
    def log_event_processed(
        self,
        chat_id: int,
        message_id: int,
        stream_id: str,
        processing_time_ms: float,
        is_duplicate: bool = False
    ) -> None:
        """
        Log event processing metrics.
        
        Args:
            chat_id: Telegram chat ID
            message_id: Telegram message ID
            stream_id: Redis stream ID
            processing_time_ms: Processing time in milliseconds
            is_duplicate: Whether event was duplicate
        """
        self.logger.info(
            "Event processed",
            extra={
                "metric_type": "event_processed",
                "chat_id": chat_id,
                "message_id": message_id,
                "stream_id": stream_id,
                "processing_time_ms": round(processing_time_ms, 2),
                "is_duplicate": is_duplicate
            }
        )
    
    def log_redis_operation(
        self,
        operation: str,
        duration_ms: float,
        success: bool,
        error: Optional[str] = None
    ) -> None:
        """
        Log Redis operation metrics.
        
        Args:
            operation: Operation name (xadd, sadd, etc.)
            duration_ms: Operation duration in milliseconds
            success: Whether operation succeeded
            error: Error message if failed
        """
        self.logger.info(
            f"Redis operation: {operation}",
            extra={
                "metric_type": "redis_operation",
                "operation": operation,
                "duration_ms": round(duration_ms, 2),
                "success": success,
                "error": error
            }
        )
    
    def log_http_request(
        self,
        method: str,
        path: str,
        status_code: int,
        duration_ms: float,
        client_ip: Optional[str] = None
    ) -> None:
        """
        Log HTTP request metrics.
        
        Args:
            method: HTTP method
            path: Request path
            status_code: Response status code
            duration_ms: Request duration in milliseconds
            client_ip: Client IP address
        """
        self.logger.info(
            f"HTTP request: {method} {path}",
            extra={
                "metric_type": "http_request",
                "method": method,
                "path": path,
                "status_code": status_code,
                "duration_ms": round(duration_ms, 2),
                "client_ip": client_ip
            }
        )