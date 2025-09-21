"""
Logging configuration for message-preprocessor service.
Provides structured JSON logging with correlation IDs and metrics.
"""

import logging
import logging.config
import sys
import json
import time
from datetime import datetime
from typing import Any, Dict, Optional, List


class JSONFormatter(logging.Formatter):
    """
    Custom JSON formatter for structured logging.
    Formats log records as JSON with consistent fields.
    """

    def __init__(
            self,
            service_name: str = "message-preprocessor",
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
    Useful for tracing messages across the system.
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
        return f"mp-{int(time.time() * 1000)}"


def setup_logging(
        level: str = "INFO",
        service_name: str = "message-preprocessor",
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
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("redis").setLevel(logging.WARNING)

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

    def log_message_processed(
            self,
            idempotency_key: str,
            classification: str,
            processing_time_ms: float,
            model_used: str,
            provider: str,
            retry_count: int = 0,
            success: bool = True,
            error: Optional[str] = None
    ) -> None:
        """
        Log message processing metrics.

        Args:
            idempotency_key: Message idempotency key
            classification: Message classification (ORDER/OFFER/OTHER)
            processing_time_ms: Processing time in milliseconds
            model_used: LLM model used
            provider: LLM provider used
            retry_count: Number of retries
            success: Whether processing succeeded
            error: Error message if failed
        """
        self.logger.info(
            "Message processed",
            extra={
                "metric_type": "message_processed",
                "idempotency_key": idempotency_key,
                "classification": classification,
                "processing_time_ms": round(processing_time_ms, 2),
                "model_used": model_used,
                "provider": provider,
                "retry_count": retry_count,
                "success": success,
                "error": error
            }
        )

    def log_llm_request(
            self,
            model_name: str,
            provider: str,
            duration_ms: float,
            success: bool,
            retry_count: int = 0,
            error_type: Optional[str] = None,
            key_name: Optional[str] = None
    ) -> None:
        """
        Log LLM request metrics.

        Args:
            model_name: Model name
            provider: Provider name
            duration_ms: Request duration in milliseconds
            success: Whether request succeeded
            retry_count: Number of retries attempted
            error_type: Error type if failed
            key_name: API key name used (for tracking)
        """
        self.logger.info(
            f"LLM request: {model_name}",
            extra={
                "metric_type": "llm_request",
                "model_name": model_name,
                "provider": provider,
                "duration_ms": round(duration_ms, 2),
                "success": success,
                "retry_count": retry_count,
                "error_type": error_type,
                "key_name": key_name
            }
        )

    def log_key_usage(
            self,
            key_name: str,
            requests_today: int,
            limit_per_day: int,
            usage_percentage: float,
            is_exhausted: bool
    ) -> None:
        """
        Log API key usage metrics.

        Args:
            key_name: Key identifier
            requests_today: Requests made today
            limit_per_day: Daily limit
            usage_percentage: Usage percentage
            is_exhausted: Whether key is exhausted
        """
        self.logger.info(
            f"Key usage: {key_name}",
            extra={
                "metric_type": "key_usage",
                "key_name": key_name,
                "requests_today": requests_today,
                "limit_per_day": limit_per_day,
                "usage_percentage": round(usage_percentage, 1),
                "is_exhausted": is_exhausted
            }
        )

    def log_parsing_result(
            self,
            idempotency_key: str,
            classification: str,
            parsing_success: bool,
            validation_success: bool,
            schema_version: str,
            validation_errors: List[str] = None
    ) -> None:
        """
        Log message parsing and validation results.

        Args:
            idempotency_key: Message idempotency key
            classification: Message classification
            parsing_success: Whether parsing succeeded
            validation_success: Whether validation succeeded
            schema_version: Schema version used
            validation_errors: List of validation errors
        """
        self.logger.info(
            "Message parsing result",
            extra={
                "metric_type": "parsing_result",
                "idempotency_key": idempotency_key,
                "classification": classification,
                "parsing_success": parsing_success,
                "validation_success": validation_success,
                "schema_version": schema_version,
                "validation_errors": validation_errors or []
            }
        )

    def log_service_stats(
            self,
            messages_processed: int,
            classifications: Dict[str, int],
            messages_failed: int,
            avg_processing_time_ms: float,
            models_used: Dict[str, int],
            key_stats: Dict[str, Any]
    ) -> None:
        """
        Log service-level statistics.

        Args:
            messages_processed: Total messages processed
            classifications: Messages by classification type
            messages_failed: Messages that failed processing
            avg_processing_time_ms: Average processing time
            models_used: Usage count by model
            key_stats: Key usage statistics
        """
        self.logger.info(
            "Service statistics",
            extra={
                "metric_type": "service_stats",
                "messages_processed": messages_processed,
                "classifications": classifications,
                "messages_failed": messages_failed,
                "avg_processing_time_ms": round(avg_processing_time_ms, 2),
                "models_used": models_used,
                "key_stats": key_stats
            }
        )

    def log_retry_attempt(
            self,
            idempotency_key: str,
            attempt_number: int,
            reason: str,
            delay_seconds: Optional[int] = None
    ) -> None:
        """
        Log retry attempt information.

        Args:
            idempotency_key: Message being retried
            attempt_number: Current attempt number
            reason: Reason for retry
            delay_seconds: Delay before retry if applicable
        """
        self.logger.info(
            "Retry attempt",
            extra={
                "metric_type": "retry_attempt",
                "idempotency_key": idempotency_key,
                "attempt_number": attempt_number,
                "reason": reason,
                "delay_seconds": delay_seconds
            }
        )