"""
HTTP publisher implementation for sending events to queue-writer service.
Handles retries, authentication, and error handling.
"""

import asyncio
import logging
from typing import Optional

import httpx
from httpx import HTTPStatusError, RequestError

from domain.ports import Publisher, PublishError
from domain.dto import TelegramEventDTO, PublishResult


logger = logging.getLogger(__name__)


class HTTPPublisher(Publisher):
    """
    HTTP-based publisher that sends events to queue-writer service.
    Implements retry logic with exponential backoff.
    """
    
    def __init__(
        self,
        endpoint: str,
        shared_token: str,
        timeout_ms: int = 2000,
        max_retries: int = 5,
        retry_backoff_ms: int = 500,
        max_backoff_ms: int = 10000
    ):
        """
        Initialize HTTP publisher.
        
        Args:
            endpoint: Queue-writer endpoint URL (e.g., "http://queue-writer:8080/events")
            shared_token: Shared authentication token
            timeout_ms: Request timeout in milliseconds
            max_retries: Maximum number of retry attempts
            retry_backoff_ms: Initial backoff time between retries in milliseconds
            max_backoff_ms: Maximum backoff time in milliseconds
        """
        self.endpoint = endpoint
        self.shared_token = shared_token
        self.timeout = timeout_ms / 1000.0  # Convert to seconds
        self.max_retries = max_retries
        self.retry_backoff_ms = retry_backoff_ms
        self.max_backoff_ms = max_backoff_ms
        
        # Create HTTP client with custom configuration
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.timeout),
            headers={
                "Authorization": f"Bearer {shared_token}",
                "Content-Type": "application/json",
                "User-Agent": "tg-ingestor/1.0"
            },
            follow_redirects=True,
            limits=httpx.Limits(
                max_keepalive_connections=5,
                max_connections=10
            )
        )
    
    async def publish_event(self, event: TelegramEventDTO) -> PublishResult:
        """
        Publish event to queue-writer with retry logic.
        
        Args:
            event: The telegram event to publish
            
        Returns:
            PublishResult with success status and details
            
        Raises:
            PublishError: If publishing fails after all retries
        """
        retry_count = 0
        last_error: Optional[Exception] = None
        
        for attempt in range(self.max_retries + 1):
            try:
                # Attempt to publish
                result = await self._send_request(event)
                
                if result.success:
                    # Log successful publication
                    logger.info(
                        f"Event published successfully",
                        extra={
                            "component": "http_publisher",
                            "chat_id": event.chat.id,
                            "message_id": event.message.id,
                            "stream_id": result.stream_id,
                            "is_duplicate": result.is_duplicate,
                            "retry_count": retry_count
                        }
                    )
                    result.retry_count = retry_count
                    return result
                
                # If not successful, treat as error for retry logic
                last_error = Exception(result.error or "Unknown error")
                
            except Exception as e:
                last_error = e
                logger.warning(
                    f"Publish attempt {attempt + 1} failed: {e}",
                    extra={
                        "component": "http_publisher",
                        "chat_id": event.chat.id,
                        "message_id": event.message.id,
                        "attempt": attempt + 1,
                        "error": str(e)
                    }
                )
            
            # Don't retry on final attempt
            if attempt == self.max_retries:
                break
            
            # Exponential backoff with jitter
            backoff_ms = min(
                self.retry_backoff_ms * (2 ** attempt),
                self.max_backoff_ms
            )
            
            logger.debug(
                f"Retrying in {backoff_ms}ms",
                extra={
                    "component": "http_publisher",
                    "chat_id": event.chat.id,
                    "message_id": event.message.id,
                    "backoff_ms": backoff_ms
                }
            )
            
            await asyncio.sleep(backoff_ms / 1000.0)
            retry_count += 1
        
        # All retries exhausted
        error_msg = f"Failed to publish after {retry_count} retries: {last_error}"
        logger.error(
            error_msg,
            extra={
                "component": "http_publisher",
                "chat_id": event.chat.id,
                "message_id": event.message.id,
                "retry_count": retry_count,
                "final_error": str(last_error)
            }
        )
        
        raise PublishError(error_msg) from last_error
    
    async def _send_request(self, event: TelegramEventDTO) -> PublishResult:
        """
        Send HTTP request to queue-writer.
        
        Args:
            event: Event to send
            
        Returns:
            PublishResult with response details
            
        Raises:
            HTTPStatusError: For HTTP errors
            RequestError: For connection/timeout errors
        """
        try:
            # Send POST request
            event_data = event.model_dump(mode='json')
            
            # Альтернативный способ - использовать model_dump_json() и затем json.loads()
            # import json
            # event_json_str = event.model_dump_json()
            # event_data = json.loads(event_json_str)
            
            logger.debug(
                f"Sending event data",
                extra={
                    "component": "http_publisher",
                    "chat_id": event.chat.id,
                    "message_id": event.message.id,
                    "event_ts": event_data.get("event_ts"),  # Теперь это строка ISO
                    "data_keys": list(event_data.keys())
                }
            )
            
            # Send POST request
            response = await self.client.post(
                self.endpoint,
                json=event_data,  # Теперь все datetime объекты сериализованы в строки
                headers={
                    "X-Idempotency-Key": event.idempotency_key,
                    "X-Event-Type": event.event_type
                }
            )
            
            # Check for HTTP errors
            response.raise_for_status()
            
            # Parse response
            response_data = response.json()
            
            return PublishResult(
                success=True,
                stream_id=response_data.get("stream_id"),
                is_duplicate=response_data.get("is_duplicate", False)
            )
            
        except HTTPStatusError as e:
            # Handle specific HTTP status codes
            status_code = e.response.status_code
            
            if status_code == 401:
                error_msg = "Authentication failed - invalid token"
            elif status_code == 422:
                error_msg = f"Validation error: {e.response.text}"
            elif status_code == 429:
                error_msg = "Rate limited by queue-writer"
            elif status_code >= 500:
                error_msg = f"Queue-writer server error: {status_code}"
            else:
                error_msg = f"HTTP error {status_code}: {e.response.text}"
            
            return PublishResult(
                success=False,
                error=error_msg
            )
            
        except RequestError as e:
            # Handle connection/timeout errors
            if "timeout" in str(e).lower():
                error_msg = f"Request timeout: {e}"
            elif "connection" in str(e).lower():
                error_msg = f"Connection error: {e}"
            else:
                error_msg = f"Request error: {e}"
            
            return PublishResult(
                success=False,
                error=error_msg
            )
    
    async def check_health(self) -> bool:
        """
        Check if queue-writer is healthy by calling healthz endpoint.
        
        Returns:
            True if healthy, False otherwise
        """
        try:
            # Derive health endpoint from events endpoint
            health_url = self.endpoint.replace("/events", "/healthz")
            
            response = await self.client.get(
                health_url,
                timeout=5.0  # Shorter timeout for health checks
            )
            
            return response.status_code == 200
            
        except Exception as e:
            logger.warning(
                f"Health check failed: {e}",
                extra={
                    "component": "http_publisher",
                    "error": str(e)
                }
            )
            return False
    
    async def close(self) -> None:
        """Close HTTP client and cleanup resources."""
        await self.client.aclose()
        logger.debug("HTTP publisher closed")


class TokenLoader:
    """
    Utility class for loading authentication tokens from files.
    """
    
    @staticmethod
    def load_token_from_file(file_path: str) -> str:
        """
        Load authentication token from file.
        
        Args:
            file_path: Path to token file
            
        Returns:
            Token string
            
        Raises:
            FileNotFoundError: If token file not found
            ValueError: If token is empty or invalid
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                token = f.read().strip()
            
            if not token:
                raise ValueError(f"Token file is empty: {file_path}")
            
            if len(token) < 16:
                logger.warning("Token is shorter than 16 characters")
            
            logger.info(f"Loaded token from {file_path}")
            return token
            
        except FileNotFoundError:
            logger.error(f"Token file not found: {file_path}")
            raise
        except Exception as e:
            logger.error(f"Failed to load token: {e}")
            raise ValueError(f"Failed to load token from {file_path}: {e}") from e