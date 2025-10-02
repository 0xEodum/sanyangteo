"""
LLM client adapter using OpenAI library for API communication.
Supports multiple providers (OpenAI, Groq, Anthropic, etc.) through OpenAI-compatible APIs.
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, Optional

import openai
from openai import AsyncOpenAI
from openai.types.chat import ChatCompletion

from domain.ports import LLMClient, LLMClientError
from domain.dto import LLMRequest, LLMResponse

logger = logging.getLogger(__name__)


class OpenAICompatibleClient(LLMClient):
    """
    LLM client using OpenAI library for multiple providers.

    Supports:
    - OpenAI (api.openai.com)
    - Groq (api.groq.com)
    - Any OpenAI-compatible API
    """

    def __init__(self, timeout_seconds: int = 30):
        """
        Initialize LLM client.

        Args:
            timeout_seconds: Request timeout in seconds
        """
        self.timeout_seconds = timeout_seconds

        # Client instances cache by base_url to avoid recreation
        self._client_cache: Dict[str, AsyncOpenAI] = {}

    async def send_request(
        self,
        request: LLMRequest,
        api_key: str,
        api_url: str,
        provider: str,
        model_config: Optional['ModelConfig'] = None  # Добавляем model_config
    ) -> LLMResponse:
        """
        Send request to LLM API using OpenAI library.

        Args:
            request: LLM request with messages and parameters
            api_key: API key for authentication
            api_url: API endpoint URL
            provider: Provider name for logging
            model_config: Model configuration with extra parameters

        Returns:
            LLM response with content or error
        """
        start_time = datetime.utcnow()

        try:
            # Get or create client for this API URL
            client = self._get_client(api_url, api_key)

            # Prepare base request parameters
            request_params = self._prepare_request_params(request, provider)
            
            # Add model-specific extra parameters if provided
            if model_config:
                extra_params = model_config.get_request_extras()
                request_params.update(extra_params)

            logger.debug(
                f"Sending LLM request to {provider}",
                extra={
                    "component": "llm_client",
                    "provider": provider,
                    "model": request.model_name,
                    "api_url": api_url,
                    "message_count": len(request.messages),
                    "max_tokens": request.max_tokens,
                    "has_extra_params": bool(model_config and model_config.get_request_extras())
                }
            )

            logger.debug(
                "LLM request payload",
                extra={
                    "component": "llm_client",
                    "provider": provider,
                    "model": request.model_name,
                    "messages": request_params.get("messages")
                }
            )

            # Make the API call with all parameters
            completion = await client.chat.completions.create(
                timeout=self.timeout_seconds,
                **request_params
            )

            # Extract response content
            content = self._extract_content(completion)

            processing_time_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)

            logger.info(
                f"LLM request successful",
                extra={
                    "component": "llm_client",
                    "provider": provider,
                    "model": request.model_name,
                    "processing_time_ms": processing_time_ms,
                    "response_length": len(content) if content else 0
                }
            )

            return LLMResponse(
                success=True,
                content=content,
                error=None,
                model_used=request.model_name,
                provider=provider,
                processing_time_ms=processing_time_ms
            )

        except openai.RateLimitError as e:
            # Rate limit - should exclude key from rotation
            processing_time_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            error_msg = f"Rate limit exceeded: {str(e)}"

            logger.warning(
                f"LLM rate limit hit",
                extra={
                    "component": "llm_client",
                    "provider": provider,
                    "model": request.model_name,
                    "error": error_msg,
                    "processing_time_ms": processing_time_ms
                }
            )

            return LLMResponse(
                success=False,
                content=None,
                error=f"RATE_LIMIT: {error_msg}",  # Special prefix for key exclusion
                model_used=request.model_name,
                provider=provider,
                processing_time_ms=processing_time_ms
            )

        except openai.AuthenticationError as e:
            # Authentication error - likely bad key
            processing_time_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            error_msg = f"Authentication failed: {str(e)}"

            logger.error(
                f"LLM authentication error",
                extra={
                    "component": "llm_client",
                    "provider": provider,
                    "model": request.model_name,
                    "error": error_msg,
                    "processing_time_ms": processing_time_ms
                }
            )

            return LLMResponse(
                success=False,
                content=None,
                error=f"AUTH_ERROR: {error_msg}",  # Special prefix for key issues
                model_used=request.model_name,
                provider=provider,
                processing_time_ms=processing_time_ms
            )

        except openai.APITimeoutError as e:
            # Timeout - can retry
            processing_time_ms = self.timeout_seconds * 1000  # Approximate
            error_msg = f"Request timeout: {str(e)}"

            logger.warning(
                f"LLM request timeout",
                extra={
                    "component": "llm_client",
                    "provider": provider,
                    "model": request.model_name,
                    "timeout_seconds": self.timeout_seconds,
                    "processing_time_ms": processing_time_ms
                }
            )

            return LLMResponse(
                success=False,
                content=None,
                error=f"TIMEOUT: {error_msg}",
                model_used=request.model_name,
                provider=provider,
                processing_time_ms=processing_time_ms
            )

        except openai.APIError as e:
            # General API error
            processing_time_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            error_msg = f"API error: {str(e)}"

            logger.error(
                f"LLM API error",
                extra={
                    "component": "llm_client",
                    "provider": provider,
                    "model": request.model_name,
                    "error": error_msg,
                    "processing_time_ms": processing_time_ms
                }
            )

            return LLMResponse(
                success=False,
                content=None,
                error=f"API_ERROR: {error_msg}",
                model_used=request.model_name,
                provider=provider,
                processing_time_ms=processing_time_ms
            )

        except Exception as e:
            # Unexpected error
            processing_time_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            error_msg = f"Unexpected error: {str(e)}"

            logger.error(
                f"LLM unexpected error",
                extra={
                    "component": "llm_client",
                    "provider": provider,
                    "model": request.model_name,
                    "error": error_msg,
                    "processing_time_ms": processing_time_ms
                }
            )

            return LLMResponse(
                success=False,
                content=None,
                error=f"UNEXPECTED: {error_msg}",
                model_used=request.model_name,
                provider=provider,
                processing_time_ms=processing_time_ms
            )

    def _get_client(self, api_url: str, api_key: str) -> AsyncOpenAI:
        """Get or create OpenAI client for the given URL."""
        # Use URL as cache key (API key changes but URL is stable)
        if api_url not in self._client_cache:
            
            # Теперь api_url уже является base_url (без /chat/completions)
            # Специальная обработка только для стандартного OpenAI API
            if api_url == "https://api.openai.com/v1":
                base_url = None  # Use default OpenAI base URL
            else:
                # Для всех остальных (включая proxy API) используем URL как base_url
                base_url = api_url

            self._client_cache[api_url] = AsyncOpenAI(
                api_key=api_key,
                base_url=base_url,
                timeout=self.timeout_seconds
            )

            logger.debug(
                f"Created new OpenAI client",
                extra={
                    "component": "llm_client",
                    "api_url": api_url,
                    "base_url": base_url,
                    "is_default_openai": base_url is None
                }
            )
        else:
            # Update API key for existing client (in case of key rotation)
            self._client_cache[api_url].api_key = api_key

        return self._client_cache[api_url]

    def _prepare_request_params(self, request: LLMRequest, provider: str) -> Dict[str, Any]:
        """Prepare request parameters based on provider specifics."""
        params = {
            "model": request.model_name,
            "messages": request.messages,
            "max_tokens": request.max_tokens,
            "temperature": request.temperature,
        }

        # Provider-specific adjustments
        if provider == "anthropic":
            # Anthropic through OpenAI-compatible proxy
            # May need special handling depending on proxy
            pass
        elif provider == "groq":
            # Groq-specific optimizations
            params["stream"] = False  # Ensure no streaming
        elif provider == "openai":
            # OpenAI standard parameters
            pass

        return params

    def _extract_content(self, completion: ChatCompletion) -> Optional[str]:
        """Extract content from API response."""
        try:
            if completion.choices and len(completion.choices) > 0:
                choice = completion.choices[0]
                if choice.message and choice.message.content:
                    return choice.message.content.strip()

            logger.warning("No content in LLM response")
            return None

        except Exception as e:
            logger.error(f"Failed to extract content from response: {e}")
            return None

    async def check_health(self) -> bool:
        """
        Check if LLM client is healthy.

        Returns:
            True if healthy (can create clients), False otherwise
        """
        try:
            # Simple health check - verify we can create a client
            test_client = AsyncOpenAI(
                api_key="test-key-for-health-check",
                timeout=5
            )
            return test_client is not None

        except Exception as e:
            logger.warning(f"LLM client health check failed: {e}")
            return False

    async def close(self) -> None:
        """Close all client connections."""
        try:
            for client in self._client_cache.values():
                await client.close()

            self._client_cache.clear()

            logger.info("LLM client connections closed")

        except Exception as e:
            logger.error(f"Error closing LLM client: {e}")


def create_llm_request(
        model_name: str,
        system_prompt: str,
        user_message: str,
        max_tokens: int = 1000,
        temperature: float = 0.1
) -> LLMRequest:
    """
    Create LLM request with system prompt and user message.

    Args:
        model_name: Model to use
        system_prompt: System prompt for classification
        user_message: User message to classify
        max_tokens: Maximum tokens in response
        temperature: Temperature for generation

    Returns:
        LLM request ready to send
    """
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_message}
    ]

    return LLMRequest(
        model_name=model_name,
        messages=messages,
        max_tokens=max_tokens,
        temperature=temperature
    )
