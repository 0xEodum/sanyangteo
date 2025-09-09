"""
HTTP server for queue-writer service using FastAPI.
Provides REST API for receiving events from tg-ingestor.
"""

import logging
import time
from typing import Optional

from fastapi import FastAPI, HTTPException, Header, Depends, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from domain.ports import QueueWriter, TokenValidator, AuthenticationError
from domain.schema import TelegramEvent, QueueWriteResult, HealthStatus
from api.auth import AuthDependency


logger = logging.getLogger(__name__)


class QueueWriterAPI:
    """
    FastAPI application for queue-writer service.
    Handles event ingestion with authentication and health checks.
    """
    
    def __init__(
        self,
        queue_writer: QueueWriter,
        token_validator: TokenValidator,
        title: str = "Queue Writer Service",
        version: str = "1.0.0"
    ):
        """
        Initialize FastAPI application.
        
        Args:
            queue_writer: Queue writer implementation
            token_validator: Token validator implementation
            title: API title
            version: API version
        """
        self.queue_writer = queue_writer
        self.token_validator = token_validator
        
        # Create FastAPI app
        self.app = FastAPI(
            title=title,
            version=version,
            description="HTTP API for writing Telegram events to Redis Streams",
            docs_url="/docs",
            redoc_url="/redoc"
        )
        
        # Setup middleware
        self._setup_middleware()
        
        # Setup dependencies
        self.auth_dependency = AuthDependency(token_validator)
        
        # Setup routes
        self._setup_routes()
        
        # Setup exception handlers
        self._setup_exception_handlers()
    
    def _setup_middleware(self) -> None:
        """Setup FastAPI middleware."""
        
        # CORS middleware (restrictive for internal service)
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["http://tg-ingestor:*"],  # Only allow ingestor service
            allow_credentials=False,
            allow_methods=["POST", "GET"],
            allow_headers=["Content-Type", "Authorization"]
        )
        
        # Request logging middleware
        @self.app.middleware("http")
        async def log_requests(request: Request, call_next):
            start_time = time.time()
            
            # Log request
            logger.info(
                f"Request started: {request.method} {request.url.path}",
                extra={
                    "component": "http_server",
                    "method": request.method,
                    "path": request.url.path,
                    "client_ip": request.client.host if request.client else "unknown"
                }
            )
            
            # Process request
            response = await call_next(request)
            
            # Log response
            elapsed_ms = (time.time() - start_time) * 1000
            logger.info(
                f"Request completed: {response.status_code}",
                extra={
                    "component": "http_server",
                    "method": request.method,
                    "path": request.url.path,
                    "status_code": response.status_code,
                    "elapsed_ms": round(elapsed_ms, 2)
                }
            )
            
            return response
    
    def _setup_routes(self) -> None:
        """Setup FastAPI routes."""
        
        @self.app.post(
            "/events",
            response_model=QueueWriteResult,
            status_code=202,
            summary="Submit Telegram Event",
            description="Submit a new Telegram event to be written to the queue"
        )
        async def submit_event(
            event: TelegramEvent,
            _: bool = Depends(self.auth_dependency)
        ) -> QueueWriteResult:
            """
            Submit a Telegram event for processing.
            
            Requires valid Bearer token in Authorization header.
            Returns 202 Accepted with stream ID.
            """
            try:
                result = await self.queue_writer.write_event(event)
                
                # Log successful processing
                logger.info(
                    f"Event processed successfully",
                    extra={
                        "component": "http_server",
                        "chat_id": event.chat.id,
                        "message_id": event.message.id,
                        "stream_id": result.stream_id,
                        "is_duplicate": result.is_duplicate
                    }
                )
                
                return result
                
            except Exception as e:
                logger.error(
                    f"Failed to process event: {e}",
                    extra={
                        "component": "http_server",
                        "chat_id": event.chat.id,
                        "message_id": event.message.id,
                        "error": str(e)
                    }
                )
                raise HTTPException(
                    status_code=503,
                    detail=f"Failed to process event: {str(e)}"
                )
        
        @self.app.get(
            "/healthz",
            response_model=dict,
            summary="Health Check",
            description="Basic health check endpoint"
        )
        async def health_check() -> dict:
            """Basic health check - always returns OK if service is running."""
            return {
                "status": "ok",
                "service": "queue-writer",
                "timestamp": time.time()
            }
        
        @self.app.get(
            "/readyz",
            response_model=HealthStatus,
            summary="Readiness Check",
            description="Detailed readiness check including dependencies"
        )
        async def readiness_check() -> HealthStatus:
            """
            Detailed readiness check.
            
            Checks Redis connectivity and queue health.
            Returns 503 if not ready.
            """
            try:
                health_status = await self.queue_writer.check_health()
                
                if health_status.status != "healthy":
                    logger.warning(
                        f"Service not ready: {health_status.status}",
                        extra={
                            "component": "http_server",
                            "checks": health_status.checks
                        }
                    )
                    raise HTTPException(
                        status_code=503,
                        detail=f"Service not ready: {health_status.status}"
                    )
                
                return health_status
                
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Readiness check failed: {e}")
                raise HTTPException(
                    status_code=503,
                    detail=f"Readiness check failed: {str(e)}"
                )
        
        @self.app.get(
            "/metrics",
            summary="Metrics",
            description="Basic metrics endpoint"
        )
        async def metrics() -> dict:
            """Basic metrics endpoint for monitoring."""
            try:
                health_status = await self.queue_writer.check_health()
                return {
                    "service": "queue-writer",
                    "status": health_status.status,
                    "checks": health_status.checks,
                    "timestamp": health_status.timestamp.isoformat()
                }
            except Exception as e:
                return {
                    "service": "queue-writer",
                    "status": "error",
                    "error": str(e),
                    "timestamp": time.time()
                }
    
    def _setup_exception_handlers(self) -> None:
        """Setup custom exception handlers."""
        
        @self.app.exception_handler(AuthenticationError)
        async def auth_exception_handler(request: Request, exc: AuthenticationError):
            """Handle authentication errors."""
            logger.warning(
                f"Authentication failed: {exc}",
                extra={
                    "component": "http_server",
                    "path": request.url.path,
                    "client_ip": request.client.host if request.client else "unknown"
                }
            )
            return JSONResponse(
                status_code=401,
                content={"detail": "Authentication failed"}
            )
        
        @self.app.exception_handler(ValueError)
        async def validation_exception_handler(request: Request, exc: ValueError):
            """Handle validation errors."""
            logger.warning(f"Validation error: {exc}")
            return JSONResponse(
                status_code=422,
                content={"detail": f"Validation error: {str(exc)}"}
            )
    
    def run(
        self,
        host: str = "0.0.0.0",
        port: int = 8080,
        log_level: str = "info",
        workers: int = 1
    ) -> None:
        """
        Run the FastAPI server using uvicorn.
        
        Args:
            host: Host to bind to
            port: Port to bind to
            log_level: Logging level
            workers: Number of worker processes
        """
        uvicorn.run(
            self.app,
            host=host,
            port=port,
            log_level=log_level,
            workers=workers,
            access_log=True
        )