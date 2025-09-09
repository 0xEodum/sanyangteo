"""
Main application module for queue-writer service.
Implements dependency injection and service composition following SOLID principles.
"""

import asyncio
import logging
import signal
import sys
from contextlib import asynccontextmanager
from typing import Optional

from config import load_config, AppConfig
from telemetry.logger import setup_logging
from infra.redis_client import RedisClient
from infra.redis_writer import RedisStreamWriter
from api.auth import SharedTokenValidator
from api.http_server import QueueWriterAPI


logger = logging.getLogger(__name__)


class QueueWriterService:
    """
    Main service class that composes all dependencies.
    Follows Dependency Inversion Principle and manages service lifecycle.
    """
    
    def __init__(self, config: AppConfig):
        """
        Initialize service with configuration.
        
        Args:
            config: Application configuration
        """
        self.config = config
        
        # Dependencies (will be initialized in setup)
        self.redis_client: Optional[RedisClient] = None
        self.queue_writer: Optional[RedisStreamWriter] = None
        self.token_validator: Optional[SharedTokenValidator] = None
        self.api: Optional[QueueWriterAPI] = None
        
        # Lifecycle management
        self._shutdown_event = asyncio.Event()
        self._setup_signal_handlers()
    
    def _setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating shutdown")
            self._shutdown_event.set()
        
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
    
    async def setup(self) -> None:
        """
        Setup all service dependencies.
        
        Raises:
            Exception: If setup fails
        """
        try:
            logger.info("Setting up queue-writer service")
            
            # Initialize Redis client
            self.redis_client = RedisClient(
                url=self.config.redis.url,
                max_connections=self.config.redis.max_connections,
                socket_timeout=self.config.redis.socket_timeout,
                socket_connect_timeout=self.config.redis.socket_connect_timeout,
                health_check_interval=self.config.redis.health_check_interval
            )
            
            # Connect to Redis
            await self.redis_client.connect()
            
            # Initialize queue writer
            self.queue_writer = RedisStreamWriter(
                redis_client=self.redis_client,
                stream_key=self.config.redis.stream_key,
                dedup_set_key=self.config.redis.dedup_set_key,
                dedup_ttl_seconds=self.config.redis.dedup_ttl_seconds,
                maxlen_approx=self.config.redis.maxlen_approx
            )
            
            # Initialize token validator
            self.token_validator = SharedTokenValidator(
                token_file_path=self.config.security.shared_token_file
            )
            
            # Initialize API
            self.api = QueueWriterAPI(
                queue_writer=self.queue_writer,
                token_validator=self.token_validator,
                title="Queue Writer Service",
                version="1.0.0"
            )
            
            logger.info("Service setup completed successfully")
            
        except Exception as e:
            logger.error(f"Service setup failed: {e}")
            await self.cleanup()
            raise
    
    async def cleanup(self) -> None:
        """Cleanup service resources."""
        logger.info("Cleaning up service resources")
        
        try:
            # Close Redis connection
            if self.redis_client:
                await self.redis_client.close()
            
            logger.info("Service cleanup completed")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
    
    async def run(self) -> None:
        """
        Run the service.
        
        This method starts the HTTP server and waits for shutdown signal.
        """
        if not self.api:
            raise RuntimeError("Service not setup. Call setup() first.")
        
        logger.info(
            f"Starting queue-writer service on {self.config.server.host}:{self.config.server.port}"
        )
        
        # Note: In a real deployment, we'd use uvicorn programmatically
        # For now, we'll run the FastAPI app directly
        import uvicorn
        
        # Create server config
        server_config = uvicorn.Config(
            app=self.api.app,
            host=self.config.server.host,
            port=self.config.server.port,
            log_level=self.config.server.log_level,
            workers=1,  # Use 1 worker for simplicity
            access_log=True
        )
        
        server = uvicorn.Server(server_config)
        
        # Run server with graceful shutdown
        try:
            await server.serve()
        except Exception as e:
            logger.error(f"Server error: {e}")
            raise
    
    @asynccontextmanager
    async def lifespan(self):
        """
        Context manager for service lifecycle.
        
        Handles setup and cleanup automatically.
        """
        try:
            await self.setup()
            yield self
        finally:
            await self.cleanup()


class EventProcessor:
    """
    Service layer for processing events.
    
    Implements business logic and coordinates between layers.
    This could be expanded for more complex processing logic.
    """
    
    def __init__(self, queue_writer: RedisStreamWriter):
        """
        Initialize event processor.
        
        Args:
            queue_writer: Queue writer implementation
        """
        self.queue_writer = queue_writer
    
    async def process_event(self, event) -> dict:
        """
        Process an incoming event.
        
        Args:
            event: The event to process
            
        Returns:
            Processing result
        """
        # Add any business logic here
        # For now, just delegate to queue writer
        return await self.queue_writer.write_event(event)


async def create_app() -> QueueWriterService:
    """
    Factory function to create and setup the application.
    
    Returns:
        Configured and setup service instance
    """
    # Load configuration
    config = load_config()
    
    # Setup logging
    setup_logging(
        level=config.logging.level,
        service_name="queue-writer",
        enable_json=config.logging.json_format,
        enable_correlation=config.logging.enable_correlation
    )
    
    # Create service
    service = QueueWriterService(config)
    await service.setup()
    
    return service


async def main() -> None:
    """
    Main entry point for the application.
    """
    try:
        # Create and run service
        async with QueueWriterService(load_config()).lifespan() as service:
            # Setup logging first
            setup_logging(
                level=service.config.logging.level,
                service_name="queue-writer",
                enable_json=service.config.logging.json_format,
                enable_correlation=service.config.logging.enable_correlation
            )
            
            # Run service
            await service.run()
            
    except KeyboardInterrupt:
        logger.info("Service interrupted by user")
    except Exception as e:
        logger.error(f"Service failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())