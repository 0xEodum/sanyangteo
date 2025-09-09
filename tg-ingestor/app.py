"""
Main application module for tg-ingestor service.
Implements dependency injection and service composition following SOLID principles.
"""

import asyncio
import logging
import signal
import sys
from contextlib import asynccontextmanager
from typing import Optional

from config import load_config, validate_secrets_exist, AppConfig
from telemetry.logger import setup_logging

# Domain imports
from domain.dto import ChatConfig

# Adapter imports
from adapters.telethon_client import TelethonClientAdapter
from adapters.http_publisher import HTTPPublisher, TokenLoader
from adapters.chat_resolver import TelegramChatResolver
from adapters.filters import create_default_filter
from adapters.mapper import TelethonMessageMapper

# Service imports
from services.ingestion_service import TelegramIngestionService


logger = logging.getLogger(__name__)


class TgIngestorApplication:
    """
    Main application class that composes all dependencies.
    Follows Dependency Inversion Principle and manages service lifecycle.
    """
    
    def __init__(self, config: AppConfig):
        """
        Initialize application with configuration.
        
        Args:
            config: Application configuration
        """
        self.config = config
        
        # Dependencies (will be initialized in setup)
        self.telegram_client: Optional[TelethonClientAdapter] = None
        self.publisher: Optional[HTTPPublisher] = None
        self.chat_resolver: Optional[TelegramChatResolver] = None
        self.message_filter = None
        self.message_mapper: Optional[TelethonMessageMapper] = None
        self.ingestion_service: Optional[TelegramIngestionService] = None
        
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
        Setup all service dependencies using dependency injection.
        
        Raises:
            Exception: If setup fails
        """
        try:
            logger.info("Setting up tg-ingestor application")
            
            # Validate secrets exist
            validate_secrets_exist(self.config)
            
            # Initialize Telegram client
            self.telegram_client = TelethonClientAdapter(
                api_id=self.config.telegram.api_id,
                api_hash=self.config.telegram.api_hash,
                session_path=self.config.telegram.session,
                device_model=self.config.telegram.device_model,
                system_version=self.config.telegram.system_version,
                app_version=self.config.telegram.app_version,
                flood_sleep_threshold=self.config.telegram.flood_sleep_threshold
            )
            
            # Initialize HTTP publisher
            shared_token = TokenLoader.load_token_from_file(
                self.config.security.shared_token_file
            )
            
            self.publisher = HTTPPublisher(
                endpoint=self.config.output.endpoint,
                shared_token=shared_token,
                timeout_ms=self.config.output.timeout_ms,
                max_retries=self.config.output.max_retries,
                retry_backoff_ms=self.config.output.retry_backoff_ms,
                max_backoff_ms=self.config.output.max_backoff_ms
            )
            
            # Initialize chat resolver
            self.chat_resolver = TelegramChatResolver(
                telegram_client=self.telegram_client,
                allowed_chat_types={'group', 'supergroup'},
                validate_access=True
            )
            
            # Initialize message filter
            self.message_filter = create_default_filter(
                allow_bots=self.config.processing.allow_bots,
                max_text_length=self.config.processing.max_text_length
            )
            
            # Initialize message mapper
            self.message_mapper = TelethonMessageMapper()
            
            # Convert chat IDs to ChatConfig objects
            chat_configs = [
                ChatConfig(id=chat_id, enabled=True)
                for chat_id in self.config.chats.ids
            ]
            
            # Initialize main ingestion service
            self.ingestion_service = TelegramIngestionService(
                telegram_client=self.telegram_client,
                publisher=self.publisher,
                chat_resolver=self.chat_resolver,
                message_filter=self.message_filter,
                message_mapper=self.message_mapper,
                chat_configs=chat_configs,
                bootstrap_lookback_hours=self.config.bootstrap.lookback_hours if self.config.bootstrap.enabled else 0,
                max_concurrent_messages=self.config.processing.max_concurrent_messages
            )
            
            logger.info("Application setup completed successfully")
            
        except Exception as e:
            logger.error(f"Application setup failed: {e}")
            await self.cleanup()
            raise
    
    async def cleanup(self) -> None:
        """Cleanup application resources."""
        logger.info("Cleaning up application resources")
        
        try:
            # Stop ingestion service
            if self.ingestion_service:
                await self.ingestion_service.stop()
            
            # Close HTTP publisher
            if self.publisher:
                await self.publisher.close()
            
            # Disconnect Telegram client
            if self.telegram_client:
                await self.telegram_client.disconnect()
            
            logger.info("Application cleanup completed")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
    
    async def run(self) -> None:
        """
        Run the application.
        
        This method starts the ingestion service and waits for shutdown signal.
        """
        if not self.ingestion_service:
            raise RuntimeError("Application not setup. Call setup() first.")
        
        logger.info("Starting tg-ingestor application")
        
        try:
            # Start ingestion service
            await self.ingestion_service.start()
            
            logger.info(
                "Application started successfully",
                extra={
                    "component": "app",
                    "monitored_chats": len(self.config.chats.ids),
                    "bootstrap_enabled": self.config.bootstrap.enabled
                }
            )
            
            # Wait for shutdown signal
            await self._wait_for_shutdown()
            
        except Exception as e:
            logger.error(f"Application error: {e}")
            raise
        finally:
            # Ensure cleanup happens
            await self.cleanup()
    
    async def _wait_for_shutdown(self) -> None:
        """Wait for shutdown signal or service failure."""
        try:
            # Monitor service health and wait for shutdown
            while not self._shutdown_event.is_set():
                # Check if ingestion service is still running
                if self.ingestion_service and not self.ingestion_service._is_running:
                    logger.error("Ingestion service stopped unexpectedly")
                    break
                
                # Check publisher health periodically
                if self.publisher:
                    health_ok = await self.publisher.check_health()
                    if not health_ok:
                        logger.warning("Publisher health check failed")
                
                # Wait a bit before next check
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=30.0)
                    break  # Shutdown event was set
                except asyncio.TimeoutError:
                    continue  # Continue monitoring
                
        except Exception as e:
            logger.error(f"Error while waiting for shutdown: {e}")
        
        logger.info("Shutdown signal received, stopping application")
    
    @asynccontextmanager
    async def lifespan(self):
        """
        Context manager for application lifecycle.
        
        Handles setup and cleanup automatically.
        """
        try:
            await self.setup()
            yield self
        finally:
            await self.cleanup()
    
    async def get_stats(self) -> dict:
        """
        Get application statistics.
        
        Returns:
            Dictionary with current statistics
        """
        stats = {
            "application": "tg-ingestor",
            "status": "running" if self._is_running() else "stopped",
            "config": {
                "monitored_chats": len(self.config.chats.ids),
                "bootstrap_enabled": self.config.bootstrap.enabled,
                "max_concurrent": self.config.processing.max_concurrent_messages
            }
        }
        
        # Add ingestion service stats if available
        if self.ingestion_service:
            try:
                ingestion_stats = await self.ingestion_service.get_stats()
                stats["ingestion"] = ingestion_stats.model_dump()
            except Exception as e:
                stats["ingestion_error"] = str(e)
        
        return stats
    
    def _is_running(self) -> bool:
        """Check if application is running."""
        return (
            self.ingestion_service is not None and 
            self.ingestion_service._is_running and
            not self._shutdown_event.is_set()
        )


async def create_app() -> TgIngestorApplication:
    """
    Factory function to create and setup the application.
    
    Returns:
        Configured and setup application instance
    """
    # Load configuration
    config = load_config()
    
    # Setup logging
    setup_logging(
        level=config.logging.level,
        service_name="tg-ingestor",
        enable_json=config.logging.json_format,
        enable_correlation=config.logging.enable_correlation
    )
    
    # Create application
    app = TgIngestorApplication(config)
    await app.setup()
    
    return app


async def main() -> None:
    """
    Main entry point for the application.
    """
    try:
        # Load configuration first for logging setup
        config = load_config()
        
        # Setup logging
        setup_logging(
            level=config.logging.level,
            service_name="tg-ingestor",
            enable_json=config.logging.json_format,
            enable_correlation=config.logging.enable_correlation
        )
        
        logger.info("Starting tg-ingestor service")
        
        # Create and run application
        async with TgIngestorApplication(config).lifespan() as app:
            await app.run()
            
    except KeyboardInterrupt:
        logger.info("Service interrupted by user")
    except Exception as e:
        logger.error(f"Service failed: {e}")
        sys.exit(1)


# Health check endpoint for container health checks
async def health_check() -> bool:
    """
    Simple health check function for container health checks.
    
    Returns:
        True if service appears healthy, False otherwise
    """
    try:
        # Basic health check - could be expanded
        config = load_config()
        
        # Check if required files exist
        validate_secrets_exist(config)
        
        return True
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return False


if __name__ == "__main__":
    # Check if this is a health check call
    if len(sys.argv) > 1 and sys.argv[1] == "healthcheck":
        # Simple health check for Docker
        import asyncio
        
        async def run_health_check():
            healthy = await health_check()
            sys.exit(0 if healthy else 1)
        
        asyncio.run(run_health_check())
    else:
        # Normal service startup
        asyncio.run(main())