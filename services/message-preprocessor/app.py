"""
Main application module for message-preprocessor service.
Integrates LLM classification, parsing, validation and queue processing.
"""

import asyncio
import logging
import signal
import sys
from contextlib import asynccontextmanager
from typing import Optional
from pathlib import Path

from config import (
    load_config, 
    load_api_keys, 
    load_api_models, 
    load_system_prompt,
    AppConfig
)
from telemetry.logger import setup_logging, MetricsLogger

# Domain imports
from domain.dto import ProcessingMode, ParsingSchema, FieldConfig

# Adapter imports
from adapters import (
    OpenAICompatibleClient,
    create_key_manager,
    create_message_parser,
    create_model_manager,
    create_message_validator,
    create_input_consumer,
    create_output_publisher
)

# Service imports
from services.preprocessing_service import TelegramPreprocessingService


logger = logging.getLogger(__name__)


class MessagePreprocessorApplication:
    """
    Main application for message preprocessing service.
    """
    
    def __init__(self, config: AppConfig):
        """
        Initialize application with configuration.
        
        Args:
            config: Application configuration
        """
        self.config = config
        
        # Dependencies
        self.llm_client: Optional[OpenAICompatibleClient] = None
        self.key_manager = None
        self.model_manager = None
        self.message_parser = None
        self.message_validator = None
        self.input_consumer = None
        self.output_publisher = None
        self.preprocessing_service: Optional[TelegramPreprocessingService] = None
        
        # External data
        self.system_prompt: Optional[str] = None
        
        # Lifecycle management
        self._shutdown_event = asyncio.Event()
        self._setup_signal_handlers()
        
        # Metrics
        self.metrics = MetricsLogger("app")
    
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
        """
        try:
            logger.info("Setting up message-preprocessor service")
            
            # Load external dependencies
            await self._load_external_dependencies()
            
            # Initialize adapters
            await self._initialize_adapters()
            
            # Initialize main service
            await self._initialize_service()
            
            logger.info(
                "Application setup completed",
                extra={
                    "component": "app",
                    "processing_mode": self.config.mode.value,
                    "input_queue": self.config.input_queue.queue_name,
                    "output_queue": self.config.output_queue.queue_name
                }
            )
            
        except Exception as e:
            logger.error(f"Application setup failed: {e}")
            await self.cleanup()
            raise
    
    async def _load_external_dependencies(self) -> None:
        """Load external dependencies from files."""
        # Load system prompt
        self.system_prompt = load_system_prompt(self.config.dependencies.sys_prompt_file)
        
        # Load API keys
        api_keys = load_api_keys(self.config.dependencies.api_keys_file)
        
        # Load API models
        api_models = load_api_models(self.config.dependencies.api_urls_file)
        
        # Store for adapter initialization
        self._api_keys = api_keys
        self._api_models = api_models
    
    async def _initialize_adapters(self) -> None:
        """Initialize all adapter components."""
        
        # Initialize LLM client
        self.llm_client = OpenAICompatibleClient(
            timeout_seconds=self.config.llm.timeout_seconds
        )
        
        # Initialize key manager
        self.key_manager = create_key_manager(
            redis_url=self.config.redis_counters.url,
            key_prefix=self.config.redis_counters.key_prefix,
            rotation_keys=self._api_keys['rotation'],
            direct_keys=self._api_keys['direct']
        )
        
        await self.key_manager.connect()
        
        # Initialize model manager
        self.model_manager = create_model_manager(self._api_models)
        
        # Initialize message parser with schemas
        parsing_schemas = self._convert_schemas_to_dto(self.config.parsing_schemas)
        self.message_parser = create_message_parser(parsing_schemas)
        
        # Initialize message validator
        self.message_validator = create_message_validator()
        
        # Initialize input consumer
        self.input_consumer = create_input_consumer(
            queue_type=self.config.input_queue.type,
            queue_config=self.config.input_queue.config,
            queue_name=self.config.input_queue.queue_name,
            consumer_group=self.config.input_queue.consumer_group or "message_preprocessor",
            consumer_id="processor-1"
        )
        
        await self.input_consumer.connect()
        
        # Initialize output publisher
        self.output_publisher = create_output_publisher(
            queue_type=self.config.output_queue.type,
            queue_config=self.config.output_queue.config,
            queue_name=self.config.output_queue.queue_name
        )
        
        await self.output_publisher.connect()
    
    def _convert_schemas_to_dto(self, parsing_schemas_config) -> dict:
        """Convert config parsing schemas to DTO format."""
        schemas = {}
        
        for schema_type, versions in parsing_schemas_config.model_dump().items():
            if schema_type.startswith('active_'):
                continue  # Skip active version settings
            
            schemas[schema_type] = {}
            
            for version, schema_data in versions.items():
                fields = []
                for field_data in schema_data['fields']:
                    fields.append(FieldConfig(**field_data))
                
                # ✅ Добавляем version из ключа при создании ParsingSchema
                schemas[schema_type][version] = ParsingSchema(
                    version=version,  # ✅ Берем версию из ключа (v1, v2, etc.)
                    fields=fields
                )
        
        return schemas
    
    async def _initialize_service(self) -> None:
        """Initialize main preprocessing service."""
        
        # Determine active schema version
        active_order_schema = self.config.parsing_schemas.active_order_schema
        
        self.preprocessing_service = TelegramPreprocessingService(
            llm_client=self.llm_client,
            key_manager=self.key_manager,
            model_manager=self.model_manager,
            message_parser=self.message_parser,
            message_validator=self.message_validator,
            output_publisher=self.output_publisher,
            processing_mode=self.config.mode,
            system_prompt=self.system_prompt,
            parsing_schema_version=active_order_schema,
            retry_delay_seconds=self.config.retry.delay_seconds,
            max_attempts_per_model=self.config.retry.max_attempts_per_model,
            max_concurrent_messages=self.config.processing.max_concurrent_messages
        )
        
        await self.preprocessing_service.start()
    
    async def cleanup(self) -> None:
        """Cleanup application resources."""
        logger.info("Cleaning up application resources")
        
        try:
            # Stop preprocessing service
            if self.preprocessing_service:
                await self.preprocessing_service.stop()
            
            # Stop input consumer
            if self.input_consumer:
                await self.input_consumer.close()
            
            # Close output publisher
            if self.output_publisher:
                await self.output_publisher.close()
            
            # Close key manager
            if self.key_manager:
                await self.key_manager.close()
            
            # Close LLM client
            if self.llm_client:
                await self.llm_client.close()
            
            logger.info("Application cleanup completed")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
    
    async def run(self) -> None:
        """
        Run the application.
        """
        if not self.preprocessing_service:
            raise RuntimeError("Application not setup. Call setup() first.")
        
        logger.info("Starting message-preprocessor service")
        
        try:
            # Start consuming messages
            await self.input_consumer.start_consuming(
                self.preprocessing_service.process_message
            )
            
            logger.info(
                "Application started successfully",
                extra={
                    "component": "app",
                    "processing_mode": self.config.mode.value,
                    "input_queue": self.config.input_queue.queue_name,
                    "output_queue": self.config.output_queue.queue_name
                }
            )
            
            # Wait for shutdown signal
            await self._wait_for_shutdown()
            
        except Exception as e:
            logger.error(f"Application error: {e}")
            raise
        finally:
            await self.cleanup()
    
    async def _wait_for_shutdown(self) -> None:
        """Wait for shutdown signal or service failure."""
        try:
            while not self._shutdown_event.is_set():
                # Check service health periodically
                if self.preprocessing_service:
                    health = await self.preprocessing_service.health_check()
                    if not health.get("overall_healthy", False):
                        logger.error("Service unhealthy, initiating shutdown")
                        break
                
                # Check queue health
                if self.output_publisher:
                    if not await self.output_publisher.check_health():
                        logger.warning("Output publisher unhealthy")
                
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
        """
        try:
            await self.setup()
            yield self
        finally:
            await self.cleanup()
    
    async def get_stats(self) -> dict:
        """
        Get application statistics.
        """
        stats = {
            "application": "message-preprocessor",
            "status": "running" if self._is_running() else "stopped",
            "config": {
                "processing_mode": self.config.mode.value,
                "input_queue": self.config.input_queue.queue_name,
                "output_queue": self.config.output_queue.queue_name,
                "max_concurrent": self.config.processing.max_concurrent_messages,
                "retry_delay_seconds": self.config.retry.delay_seconds,
                "active_schema": self.config.parsing_schemas.active_order_schema
            }
        }
        
        # Add preprocessing service stats if available
        if self.preprocessing_service:
            try:
                processing_stats = await self.preprocessing_service.get_stats()
                stats["processing"] = processing_stats.model_dump()
            except Exception as e:
                stats["processing_error"] = str(e)
        
        # Add health check results
        if self.preprocessing_service:
            try:
                health = await self.preprocessing_service.health_check()
                stats["health"] = health
            except Exception as e:
                stats["health_error"] = str(e)
        
        return stats
    
    def _is_running(self) -> bool:
        """Check if application is running."""
        return (
            self.preprocessing_service is not None and 
            self.preprocessing_service._is_running and
            not self._shutdown_event.is_set()
        )


async def create_app() -> MessagePreprocessorApplication:
    """
    Factory function to create and setup the application.
    """
    # Load configuration
    config = load_config()
    
    # Setup logging
    setup_logging(
        level=config.logging.level,
        service_name="message-preprocessor",
        enable_json=config.logging.json_format,
        enable_correlation=config.logging.enable_correlation
    )
    
    # Create application
    app = MessagePreprocessorApplication(config)
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
            service_name="message-preprocessor",
            enable_json=config.logging.json_format,
            enable_correlation=config.logging.enable_correlation
        )
        
        logger.info(
            "Starting message-preprocessor service",
            extra={
                "component": "app",
                "processing_mode": config.mode.value,
                "input_queue": config.input_queue.queue_name,
                "output_queue": config.output_queue.queue_name
            }
        )
        
        # Create and run application
        async with MessagePreprocessorApplication(config).lifespan() as app:
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
    """
    try:
        config = load_config()
        
        # Basic validation
        if not config.mode:
            return False
        
        if not Path(config.dependencies.sys_prompt_file).exists():
            return False
        
        if not Path(config.dependencies.api_keys_file).exists():
            return False
            
        if not Path(config.dependencies.api_urls_file).exists():
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return False


if __name__ == "__main__":
    # Check if this is a health check call
    if len(sys.argv) > 1 and sys.argv[1] == "healthcheck":
        import asyncio
        
        async def run_health_check():
            healthy = await health_check()
            sys.exit(0 if healthy else 1)
        
        asyncio.run(run_health_check())
    else:
        # Normal service startup
        asyncio.run(main())