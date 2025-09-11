"""
Main ingestion service for tg-ingestor.
Coordinates message processing, filtering, mapping, and publishing.
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Optional

from telethon.types import Message

from domain.ports import (
    IngestionService,
    TelegramClient,
    Publisher,
    ChatResolver,
    MessageFilter,
    MessageMapper,
    IngestionError,
)
from domain.dto import (
    ChatConfig,
    IngestionStats,
    MessageContext,
    TelegramEventDTO,
)


logger = logging.getLogger(__name__)


class TelegramIngestionService(IngestionService):
    """
    Main service that coordinates the entire ingestion pipeline.
    
    Pipeline: Telegram → Filter → Map → Publish → Queue-Writer
    """
    
    def __init__(
        self,
        telegram_client: TelegramClient,
        publisher: Publisher,
        chat_resolver: ChatResolver,
        message_filter: MessageFilter,
        message_mapper: MessageMapper,
        chat_configs: List[ChatConfig],
        bootstrap_lookback_hours: int = 12,
        max_concurrent_messages: int = 10
    ):

        self.telegram_client = telegram_client
        self.publisher = publisher
        self.chat_resolver = chat_resolver
        self.message_filter = message_filter
        self.message_mapper = message_mapper
        self.chat_configs = chat_configs
        self.bootstrap_lookback_hours = bootstrap_lookback_hours
        self.max_concurrent_messages = max_concurrent_messages
        
        # Service state
        self._is_running = False
        self._validated_chats: List[ChatConfig] = []
        self._stats = IngestionStats()
        self._semaphore = asyncio.Semaphore(max_concurrent_messages)
        
        # Background tasks
        self._monitoring_task: Optional[asyncio.Task] = None
        self._bootstrap_task: Optional[asyncio.Task] = None
    
    async def start(self) -> None:

        if self._is_running:
            logger.warning("Ingestion service already running")
            return
        
        try:
            logger.info("Starting Telegram ingestion service")
            
            # Step 1: Connect to Telegram
            await self._connect_telegram()
            
            # Step 2: Validate chat configurations
            await self._validate_chats()
            
            # Step 3: Start live monitoring
            await self._start_monitoring()
            
            # Step 4: Perform bootstrap in background
            if self.bootstrap_lookback_hours > 0:
                self._bootstrap_task = asyncio.create_task(self._perform_bootstrap())
            
            self._is_running = True
            
            logger.info(
                "Ingestion service started successfully",
                extra={
                    "component": "ingestion_service",
                    "monitored_chats": len(self._validated_chats),
                    "bootstrap_hours": self.bootstrap_lookback_hours
                }
            )
            
        except Exception as e:
            logger.error(f"Failed to start ingestion service: {e}")
            await self.stop()
            raise IngestionError(f"Service startup failed: {e}") from e
    
    async def stop(self) -> None:
        """Stop the ingestion service and cleanup resources."""
        if not self._is_running:
            return
        
        logger.info("Stopping ingestion service")
        
        try:
            # Cancel background tasks
            if self._bootstrap_task and not self._bootstrap_task.done():
                self._bootstrap_task.cancel()
                try:
                    await self._bootstrap_task
                except asyncio.CancelledError:
                    pass
            
            if self._monitoring_task and not self._monitoring_task.done():
                self._monitoring_task.cancel()
                try:
                    await self._monitoring_task
                except asyncio.CancelledError:
                    pass
            
            # Stop Telegram monitoring
            await self.telegram_client.stop_monitoring()
            
            # Disconnect from Telegram
            await self.telegram_client.disconnect()
            
            self._is_running = False
            
            logger.info(
                "Ingestion service stopped",
                extra={
                    "component": "ingestion_service",
                    "final_stats": self._stats.model_dump()
                }
            )
            
        except Exception as e:
            logger.error(f"Error during service shutdown: {e}")
    
    async def get_stats(self) -> IngestionStats:

        return self._stats.copy()
    
    async def perform_bootstrap(self, chat_id: int, since: datetime) -> int:

        logger.info(
            f"Starting bootstrap for chat {chat_id} since {since}",
            extra={
                "component": "ingestion_service",
                "chat_id": chat_id,
                "since": since.isoformat()
            }
        )
        
        processed_count = 0
        
        try:
            async for message in self.telegram_client.get_chat_history(chat_id, since):
                try:
                    # Process message with bootstrap context
                    context = MessageContext(
                        chat_id=chat_id,
                        message_id=message.id,
                        is_bootstrap=True
                    )
                    
                    success = await self._process_message(message, context)
                    if success:
                        processed_count += 1
                        
                except Exception as e:
                    logger.error(
                        f"Failed to process bootstrap message: {e}",
                        extra={
                            "component": "ingestion_service",
                            "chat_id": chat_id,
                            "message_id": getattr(message, 'id', 'unknown'),
                            "error": str(e)
                        }
                    )
        
        except Exception as e:
            logger.error(
                f"Bootstrap failed for chat {chat_id}: {e}",
                extra={
                    "component": "ingestion_service",
                    "chat_id": chat_id,
                    "error": str(e)
                }
            )
            raise
        
        logger.info(
            f"Bootstrap completed for chat {chat_id}: {processed_count} messages",
            extra={
                "component": "ingestion_service",
                "chat_id": chat_id,
                "processed_count": processed_count
            }
        )
        
        return processed_count
    
    async def _connect_telegram(self) -> None:
        """Connect to Telegram."""
        logger.info("Connecting to Telegram")
        await self.telegram_client.connect()
    
    async def _validate_chats(self) -> None:
        """Validate chat configurations."""
        logger.info("Validating chat configurations")
        
        self._validated_chats = await self.chat_resolver.validate_chats(self.chat_configs)
        self._stats.chats_monitored = len(self._validated_chats)
        
        if not self._validated_chats:
            raise IngestionError("No valid chats found")
        
        logger.info(
            f"Validated {len(self._validated_chats)} chats",
            extra={
                "component": "ingestion_service",
                "validated_count": len(self._validated_chats)
            }
        )
    
    async def _start_monitoring(self) -> None:
        """Start live message monitoring."""
        chat_ids = [chat.id for chat in self._validated_chats]
        
        # Set message handler
        self.telegram_client.set_message_handler(self._handle_live_message)
        
        # Start monitoring
        await self.telegram_client.start_monitoring(chat_ids)
        
        logger.info(
            f"Started monitoring {len(chat_ids)} chats",
            extra={
                "component": "ingestion_service",
                "chat_ids": chat_ids
            }
        )
    
    async def _handle_live_message(self, message: Message) -> None:

        try:
            # Create message context for live message
            context = MessageContext(
                chat_id=getattr(message, 'chat_id', 0),
                message_id=message.id,
                is_bootstrap=False
            )
            
            # Process message with concurrency control
            async with self._semaphore:
                await self._process_message(message, context)
                
        except Exception as e:
            logger.error(
                f"Failed to handle live message: {e}",
                extra={
                    "component": "ingestion_service",
                    "message_id": getattr(message, 'id', 'unknown'),
                    "error": str(e)
                }
            )
    
    async def _process_message(self, message: Message, context: MessageContext) -> bool:

        self._stats.increment_processed()
        
        try:
            # Step 1: Filter message
            if not self.message_filter.should_process(message):
                filter_reason = self.message_filter.get_filter_reason(message)
                logger.debug(
                    f"Message filtered: {filter_reason}",
                    extra={
                        "component": "ingestion_service",
                        "chat_id": context.chat_id,
                        "message_id": context.message_id,
                        "filter_reason": filter_reason
                    }
                )
                self._stats.increment_filtered()
                return False
            
            # Step 2: Map message to DTO
            event_dto = await self.message_mapper.map_message(message, context)
            if not event_dto:
                logger.debug(
                    f"Message mapping returned None",
                    extra={
                        "component": "ingestion_service",
                        "chat_id": context.chat_id,
                        "message_id": context.message_id
                    }
                )
                self._stats.increment_failed()
                return False
            
            # Step 3: Publish event
            result = await self.publisher.publish_event(event_dto)
            
            if result.success:
                if result.is_duplicate:
                    self._stats.increment_duplicate()
                    logger.debug(
                        f"Duplicate message detected",
                        extra={
                            "component": "ingestion_service",
                            "chat_id": context.chat_id,
                            "message_id": context.message_id,
                            "idempotency_key": event_dto.idempotency_key
                        }
                    )
                else:
                    self._stats.increment_published()
                    if context.is_bootstrap:
                        self._stats.bootstrap_messages += 1
                
                # Log successful processing
                processing_time = context.get_processing_time_ms()
                logger.info(
                    f"Message processed successfully",
                    extra={
                        "component": "ingestion_service",
                        "chat_id": context.chat_id,
                        "message_id": context.message_id,
                        "stream_id": result.stream_id,
                        "is_duplicate": result.is_duplicate,
                        "is_bootstrap": context.is_bootstrap,
                        "processing_time_ms": processing_time
                    }
                )
                
                return True
            else:
                logger.error(
                    f"Failed to publish message: {result.error}",
                    extra={
                        "component": "ingestion_service",
                        "chat_id": context.chat_id,
                        "message_id": context.message_id,
                        "error": result.error
                    }
                )
                self._stats.increment_failed()
                return False
                
        except Exception as e:
            logger.error(
                f"Message processing failed: {e}",
                extra={
                    "component": "ingestion_service",
                    "chat_id": context.chat_id,
                    "message_id": context.message_id,
                    "error": str(e)
                }
            )
            self._stats.increment_failed()
            return False
    
    async def _perform_bootstrap(self) -> None:
        """Perform bootstrap for all validated chats."""
        logger.info("Starting bootstrap process")
        
        # Calculate bootstrap time
        since = datetime.now(timezone.utc) - timedelta(hours=self.bootstrap_lookback_hours)
        
        total_processed = 0
        
        for chat_config in self._validated_chats:
            try:
                count = await self.perform_bootstrap(chat_config.id, since)
                total_processed += count
                
            except Exception as e:
                logger.error(
                    f"Bootstrap failed for chat {chat_config.id}: {e}",
                    extra={
                        "component": "ingestion_service",
                        "chat_id": chat_config.id,
                        "error": str(e)
                    }
                )
        
        self._stats.bootstrap_completed = True
        
        logger.info(
            f"Bootstrap completed: {total_processed} total messages",
            extra={
                "component": "ingestion_service",
                "total_processed": total_processed,
                "lookback_hours": self.bootstrap_lookback_hours
            }
        )