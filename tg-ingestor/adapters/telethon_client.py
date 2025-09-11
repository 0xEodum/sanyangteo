"""
Telethon client adapter с правильной обработкой access_hash и кеша диалогов.
"""

import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, AsyncGenerator, Callable, Any, Dict

from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError,
    SessionPasswordNeededError,
    ApiIdInvalidError,
    PhoneCodeInvalidError,
    AuthKeyUnregisteredError
)
from telethon.types import Message, Chat, Channel, User
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import (
    InputPeerEmpty, InputPeerChat, InputPeerChannel, InputPeerUser,
    PeerUser, PeerChat, PeerChannel
)
from telethon.utils import get_peer_id

from domain.ports import (
    TelegramClient as TelegramClientInterface,
    ConnectionError,
    MonitoringError,
    HistoryError,
)
from domain.dto import MessageContext


logger = logging.getLogger(__name__)


class TelethonClientAdapter(TelegramClientInterface):
    """
    Telethon-based implementation с правильной обработкой кеша и access_hash.
    """
    
    def __init__(
        self,
        api_id: int,
        api_hash: str,
        session_path: str,
        device_model: str = "tg-ingestor",
        system_version: str = "1.0",
        app_version: str = "1.0",
        flood_sleep_threshold: int = 60
    ):
        """Initialize Telethon client adapter."""
        self.api_id = api_id
        self.api_hash = api_hash
        self.session_path = Path(session_path)
        self.flood_sleep_threshold = flood_sleep_threshold
        
        # Create session directory if needed
        self.session_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize Telethon client
        self.client = TelegramClient(
            str(self.session_path),
            api_id,
            api_hash,
            device_model=device_model,
            system_version=system_version,
            app_version=app_version,
            flood_sleep_threshold=flood_sleep_threshold
        )
        
        # Message handler callback
        self._message_handler: Optional[Callable[[Message], Any]] = None
        self._monitored_chats: List[int] = []
        self._is_monitoring = False
        
        # Кеш для entities с access_hash
        self._entity_cache: Dict[int, dict] = {}
        self._dialogs_loaded = False
    
    async def connect(self) -> None:
        """Connect to Telegram and authenticate."""
        try:
            logger.info("Connecting to Telegram...")
            await self.client.connect()
            
            if not await self.client.is_user_authorized():
                logger.error("Session is not authorized. Please run authentication setup first.")
                raise ConnectionError(
                    "Telegram session not authorized. Please authenticate manually first."
                )
            
            me = await self.client.get_me()
            logger.info(
                f"Connected to Telegram as {me.first_name} (@{me.username})",
                extra={
                    "component": "telethon_client",
                    "user_id": me.id,
                    "username": me.username
                }
            )
            
            # ВАЖНО: Загружаем диалоги в кеш сразу после подключения
            await self._ensure_dialogs_loaded()
            
        except ApiIdInvalidError as e:
            logger.error(f"Invalid API credentials: {e}")
            raise ConnectionError(f"Invalid Telegram API credentials: {e}") from e
        except AuthKeyUnregisteredError as e:
            logger.error(f"Auth key unregistered: {e}")
            raise ConnectionError(f"Telegram session expired, please re-authenticate: {e}") from e
        except Exception as e:
            logger.error(f"Failed to connect to Telegram: {e}")
            await self.disconnect()
            raise ConnectionError(f"Telegram connection failed: {e}") from e
    
    async def _ensure_dialogs_loaded(self) -> None:
        """
        Загружаем все диалоги в кеш Telethon.
        Это критически важно для правильной работы get_entity().
        """
        if self._dialogs_loaded:
            return
        
        logger.info("Loading dialogs into Telethon cache...")
        dialog_count = 0
        group_count = 0
        
        try:
            # Загружаем ВСЕ диалоги - это заполняет внутренний кеш Telethon
            async for dialog in self.client.iter_dialogs():
                entity = dialog.entity
                dialog_count += 1
                
                # Сохраняем информацию о группах и супергруппах
                if self._is_group_or_supergroup(entity):
                    group_count += 1
                    entity_info = {
                        "id": entity.id,
                        "title": getattr(entity, 'title', f'Chat {entity.id}'),
                        "type": self._get_entity_type(entity),
                        "username": getattr(entity, 'username', None),
                        "entity": entity,  # Сохраняем сам entity с access_hash
                        "dialog": dialog,
                        "accessible": True
                    }
                    
                    # Добавляем access_hash если есть
                    if hasattr(entity, 'access_hash') and entity.access_hash:
                        entity_info["access_hash"] = entity.access_hash
                    
                    self._entity_cache[entity.id] = entity_info
                    
                    logger.debug(
                        f"Cached group: {entity_info['title']} (ID: {entity.id}, Type: {entity_info['type']})"
                    )
            
            self._dialogs_loaded = True
            
            logger.info(
                f"Loaded {dialog_count} dialogs, found {group_count} groups/supergroups",
                extra={
                    "component": "telethon_client",
                    "total_dialogs": dialog_count,
                    "groups_found": group_count
                }
            )
            
        except Exception as e:
            logger.error(f"Failed to load dialogs: {e}")
            raise
    
    async def disconnect(self) -> None:
        """Disconnect from Telegram and cleanup resources."""
        try:
            if self._is_monitoring:
                await self.stop_monitoring()
            
            if self.client.is_connected():
                await self.client.disconnect()
            
            logger.info("Disconnected from Telegram")
            
        except Exception as e:
            logger.error(f"Error during disconnect: {e}")
    
    async def resolve_chat(self, chat_id: int) -> Optional[dict]:
        """
        Resolve chat information by ID используя загруженный кеш.
        """
        try:
            # Убеждаемся, что диалоги загружены
            await self._ensure_dialogs_loaded()
            
            # Проверяем кеш сначала
            if chat_id in self._entity_cache:
                cached_info = self._entity_cache[chat_id].copy()
                # Убираем entity и dialog из возвращаемой информации
                cached_info.pop('entity', None)
                cached_info.pop('dialog', None)
                
                logger.debug(f"Found chat {chat_id} in cache: {cached_info['title']}")
                return cached_info
            
            # Если не в кеше групп, попробуем прямой доступ
            logger.debug(f"Chat {chat_id} not found in group cache, trying direct access...")
            
            try:
                entity = await self.client.get_entity(chat_id)
                
                if not self._is_group_or_supergroup(entity):
                    logger.warning(f"Entity {chat_id} is not a group or supergroup: {type(entity).__name__}")
                    return None
                
                # Создаем информацию о чате
                chat_info = {
                    "id": chat_id,
                    "title": getattr(entity, 'title', f'Chat {chat_id}'),
                    "type": self._get_entity_type(entity),
                    "accessible": True
                }
                
                if hasattr(entity, 'username') and entity.username:
                    chat_info["username"] = entity.username
                
                logger.info(f"Successfully resolved chat {chat_id} directly: {chat_info['title']}")
                return chat_info
                
            except Exception as e:
                logger.error(
                    f"Failed to resolve chat {chat_id}: {e}",
                    extra={
                        "component": "telethon_client", 
                        "chat_id": chat_id,
                        "error": str(e)
                    }
                )
                return None
                
        except Exception as e:
            logger.error(f"Error in resolve_chat for {chat_id}: {e}")
            return None
    
    async def start_monitoring(self, chat_ids: List[int]) -> None:
        """Start monitoring specified chats for new messages."""
        try:
            if self._is_monitoring:
                logger.warning("Already monitoring, stopping previous monitoring")
                await self.stop_monitoring()
            
            # Убеждаемся, что диалоги загружены
            await self._ensure_dialogs_loaded()
            
            # Валидируем и резолвим все чаты
            accessible_entities = []
            monitoring_entities = []  # Сохраняем entity объекты для мониторинга
            
            for chat_id in chat_ids:
                chat_info = await self.resolve_chat(chat_id)
                if chat_info:
                    accessible_entities.append(chat_id)
                    
                    # Получаем entity для мониторинга
                    if chat_id in self._entity_cache:
                        entity = self._entity_cache[chat_id]['entity']
                        monitoring_entities.append(entity)
                    else:
                        # Fallback - пытаемся получить entity напрямую
                        try:
                            entity = await self.client.get_entity(chat_id)
                            monitoring_entities.append(entity)
                        except Exception as e:
                            logger.error(f"Cannot get entity for monitoring chat {chat_id}: {e}")
                            continue
                    
                    logger.info(
                        f"Will monitor chat: {chat_info['title']} ({chat_id})",
                        extra={
                            "component": "telethon_client",
                            "chat_id": chat_id,
                            "chat_title": chat_info['title']
                        }
                    )
                else:
                    logger.error(
                        f"Cannot access chat {chat_id}",
                        extra={
                            "component": "telethon_client",
                            "chat_id": chat_id
                        }
                    )
            
            if not accessible_entities:
                raise MonitoringError("No accessible chats to monitor")
            
            if not monitoring_entities:
                raise MonitoringError("No entities available for monitoring")
            
            self._monitored_chats = accessible_entities
            
            # Import events here to avoid circular imports
            from telethon import events
            
            # Register message handler с entity объектами
            @self.client.on(events.NewMessage(chats=monitoring_entities))
            async def message_handler(event):
                if self._message_handler and event.message:
                    try:
                        await self._message_handler(event.message)
                    except Exception as e:
                        logger.error(
                            f"Error in message handler: {e}",
                            extra={
                                "component": "telethon_client",
                                "chat_id": getattr(event.message, 'chat_id', 'unknown'),
                                "message_id": getattr(event.message, 'id', 'unknown'),
                                "error": str(e)
                            }
                        )
            
            self._is_monitoring = True
            logger.info(
                f"Started monitoring {len(self._monitored_chats)} chats",
                extra={
                    "component": "telethon_client",
                    "chat_count": len(self._monitored_chats),
                    "chat_ids": self._monitored_chats
                }
            )
            
        except Exception as e:
            logger.error(f"Failed to start monitoring: {e}")
            raise MonitoringError(f"Failed to start monitoring: {e}") from e
    
    async def stop_monitoring(self) -> None:
        """Stop monitoring all chats."""
        try:
            if self._is_monitoring:
                # Remove event handlers
                self.client.remove_event_handler(self._message_handler)
                self._is_monitoring = False
                self._monitored_chats.clear()
                self._message_handler = None
                
                logger.info("Stopped monitoring chats")
                
        except Exception as e:
            logger.error(f"Error stopping monitoring: {e}")
    
    async def get_chat_history(
        self,
        chat_id: int,
        since: datetime,
        limit: Optional[int] = None
    ) -> AsyncGenerator[Message, None]:
        """Get chat history since a specific datetime."""
        try:
            logger.info(
                f"Fetching history for chat {chat_id} since {since}",
                extra={
                    "component": "telethon_client",
                    "chat_id": chat_id,
                    "since": since.isoformat(),
                    "limit": limit
                }
            )
            
            # Убеждаемся, что диалоги загружены
            await self._ensure_dialogs_loaded()
            
            # Получаем entity
            entity = None
            
            # Сначала пробуем из кеша
            if chat_id in self._entity_cache:
                entity = self._entity_cache[chat_id]['entity']
                logger.debug(f"Using cached entity for history fetch: {chat_id}")
            else:
                # Fallback - прямой доступ
                try:
                    entity = await self.client.get_entity(chat_id)
                    logger.debug(f"Got entity directly for history fetch: {chat_id}")
                except Exception as e:
                    logger.error(f"Cannot get entity for history fetch: {e}")
                    raise HistoryError(f"Cannot resolve chat {chat_id} for history: {e}")
            
            if not entity:
                raise HistoryError(f"Cannot get entity for chat {chat_id}")
            
            # Convert since to offset_date
            offset_date = since.replace(tzinfo=timezone.utc) if since.tzinfo is None else since
            
            # Fetch messages
            total_fetched = 0
            
            async for message in self.client.iter_messages(
                entity,
                offset_date=offset_date,
                reverse=True,  # Start from oldest
                limit=limit
            ):
                try:
                    yield message
                    total_fetched += 1
                    
                    if limit and total_fetched >= limit:
                        break
                        
                except FloodWaitError as e:
                    logger.warning(
                        f"Flood wait: sleeping for {e.seconds} seconds",
                        extra={
                            "component": "telethon_client",
                            "chat_id": chat_id,
                            "sleep_seconds": e.seconds
                        }
                    )
                    await asyncio.sleep(e.seconds)
                    yield message
                    total_fetched += 1
            
            logger.info(
                f"Fetched {total_fetched} messages from chat {chat_id}",
                extra={
                    "component": "telethon_client",
                    "chat_id": chat_id,
                    "messages_fetched": total_fetched
                }
            )
            
        except Exception as e:
            logger.error(
                f"Failed to fetch history for chat {chat_id}: {e}",
                extra={
                    "component": "telethon_client",
                    "chat_id": chat_id,
                    "error": str(e)
                }
            )
            raise HistoryError(f"Failed to fetch history: {e}") from e
    
    def set_message_handler(self, handler: Callable[[Message], Any]) -> None:
        """Set message handler for live monitoring."""
        self._message_handler = handler
    
    def _is_group_or_supergroup(self, entity) -> bool:
        """Check if entity is a group or supergroup."""
        if isinstance(entity, Chat):
            return True
        elif isinstance(entity, Channel):
            return getattr(entity, 'megagroup', False)
        return False
    
    def _get_entity_type(self, entity) -> str:
        """Get entity type string."""
        if isinstance(entity, Channel):
            if getattr(entity, 'megagroup', False):
                return 'supergroup'
            else:
                return 'channel'
        elif isinstance(entity, Chat):
            return 'group'
        elif isinstance(entity, User):
            return 'user'
        else:
            return 'unknown'
    
    async def get_cache_stats(self) -> dict:
        """Get cache statistics for debugging."""
        await self._ensure_dialogs_loaded()
        
        return {
            "dialogs_loaded": self._dialogs_loaded,
            "cached_groups": len(self._entity_cache),
            "group_ids": list(self._entity_cache.keys()),
            "is_monitoring": self._is_monitoring,
            "monitored_chats": self._monitored_chats
        }