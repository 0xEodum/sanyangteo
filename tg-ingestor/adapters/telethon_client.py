"""
Telethon client adapter for Telegram API operations.
Handles connection, authentication, message monitoring, and history fetching.
"""

import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, AsyncGenerator, Callable, Any

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
from telethon.tl.types import InputPeerEmpty

from ..domain.ports import TelegramClient as TelegramClientInterface, ConnectionError, MonitoringError, HistoryError
from ..domain.dto import MessageContext


logger = logging.getLogger(__name__)


class TelethonClientAdapter(TelegramClientInterface):
    """
    Telethon-based implementation of TelegramClient interface.
    Handles authentication, monitoring, and message fetching.
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
        """
        Initialize Telethon client adapter.
        
        Args:
            api_id: Telegram API ID
            api_hash: Telegram API hash
            session_path: Path to session file
            device_model: Device model for Telegram session
            system_version: System version for Telegram session
            app_version: App version for Telegram session
            flood_sleep_threshold: Automatic sleep threshold for flood wait
        """
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
    
    async def connect(self) -> None:
        """
        Connect to Telegram and authenticate.
        
        Raises:
            ConnectionError: If connection or authentication fails
        """
        try:
            logger.info("Connecting to Telegram...")
            
            # Connect to Telegram
            await self.client.connect()
            
            # Check if already authorized
            if not await self.client.is_user_authorized():
                logger.error("Session is not authorized. Please run authentication setup first.")
                raise ConnectionError(
                    "Telegram session not authorized. Please authenticate manually first."
                )
            
            # Get current user info
            me = await self.client.get_me()
            logger.info(
                f"Connected to Telegram as {me.first_name} (@{me.username})",
                extra={
                    "component": "telethon_client",
                    "user_id": me.id,
                    "username": me.username
                }
            )
            
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
    
    async def start_monitoring(self, chat_ids: List[int]) -> None:
        """
        Start monitoring specified chats for new messages.
        
        Args:
            chat_ids: List of chat IDs to monitor
            
        Raises:
            MonitoringError: If monitoring setup fails
        """
        try:
            if self._is_monitoring:
                logger.warning("Already monitoring, stopping previous monitoring")
                await self.stop_monitoring()
            
            self._monitored_chats = chat_ids.copy()
            
            # Validate chat access
            accessible_chats = []
            for chat_id in chat_ids:
                try:
                    entity = await self.client.get_entity(chat_id)
                    accessible_chats.append(chat_id)
                    logger.info(
                        f"Monitoring chat: {getattr(entity, 'title', 'Unknown')} ({chat_id})",
                        extra={
                            "component": "telethon_client",
                            "chat_id": chat_id,
                            "chat_title": getattr(entity, 'title', 'Unknown')
                        }
                    )
                except Exception as e:
                    logger.error(
                        f"Cannot access chat {chat_id}: {e}",
                        extra={
                            "component": "telethon_client",
                            "chat_id": chat_id,
                            "error": str(e)
                        }
                    )
            
            if not accessible_chats:
                raise MonitoringError("No accessible chats to monitor")
            
            self._monitored_chats = accessible_chats
            
            # Register message handler
            @self.client.on(events.NewMessage(chats=self._monitored_chats))
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
            
            # Import events here to avoid circular imports
            from telethon import events
            
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
        """
        Get chat history since a specific datetime.
        
        Args:
            chat_id: Chat ID to fetch history from
            since: Fetch messages since this datetime
            limit: Maximum number of messages to fetch
            
        Yields:
            Telethon Message objects
            
        Raises:
            HistoryError: If fetching history fails
        """
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
            
            # Get chat entity
            entity = await self.client.get_entity(chat_id)
            
            # Convert since to offset_date
            offset_date = since.replace(tzinfo=timezone.utc) if since.tzinfo is None else since
            
            # Fetch messages in batches
            batch_size = min(100, limit) if limit else 100
            total_fetched = 0
            
            async for message in self.client.iter_messages(
                entity,
                offset_date=offset_date,
                reverse=True,  # Start from oldest
                limit=limit
            ):
                # Handle flood wait
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
    
    async def resolve_chat(self, chat_id: int) -> Optional[dict]:
        """
        Resolve chat information by ID.
        
        Args:
            chat_id: Chat ID to resolve
            
        Returns:
            Chat information dict or None if not found/accessible
        """
        try:
            entity = await self.client.get_entity(chat_id)
            
            # Extract basic info
            chat_info = {
                "id": chat_id,
                "title": getattr(entity, 'title', f'Chat {chat_id}'),
                "type": self._get_entity_type(entity),
                "accessible": True
            }
            
            # Add additional info if available
            if hasattr(entity, 'username') and entity.username:
                chat_info["username"] = entity.username
            
            if hasattr(entity, 'participants_count'):
                chat_info["participants_count"] = entity.participants_count
            
            return chat_info
            
        except Exception as e:
            logger.warning(
                f"Failed to resolve chat {chat_id}: {e}",
                extra={
                    "component": "telethon_client",
                    "chat_id": chat_id,
                    "error": str(e)
                }
            )
            return None
    
    def set_message_handler(self, handler: Callable[[Message], Any]) -> None:
        """
        Set message handler for live monitoring.
        
        Args:
            handler: Async function to handle incoming messages
        """
        self._message_handler = handler
    
    def _get_entity_type(self, entity) -> str:
        """
        Get entity type string.
        
        Args:
            entity: Telethon entity object
            
        Returns:
            Entity type string
        """
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


async def authenticate_session(
    api_id: int,
    api_hash: str,
    session_path: str,
    phone_number: str
) -> None:
    """
    Helper function to authenticate a new session interactively.
    This should be run separately before starting the service.
    
    Args:
        api_id: Telegram API ID
        api_hash: Telegram API hash
        session_path: Path to session file
        phone_number: Phone number for authentication
    """
    client = TelegramClient(session_path, api_id, api_hash)
    
    try:
        await client.connect()
        
        if not await client.is_user_authorized():
            print("Starting authentication process...")
            
            # Send code request
            await client.send_code_request(phone_number)
            code = input("Enter the code you received: ")
            
            try:
                await client.sign_in(phone_number, code)
            except SessionPasswordNeededError:
                password = input("Two-factor authentication enabled. Enter password: ")
                await client.sign_in(password=password)
            
            print("Authentication successful!")
        else:
            print("Already authenticated!")
            
    except Exception as e:
        print(f"Authentication failed: {e}")
        raise
    finally:
        await client.disconnect()