"""
Message mapper для конвертации Telethon messages в DTOs.
Использует правильные методы получения информации об отправителях.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from telethon.types import Message, User, Chat, Channel
from telethon.tl.types import PeerUser, PeerChat, PeerChannel
from telethon import utils

from domain.ports import MessageMapper, MappingError
from domain.dto import (
    TelegramEventDTO,
    ChatInfo,
    MessageInfo,
    SenderInfo,
    MessageContext
)


logger = logging.getLogger(__name__)


class TelethonMessageMapper(MessageMapper):

    def __init__(self, telegram_client=None):
        self.telegram_client = telegram_client
    
    async def map_message(
        self,
        message: Message,
        context: MessageContext
    ) -> Optional[TelegramEventDTO]:
        try:
            # Extract basic message info
            if not message.message or not isinstance(message.message, str):
                logger.debug(
                    f"Message has no text content",
                    extra={
                        "component": "mapper",
                        "chat_id": context.chat_id,
                        "message_id": context.message_id
                    }
                )
                return None
            
            # Map chat information
            chat_info = await self._map_chat_info(message, context)
            if not chat_info:
                logger.warning(
                    f"Failed to map chat info",
                    extra={
                        "component": "mapper",
                        "chat_id": context.chat_id,
                        "message_id": context.message_id
                    }
                )
                return None
            
            # Map message information
            message_info = self._map_message_info(message)
            
            # Map sender information - ИСПРАВЛЕНО: используем правильные методы
            sender_info = await self._map_sender_info(message, context)
            if not sender_info:
                logger.warning(
                    f"Failed to map sender info",
                    extra={
                        "component": "mapper",
                        "chat_id": context.chat_id,
                        "message_id": context.message_id
                    }
                )
                return None
            
            # Create event timestamp
            event_ts = self._get_message_timestamp(message)
            
            # Create idempotency key
            idempotency_key = TelegramEventDTO.create_idempotency_key(
                chat_info.id,
                message_info.id
            )
            
            # Create the DTO
            event_dto = TelegramEventDTO(
                schema_version="1.0",
                event_type="message_new",
                event_ts=event_ts,
                chat=chat_info,
                message=message_info,
                sender=sender_info,
                idempotency_key=idempotency_key
            )
            
            logger.debug(
                f"Successfully mapped message",
                extra={
                    "component": "mapper",
                    "chat_id": chat_info.id,
                    "message_id": message_info.id,
                    "sender_id": sender_info.id,
                    "sender_name": sender_info.display_name,
                    "text_length": len(message_info.text)
                }
            )
            
            return event_dto
            
        except Exception as e:
            logger.error(
                f"Failed to map message: {e}",
                extra={
                    "component": "mapper",
                    "chat_id": context.chat_id,
                    "message_id": context.message_id,
                    "error": str(e)
                }
            )
            raise MappingError(f"Message mapping failed: {e}") from e
    
    async def _map_chat_info(self, message: Message, context: MessageContext) -> Optional[ChatInfo]:
        """
        Map chat information from message.
        """
        try:
            # Get chat ID from peer
            chat_id = self._extract_chat_id(message)
            if chat_id is None:
                return None
            
            # Determine chat type and title
            chat_type, chat_title = await self._get_chat_details(message)
            
            return ChatInfo(
                id=chat_id,
                type=chat_type,
                title=chat_title or f"Chat {chat_id}"
            )
            
        except Exception as e:
            logger.error(f"Failed to map chat info: {e}")
            return None
    
    def _map_message_info(self, message: Message) -> MessageInfo:
        """Map message information."""
        return MessageInfo(
            id=message.id,
            text=message.message.strip()
        )
    
    async def _map_sender_info(self, message: Message, context: MessageContext) -> Optional[SenderInfo]:
        """
        Map sender information from message.
        ИСПРАВЛЕНО: Используем правильные методы как в рабочем скрипте.
        
        Args:
            message: Telethon Message object
            context: Message processing context
            
        Returns:
            SenderInfo object or None if mapping fails
        """
        try:
            sender = None
            
            # Способ 1: Попробуем message.sender (быстрый)
            if hasattr(message, 'sender') and message.sender:
                sender = message.sender
                logger.debug("Got sender from message.sender")
            
            # Способ 2: Если sender None, попробуем через telegram_client
            elif self.telegram_client and hasattr(message, 'sender_id') and message.sender_id:
                try:
                    sender = await self.telegram_client.get_entity(message.sender_id)
                    logger.debug("Got sender via telegram_client.get_entity")
                except Exception as e:
                    logger.debug(f"Failed to get sender via client: {e}")
            
            # Способ 3: Попробуем через from_id (fallback)
            elif hasattr(message, 'from_id') and message.from_id:
                try:
                    if self.telegram_client:
                        if hasattr(message.from_id, 'user_id'):
                            sender = await self.telegram_client.get_entity(message.from_id.user_id)
                        else:
                            sender = await self.telegram_client.get_entity(message.from_id)
                        logger.debug("Got sender via from_id")
                except Exception as e:
                    logger.debug(f"Failed to get sender via from_id: {e}")
            
            if not sender:
                # Последний fallback - анонимный администратор
                logger.warning(
                    f"Could not resolve sender for message",
                    extra={
                        "component": "mapper",
                        "message_id": message.id,
                        "sender_id": getattr(message, 'sender_id', None),
                        "from_id": getattr(message, 'from_id', None)
                    }
                )
                
                return SenderInfo(
                    id=getattr(message, 'sender_id', 0) or 0,
                    username=None,
                    display_name="Анонимный администратор",
                    is_bot=False
                )
            
            # Извлекаем информацию из sender
            sender_id = getattr(sender, 'id', 0)
            username = getattr(sender, 'username', None)
            is_bot = getattr(sender, 'bot', False)
            
            # ИСПРАВЛЕНО: Используем utils.get_display_name как в рабочем скрипте
            display_name = self._get_human_sender_name(sender)
            
            return SenderInfo(
                id=sender_id,
                username=username,
                display_name=display_name,
                is_bot=is_bot
            )
            
        except Exception as e:
            logger.error(
                f"Failed to map sender info: {e}",
                extra={
                    "component": "mapper",
                    "message_id": getattr(message, 'id', 'unknown'),
                    "error": str(e)
                }
            )
            return None
    
    def _get_human_sender_name(self, sender) -> str:
        try:
            if sender is None:
                return "Анонимный администратор"
            
            # utils.get_display_name сам соберёт имя/фамилию/username
            name = utils.get_display_name(sender).strip()
            if not name:
                # на всякий случай
                if getattr(sender, "username", None):
                    return f"@{sender.username}"
                return f"UserID {getattr(sender, 'id', 'unknown')}"
            
            return name
            
        except Exception as e:
            logger.error(f"Failed to get human sender name: {e}")
            return f"Unknown {getattr(sender, 'id', 'sender')}"
    
    def _extract_chat_id(self, message: Message) -> Optional[int]:
        try:
            if not message.peer_id:
                return None
            
            peer = message.peer_id
            
            if isinstance(peer, PeerChannel):
                # Channel/Supergroup - use channel_id as is (positive)
                return peer.channel_id
            elif isinstance(peer, PeerChat):
                # Regular group - use chat_id as is (positive)
                return peer.chat_id
            elif isinstance(peer, PeerUser):
                # Private chat - not supported for our use case
                return None
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to extract chat ID: {e}")
            return None
    
    async def _get_chat_details(self, message: Message) -> tuple[str, Optional[str]]:
            peer = message.peer_id
            
            if isinstance(peer, PeerChannel):
                # Try to get chat info from message
                chat_title = None
                if hasattr(message, 'chat') and message.chat:
                    chat_title = getattr(message.chat, 'title', None)
                
                # Determine if supergroup or channel
                if hasattr(message, 'chat') and hasattr(message.chat, 'megagroup'):
                    if message.chat.megagroup:
                        return "supergroup", chat_title
                    else:
                        return "channel", chat_title
                else:
                    return "supergroup", chat_title  # Default assumption
                    
            elif isinstance(peer, PeerChat):
                chat_title = None
                if hasattr(message, 'chat') and message.chat:
                    chat_title = getattr(message.chat, 'title', None)
                return "group", chat_title
            
            return "unknown", None
            
        except Exception as e:
            logger.error(f"Failed to get chat details: {e}")
            return "unknown", None
    
    def _get_message_timestamp(self, message: Message) -> datetime:
        try:
            if message.date:
                # Ensure timezone aware
                if message.date.tzinfo is None:
                    # Assume UTC if no timezone
                    return message.date.replace(tzinfo=timezone.utc)
                else:
                    # Convert to UTC
                    return message.date.astimezone(timezone.utc)
            
            # Fallback to current time
            return datetime.now(timezone.utc)
            
        except Exception as e:
            logger.error(f"Failed to get message timestamp: {e}")
            return datetime.now(timezone.utc)
    
    def set_telegram_client(self, client):
        self.telegram_client = client