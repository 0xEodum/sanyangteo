"""
Message mapper для конвертации Telethon messages в DTOs.
Исправленная версия с использованием подходов из рабочего скрипта.
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
    """
    Исправленный mapper с использованием надежных методов получения chat и sender.
    """

    def __init__(self, telegram_client=None):
        self.telegram_client = telegram_client
    
    async def map_message(
        self,
        message: Message,
        context: MessageContext
    ) -> Optional[TelegramEventDTO]:
        """
        Map Telethon message to DTO using reliable methods.
        """
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
            
            # Map chat information - ИСПРАВЛЕНО: используем надежный метод
            chat_info = await self._map_chat_info_fixed(message, context)
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
            
            # Map sender information - ИСПРАВЛЕНО: используем надежный метод
            sender_info = await self._map_sender_info_fixed(message, context)
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
                    "chat_title": chat_info.title,
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
    
    async def _map_chat_info_fixed(self, message: Message, context: MessageContext) -> Optional[ChatInfo]:
        """
        ИСПРАВЛЕНО: Используем прямые методы получения chat как в рабочем скрипте.
        """
        try:
            chat = None
            
            # Способ 1: Используем get_chat() как в рабочем скрипте
            if hasattr(message, 'get_chat'):
                try:
                    chat = await message.get_chat()
                    logger.debug("Got chat via message.get_chat()")
                except Exception as e:
                    logger.debug(f"message.get_chat() failed: {e}")
            
            # Способ 2: Через telegram_client если get_chat не сработал
            if not chat and self.telegram_client:
                try:
                    chat_id = self._extract_chat_id_from_peer(message)
                    if chat_id:
                        chat = await self.telegram_client.get_entity(chat_id)
                        logger.debug("Got chat via telegram_client.get_entity")
                except Exception as e:
                    logger.debug(f"Failed to get chat via client: {e}")
            
            if not chat:
                logger.warning(f"Could not resolve chat for message {message.id}")
                return None
            
            # Извлекаем информацию из chat entity
            chat_id = getattr(chat, 'id', 0)
            chat_title = getattr(chat, 'title', f'Chat {chat_id}')
            
            # Определяем тип чата как в рабочем скрипте
            if isinstance(chat, Chat):
                chat_type = "group"
            elif isinstance(chat, Channel):
                if getattr(chat, 'megagroup', False):
                    chat_type = "supergroup"
                else:
                    chat_type = "channel"
            else:
                chat_type = "unknown"
            
            return ChatInfo(
                id=chat_id,
                type=chat_type,
                title=chat_title
            )
            
        except Exception as e:
            logger.error(f"Failed to map chat info: {e}")
            return None
    
    async def _map_sender_info_fixed(self, message: Message, context: MessageContext) -> Optional[SenderInfo]:
        """
        ИСПРАВЛЕНО: Используем прямые методы получения sender как в рабочем скрипте.
        """
        try:
            sender = None
            
            # Способ 1: Используем get_sender() как в рабочем скрипте
            if hasattr(message, 'get_sender'):
                try:
                    sender = await message.get_sender()
                    logger.debug("Got sender via message.get_sender()")
                except Exception as e:
                    logger.debug(f"message.get_sender() failed: {e}")
            
            # Способ 2: Fallback через message.sender
            if not sender and hasattr(message, 'sender') and message.sender:
                sender = message.sender
                logger.debug("Got sender from message.sender")
            
            # Способ 3: Через telegram_client
            if not sender and self.telegram_client and hasattr(message, 'sender_id') and message.sender_id:
                try:
                    sender = await self.telegram_client.get_entity(message.sender_id)
                    logger.debug("Got sender via telegram_client.get_entity")
                except Exception as e:
                    logger.debug(f"Failed to get sender via client: {e}")
            
            if not sender:
                # Fallback - анонимный администратор
                logger.warning(
                    f"Could not resolve sender for message",
                    extra={
                        "component": "mapper",
                        "message_id": message.id,
                        "sender_id": getattr(message, 'sender_id', None)
                    }
                )
                
                return SenderInfo(
                    id=getattr(message, 'sender_id', 0) or 0,
                    username=None,
                    display_name="Анонимный администратор",
                    is_bot=False
                )
            
            # Извлекаем информацию из sender entity
            sender_id = getattr(sender, 'id', 0)
            username = getattr(sender, 'username', None)
            is_bot = getattr(sender, 'bot', False)
            
            # ИСПРАВЛЕНО: Используем utils.get_display_name как в рабочем скрипте
            display_name = self._get_human_sender_name_fixed(sender)
            
            return SenderInfo(
                id=sender_id,
                username=username,
                display_name=display_name,
                is_bot=is_bot
            )
            
        except Exception as e:
            logger.error(f"Failed to map sender info: {e}")
            return None
    
    def _get_human_sender_name_fixed(self, sender) -> str:
        """
        ИСПРАВЛЕНО: Точно такая же логика как в рабочем скрипте.
        """
        try:
            if sender is None:
                return "Анонимный администратор"
            
            # Используем utils.get_display_name точно как в рабочем скрипте
            name = utils.get_display_name(sender).strip()
            if not name:
                if getattr(sender, "username", None):
                    return f"@{sender.username}"
                return f"UserID {getattr(sender, 'id', 'unknown')}"
            
            return name
            
        except Exception as e:
            logger.error(f"Failed to get human sender name: {e}")
            return f"Unknown {getattr(sender, 'id', 'sender')}"
    
    def _extract_chat_id_from_peer(self, message: Message) -> Optional[int]:
        """
        Извлекаем chat_id из peer_id для fallback случаев.
        """
        try:
            if not message.peer_id:
                return None
            
            peer = message.peer_id
            
            if isinstance(peer, PeerChannel):
                return peer.channel_id
            elif isinstance(peer, PeerChat):
                return peer.chat_id
            elif isinstance(peer, PeerUser):
                return None  # Private chat - not supported
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to extract chat ID from peer: {e}")
            return None
    
    def _map_message_info(self, message: Message) -> MessageInfo:
        """Map message information."""
        return MessageInfo(
            id=message.id,
            text=message.message.strip()
        )
    
    def _get_message_timestamp(self, message: Message) -> datetime:
        """Get message timestamp with timezone handling."""
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
        """Set telegram client for entity resolution."""
        self.telegram_client = client