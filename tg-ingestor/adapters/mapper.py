"""
Message mapper for converting Telethon messages to DTOs.
Handles the transformation from Telegram API objects to internal event format.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from telethon.types import Message, User, Chat, Channel
from telethon.tl.types import PeerUser, PeerChat, PeerChannel

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
    Maps Telethon Message objects to TelegramEventDTO.
    Handles various Telegram entity types and edge cases.
    """
    
    def __init__(self):
        """Initialize the mapper."""
        pass
    
    async def map_message(
        self,
        message: Message,
        context: MessageContext
    ) -> Optional[TelegramEventDTO]:
        """
        Map a Telethon Message to TelegramEventDTO.
        
        Args:
            message: Telethon Message object
            context: Message processing context
            
        Returns:
            Mapped TelegramEventDTO or None if mapping fails
            
        Raises:
            MappingError: If critical mapping error occurs
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
            
            # Map sender information
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
        
        Args:
            message: Telethon Message object
            context: Message processing context
            
        Returns:
            ChatInfo object or None if mapping fails
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
        """
        Map message information.
        
        Args:
            message: Telethon Message object
            
        Returns:
            MessageInfo object
        """
        return MessageInfo(
            id=message.id,
            text=message.message.strip()
        )
    
    async def _map_sender_info(self, message: Message, context: MessageContext) -> Optional[SenderInfo]:
        """
        Map sender information from message.
        
        Args:
            message: Telethon Message object
            context: Message processing context
            
        Returns:
            SenderInfo object or None if mapping fails
        """
        try:
            if not message.sender:
                logger.warning(f"Message has no sender")
                return None
            
            sender = message.sender
            
            # Extract basic info
            sender_id = sender.id
            username = getattr(sender, 'username', None)
            is_bot = getattr(sender, 'bot', False)
            
            # Build display name
            display_name = self._build_display_name(sender)
            
            return SenderInfo(
                id=sender_id,
                username=username,
                display_name=display_name,
                is_bot=is_bot
            )
            
        except Exception as e:
            logger.error(f"Failed to map sender info: {e}")
            return None
    
    def _extract_chat_id(self, message: Message) -> Optional[int]:
        """
        Extract chat ID from message peer (MTProto format - positive numbers).
        
        Args:
            message: Telethon Message object
            
        Returns:
            Chat ID or None if extraction fails
        """
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
        """
        Get chat type and title.
        
        Args:
            message: Telethon Message object
            
        Returns:
            Tuple of (chat_type, chat_title)
        """
        try:
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
    
    def _build_display_name(self, sender) -> str:
        """
        Build display name for sender.
        
        Args:
            sender: Telethon User/Chat object
            
        Returns:
            Display name string
        """
        try:
            if isinstance(sender, User):
                # Build name from first_name and last_name
                parts = []
                if sender.first_name:
                    parts.append(sender.first_name)
                if sender.last_name:
                    parts.append(sender.last_name)
                
                if parts:
                    return " ".join(parts)
                elif sender.username:
                    return f"@{sender.username}"
                else:
                    return f"User {sender.id}"
            
            # For other types, use title or ID
            if hasattr(sender, 'title') and sender.title:
                return sender.title
            
            return f"Sender {sender.id}"
            
        except Exception as e:
            logger.error(f"Failed to build display name: {e}")
            return f"Unknown {getattr(sender, 'id', 'sender')}"
    
    def _get_message_timestamp(self, message: Message) -> datetime:
        """
        Get message timestamp in UTC.
        
        Args:
            message: Telethon Message object
            
        Returns:
            UTC datetime
        """
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