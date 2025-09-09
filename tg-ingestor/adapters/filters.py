"""
Message filters for tg-ingestor service.
Implements filtering logic to process only relevant messages.
"""

import logging
from typing import Optional, List

from telethon.types import (
    Message, 
    MessageService,
    Chat,
    Channel,
    User,
    MessageActionChatJoinedByLink,
    MessageActionChatAddUser,
    MessageActionChatDeleteUser,
    MessageActionChatEditTitle,
    MessageActionChatEditPhoto,
    MessageActionChannelCreate,
    MessageActionChatMigrateTo,
    MessageActionChannelMigrateFrom
)

from ..domain.ports import MessageFilter


logger = logging.getLogger(__name__)


class TelegramMessageFilter(MessageFilter):
    """
    Filter implementation for Telegram messages using Telethon types.
    
    Filters out:
    - Service messages (joins, leaves, title changes, etc.)
    - Messages without text content
    - Messages from non-group chats
    - Bot messages (optionally)
    """
    
    def __init__(
        self,
        allow_bots: bool = True,
        max_text_length: int = 16384,
        min_text_length: int = 1
    ):
        """
        Initialize message filter.
        
        Args:
            allow_bots: Whether to allow messages from bots
            max_text_length: Maximum allowed text length
            min_text_length: Minimum required text length
        """
        self.allow_bots = allow_bots
        self.max_text_length = max_text_length
        self.min_text_length = min_text_length
    
    def should_process(self, message: Message) -> bool:
        """
        Determine if a message should be processed.
        
        Args:
            message: Telethon Message object
            
        Returns:
            True if message should be processed, False otherwise
        """
        # Quick check - must be a Message instance
        if not isinstance(message, Message):
            return False
        
        # Filter out service messages
        if isinstance(message, MessageService) or message.action is not None:
            return False
        
        # Must have text content
        if not message.message or not isinstance(message.message, str):
            return False
        
        # Check text length
        text_length = len(message.message.strip())
        if text_length < self.min_text_length or text_length > self.max_text_length:
            return False
        
        # Check if sender is bot (if filtering enabled)
        if not self.allow_bots and message.sender and getattr(message.sender, 'bot', False):
            return False
        
        # Check if message is from a group/supergroup
        if not self._is_group_message(message):
            return False
        
        return True
    
    def get_filter_reason(self, message: Message) -> Optional[str]:
        """
        Get reason why a message was filtered out.
        
        Args:
            message: Telethon Message object
            
        Returns:
            Filter reason string or None if message passed filters
        """
        if not isinstance(message, Message):
            return "not_message_type"
        
        if isinstance(message, MessageService) or message.action is not None:
            return "service_message"
        
        if not message.message or not isinstance(message.message, str):
            return "no_text_content"
        
        text_length = len(message.message.strip()) if message.message else 0
        if text_length < self.min_text_length:
            return f"text_too_short_{text_length}"
        
        if text_length > self.max_text_length:
            return f"text_too_long_{text_length}"
        
        if not self.allow_bots and message.sender and getattr(message.sender, 'bot', False):
            return "bot_message"
        
        if not self._is_group_message(message):
            return "not_group_message"
        
        return None  # Message passed all filters
    
    def _is_group_message(self, message: Message) -> bool:
        """
        Check if message is from a group or supergroup.
        
        Args:
            message: Telethon Message object
            
        Returns:
            True if from group/supergroup, False otherwise
        """
        # Check peer type - should be negative for groups/supergroups
        if message.peer_id and hasattr(message.peer_id, 'channel_id'):
            # Supergroup/channel
            return True
        elif message.peer_id and hasattr(message.peer_id, 'chat_id'):
            # Regular group
            return True
        
        return False


class CompositeMessageFilter(MessageFilter):
    """
    Composite filter that applies multiple filters in sequence.
    Message must pass all filters to be accepted.
    """
    
    def __init__(self, filters: List[MessageFilter]):
        """
        Initialize composite filter.
        
        Args:
            filters: List of filters to apply
        """
        self.filters = filters
    
    def should_process(self, message: Message) -> bool:
        """
        Check if message passes all filters.
        
        Args:
            message: Message to check
            
        Returns:
            True if passes all filters, False otherwise
        """
        for filter_instance in self.filters:
            if not filter_instance.should_process(message):
                return False
        return True
    
    def get_filter_reason(self, message: Message) -> Optional[str]:
        """
        Get reason from first filter that rejects the message.
        
        Args:
            message: Message to check
            
        Returns:
            First filter reason or None if all pass
        """
        for i, filter_instance in enumerate(self.filters):
            reason = filter_instance.get_filter_reason(message)
            if reason:
                return f"filter_{i}_{reason}"
        return None


class ChatTypeFilter(MessageFilter):
    """
    Filter that only allows messages from specific chat types.
    """
    
    def __init__(self, allowed_types: List[str]):
        """
        Initialize chat type filter.
        
        Args:
            allowed_types: List of allowed chat types ('group', 'supergroup', 'channel')
        """
        self.allowed_types = set(allowed_types)
    
    def should_process(self, message: Message) -> bool:
        """
        Check if message is from allowed chat type.
        
        Args:
            message: Message to check
            
        Returns:
            True if from allowed chat type, False otherwise
        """
        chat_type = self._get_chat_type(message)
        return chat_type in self.allowed_types
    
    def get_filter_reason(self, message: Message) -> Optional[str]:
        """
        Get filter reason for chat type.
        
        Args:
            message: Message to check
            
        Returns:
            Filter reason or None if allowed
        """
        chat_type = self._get_chat_type(message)
        if chat_type not in self.allowed_types:
            return f"chat_type_{chat_type}_not_allowed"
        return None
    
    def _get_chat_type(self, message: Message) -> str:
        """
        Determine chat type from message.
        
        Args:
            message: Message to analyze
            
        Returns:
            Chat type string
        """
        if message.peer_id and hasattr(message.peer_id, 'channel_id'):
            # Could be supergroup or channel
            if message.peer_id.channel_id:
                # Check if it's a supergroup (negative ID) or channel
                if str(message.peer_id.channel_id).startswith('-100'):
                    return 'supergroup'
                else:
                    return 'channel'
        elif message.peer_id and hasattr(message.peer_id, 'chat_id'):
            return 'group'
        elif message.peer_id and hasattr(message.peer_id, 'user_id'):
            return 'private'
        
        return 'unknown'


class ServiceMessageFilter(MessageFilter):
    """
    Filter that blocks specific types of service messages.
    """
    
    # Service actions to filter out
    FILTERED_ACTIONS = {
        MessageActionChatJoinedByLink,
        MessageActionChatAddUser,
        MessageActionChatDeleteUser,
        MessageActionChatEditTitle,
        MessageActionChatEditPhoto,
        MessageActionChannelCreate,
        MessageActionChatMigrateTo,
        MessageActionChannelMigrateFrom
    }
    
    def should_process(self, message: Message) -> bool:
        """
        Check if message is not a filtered service message.
        
        Args:
            message: Message to check
            
        Returns:
            True if not a filtered service message, False otherwise
        """
        if isinstance(message, MessageService):
            return False
        
        if message.action:
            action_type = type(message.action)
            return action_type not in self.FILTERED_ACTIONS
        
        return True
    
    def get_filter_reason(self, message: Message) -> Optional[str]:
        """
        Get service message filter reason.
        
        Args:
            message: Message to check
            
        Returns:
            Filter reason or None if allowed
        """
        if isinstance(message, MessageService):
            return "service_message"
        
        if message.action:
            action_type = type(message.action)
            if action_type in self.FILTERED_ACTIONS:
                return f"service_action_{action_type.__name__}"
        
        return None


def create_default_filter(
    allow_bots: bool = True,
    max_text_length: int = 16384
) -> MessageFilter:
    """
    Create default message filter configuration.
    
    Args:
        allow_bots: Whether to allow bot messages
        max_text_length: Maximum text length
        
    Returns:
        Configured composite filter
    """
    filters = [
        # Only allow groups and supergroups
        ChatTypeFilter(['group', 'supergroup']),
        
        # Filter out service messages
        ServiceMessageFilter(),
        
        # Main telegram filter
        TelegramMessageFilter(
            allow_bots=allow_bots,
            max_text_length=max_text_length
        )
    ]
    
    return CompositeMessageFilter(filters)