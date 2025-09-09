"""
Chat resolver for validating and managing chat configurations.
Ensures that configured chats are accessible and of the correct type.
"""

import logging
from typing import List, Optional, Dict, Set

from domain.ports import ChatResolver, TelegramClient, ValidationError
from domain.dto import ChatConfig


logger = logging.getLogger(__name__)


class TelegramChatResolver(ChatResolver):
    """
    Resolves and validates Telegram chat configurations.
    Ensures chats are accessible, of correct type, and properly configured.
    """
    
    def __init__(
        self,
        telegram_client: TelegramClient,
        allowed_chat_types: Optional[Set[str]] = None,
        validate_access: bool = True
    ):
        """
        Initialize chat resolver.
        
        Args:
            telegram_client: Telegram client for chat resolution
            allowed_chat_types: Set of allowed chat types ('group', 'supergroup', 'channel')
            validate_access: Whether to validate chat access during validation
        """
        self.telegram_client = telegram_client
        self.allowed_chat_types = allowed_chat_types or {'group', 'supergroup'}
        self.validate_access = validate_access
        
        # Cache for resolved chat info
        self._chat_cache: Dict[int, dict] = {}
    
    async def validate_chats(self, chat_configs: List[ChatConfig]) -> List[ChatConfig]:
        """
        Validate that chats are accessible and of correct type.
        
        Args:
            chat_configs: List of chat configurations to validate
            
        Returns:
            List of validated chat configurations (may be filtered)
            
        Raises:
            ValidationError: If validation fails completely
        """
        if not chat_configs:
            logger.warning("No chat configurations provided")
            return []
        
        validated_chats = []
        validation_errors = []
        
        logger.info(
            f"Validating {len(chat_configs)} chat configurations",
            extra={
                "component": "chat_resolver",
                "total_chats": len(chat_configs),
                "allowed_types": list(self.allowed_chat_types)
            }
        )
        
        for chat_config in chat_configs:
            try:
                if not chat_config.enabled:
                    logger.debug(
                        f"Skipping disabled chat {chat_config.id}",
                        extra={
                            "component": "chat_resolver",
                            "chat_id": chat_config.id
                        }
                    )
                    continue
                
                # Validate chat ID format
                if not self._is_valid_chat_id(chat_config.id):
                    error_msg = f"Invalid chat ID format: {chat_config.id}"
                    logger.warning(error_msg)
                    validation_errors.append(error_msg)
                    continue
                
                # Resolve chat info
                chat_info = await self.get_chat_info(chat_config.id)
                
                if not chat_info:
                    error_msg = f"Cannot access chat {chat_config.id}"
                    logger.warning(
                        error_msg,
                        extra={
                            "component": "chat_resolver",
                            "chat_id": chat_config.id
                        }
                    )
                    validation_errors.append(error_msg)
                    continue
                
                # Validate chat type
                chat_type = chat_info.get('type', 'unknown')
                if chat_type not in self.allowed_chat_types:
                    error_msg = f"Chat {chat_config.id} type '{chat_type}' not allowed"
                    logger.warning(
                        error_msg,
                        extra={
                            "component": "chat_resolver",
                            "chat_id": chat_config.id,
                            "chat_type": chat_type,
                            "allowed_types": list(self.allowed_chat_types)
                        }
                    )
                    validation_errors.append(error_msg)
                    continue
                
                # Update chat config with resolved info
                validated_config = ChatConfig(
                    id=chat_config.id,
                    enabled=chat_config.enabled,
                    title=chat_info.get('title', chat_config.title)
                )
                
                validated_chats.append(validated_config)
                
                logger.info(
                    f"Validated chat: {chat_info.get('title', 'Unknown')} ({chat_config.id})",
                    extra={
                        "component": "chat_resolver",
                        "chat_id": chat_config.id,
                        "chat_title": chat_info.get('title'),
                        "chat_type": chat_type
                    }
                )
                
            except Exception as e:
                error_msg = f"Failed to validate chat {chat_config.id}: {e}"
                logger.error(
                    error_msg,
                    extra={
                        "component": "chat_resolver",
                        "chat_id": chat_config.id,
                        "error": str(e)
                    }
                )
                validation_errors.append(error_msg)
        
        # Check if we have any valid chats
        if not validated_chats:
            error_summary = "; ".join(validation_errors[:5])  # Limit error details
            if len(validation_errors) > 5:
                error_summary += f" (and {len(validation_errors) - 5} more)"
            
            raise ValidationError(
                f"No valid chats found. Errors: {error_summary}"
            )
        
        # Log validation summary
        if validation_errors:
            logger.warning(
                f"Chat validation completed with {len(validation_errors)} errors",
                extra={
                    "component": "chat_resolver",
                    "validated_count": len(validated_chats),
                    "error_count": len(validation_errors),
                    "total_configs": len(chat_configs)
                }
            )
        else:
            logger.info(
                f"All {len(validated_chats)} chats validated successfully",
                extra={
                    "component": "chat_resolver",
                    "validated_count": len(validated_chats)
                }
            )
        
        return validated_chats
    
    async def get_chat_info(self, chat_id: int) -> Optional[dict]:
        """
        Get detailed information about a chat.
        
        Args:
            chat_id: Chat ID to get info for
            
        Returns:
            Chat information dict or None if not accessible
        """
        # Check cache first
        if chat_id in self._chat_cache:
            logger.debug(
                f"Using cached info for chat {chat_id}",
                extra={
                    "component": "chat_resolver",
                    "chat_id": chat_id
                }
            )
            return self._chat_cache[chat_id]
        
        try:
            # Resolve through Telegram client
            chat_info = await self.telegram_client.resolve_chat(chat_id)
            
            if chat_info:
                # Enhance with additional validation info
                enhanced_info = {
                    **chat_info,
                    "validation_timestamp": "timestamp",  # In real app, use actual timestamp
                    "validation_status": "valid"
                }
                
                # Cache the result
                self._chat_cache[chat_id] = enhanced_info
                
                logger.debug(
                    f"Resolved chat info for {chat_id}",
                    extra={
                        "component": "chat_resolver",
                        "chat_id": chat_id,
                        "chat_title": chat_info.get('title'),
                        "chat_type": chat_info.get('type')
                    }
                )
                
                return enhanced_info
            
            return None
            
        except Exception as e:
            logger.error(
                f"Failed to get chat info for {chat_id}: {e}",
                extra={
                    "component": "chat_resolver",
                    "chat_id": chat_id,
                    "error": str(e)
                }
            )
            return None
    
    def _is_valid_chat_id(self, chat_id: int) -> bool:
        """
        Validate chat ID format for Telethon (MTProto).
        
        Args:
            chat_id: Chat ID to validate
            
        Returns:
            True if valid format, False otherwise
        """
        # Telethon (MTProto) chat ID rules:
        # - Groups and supergroups: positive numbers 
        # - Reasonable range for Telegram IDs
        
        if chat_id <= 0:
            # In MTProto format, chat IDs are positive
            return False
        
        # Check for reasonable range
        # Telegram IDs are typically in range 1 to 999999999999
        if chat_id > 999999999999:
            return False
        
        return True
    
    def clear_cache(self) -> None:
        """Clear the chat info cache."""
        self._chat_cache.clear()
        logger.debug(
            "Chat info cache cleared",
            extra={"component": "chat_resolver"}
        )
    
    def get_cache_stats(self) -> dict:
        """
        Get cache statistics.
        
        Returns:
            Dictionary with cache statistics
        """
        return {
            "cached_chats": len(self._chat_cache),
            "chat_ids": list(self._chat_cache.keys())
        }
    
    async def refresh_chat_info(self, chat_id: int) -> Optional[dict]:
        """
        Force refresh chat info from Telegram API.
        
        Args:
            chat_id: Chat ID to refresh
            
        Returns:
            Updated chat info or None if not accessible
        """
        # Remove from cache to force refresh
        self._chat_cache.pop(chat_id, None)
        
        logger.debug(
            f"Refreshing chat info for {chat_id}",
            extra={
                "component": "chat_resolver",
                "chat_id": chat_id
            }
        )
        
        return await self.get_chat_info(chat_id)
    
    async def validate_single_chat(self, chat_id: int) -> bool:
        """
        Validate a single chat ID.
        
        Args:
            chat_id: Chat ID to validate
            
        Returns:
            True if valid and accessible, False otherwise
        """
        try:
            chat_config = ChatConfig(id=chat_id, enabled=True)
            validated = await self.validate_chats([chat_config])
            return len(validated) > 0
            
        except Exception as e:
            logger.error(
                f"Single chat validation failed for {chat_id}: {e}",
                extra={
                    "component": "chat_resolver",
                    "chat_id": chat_id,
                    "error": str(e)
                }
            )
            return False