"""
Configuration management for tg-ingestor service - CLEAN VERSION
Убраны все legacy и compatibility части
"""

import os
import logging
from pathlib import Path
from typing import List, Optional, Dict, Any

import yaml
from pydantic import BaseModel, Field, validator, ConfigDict


logger = logging.getLogger(__name__)


class TelegramConfig(BaseModel):
    """Telegram API configuration."""
    model_config = ConfigDict(extra='forbid')
    
    session: str = Field(
        default="/app/data/session.session",
        description="Path to Telethon session file"
    )
    api_id: int = Field(
        ...,
        description="Telegram API ID from https://my.telegram.org"
    )
    api_hash: str = Field(
        ..., 
        min_length=32,
        max_length=32,
        description="Telegram API hash from https://my.telegram.org"
    )
    device_model: str = Field(
        default="tg-ingestor",
        description="Device model for Telegram session"
    )
    system_version: str = Field(
        default="1.0",
        description="System version for Telegram session"
    )
    app_version: str = Field(
        default="1.0",
        description="App version for Telegram session"
    )
    flood_sleep_threshold: int = Field(
        default=60,
        ge=10,
        le=300,
        description="Automatic sleep threshold for flood wait (seconds)"
    )


class ChatsConfig(BaseModel):
    """Chat monitoring configuration."""
    model_config = ConfigDict(extra='forbid')
    
    ids: List[int] = Field(
        ...,
        min_items=1,
        description="List of chat IDs to monitor (positive for MTProto format)"
    )
    
    @validator('ids')
    def validate_chat_ids(cls, v):
        """Validate chat IDs format for Telethon (MTProto)."""
        for chat_id in v:
            if chat_id <= 0:
                raise ValueError(
                    f"Chat ID {chat_id} should be positive for Telethon (MTProto format). "
                    f"Use the ID from your list_dialogs.py script directly."
                )
            if chat_id > 999999999999:  # Very large number sanity check
                logger.warning(f"Chat ID {chat_id} seems unusually large, double-check it")
        return v


class BootstrapConfig(BaseModel):
    """Bootstrap configuration for fetching historical messages."""
    model_config = ConfigDict(extra='forbid')
    
    lookback_hours: int = Field(
        default=12,
        ge=0,
        le=168,  # 7 days max
        description="Hours to look back for historical messages during startup"
    )
    enabled: bool = Field(
        default=True,
        description="Whether to perform bootstrap on startup"
    )
    max_messages_per_chat: Optional[int] = Field(
        default=None,
        ge=1,
        le=10000,
        description="Maximum messages to fetch per chat during bootstrap"
    )


class QueueConfig(BaseModel):
    """Queue configuration using shared library."""
    model_config = ConfigDict(extra='forbid')
    
    type: str = Field(
        default="redis",
        description="Queue type: redis"
    )
    queue_name: str = Field(
        default="telegram_events",
        description="Name of the queue for telegram events"
    )
    config: Dict[str, Any] = Field(
        default_factory=lambda: {
            "url": "redis://redis:6379/0",
            "dedup_ttl_seconds": 604800,
            "maxlen_approx": 1000000,
            "max_connections": 10,
            "socket_timeout": 5.0
        },
        description="Redis-specific configuration"
    )
    
    @validator('type')
    def validate_queue_type(cls, v):
        """Validate queue type."""
        if v.lower() != "redis":
            raise ValueError("Only 'redis' queue type is supported in this version")
        return v.lower()


class ProcessingConfig(BaseModel):
    """Message processing configuration."""
    model_config = ConfigDict(extra='forbid')
    
    max_concurrent_messages: int = Field(
        default=10,
        ge=1,
        le=50,
        description="Maximum concurrent message processing"
    )
    allow_bots: bool = Field(
        default=True,
        description="Whether to process messages from bots"
    )
    max_text_length: int = Field(
        default=16384,
        ge=1,
        le=65536,
        description="Maximum allowed message text length"
    )
    min_text_length: int = Field(
        default=1,
        ge=1,
        le=100,
        description="Minimum required message text length"
    )


class LoggingConfig(BaseModel):
    """Logging configuration."""
    model_config = ConfigDict(extra='forbid')

    level: str = Field(
        default="INFO",
        description="Logging level"
    )
    json_format: bool = Field(
        default=True,
        description="Enable JSON log formatting"
    )
    enable_correlation: bool = Field(
        default=True,
        description="Enable correlation IDs in logs"
    )
    
    @validator('level')
    def validate_log_level(cls, v):
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if v.upper() not in valid_levels:
            raise ValueError(f"Invalid log level. Must be one of: {valid_levels}")
        return v.upper()


class SecurityConfig(BaseModel):
    """Security configuration for inter-service communication."""

    model_config = ConfigDict(extra='forbid')

    shared_token: Optional[str] = Field(
        default=None,
        description="Shared authentication token loaded from secrets or file"
    )
    shared_token_file: Optional[str] = Field(
        default=None,
        description="Path to file containing shared authentication token"
    )

    def get_token(self) -> Optional[str]:
        """Return the loaded shared token if available."""
        return self.shared_token


class AppConfig(BaseModel):
    """Main application configuration."""
    model_config = ConfigDict(extra='forbid')

    telegram: TelegramConfig = Field(..., description="Telegram API configuration")
    chats: ChatsConfig = Field(..., description="Chat monitoring configuration")
    bootstrap: BootstrapConfig = Field(default_factory=BootstrapConfig)
    queue: QueueConfig = Field(default_factory=QueueConfig)
    processing: ProcessingConfig = Field(default_factory=ProcessingConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    security: SecurityConfig = Field(default_factory=SecurityConfig)


def load_config(config_path: Optional[str] = None) -> AppConfig:
    """
    Load configuration from YAML file with environment variable overrides.
    """
    # Determine config file path
    if config_path is None:
        config_path = os.getenv('CONFIG_PATH', './config.yml')
    
    config_file = Path(config_path)
    
    if not config_file.exists():
        raise FileNotFoundError(f"Config file not found: {config_file}")
    
    try:
        # Load YAML config
        with open(config_file, 'r', encoding='utf-8') as f:
            yaml_data = yaml.safe_load(f) or {}
        
        logger.info(f"Loaded config from: {config_file}")
        
        # Apply environment variable overrides
        yaml_data = _apply_env_overrides(yaml_data)
        
        # Load secrets from Docker secrets
        yaml_data = _load_secrets(yaml_data)
        yaml_data = _load_security_token_from_file(yaml_data)
        
        # Validate and create config
        config = AppConfig(**yaml_data)
        
        logger.info(
            "Configuration loaded successfully",
            extra={
                "component": "config",
                "config_file": str(config_file),
                "monitored_chats": len(config.chats.ids),
                "bootstrap_enabled": config.bootstrap.enabled,
                "queue_type": config.queue.type,
                "queue_name": config.queue.queue_name
            }
        )
        
        return config
        
    except yaml.YAMLError as e:
        logger.error(f"Failed to parse YAML config: {e}")
        raise ValueError(f"Invalid YAML config: {e}") from e
    
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        raise


def _apply_env_overrides(config_data: dict) -> dict:
    """
    Apply environment variable overrides to config data.
    """
    env_mappings = {
        # Telegram
        'TELEGRAM_API_ID': 'telegram.api_id',
        'TELEGRAM_API_HASH': 'telegram.api_hash',
        'TELEGRAM_SESSION': 'telegram.session',
        
        # Chats
        'CHATS_IDS': 'chats.ids',
        
        # Bootstrap
        'BOOTSTRAP_LOOKBACK_HOURS': 'bootstrap.lookback_hours',
        'BOOTSTRAP_ENABLED': 'bootstrap.enabled',
        
        # Queue
        'QUEUE_TYPE': 'queue.type',
        'QUEUE_NAME': 'queue.queue_name',
        'QUEUE_URL': 'queue.config.url',
        
        # Processing
        'PROCESSING_MAX_CONCURRENT': 'processing.max_concurrent_messages',
        'PROCESSING_ALLOW_BOTS': 'processing.allow_bots',

        # Logging
        'LOG_LEVEL': 'logging.level',
        'LOG_JSON': 'logging.json_format',

        # Security
        'QUEUE_SHARED_TOKEN': 'security.shared_token',
        'QUEUE_SHARED_TOKEN_FILE': 'security.shared_token_file'
    }
    
    for env_var, config_path in env_mappings.items():
        env_value = os.getenv(env_var)
        if env_value is not None:
            _set_nested_value(config_data, config_path, env_value)
            logger.debug(f"Applied env override: {env_var} -> {config_path}")
    
    return config_data


def _load_secrets(config_data: dict) -> dict:
    """Load secrets from Docker secrets files if available."""
    secret_mappings = {
        '/run/secrets/telegram_api_id': 'telegram.api_id',
        '/run/secrets/telegram_api_hash': 'telegram.api_hash',
        '/run/secrets/queue_token': 'security.shared_token',
    }
    
    for secret_file, config_path in secret_mappings.items():
        try:
            secret_path = Path(secret_file)
            if secret_path.exists():
                with open(secret_path, 'r', encoding='utf-8') as f:
                    secret_value = f.read().strip()
                
                if secret_value:
                    _set_nested_value(config_data, config_path, secret_value)
                    logger.debug(f"Loaded secret from {secret_file}")
                
        except Exception as e:
            logger.warning(f"Failed to load secret from {secret_file}: {e}")
    
    return config_data


def _load_security_token_from_file(config_data: dict) -> dict:
    """Load shared security token from file path specified in config."""

    security_config = config_data.get('security')
    if not isinstance(security_config, dict):
        return config_data

    token_file = security_config.get('shared_token_file')
    if not token_file:
        return config_data

    try:
        token_path = Path(token_file)
        if not token_path.exists():
            logger.warning(f"Shared token file not found: {token_file}")
            return config_data

        token_value = token_path.read_text(encoding='utf-8').strip()
        if not token_value:
            logger.warning(f"Shared token file is empty: {token_file}")
            return config_data

        security_config['shared_token'] = token_value
        logger.debug(f"Loaded shared token from {token_file}")
    except Exception as e:
        logger.warning(f"Failed to load shared token from {token_file}: {e}")

    return config_data


def _set_nested_value(data: dict, path: str, value: str) -> None:
    """Set a nested dictionary value using dot notation."""
    keys = path.split('.')
    current = data
    
    # Navigate to parent of target key
    for key in keys[:-1]:
        if key not in current:
            current[key] = {}
        current = current[key]
    
    # Set the final value with type conversion
    final_key = keys[-1]
    current[final_key] = _convert_env_value(value)


def _convert_env_value(value: str):
    """Convert environment variable string to appropriate Python type."""
    # Boolean conversion
    if value.lower() in ('true', 'yes', '1', 'on'):
        return True
    elif value.lower() in ('false', 'no', '0', 'off'):
        return False
    
    # List conversion (for chat IDs)
    if ',' in value:
        try:
            return [int(x.strip()) for x in value.split(',')]
        except ValueError:
            pass
    
    # Numeric conversion
    try:
        if '.' in value:
            return float(value)
        else:
            return int(value)
    except ValueError:
        pass
    
    # Return as string
    return value