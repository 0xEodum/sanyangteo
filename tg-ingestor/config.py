"""
Configuration management for tg-ingestor service.
Loads and validates configuration from YAML files using Pydantic.
"""

import os
import logging
from pathlib import Path
from typing import List, Optional

import yaml
from pydantic import BaseModel, Field, validator, ConfigDict


logger = logging.getLogger(__name__)


class TelegramConfig(BaseModel):
    """Telegram API configuration."""
    model_config = ConfigDict(extra='forbid')
    
    session: str = Field(
        default="/data/session.session",
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
        description="List of chat IDs to monitor (negative for groups/supergroups)"
    )
    
    @validator('ids')
    def validate_chat_ids(cls, v):
        """Validate chat IDs are negative (groups/supergroups)."""
        for chat_id in v:
            if chat_id > 0:
                raise ValueError(f"Chat ID {chat_id} should be negative for groups/supergroups")
            if chat_id > -1000:
                logger.warning(f"Chat ID {chat_id} might be a regular group, consider using supergroup")
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


class OutputConfig(BaseModel):
    """Output configuration for queue-writer connection."""
    model_config = ConfigDict(extra='forbid')
    
    endpoint: str = Field(
        default="http://queue-writer:8080/events",
        description="Queue-writer events endpoint URL"
    )
    timeout_ms: int = Field(
        default=2000,
        ge=500,
        le=30000,
        description="HTTP request timeout in milliseconds"
    )
    max_retries: int = Field(
        default=5,
        ge=0,
        le=10,
        description="Maximum number of retry attempts"
    )
    retry_backoff_ms: int = Field(
        default=500,
        ge=100,
        le=5000,
        description="Initial backoff time between retries in milliseconds"
    )
    max_backoff_ms: int = Field(
        default=10000,
        ge=1000,
        le=60000,
        description="Maximum backoff time between retries in milliseconds"
    )
    
    @validator('endpoint')
    def validate_endpoint(cls, v):
        """Validate endpoint URL format."""
        if not v.startswith(('http://', 'https://')):
            raise ValueError("Endpoint must start with http:// or https://")
        if not v.endswith('/events'):
            raise ValueError("Endpoint must end with /events")
        return v


class SecurityConfig(BaseModel):
    """Security configuration."""
    model_config = ConfigDict(extra='forbid')
    
    shared_token_file: str = Field(
        default="/run/secrets/queue_token",
        description="Path to shared token file for queue-writer authentication"
    )
    
    @validator('shared_token_file')
    def validate_token_file_path(cls, v):
        if not v:
            raise ValueError("Token file path cannot be empty")
        return v


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


class AppConfig(BaseModel):
    """Main application configuration."""
    model_config = ConfigDict(extra='forbid')
    
    telegram: TelegramConfig = Field(..., description="Telegram API configuration")
    chats: ChatsConfig = Field(..., description="Chat monitoring configuration")
    bootstrap: BootstrapConfig = Field(default_factory=BootstrapConfig)
    output: OutputConfig = Field(default_factory=OutputConfig)
    security: SecurityConfig = Field(default_factory=SecurityConfig)
    processing: ProcessingConfig = Field(default_factory=ProcessingConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)


def load_config(config_path: Optional[str] = None) -> AppConfig:
    """
    Load configuration from YAML file with environment variable overrides.
    
    Args:
        config_path: Path to config file, defaults to CONFIG_PATH env var or ./config.yml
        
    Returns:
        Loaded and validated configuration
        
    Raises:
        FileNotFoundError: If config file not found
        ValueError: If config validation fails
        yaml.YAMLError: If YAML parsing fails
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
        
        # Load secrets if running in Docker
        yaml_data = _load_secrets(yaml_data)
        
        # Validate and create config
        config = AppConfig(**yaml_data)
        
        logger.info(
            "Configuration loaded successfully",
            extra={
                "component": "config",
                "config_file": str(config_file),
                "monitored_chats": len(config.chats.ids),
                "bootstrap_enabled": config.bootstrap.enabled,
                "output_endpoint": config.output.endpoint
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
    
    Supports dot notation for nested keys:
    - TELEGRAM_API_ID -> telegram.api_id
    - CHATS_IDS -> chats.ids (comma-separated)
    - OUTPUT_ENDPOINT -> output.endpoint
    
    Args:
        config_data: Base configuration data
        
    Returns:
        Configuration data with environment overrides applied
    """
    # Environment variable mappings
    env_mappings = {
        'TELEGRAM_API_ID': 'telegram.api_id',
        'TELEGRAM_API_HASH': 'telegram.api_hash',
        'TELEGRAM_SESSION': 'telegram.session',
        'CHATS_IDS': 'chats.ids',
        'BOOTSTRAP_LOOKBACK_HOURS': 'bootstrap.lookback_hours',
        'BOOTSTRAP_ENABLED': 'bootstrap.enabled',
        'OUTPUT_ENDPOINT': 'output.endpoint',
        'OUTPUT_TIMEOUT_MS': 'output.timeout_ms',
        'OUTPUT_MAX_RETRIES': 'output.max_retries',
        'SHARED_TOKEN_FILE': 'security.shared_token_file',
        'PROCESSING_MAX_CONCURRENT': 'processing.max_concurrent_messages',
        'PROCESSING_ALLOW_BOTS': 'processing.allow_bots',
        'LOG_LEVEL': 'logging.level',
        'LOG_JSON': 'logging.json_format'
    }
    
    for env_var, config_path in env_mappings.items():
        env_value = os.getenv(env_var)
        if env_value is not None:
            _set_nested_value(config_data, config_path, env_value)
            logger.debug(f"Applied env override: {env_var} -> {config_path}")
    
    return config_data


def _load_secrets(config_data: dict) -> dict:
    """
    Load secrets from Docker secrets files if available.
    
    Args:
        config_data: Configuration data
        
    Returns:
        Configuration data with secrets loaded
    """
    secret_mappings = {
        '/run/secrets/telegram_api_id': 'telegram.api_id',
        '/run/secrets/telegram_api_hash': 'telegram.api_hash',
        '/run/secrets/queue_token': '_queue_token_content'  # Special handling
    }
    
    for secret_file, config_path in secret_mappings.items():
        try:
            secret_path = Path(secret_file)
            if secret_path.exists():
                with open(secret_path, 'r', encoding='utf-8') as f:
                    secret_value = f.read().strip()
                
                if secret_value:
                    if config_path == '_queue_token_content':
                        # Don't store token in config, just validate file exists
                        continue
                    else:
                        _set_nested_value(config_data, config_path, secret_value)
                    logger.debug(f"Loaded secret from {secret_file}")
                
        except Exception as e:
            logger.warning(f"Failed to load secret from {secret_file}: {e}")
    
    return config_data


def _set_nested_value(data: dict, path: str, value: str) -> None:
    """
    Set a nested dictionary value using dot notation.
    
    Args:
        data: Dictionary to modify
        path: Dot-separated path (e.g., 'telegram.api_id')
        value: Value to set (string, will be converted to appropriate type)
    """
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
    """
    Convert environment variable string to appropriate Python type.
    
    Args:
        value: String value from environment
        
    Returns:
        Converted value (bool, int, list, or str)
    """
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


def validate_secrets_exist(config: AppConfig) -> None:
    """
    Validate that required secret files exist.
    
    Args:
        config: Application configuration
        
    Raises:
        FileNotFoundError: If required secrets are missing
    """
    required_secrets = [
        config.security.shared_token_file
    ]
    
    missing_secrets = []
    for secret_file in required_secrets:
        if not Path(secret_file).exists():
            missing_secrets.append(secret_file)
    
    if missing_secrets:
        raise FileNotFoundError(
            f"Required secret files not found: {', '.join(missing_secrets)}"
        )
    
    logger.info("All required secrets found")