"""
Configuration management for queue-writer service.
Loads and validates configuration from YAML files using Pydantic.
"""

import os
import logging
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, Field, validator, ConfigDict


logger = logging.getLogger(__name__)


class RedisConfig(BaseModel):
    """Redis connection and stream configuration."""
    model_config = ConfigDict(extra='forbid')
    
    url: str = Field(
        default="redis://redis:6379/0",
        description="Redis connection URL"
    )
    stream_key: str = Field(
        default="tg:messages",
        description="Redis Stream key name"
    )
    maxlen_approx: int = Field(
        default=1000000,
        ge=1000,
        description="Approximate max length for stream trimming"
    )
    dedup_set_key: str = Field(
        default="tg:dedup",
        description="Redis Set key for deduplication"
    )
    dedup_ttl_seconds: int = Field(
        default=604800,  # 7 days
        ge=3600,
        le=2592000,  # 30 days max
        description="TTL for deduplication entries in seconds"
    )
    max_connections: int = Field(
        default=10,
        ge=1,
        le=50,
        description="Maximum connections in Redis pool"
    )
    socket_timeout: float = Field(
        default=5.0,
        ge=1.0,
        le=30.0,
        description="Socket timeout in seconds"
    )
    socket_connect_timeout: float = Field(
        default=5.0,
        ge=1.0,
        le=30.0,
        description="Connection timeout in seconds"
    )
    health_check_interval: int = Field(
        default=30,
        ge=10,
        le=300,
        description="Health check interval in seconds"
    )


class ServerConfig(BaseModel):
    """HTTP server configuration."""
    model_config = ConfigDict(extra='forbid')
    
    host: str = Field(
        default="0.0.0.0",
        description="Host to bind to"
    )
    port: int = Field(
        default=8080,
        ge=1024,
        le=65535,
        description="Port to bind to"
    )
    workers: int = Field(
        default=1,
        ge=1,
        le=8,
        description="Number of worker processes"
    )
    log_level: str = Field(
        default="info",
        description="Uvicorn log level"
    )
    
    @validator('log_level')
    def validate_log_level(cls, v):
        valid_levels = ['critical', 'error', 'warning', 'info', 'debug', 'trace']
        if v.lower() not in valid_levels:
            raise ValueError(f"Invalid log level. Must be one of: {valid_levels}")
        return v.lower()


class SecurityConfig(BaseModel):
    """Security configuration."""
    model_config = ConfigDict(extra='forbid')
    
    shared_token_file: str = Field(
        default="/run/secrets/queue_token",
        description="Path to shared token file"
    )
    
    @validator('shared_token_file')
    def validate_token_file_path(cls, v):
        if not v:
            raise ValueError("Token file path cannot be empty")
        return v


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
    
    redis: RedisConfig = Field(default_factory=RedisConfig)
    server: ServerConfig = Field(default_factory=ServerConfig)
    security: SecurityConfig = Field(default_factory=SecurityConfig)
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
        logger.warning(f"Config file not found: {config_file}, using defaults")
        return AppConfig()
    
    try:
        # Load YAML config
        with open(config_file, 'r', encoding='utf-8') as f:
            yaml_data = yaml.safe_load(f) or {}
        
        logger.info(f"Loaded config from: {config_file}")
        
        # Apply environment variable overrides
        yaml_data = _apply_env_overrides(yaml_data)
        
        # Validate and create config
        config = AppConfig(**yaml_data)
        
        logger.info(
            "Configuration loaded successfully",
            extra={
                "component": "config",
                "config_file": str(config_file),
                "redis_url": config.redis.url,
                "server_port": config.server.port
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
    - REDIS_URL -> redis.url
    - SERVER_PORT -> server.port
    - LOGGING_LEVEL -> logging.level
    
    Args:
        config_data: Base configuration data
        
    Returns:
        Configuration data with environment overrides applied
    """
    # Environment variable mappings
    env_mappings = {
        'REDIS_URL': 'redis.url',
        'REDIS_STREAM_KEY': 'redis.stream_key',
        'REDIS_MAXLEN': 'redis.maxlen_approx',
        'REDIS_DEDUP_TTL': 'redis.dedup_ttl_seconds',
        'SERVER_HOST': 'server.host',
        'SERVER_PORT': 'server.port',
        'SERVER_WORKERS': 'server.workers',
        'SHARED_TOKEN_FILE': 'security.shared_token_file',
        'LOG_LEVEL': 'logging.level',
        'LOG_JSON': 'logging.json_format'
    }
    
    for env_var, config_path in env_mappings.items():
        env_value = os.getenv(env_var)
        if env_value is not None:
            _set_nested_value(config_data, config_path, env_value)
            logger.debug(f"Applied env override: {env_var} -> {config_path}")
    
    return config_data


def _set_nested_value(data: dict, path: str, value: str) -> None:
    """
    Set a nested dictionary value using dot notation.
    
    Args:
        data: Dictionary to modify
        path: Dot-separated path (e.g., 'redis.url')
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
        Converted value (bool, int, float, or str)
    """
    # Boolean conversion
    if value.lower() in ('true', 'yes', '1', 'on'):
        return True
    elif value.lower() in ('false', 'no', '0', 'off'):
        return False
    
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