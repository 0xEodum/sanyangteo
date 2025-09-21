"""
Configuration management for message-preprocessor service.
Loads configuration from YAML files and environment variables.
"""

import os
import logging
from pathlib import Path
from typing import List, Optional, Dict, Any

import yaml
from pydantic import BaseModel, Field, validator, ConfigDict

from domain.dto import ProcessingMode, APIKeyConfig, ModelConfig, FieldConfig, ParsingSchema


logger = logging.getLogger(__name__)


class QueueConfig(BaseModel):
    """Queue configuration using shared library."""
    model_config = ConfigDict(extra='forbid')
    
    type: str = Field(default="redis", description="Queue type")
    queue_name: str = Field(..., description="Queue name")
    consumer_group: Optional[str] = Field(None, description="Consumer group for input queue")
    config: Dict[str, Any] = Field(default_factory=dict, description="Queue-specific configuration")


class RedisCountersConfig(BaseModel):
    """Redis configuration for key usage counters."""
    model_config = ConfigDict(extra='forbid')
    
    url: str = Field(..., description="Redis connection URL")
    key_prefix: str = Field(default="llm_key_usage:", description="Prefix for usage counter keys")


class RetryConfig(BaseModel):
    """Retry configuration for test mode."""
    model_config = ConfigDict(extra='forbid')
    
    delay_seconds: int = Field(default=300, ge=10, le=3600, description="Delay when all models exhausted")
    max_attempts_per_model: int = Field(default=2, ge=1, le=5, description="Max attempts per model")


class LLMConfig(BaseModel):
    """LLM request configuration."""
    model_config = ConfigDict(extra='forbid')
    
    timeout_seconds: int = Field(default=30, ge=5, le=120, description="Request timeout")
    max_tokens: int = Field(default=1000, ge=100, le=4000, description="Maximum tokens")
    temperature: float = Field(default=0.1, ge=0.0, le=1.0, description="Temperature")


class ProcessingConfig(BaseModel):
    """Message processing configuration."""
    model_config = ConfigDict(extra='forbid')
    
    max_concurrent_messages: int = Field(default=5, ge=1, le=20, description="Max concurrent processing")
    batch_size: int = Field(default=1, ge=1, le=10, description="Batch size for processing")


class LoggingConfig(BaseModel):
    """Logging configuration."""
    model_config = ConfigDict(extra='forbid')
    
    level: str = Field(default="INFO", description="Logging level")
    json_format: bool = Field(default=True, description="Enable JSON formatting")
    enable_correlation: bool = Field(default=True, description="Enable correlation IDs")
    
    @validator('level')
    def validate_log_level(cls, v):
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if v.upper() not in valid_levels:
            raise ValueError(f"Invalid log level. Must be one of: {valid_levels}")
        return v.upper()


class DependenciesConfig(BaseModel):
    """Configuration for external dependencies."""
    model_config = ConfigDict(extra='forbid')
    
    sys_prompt_file: str = Field(..., description="Path to system prompt file")
    api_keys_file: str = Field(..., description="Path to API keys file")
    api_urls_file: str = Field(..., description="Path to API URLs file")


class ParsingSchemasConfig(BaseModel):
    """Configuration for parsing schemas."""
    model_config = ConfigDict(extra='forbid')
    
    order: Dict[str, ParsingSchema] = Field(default_factory=dict, description="ORDER parsing schemas")
    offer: Dict[str, ParsingSchema] = Field(default_factory=dict, description="OFFER parsing schemas")
    
    # Active schema versions
    active_order_schema: str = Field(default="v1", description="Active ORDER schema version")
    active_offer_schema: str = Field(default="v1", description="Active OFFER schema version")


class AppConfig(BaseModel):
    """Main application configuration."""
    model_config = ConfigDict(extra='forbid')
    
    # Processing mode
    mode: ProcessingMode = Field(..., description="Processing mode: prod or test")
    
    # Queue configurations
    input_queue: QueueConfig = Field(..., description="Input queue configuration")
    output_queue: QueueConfig = Field(..., description="Output queue configuration")
    
    # Redis for counters
    redis_counters: RedisCountersConfig = Field(..., description="Redis counters configuration")
    
    # Retry and processing
    retry: RetryConfig = Field(default_factory=RetryConfig)
    processing: ProcessingConfig = Field(default_factory=ProcessingConfig)
    
    # LLM configuration
    llm: LLMConfig = Field(default_factory=LLMConfig)
    
    # Parsing schemas
    parsing_schemas: ParsingSchemasConfig = Field(default_factory=ParsingSchemasConfig)
    
    # Logging
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    
    # Dependencies paths
    dependencies: DependenciesConfig = Field(..., description="External dependencies")


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
        
        # Validate and create config
        config = AppConfig(**yaml_data)
        
        logger.info(
            "Configuration loaded successfully",
            extra={
                "component": "config",
                "config_file": str(config_file),
                "mode": config.mode.value,
                "input_queue": config.input_queue.queue_name,
                "output_queue": config.output_queue.queue_name
            }
        )
        
        return config
        
    except yaml.YAMLError as e:
        logger.error(f"Failed to parse YAML config: {e}")
        raise ValueError(f"Invalid YAML config: {e}") from e
    
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        raise


def load_api_keys(keys_file_path: str) -> Dict[str, List[APIKeyConfig]]:
    """
    Load API keys from YAML file.
    
    Returns:
        Dictionary with 'rotation' and 'direct' keys
    """
    keys_file = Path(keys_file_path)
    
    if not keys_file.exists():
        raise FileNotFoundError(f"API keys file not found: {keys_file}")
    
    try:
        with open(keys_file, 'r', encoding='utf-8') as f:
            keys_data = yaml.safe_load(f) or {}
        
        # Parse rotation keys
        rotation_keys = []
        for key_data in keys_data.get('rotation', []):
            rotation_keys.append(APIKeyConfig(
                key=key_data['key'],
                name=key_data['name'],
                limit_per_day=key_data['limit_per_day']
            ))
        
        # Parse direct keys
        direct_keys = []
        for key_data in keys_data.get('direct', []):
            direct_keys.append(APIKeyConfig(
                key=key_data['key'],
                name=key_data['name'],
                limit_per_day=None  # No limit for direct keys
            ))
        
        logger.info(
            f"Loaded API keys: {len(rotation_keys)} rotation, {len(direct_keys)} direct",
            extra={
                "component": "config",
                "rotation_keys": len(rotation_keys),
                "direct_keys": len(direct_keys)
            }
        )
        
        return {
            'rotation': rotation_keys,
            'direct': direct_keys
        }
        
    except Exception as e:
        logger.error(f"Failed to load API keys: {e}")
        raise


def load_api_models(urls_file_path: str) -> Dict[str, List[ModelConfig]]:
    """
    Load API models from YAML file.
    
    Returns:
        Dictionary with 'main', 'fallback', and 'free' models
    """
    urls_file = Path(urls_file_path)
    
    if not urls_file.exists():
        raise FileNotFoundError(f"API URLs file not found: {urls_file}")
    
    try:
        with open(urls_file, 'r', encoding='utf-8') as f:
            urls_data = yaml.safe_load(f) or {}
        
        result = {}
        
        for section in ['main', 'fallback', 'free']:
            models = []
            for model_data in urls_data.get(section, []):
                models.append(ModelConfig(
                    name=model_data['name'],
                    url=model_data['url'],
                    provider=model_data['provider']
                ))
            result[section] = models
        
        logger.info(
            f"Loaded API models: main={len(result.get('main', []))}, "
            f"fallback={len(result.get('fallback', []))}, "
            f"free={len(result.get('free', []))}",
            extra={
                "component": "config",
                "main_models": len(result.get('main', [])),
                "fallback_models": len(result.get('fallback', [])),
                "free_models": len(result.get('free', []))
            }
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to load API models: {e}")
        raise


def load_system_prompt(prompt_file_path: str) -> str:
    """
    Load system prompt from text file.
    
    Returns:
        System prompt text
    """
    prompt_file = Path(prompt_file_path)
    
    if not prompt_file.exists():
        raise FileNotFoundError(f"System prompt file not found: {prompt_file}")
    
    try:
        with open(prompt_file, 'r', encoding='utf-8') as f:
            prompt = f.read().strip()
        
        if not prompt:
            raise ValueError("System prompt file is empty")
        
        logger.info(
            f"Loaded system prompt ({len(prompt)} characters)",
            extra={
                "component": "config",
                "prompt_length": len(prompt)
            }
        )
        
        return prompt
        
    except Exception as e:
        logger.error(f"Failed to load system prompt: {e}")
        raise


def _apply_env_overrides(config_data: dict) -> dict:
    """
    Apply environment variable overrides to config data.
    """
    env_mappings = {
        # Processing mode
        'PROCESSING_MODE': 'mode',
        
        # Queue configurations
        'INPUT_QUEUE_NAME': 'input_queue.queue_name',
        'INPUT_QUEUE_URL': 'input_queue.config.url',
        'OUTPUT_QUEUE_NAME': 'output_queue.queue_name',
        'OUTPUT_QUEUE_URL': 'output_queue.config.url',
        
        # Redis counters
        'REDIS_COUNTERS_URL': 'redis_counters.url',
        
        # Logging
        'LOG_LEVEL': 'logging.level',
        'LOG_JSON': 'logging.json_format',
        
        # Dependencies
        'SYS_PROMPT_FILE': 'dependencies.sys_prompt_file',
        'API_KEYS_FILE': 'dependencies.api_keys_file',
        'API_URLS_FILE': 'dependencies.api_urls_file'
    }
    
    for env_var, config_path in env_mappings.items():
        env_value = os.getenv(env_var)
        if env_value is not None:
            _set_nested_value(config_data, config_path, env_value)
            logger.debug(f"Applied env override: {env_var} -> {config_path}")
    
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