"""
Model manager for LLM model selection and retry strategy.
Handles model selection based on processing mode and context.
"""

import logging
from typing import List, Optional, Dict, Any

from domain.ports import ModelManager
from domain.dto import ProcessingMode, ModelConfig, ProcessingContext


logger = logging.getLogger(__name__)


class ConfigurableModelManager(ModelManager):
    """
    Model manager with configurable model lists and retry strategy.
    
    Processing modes:
    - PROD: main models -> fallback models -> exhausted
    - TEST: free models -> exhausted (no fallback)
    """
    
    def __init__(self, models_config: Dict[str, List[ModelConfig]]):
        """
        Initialize model manager with configuration.
        
        Args:
            models_config: Dict with 'main', 'fallback', 'free' model lists
        """
        self.models_config = models_config
        
        # Validate configuration
        self._validate_config()
        
        logger.info(
            f"Model manager initialized",
            extra={
                "component": "model_manager",
                "main_models": len(self.models_config.get('main', [])),
                "fallback_models": len(self.models_config.get('fallback', [])),
                "free_models": len(self.models_config.get('free', []))
            }
        )
    
    def _validate_config(self) -> None:
        """Validate model configuration."""
        required_sections = ['main', 'fallback', 'free']
        
        for section in required_sections:
            if section not in self.models_config:
                logger.warning(f"Missing model section: {section}")
                self.models_config[section] = []
        
        # Check that we have models for each mode
        if not self.models_config['main'] and not self.models_config['fallback']:
            logger.warning("No models configured for PROD mode")
        
        if not self.models_config['free']:
            logger.warning("No models configured for TEST mode")
    
    def get_models_for_mode(self, processing_mode: ProcessingMode) -> List[ModelConfig]:
        """
        Get list of models for processing mode.
        
        Args:
            processing_mode: PROD uses main+fallback, TEST uses free models
            
        Returns:
            Ordered list of models to try
        """
        if processing_mode == ProcessingMode.PROD:
            # PROD: main models first, then fallback
            models = (
                self.models_config.get('main', []) + 
                self.models_config.get('fallback', [])
            )
        else:  # TEST mode
            # TEST: only free models
            models = self.models_config.get('free', [])
        
        logger.debug(
            f"Got {len(models)} models for {processing_mode.value} mode",
            extra={
                "component": "model_manager",
                "processing_mode": processing_mode.value,
                "model_count": len(models),
                "model_names": [m.name for m in models]
            }
        )
        
        return models
    
    def get_next_model(
        self, 
        context: ProcessingContext,
        processing_mode: ProcessingMode
    ) -> Optional[ModelConfig]:
        """
        Get next model to try based on context.
        
        Args:
            context: Current processing context with attempt history
            processing_mode: Processing mode
            
        Returns:
            Next model to try or None if all exhausted
        """
        available_models = self.get_models_for_mode(processing_mode)
        
        if not available_models:
            logger.warning(
                f"No models available for {processing_mode.value} mode",
                extra={
                    "component": "model_manager",
                    "processing_mode": processing_mode.value,
                    "context_id": context.idempotency_key
                }
            )
            return None
        
        # Skip models that were already attempted
        for model in available_models:
            if model.name not in context.models_attempted:
                logger.debug(
                    f"Selected model: {model.name}",
                    extra={
                        "component": "model_manager",
                        "model_name": model.name,
                        "provider": model.provider,
                        "processing_mode": processing_mode.value,
                        "context_id": context.idempotency_key,
                        "models_attempted": len(context.models_attempted)
                    }
                )
                return model
        
        # All models exhausted
        logger.warning(
            f"All models exhausted for {processing_mode.value} mode",
            extra={
                "component": "model_manager",
                "processing_mode": processing_mode.value,
                "context_id": context.idempotency_key,
                "total_models": len(available_models),
                "models_attempted": context.models_attempted
            }
        )
        
        return None
    
    def get_model_by_name(self, model_name: str) -> Optional[ModelConfig]:
        """
        Get model configuration by name.
        
        Args:
            model_name: Name of the model to find
            
        Returns:
            Model configuration or None if not found
        """
        all_models = (
            self.models_config.get('main', []) +
            self.models_config.get('fallback', []) +
            self.models_config.get('free', [])
        )
        
        for model in all_models:
            if model.name == model_name:
                return model
        
        logger.warning(
            f"Model not found: {model_name}",
            extra={
                "component": "model_manager",
                "requested_model": model_name
            }
        )
        return None
    
    def is_fallback_model(self, model_name: str, processing_mode: ProcessingMode) -> bool:
        """
        Check if model is a fallback model.
        
        Args:
            model_name: Model name to check
            processing_mode: Processing mode
            
        Returns:
            True if model is in fallback section for PROD mode
        """
        if processing_mode != ProcessingMode.PROD:
            return False
        
        fallback_models = self.models_config.get('fallback', [])
        return any(model.name == model_name for model in fallback_models)
    
    def get_model_stats(self) -> Dict[str, Any]:
        """
        Get statistics about model configuration.
        
        Returns:
            Dictionary with model statistics
        """
        stats = {
            "total_models": 0,
            "models_by_section": {},
            "models_by_provider": {},
            "unique_providers": set()
        }
        
        for section_name, models in self.models_config.items():
            stats["models_by_section"][section_name] = len(models)
            stats["total_models"] += len(models)
            
            for model in models:
                provider = model.provider
                stats["unique_providers"].add(provider)
                
                if provider not in stats["models_by_provider"]:
                    stats["models_by_provider"][provider] = 0
                stats["models_by_provider"][provider] += 1
        
        stats["unique_providers"] = list(stats["unique_providers"])
        
        return stats
    
    def update_models_config(self, new_config: Dict[str, List[ModelConfig]]) -> None:
        """
        Update model configuration (for hot reload scenarios).
        
        Args:
            new_config: New model configuration
        """
        old_stats = self.get_model_stats()
        
        self.models_config = new_config
        self._validate_config()
        
        new_stats = self.get_model_stats()
        
        logger.info(
            f"Model configuration updated",
            extra={
                "component": "model_manager",
                "old_total": old_stats["total_models"],
                "new_total": new_stats["total_models"],
                "sections_updated": list(new_config.keys())
            }
        )


def create_model_manager(models_config: Dict[str, List[ModelConfig]]) -> ConfigurableModelManager:
    """
    Create model manager with configuration.
    
    Args:
        models_config: Dictionary with model configurations
        
    Returns:
        Configured model manager
    """
    return ConfigurableModelManager(models_config)