"""
Sparse embedding factory for creating sparse embedding instances

This module provides a factory for creating different sparse embedding backends.
"""

import importlib
from powermem.integrations.embeddings.config.sparse_base import SparseEmbedderConfig


def load_class(class_type):
    module_path, class_name = class_type.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


class SparseEmbedderFactory:
    """Factory for creating sparse embedding instances."""
    
    provider_to_class = {
        "qwen": "powermem.integrations.embeddings.qwen_sparse.QwenSparseEmbedding",
    }

    @classmethod
    def create(cls, provider_name: str, config):
        """
        Create a sparse embedding instance.
        
        Args:
            provider_name: Name of the sparse embedding provider (e.g., 'qwen')
            config: Configuration dictionary or SparseEmbedderConfig object
            
        Returns:
            Sparse embedding instance
        """
        class_type = cls.provider_to_class.get(provider_name)
        if class_type:
            if not isinstance(config, dict):
                # If config is already a SparseEmbedderConfig object, use it directly
                if hasattr(config, 'model') or hasattr(config, 'api_key'):
                    # It's already a config object
                    config_obj = config
                else:
                    # Try to convert to dict
                    config = config.model_dump() if hasattr(config, 'model_dump') else {}
                    config_obj = SparseEmbedderConfig(**config)
            else:
                # Convert dict to SparseEmbedderConfig
                config_obj = SparseEmbedderConfig(**config)
            
            sparse_embedder_class = load_class(class_type)
            return sparse_embedder_class(config_obj)
        else:
            raise ValueError(f"Unsupported SparseEmbedder provider: {provider_name}")
