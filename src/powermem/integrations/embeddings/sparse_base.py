from abc import ABC, abstractmethod
from typing import Optional

from src.powermem.integrations.embeddings.config.sparse_base import SparseEmbedderConfig


class SparseEmbeddingBase(ABC):
    """Initialized a base sparse embedding class

    :param config: Sparse embedding configuration option class, defaults to None
    :type config: Optional[SparseEmbedderConfig], optional
    """

    def __init__(self, config: Optional[SparseEmbedderConfig] = None):
        if config is None:
            self.config = SparseEmbedderConfig()
        else:
            self.config = config

    @abstractmethod
    def embed(self, text) -> dict:
        """
        Get the sparse embedding for the given text.

        Args:
            text (str): The text to embed.
        Returns:
            dict: The sparse embedding dictionary.
        """
        pass
