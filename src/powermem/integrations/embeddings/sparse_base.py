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
    def embed_sparse(self, text) -> dict:
        """
        Get the sparse embedding for the given text.

        Args:
            text (str): The text to embed.
        Returns:
            dict: The sparse embedding dictionary.
            like {1:0.1, 2:0.2, 3:0.3} or {'1':0.1, '2':0.2, '3':0.3}
        """
        pass
