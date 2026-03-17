from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional

from powermem.integrations.embeddings.config.sparse_base import BaseSparseEmbedderConfig

if TYPE_CHECKING:
    from powermem.core.token_tracker import TokenTracker


class SparseEmbeddingBase(ABC):
    """Initialized a base sparse embedding class

    :param config: Sparse embedding configuration option class, defaults to None
    :type config: Optional[BaseSparseEmbedderConfig], optional
    """

    def __init__(self, config: Optional[BaseSparseEmbedderConfig] = None):
        if config is None:
            self.config = BaseSparseEmbedderConfig()
        else:
            self.config = config

        # Token tracker injected externally by Memory/AsyncMemory after initialisation.
        self.token_tracker: Optional["TokenTracker"] = None

    @abstractmethod
    def embed_sparse(self, text: str) -> dict[int, float]:
        """
        Get the sparse embedding for the given text.

        Args:
            text (str): The text to embed.
        Returns:
            dict: The sparse embedding dictionary.
            like {1:0.1, 2:0.2, 3:0.3} or {'1':0.1, '2':0.2, '3':0.3}
        """
        pass
