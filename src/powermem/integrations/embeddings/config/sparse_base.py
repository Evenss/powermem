from abc import ABC
from typing import Optional


class SparseEmbedderConfig(ABC):
    """
    Config for Sparse Embeddings.
    """

    def __init__(
        self,
        model: Optional[str] = None,
        api_key: Optional[str] = None,
        embedding_dims: Optional[int] = None,
        base_url: Optional[str] = None,
    ):
        """
        Initializes a configuration class instance for the Sparse Embeddings.

        :param model: Embedding model to use, defaults to None
        :type model: Optional[str], optional
        :param api_key: API key to use, defaults to None
        :type api_key: Optional[str], optional
        :param embedding_dims: The number of dimensions in the embedding, defaults to None
        :type embedding_dims: Optional[int], optional
        :param base_url: Base URL for the API, defaults to None
        :type base_url: Optional[str], optional
        """

        self.model = model
        self.api_key = api_key
        self.embedding_dims = embedding_dims
        self.base_url = base_url

