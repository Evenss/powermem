from typing import Any, ClassVar, Dict, Optional, Union
from pydantic import AliasChoices, Field, field_validator, model_validator
from pydantic_settings import BaseSettings
import logging
logger = logging.getLogger(__name__)
from powermem.settings import settings_config

logger = logging.getLogger(__name__)


class BaseVectorStoreConfig(BaseSettings):
    """
    Base configuration class for all vector store providers.
    
    This class provides common fields and validation logic shared
    across all vector store implementations.
    """
    
    # Model config
    model_config = settings_config("VECTOR_STORE_", extra="allow", env_file=None)
    
    # Registry mechanism (same as LLM/Rerank)
    _provider_name: ClassVar[Optional[str]] = None
    _class_path: ClassVar[Optional[str]] = None
    _registry: ClassVar[dict[str, type["BaseVectorStoreConfig"]]] = {}
    _class_paths: ClassVar[dict[str, str]] = {}
    
    # Common fields across all providers
    collection_name: str = Field(
        default="memories",
        description="Name of the collection/table"
    )
    
    embedding_model_dims: Optional[int] = Field(
        default=None,
        description="Dimension of embedding vectors"
    )
    
    @classmethod
    def _register_provider(cls) -> None:
        """Register provider in the global registry."""
        provider = getattr(cls, "_provider_name", None)
        class_path = getattr(cls, "_class_path", None)
        if provider:
            BaseVectorStoreConfig._registry[provider] = cls
            if class_path:
                BaseVectorStoreConfig._class_paths[provider] = class_path
    
    def __init_subclass__(cls, **kwargs) -> None:
        """Called when a class inherits from BaseVectorStoreConfig."""
        super().__init_subclass__(**kwargs)
        cls._register_provider()
    
    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs) -> None:
        """Called by Pydantic when a class inherits from BaseVectorStoreConfig."""
        super().__pydantic_init_subclass__(**kwargs)
        cls._register_provider()
    
    @classmethod
    def get_provider_config_cls(cls, provider: str) -> Optional[type["BaseVectorStoreConfig"]]:
        """Get the config class for a specific provider."""
        return cls._registry.get(provider)
    
    @classmethod
    def get_provider_class_path(cls, provider: str) -> Optional[str]:
        """Get the class path for a specific provider."""
        return cls._class_paths.get(provider)
    
    @classmethod
    def has_provider(cls, provider: str) -> bool:
        """Check if a provider is registered."""
        return provider in cls._registry
    
    def to_component_dict(self) -> Dict[str, Any]:
        """
        Convert config to component dictionary format.
        
        Returns:
            Dict with 'provider' and 'config' keys
        """
        return {
            "provider": self._provider_name,
            "config": self.model_dump(exclude_none=True)
        }


class BaseGraphStoreConfig(BaseVectorStoreConfig):
    """
    Base configuration class for all graph store providers.
    
    Inherits from BaseVectorStoreConfig to reuse connection and vector parameters.
    Adds graph-specific fields like max_hops.
    
    Environment variable priority (via AliasChoices):
    1. GRAPH_STORE_* (highest priority)
    2. OCEANBASE_* (fallback to VectorStore config)
    3. Default values
    """
    
    model_config = settings_config("GRAPH_STORE_", extra="allow", env_file=None)
    
    _provider_name: ClassVar[Optional[str]] = None
    _class_path: ClassVar[Optional[str]] = None
    _registry: ClassVar[dict[str, type["BaseGraphStoreConfig"]]] = {}
    _class_paths: ClassVar[dict[str, str]] = {}
    
    # Override connection fields with GRAPH_STORE_ fallback aliases
    host: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices(
            "host",
            "GRAPH_STORE_HOST",    # Priority 1
            "OCEANBASE_HOST",      # Priority 2 (fallback)
        ),
        description="Database server host (for remote mode)"
    )
    
    port: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices(
            "port",
            "GRAPH_STORE_PORT",
            "OCEANBASE_PORT",
        ),
        description="Database server port (for remote mode)"
    )

    @field_validator("port", mode="before")
    @classmethod
    def _coerce_port_to_str(cls, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, int):
            return str(value)
        return value

    @field_validator("host", "path", mode="before")
    @classmethod
    def _convert_empty_string_to_none(cls, value: Any) -> Any:
        """Convert empty strings to None for host and path fields.
        
        Also prevent PATH environment variable from being used as path field.
        """
        if value is not None and isinstance(value, str):
            # Empty string -> None
            if not value.strip():
                return None
            # If path contains ':' (Unix PATH separator) or ';' (Windows PATH separator),
            # it's likely the PATH environment variable, not a directory path
            if ":" in value and value.count(":") > 1:
                return None
            if ";" in value and value.count(";") > 1:
                return None
        return value

    user: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices(
            "GRAPH_STORE_USER",
            "OCEANBASE_USER",
            "user", # avoid using system USER environment variable first
        ),
        description="Database username (for remote mode)"
    )
    
    password: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices(
            "password",
            "GRAPH_STORE_PASSWORD",
            "OCEANBASE_PASSWORD",
        ),
        description="Database password (for remote mode)"
    )
    
    db_name: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices(
            "db_name",
            "GRAPH_STORE_DB_NAME",
            "OCEANBASE_DATABASE",
        ),
        description="Database name"
    )
    
    # Connection parameter for embedded mode
    path: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices(
            "path",
            "GRAPH_STORE_PATH",
            "OCEANBASE_EMBEDDED_PATH",
        ),
        description="Path for embedded seekdb mode (auto-creates if not exists)"
    )

    @model_validator(mode="after")
    def _validate_connection_mode(self) -> "BaseGraphStoreConfig":
        """Validate connection mode and set defaults based on priority: host > path.
        
        Note: Unlike OceanBaseConfig, GraphStore does not set default embedded mode
        automatically. It requires either host or path to be explicitly configured,
        or it will inherit from the vector store configuration.
        """
        if self.host:
            # Remote mode: host provided (highest priority)
            logger.info(f"GraphStore using remote mode: {self.host}:{self.port or '2881'}")
            # Ensure required remote parameters have defaults
            if self.port is None:
                self.port = "2881"
            if self.user is None:
                self.user = "root@test"
            if self.password is None:
                self.password = ""
            if self.db_name is None:
                self.db_name = "test"
        elif self.path:
            # Embedded mode: path provided, no host
            logger.info(f"GraphStore using embedded seekdb mode: path={self.path}")
            if self.db_name is None:
                self.db_name = "test"
        # If neither host nor path is set, that's OK - it will be set when config is loaded
        # or the graph store won't be initialized (if graph_store.enabled=false)
        
        return self
    
    # Override vector configuration fields
    vidx_metric_type: str = Field(
        default="l2",
        validation_alias=AliasChoices(
            "vidx_metric_type",
            "GRAPH_STORE_VECTOR_METRIC_TYPE",
            "OCEANBASE_VECTOR_METRIC_TYPE",
        ),
        description="Distance metric (l2, inner_product, cosine)"
    )
    
    index_type: str = Field(
        default="HNSW",
        validation_alias=AliasChoices(
            "index_type",
            "GRAPH_STORE_INDEX_TYPE",
            "OCEANBASE_INDEX_TYPE",
        ),
        description="Type of vector index (HNSW, IVF, FLAT, etc.)"
    )
    
    embedding_model_dims: Optional[int] = Field(
        default=None,
        validation_alias=AliasChoices(
            "embedding_model_dims",
            "GRAPH_STORE_EMBEDDING_MODEL_DIMS",
            "OCEANBASE_EMBEDDING_MODEL_DIMS",
        ),
        description="Dimension of embedding vectors"
    )
    
    # Graph-specific fields
    max_hops: int = Field(
        default=3,
        validation_alias=AliasChoices(
            "max_hops",
            "GRAPH_STORE_MAX_HOPS",
        ),
        description="Maximum number of hops for multi-hop graph search"
    )
    
    # GraphStore metadata fields (from GraphStoreConfig)
    # Note: BaseLLMConfig is imported lazily to avoid circular imports
    llm: Optional[Any] = Field(
        default=None,
        description="LLM configuration for querying the graph store (overrides global LLM)"
    )
    custom_prompt: Optional[str] = Field(
        default=None,
        description="Custom prompt to fetch entities from the given text"
    )
    custom_extract_relations_prompt: Optional[str] = Field(
        default=None,
        description="Custom prompt for extracting relations from text"
    )
    custom_update_graph_prompt: Optional[str] = Field(
        default=None,
        description="Custom prompt for updating graph memories"
    )
    custom_delete_relations_prompt: Optional[str] = Field(
        default=None,
        description="Custom prompt for deleting relations"
    )
    
    @classmethod
    def _register_provider(cls) -> None:
        """Register provider in the global registry."""
        provider = getattr(cls, "_provider_name", None)
        class_path = getattr(cls, "_class_path", None)
        if provider:
            BaseGraphStoreConfig._registry[provider] = cls
            if class_path:
                BaseGraphStoreConfig._class_paths[provider] = class_path
    
    def __init_subclass__(cls, **kwargs) -> None:
        """Called when a class inherits from BaseGraphStoreConfig."""
        super().__init_subclass__(**kwargs)
        cls._register_provider()
    
    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs) -> None:
        """Called by Pydantic when a class inherits from BaseGraphStoreConfig."""
        super().__pydantic_init_subclass__(**kwargs)
        cls._register_provider()
    
    @classmethod
    def get_provider_config_cls(cls, provider: str) -> Optional[type["BaseGraphStoreConfig"]]:
        """Get the config class for a specific provider."""
        return cls._registry.get(provider)
    
    @classmethod
    def get_provider_class_path(cls, provider: str) -> Optional[str]:
        """Get the class path for a specific provider."""
        return cls._class_paths.get(provider)
    
    @classmethod
    def has_provider(cls, provider: str) -> bool:
        """Check if a provider is registered."""
        return provider in cls._registry
    
    def to_component_dict(self) -> Dict[str, Any]:
        """
        Convert config to component dictionary format.
        
        Returns:
            Dict with 'provider' and 'config' keys
        """
        return {
            "provider": self._provider_name,
            "config": self.model_dump(exclude_none=True)
        }