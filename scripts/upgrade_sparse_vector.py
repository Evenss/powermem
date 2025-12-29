"""
Sparse Vector Upgrade Script

This script is used to add sparse vector support to existing OceanBase tables:
- Add sparse_embedding column (SPARSE_VECTOR type)
- Create sparse_embedding_idx index
"""
import logging

from pyobvector import SPARSE_VECTOR, IndexParam
from sqlalchemy import text, Column

logger = logging.getLogger(__name__)


def upgrade_sparse_vector(memory) -> bool:
    """
    Add sparse vector support to OceanBase table
    
    This function checks and adds the columns and indexes required for sparse vectors.
    It is idempotent and can be safely called multiple times
    (existing columns and indexes will be skipped).
    
    Args:
        memory: Instance of Memory or AsyncMemory class, containing OceanBase configuration
        
    Returns:
        bool: Returns True on successful upgrade, False on failure
        
    Raises:
        TypeError: If memory is not a Memory or AsyncMemory instance
        ValueError: If storage is not OceanBase type
        RuntimeError: If database version does not support sparse vectors
    """
    from powermem.core.memory import Memory
    from powermem.core.async_memory import AsyncMemory
    from powermem.storage.oceanbase.oceanbase import OceanBaseVectorStore
    from powermem.utils.oceanbase_util import OceanBaseUtil
    
    # 1. Validate memory parameter type
    if not isinstance(memory, (Memory, AsyncMemory)):
        raise TypeError(
            f"Expected Memory or AsyncMemory instance, got {type(memory).__name__}. "
            f"Please pass a Memory instance:\n"
            f"  from powermem import Memory\n"
            f"  memory = Memory(config={{...}})\n"
            f"  upgrade_sparse_vector(memory)"
        )
    
    # 2. Get vector_store and validate type
    if not hasattr(memory, 'storage') or not hasattr(memory.storage, 'vector_store'):
        raise ValueError(
            "Memory instance does not have a valid storage adapter. "
            "Please ensure Memory is properly initialized."
        )
    
    vector_store = memory.storage.vector_store
    
    if not isinstance(vector_store, OceanBaseVectorStore):
        raise ValueError(
            f"Storage type is not OceanBase (got {type(vector_store).__name__}). "
            f"This upgrade script only supports OceanBase storage. "
            f"Please configure vector_store provider as 'oceanbase'."
        )
    
    # 3. Get necessary attributes
    obvector = vector_store.obvector
    collection_name = vector_store.collection_name
    
    logger.info(f"Starting sparse vector upgrade for table '{collection_name}'")
    
    # 4. Check if database version supports sparse vectors
    if not OceanBaseUtil.check_sparse_vector_version_support(obvector):
        raise RuntimeError(
            "Database version does not support SPARSE_VECTOR type. "
            "Sparse vector requires seekdb or OceanBase >= 4.5.0. "
            "Please upgrade your database before running this script."
        )
    
    # 5. Check if table exists
    if not OceanBaseUtil.check_table_exists(obvector, collection_name):
        raise RuntimeError(
            f"Table '{collection_name}' does not exist. "
            f"Please create the table first by initializing Memory with the correct configuration."
        )
    
    try:
        # 6. Add sparse_embedding column (if not exists)
        if not OceanBaseUtil.check_column_exists(obvector, collection_name, 'sparse_embedding'):
            logger.info(f"Adding sparse_embedding column to table '{collection_name}'")
            try:
                # Use pyobvector's add_columns method
                obvector.add_columns(
                    table_name=collection_name,
                    columns=[Column('sparse_embedding', SPARSE_VECTOR)]
                )
                logger.info("sparse_embedding column added successfully")
            except Exception as e:
                error_str = str(e).lower()
                if '1060' in str(e) or 'already exists' in error_str or 'duplicate column' in error_str:
                    logger.info("sparse_embedding column already exists (race condition), skipping")
                else:
                    raise RuntimeError(f"Failed to add sparse_embedding column: {e}") from e
        else:
            logger.info("sparse_embedding column already exists, skipping")
        
        # 7. Create sparse_embedding_idx index (if not exists)
        if not OceanBaseUtil.check_index_exists(obvector, collection_name, 'sparse_embedding_idx'):
            logger.info(f"Creating sparse vector index on table '{collection_name}'")
            try:
                vidx_param = IndexParam(
                    field_name="sparse_embedding",
                    index_type="daat",  # Sparse vector index type
                    index_name="sparse_embedding_idx",
                    metric_type="inner_product",
                    sparse_index_type="sindi",  # Use sindi type
                )
                
                # Create sparse vector index on existing table
                obvector.create_vidx_with_vec_index_param(
                    table_name=collection_name,
                    vidx_param=vidx_param,
                )
                logger.info("sparse_embedding_idx created successfully")
            except Exception as e:
                error_str = str(e).lower()
                if '1061' in str(e) or 'already exists' in error_str or 'duplicate key' in error_str:
                    logger.info("sparse_embedding_idx already exists (race condition), skipping")
                else:
                    raise RuntimeError(f"Failed to create sparse_embedding_idx: {e}") from e
        else:
            logger.info("sparse_embedding_idx already exists, skipping")
        
        logger.info(f"Sparse vector upgrade completed successfully for table '{collection_name}'")
        return True
        
    except RuntimeError:
        raise
    except Exception as e:
        logger.error(f"Sparse vector upgrade failed: {e}", exc_info=True)
        raise RuntimeError(f"Sparse vector upgrade failed: {e}") from e


def downgrade_sparse_vector(memory) -> bool:
    """
    Remove sparse vector support from OceanBase table (rollback operation)
    
    This function removes sparse vector columns and indexes.
    
    Warning: This operation will delete all sparse vector data and is irreversible!
    
    Args:
        memory: Instance of Memory or AsyncMemory class, containing OceanBase configuration
        
    Returns:
        bool: Returns True on successful rollback, False on failure
        
    Raises:
        TypeError: If memory is not a Memory or AsyncMemory instance
        ValueError: If storage is not OceanBase type
    """
    from powermem.core.memory import Memory
    from powermem.core.async_memory import AsyncMemory
    from powermem.storage.oceanbase.oceanbase import OceanBaseVectorStore
    from powermem.utils.oceanbase_util import OceanBaseUtil
    
    # 1. Validate memory parameter type
    if not isinstance(memory, (Memory, AsyncMemory)):
        raise TypeError(
            f"Expected Memory or AsyncMemory instance, got {type(memory).__name__}."
        )
    
    # 2. Get vector_store and validate type
    if not hasattr(memory, 'storage') or not hasattr(memory.storage, 'vector_store'):
        raise ValueError("Memory instance does not have a valid storage adapter.")
    
    vector_store = memory.storage.vector_store
    
    if not isinstance(vector_store, OceanBaseVectorStore):
        raise ValueError(
            f"Storage type is not OceanBase (got {type(vector_store).__name__})."
        )
    
    # 3. Get necessary attributes
    obvector = vector_store.obvector
    collection_name = vector_store.collection_name
    
    logger.info(f"Starting sparse vector downgrade for table '{collection_name}'")
    
    # 4. Check if table exists
    if not OceanBaseUtil.check_table_exists(obvector, collection_name):
        logger.info(f"Table '{collection_name}' does not exist, nothing to downgrade")
        return True
    
    try:
        # 5. Drop sparse_embedding_idx index (if exists)
        if OceanBaseUtil.check_index_exists(obvector, collection_name, 'sparse_embedding_idx'):
            logger.info(f"Dropping sparse vector index from table '{collection_name}'")
            try:
                with obvector.engine.connect() as conn:
                    conn.execute(text(f"DROP INDEX sparse_embedding_idx ON {collection_name}"))
                    conn.commit()
                logger.info("sparse_embedding_idx dropped successfully")
            except Exception as e:
                error_str = str(e).lower()
                if '1091' in str(e) or "doesn't exist" in error_str:
                    logger.info("sparse_embedding_idx does not exist, skipping")
                else:
                    raise RuntimeError(f"Failed to drop sparse_embedding_idx: {e}") from e
        else:
            logger.info("sparse_embedding_idx does not exist, skipping")
        
        # 6. Drop sparse_embedding column (if exists)
        if OceanBaseUtil.check_column_exists(obvector, collection_name, 'sparse_embedding'):
            logger.info(f"Dropping sparse_embedding column from table '{collection_name}'")
            try:
                with obvector.engine.connect() as conn:
                    conn.execute(text(f"ALTER TABLE {collection_name} DROP COLUMN sparse_embedding"))
                    conn.commit()
                logger.info("sparse_embedding column dropped successfully")
            except Exception as e:
                error_str = str(e).lower()
                if '1091' in str(e) or "doesn't exist" in error_str:
                    logger.info("sparse_embedding column does not exist, skipping")
                else:
                    raise RuntimeError(f"Failed to drop sparse_embedding column: {e}") from e
        else:
            logger.info("sparse_embedding column does not exist, skipping")
        
        logger.info(f"Sparse vector downgrade completed successfully for table '{collection_name}'")
        return True
        
    except RuntimeError:
        raise
    except Exception as e:
        logger.error(f"Sparse vector downgrade failed: {e}", exc_info=True)
        raise RuntimeError(f"Sparse vector downgrade failed: {e}") from e