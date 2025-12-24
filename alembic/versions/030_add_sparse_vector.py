"""Add sparse vector support

Revision ID: 030_add_sparse_vector
Revises: 020_baseline

This migration adds sparse vector support to the existing schema:
- Adds sparse_embedding column (SPARSE_VECTOR type from pyobvector)
- Creates sparse vector index
"""
import logging
import os
import sys
from typing import Sequence, Union

from alembic import op
from sqlalchemy import Column

try:
    from pyobvector import SPARSE_VECTOR
except ImportError:
    raise ImportError(
        "pyobvector is required for this migration. "
        "Please install it: pip install pyobvector"
    )

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.powermem.utils.oceanbase_util import OceanBaseUtil

# revision identifiers, used by Alembic.
revision: str = '030_add_sparse_vector'
down_revision: Union[str, None] = '020_baseline'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

logger = logging.getLogger('alembic.runtime.migration')


def _get_table_name() -> str:
    """
    从 Alembic config.attributes 获取表名
    
    表名由程序运行时（OceanBaseVectorStore初始化）通过 config.attributes 传入
    
    Returns:
        表名
    
    Raises:
        ValueError: 如果表名未在 config.attributes 中提供
    """
    try:
        from alembic import context
        config = context.config
        table_name = config.attributes.get('table_name')
        
        if table_name is None or table_name == '':
            raise ValueError(
                "table_name is required in config.attributes. "
                "This should be set by OceanBaseVectorStore during schema upgrade."
            )
        
        logger.info(f"Using table name from config.attributes: {table_name}")
        return table_name
    except Exception as e:
        logger.error(f"Failed to get table name from config.attributes: {e}")
        raise

def _get_obvector():
    """
    从 config.attributes 获取 obvector 对象
    
    Returns:
        obvector instance or None
    """
    try:
        from alembic import context
        config = context.config
        return config.attributes.get('obvector')
    except Exception as e:
        logger.error(f"Failed to get obvector from config: {e}")
        return None

def upgrade() -> None:
    """
    Upgrade to version 030 with sparse vector support.
    
    Uses Alembic standard operations:
    1. op.add_column() for adding sparse_embedding column
    2. op.execute() for creating special vector index
    """
    table_name = _get_table_name()
    obvector = _get_obvector()

    logger.info(f"Starting upgrade to add sparse vector support for table '{table_name}'")
    
    # 检查表是否存在
    if not OceanBaseUtil.check_table_exists(obvector, table_name):
        logger.info(f"Table '{table_name}' does not exist, skipping migration")
        return
    
    # 检查数据库是否支持稀疏向量
    if not OceanBaseUtil.check_sparse_vector_version_support(obvector):
        logger.warning(
            "Database does not support SPARSE_VECTOR type. "
            "Sparse vector features will not be available. "
            "Please upgrade to seekdb or OceanBase >= 4.5.0 for sparse vector support."
        )
        # 不抛出异常，允许迁移继续（只是没有稀疏向量支持）
        return
    
    # 1. 添加 sparse_embedding 列（使用Alembic标准操作）
    # 优先使用 ORM 检查，失败则使用 OceanBaseUtil
    has_sparse_column = OceanBaseUtil.check_column_exists(obvector, table_name, 'sparse_embedding')
    
    if not has_sparse_column:
        logger.info(f"Adding sparse_embedding column to table '{table_name}'")
        op.add_column(
            table_name,
            Column('sparse_embedding', SPARSE_VECTOR, nullable=True)
        )
        logger.info("sparse_embedding column added successfully")
    else:
        logger.info("sparse_embedding column already exists (checked via ORM/Inspector or SQL), skipping")
    
    # 2. 创建稀疏向量索引（OceanBase特殊语法，使用op.execute）
    if not OceanBaseUtil.check_index_exists(obvector, table_name, 'sparse_embedding_idx'):
        logger.info(f"Creating sparse vector index on table '{table_name}'")
        op.execute(
            f"CREATE VECTOR INDEX sparse_embedding_idx ON {table_name}(sparse_embedding) "
            f"WITH (lib=vsag, type=sindi, distance=inner_product)"
        )
        logger.info("sparse_embedding_idx created successfully")
    else:
        logger.info("sparse_embedding_idx already exists, skipping")
    
    logger.info("Upgrade to version 030 completed successfully")


def downgrade() -> None:
    """
    Downgrade from version 030 to 020 (remove sparse vector support).
    
    Uses Alembic standard operations:
    1. op.drop_index() for removing vector index
    2. op.drop_column() for removing sparse_embedding column
    """
    table_name = _get_table_name()
    obvector = _get_obvector()
    
    logger.info(f"Starting downgrade to remove sparse vector support from table '{table_name}'")
    
    # 检查表是否存在
    if not OceanBaseUtil.check_table_exists(obvector, table_name):
        logger.info(f"Table '{table_name}' does not exist, skipping downgrade")
        return
    
    # 1. 删除稀疏向量索引（使用op.execute因为是特殊索引类型）
    if OceanBaseUtil.check_index_exists(obvector, table_name, 'sparse_embedding_idx'):
        logger.info(f"Dropping sparse vector index from table '{table_name}'")
        op.execute(f"DROP INDEX sparse_embedding_idx ON {table_name}")
        logger.info("sparse_embedding_idx dropped successfully")
    else:
        logger.info("sparse_embedding_idx does not exist, skipping")
    
    # 2. 删除 sparse_embedding 列（使用Alembic标准操作）
    if OceanBaseUtil.check_column_exists(obvector, table_name, 'sparse_embedding'):
        logger.info(f"Dropping sparse_embedding column from table '{table_name}'")
        op.drop_column(table_name, 'sparse_embedding')
        logger.info("sparse_embedding column dropped successfully")
    else:
        logger.info("sparse_embedding column does not exist, skipping")
    
    logger.info("Downgrade to version 020 completed successfully")
