"""Add sparse vector support

Revision ID: 030_add_sparse_vector
Revises: 020_baseline
Create Date: 2025-01-01 00:01:00.000000

This migration adds sparse vector support to the existing schema:
- Adds sparse_embedding column (SPARSEVECTOR type)
- Creates sparse vector index
"""
from typing import Sequence, Union
import logging
import re
import os

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = '030_add_sparse_vector'
down_revision: Union[str, None] = '020_baseline'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

logger = logging.getLogger('alembic.runtime.migration')


def get_table_name() -> str:
    """从环境变量或配置获取表名"""
    try:
        import sys
        sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
        from src.powermem.config_loader import load_config_from_env
        
        config = load_config_from_env()
        table_name = config.get('vector_store', {}).get('config', {}).get('collection_name')
        if table_name:
            return table_name
    except Exception as e:
        logger.warning(f"Failed to load table name from config: {e}")
    
    # 回退到环境变量
    return os.getenv('OCEANBASE_COLLECTION', 'memories')


def check_sparse_vector_support(connection) -> bool:
    """检查数据库是否支持SPARSEVECTOR类型"""
    try:
        # 获取版本信息
        result = connection.execute(text("SELECT VERSION()"))
        version_str = result.scalar()
        
        # 检查是否是seekdb
        if 'seekdb' in version_str.lower():
            logger.info("Detected seekdb, sparse vector is supported")
            return True
        
        # 检查OceanBase版本
        # 提取版本号 (格式如: 4.5.0-100000012025010101)
        match = re.search(r'(\d+)\.(\d+)\.(\d+)', version_str)
        if match:
            major, minor, patch = map(int, match.groups())
            if major > 4 or (major == 4 and minor >= 5):
                logger.info(f"OceanBase version {major}.{minor}.{patch} supports sparse vector")
                return True
            else:
                logger.warning(
                    f"OceanBase version {major}.{minor}.{patch} does not support sparse vector. "
                    "Requires OceanBase >= 4.5.0"
                )
                return False
        
        logger.warning("Could not determine database version")
        return False
        
    except Exception as e:
        logger.error(f"Failed to check sparse vector support: {e}")
        return False


def column_exists(connection, table_name: str, column_name: str) -> bool:
    """检查列是否存在"""
    try:
        result = connection.execute(text(
            f"SELECT COUNT(*) FROM information_schema.COLUMNS "
            f"WHERE TABLE_SCHEMA = DATABASE() "
            f"AND TABLE_NAME = '{table_name}' "
            f"AND COLUMN_NAME = '{column_name}'"
        ))
        return result.scalar() > 0
    except Exception as e:
        logger.error(f"Failed to check if column exists: {e}")
        return False


def index_exists(connection, table_name: str, index_name: str) -> bool:
    """检查索引是否存在"""
    try:
        result = connection.execute(text(
            f"SELECT COUNT(*) FROM information_schema.STATISTICS "
            f"WHERE TABLE_SCHEMA = DATABASE() "
            f"AND TABLE_NAME = '{table_name}' "
            f"AND INDEX_NAME = '{index_name}'"
        ))
        return result.scalar() > 0
    except Exception as e:
        logger.error(f"Failed to check if index exists: {e}")
        return False


def table_exists(connection, table_name: str) -> bool:
    """检查表是否存在"""
    try:
        result = connection.execute(text(
            f"SELECT COUNT(*) FROM information_schema.TABLES "
            f"WHERE TABLE_SCHEMA = DATABASE() "
            f"AND TABLE_NAME = '{table_name}'"
        ))
        return result.scalar() > 0
    except Exception as e:
        logger.error(f"Failed to check if table exists: {e}")
        return False


def upgrade() -> None:
    """
    Upgrade to version 030 with sparse vector support.
    
    Adds:
    1. sparse_embedding column (SPARSEVECTOR type, nullable)
    2. sparse_embedding_idx vector index
    """
    connection = op.get_bind()
    table_name = get_table_name()
    
    logger.info(f"Starting upgrade to add sparse vector support for table '{table_name}'")
    
    # 检查表是否存在
    if not table_exists(connection, table_name):
        logger.info(f"Table '{table_name}' does not exist, skipping migration")
        return
    
    # 检查数据库是否支持稀疏向量
    if not check_sparse_vector_support(connection):
        logger.warning(
            "Database does not support SPARSEVECTOR type. "
            "Sparse vector features will not be available. "
            "Please upgrade to seekdb or OceanBase >= 4.5.0 for sparse vector support."
        )
        # 不抛出异常，允许迁移继续（只是没有稀疏向量支持）
        return
    
    # 1. 添加 sparse_embedding 列（如果不存在）
    if not column_exists(connection, table_name, 'sparse_embedding'):
        logger.info(f"Adding sparse_embedding column to table '{table_name}'")
        connection.execute(text(
            f"ALTER TABLE {table_name} ADD COLUMN sparse_embedding SPARSEVECTOR"
        ))
        connection.commit()
        logger.info("sparse_embedding column added successfully")
    else:
        logger.info("sparse_embedding column already exists, skipping")
    
    # 2. 创建稀疏向量索引（如果不存在）
    if not index_exists(connection, table_name, 'sparse_embedding_idx'):
        logger.info(f"Creating sparse vector index on table '{table_name}'")
        connection.execute(text(
            f"CREATE VECTOR INDEX sparse_embedding_idx ON {table_name}(sparse_embedding) "
            f"WITH (lib=vsag, type=sindi, distance=inner_product)"
        ))
        connection.commit()
        logger.info("sparse_embedding_idx created successfully")
    else:
        logger.info("sparse_embedding_idx already exists, skipping")
    
    logger.info("Upgrade to version 030 completed successfully")


def downgrade() -> None:
    """
    Downgrade from version 030 to 020 (remove sparse vector support).
    
    Removes:
    1. sparse_embedding_idx vector index
    2. sparse_embedding column
    """
    connection = op.get_bind()
    table_name = get_table_name()
    
    logger.info(f"Starting downgrade to remove sparse vector support from table '{table_name}'")
    
    # 检查表是否存在
    if not table_exists(connection, table_name):
        logger.info(f"Table '{table_name}' does not exist, skipping downgrade")
        return
    
    # 1. 删除稀疏向量索引（如果存在）
    if index_exists(connection, table_name, 'sparse_embedding_idx'):
        logger.info(f"Dropping sparse vector index from table '{table_name}'")
        connection.execute(text(
            f"DROP INDEX sparse_embedding_idx ON {table_name}"
        ))
        connection.commit()
        logger.info("sparse_embedding_idx dropped successfully")
    else:
        logger.info("sparse_embedding_idx does not exist, skipping")
    
    # 2. 删除 sparse_embedding 列（如果存在）
    if column_exists(connection, table_name, 'sparse_embedding'):
        logger.info(f"Dropping sparse_embedding column from table '{table_name}'")
        connection.execute(text(
            f"ALTER TABLE {table_name} DROP COLUMN sparse_embedding"
        ))
        connection.commit()
        logger.info("sparse_embedding column dropped successfully")
    else:
        logger.info("sparse_embedding column does not exist, skipping")
    
    logger.info("Downgrade to version 020 completed successfully")

