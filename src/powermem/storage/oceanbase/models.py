"""
OceanBase ORM模型定义

此模块定义了OceanBase存储的SQLAlchemy ORM模型。
支持动态表名和向量维度配置，兼容Alembic自动迁移检测。
"""
from typing import Optional, Type

from sqlalchemy import BigInteger, String, JSON, Column
from sqlalchemy.orm import declarative_base
from sqlalchemy.dialects.mysql import LONGTEXT

try:
    from pyobvector import VECTOR, SPARSE_VECTOR
except ImportError:
    raise ImportError(
        "pyobvector is required for ORM models. "
        "Please install it: pip install pyobvector"
    )

# 创建声明式基类
Base = declarative_base()

# 默认配置
DEFAULT_TABLE_NAME = 'memories'
DEFAULT_EMBEDDING_DIMS = 1536


class MemoryRecord(Base):
    """
    内存记录ORM模型
    
    这是默认的模型定义，使用固定的表名和向量维度。
    对于需要动态配置的场景，请使用 create_memory_model() 工厂函数。
    """
    __tablename__ = DEFAULT_TABLE_NAME
    __table_args__ = {'extend_existing': True}
    
    # 主键 - 使用Snowflake ID
    id = Column(BigInteger, primary_key=True, autoincrement=False)
    
    # 向量字段
    embedding = Column(VECTOR(DEFAULT_EMBEDDING_DIMS), nullable=False)
    sparse_embedding = Column(SPARSE_VECTOR, nullable=True)
    
    # 文本字段
    document = Column(LONGTEXT, nullable=False)
    fulltext_content = Column(LONGTEXT, nullable=True)
    
    # 元数据 (注意：metadata是SQLAlchemy保留字，使用metadata_字段映射到'metadata'列)
    metadata_ = Column('metadata', JSON, nullable=True)
    
    # 标识字段
    user_id = Column(String(128), nullable=True)
    agent_id = Column(String(128), nullable=True)
    run_id = Column(String(128), nullable=True)
    actor_id = Column(String(128), nullable=True)
    
    # 其他字段
    hash = Column(String(32), nullable=True)
    created_at = Column(String(128), nullable=True)
    updated_at = Column(String(128), nullable=True)
    category = Column(String(64), nullable=True)
    
    def __repr__(self):
        return f"<MemoryRecord(id={self.id}, user_id={self.user_id}, agent_id={self.agent_id})>"


# 缓存动态创建的模型类
_model_cache = {}


def create_memory_model(
    table_name: str,
    embedding_dims: int,
    include_sparse: bool = True
) -> Type[Base]:
    """
    动态创建Memory模型类
    
    为不同的collection_name和embedding_dims创建专用的ORM模型。
    模型会被缓存以避免重复创建。
    
    Args:
        table_name: 表名
        embedding_dims: 向量维度
        include_sparse: 是否包含稀疏向量列
    
    Returns:
        配置好的模型类
    
    Example:
        >>> Model = create_memory_model('my_memories', 1024)
        >>> record = Model(id=123, embedding=[...], document="text")
    """
    cache_key = (table_name, embedding_dims, include_sparse)
    
    if cache_key in _model_cache:
        return _model_cache[cache_key]
    
    # 动态创建类
    class_name = f"DynamicMemoryRecord_{table_name}_{embedding_dims}"
    
    # 定义列
    columns = {
        '__tablename__': table_name,
        '__table_args__': {'extend_existing': True},
        
        # 主键
        'id': Column(BigInteger, primary_key=True, autoincrement=False),
        
        # 向量字段
        'embedding': Column(VECTOR(embedding_dims), nullable=False),
        
        # 文本字段
        'document': Column(LONGTEXT, nullable=False),
        'fulltext_content': Column(LONGTEXT, nullable=True),
        
        # 元数据 (metadata是SQLAlchemy保留字，使用metadata_映射)
        'metadata_': Column('metadata', JSON, nullable=True),
        
        # 标识字段
        'user_id': Column(String(128), nullable=True),
        'agent_id': Column(String(128), nullable=True),
        'run_id': Column(String(128), nullable=True),
        'actor_id': Column(String(128), nullable=True),
        
        # 其他字段
        'hash': Column(String(32), nullable=True),
        'created_at': Column(String(128), nullable=True),
        'updated_at': Column(String(128), nullable=True),
        'category': Column(String(64), nullable=True),
    }
    
    # 添加稀疏向量列（如果启用）
    if include_sparse:
        columns['sparse_embedding'] = Column(SPARSE_VECTOR, nullable=True)
    
    # 动态创建类
    model_class = type(class_name, (Base,), columns)
    
    # 缓存
    _model_cache[cache_key] = model_class
    
    return model_class


def get_model_for_table(
    table_name: str,
    embedding_dims: int = DEFAULT_EMBEDDING_DIMS,
    include_sparse: bool = True
) -> Type[Base]:
    """
    获取指定表的模型类
    
    如果是默认表名和维度，返回MemoryRecord类。
    否则使用工厂函数创建动态模型。
    
    Args:
        table_name: 表名
        embedding_dims: 向量维度
        include_sparse: 是否包含稀疏向量
    
    Returns:
        模型类
    """
    if table_name == DEFAULT_TABLE_NAME and embedding_dims == DEFAULT_EMBEDDING_DIMS:
        return MemoryRecord
    
    return create_memory_model(table_name, embedding_dims, include_sparse)

