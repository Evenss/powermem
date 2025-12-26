"""
OceanBase ORM模型定义

此模块定义了OceanBase存储的SQLAlchemy ORM模型。
支持动态表名和向量维度配置。
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

# 缓存动态创建的模型类
_model_cache = {}


def create_memory_model(
    table_name: str,
    embedding_dims: int,
    include_sparse: bool = True,
    primary_field: str = "id",
    vector_field: str = "embedding",
    text_field: str = "document",
    metadata_field: str = "metadata",
    fulltext_field: str = "fulltext_content",
    sparse_vector_field: str = "sparse_embedding"
) -> Type[Base]:
    """
    动态创建Memory模型类
    
    根据传入的表名和向量维度创建专用的ORM模型。
    模型会被缓存以避免重复创建。
    
    Args:
        table_name: 表名
        embedding_dims: 向量维度
        include_sparse: 是否包含稀疏向量列
        primary_field: 主键字段名 (默认: "id")
        vector_field: 向量字段名 (默认: "embedding")
        text_field: 文本字段名 (默认: "document")
        metadata_field: 元数据字段名 (默认: "metadata")
        fulltext_field: 全文搜索字段名 (默认: "fulltext_content")
        sparse_vector_field: 稀疏向量字段名 (默认: "sparse_embedding")
    
    Returns:
        配置好的模型类
    
    Example:
        >>> Model = create_memory_model('my_memories', 1024, include_sparse=True)
        >>> record = Model(id=123, embedding=[...], document="text")
    """
    cache_key = (table_name, embedding_dims, include_sparse, primary_field, 
                 vector_field, text_field, metadata_field, fulltext_field, sparse_vector_field)
    
    if cache_key in _model_cache:
        return _model_cache[cache_key]
    
    # 动态创建类名
    class_name = f"MemoryRecord_{table_name}_{embedding_dims}"
    
    # 定义列
    columns = {
        '__tablename__': table_name,
        '__table_args__': {'extend_existing': True},
        
        # 主键 - 使用Snowflake ID (BIGINT without AUTO_INCREMENT)
        primary_field: Column(BigInteger, primary_key=True, autoincrement=False),
        
        # 向量字段
        vector_field: Column(VECTOR(embedding_dims), nullable=False),
        
        # 文本字段
        text_field: Column(LONGTEXT, nullable=False),
        
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
        
        # 全文搜索字段
        fulltext_field: Column(LONGTEXT, nullable=True),
        
        # 添加 __repr__ 方法
        '__repr__': lambda self: f"<{class_name}(id={getattr(self, primary_field)}, user_id={self.user_id}, agent_id={self.agent_id})>"
    }
    
    # 添加元数据字段 (JSON)
    # 如果 metadata_field 是 'metadata'，与 SQLAlchemy 保留字冲突，使用属性映射
    if metadata_field == 'metadata':
        columns['metadata_'] = Column('metadata', JSON, nullable=True)
    else:
        columns[metadata_field] = Column(JSON, nullable=True)
    
    # 添加稀疏向量列（如果启用）
    if include_sparse:
        columns[sparse_vector_field] = Column(SPARSE_VECTOR, nullable=True)
    
    # 动态创建类并继承自Base
    model_class = type(class_name, (Base,), columns)
    
    # 缓存模型类
    _model_cache[cache_key] = model_class
    
    return model_class

