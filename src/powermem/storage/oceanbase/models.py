"""
OceanBase ORM模型定义

此模块定义了OceanBase存储的SQLAlchemy ORM模型。
支持动态表名和向量维度配置，兼容Alembic自动迁移检测。

使用方式：

1. 运行时使用（OceanBaseVectorStore）：
   通过初始化参数指定所有配置，系统会自动处理 schema 迁移：
   
   from powermem.storage.oceanbase import OceanBaseVectorStore
   
   store = OceanBaseVectorStore(
       collection_name="my_custom_table",  # 自定义表名
       embedding_model_dims=768,           # 自定义向量维度
       include_sparse=True,                # 是否支持稀疏向量
       connection_args={
           'host': '127.0.0.1',
           'port': '2881',
           'user': 'root@sys',
           'password': 'password',
           'db_name': 'powermem'
       }
   )
   
   # 初始化时会自动：
   # 1. 检测当前数据库 schema 版本
   # 2. 如需升级，自动调用 Alembic 并传递所有参数
   # 3. 创建或更新表结构

2. Alembic 迁移管理：
   所有配置参数（表名、维度、连接信息）都由 OceanBaseVectorStore 自动传递给 Alembic。
   env.py 从 config.attributes 读取这些参数，无需手动配置环境变量或配置文件。

工作原理：
   - OceanBaseVectorStore.__init__() 接收所有参数
   - 调用 check_and_upgrade_schema() 时传递这些参数
   - SchemaVersionManager 将参数设置到 alembic_cfg.attributes
   - alembic/env.py 从 config.attributes 读取并应用这些参数

优势：
   ✅ 配置集中：所有参数在代码初始化时传入
   ✅ 自动迁移：无需手动执行 alembic 命令
   ✅ 类型安全：参数在 Python 代码中直接传递
   ✅ 零配置：不依赖环境变量或配置文件
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
    include_sparse: bool = True
) -> Type[Base]:
    """
    动态创建Memory模型类
    
    根据传入的表名和向量维度创建专用的ORM模型。
    模型会被缓存以避免重复创建。
    
    Args:
        table_name: 表名
        embedding_dims: 向量维度
        include_sparse: 是否包含稀疏向量列
    
    Returns:
        配置好的模型类
    
    Example:
        >>> Model = create_memory_model('my_memories', 1024, include_sparse=True)
        >>> record = Model(id=123, embedding=[...], document="text")
    
    Note:
        此函数由 alembic/env.py 调用，使用从 config.attributes 读取的参数。
        运行时和迁移时使用相同的参数确保表结构一致。
    """
    cache_key = (table_name, embedding_dims, include_sparse)
    
    if cache_key in _model_cache:
        return _model_cache[cache_key]
    
    # 动态创建类名
    class_name = f"MemoryRecord_{table_name}_{embedding_dims}"
    
    # 定义列
    columns = {
        '__tablename__': table_name,
        '__table_args__': {'extend_existing': True},
        
        # 主键 - 使用Snowflake ID
        'id': Column(BigInteger, primary_key=True, autoincrement=False),
        
        # 向量字段
        'embedding': Column(VECTOR(embedding_dims), nullable=False),
        
        # 文本字段
        'document': Column(LONGTEXT, nullable=False),
        'fulltext_content': Column(LONGTEXT, nullable=True),
        
        # 元数据 (metadata是SQLAlchemy保留字，使用metadata_映射到'metadata'列)
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
        
        # 添加 __repr__ 方法
        '__repr__': lambda self: f"<{class_name}(id={self.id}, user_id={self.user_id}, agent_id={self.agent_id})>"
    }
    
    # 添加稀疏向量列（如果启用）
    if include_sparse:
        columns['sparse_embedding'] = Column(SPARSE_VECTOR, nullable=True)
    
    # 动态创建类并继承自Base
    model_class = type(class_name, (Base,), columns)
    
    # 缓存模型类
    _model_cache[cache_key] = model_class
    
    return model_class

