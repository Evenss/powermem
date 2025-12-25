"""Alembic环境配置文件

此文件用于配置Alembic迁移环境，支持从环境变量读取数据库连接信息
"""
import logging
import os
import sys
from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# 获取logger实例
logger = logging.getLogger('alembic.env')

# 导入ORM模型（主要用于 autogenerate 功能）
# 注意：当前项目使用手动编写的迁移脚本，ORM模型在迁移执行时不是必需的
# 但如果将来需要使用 `alembic revision --autogenerate`，则需要这些代码
from src.powermem.storage.oceanbase.models import Base, create_memory_model

# 动态获取表名和向量维度配置
_table_name = config.attributes.get('table_name')
_embedding_dims = config.attributes.get('embedding_dims')
_include_sparse = config.attributes.get('include_sparse')

# 如果提供了参数，创建ORM模型（用于autogenerate）
if _table_name and _embedding_dims:
    _embedding_dims = int(_embedding_dims)
    _include_sparse = str(_include_sparse).lower() in ('true', '1', 'yes') if _include_sparse is not None else True
    
    # 动态创建模型并注册到Base.metadata
    ModelClass = create_memory_model(_table_name, _embedding_dims, _include_sparse)
    logger.info(f"Created ORM model: {_table_name} (dims={_embedding_dims}, sparse={_include_sparse})")

# 设置target_metadata（用于autogenerate功能）
target_metadata = Base.metadata


def get_url():
    """
    从 config.attributes 获取数据库连接URL
    
    所有连接参数都是必需的，不提供默认值
    
    Raises:
        ValueError: 如果必需参数未提供
    """
    # 从程序传入的参数读取连接信息
    host = config.attributes.get('host')
    port = config.attributes.get('port')
    user = config.attributes.get('user')
    password = config.attributes.get('password')
    db_name = config.attributes.get('db_name')
    
    # 验证必需参数
    if host is None:
        raise ValueError("host is required in config.attributes")
    if port is None:
        raise ValueError("port is required in config.attributes")
    if user is None:
        raise ValueError("user is required in config.attributes")
    if password is None:
        raise ValueError("password is required in config.attributes")
    if db_name is None:
        raise ValueError("db_name is required in config.attributes")
    
    # 构建MySQL连接URL (OceanBase兼容MySQL协议)
    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}"
    return url


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.
    """
    url = get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.
    """
    # 覆盖配置文件中的URL
    configuration = config.get_section(config.config_ini_section)
    configuration["sqlalchemy.url"] = get_url()
    
    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()

