"""Alembic环境配置文件

此文件用于配置Alembic迁移环境，支持从环境变量读取数据库连接信息
"""
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

# 导入ORM模型以支持autogenerate
# 注意：使用默认的MemoryRecord模型（表名: memories, 维度: 1536）
# 对于动态表名，autogenerate可能需要额外配置
from src.powermem.storage.oceanbase.models import Base

# 设置target_metadata以支持autogenerate
target_metadata = Base.metadata


def get_url():
    """从环境变量获取数据库连接URL"""
    try:
        # 导入powermem配置加载器
        from src.powermem.config_loader import load_config_from_env
        
        # 加载配置
        pm_config = load_config_from_env()
        
        # 获取OceanBase连接参数
        vector_store_config = pm_config.get('vector_store', {}).get('config', {})
        connection_args = vector_store_config.get('connection_args', {})
        
        # 如果没有connection_args，尝试直接从config读取
        if not connection_args:
            connection_args = {
                'host': vector_store_config.get('host', os.getenv('OCEANBASE_HOST', '127.0.0.1')),
                'port': vector_store_config.get('port', os.getenv('OCEANBASE_PORT', '2881')),
                'user': vector_store_config.get('user', os.getenv('OCEANBASE_USER', 'root@sys')),
                'password': vector_store_config.get('password', os.getenv('OCEANBASE_PASSWORD', 'password')),
                'db_name': vector_store_config.get('db_name', os.getenv('OCEANBASE_DATABASE', 'powermem'))
            }
        
        host = connection_args.get('host', '127.0.0.1')
        port = connection_args.get('port', 2881)
        user = connection_args.get('user', 'root@sys')
        password = connection_args.get('password', 'password')
        db_name = connection_args.get('db_name', 'powermem')
        
        # 构建MySQL连接URL (OceanBase兼容MySQL协议)
        url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}"
        return url
        
    except Exception as e:
        print(f"Warning: Failed to load config from environment: {e}")
        # 回退到环境变量
        host = os.getenv('OCEANBASE_HOST', '127.0.0.1')
        port = os.getenv('OCEANBASE_PORT', '2881')
        user = os.getenv('OCEANBASE_USER', 'root@sys')
        password = os.getenv('OCEANBASE_PASSWORD', 'password')
        db_name = os.getenv('OCEANBASE_DATABASE', 'powermem')
        
        return f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}"


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

