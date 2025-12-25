"""
OceanBase Schema版本管理模块

此模块负责：
1. 检测当前数据库schema版本
2. 自动执行schema升级
3. 管理Alembic迁移
"""
import logging
import os
from typing import Optional, Tuple
from sqlalchemy import text

logger = logging.getLogger(__name__)

# 常量定义
DEFAULT_SCHEMA_VERSION = "030"
ALEMBIC_VERSION_TABLE = "alembic_version"
SPARSE_EMBEDDING_COLUMN = "sparse_embedding"

# Schema版本定义（使用项目版本号）
SCHEMA_VERSIONS = {
    "020": {
        "alembic_revision": "020_baseline",
        "features": ["dense_vector", "fulltext"],
        "required_columns": ["embedding", "document", "fulltext_content"]
    },
    "030": {
        "alembic_revision": "030_add_sparse_vector",
        "features": ["dense_vector", "fulltext", "sparse_vector"],
        "required_columns": ["embedding", "document", "fulltext_content", "sparse_embedding"]
    }
}


# 辅助函数
def _find_alembic_ini() -> Optional[str]:
    """
    查找 alembic.ini 文件路径
    
    Returns:
        alembic.ini 的绝对路径，如果未找到则返回 None
    """
    # 从当前模块位置向上查找项目根目录
    current_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(
        os.path.join(current_dir, '..', '..', '..', '..')
    )
    alembic_ini = os.path.join(project_root, 'alembic.ini')

    if os.path.exists(alembic_ini):
        return alembic_ini

    # 尝试其他可能的路径
    possible_paths = [
        os.path.join(os.getcwd(), 'alembic.ini'),
        os.path.join(os.path.dirname(os.getcwd()), 'alembic.ini'),
    ]
    for path in possible_paths:
        if os.path.exists(path):
            return path

    return None


def _parse_version_from_revision(revision: Optional[str]) -> Optional[str]:
    """
    从 Alembic revision 名称中解析版本号

    Args:
        revision: Alembic revision 字符串，如 "030_add_sparse_vector"

    Returns:
        版本号字符串（如 "030"），如果无法解析则返回 None
    """
    if not revision:
        return None
    if '_' in revision:
        prefix = revision.split('_')[0]
        if prefix.isdigit():
            return prefix
    return None


def _find_version_by_revision(revision: Optional[str]) -> Optional[str]:
    """
    通过 Alembic revision 查找对应的 schema 版本号

    Args:
        revision: Alembic revision 字符串

    Returns:
        版本号字符串，如果未找到则返回 None
    """
    if not revision:
        return None
    
    # 首先尝试从 SCHEMA_VERSIONS 中精确匹配
    for version, info in SCHEMA_VERSIONS.items():
        if info["alembic_revision"] == revision:
            return version

    # 如果不在 SCHEMA_VERSIONS 中，尝试从 revision 名称推断
    return _parse_version_from_revision(revision)


def _compare_versions(current: str, target: str) -> int:
    """
    比较两个版本号
    
    Args:
        current: 当前版本号
        target: 目标版本号
    
    Returns:
        -1: current < target (需要升级)
         0: current == target (版本相同)
         1: current > target (不需要升级)
    """
    # 特殊情况处理
    if current == "none":
        return -1  # 全新安装
    if current == "unknown":
        return 0  # 未知版本，不确定是否需要升级
    if current == target:
        return 0

    # 尝试数字比较
    try:
        current_num = int(current) if current.isdigit() else 0
        target_num = int(target) if target.isdigit() else 0

        if current_num < target_num:
            return -1
        elif current_num > target_num:
            return 1
        else:
            return 0
    except (ValueError, AttributeError):
        # 如果无法转换为数字，使用字符串比较
        if current < target:
            return -1
        elif current > target:
            return 1
        else:
            return 0


def get_target_schema_version() -> str:
    """
    动态获取目标 schema 版本（从 Alembic 迁移链中获取最新版本）
    
    Returns:
        目标版本字符串，如 "030", "040" 等
    """
    try:
        from alembic.config import Config
        from alembic.script import ScriptDirectory

        # 查找 alembic.ini 文件
        alembic_ini = _find_alembic_ini()
        if not alembic_ini:
            logger.warning(f"alembic.ini not found, using default version {DEFAULT_SCHEMA_VERSION}")
            return DEFAULT_SCHEMA_VERSION

        # 创建 Alembic 配置和脚本目录
        alembic_cfg = Config(alembic_ini)
        script_dir = ScriptDirectory.from_config(alembic_cfg)

        # 获取 head 版本（最新版本）
        head_revision = script_dir.get_current_head()
        if not head_revision:
            logger.warning(f"No head revision found, using default version {DEFAULT_SCHEMA_VERSION}")
            return DEFAULT_SCHEMA_VERSION

        # 从 revision 映射到 schema 版本
        version = _find_version_by_revision(head_revision)
        if version:
            logger.debug(f"Target schema version: {version} (alembic revision: {head_revision})")
            return version

        logger.warning(f"Could not determine schema version from revision {head_revision}, "
                       f"using default {DEFAULT_SCHEMA_VERSION}")
        return DEFAULT_SCHEMA_VERSION

    except Exception as e:
        logger.warning(f"Failed to get target schema version: {e}, using default {DEFAULT_SCHEMA_VERSION}")
        return DEFAULT_SCHEMA_VERSION


class SchemaVersionManager:
    """Schema 版本管理器"""

    def __init__(self, obvector, collection_name: str, embedding_dims: Optional[int] = None,
                 include_sparse: bool = True, connection_args: Optional[dict] = None):
        """
        初始化版本管理器
        
        Args:
            obvector: OceanBase 客户端实例
            collection_name: 表名
            embedding_dims: 向量维度（可选）
            include_sparse: 是否包含稀疏向量（默认 True）
            connection_args: 数据库连接参数（可选，包含 host/port/user/password/db_name）
        """
        self.obvector = obvector
        self.collection_name = collection_name
        self.embedding_dims = embedding_dims
        self.include_sparse = include_sparse
        self.connection_args = connection_args or {}

    def get_current_version(self) -> str:
        """
        获取当前数据库的 schema 版本
        
        Returns:
            版本字符串 ("020", "030", "none", 或 "unknown")
        """
        try:
            with self.obvector.engine.connect() as conn:
                # 1. 检查 alembic_version 表是否存在
                result = conn.execute(text(
                    "SELECT COUNT(*) FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA = DATABASE() "
                    f"AND TABLE_NAME = '{ALEMBIC_VERSION_TABLE}'"
                ))
                alembic_table_exists = result.scalar() > 0

                if alembic_table_exists:
                    # 读取当前版本号
                    result = conn.execute(text(f"SELECT version_num FROM {ALEMBIC_VERSION_TABLE}"))
                    version_num = result.scalar()

                    # 处理表为空的情况
                    if version_num is None:
                        logger.warning(f"{ALEMBIC_VERSION_TABLE} table exists but is empty, "
                                     "will check table structure to determine version")
                        # 继续执行后续的表结构检查逻辑
                    else:
                        # 从 revision 映射到 schema 版本
                        version = _find_version_by_revision(version_num)
                        if version:
                            logger.info(f"Detected schema version: {version} (alembic revision: {version_num})")
                            return version

                        logger.warning(f"Unknown alembic revision: {version_num}")
                        return "unknown"

                # 2. alembic_version 表不存在，检查表是否存在
                result = conn.execute(text(
                    "SELECT COUNT(*) FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA = DATABASE() "
                    f"AND TABLE_NAME = '{self.collection_name}'"
                ))
                table_exists = result.scalar() > 0

                if not table_exists:
                    # 全新安装
                    logger.info("Table does not exist - fresh installation")
                    return "none"

                # 3. 表存在但无 alembic_version，检查 sparse_embedding 列
                result = conn.execute(text(
                    "SELECT COUNT(*) FROM information_schema.COLUMNS "
                    "WHERE TABLE_SCHEMA = DATABASE() "
                    f"AND TABLE_NAME = '{self.collection_name}' "
                    f"AND COLUMN_NAME = '{SPARSE_EMBEDDING_COLUMN}'"
                ))
                has_sparse = result.scalar() > 0

                if has_sparse:
                    logger.info("Detected schema version 030 (manual creation or pre-alembic)")
                    return "030"
                else:
                    logger.info("Detected schema version 020 (legacy)")
                    return "020"

        except Exception as e:
            logger.error(f"Failed to detect schema version: {e}", exc_info=True)
            return "unknown"

    def needs_upgrade(self) -> bool:
        """
        检查是否需要升级
        
        Returns:
            True 表示需要升级，False 表示不需要升级
        """
        current = self.get_current_version()
        target = get_target_schema_version()

        if current == "none":
            # 全新安装，不需要升级（会直接创建最新版本的表）
            logger.info("Fresh installation detected, no upgrade needed")
            return False

        if current == "unknown":
            logger.warning("Cannot determine if upgrade is needed (unknown schema version)")
            return False

        # 使用统一的版本比较函数
        comparison = _compare_versions(current, target)

        if comparison < 0:
            logger.info(f"Schema upgrade needed: {current} -> {target}")
            return True
        else:
            logger.info(f"Schema is up to date (current version: {current})")
            return False

    def run_upgrade(self) -> bool:
        """
        执行 schema 升级
        
        Returns:
            True 表示升级成功，False 表示升级失败
        """
        try:
            current_version = self.get_current_version()
            target_version = get_target_schema_version()

            if current_version == "none":
                logger.info("Fresh installation detected, no upgrade needed")
                return True

            if current_version == "unknown":
                logger.warning("Unknown schema version, cannot determine if upgrade is needed")
                return True

            # 使用统一的版本比较函数
            comparison = _compare_versions(current_version, target_version)
            if comparison >= 0:
                logger.info(f"Schema already at target version: {current_version}")
                return True

            logger.info(f"Starting schema upgrade: {current_version} -> {target_version}")

            # 先初始化 alembic_version 表（如果不存在）
            with self.obvector.engine.connect() as conn:
                result = conn.execute(text(
                    "SELECT COUNT(*) FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA = DATABASE() "
                    f"AND TABLE_NAME = '{ALEMBIC_VERSION_TABLE}'"
                ))
                if result.scalar() == 0:
                    logger.info(f"Initializing {ALEMBIC_VERSION_TABLE} table...")
                    if not self.initialize_alembic_version():
                        logger.error(f"Failed to initialize {ALEMBIC_VERSION_TABLE} table")
                        return False

            # 使用 Alembic API 执行升级
            success = self._run_alembic_upgrade()

            if success:
                logger.info("Schema upgrade completed successfully")
                return True
            else:
                logger.error("Schema upgrade failed")
                return False

        except Exception as e:
            logger.error(f"Failed to run schema upgrade: {e}", exc_info=True)
            return False

    def _run_alembic_upgrade(self) -> bool:
        """
        使用 Alembic API 执行升级
        
        将表名、维度等参数传递给 Alembic env.py
        
        Returns:
            True 表示成功，False 表示失败
        """
        try:
            from alembic.config import Config
            from alembic import command

            # 查找 alembic.ini 文件
            alembic_ini = _find_alembic_ini()
            if not alembic_ini:
                logger.error("alembic.ini not found")
                return False

            # 创建 Alembic 配置
            alembic_cfg = Config(alembic_ini)

            # 通过 attributes 传递所有运行时参数给 env.py
            # 1. 表相关参数
            alembic_cfg.attributes['table_name'] = self.collection_name
            if self.embedding_dims:
                alembic_cfg.attributes['embedding_dims'] = str(self.embedding_dims)
            alembic_cfg.attributes['include_sparse'] = str(self.include_sparse)

            # 2. 数据库连接参数
            if self.connection_args:
                alembic_cfg.attributes['host'] = self.connection_args.get('host', '127.0.0.1')
                alembic_cfg.attributes['port'] = str(self.connection_args.get('port', '2881'))
                alembic_cfg.attributes['user'] = self.connection_args.get('user', 'root@sys')
                alembic_cfg.attributes['password'] = self.connection_args.get('password', 'password')
                alembic_cfg.attributes['db_name'] = self.connection_args.get('db_name', 'powermem')

            # 3. 传递 obvector 对象（用于迁移脚本中的工具函数）
            alembic_cfg.attributes['obvector'] = self.obvector

            logger.info(f"Running Alembic upgrade: table_name={self.collection_name}, "
                        f"embedding_dims={self.embedding_dims}, include_sparse={self.include_sparse}")

            # 执行升级到最新版本
            command.upgrade(alembic_cfg, "head")

            logger.info("Alembic upgrade completed successfully")
            return True

        except ImportError as e:
            logger.error(f"Alembic not installed: {e}")
            logger.error("Please install Alembic: pip install alembic")
            return False
        except Exception as e:
            logger.error(f"Alembic upgrade failed: {e}", exc_info=True)
            return False

    def initialize_alembic_version(self) -> bool:
        """
        初始化 alembic_version 表（用于已存在的旧版本表）

        Returns:
            True 表示成功，False 表示失败
        """
        try:
            with self.obvector.engine.connect() as conn:
                # 创建 alembic_version 表
                conn.execute(text(
                    f"CREATE TABLE IF NOT EXISTS {ALEMBIC_VERSION_TABLE} ("
                    "version_num VARCHAR(32) NOT NULL, "
                    "PRIMARY KEY (version_num)"
                    ")"
                ))

                # 插入 baseline 版本（使用 020_baseline，这是 Alembic 迁移链的起点）
                conn.execute(text(
                    f"INSERT INTO {ALEMBIC_VERSION_TABLE} (version_num) "
                    "VALUES ('020_baseline') "
                    "ON DUPLICATE KEY UPDATE version_num = version_num"
                ))

                logger.info(f"Initialized {ALEMBIC_VERSION_TABLE} table with baseline version (020_baseline)")
                return True

        except Exception as e:
            logger.error(f"Failed to initialize {ALEMBIC_VERSION_TABLE}: {e}", exc_info=True)
            return False


def check_and_upgrade_schema(obvector, collection_name: str, embedding_dims: int,
                             include_sparse: bool, connection_args: dict) -> bool:
    """
    检查并自动升级 schema（如果需要）
    
    Args:
        obvector: OceanBase 客户端实例
        collection_name: 表名
        embedding_dims: 向量维度（用于传递给 Alembic）
        include_sparse: 是否需要稀疏向量支持
        connection_args: 数据库连接参数（用于传递给 Alembic）
    
    Returns:
        True 表示 schema 已就绪（已是最新版本或升级成功）
        False 表示升级失败
    """
    manager = SchemaVersionManager(obvector, collection_name, embedding_dims, include_sparse, connection_args)

    return manager.run_upgrade()
