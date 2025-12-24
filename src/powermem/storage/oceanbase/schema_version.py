"""
OceanBase Schema版本管理模块

此模块负责：
1. 检测当前数据库schema版本
2. 自动执行schema升级
3. 管理Alembic迁移
"""
import logging
import os
from typing import Optional
from sqlalchemy import text

logger = logging.getLogger(__name__)

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

# 当前SDK要求的schema版本（动态获取，如果获取失败则使用默认值）
def get_target_schema_version() -> str:
    """
    动态获取目标schema版本（从Alembic迁移链中获取最新版本）
    
    Returns:
        目标版本字符串，如 "030", "040" 等
    """
    try:
        from alembic.config import Config
        from alembic.script import ScriptDirectory
        
        # 获取alembic.ini路径
        current_dir = os.path.dirname(__file__)
        project_root = os.path.abspath(
            os.path.join(current_dir, '..', '..', '..', '..')
        )
        alembic_ini = os.path.join(project_root, 'alembic.ini')
        
        if not os.path.exists(alembic_ini):
            # 尝试其他可能的路径
            possible_paths = [
                os.path.join(os.getcwd(), 'alembic.ini'),
                os.path.join(os.path.dirname(os.getcwd()), 'alembic.ini'),
            ]
            for path in possible_paths:
                if os.path.exists(path):
                    alembic_ini = path
                    break
            else:
                logger.warning("alembic.ini not found, using default version 030")
                return "030"
        
        # 创建Alembic配置和脚本目录
        alembic_cfg = Config(alembic_ini)
        script_dir = ScriptDirectory.from_config(alembic_cfg)
        
        # 获取head版本（最新版本）
        head_revision = script_dir.get_current_head()
        if not head_revision:
            logger.warning("No head revision found, using default version 030")
            return "030"
        
        # 从revision映射到schema版本
        # 首先尝试从SCHEMA_VERSIONS中查找
        for version, info in SCHEMA_VERSIONS.items():
            if info["alembic_revision"] == head_revision:
                logger.debug(f"Target schema version: {version} (alembic: {head_revision})")
                return version
        
        # 如果不在SCHEMA_VERSIONS中，尝试从revision名称推断（格式：030_xxx）
        # 提取版本号前缀（如 "030_add_sparse_vector" -> "030"）
        if '_' in head_revision:
            prefix = head_revision.split('_')[0]
            if prefix.isdigit():
                logger.debug(f"Target schema version inferred from revision: {prefix}")
                return prefix
        
        logger.warning(f"Could not determine schema version from revision {head_revision}, using default 030")
        return "030"
        
    except Exception as e:
        logger.warning(f"Failed to get target schema version: {e}, using default 030")
        return "030"


class SchemaVersionManager:
    """Schema版本管理器"""
    
    def __init__(self, obvector, collection_name: str, embedding_dims: Optional[int] = None, 
                 include_sparse: bool = True, connection_args: Optional[dict] = None):
        """
        初始化版本管理器
        
        Args:
            obvector: OceanBase客户端实例
            collection_name: 表名
            embedding_dims: 向量维度（可选）
            include_sparse: 是否包含稀疏向量（默认True）
            connection_args: 数据库连接参数（可选，包含host/port/user/password/db_name）
        """
        self.obvector = obvector
        self.collection_name = collection_name
        self.embedding_dims = embedding_dims
        self.include_sparse = include_sparse
        self.connection_args = connection_args or {}
    
    def get_current_version(self) -> str:
        """
        获取当前数据库的schema版本
        
        Returns:
            版本字符串 ("020", "030", "none", 或 "unknown")
        """
        try:
            with self.obvector.engine.connect() as conn:
                # 1. 检查alembic_version表是否存在
                result = conn.execute(text(
                    "SELECT COUNT(*) FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA = DATABASE() "
                    "AND TABLE_NAME = 'alembic_version'"
                ))
                alembic_table_exists = result.scalar() > 0
                
                if alembic_table_exists:
                    # 读取当前版本号
                    result = conn.execute(text("SELECT version_num FROM alembic_version"))
                    version_num = result.scalar()
                    
                    # 映射到schema版本
                    for version, info in SCHEMA_VERSIONS.items():
                        if info["alembic_revision"] == version_num:
                            logger.info(f"Detected schema version: {version} (alembic: {version_num})")
                            return version
                    
                    # 如果不在SCHEMA_VERSIONS中，尝试从revision名称推断（格式：040_xxx -> 040）
                    if '_' in version_num:
                        prefix = version_num.split('_')[0]
                        if prefix.isdigit():
                            logger.info(f"Detected schema version from revision: {prefix} (alembic: {version_num})")
                            return prefix
                    
                    logger.warning(f"Unknown alembic revision: {version_num}")
                    return "unknown"
                
                # 2. alembic_version表不存在，检查表是否存在
                result = conn.execute(text(
                    f"SELECT COUNT(*) FROM information_schema.TABLES "
                    f"WHERE TABLE_SCHEMA = DATABASE() "
                    f"AND TABLE_NAME = '{self.collection_name}'"
                ))
                table_exists = result.scalar() > 0
                
                if not table_exists:
                    # 全新安装
                    logger.info("Table does not exist - fresh installation")
                    return "none"
                
                # 3. 表存在但无alembic_version，检查sparse_embedding列
                result = conn.execute(text(
                    f"SELECT COUNT(*) FROM information_schema.COLUMNS "
                    f"WHERE TABLE_SCHEMA = DATABASE() "
                    f"AND TABLE_NAME = '{self.collection_name}' "
                    f"AND COLUMN_NAME = 'sparse_embedding'"
                ))
                has_sparse = result.scalar() > 0
                
                if has_sparse:
                    logger.info("Detected version 030 schema (manual creation or pre-alembic)")
                    return "030"
                else:
                    logger.info("Detected version 020 schema (legacy)")
                    return "020"
                    
        except Exception as e:
            logger.error(f"Failed to detect schema version: {e}", exc_info=True)
            return "unknown"
    
    def needs_upgrade(self) -> bool:
        """
        检查是否需要升级
        
        Returns:
            True if upgrade is needed, False otherwise
        """
        current = self.get_current_version()
        target = get_target_schema_version()
        
        if current == "none":
            # 全新安装，不需要升级（会直接创建v2表）
            return False
        
        if current == "unknown":
            logger.warning("Cannot determine if upgrade is needed (unknown version)")
            return False
        
        # 版本比较：将字符串版本转换为数字进行比较
        try:
            current_num = int(current) if current.isdigit() else 0
            target_num = int(target) if target.isdigit() else 0
            
            if current_num >= target_num:
                logger.info(f"Schema is up to date (version: {current})")
                return False
            
            # 版本过旧，需要升级
            logger.info(f"Schema upgrade needed: {current} -> {target}")
            return True
        except (ValueError, AttributeError):
            # 如果版本号不是数字格式，使用字符串比较
            if current == target:
                logger.info(f"Schema is up to date (version: {current})")
                return False
            logger.info(f"Schema upgrade needed: {current} -> {target}")
            return True
    
    def run_upgrade(self) -> bool:
        """
        执行schema升级
        
        Returns:
            True if upgrade succeeded, False otherwise
        """
        try:
            current_version = self.get_current_version()
            target_version = get_target_schema_version()
            
            if current_version == "none":
                logger.info("Fresh installation, no upgrade needed")
                return True
            
            # 版本比较
            try:
                current_num = int(current_version) if current_version.isdigit() else 0
                target_num = int(target_version) if target_version.isdigit() else 0
                if current_num >= target_num:
                    logger.info(f"Schema already at target version: {current_version}")
                    return True
            except (ValueError, AttributeError):
                if current_version == target_version:
                    logger.info(f"Schema already at target version: {current_version}")
                    return True
            
            logger.info(f"Starting schema upgrade: {current_version} -> {target_version}")
            
            # 使用Alembic API执行升级
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
        使用Alembic API执行升级
        
        将表名、维度等参数传递给Alembic env.py
        
        Returns:
            True if successful, False otherwise
        """
        try:
            from alembic.config import Config
            from alembic import command
            
            # 获取alembic.ini路径
            # 从当前模块位置向上查找项目根目录
            current_dir = os.path.dirname(__file__)
            project_root = os.path.abspath(
                os.path.join(current_dir, '..', '..', '..', '..')
            )
            alembic_ini = os.path.join(project_root, 'alembic.ini')
            
            if not os.path.exists(alembic_ini):
                # 尝试其他可能的路径
                possible_paths = [
                    os.path.join(os.getcwd(), 'alembic.ini'),
                    os.path.join(os.path.dirname(os.getcwd()), 'alembic.ini'),
                ]
                for path in possible_paths:
                    if os.path.exists(path):
                        alembic_ini = path
                        break
                else:
                    logger.error(f"alembic.ini not found at {alembic_ini} or other locations")
                    return False
            
            # 创建Alembic配置
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
            
            logger.info(f"Running alembic upgrade with table_name={self.collection_name}, "
                       f"embedding_dims={self.embedding_dims}, include_sparse={self.include_sparse}")
            
            # 执行升级到最新版本
            logger.info("Running alembic upgrade head...")
            command.upgrade(alembic_cfg, "head")
            
            logger.info("Alembic upgrade completed")
            return True
            
        except ImportError as e:
            logger.error(f"Alembic not installed: {e}")
            logger.error("Please install alembic: pip install alembic")
            return False
        except Exception as e:
            logger.error(f"Alembic upgrade failed: {e}", exc_info=True)
            return False
    
    def initialize_alembic_version(self) -> bool:
        """
        初始化alembic_version表（用于已存在的v1表）
        
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.obvector.engine.connect() as conn:
                # 创建alembic_version表
                conn.execute(text(
                    "CREATE TABLE IF NOT EXISTS alembic_version ("
                    "version_num VARCHAR(32) NOT NULL, "
                    "PRIMARY KEY (version_num)"
                    ")"
                ))
                
                # 插入baseline版本
                conn.execute(text(
                    "INSERT INTO alembic_version (version_num) "
                    "VALUES ('000_baseline_v1') "
                    "ON DUPLICATE KEY UPDATE version_num = version_num"
                ))
                
                conn.commit()
                logger.info("Initialized alembic_version table with baseline v1")
                return True
                
        except Exception as e:
            logger.error(f"Failed to initialize alembic_version: {e}", exc_info=True)
            return False


def check_and_upgrade_schema(obvector, collection_name: str, include_sparse: bool = False, 
                           embedding_dims: Optional[int] = None, connection_args: Optional[dict] = None) -> bool:
    """
    检查并自动升级schema（如果需要）
    
    Args:
        obvector: OceanBase客户端实例
        collection_name: 表名
        include_sparse: 是否需要稀疏向量支持（默认False）
        embedding_dims: 向量维度（可选，用于传递给Alembic）
        connection_args: 数据库连接参数（可选，用于传递给Alembic）
    
    Returns:
        True if schema is ready (either already up-to-date or upgraded successfully)
        False if upgrade failed
    """
    manager = SchemaVersionManager(obvector, collection_name, embedding_dims, include_sparse, connection_args)
    
    current_version = manager.get_current_version()
    target_version = get_target_schema_version()
    
    # 如果是全新安装，不需要升级
    if current_version == "none":
        logger.info("Fresh installation detected, will create schema directly")
        return True
    
    # 检查是否需要升级
    try:
        current_num = int(current_version) if current_version.isdigit() else 0
        target_num = int(target_version) if target_version.isdigit() else 0
        
        if current_num >= target_num:
            logger.info(f"Schema is already at target version: {current_version}")
            return True
        
        # 需要升级
        logger.info(f"Detected version {current_version} schema, upgrading to {target_version}...")
        
        # 先初始化alembic_version表（如果不存在）
        with obvector.engine.connect() as conn:
            result = conn.execute(text(
                "SELECT COUNT(*) FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = DATABASE() "
                "AND TABLE_NAME = 'alembic_version'"
            ))
            if result.scalar() == 0:
                logger.info("Initializing alembic_version table...")
                if not manager.initialize_alembic_version():
                    logger.error("Failed to initialize alembic_version table")
                    return False
        
        # 执行升级
        return manager.run_upgrade()
        
    except (ValueError, AttributeError):
        # 如果版本号不是数字格式，使用字符串比较
        if current_version == target_version:
            logger.info(f"Schema is already at target version: {current_version}")
            return True
        
        if current_version == "unknown":
            logger.warning(f"Unknown schema version, cannot determine if upgrade is needed")
            return True
        
        # 需要升级
        logger.info(f"Detected version {current_version} schema, upgrading to {target_version}...")
        
        # 先初始化alembic_version表（如果不存在）
        with obvector.engine.connect() as conn:
            result = conn.execute(text(
                "SELECT COUNT(*) FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = DATABASE() "
                "AND TABLE_NAME = 'alembic_version'"
            ))
            if result.scalar() == 0:
                logger.info("Initializing alembic_version table...")
                if not manager.initialize_alembic_version():
                    logger.error("Failed to initialize alembic_version table")
                    return False
        
        # 执行升级
        return manager.run_upgrade()

