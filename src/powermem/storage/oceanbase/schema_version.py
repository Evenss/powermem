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

# 当前SDK要求的schema版本
CURRENT_SCHEMA_VERSION = "030"


class SchemaVersionManager:
    """Schema版本管理器"""
    
    def __init__(self, obvector, collection_name: str):
        """
        初始化版本管理器
        
        Args:
            obvector: OceanBase客户端实例
            collection_name: 表名
        """
        self.obvector = obvector
        self.collection_name = collection_name
    
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
        
        if current == "none":
            # 全新安装，不需要升级（会直接创建v2表）
            return False
        
        if current == "unknown":
            logger.warning("Cannot determine if upgrade is needed (unknown version)")
            return False
        
        if current == CURRENT_SCHEMA_VERSION:
            logger.info(f"Schema is up to date (version: {current})")
            return False
        
        # 版本过旧，需要升级
        logger.info(f"Schema upgrade needed: {current} -> {CURRENT_SCHEMA_VERSION}")
        return True
    
    def run_upgrade(self) -> bool:
        """
        执行schema升级
        
        Returns:
            True if upgrade succeeded, False otherwise
        """
        try:
            current_version = self.get_current_version()
            
            if current_version == "none":
                logger.info("Fresh installation, no upgrade needed")
                return True
            
            if current_version == CURRENT_SCHEMA_VERSION:
                logger.info("Schema already at target version")
                return True
            
            logger.info(f"Starting schema upgrade: {current_version} -> {CURRENT_SCHEMA_VERSION}")
            
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


def check_and_upgrade_schema(obvector, collection_name: str, include_sparse: bool = False) -> bool:
    """
    检查并自动升级schema（如果需要）
    
    Args:
        obvector: OceanBase客户端实例
        collection_name: 表名
        include_sparse: 是否需要稀疏向量支持
    
    Returns:
        True if schema is ready (either already up-to-date or upgraded successfully)
        False if upgrade failed
    """
    manager = SchemaVersionManager(obvector, collection_name)
    
    current_version = manager.get_current_version()
    
    # 如果是全新安装，不需要升级
    if current_version == "none":
        logger.info("Fresh installation detected, will create v2 schema directly")
        return True
    
    # 如果是020且需要稀疏向量支持，执行升级
    if current_version == "020" and include_sparse:
        logger.info("Detected version 020 schema, upgrading to 030 for sparse vector support...")
        
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
    
    # 如果已经是030或更高版本
    if current_version == "030":
        logger.info("Schema is already at version 030")
        return True
    
    # 其他情况（020但不需要sparse，或unknown）
    logger.info(f"Current schema version: {current_version}, no upgrade needed")
    return True

