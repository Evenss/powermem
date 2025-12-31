"""
稀疏向量数据迁移脚本

用于将历史数据迁移到稀疏向量格式。

Usage:
    from powermem import Memory, auto_config
    from scripts.script_manager import ScriptManager
    
    config = auto_config()
    memory = Memory(config=config)
    
    # 使用 ScriptManager 执行迁移
    ScriptManager.run('migrate-sparse-vector', memory, batch_size=1000, dry_run=False)
    
    # 或直接调用函数
    from scripts.migrate_sparse_vector import migrate_sparse_vector
    migrate_sparse_vector(memory, batch_size=1000, dry_run=False)
"""
import logging
import time
from typing import List, Optional, Any, Dict
from sqlalchemy import text

from src.powermem.utils import OceanBaseUtil

logger = logging.getLogger(__name__)


class SparseMigrationWorker:
    """稀疏向量迁移Worker"""
    
    def __init__(
        self,
        memory,
        batch_size: int = 1000,
        delay: float = 0.1,
        dry_run: bool = False
    ):
        """
        初始化Worker
        
        Args:
            memory: Memory对象实例
            batch_size: 批量处理大小
            delay: 批次间延迟（秒）
            dry_run: 是否为dry-run模式（测试100条数据）
        """
        self.memory = memory
        self.batch_size = batch_size
        self.delay = delay
        self.dry_run = dry_run
        
        # 统计信息
        self.total_count = 0
        self.migrated_count = 0
        self.failed_count = 0
        self.start_time = None
        
        # 从Memory对象获取必要的组件
        self._init_from_memory()
        
    def _init_from_memory(self):
        """从Memory对象初始化组件"""
        # 获取storage
        self.storage = self.memory.storage
        if not hasattr(self.storage, 'obvector'):
            raise ValueError("Memory storage must be OceanBaseVectorStore")
        
        # 获取数据库引擎
        self.engine = self.storage.obvector.engine
        self.table_name = self.storage.collection_name
        self.text_field = self.storage.text_field
        
        # 获取稀疏嵌入器
        self.sparse_embedder = getattr(self.memory, 'sparse_embedder', None)
        if not self.sparse_embedder:
            raise ValueError(
                "sparse_embedder not found in Memory object. "
                "Please configure sparse_embedder in your config."
            )
        
        # 获取审计日志
        self.audit = getattr(self.memory, 'audit', None)
        
        logger.info(f"Initialized migration worker for sparse vector")
        logger.info(f"  Database table: {self.table_name}")
        logger.info(f"  Text field: {self.text_field}")
    
    def _get_total_count(self) -> int:
        """获取待迁移数据总数"""
        with self.engine.connect() as conn:
            result = conn.execute(text(
                f"SELECT COUNT(*) FROM {self.table_name} "
                f"WHERE sparse_embedding IS NULL"
            ))
            return result.scalar()
    
    def _fetch_batch(self, offset: int) -> List[Dict]:
        """
        获取一批待迁移数据
        
        Args:
            offset: 偏移量
        
        Returns:
            记录列表 [{'id': ..., 'text_content': ...}, ...]
        """
        limit = 100 if self.dry_run else self.batch_size
        
        with self.engine.connect() as conn:
            result = conn.execute(text(
                f"SELECT id, {self.text_field} as text_content "
                f"FROM {self.table_name} "
                f"WHERE sparse_embedding IS NULL "
                f"ORDER BY id "
                f"LIMIT :limit OFFSET :offset"
            ), {'limit': limit, 'offset': offset})
            
            return [dict(row._mapping) for row in result]
    
    def _compute_sparse_embeddings(self, texts: List[str]) -> List[Optional[Dict[int, float]]]:
        """
        批量计算稀疏向量
        
        Args:
            texts: 文本列表
        
        Returns:
            稀疏向量列表（失败的为None）
        """
        results = []
        for text in texts:
            try:
                if not text or not text.strip():
                    results.append(None)
                    continue
                    
                sparse_embedding = self.sparse_embedder.embed_sparse(text)
                results.append(sparse_embedding)
            except Exception as e:
                logger.warning(f"Failed to compute sparse embedding: {e}")
                results.append(None)
        
        return results
    
    def _update_batch(self, updates: List[Dict]) -> int:
        """
        批量更新数据库
        
        Args:
            updates: 更新列表 [{'id': ..., 'sparse_embedding': ...}, ...]
        
        Returns:
            成功更新的数量
        """
        if self.dry_run:
            logger.info(f"[DRY RUN] Would update {len(updates)} records")
            return len(updates)
        
        success_count = 0
        
        with self.engine.connect() as conn:
            for update in updates:
                if update['sparse_embedding'] is None:
                    continue
                    
                try:
                    sparse_str = OceanBaseUtil.format_sparse_vector(update['sparse_embedding'])
                    conn.execute(text(
                        f"UPDATE {self.table_name} "
                        f"SET sparse_embedding = '{sparse_str}' "
                        f"WHERE id = :id"
                    ), {'id': update['id']})
                    success_count += 1
                except Exception as e:
                    logger.warning(f"Failed to update record {update['id']}: {e}")
            
            conn.commit()
        
        return success_count
    
    def _format_duration(self, seconds: float) -> str:
        """格式化时间"""
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            minutes = int(seconds // 60)
            secs = int(seconds % 60)
            return f"{minutes}m {secs}s"
        else:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            return f"{hours}h {minutes}m"
    
    def _log_audit_event(self, status: str, details: Dict[str, Any]):
        """记录审计事件"""
        if self.audit is None:
            return
        
        try:
            self.audit.log_security_event(
                event_type='sparse_migration_progress',
                severity='info' if status != 'failed' else 'error',
                details=details
            )
        except Exception as e:
            logger.warning(f"Failed to log audit event: {e}")
    
    def run(self):
        """执行迁移"""
        try:
            from rich.live import Live
            from rich.table import Table
            from rich.console import Console
            
            use_rich = True
        except ImportError:
            logger.warning("rich library not found, using simple progress display")
            use_rich = False
        
        self.start_time = time.time()
        self.total_count = self._get_total_count()
        
        logger.info(f"Starting sparse vector migration")
        logger.info(f"Total records to migrate: {self.total_count}")
        logger.info(f"Batch size: {self.batch_size}")
        
        if self.dry_run:
            logger.info("[DRY RUN] Mode enabled - will only test with 100 records")
        
        # 记录开始事件
        self._log_audit_event('started', {
            'status': 'started',
            'total_records': self.total_count,
            'batch_size': self.batch_size,
            'dry_run': self.dry_run
        })
        
        offset = 0
        
        try:
            if use_rich:
                self._run_with_rich(offset)
            else:
                self._run_simple(offset)
            
            # 记录成功结束事件
            duration = time.time() - self.start_time
            self._log_audit_event('completed', {
                'status': 'completed',
                'total_records': self.total_count,
                'migrated_count': self.migrated_count,
                'failed_count': self.failed_count,
                'duration_seconds': duration
            })
            
            logger.info("=" * 50)
            logger.info("Migration completed!")
            logger.info(f"  Migrated: {self.migrated_count}")
            logger.info(f"  Failed: {self.failed_count}")
            logger.info(f"  Duration: {self._format_duration(duration)}")
            
        except Exception as e:
            # 记录失败事件
            self._log_audit_event('failed', {
                'status': 'failed',
                'error': str(e),
                'migrated_count': self.migrated_count,
                'failed_count': self.failed_count
            })
            raise
    
    def _create_progress_table(self) -> 'Table':
        """创建进度表格"""
        from rich.table import Table
        
        elapsed = time.time() - self.start_time if self.start_time else 0
        speed = self.migrated_count / elapsed if elapsed > 0 else 0
        progress_pct = (self.migrated_count / self.total_count * 100) if self.total_count > 0 else 0
        
        table = Table(title=f"Sparse Vector Migration Progress")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("Total Records", str(self.total_count))
        table.add_row("Migrated", f"{self.migrated_count} ({progress_pct:.1f}%)")
        table.add_row("Failed", str(self.failed_count))
        table.add_row("Speed", f"{speed:.1f} rec/sec")
        table.add_row("Elapsed", self._format_duration(elapsed))
        
        if self.dry_run:
            table.add_row("Mode", "[yellow]DRY RUN[/yellow]")
        
        return table
    
    def _run_with_rich(self, offset: int):
        """使用rich库运行（带动态进度显示）"""
        from rich.live import Live
        
        with Live(self._create_progress_table(), refresh_per_second=2) as live:
            while True:
                # 获取一批数据
                batch = self._fetch_batch(offset)
                
                if not batch:
                    logger.info("No more data to process")
                    break
                
                # 提取文本
                texts = [record['text_content'] for record in batch]
                
                # 计算稀疏向量
                sparse_embeddings = self._compute_sparse_embeddings(texts)
                
                # 构建更新数据
                updates = []
                for record, sparse_emb in zip(batch, sparse_embeddings):
                    updates.append({
                        'id': record['id'],
                        'sparse_embedding': sparse_emb
                    })
                    if sparse_emb is None:
                        self.failed_count += 1
                
                # 批量更新
                success_count = self._update_batch(updates)
                self.migrated_count += success_count
                
                # 更新进度显示
                live.update(self._create_progress_table())
                
                # dry-run模式只运行一次
                if self.dry_run:
                    break
                
                # 延迟控制
                if self.delay > 0:
                    time.sleep(self.delay)
                
                offset += self.batch_size
    
    def _run_simple(self, offset: int):
        """简单模式运行（无rich库）"""
        while True:
            # 获取一批数据
            batch = self._fetch_batch(offset)
            
            if not batch:
                logger.info("No more data to process")
                break
            
            # 提取文本
            texts = [record['text_content'] for record in batch]
            
            # 计算稀疏向量
            sparse_embeddings = self._compute_sparse_embeddings(texts)
            
            # 构建更新数据
            updates = []
            for record, sparse_emb in zip(batch, sparse_embeddings):
                updates.append({
                    'id': record['id'],
                    'sparse_embedding': sparse_emb
                })
                if sparse_emb is None:
                    self.failed_count += 1
            
            # 批量更新
            success_count = self._update_batch(updates)
            self.migrated_count += success_count
            
            # 打印进度
            elapsed = time.time() - self.start_time
            speed = self.migrated_count / elapsed if elapsed > 0 else 0
            progress_pct = (self.migrated_count / self.total_count * 100) if self.total_count > 0 else 0
            
            logger.info(
                f"{self.migrated_count}/{self.total_count} "
                f"({progress_pct:.1f}%) | Failed: {self.failed_count} | "
                f"Speed: {speed:.1f} rec/sec"
            )
            
            # dry-run模式只运行一次
            if self.dry_run:
                break
            
            # 延迟控制
            if self.delay > 0:
                time.sleep(self.delay)
            
            offset += self.batch_size


def migrate_sparse_vector(
    memory: 'Memory',
    batch_size: int = 1000,
    delay: float = 0.1,
    dry_run: bool = False
) -> bool:
    """
    迁移历史数据以添加稀疏向量
    
    Args:
        memory: Memory对象实例（必需）
        batch_size: 批量处理大小（默认1000）
        delay: 批次间延迟秒数（默认0.1）
        dry_run: 是否为干运行模式，只测试100条数据（默认False）
    
    Returns:
        bool: 成功返回True，失败返回False
    
    Example:
        ```python
        from powermem import Memory, auto_config
        from scripts.script_manager import ScriptManager
        
        config = auto_config()
        memory = Memory(config=config)
        
        # 使用ScriptManager执行（推荐）
        ScriptManager.run('migrate-sparse-vector', memory, dry_run=True)
        
        # 或直接调用
        from scripts.migrate_sparse_vector import migrate_sparse_vector
        migrate_sparse_vector(memory, batch_size=500, dry_run=True)
        ```
    """
    try:
        # 创建并运行Worker
        worker = SparseMigrationWorker(
            memory=memory,
            batch_size=batch_size,
            delay=delay,
            dry_run=dry_run
        )
        worker.run()
        
        return True
        
    except Exception as e:
        logger.error(f"Migration failed: {e}", exc_info=True)
        return False
