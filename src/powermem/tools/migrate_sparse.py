"""
稀疏向量数据迁移工具

用于将历史数据迁移到稀疏向量格式。

使用方式:
    python -m powermem.tools.migrate_sparse --dry-run
    python -m powermem.tools.migrate_sparse --batch-size 500 --delay 0.2
    python -m powermem.tools.migrate_sparse --workers 4 --worker-id 0
"""
import argparse
import logging
import os
import sys
import time
from typing import Dict, List, Optional, Any

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='Migrate historical data to add sparse embeddings'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Batch size for processing (default: 1000)'
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=1,
        help='Number of parallel workers (default: 1)'
    )
    parser.add_argument(
        '--worker-id',
        type=int,
        default=0,
        help='Worker ID (0 to workers-1)'
    )
    parser.add_argument(
        '--delay',
        type=float,
        default=0.1,
        help='Delay in seconds between batches (default: 0.1)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Dry run mode (test with 100 records, no write)'
    )
    return parser.parse_args()


class SparseMigrationWorker:
    """稀疏向量迁移Worker"""
    
    def __init__(self, config: Dict[str, Any], args):
        """
        初始化Worker
        
        Args:
            config: 从环境变量加载的配置
            args: 命令行参数
        """
        self.config = config
        self.args = args
        self.batch_size = args.batch_size
        self.worker_id = args.worker_id
        self.workers = args.workers
        self.delay = args.delay
        self.dry_run = args.dry_run
        
        # 统计信息
        self.total_count = 0
        self.migrated_count = 0
        self.failed_count = 0
        self.start_time = None
        
        # 初始化组件
        self._init_database()
        self._init_sparse_embedder()
        self._init_audit()
        
    def _init_database(self):
        """初始化数据库连接"""
        try:
            from sqlalchemy import create_engine
            
            vector_store_config = self.config.get('vector_store', {}).get('config', {})
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
            
            # 获取表名
            self.table_name = vector_store_config.get('collection_name', os.getenv('OCEANBASE_COLLECTION', 'memories'))
            self.text_field = vector_store_config.get('text_field', 'document')
            
            # 构建连接URL
            url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}"
            self.engine = create_engine(url)
            
            logger.info(f"Database connection initialized: {host}:{port}/{db_name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    def _init_sparse_embedder(self):
        """初始化稀疏嵌入器"""
        try:
            from powermem.integrations.embeddings.sparse_factory import SparseEmbedderFactory
            
            sparse_config = self.config.get('sparse_embedder', {})
            if not sparse_config:
                raise ValueError(
                    "sparse_embedder config not found. "
                    "Please set SPARSE_EMBEDDER_PROVIDER environment variable."
                )
            
            provider = sparse_config.get('provider')
            config_dict = sparse_config.get('config', {})
            
            self.sparse_embedder = SparseEmbedderFactory.create(provider, config_dict)
            logger.info(f"Sparse embedder initialized: {provider}")
            
        except Exception as e:
            logger.error(f"Failed to initialize sparse embedder: {e}")
            raise
    
    def _init_audit(self):
        """初始化审计日志"""
        try:
            from powermem.core.audit import AuditLogger
            
            audit_config = self.config.get('audit', {})
            self.audit = AuditLogger(audit_config)
            logger.info("Audit logger initialized")
            
        except Exception as e:
            logger.warning(f"Failed to initialize audit logger: {e}")
            self.audit = None
    
    def _get_total_count(self) -> int:
        """获取待迁移数据总数"""
        from sqlalchemy import text
        
        with self.engine.connect() as conn:
            result = conn.execute(text(
                f"SELECT COUNT(*) FROM {self.table_name} "
                f"WHERE sparse_embedding IS NULL"
            ))
            return result.scalar()
    
    def _fetch_batch(self, round_num: int) -> List[Dict]:
        """
        获取一批待迁移数据
        
        Args:
            round_num: 当前轮次（从0开始）
        
        Returns:
            记录列表 [{'id': ..., 'text_content': ...}, ...]
        """
        from sqlalchemy import text
        
        # 多worker轮询分片
        offset = self.worker_id * self.batch_size + round_num * self.workers * self.batch_size
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
    
    def _format_sparse_vector(self, sparse_dict: Dict[int, float]) -> str:
        """格式化稀疏向量为SQL字符串"""
        if not sparse_dict:
            return "{}"
        formatted = "{" + ", ".join(f"{k}:{v}" for k, v in sparse_dict.items()) + "}"
        return formatted
    
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
        
        from sqlalchemy import text
        
        success_count = 0
        
        with self.engine.connect() as conn:
            for update in updates:
                if update['sparse_embedding'] is None:
                    continue
                    
                try:
                    sparse_str = self._format_sparse_vector(update['sparse_embedding'])
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
        
        logger.info(f"Starting migration for Worker {self.worker_id}")
        logger.info(f"Total records to migrate: {self.total_count}")
        logger.info(f"Batch size: {self.batch_size}, Workers: {self.workers}")
        
        if self.dry_run:
            logger.info("[DRY RUN] Mode enabled - will only test with 100 records")
        
        # 记录开始事件
        self._log_audit_event('started', {
            'status': 'started',
            'worker_id': self.worker_id,
            'total_workers': self.workers,
            'total_records': self.total_count,
            'batch_size': self.batch_size,
            'dry_run': self.dry_run
        })
        
        round_num = 0
        
        try:
            if use_rich:
                self._run_with_rich(round_num)
            else:
                self._run_simple(round_num)
            
            # 记录成功结束事件
            duration = time.time() - self.start_time
            self._log_audit_event('completed', {
                'status': 'completed',
                'worker_id': self.worker_id,
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
                'worker_id': self.worker_id,
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
        
        table = Table(title=f"Worker {self.worker_id} Migration Progress")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("Worker ID", str(self.worker_id))
        table.add_row("Total Records", str(self.total_count))
        table.add_row("Migrated", f"{self.migrated_count} ({progress_pct:.1f}%)")
        table.add_row("Failed", str(self.failed_count))
        table.add_row("Speed", f"{speed:.1f} rec/sec")
        table.add_row("Elapsed", self._format_duration(elapsed))
        
        if self.dry_run:
            table.add_row("Mode", "[yellow]DRY RUN[/yellow]")
        
        return table
    
    def _run_with_rich(self, round_num: int):
        """使用rich库运行（带动态进度显示）"""
        from rich.live import Live
        
        with Live(self._create_progress_table(), refresh_per_second=2) as live:
            while True:
                # 获取一批数据
                batch = self._fetch_batch(round_num)
                
                if not batch:
                    logger.info(f"Worker {self.worker_id}: No more data to process")
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
                
                round_num += 1
    
    def _run_simple(self, round_num: int):
        """简单模式运行（无rich库）"""
        while True:
            # 获取一批数据
            batch = self._fetch_batch(round_num)
            
            if not batch:
                logger.info(f"Worker {self.worker_id}: No more data to process")
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
                f"Worker {self.worker_id}: {self.migrated_count}/{self.total_count} "
                f"({progress_pct:.1f}%) | Failed: {self.failed_count} | "
                f"Speed: {speed:.1f} rec/sec"
            )
            
            # dry-run模式只运行一次
            if self.dry_run:
                break
            
            # 延迟控制
            if self.delay > 0:
                time.sleep(self.delay)
            
            round_num += 1


def main():
    """主入口函数"""
    args = parse_args()
    
    # 验证worker_id
    if args.worker_id < 0 or args.worker_id >= args.workers:
        logger.error(f"Invalid worker_id: {args.worker_id}. Must be between 0 and {args.workers - 1}")
        sys.exit(1)
    
    # 加载配置
    try:
        from powermem.config_loader import load_config_from_env
        config = load_config_from_env()
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        sys.exit(1)
    
    # 检查sparse_embedder配置
    if not config.get('sparse_embedder'):
        logger.error(
            "sparse_embedder config not found. "
            "Please set the following environment variables:\n"
            "  SPARSE_EMBEDDER_PROVIDER (e.g., 'qwen')\n"
            "  SPARSE_EMBEDDER_API_KEY\n"
            "  SPARSE_EMBEDDER_MODEL"
        )
        sys.exit(1)
    
    # 创建Worker并执行
    try:
        worker = SparseMigrationWorker(config, args)
        worker.run()
    except KeyboardInterrupt:
        logger.info("Migration interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Migration failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()

