#!/bin/bash
# =============================================================================
# 稀疏向量数据迁移脚本
# 
# 用于将历史数据迁移到稀疏向量格式
#
# 使用方式:
#   ./scripts/migrate_sparse.sh --dry-run                # 测试模式
#   ./scripts/migrate_sparse.sh --batch-size 500         # 单worker迁移
#   ./scripts/migrate_sparse.sh --workers 4 --worker-id 0  # 多worker并行
#
# 环境变量要求:
#   SPARSE_EMBEDDER_PROVIDER  - 稀疏嵌入器提供商 (如 qwen)
#   SPARSE_EMBEDDER_API_KEY   - API密钥
#   SPARSE_EMBEDDER_MODEL     - 模型名称
#   OCEANBASE_HOST            - 数据库主机
#   OCEANBASE_PORT            - 数据库端口
#   OCEANBASE_USER            - 数据库用户
#   OCEANBASE_PASSWORD        - 数据库密码
#   OCEANBASE_DATABASE        - 数据库名称
#   OCEANBASE_COLLECTION      - 表名
# =============================================================================

set -e

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# 切换到项目根目录
cd "$PROJECT_ROOT"

# 激活虚拟环境（如果存在）
if [ -d ".venv" ]; then
    echo "Activating virtual environment..."
    source .venv/bin/activate
elif [ -d "venv" ]; then
    echo "Activating virtual environment..."
    source venv/bin/activate
fi

# 检查必要的环境变量
if [ -z "$SPARSE_EMBEDDER_PROVIDER" ]; then
    echo "Warning: SPARSE_EMBEDDER_PROVIDER is not set"
    echo "Please set the following environment variables:"
    echo "  SPARSE_EMBEDDER_PROVIDER (e.g., 'qwen')"
    echo "  SPARSE_EMBEDDER_API_KEY"
    echo "  SPARSE_EMBEDDER_MODEL"
fi

# 调用Python CLI工具
echo "Starting sparse vector migration..."
python -m powermem.tools.migrate_sparse "$@"

# 返回Python脚本的退出码
exit $?

