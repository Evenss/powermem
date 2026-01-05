# Sparse Vector Guide

This guide explains how to use the Sparse Vector feature in PowerMem, including configuration, query usage, schema upgrades, and historical data migration.

## Prerequisites

- Python 3.10+
- powermem installed (`pip install powermem`)
- Database requirements: **seekdb** or **OceanBase >= 4.5.0**

> **Note**: Sparse vector feature only supports OceanBase storage backend, SQLite does not support this feature.

## Configuring Sparse Vector

To enable sparse vector functionality, you need to configure two parts:

1. `vector_store.config.include_sparse = True` - Enable sparse vector support
2. `sparse_embedder` - Configure sparse vector embedding service

### Environment Variable Configuration

Add the following configuration to your `.env` file:

```env
# Database configuration
DATABASE_PROVIDER=oceanbase
OCEANBASE_HOST=127.0.0.1
OCEANBASE_PORT=2881
OCEANBASE_USER=root
OCEANBASE_PASSWORD=your_password
OCEANBASE_DATABASE=powermem
OCEANBASE_COLLECTION=memories
OCEANBASE_EMBEDDING_MODEL_DIMS=1536

# Enable sparse vector
OCEANBASE_INCLUDE_SPARSE=true

# Sparse vector embedding configuration
SPARSE_EMBEDDER_PROVIDER=qwen
SPARSE_EMBEDDER_API_KEY=your_api_key
SPARSE_EMBEDDER_MODEL=text-embedding-v4
SPARSE_EMBEDDER_DIMS=1536
```

### Dictionary Configuration

Configure sparse vector using Python dictionary:

```python
from powermem import Memory

config = {
    'llm': {
        'provider': 'qwen',
        'config': {
            'api_key': 'your_api_key',
            'model': 'qwen-plus'
        }
    },
    'embedder': {
        'provider': 'qwen',
        'config': {
            'api_key': 'your_api_key',
            'model': 'text-embedding-v4',
            'embedding_dims': 1536
        }
    },
    # Sparse vector embedding configuration
    'sparse_embedder': {
        'provider': 'qwen',
        'config': {
            'api_key': 'your_api_key',
            'model': 'text-embedding-v4'
        }
    },
    'vector_store': {
        'provider': 'oceanbase',
        'config': {
            'collection_name': 'memories',
            'embedding_model_dims': 1536,
            'include_sparse': True,  # Enable sparse vector
            'connection_args': {
                'host': '127.0.0.1',
                'port': 2881,
                'user': 'root',
                'password': 'your_password',
                'db_name': 'powermem'
            },
            # Optional: Configure search weights
            'vector_weight': 0.5,
            'fts_weight': 0.5,
            'sparse_weight': 0.25
        }
    }
}

memory = Memory(config=config)
```

### Configuration Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `include_sparse` | bool | `False` | Whether to enable sparse vector support |
| `sparse_embedder.provider` | string | - | Sparse vector embedding provider (currently supports `qwen`) |
| `sparse_embedder.config.api_key` | string | - | API key |
| `sparse_embedder.config.model` | string | - | Embedding model name |
| `vector_weight` | float | `0.5` | Vector search weight |
| `fts_weight` | float | `0.5` | Full-text search weight |
| `sparse_weight` | float | `0.25` | Sparse vector search weight |

## Query Usage

After configuring sparse vector, searches will automatically use sparse vector for hybrid search without any code changes.

### Basic Search

```python
from powermem import Memory, auto_config

# Load configuration (automatically loads from .env)
config = auto_config()
memory = Memory(config=config)

# Add memory (automatically generates sparse vector)
memory.add(
    messages="Machine learning is a branch of artificial intelligence, I love machine learning",
    user_id="user123"
)

# Search (automatically uses sparse vector for hybrid search)
results = memory.search(
    query="AI technology",
    user_id="user123",
    limit=10
)
```

### Search Weight Configuration

Search combines three methods: vector search, full-text search, and sparse vector search. You can adjust the influence of each search method by configuring weights:

- `vector_weight`: Vector search weight (default 0.5)
- `fts_weight`: Full-text search weight (default 0.5)
- `sparse_weight`: Sparse vector search weight (default 0.25)

## Schema Upgrade

If you already have a table without sparse vector support, you need to run the upgrade script first to add sparse vector support.

### List Available Scripts

```python
from scripts.script_manager import ScriptManager

# List all available scripts
ScriptManager.list_scripts()
```

Example output:
```
======================================================================
PowerMem Available Scripts
======================================================================

【Upgrade Scripts - Add new features or upgrade existing features】
----------------------------------------------------------------------
  • upgrade-sparse-vector
    Add sparse vector support to OceanBase table (add sparse_embedding column and index) (requires: dict)

======================================================================
```

### View Script Details

```python
from scripts.script_manager import ScriptManager

# View upgrade script details
ScriptManager.info('upgrade-sparse-vector')
```

Example output:
```
======================================================================
Script: upgrade-sparse-vector
======================================================================
Category: upgrade
Description: Add sparse vector support to OceanBase table (add sparse_embedding column and index)

----------------------------------------------------------------------
Parameters:
----------------------------------------------------------------------
  config (dict) (required)
```

### Execute Upgrade Script

```python
from powermem import auto_config
from scripts.script_manager import ScriptManager

# Load configuration
config = auto_config()

# Run upgrade script
ScriptManager.run('upgrade-sparse-vector', config)
```

The upgrade script performs the following operations:
1. Check if the database version supports sparse vector
2. Add `sparse_embedding` column (SPARSE_VECTOR type)
3. Create `sparse_embedding_idx` index

> **Note**: The upgrade script is idempotent and can be safely executed multiple times.

## Historical Data Migration

After schema upgrade, the `sparse_embedding` column for historical data is empty. **Historical data migration is not required**, but it is recommended to run the migration script to generate sparse vectors for historical data, for the following reasons:

- **Only migrated data will participate in sparse vector retrieval**: Unmigrated historical data will not use sparse vector during search, only newly added data and migrated data will participate in sparse vector search
- **More accurate results after migration**: Sparse vector search provides more accurate semantic matching. After migrating historical data, all data can benefit from the improved search accuracy brought by sparse vector
- **New data automatically generated**: Even without migrating historical data, newly added data will automatically generate sparse vectors and participate in search

### Execute Migration Script

```python
from powermem import Memory, auto_config
from scripts.script_manager import ScriptManager

# Load configuration
config = auto_config()

# Create Memory instance (migration script requires Memory instance)
memory = Memory(config=config)

# Run migration script
ScriptManager.run('migrate-sparse-vector', memory, batch_size=100, workers=3)
```

### Migration Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `batch_size` | int | `100` | Number of records processed per batch |
| `workers` | int | `1` | Number of concurrent threads, increasing can improve migration speed |
| `delay` | float | `0.1` | Delay between batches (seconds) |
| `dry_run` | bool | `False` | Test mode, only processes 100 records and does not write to database |

### Test with dry-run Mode

Before formal migration, it is recommended to test with dry-run mode first:

```python
# Test mode (only processes 100 records, does not write to database)
ScriptManager.run('migrate-sparse-vector', memory, dry_run=True)
```

### Migration Progress

Real-time progress will be displayed during migration:

```
Total: [████████░░░░░░] 57.1% | 5,710/10,000
  ✓ Migrated: 5,710 | ✗ Failed: 0
  ⏱ Elapsed: 2m 30s | Remaining: ~1m 52s | 📊 38.1 rec/s

Workers (3):
  Worker 0: ✓ 1,903 | ✗ 0
  Worker 1: ✓ 1,904 | ✗ 0
  Worker 2: ✓ 1,903 | ✗ 0
```

### Verify Migration Results

After migration is complete, you can verify if sparse vector is working by performing a search:

```python
import logging

# Enable DEBUG logging to view search details
logging.getLogger().setLevel(logging.DEBUG)

# Execute search
result = memory.search(query="test query", limit=10)
```

You can see sparse vector search related information in the DEBUG logs.

## Complete Usage Workflow

### New Table (Recommended)

If creating a new table, simply enable sparse vector in the configuration:

```python
from powermem import Memory, auto_config

config = auto_config()  # Ensure include_sparse=True in configuration
memory = Memory(config=config)

# Add memory (automatically generates sparse vector)
memory.add(messages="memory content", user_id="user123")

# Search (automatically uses sparse vector)
results = memory.search(query="query content", user_id="user123")
```

### Existing Table Upgrade

If upgrading an existing table, follow these steps:

```python
from powermem import Memory, auto_config
from scripts.script_manager import ScriptManager

# 1. Load configuration
config = auto_config()

# 2. Run schema upgrade script (required)
ScriptManager.run('upgrade-sparse-vector', config)

# 3. Create Memory instance
memory = Memory(config=config)

# 4. Run data migration script (optional, but recommended)
# Note: Only migrated data will participate in sparse vector retrieval, results are more accurate after migration
ScriptManager.run('migrate-sparse-vector', memory, batch_size=100, workers=3)

# 5. Verify
results = memory.search(query="test query", limit=10)
```

> **Note**: Step 4 data migration is optional. If historical data is not migrated:
> - Newly added data will automatically generate sparse vectors and participate in search
> - Historical data will not participate in sparse vector retrieval, but can be found through vector search and full-text search
> - After migrating historical data, all data can benefit from the improved search accuracy brought by sparse vector

## Rollback (Optional)

If you need to remove sparse vector support, you can run the downgrade script:

```python
from powermem import auto_config
from scripts.script_manager import ScriptManager

config = auto_config()

# Run downgrade script (will delete all sparse vector data)
ScriptManager.run('downgrade-sparse-vector', config)
```

> **Warning**: The downgrade script will delete the `sparse_embedding` column and index, all sparse vector data will be permanently deleted!

