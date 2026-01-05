# Scenario 10: Sparse Vector

This example demonstrates how to use the sparse vector feature, including configuration, adding memories, searching, and upgrading existing tables and migrating historical data.

## Prerequisites

- Python 3.10+
- powermem installed (`pip install powermem`)
- Database: **seekdb** or **OceanBase >= 4.5.0**

## Step 1: Configure Sparse Vector

Create configuration to enable sparse vector support:

```python
# sparse_vector_example.py
from powermem import Memory

config = {
    'llm': {
        'provider': 'qwen',
        'config': {
            'api_key': 'your_api_key',
            'model': 'qwen-plus',
            'temperature': 0.2
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
            'collection_name': 'sparse_demo',
            'embedding_model_dims': 1536,
            'include_sparse': True,  # Enable sparse vector
            'connection_args': {
                'host': '127.0.0.1',
                'port': 2881,
                'user': 'root',
                'password': 'your_password',
                'db_name': 'powermem'
            }
        }
    }
}

print("✓ Configuration created successfully")
```

**Run the code:**
```bash
python sparse_vector_example.py
```

**Expected output:**
```
✓ Configuration created successfully
```

## Step 2: Initialize Memory

Create a Memory instance using the configuration. For new tables, the system will automatically create a table structure with sparse vector support:

```python
# sparse_vector_example.py
from powermem import Memory

# ... configuration code (same as Step 1)

# Create Memory instance
memory = Memory(config=config)

print("✓ Memory initialized successfully")
print(f"  - sparse_embedder: {memory.sparse_embedder is not None}")
```

**Expected output:**
```
✓ Memory initialized successfully
  - sparse_embedder: True
```

## Step 3: Add Memories

When adding memories, the system will automatically generate sparse vectors:

```python
# sparse_vector_example.py
from powermem import Memory

# ... initialization code (same as Step 2)

# Add test memories
test_memories = [
    "Machine learning is a branch of artificial intelligence, focusing on algorithms and statistical models",
    "Natural language processing is an interdisciplinary field of computer science and artificial intelligence",
    "Vector search is an important technology for information retrieval, used for similarity matching",
    "Deep learning uses multi-layer neural networks for feature learning",
    "Knowledge graphs are graph-structured data used to represent entities and their relationships"
]

print("Adding test memories...")
for content in test_memories:
    memory.add(
        messages=content,
        user_id="user123",
        infer=False  # Do not use intelligent inference, store directly
    )

print(f"✓ Successfully added {len(test_memories)} memories")
```

**Expected output:**
```
Adding test memories...
✓ Successfully added 5 memories
```

## Step 4: Execute Search

Search will automatically use sparse vector for hybrid search:

```python
# sparse_vector_example.py
from powermem import Memory

# ... add memory code (same as Step 3)

# Execute search
query = "AI algorithms"
print(f"\nSearch query: '{query}'")

results = memory.search(
    query=query,
    user_id="user123",
    limit=5
)

# Display search results
print(f"Found {len(results.get('results', []))} results:\n")
for i, result in enumerate(results.get('results', []), 1):
    print(f"{i}. Score: {result['score']:.4f}")
    print(f"   Content: {result['memory'][:50]}...")
    print()
```

## Step 5: Upgrade Schema for Existing Table

If you already have a table without sparse vector support, you need to run the upgrade script first:

```python
# upgrade_example.py
from powermem import auto_config
from scripts.script_manager import ScriptManager

# List available scripts
print("Available scripts:")
ScriptManager.list_scripts()

# View upgrade script details
print("\nUpgrade script details:")
ScriptManager.info('upgrade-sparse-vector')

# Load configuration
config = auto_config()

# Run upgrade script
print("\nExecuting upgrade...")
ScriptManager.run('upgrade-sparse-vector', config)
```

**Expected output:**
```
Available scripts:
======================================================================
PowerMem Available Scripts
======================================================================

【Upgrade Scripts - Add new features or upgrade existing features】
----------------------------------------------------------------------
  • upgrade-sparse-vector
    Add sparse vector support to OceanBase table (add sparse_embedding column and index) (requires: dict)
...

Executing upgrade...
Preparing to execute script: upgrade-sparse-vector
Description: Add sparse vector support to OceanBase table (add sparse_embedding column and index)
Loading module: scripts.upgrade_sparse_vector
Executing script function: upgrade_sparse_vector
Starting sparse vector upgrade for table 'memories'
Adding sparse_embedding column to table 'memories'
sparse_embedding column added successfully
Creating sparse vector index on table 'memories'
sparse_embedding_idx created successfully
Sparse vector upgrade completed successfully for table 'memories'

✓ Script 'upgrade-sparse-vector' executed successfully!
```

## Step 6: Migrate Historical Data (Optional)

After schema upgrade, the `sparse_embedding` column for historical data is empty. **Historical data migration is not required**, but it is recommended to perform migration for the following reasons:

- **Only migrated data will participate in sparse vector retrieval**: Unmigrated historical data will not use sparse vector during search
- **More accurate results after migration**: Sparse vector search provides more accurate semantic matching
- **New data automatically generated**: Even without migrating historical data, newly added data will automatically generate sparse vectors

Generate sparse vectors for historical data:

```python
# migration_example.py
from powermem import Memory, auto_config
from scripts.script_manager import ScriptManager

# Load configuration
config = auto_config()

# Create Memory instance (migration script requires Memory instance)
memory = Memory(config=config)

# Test with dry-run mode first
print("Test mode (dry-run):")
ScriptManager.run('migrate-sparse-vector', memory, dry_run=True)

# Formal migration
print("\nFormal migration:")
ScriptManager.run('migrate-sparse-vector', memory, batch_size=100, workers=3)
```

**Expected output:**
```
Test mode (dry-run):
Preparing to execute script: migrate-sparse-vector
...
[DRY RUN] Mode enabled - will only test with 100 records

Total: [██████████████]  100.0% | 100/100
  ✓ Migrated: 100 | ✗ Failed: 0
  ⏱ Elapsed: 5.2s | Remaining: ~0s | 📊 19.2 rec/s

✓ Script 'migrate-sparse-vector' executed successfully!

Formal migration:
Preparing to execute script: migrate-sparse-vector
...
Total records to migrate: 10000
Batch size: 100
Thread pool size: 3

Total: [████████████░░]  85.0% | 8,500/10,000
  ✓ Migrated: 8,500 | ✗ Failed: 0
  ⏱ Elapsed: 3m 42s | Remaining: ~39s | 📊 38.3 rec/s

Workers (3):
  Worker 0: ✓ 2,834 | ✗ 0
  Worker 1: ✓ 2,833 | ✗ 0
  Worker 2: ✓ 2,833 | ✗ 0
```

## Step 7: Verify Migration Results

After migration is complete, verify if sparse vector is working:

```python
# verify_example.py
from powermem import Memory, auto_config
import logging

# Load configuration
config = auto_config()
memory = Memory(config=config)

# Enable DEBUG logging to view search details
logging.getLogger().setLevel(logging.DEBUG)

# Execute search
print("Executing verification search...")
result = memory.search(query="test query", limit=10)

print(f"\n✓ Search returned {len(result.get('results', []))} results")
print("  Sparse vector search is active (check DEBUG logs to confirm)")
```

**Expected output:**
```
Executing verification search...
DEBUG:powermem.storage.oceanbase.oceanbase:Executing sparse vector search query with sparse_vector: ...
DEBUG:powermem.storage.oceanbase.oceanbase:_sparse_search results, len : 10

✓ Search returned 10 results
  Sparse vector search is active (check DEBUG logs to confirm)
```

## Complete Example Code

Here is a complete usage example:

```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Complete Sparse Vector Example
Demonstrates how to use sparse vector functionality
"""
from powermem import Memory, auto_config
from scripts.script_manager import ScriptManager

def main():
    # 1. List available scripts
    print("=" * 60)
    print("Step 1: List Available Scripts")
    print("=" * 60)
    ScriptManager.list_scripts()

    # 2. View script details
    print("\n" + "=" * 60)
    print("Step 2: View Script Details")
    print("=" * 60)
    ScriptManager.info("upgrade-sparse-vector")
    ScriptManager.info("migrate-sparse-vector")

    # 3. Load configuration
    config = auto_config()

    # 4. Run upgrade script (add sparse vector support to existing table)
    print("\n" + "=" * 60)
    print("Step 3: Run Upgrade Script")
    print("=" * 60)
    ScriptManager.run('upgrade-sparse-vector', config)

    # 5. Create Memory instance
    memory = Memory(config=config)

    # 6. Run migration script (optional: generate sparse vectors for historical data)
    # Note: Only migrated data will participate in sparse vector retrieval, results are more accurate after migration
    print("\n" + "=" * 60)
    print("Step 4: Run Migration Script (Optional)")
    print("=" * 60)
    ScriptManager.run('migrate-sparse-vector', memory, batch_size=100, workers=3)

    # 7. Verify search
    print("\n" + "=" * 60)
    print("Step 5: Verify Search")
    print("=" * 60)
    result = memory.search(query="test query", limit=10)
    print(f"Search returned {len(result.get('results', []))} results")

if __name__ == "__main__":
    main()
```

## Extended Exercises

1. **Try different search weights**: Modify `vector_weight`, `fts_weight`, and `sparse_weight` parameters to observe changes in search results.

2. **Test large batch migration**: Increase the `workers` parameter value to observe improvements in migration speed.

3. **Compare search effectiveness**: Test search results with sparse vector enabled and disabled separately, and compare relevance.

## Related Documentation

- [Sparse Vector Guide](../guides/0011-sparse_vector.md) - Detailed sparse vector configuration guide
- [Configuration Guide](../guides/0003-configuration.md) - Complete configuration reference
- [Getting Started](../guides/0001-getting_started.md) - Quick start guide

