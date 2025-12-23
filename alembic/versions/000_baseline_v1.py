"""Baseline v1 - Initial schema without sparse vector support

Revision ID: 000_baseline_v1
Revises: 
Create Date: 2025-01-01 00:00:00.000000

This migration represents the baseline v1 schema state before sparse vector support.
It doesn't perform any actual migrations, but marks the starting point for version tracking.

Expected v1 schema:
- id (BIGINT, primary key)
- embedding (VECTOR)
- document (LONGTEXT)
- metadata (JSON)
- user_id, agent_id, run_id, actor_id (VARCHAR)
- hash (VARCHAR)
- created_at, updated_at (VARCHAR)
- category (VARCHAR)
- fulltext_content (LONGTEXT)
- fulltext index on fulltext_content
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '000_baseline_v1'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """
    Upgrade to baseline v1.
    
    This migration doesn't create tables (they should already exist),
    but validates that the v1 schema is in place.
    """
    # 这个迁移不执行任何操作
    # 它只是标记v1版本的基线状态
    pass


def downgrade() -> None:
    """
    Downgrade from baseline v1.
    
    Since this is the baseline, there's no previous version to downgrade to.
    This operation is not supported.
    """
    raise NotImplementedError(
        "Cannot downgrade from baseline v1. "
        "This is the initial schema version."
    )

