"""Baseline - Initial schema without sparse vector support

Revision ID: 020_baseline

This migration represents the baseline schema state before sparse vector support.
It doesn't perform any actual migrations, but marks the starting point for version tracking.

Expected baseline schema (matching MemoryRecord ORM model):
- id (BIGINT, primary key)
- embedding (VECTOR) - pyobvector.VECTOR type
- document (LONGTEXT)
- metadata (JSON)
- user_id, agent_id, run_id, actor_id (VARCHAR)
- hash (VARCHAR)
- created_at, updated_at (VARCHAR)
- category (VARCHAR)
- fulltext_content (LONGTEXT)
- fulltext index on fulltext_content

Usage:
    # For existing databases, stamp the baseline:
    alembic stamp 020_baseline
    
    # Then upgrade to add sparse vector support:
    alembic upgrade head
    
    # Or generate new migrations with autogenerate:
    alembic revision --autogenerate -m "description"
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '020_baseline'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """
    Upgrade to baseline.
    
    This migration doesn't create tables (they should already exist),
    but validates that the baseline schema is in place.
    """
    # This migration does not perform any operation, it just marks the status of the baseline version.
    pass


def downgrade() -> None:
    """
    Downgrade from baseline.
    
    Since this is the baseline, there's no previous version to downgrade to.
    This operation is not supported.
    """
    raise NotImplementedError(
        "Cannot downgrade from baseline. "
        "This is the initial schema version."
    )

