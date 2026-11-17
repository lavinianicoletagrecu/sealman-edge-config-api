"""add deployment descriptions and make target rules reusable

Revision ID: b8c9d0e1f2a3
Revises: a7b8c9d0e1f2
Create Date: 2026-11-05

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "b8c9d0e1f2a3"
down_revision: Union[str, None] = "a7b8c9d0e1f2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("deployments", sa.Column("description", sa.Text(), nullable=True))
    op.alter_column(
        "deployment_target_rules",
        "deployment_id",
        existing_type=sa.Integer(),
        nullable=True,
    )


def downgrade() -> None:
    op.alter_column(
        "deployment_target_rules",
        "deployment_id",
        existing_type=sa.Integer(),
        nullable=False,
    )
    op.drop_column("deployments", "description")
