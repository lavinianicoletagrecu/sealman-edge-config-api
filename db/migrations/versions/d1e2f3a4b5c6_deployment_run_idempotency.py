"""make deployment run creation idempotent

Revision ID: d1e2f3a4b5c6
Revises: d0e1f2a3b4c5
Create Date: 2026-11-11

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "d1e2f3a4b5c6"
down_revision: Union[str, None] = "d0e1f2a3b4c5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "deployment_runs",
        sa.Column("idempotency_key", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.create_unique_constraint(
        "uq_deployment_runs_idempotency_key",
        "deployment_runs",
        ["idempotency_key"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_deployment_runs_idempotency_key",
        "deployment_runs",
        type_="unique",
    )
    op.drop_column("deployment_runs", "idempotency_key")
