"""add durable per-device deployment dispatch outbox

Revision ID: e2f3a4b5c6d7
Revises: d1e2f3a4b5c6
Create Date: 2026-11-17

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "e2f3a4b5c6d7"
down_revision: Union[str, None] = "d1e2f3a4b5c6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "deployment_dispatch_jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("execution_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("device_id", sa.Text(), nullable=False),
        sa.Column("device_deployment_id", sa.Integer(), nullable=False),
        sa.Column("expected_generation", sa.Integer(), nullable=False),
        sa.Column("state", sa.Text(), server_default="QUEUED", nullable=False),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("max_attempts", sa.Integer(), server_default="8", nullable=False),
        sa.Column(
            "not_before",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("lease_token", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("lease_operation", sa.Text(), nullable=True),
        sa.Column("lease_expires_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("desired_twin_version", sa.Integer(), nullable=True),
        sa.Column("last_error_code", sa.Integer(), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("submitted_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "expected_generation > 0",
            name="ck_deployment_dispatch_jobs_positive_generation",
        ),
        sa.CheckConstraint(
            "attempts >= 0 AND max_attempts > 0",
            name="ck_deployment_dispatch_jobs_attempt_limits",
        ),
        sa.ForeignKeyConstraint(
            ["device_deployment_id"],
            ["device_deployments.id"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["device_id"], ["devices.device_id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["execution_id"],
            ["deployment_executions.id"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "execution_id", name="uq_deployment_dispatch_jobs_execution"
        ),
    )
    op.create_index(
        "ix_deployment_dispatch_jobs_due",
        "deployment_dispatch_jobs",
        ["state", "not_before", "created_at"],
    )
    op.create_index(
        "uq_deployment_dispatch_jobs_one_lease_per_device",
        "deployment_dispatch_jobs",
        ["device_id"],
        unique=True,
        postgresql_where=sa.text("state = 'LEASED'"),
    )


def downgrade() -> None:
    op.drop_index(
        "uq_deployment_dispatch_jobs_one_lease_per_device",
        table_name="deployment_dispatch_jobs",
    )
    op.drop_index(
        "ix_deployment_dispatch_jobs_due", table_name="deployment_dispatch_jobs"
    )
    op.drop_table("deployment_dispatch_jobs")
