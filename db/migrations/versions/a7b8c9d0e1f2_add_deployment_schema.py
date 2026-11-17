"""add deployment definitions and execution history

Revision ID: a7b8c9d0e1f2
Revises: f6a7b8c9d0e1
Create Date: 2026-11-05

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "a7b8c9d0e1f2"
down_revision: Union[str, None] = "f6a7b8c9d0e1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

DEPLOYMENT_TABLES = {
    "deployments",
    "deployment_versions",
    "deployment_version_containers",
    "deployment_target_rules",
    "deployment_target_rule_versions",
    "deployment_runs",
    "deployment_run_devices",
    "device_deployments",
    "deployment_executions",
}


def upgrade() -> None:
    existing_tables = set(sa.inspect(op.get_bind()).get_table_names())
    existing_deployment_tables = existing_tables & DEPLOYMENT_TABLES
    if existing_deployment_tables:
        if existing_deployment_tables == DEPLOYMENT_TABLES:
            return
        missing_tables = sorted(DEPLOYMENT_TABLES - existing_deployment_tables)
        raise RuntimeError(
            "Cannot safely adopt a partial deployment schema. "
            f"Existing tables: {sorted(existing_deployment_tables)}; "
            f"missing tables: {missing_tables}."
        )

    op.create_table(
        "deployments",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("deployment_type", sa.Text(), nullable=False),
        sa.Column("deleted_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", name="uq_deployments_name"),
    )
    op.create_table(
        "deployment_versions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("deployment_id", sa.Integer(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("content_hash", sa.Text(), nullable=False),
        sa.Column("definition", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(
            ["deployment_id"], ["deployments.id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "deployment_id",
            "version",
            name="uq_deployment_versions_deployment_version",
        ),
    )
    op.create_table(
        "deployment_version_containers",
        sa.Column("deployment_version_id", sa.Integer(), nullable=False),
        sa.Column("service_name", sa.Text(), nullable=False),
        sa.Column("image", sa.Text(), nullable=False),
        sa.Column("config_spec", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.ForeignKeyConstraint(
            ["deployment_version_id"],
            ["deployment_versions.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("deployment_version_id", "service_name"),
    )
    op.create_table(
        "deployment_target_rules",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("deployment_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("enabled", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("deleted_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["deployment_id"], ["deployments.id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "deployment_target_rule_versions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("rule_id", sa.Integer(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("expression", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("created_by", sa.Text(), nullable=True),
        sa.CheckConstraint("version > 0", name="ck_target_rule_versions_positive_version"),
        sa.ForeignKeyConstraint(
            ["rule_id"], ["deployment_target_rules.id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "rule_id", "version", name="uq_target_rule_versions_rule_version"
        ),
    )
    op.create_table(
        "deployment_runs",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("deployment_id", sa.Integer(), nullable=False),
        sa.Column("deployment_version_id", sa.Integer(), nullable=False),
        sa.Column("rule_version_id", sa.Integer(), nullable=False),
        sa.Column("triggered_by", sa.Text(), nullable=True),
        sa.Column(
            "triggered_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("total_devices", sa.Integer(), server_default="0", nullable=False),
        sa.Column("succeeded", sa.Integer(), server_default="0", nullable=False),
        sa.Column("failed", sa.Integer(), server_default="0", nullable=False),
        sa.Column("pending", sa.Integer(), server_default="0", nullable=False),
        sa.CheckConstraint("total_devices >= 0", name="ck_deployment_runs_total_devices_nonnegative"),
        sa.CheckConstraint("succeeded >= 0", name="ck_deployment_runs_succeeded_nonnegative"),
        sa.CheckConstraint("failed >= 0", name="ck_deployment_runs_failed_nonnegative"),
        sa.CheckConstraint("pending >= 0", name="ck_deployment_runs_pending_nonnegative"),
        sa.ForeignKeyConstraint(
            ["deployment_id"], ["deployments.id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["deployment_version_id"], ["deployment_versions.id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["rule_version_id"],
            ["deployment_target_rule_versions.id"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "device_deployments",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.Text(), nullable=False),
        sa.Column("deployment_id", sa.Integer(), nullable=False),
        sa.Column("desired_version_id", sa.Integer(), nullable=False),
        sa.Column("desired_state", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("rule_version_id", sa.Integer(), nullable=True),
        sa.Column("assignment_generation", sa.Integer(), server_default="1", nullable=False),
        sa.CheckConstraint(
            "assignment_generation > 0",
            name="ck_device_deployments_positive_generation",
        ),
        sa.ForeignKeyConstraint(
            ["device_id"], ["devices.device_id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["deployment_id"], ["deployments.id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["desired_version_id"], ["deployment_versions.id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["rule_version_id"],
            ["deployment_target_rule_versions.id"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "device_id",
            "deployment_id",
            name="uq_device_deployments_device_deployment",
        ),
    )
    op.create_table(
        "deployment_run_devices",
        sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("device_id", sa.Text(), nullable=False),
        sa.Column("state", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(
            ["device_id"], ["devices.device_id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["run_id"], ["deployment_runs.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("run_id", "device_id"),
    )
    op.create_table(
        "deployment_executions",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("device_deployment_id", sa.Integer(), nullable=False),
        sa.Column("deployment_version_id", sa.Integer(), nullable=False),
        sa.Column("rule_version_id", sa.Integer(), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("content_hash", sa.Text(), nullable=False),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column(
            "triggered_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("trigger_source", sa.Text(), nullable=False),
        sa.CheckConstraint(
            "generation > 0", name="ck_deployment_executions_positive_generation"
        ),
        sa.ForeignKeyConstraint(
            ["run_id"], ["deployment_runs.id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["device_deployment_id"],
            ["device_deployments.id"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["deployment_version_id"], ["deployment_versions.id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["rule_version_id"],
            ["deployment_target_rule_versions.id"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    op.drop_table("deployment_executions")
    op.drop_table("deployment_run_devices")
    op.drop_table("device_deployments")
    op.drop_table("deployment_runs")
    op.drop_table("deployment_target_rule_versions")
    op.drop_table("deployment_target_rules")
    op.drop_table("deployment_version_containers")
    op.drop_table("deployment_versions")
    op.drop_table("deployments")
