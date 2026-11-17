"""remove the unused target-rule enabled flag

Revision ID: d0e1f2a3b4c5
Revises: c9d0e1f2a3b4
Create Date: 2026-11-11

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "d0e1f2a3b4c5"
down_revision: Union[str, None] = "c9d0e1f2a3b4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        sa.text(
            """
            UPDATE deployment_target_rule_versions
            SET expression = expression - 'enabled'
            WHERE jsonb_typeof(expression) = 'object'
              AND expression ? 'enabled'
            """
        )
    )
    op.drop_column("deployment_target_rules", "enabled")


def downgrade() -> None:
    op.add_column(
        "deployment_target_rules",
        sa.Column(
            "enabled",
            sa.Boolean(),
            server_default=sa.text("true"),
            nullable=False,
        ),
    )
    op.execute(
        sa.text(
            """
            UPDATE deployment_target_rule_versions
            SET expression = jsonb_set(expression, '{enabled}', 'true'::jsonb, true)
            WHERE jsonb_typeof(expression) = 'object'
              AND NOT (expression ? 'enabled')
            """
        )
    )
