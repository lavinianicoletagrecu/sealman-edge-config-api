import uuid
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import CheckConstraint, ForeignKey, Integer, Text, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base import Base


class Deployment(Base):
    __tablename__ = "deployments"
    __table_args__ = (UniqueConstraint("name", name="uq_deployments_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    deployment_type: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )
    deleted_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )

    versions: Mapped[list["DeploymentVersion"]] = relationship(
        back_populates="deployment", lazy="noload"
    )
    target_rules: Mapped[list["DeploymentTargetRule"]] = relationship(
        back_populates="deployment", lazy="noload"
    )
    runs: Mapped[list["DeploymentRun"]] = relationship(
        back_populates="deployment", lazy="noload"
    )
    device_deployments: Mapped[list["DeviceDeployment"]] = relationship(
        back_populates="deployment", lazy="noload"
    )


class DeploymentVersion(Base):
    __tablename__ = "deployment_versions"
    __table_args__ = (
        UniqueConstraint(
            "deployment_id", "version", name="uq_deployment_versions_deployment_version"
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    deployment_id: Mapped[int] = mapped_column(
        ForeignKey("deployments.id", ondelete="RESTRICT"), nullable=False
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    content_hash: Mapped[str] = mapped_column(Text, nullable=False)
    definition: Mapped[str] = mapped_column(Text, nullable=False)
    change_note: Mapped[str] = mapped_column(
        Text, nullable=False, default="", server_default=""
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )

    deployment: Mapped[Deployment] = relationship(
        back_populates="versions", lazy="noload"
    )
    containers: Mapped[list["DeploymentVersionContainer"]] = relationship(
        back_populates="deployment_version",
        cascade="all, delete-orphan",
        lazy="noload",
    )
    runs: Mapped[list["DeploymentRun"]] = relationship(
        back_populates="deployment_version", lazy="noload"
    )
    device_deployments: Mapped[list["DeviceDeployment"]] = relationship(
        back_populates="desired_version", lazy="noload"
    )
    executions: Mapped[list["DeploymentExecution"]] = relationship(
        back_populates="deployment_version", lazy="noload"
    )


class DeploymentVersionContainer(Base):
    __tablename__ = "deployment_version_containers"

    deployment_version_id: Mapped[int] = mapped_column(
        ForeignKey("deployment_versions.id", ondelete="CASCADE"), primary_key=True
    )
    service_name: Mapped[str] = mapped_column(Text, primary_key=True)
    image: Mapped[str] = mapped_column(Text, nullable=False)
    config_spec: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)

    deployment_version: Mapped[DeploymentVersion] = relationship(
        back_populates="containers", lazy="noload"
    )


class DeploymentTargetRule(Base):
    __tablename__ = "deployment_target_rules"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    deployment_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("deployments.id", ondelete="RESTRICT"), nullable=True
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    deleted_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )

    deployment: Mapped[Optional[Deployment]] = relationship(
        back_populates="target_rules", lazy="noload"
    )
    versions: Mapped[list["DeploymentTargetRuleVersion"]] = relationship(
        back_populates="rule",
        cascade="all, delete-orphan",
        lazy="noload",
    )


class DeploymentTargetRuleVersion(Base):
    __tablename__ = "deployment_target_rule_versions"
    __table_args__ = (
        UniqueConstraint("rule_id", "version", name="uq_target_rule_versions_rule_version"),
        CheckConstraint("version > 0", name="ck_target_rule_versions_positive_version"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    rule_id: Mapped[int] = mapped_column(
        ForeignKey("deployment_target_rules.id", ondelete="RESTRICT"), nullable=False
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    expression: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )
    created_by: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    rule: Mapped[DeploymentTargetRule] = relationship(
        back_populates="versions", lazy="noload"
    )
    runs: Mapped[list["DeploymentRun"]] = relationship(
        back_populates="rule_version", lazy="noload"
    )
    device_deployments: Mapped[list["DeviceDeployment"]] = relationship(
        back_populates="rule_version", lazy="noload"
    )
    executions: Mapped[list["DeploymentExecution"]] = relationship(
        back_populates="rule_version", lazy="noload"
    )


class DeploymentRun(Base):
    __tablename__ = "deployment_runs"
    __table_args__ = (
        UniqueConstraint(
            "idempotency_key", name="uq_deployment_runs_idempotency_key"
        ),
        CheckConstraint("total_devices >= 0", name="ck_deployment_runs_total_devices_nonnegative"),
        CheckConstraint("succeeded >= 0", name="ck_deployment_runs_succeeded_nonnegative"),
        CheckConstraint("failed >= 0", name="ck_deployment_runs_failed_nonnegative"),
        CheckConstraint("pending >= 0", name="ck_deployment_runs_pending_nonnegative"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    deployment_id: Mapped[int] = mapped_column(
        ForeignKey("deployments.id", ondelete="RESTRICT"), nullable=False
    )
    deployment_version_id: Mapped[int] = mapped_column(
        ForeignKey("deployment_versions.id", ondelete="RESTRICT"), nullable=False
    )
    rule_version_id: Mapped[int] = mapped_column(
        ForeignKey("deployment_target_rule_versions.id", ondelete="RESTRICT"),
        nullable=False,
    )
    idempotency_key: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    triggered_by: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    triggered_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )
    total_devices: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    succeeded: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    failed: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    pending: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")

    deployment: Mapped[Deployment] = relationship(back_populates="runs", lazy="noload")
    deployment_version: Mapped[DeploymentVersion] = relationship(
        back_populates="runs", lazy="noload"
    )
    rule_version: Mapped[DeploymentTargetRuleVersion] = relationship(
        back_populates="runs", lazy="noload"
    )
    devices: Mapped[list["DeploymentRunDevice"]] = relationship(
        back_populates="run",
        cascade="all, delete-orphan",
        lazy="noload",
    )
    executions: Mapped[list["DeploymentExecution"]] = relationship(
        back_populates="run", lazy="noload"
    )


class DeploymentRunDevice(Base):
    __tablename__ = "deployment_run_devices"

    run_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("deployment_runs.id", ondelete="CASCADE"), primary_key=True
    )
    device_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("devices.device_id", ondelete="RESTRICT"),
        primary_key=True,
    )
    state: Mapped[str] = mapped_column(Text, nullable=False)

    run: Mapped[DeploymentRun] = relationship(back_populates="devices", lazy="noload")
    device: Mapped["Device"] = relationship(
        back_populates="deployment_run_devices", lazy="noload"
    )


class DeviceDeployment(Base):
    __tablename__ = "device_deployments"
    __table_args__ = (
        UniqueConstraint(
            "device_id", "deployment_id", name="uq_device_deployments_device_deployment"
        ),
        CheckConstraint(
            "assignment_generation > 0",
            name="ck_device_deployments_positive_generation",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[str] = mapped_column(
        Text, ForeignKey("devices.device_id", ondelete="RESTRICT"), nullable=False
    )
    deployment_id: Mapped[int] = mapped_column(
        ForeignKey("deployments.id", ondelete="RESTRICT"), nullable=False
    )
    desired_version_id: Mapped[int] = mapped_column(
        ForeignKey("deployment_versions.id", ondelete="RESTRICT"), nullable=False
    )
    desired_state: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    rule_version_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("deployment_target_rule_versions.id", ondelete="RESTRICT"),
        nullable=True,
    )
    assignment_generation: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1, server_default="1"
    )

    device: Mapped["Device"] = relationship(
        back_populates="deployment_assignments", lazy="noload"
    )
    deployment: Mapped[Deployment] = relationship(
        back_populates="device_deployments", lazy="noload"
    )
    desired_version: Mapped[DeploymentVersion] = relationship(
        back_populates="device_deployments", lazy="noload"
    )
    rule_version: Mapped[Optional[DeploymentTargetRuleVersion]] = relationship(
        back_populates="device_deployments", lazy="noload"
    )
    executions: Mapped[list["DeploymentExecution"]] = relationship(
        back_populates="device_deployment", lazy="noload"
    )


class DeploymentExecution(Base):
    __tablename__ = "deployment_executions"
    __table_args__ = (
        CheckConstraint("generation > 0", name="ck_deployment_executions_positive_generation"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("deployment_runs.id", ondelete="RESTRICT"), nullable=False
    )
    device_deployment_id: Mapped[int] = mapped_column(
        ForeignKey("device_deployments.id", ondelete="RESTRICT"), nullable=False
    )
    deployment_version_id: Mapped[int] = mapped_column(
        ForeignKey("deployment_versions.id", ondelete="RESTRICT"), nullable=False
    )
    rule_version_id: Mapped[int] = mapped_column(
        ForeignKey("deployment_target_rule_versions.id", ondelete="RESTRICT"),
        nullable=False,
    )
    generation: Mapped[int] = mapped_column(Integer, nullable=False)
    content_hash: Mapped[str] = mapped_column(Text, nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False)
    triggered_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )
    trigger_source: Mapped[str] = mapped_column(Text, nullable=False)

    run: Mapped[DeploymentRun] = relationship(back_populates="executions", lazy="noload")
    device_deployment: Mapped[DeviceDeployment] = relationship(
        back_populates="executions", lazy="noload"
    )
    deployment_version: Mapped[DeploymentVersion] = relationship(
        back_populates="executions", lazy="noload"
    )
    rule_version: Mapped[DeploymentTargetRuleVersion] = relationship(
        back_populates="executions", lazy="noload"
    )
    dispatch_job: Mapped[Optional["DeploymentDispatchJob"]] = relationship(
        back_populates="execution", lazy="noload", uselist=False
    )


class DeploymentDispatchJob(Base):
    __tablename__ = "deployment_dispatch_jobs"
    __table_args__ = (
        UniqueConstraint("execution_id", name="uq_deployment_dispatch_jobs_execution"),
        CheckConstraint(
            "expected_generation > 0",
            name="ck_deployment_dispatch_jobs_positive_generation",
        ),
        CheckConstraint(
            "attempts >= 0 AND max_attempts > 0",
            name="ck_deployment_dispatch_jobs_attempt_limits",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    execution_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("deployment_executions.id", ondelete="RESTRICT"), nullable=False
    )
    device_id: Mapped[str] = mapped_column(
        Text, ForeignKey("devices.device_id", ondelete="RESTRICT"), nullable=False
    )
    device_deployment_id: Mapped[int] = mapped_column(
        ForeignKey("device_deployments.id", ondelete="RESTRICT"), nullable=False
    )
    expected_generation: Mapped[int] = mapped_column(Integer, nullable=False)
    state: Mapped[str] = mapped_column(
        Text, nullable=False, default="QUEUED", server_default="QUEUED"
    )
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=8, server_default="8")
    not_before: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )
    lease_token: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    lease_operation: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    lease_expires_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    desired_twin_version: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    last_error_code: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    submitted_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    execution: Mapped[DeploymentExecution] = relationship(
        back_populates="dispatch_job", lazy="noload"
    )
    device_deployment: Mapped[DeviceDeployment] = relationship(lazy="noload")
