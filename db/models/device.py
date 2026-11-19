from sqlalchemy import TIMESTAMP, Boolean, Column, ForeignKey, Integer, Text, func
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB
from db.base import Base

class Device(Base):
    __tablename__ = "devices"

    device_id = Column(Text, primary_key=True)
    type_id = Column(Text, ForeignKey("device_types.type_id"), nullable=False)
    device_data = Column(JSONB, nullable=False, default=dict)
    maintenance_mode = Column(Boolean, nullable=False, default=False, server_default="false")
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    endpoints = relationship(
        "Endpoint", back_populates="device", lazy="noload", passive_deletes=True
    )
    deployment_assignments = relationship(
        "DeviceDeployment",
        back_populates="device",
        lazy="noload",
        passive_deletes=True,
    )
    deployment_run_devices = relationship(
        "DeploymentRunDevice",
        back_populates="device",
        lazy="noload",
        passive_deletes=True,
    )


class DeviceSnapshotCache(Base):
    __tablename__ = "device_snapshot_cache"

    cache_key = Column(Text, primary_key=True)
    devices_json = Column(JSONB, nullable=False, default=list)
    device_count = Column(Integer, nullable=False)
    cached_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)