from uuid import uuid4

import pytest
from sqlalchemy import select

from db.models.deployment import (
    Deployment,
    DeploymentRunDevice,
    DeploymentTargetRule,
    DeploymentTargetRuleVersion,
    DeploymentVersion,
)
from db.models.device import Device
from db.models.device_type import DeviceType


@pytest.fixture
async def maintenance_rollout_fixture(db_session):
    device_type = DeviceType(
        type_id=f"maintenance-test-{uuid4()}",
        label=f"Maintenance test {uuid4()}",
        fields={},
    )
    deployment = Deployment(
        name=f"Maintenance rollout {uuid4()}",
        deployment_type="IOTEDGE",
    )
    rule = DeploymentTargetRule(name=f"Maintenance target {uuid4()}")
    db_session.add_all([device_type, deployment, rule])
    await db_session.flush()

    version = DeploymentVersion(
        deployment_id=deployment.id,
        version=1,
        content_hash="sha256:maintenance-test",
        definition="{}",
        change_note="Test version",
    )
    rule_version = DeploymentTargetRuleVersion(
        rule_id=rule.id,
        version=1,
        expression={
            "expression": [
                {"key": "device.region", "values": ["EU"], "operator": "AND"}
            ],
            "excluded_device_ids": [],
        },
    )
    eligible = Device(
        device_id=f"maintenance-eligible-{uuid4()}",
        type_id=device_type.type_id,
        device_data={"region": "EU"},
    )
    maintenance = Device(
        device_id=f"maintenance-skipped-{uuid4()}",
        type_id=device_type.type_id,
        device_data={"region": "EU"},
        maintenance_mode=True,
    )
    db_session.add_all([version, rule_version, eligible, maintenance])
    await db_session.flush()
    return version, rule, eligible, maintenance


@pytest.mark.integration
async def test_device_maintenance_mode_can_be_read_updated_and_listed(
    client, maintenance_rollout_fixture
):
    _version, _rule, eligible, maintenance = maintenance_rollout_fixture

    read = await client.get(f"/devices/{maintenance.device_id}/maintenance-mode")
    assert read.status_code == 200, read.text
    assert read.json() == {
        "device_id": maintenance.device_id,
        "maintenance_mode": True,
    }

    updated = await client.patch(
        f"/devices/{eligible.device_id}/maintenance-mode",
        json={"maintenance_mode": True},
    )
    assert updated.status_code == 200, updated.text
    assert updated.json()["maintenance_mode"] is True

    listing = await client.get("/devices/maintenance-mode")
    assert listing.status_code == 200, listing.text
    listed_by_id = {row["device_id"]: row for row in listing.json()}
    assert listed_by_id[eligible.device_id]["maintenance_mode"] is True
    assert listed_by_id[maintenance.device_id]["maintenance_mode"] is True


@pytest.mark.integration
async def test_preview_includes_but_execution_skips_maintenance_devices(
    client, db_session, maintenance_rollout_fixture
):
    version, rule, eligible, maintenance = maintenance_rollout_fixture
    preview = await client.post(
        "/deployment-management/executions/preview",
        json={"deployment_version_id": version.id, "rule_id": rule.id},
    )
    assert preview.status_code == 200, preview.text
    preview_devices = {
        row["device_id"]: row
        for row in preview.json()["matching_devices"]
    }
    assert preview_devices[eligible.device_id]["maintenance_mode"] is False
    assert preview_devices[maintenance.device_id]["maintenance_mode"] is True

    created = await client.post(
        "/deployment-management/executions",
        json={
            "deployment_version_id": version.id,
            "rule_version_id": preview.json()["rule_version_id"],
            "idempotency_key": str(uuid4()),
        },
    )
    assert created.status_code == 201, created.text
    assert created.json()["total_devices"] == 1

    run_devices = (
        await db_session.scalars(
            select(DeploymentRunDevice).where(
                DeploymentRunDevice.run_id == created.json()["id"]
            )
        )
    ).all()
    assert [row.device_id for row in run_devices] == [eligible.device_id]
    assert all(row.device_id != maintenance.device_id for row in run_devices)
