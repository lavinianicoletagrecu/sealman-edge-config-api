from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker

from db.models.deployment import (
    Deployment,
    DeploymentDispatchJob,
    DeploymentExecution,
    DeploymentRun,
    DeploymentRunDevice,
    DeploymentTargetRule,
    DeploymentTargetRuleVersion,
    DeploymentVersion,
    DeviceDeployment,
)
from db.models.device import Device
from db.models.device_type import DeviceType


@pytest.fixture
async def deployment_rollout_fixture(db_session):
    device_type = DeviceType(
        type_id=f"deployment-test-{uuid4()}",
        label=f"Deployment test {uuid4()}",
        fields={},
    )
    deployment = Deployment(name=f"Rollout test {uuid4()}", deployment_type="IOTEDGE")
    rule = DeploymentTargetRule(name=f"Target test {uuid4()}")
    db_session.add_all([device_type, deployment, rule])
    await db_session.flush()

    version = DeploymentVersion(
        deployment_id=deployment.id,
        version=1,
        content_hash="sha256:test",
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
    db_session.add_all([version, rule_version])
    db_session.add_all(
        [
            Device(
                device_id=f"rollout-device-eu-{uuid4()}",
                type_id=device_type.type_id,
                device_data={"region": "EU"},
            ),
            Device(
                device_id=f"rollout-device-us-{uuid4()}",
                type_id=device_type.type_id,
                device_data={"region": "US"},
            ),
        ]
    )
    await db_session.flush()
    devices = (
        await db_session.scalars(
            select(Device).where(Device.type_id == device_type.type_id)
        )
    ).all()
    return deployment, version, rule, rule_version, devices


@pytest.mark.integration
async def test_rollout_preview_create_and_history_are_snapshotted(
    client, db_session, deployment_rollout_fixture
):
    deployment, version, rule, rule_version, devices = deployment_rollout_fixture
    expected_device = next(
        device for device in devices if device.device_data["region"] == "EU"
    )
    idempotency_key = str(uuid4())

    preview = await client.post(
        "/deployment-management/executions/preview",
        json={"deployment_version_id": version.id, "rule_id": rule.id},
    )
    assert preview.status_code == 200, preview.text
    assert preview.json()["rule_version_id"] == rule_version.id
    assert [row["device_id"] for row in preview.json()["matching_devices"]] == [
        expected_device.device_id
    ]

    create_payload = {
        "deployment_version_id": version.id,
        "rule_version_id": rule_version.id,
        "idempotency_key": idempotency_key,
    }
    created = await client.post("/deployment-management/executions", json=create_payload)
    assert created.status_code == 201, created.text
    run_id = created.json()["id"]
    assert created.json()["total_devices"] == 1
    assert created.json()["pending"] == 1
    assert created.json()["state"] == "PENDING"

    repeated = await client.post(
        "/deployment-management/executions", json=create_payload
    )
    assert repeated.status_code == 201, repeated.text
    assert repeated.json()["id"] == run_id

    run = await db_session.get(DeploymentRun, run_id)
    assert run is not None
    run_devices = (
        await db_session.scalars(
            select(DeploymentRunDevice).where(DeploymentRunDevice.run_id == run.id)
        )
    ).all()
    assignments = (
        await db_session.scalars(
            select(DeviceDeployment).where(
                DeviceDeployment.device_id == expected_device.device_id,
                DeviceDeployment.deployment_id == deployment.id,
            )
        )
    ).all()
    executions = (
        await db_session.scalars(
            select(DeploymentExecution).where(DeploymentExecution.run_id == run.id)
        )
    ).all()
    jobs = (
        await db_session.scalars(
            select(DeploymentDispatchJob).where(
                DeploymentDispatchJob.execution_id == executions[0].id
            )
        )
    ).all()
    assert len(run_devices) == len(assignments) == len(executions) == len(jobs) == 1
    assert run_devices[0].state == executions[0].state == "QUEUED"
    assert jobs[0].state == "QUEUED"
    assert jobs[0].expected_generation == executions[0].generation == 1
    assert assignments[0].desired_version_id == version.id
    assert assignments[0].assignment_generation == executions[0].generation == 1

    detail = await client.get(f"/deployment-management/executions/{run_id}")
    assert detail.status_code == 200, detail.text
    assert len(detail.json()["devices"]) == 1
    assert detail.json()["devices"][0]["device_id"] == expected_device.device_id

    history = await client.get(
        "/deployment-management/history",
        params={"deployment_id": deployment.id, "status": "QUEUED"},
    )
    assert history.status_code == 200, history.text
    assert history.json()["total"] == 1
    assert history.json()["items"][0]["run_id"] == run_id

    jobs[0].state = "FAILED"
    run_devices[0].state = "FAILED"
    executions[0].state = "FAILED"
    await db_session.flush()
    retried = await client.post(
        f"/deployment-management/executions/{run_id}/devices/"
        f"{expected_device.device_id}/retry"
    )
    assert retried.status_code == 202, retried.text
    await db_session.refresh(jobs[0])
    assert jobs[0].state == "QUEUED"
    assert jobs[0].attempts == 0


@pytest.mark.integration
async def test_rollout_rejects_reused_idempotency_key_for_different_request(
    client, db_session, deployment_rollout_fixture
):
    _deployment, version, _rule, rule_version, _devices = deployment_rollout_fixture
    idempotency_key = str(uuid4())
    response = await client.post(
        "/deployment-management/executions",
        json={
            "deployment_version_id": version.id,
            "rule_version_id": rule_version.id,
            "idempotency_key": idempotency_key,
        },
    )
    assert response.status_code == 201, response.text

    other_rule = DeploymentTargetRule(name=f"Other target {uuid4()}")
    db_session.add(other_rule)
    await db_session.flush()
    other_rule_version = DeploymentTargetRuleVersion(
        rule_id=other_rule.id,
        version=1,
        expression={"expression": [], "excluded_device_ids": []},
    )
    db_session.add(other_rule_version)
    await db_session.flush()
    repeated = await client.post(
        "/deployment-management/executions",
        json={
            "deployment_version_id": version.id,
            "rule_version_id": other_rule_version.id,
            "idempotency_key": idempotency_key,
        },
    )
    assert repeated.status_code == 409, repeated.text


@pytest.mark.integration
async def test_compose_deployments_are_excluded_from_execution(
    client, db_session, deployment_rollout_fixture
):
    _iotedge_deployment, _version, _rule, rule_version, _devices = (
        deployment_rollout_fixture
    )
    compose = Deployment(
        name=f"Compose rollout {uuid4()}",
        deployment_type="COMPOSE",
    )
    db_session.add(compose)
    await db_session.flush()
    compose_version = DeploymentVersion(
        deployment_id=compose.id,
        version=1,
        content_hash="sha256:compose-test",
        definition='{"services":[]}',
        change_note="Test compose version",
    )
    db_session.add(compose_version)
    await db_session.flush()

    preview = await client.post(
        "/deployment-management/executions/preview",
        json={"deployment_version_id": compose_version.id, "rule_id": _rule.id},
    )
    assert preview.status_code == 422, preview.text

    created = await client.post(
        "/deployment-management/executions",
        json={
            "deployment_version_id": compose_version.id,
            "rule_version_id": rule_version.id,
            "idempotency_key": str(uuid4()),
        },
    )
    assert created.status_code == 422, created.text


@pytest.mark.integration
async def test_dispatch_jobs_snapshot_generations_and_allow_one_lease_per_device(
    client, db_session, deployment_rollout_fixture
):
    _deployment, version, rule, rule_version, _devices = deployment_rollout_fixture
    for _ in range(2):
        response = await client.post(
            "/deployment-management/executions",
            json={
                "deployment_version_id": version.id,
                "rule_version_id": rule_version.id,
                "idempotency_key": str(uuid4()),
            },
        )
        assert response.status_code == 201, response.text

    jobs = (
        await db_session.scalars(
            select(DeploymentDispatchJob)
            .order_by(DeploymentDispatchJob.created_at, DeploymentDispatchJob.id)
        )
    ).all()
    assert len(jobs) == 2
    assert sorted(job.expected_generation for job in jobs) == [1, 2]

    for job in jobs:
        job.state = "LEASED"
        job.lease_operation = "DISPATCH"
        job.lease_token = uuid4()
        job.lease_expires_at = datetime.now(timezone.utc) + timedelta(minutes=3)
    with pytest.raises(IntegrityError):
        await db_session.flush()
    await db_session.rollback()


@pytest.mark.integration
async def test_worker_rejects_stale_generation_inside_the_claim_transaction(
    client, db_session, deployment_rollout_fixture, monkeypatch
):
    import routers.deployment_management.dispatcher as dispatcher

    _deployment, version, rule, rule_version, _devices = deployment_rollout_fixture
    for _ in range(2):
        response = await client.post(
            "/deployment-management/executions",
            json={
                "deployment_version_id": version.id,
                "rule_version_id": rule_version.id,
                "idempotency_key": str(uuid4()),
            },
        )
        assert response.status_code == 201, response.text

    jobs = (
        await db_session.scalars(
            select(DeploymentDispatchJob).order_by(DeploymentDispatchJob.created_at)
        )
    ).all()
    jobs[0].created_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    jobs[1].created_at = datetime(2026, 1, 2, tzinfo=timezone.utc)
    await db_session.flush()
    test_session_factory = async_sessionmaker(
        bind=db_session.bind,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )
    monkeypatch.setattr(dispatcher, "AsyncSessionLocal", test_session_factory)

    stale_claim = await dispatcher._claim_next_job()
    assert stale_claim is None
    await db_session.refresh(jobs[0])
    assert jobs[0].state == "SUPERSEDED"

    current_claim = await dispatcher._claim_next_job()
    assert current_claim is not None
    assert current_claim.expected_generation == 2
    assert current_claim.operation == "DISPATCH"
