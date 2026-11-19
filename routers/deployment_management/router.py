from datetime import datetime, timezone
import hashlib
import json
from typing import Any
from uuid import UUID

from fastapi import Depends, HTTPException
from sqlalchemy import Text, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import ValidationError
from sqlalchemy.dialects.postgresql import insert as pg_insert

from authorization.abac_permission_check import (
    ABACDeviceListFilter,
    ABACDeviceListFilterResult,
    ABACPermissionCheck,
)
from authorization.permission_types import Device, Platform
from db.models.deployment import (
    Deployment,
    DeploymentDispatchJob,
    DeploymentExecution,
    DeploymentRun,
    DeploymentRunDevice,
    DeploymentTargetRule,
    DeploymentTargetRuleVersion,
    DeploymentVersion,
    DeploymentVersionContainer,
    DeviceDeployment,
)
from db.models.device import Device as DeviceModel
from db.models.device_type import DeviceType
from db.models.endpoint import Endpoint, EndpointType
from db.models.service import Service, ServiceType
from db.session import get_db
from routers.base_api_router import BaseAPIRouter
from .schemas import (
    ComposeDefinition,
    DeploymentHistoryItem,
    DeploymentHistoryResponse,
    DeploymentResponse,
    DeploymentRunDeviceResponse,
    DeploymentRunDetailResponse,
    DeploymentRunListResponse,
    DeploymentRunPreviewRequest,
    DeploymentRunPreviewResponse,
    DeploymentRunResponse,
    DeploymentRunWrite,
    DeploymentVersionResponse,
    DeploymentVersionWrite,
    DeploymentWrite,
    IoTEdgeDefinition,
    MessageResponse,
    RegistryRepositoriesResponse,
    RegistryTagsResponse,
    TargetCondition,
    TargetDevice,
    TargetRuleResponse,
    TargetRuleValuesResponse,
    TargetRuleVersionResponse,
    TargetRuleWrite,
)
from .registry_catalog import (
    RegistryCatalogError,
    list_repositories,
    list_repository_tags,
)


deployment_management = BaseAPIRouter(
    prefix="/deployment-management",
    tags=["Deployment Management"],
)

_config_read = Depends(ABACPermissionCheck(Platform.CONFIG_READ, device_path=None))
_config_write = Depends(ABACPermissionCheck(Platform.CONFIG_WRITE, device_path=None))


@deployment_management.get(
    "/registry/repositories", response_model=RegistryRepositoriesResponse
)
async def get_registry_repositories(_=_config_read):
    try:
        return await list_repositories()
    except RegistryCatalogError as error:
        raise HTTPException(
            status_code=error.status_code, detail=str(error)
        ) from error


@deployment_management.get(
    "/registry/repositories/{repository:path}/tags",
    response_model=RegistryTagsResponse,
)
async def get_registry_repository_tags(repository: str, _=_config_read):
    try:
        return await list_repository_tags(repository)
    except RegistryCatalogError as error:
        raise HTTPException(
            status_code=error.status_code, detail=str(error)
        ) from error


def _deployment_response(
    deployment: Deployment, versions: list[DeploymentVersion]
) -> DeploymentResponse:
    ordered = sorted(versions, key=lambda version: version.version, reverse=True)
    return DeploymentResponse(
        id=deployment.id,
        name=deployment.name,
        deployment_type=deployment.deployment_type,
        description=deployment.description,
        created_at=deployment.created_at,
        latest_version=ordered[0].version if ordered else None,
        version_count=len(ordered),
    )


def _deployment_version_response(
    version: DeploymentVersion,
) -> DeploymentVersionResponse:
    return DeploymentVersionResponse(
        id=version.id,
        version=version.version,
        content_hash=version.content_hash,
        change_note=version.change_note,
        created_at=version.created_at,
        definition=json.loads(version.definition),
    )


def _snapshot_payload(body: TargetRuleWrite) -> dict[str, Any]:
    return {
        "expression": [condition.model_dump() for condition in body.expression],
        "excluded_device_ids": sorted(set(body.excluded_device_ids)),
    }


def _rule_version_response(
    version: DeploymentTargetRuleVersion,
) -> TargetRuleVersionResponse:
    snapshot = version.expression or {}
    return TargetRuleVersionResponse(
        id=version.id,
        version=version.version,
        expression=snapshot.get("expression", []),
        excluded_device_ids=snapshot.get("excluded_device_ids", []),
        created_at=version.created_at,
        created_by=version.created_by,
    )


async def _load_target_devices(
    session: AsyncSession,
    device_filter,
) -> list[TargetDevice]:
    devices = (
        await session.execute(
            select(DeviceModel, DeviceType.label)
            .join(DeviceType, DeviceModel.type_id == DeviceType.type_id)
            .order_by(DeviceModel.device_id)
        )
    ).all()
    visible = {
        device.device_id: (dict(device.device_data or {}), device_type)
        for device, device_type in devices
        if device_filter(dict(device.device_data or {}))
    }

    endpoint_rows = (
        await session.execute(
            select(Endpoint.device_id, Endpoint.endpoint_data, EndpointType.label)
            .join(EndpointType, Endpoint.type_id == EndpointType.type_id)
            .where(Endpoint.device_id.in_(visible))
            .order_by(Endpoint.device_id, Endpoint.endpoint_id)
        )
    ).all() if visible else []

    services_rows = (
        await session.execute(
            select(Endpoint.device_id, ServiceType.label)
            .join(Service, Service.endpoint_id == Endpoint.endpoint_id)
            .join(ServiceType, Service.type_id == ServiceType.type_id)
            .where(Endpoint.device_id.in_(visible))
            .order_by(Endpoint.device_id, ServiceType.label)
        )
    ).all() if visible else []

    endpoints_by_device: dict[str, list[dict[str, Any]]] = {
        device_id: [] for device_id in visible
    }
    services_by_device: dict[str, set[str]] = {
        device_id: set() for device_id in visible
    }
    for device_id, endpoint_data, endpoint_type in endpoint_rows:
        endpoints_by_device[device_id].append(
            {**dict(endpoint_data or {}), "type": endpoint_type}
        )
    for device_id, service_type in services_rows:
        services_by_device[device_id].add(service_type)

    return [
        TargetDevice(
            device_id=device_id,
            device_type=device_type,
            metadata=metadata,
            endpoints=endpoints_by_device[device_id],
            services=sorted(services_by_device[device_id]),
        )
        for device_id, (metadata, device_type) in visible.items()
    ]


def _condition_matches(device: TargetDevice, condition: dict[str, Any]) -> bool:
    key = condition.get("key", "")
    values = set(condition.get("values", []))
    if not values:
        return False
    scope, _, field = key.partition(".")
    if not field:
        scope, field = "device", scope
    if scope == "device":
        value = device.device_type if field == "device_type" else device.metadata.get(field)
        return str(value) in values if value is not None else False
    if scope == "endpoint":
        return any(str(endpoint.get(field, "")) in values for endpoint in device.endpoints)
    if scope == "service" and field == "type":
        return bool(values.intersection(device.services))
    return False


def _matches_rule(device: TargetDevice, snapshot: dict[str, Any]) -> bool:
    conditions = [
        condition
        for condition in snapshot.get("expression", [])
        if condition.get("values")
    ]
    if not conditions:
        return True

    groups: list[list[dict[str, Any]]] = [[]]
    for index, condition in enumerate(conditions):
        if index and condition.get("operator") == "OR":
            groups.append([])
        groups[-1].append(condition)
    return any(
        all(_condition_matches(device, condition) for condition in group)
        for group in groups
    )


@deployment_management.get("/definitions", response_model=list[DeploymentResponse])
async def list_deployment_definitions(
    session: AsyncSession = Depends(get_db),
    _=_config_read,
):
    deployments = (
        await session.scalars(
            select(Deployment)
            .where(Deployment.deleted_at.is_(None))
            .order_by(Deployment.name)
        )
    ).all()
    versions = (
        await session.scalars(
            select(DeploymentVersion)
            .where(DeploymentVersion.deployment_id.in_([item.id for item in deployments]))
        )
    ).all() if deployments else []
    versions_by_deployment: dict[int, list[DeploymentVersion]] = {}
    for version in versions:
        versions_by_deployment.setdefault(version.deployment_id, []).append(version)
    return [
        _deployment_response(deployment, versions_by_deployment.get(deployment.id, []))
        for deployment in deployments
    ]


@deployment_management.post(
    "/definitions", response_model=DeploymentResponse, status_code=201
)
async def create_deployment_definition(
    body: DeploymentWrite,
    session: AsyncSession = Depends(get_db),
    _=_config_write,
):
    deployment = Deployment(**body.model_dump())
    session.add(deployment)
    try:
        await session.commit()
    except IntegrityError as error:
        await session.rollback()
        raise HTTPException(
            status_code=409, detail=f"Deployment '{body.name}' already exists"
        ) from error
    await session.refresh(deployment)
    return _deployment_response(deployment, [])


@deployment_management.put(
    "/definitions/{deployment_id}", response_model=DeploymentResponse
)
async def update_deployment_definition(
    deployment_id: int,
    body: DeploymentWrite,
    session: AsyncSession = Depends(get_db),
    _=_config_write,
):
    deployment = await session.get(Deployment, deployment_id)
    if deployment is None or deployment.deleted_at is not None:
        raise HTTPException(status_code=404, detail="Deployment definition not found")
    if deployment.deployment_type != body.deployment_type:
        has_versions = await session.scalar(
            select(DeploymentVersion.id)
            .where(DeploymentVersion.deployment_id == deployment_id)
            .limit(1)
        )
        if has_versions is not None:
            raise HTTPException(
                status_code=409,
                detail="Deployment type cannot change after its first version is created",
            )
    for field, value in body.model_dump().items():
        setattr(deployment, field, value)
    try:
        await session.commit()
    except IntegrityError as error:
        await session.rollback()
        raise HTTPException(
            status_code=409, detail=f"Deployment '{body.name}' already exists"
        ) from error
    versions = (
        await session.scalars(
            select(DeploymentVersion).where(
                DeploymentVersion.deployment_id == deployment_id
            )
        )
    ).all()
    return _deployment_response(deployment, list(versions))


@deployment_management.get(
    "/definitions/{deployment_id}/versions",
    response_model=list[DeploymentVersionResponse],
)
async def list_deployment_versions(
    deployment_id: int,
    session: AsyncSession = Depends(get_db),
    _=_config_read,
):
    deployment = await session.get(Deployment, deployment_id)
    if deployment is None or deployment.deleted_at is not None:
        raise HTTPException(status_code=404, detail="Deployment definition not found")
    versions = (
        await session.scalars(
            select(DeploymentVersion)
            .where(DeploymentVersion.deployment_id == deployment_id)
            .order_by(DeploymentVersion.version.desc())
        )
    ).all()
    return [_deployment_version_response(version) for version in versions]


@deployment_management.post(
    "/definitions/{deployment_id}/versions",
    response_model=DeploymentVersionResponse,
    status_code=201,
)
async def create_deployment_version(
    deployment_id: int,
    body: DeploymentVersionWrite,
    session: AsyncSession = Depends(get_db),
    _=_config_write,
):
    deployment = await session.get(Deployment, deployment_id)
    if deployment is None or deployment.deleted_at is not None:
        raise HTTPException(status_code=404, detail="Deployment definition not found")

    definition_model = (
        IoTEdgeDefinition
        if deployment.deployment_type == "IOTEDGE"
        else ComposeDefinition
    )
    if deployment.deployment_type not in {"IOTEDGE", "COMPOSE"}:
        raise HTTPException(status_code=422, detail="Unsupported deployment type")
    try:
        definition = definition_model.model_validate(body.definition).model_dump(
            mode="json"
        )
    except ValidationError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error

    items = definition.get("modules", []) if deployment.deployment_type == "IOTEDGE" else definition.get("services", [])
    item_names = [item["name"].strip() for item in items]
    if any(not item_name for item_name in item_names) or len(item_names) != len(set(item_names)):
        item_label = "module" if deployment.deployment_type == "IOTEDGE" else "service"
        raise HTTPException(
            status_code=422,
            detail=f"{item_label.capitalize()} names must be non-empty and unique",
        )

    serialized = json.dumps(definition, sort_keys=True, separators=(",", ":"))
    content_hash = f"sha256:{hashlib.sha256(serialized.encode('utf-8')).hexdigest()}"
    duplicate = await session.scalar(
        select(DeploymentVersion.id).where(
            DeploymentVersion.deployment_id == deployment_id,
            DeploymentVersion.content_hash == content_hash,
        )
    )
    if duplicate is not None:
        raise HTTPException(
            status_code=409, detail="This definition is unchanged from an existing version"
        )
    latest = await session.scalar(
        select(DeploymentVersion.version)
        .where(DeploymentVersion.deployment_id == deployment_id)
        .order_by(DeploymentVersion.version.desc())
        .limit(1)
    )
    version = DeploymentVersion(
        deployment_id=deployment_id,
        version=(latest or 0) + 1,
        content_hash=content_hash,
        definition=serialized,
        change_note=body.change_note,
    )
    session.add(version)
    if deployment.deployment_type == "COMPOSE":
        await session.flush()
        session.add_all(
            [
                DeploymentVersionContainer(
                    deployment_version_id=version.id,
                    service_name=service["name"],
                    image=(
                        f"{service['image']}:{service['version']}"
                        if service["version"]
                        else service["image"]
                    ),
                    config_spec={
                        key: value
                        for key, value in service.items()
                        if key not in {"name", "image", "version"}
                    },
                )
                for service in definition["services"]
            ]
        )
    try:
        await session.commit()
    except IntegrityError as error:
        await session.rollback()
        raise HTTPException(status_code=409, detail="A deployment version was created concurrently") from error
    await session.refresh(version)
    return _deployment_version_response(version)


@deployment_management.get("/target-rule-values", response_model=TargetRuleValuesResponse)
async def get_target_rule_values(
    device_read: ABACDeviceListFilterResult = Depends(ABACDeviceListFilter(Device.READ)),
    endpoint_read: ABACDeviceListFilterResult = Depends(
        ABACDeviceListFilter(Device.ENDPOINT_READ)
    ),
    session: AsyncSession = Depends(get_db),
    _=_config_read,
):
    def visible(metadata: dict[str, Any]) -> bool:
        return (
            device_read["filter_device"](metadata)
            and endpoint_read["filter_device"](metadata)
        )

    devices = await _load_target_devices(session, visible)
    values: dict[str, set[str]] = {}
    for device in devices:
        values.setdefault("device.device_type", set()).add(device.device_type)
        for key, value in device.metadata.items():
            if key != "device_type" and value is not None and not isinstance(value, (dict, list)):
                values.setdefault(f"device.{key}", set()).add(str(value))
        for endpoint in device.endpoints:
            for key, value in endpoint.items():
                if value is not None and not isinstance(value, (dict, list)):
                    values.setdefault(f"endpoint.{key}", set()).add(str(value))
        for service in device.services:
            values.setdefault("service.type", set()).add(service)
    return TargetRuleValuesResponse(
        values={key: sorted(items) for key, items in sorted(values.items())},
        devices=devices,
    )


@deployment_management.get("/target-rules", response_model=list[TargetRuleResponse])
async def list_target_rules(
    device_read: ABACDeviceListFilterResult = Depends(ABACDeviceListFilter(Device.READ)),
    endpoint_read: ABACDeviceListFilterResult = Depends(
        ABACDeviceListFilter(Device.ENDPOINT_READ)
    ),
    session: AsyncSession = Depends(get_db),
    _=_config_read,
):
    rules = (
        await session.scalars(
            select(DeploymentTargetRule)
            .where(DeploymentTargetRule.deleted_at.is_(None))
            .order_by(DeploymentTargetRule.name)
        )
    ).all()
    if not rules:
        return []

    rule_versions = (
        await session.scalars(
            select(DeploymentTargetRuleVersion)
            .where(
                DeploymentTargetRuleVersion.rule_id.in_([rule.id for rule in rules])
            )
            .order_by(
                DeploymentTargetRuleVersion.rule_id,
                DeploymentTargetRuleVersion.version.desc(),
            )
        )
    ).all()
    latest_by_rule: dict[int, DeploymentTargetRuleVersion] = {}
    for version in rule_versions:
        latest_by_rule.setdefault(version.rule_id, version)

    def visible(metadata: dict[str, Any]) -> bool:
        return (
            device_read["filter_device"](metadata)
            and endpoint_read["filter_device"](metadata)
        )

    devices = await _load_target_devices(session, visible)
    runs = (
        await session.scalars(
            select(DeploymentRun)
            .where(
                DeploymentRun.rule_version_id.in_([version.id for version in rule_versions])
            )
            .order_by(DeploymentRun.triggered_at.desc())
        )
    ).all() if rule_versions else []
    latest_run_by_rule: dict[int, DeploymentRun] = {}
    version_to_rule = {version.id: version.rule_id for version in rule_versions}
    for run in runs:
        rule_id = version_to_rule.get(run.rule_version_id)
        if rule_id is not None:
            latest_run_by_rule.setdefault(rule_id, run)
    used_deployment_ids = {
        run.deployment_id for run in latest_run_by_rule.values()
    }
    deployments = (
        await session.scalars(
            select(Deployment).where(Deployment.id.in_(used_deployment_ids))
        )
    ).all() if used_deployment_ids else []
    deployments_by_id = {deployment.id: deployment for deployment in deployments}
    versions_by_id = {
        version.id: version
        for version in (
            await session.scalars(
                select(DeploymentVersion).where(
                    DeploymentVersion.id.in_(
                        [run.deployment_version_id for run in latest_run_by_rule.values()]
                    )
                )
            )
        ).all()
    } if latest_run_by_rule else {}

    responses = []
    for rule in rules:
        version = latest_by_rule.get(rule.id)
        if version is None:
            continue
        snapshot = version.expression or {}
        excluded = set(snapshot.get("excluded_device_ids", []))
        matched = sum(
            1
            for device in devices
            if device.device_id not in excluded and _matches_rule(device, snapshot)
        )
        run = latest_run_by_rule.get(rule.id)
        run_deployment = deployments_by_id.get(run.deployment_id) if run else None
        run_version = versions_by_id.get(run.deployment_version_id) if run else None
        total_runs = sum(
            1 for candidate in runs if version_to_rule.get(candidate.rule_version_id) == rule.id
        )
        responses.append(
            TargetRuleResponse(
                id=rule.id,
                name=rule.name,
                updated_at=version.created_at,
                version=_rule_version_response(version),
                matching_devices=matched,
                used_deployment=run_deployment.name if run_deployment else None,
                used_version=run_version.version if run_version else None,
                deployment_count=total_runs,
                is_synced=(run.rule_version_id == version.id) if run else None,
            )
        )
    return responses


async def _create_rule_version(
    session: AsyncSession,
    rule: DeploymentTargetRule,
    body: TargetRuleWrite,
    created_by: str | None,
) -> DeploymentTargetRuleVersion:
    latest = await session.scalar(
        select(DeploymentTargetRuleVersion.version)
        .where(DeploymentTargetRuleVersion.rule_id == rule.id)
        .order_by(DeploymentTargetRuleVersion.version.desc())
        .limit(1)
    )
    version = DeploymentTargetRuleVersion(
        rule_id=rule.id,
        version=(latest or 0) + 1,
        expression=_snapshot_payload(body),
        created_by=created_by,
    )
    session.add(version)
    return version


@deployment_management.post(
    "/target-rules", response_model=TargetRuleResponse, status_code=201
)
async def create_target_rule(
    body: TargetRuleWrite,
    session: AsyncSession = Depends(get_db),
    _=_config_write,
):
    duplicate = await session.scalar(
        select(DeploymentTargetRule.id).where(
            DeploymentTargetRule.name == body.name,
            DeploymentTargetRule.deleted_at.is_(None),
        )
    )
    if duplicate is not None:
        raise HTTPException(status_code=409, detail="A target rule with this name exists")
    rule = DeploymentTargetRule(name=body.name)
    session.add(rule)
    await session.flush()
    version = await _create_rule_version(session, rule, body, None)
    await session.commit()
    await session.refresh(version)
    return TargetRuleResponse(
        id=rule.id,
        name=rule.name,
        updated_at=version.created_at,
        version=_rule_version_response(version),
        matching_devices=0,
    )


@deployment_management.put(
    "/target-rules/{rule_id}", response_model=TargetRuleResponse
)
async def update_target_rule(
    rule_id: int,
    body: TargetRuleWrite,
    session: AsyncSession = Depends(get_db),
    _=_config_write,
):
    rule = await session.get(DeploymentTargetRule, rule_id)
    if rule is None or rule.deleted_at is not None:
        raise HTTPException(status_code=404, detail="Target rule not found")
    duplicate = await session.scalar(
        select(DeploymentTargetRule.id).where(
            DeploymentTargetRule.name == body.name,
            DeploymentTargetRule.id != rule_id,
            DeploymentTargetRule.deleted_at.is_(None),
        )
    )
    if duplicate is not None:
        raise HTTPException(status_code=409, detail="A target rule with this name exists")
    rule.name = body.name
    version = await _create_rule_version(session, rule, body, None)
    await session.commit()
    await session.refresh(version)
    return TargetRuleResponse(
        id=rule.id,
        name=rule.name,
        updated_at=version.created_at,
        version=_rule_version_response(version),
        matching_devices=0,
    )


@deployment_management.delete(
    "/target-rules/{rule_id}", response_model=MessageResponse
)
async def delete_target_rule(
    rule_id: int,
    session: AsyncSession = Depends(get_db),
    _=_config_write,
):
    rule = await session.get(DeploymentTargetRule, rule_id)
    if rule is None or rule.deleted_at is not None:
        raise HTTPException(status_code=404, detail="Target rule not found")
    rule.deleted_at = datetime.now(timezone.utc)
    await session.commit()
    return MessageResponse(message="Target rule deleted")


async def _visible_device_ids(
    session: AsyncSession,
    device_read: ABACDeviceListFilterResult,
    endpoint_read: ABACDeviceListFilterResult,
) -> list[str]:
    devices = (
        await session.execute(select(DeviceModel.device_id, DeviceModel.device_data))
    ).all()
    return [
        device_id
        for device_id, metadata in devices
        if device_read["filter_device"](dict(metadata or {}))
        and endpoint_read["filter_device"](dict(metadata or {}))
    ]


def _run_state(succeeded: int, failed: int, pending: int, queued: int = 0) -> str:
    if (pending or queued) and (succeeded or failed):
        return "IN_PROGRESS"
    if pending or queued:
        return "PENDING"
    if succeeded and failed:
        return "COMPLETED_WITH_ERRORS"
    if failed:
        return "FAILED"
    return "COMPLETED"


async def _refresh_run_counts(session: AsyncSession, run_id: UUID) -> None:
    run = await session.get(DeploymentRun, run_id)
    if run is None:
        return
    states = (
        await session.scalars(
            select(DeploymentRunDevice.state).where(
                DeploymentRunDevice.run_id == run_id
            )
        )
    ).all()
    run.succeeded = sum(
        1 for state in states if state.upper() in {"APPLIED", "SUCCESS", "SUCCEEDED", "COMPLETED"}
    )
    run.failed = sum(
        1 for state in states if state.upper() in {"FAILED", "ERROR", "SUPERSEDED"}
    )
    queued = sum(1 for state in states if state.upper() == "QUEUED")
    run.pending = len(states) - run.succeeded - run.failed - queued


async def _visible_run_counts(
    session: AsyncSession, run_id: UUID, visible_ids: set[str]
) -> dict[str, int] | None:
    if not visible_ids:
        return None
    states = (
        await session.scalars(
            select(DeploymentRunDevice.state).where(
                DeploymentRunDevice.run_id == run_id,
                DeploymentRunDevice.device_id.in_(visible_ids),
            )
        )
    ).all()
    if not states:
        return None
    succeeded = sum(
        1 for state in states if state.upper() in {"APPLIED", "SUCCESS", "SUCCEEDED", "COMPLETED"}
    )
    failed = sum(
        1 for state in states if state.upper() in {"FAILED", "ERROR", "SUPERSEDED"}
    )
    queued = sum(1 for state in states if state.upper() == "QUEUED")
    return {
        "total_devices": len(states),
        "succeeded": succeeded,
        "failed": failed,
        "pending": len(states) - succeeded - failed - queued,
        "queued": queued,
    }


def _run_response(
    run: DeploymentRun,
    deployment: Deployment,
    deployment_version: DeploymentVersion,
    rule: DeploymentTargetRule,
    rule_version: DeploymentTargetRuleVersion,
    counts: dict[str, int] | None = None,
) -> DeploymentRunResponse:
    values = counts or {
        "total_devices": run.total_devices,
        "succeeded": run.succeeded,
        "failed": run.failed,
        "pending": run.pending,
        "queued": max(
            0,
            run.total_devices - run.succeeded - run.failed - run.pending,
        ),
    }
    return DeploymentRunResponse(
        id=run.id,
        deployment_id=deployment.id,
        deployment_name=deployment.name,
        deployment_type=deployment.deployment_type,
        deployment_version_id=deployment_version.id,
        deployment_version=deployment_version.version,
        rule_id=rule.id,
        rule_name=rule.name,
        rule_version_id=rule_version.id,
        rule_version=rule_version.version,
        triggered_at=run.triggered_at,
        total_devices=values["total_devices"],
        succeeded=values["succeeded"],
        failed=values["failed"],
        pending=values["pending"],
        queued=values["queued"],
        state=_run_state(
            values["succeeded"],
            values["failed"],
            values["pending"],
            values["queued"],
        ),
    )


async def _resolve_rule_preview(
    session: AsyncSession,
    deployment_version_id: int,
    rule_id: int,
) -> tuple[Deployment, DeploymentVersion, DeploymentTargetRule, DeploymentTargetRuleVersion]:
    deployment_version = await session.get(DeploymentVersion, deployment_version_id)
    if deployment_version is None:
        raise HTTPException(status_code=404, detail="Deployment version not found")
    deployment = await session.get(Deployment, deployment_version.deployment_id)
    if deployment is None or deployment.deleted_at is not None:
        raise HTTPException(status_code=404, detail="Deployment definition not found")
    if deployment.deployment_type != "IOTEDGE":
        raise HTTPException(
            status_code=422,
            detail="Only IoT Edge deployment definitions can be executed",
        )

    rule = await session.get(DeploymentTargetRule, rule_id)
    if rule is None or rule.deleted_at is not None:
        raise HTTPException(status_code=404, detail="Target rule not found")
    rule_version = await session.scalar(
        select(DeploymentTargetRuleVersion)
        .where(DeploymentTargetRuleVersion.rule_id == rule.id)
        .order_by(DeploymentTargetRuleVersion.version.desc())
        .limit(1)
    )
    if rule_version is None:
        raise HTTPException(status_code=409, detail="Target rule has no saved version")
    return deployment, deployment_version, rule, rule_version


async def _matching_devices_for_rule(
    session: AsyncSession,
    rule_version: DeploymentTargetRuleVersion,
    device_read: ABACDeviceListFilterResult,
    endpoint_read: ABACDeviceListFilterResult,
) -> list[TargetDevice]:
    def visible(metadata: dict[str, Any]) -> bool:
        return (
            device_read["filter_device"](metadata)
            and endpoint_read["filter_device"](metadata)
        )

    candidates = await _load_target_devices(session, visible)
    snapshot = rule_version.expression or {}
    excluded = set(snapshot.get("excluded_device_ids", []))
    return [
        device
        for device in candidates
        if device.device_id not in excluded and _matches_rule(device, snapshot)
    ]


@deployment_management.post(
    "/executions/preview", response_model=DeploymentRunPreviewResponse
)
async def preview_deployment_run(
    body: DeploymentRunPreviewRequest,
    device_read: ABACDeviceListFilterResult = Depends(ABACDeviceListFilter(Device.READ)),
    endpoint_read: ABACDeviceListFilterResult = Depends(
        ABACDeviceListFilter(Device.ENDPOINT_READ)
    ),
    session: AsyncSession = Depends(get_db),
    _=_config_read,
):
    deployment, deployment_version, rule, rule_version = await _resolve_rule_preview(
        session, body.deployment_version_id, body.rule_id
    )
    devices = await _matching_devices_for_rule(
        session, rule_version, device_read, endpoint_read
    )
    return DeploymentRunPreviewResponse(
        deployment_id=deployment.id,
        deployment_name=deployment.name,
        deployment_type=deployment.deployment_type,
        deployment_version_id=deployment_version.id,
        deployment_version=deployment_version.version,
        rule_id=rule.id,
        rule_name=rule.name,
        rule_version_id=rule_version.id,
        rule_version=rule_version.version,
        matching_devices=devices,
    )


@deployment_management.post(
    "/executions", response_model=DeploymentRunResponse, status_code=201
)
async def create_deployment_run(
    body: DeploymentRunWrite,
    device_read: ABACDeviceListFilterResult = Depends(ABACDeviceListFilter(Device.READ)),
    endpoint_read: ABACDeviceListFilterResult = Depends(
        ABACDeviceListFilter(Device.ENDPOINT_READ)
    ),
    session: AsyncSession = Depends(get_db),
    _=_config_write,
):
    existing_row = (
        await session.execute(
            _run_query().where(DeploymentRun.idempotency_key == body.idempotency_key)
        )
    ).first()
    if existing_row is not None:
        existing, deployment, deployment_version, rule, rule_version = existing_row
        if deployment.deployment_type != "IOTEDGE":
            raise HTTPException(
                status_code=422,
                detail="Only IoT Edge deployment definitions can be executed",
            )
        if (
            deployment_version.id != body.deployment_version_id
            or rule_version.id != body.rule_version_id
        ):
            raise HTTPException(
                status_code=409,
                detail="Idempotency key was already used for a different rollout",
            )
        visible_ids = set(await _visible_device_ids(session, device_read, endpoint_read))
        counts = await _visible_run_counts(session, existing.id, visible_ids)
        if counts is None:
            raise HTTPException(status_code=404, detail="Deployment run not found")
        return _run_response(
            existing, deployment, deployment_version, rule, rule_version, counts
        )

    deployment_version = await session.get(
        DeploymentVersion, body.deployment_version_id
    )
    if deployment_version is None:
        raise HTTPException(status_code=404, detail="Deployment version not found")
    deployment = await session.get(Deployment, deployment_version.deployment_id)
    if deployment is None or deployment.deleted_at is not None:
        raise HTTPException(status_code=404, detail="Deployment definition not found")
    if deployment.deployment_type != "IOTEDGE":
        raise HTTPException(
            status_code=422,
            detail="Only IoT Edge deployment definitions can be executed",
        )

    rule_version = await session.get(
        DeploymentTargetRuleVersion, body.rule_version_id
    )
    if rule_version is None:
        raise HTTPException(status_code=404, detail="Target rule version not found")
    rule = await session.get(DeploymentTargetRule, rule_version.rule_id)
    if rule is None or rule.deleted_at is not None:
        raise HTTPException(status_code=404, detail="Target rule not found")

    devices = await _matching_devices_for_rule(
        session, rule_version, device_read, endpoint_read
    )
    if not devices:
        raise HTTPException(
            status_code=422,
            detail="The selected target rule currently matches no visible devices",
        )

    device_ids = sorted({device.device_id for device in devices})
    run = DeploymentRun(
        deployment_id=deployment.id,
        deployment_version_id=deployment_version.id,
        rule_version_id=rule_version.id,
        idempotency_key=body.idempotency_key,
        total_devices=len(device_ids),
        pending=0,
    )
    try:
        session.add(run)
        await session.flush()

        session.add_all(
            DeploymentRunDevice(run_id=run.id, device_id=device_id, state="QUEUED")
            for device_id in device_ids
        )
        assignment_by_device: dict[str, tuple[int, int]] = {}
        for start in range(0, len(device_ids), 500):
            batch = device_ids[start : start + 500]
            statement = pg_insert(DeviceDeployment).values(
                [
                    {
                        "device_id": device_id,
                        "deployment_id": deployment.id,
                        "desired_version_id": deployment_version.id,
                        "desired_state": "DEPLOYED",
                        "source": "TARGET_RULE",
                        "rule_version_id": rule_version.id,
                        "assignment_generation": 1,
                    }
                    for device_id in batch
                ]
            )
            statement = statement.on_conflict_do_update(
                index_elements=[
                    DeviceDeployment.device_id,
                    DeviceDeployment.deployment_id,
                ],
                set_={
                    "desired_version_id": statement.excluded.desired_version_id,
                    "desired_state": statement.excluded.desired_state,
                    "source": statement.excluded.source,
                    "rule_version_id": statement.excluded.rule_version_id,
                    "assignment_generation": DeviceDeployment.assignment_generation + 1,
                },
            ).returning(
                DeviceDeployment.device_id,
                DeviceDeployment.id,
                DeviceDeployment.assignment_generation,
            )
            rows = (await session.execute(statement)).all()
            assignment_by_device.update(
                {
                    device_id: (assignment_id, generation)
                    for device_id, assignment_id, generation in rows
                }
            )

        session.add_all(
            DeploymentExecution(
                run_id=run.id,
                device_deployment_id=assignment_by_device[device_id][0],
                deployment_version_id=deployment_version.id,
                rule_version_id=rule_version.id,
                generation=assignment_by_device[device_id][1],
                content_hash=deployment_version.content_hash,
                state="QUEUED",
                trigger_source="TARGET_RULE",
            )
            for device_id in device_ids
        )
        await session.flush()
        execution_rows = (
            await session.execute(
                select(DeploymentExecution, DeviceDeployment.device_id)
                .join(
                    DeviceDeployment,
                    DeploymentExecution.device_deployment_id == DeviceDeployment.id,
                )
                .where(DeploymentExecution.run_id == run.id)
            )
        ).all()
        execution_by_device = {
            device_id: execution
            for execution, device_id in execution_rows
        }
        session.add_all(
            DeploymentDispatchJob(
                execution_id=execution_by_device[device_id].id,
                device_id=device_id,
                device_deployment_id=assignment_by_device[device_id][0],
                expected_generation=assignment_by_device[device_id][1],
            )
            for device_id in device_ids
        )
        await session.commit()
    except IntegrityError as error:
        await session.rollback()
        existing_row = (
            await session.execute(
                _run_query().where(
                    DeploymentRun.idempotency_key == body.idempotency_key
                )
            )
        ).first()
        if existing_row is not None:
            existing, existing_deployment, existing_version, existing_rule, existing_rule_version = existing_row
            if (
                existing_version.id == body.deployment_version_id
                and existing_rule_version.id == body.rule_version_id
            ):
                visible_ids = set(
                    await _visible_device_ids(session, device_read, endpoint_read)
                )
                counts = await _visible_run_counts(
                    session, existing.id, visible_ids
                )
                if counts is None:
                    raise HTTPException(
                        status_code=404, detail="Deployment run not found"
                    ) from error
                return _run_response(
                    existing,
                    existing_deployment,
                    existing_version,
                    existing_rule,
                    existing_rule_version,
                    counts,
                )
        raise HTTPException(
            status_code=409, detail="Could not create deployment run"
        ) from error
    await session.refresh(run)
    return _run_response(
        run,
        deployment,
        deployment_version,
        rule,
        rule_version,
    )


def _run_query():
    return (
        select(
            DeploymentRun,
            Deployment,
            DeploymentVersion,
            DeploymentTargetRule,
            DeploymentTargetRuleVersion,
        )
        .join(Deployment, DeploymentRun.deployment_id == Deployment.id)
        .join(
            DeploymentVersion,
            DeploymentRun.deployment_version_id == DeploymentVersion.id,
        )
        .join(
            DeploymentTargetRuleVersion,
            DeploymentRun.rule_version_id == DeploymentTargetRuleVersion.id,
        )
        .join(
            DeploymentTargetRule,
            DeploymentTargetRuleVersion.rule_id == DeploymentTargetRule.id,
        )
    )


@deployment_management.get(
    "/executions", response_model=DeploymentRunListResponse
)
async def list_deployment_runs(
    limit: int = 50,
    offset: int = 0,
    device_read: ABACDeviceListFilterResult = Depends(ABACDeviceListFilter(Device.READ)),
    endpoint_read: ABACDeviceListFilterResult = Depends(
        ABACDeviceListFilter(Device.ENDPOINT_READ)
    ),
    session: AsyncSession = Depends(get_db),
    _=_config_read,
):
    limit = min(max(limit, 1), 200)
    offset = max(offset, 0)
    visible_ids = await _visible_device_ids(session, device_read, endpoint_read)
    if not visible_ids:
        return DeploymentRunListResponse(items=[], total=0, limit=limit, offset=offset)

    visible_run_ids = (
        await session.scalars(
            select(DeploymentRunDevice.run_id)
            .where(DeploymentRunDevice.device_id.in_(visible_ids))
            .distinct()
        )
    ).all()
    if not visible_run_ids:
        return DeploymentRunListResponse(items=[], total=0, limit=limit, offset=offset)

    total = len(visible_run_ids)
    run_rows = (
        await session.execute(
            _run_query()
            .where(DeploymentRun.id.in_(visible_run_ids))
            .order_by(DeploymentRun.triggered_at.desc())
            .limit(limit)
            .offset(offset)
        )
    ).all()
    selected_run_ids = [row[0].id for row in run_rows]
    device_rows = (
        await session.execute(
            select(DeploymentRunDevice.run_id, DeploymentRunDevice.state)
            .where(
                DeploymentRunDevice.run_id.in_(selected_run_ids),
                DeploymentRunDevice.device_id.in_(visible_ids),
            )
        )
    ).all() if selected_run_ids else []
    counts_by_run: dict[Any, dict[str, int]] = {}
    for run_id, state in device_rows:
        counts = counts_by_run.setdefault(
            run_id,
            {"total_devices": 0, "succeeded": 0, "failed": 0, "pending": 0, "queued": 0},
        )
        counts["total_devices"] += 1
        normalized_state = state.upper()
        if normalized_state in {"APPLIED", "SUCCESS", "SUCCEEDED", "COMPLETED"}:
            counts["succeeded"] += 1
        elif normalized_state in {"FAILED", "ERROR", "SUPERSEDED"}:
            counts["failed"] += 1
        elif normalized_state == "QUEUED":
            counts["queued"] += 1
        else:
            counts["pending"] += 1

    return DeploymentRunListResponse(
        items=[
            _run_response(*row, counts=counts_by_run.get(row[0].id))
            for row in run_rows
            if row[0].id in counts_by_run
        ],
        total=total,
        limit=limit,
        offset=offset,
    )


@deployment_management.post(
    "/executions/{run_id}/devices/{device_id}/retry",
    response_model=MessageResponse,
    status_code=202,
)
async def retry_deployment_dispatch(
    run_id: str,
    device_id: str,
    session: AsyncSession = Depends(get_db),
    _=_config_write,
):
    try:
        run_uuid = UUID(run_id)
    except ValueError as error:
        raise HTTPException(status_code=404, detail="Deployment run not found") from error

    row = (
        await session.execute(
            select(
                DeploymentDispatchJob,
                DeploymentExecution,
                DeviceDeployment.assignment_generation,
            )
            .join(
                DeploymentExecution,
                DeploymentExecution.id == DeploymentDispatchJob.execution_id,
            )
            .join(
                DeviceDeployment,
                DeviceDeployment.id == DeploymentDispatchJob.device_deployment_id,
            )
            .where(
                DeploymentExecution.run_id == run_uuid,
                DeploymentDispatchJob.device_id == device_id,
            )
            .with_for_update(of=(DeploymentDispatchJob, DeviceDeployment))
        )
    ).first()
    if row is None:
        raise HTTPException(status_code=404, detail="Failed dispatch job not found")
    job, execution, current_generation = row
    if job.state != "FAILED":
        raise HTTPException(
            status_code=409, detail="Only failed deployment dispatches can be retried"
        )
    if current_generation != job.expected_generation:
        job.state = "SUPERSEDED"
        job.last_error = "The device assignment has a newer generation"
        execution.state = "SUPERSEDED"
        run_device = await session.get(
            DeploymentRunDevice, (execution.run_id, device_id)
        )
        if run_device is not None:
            run_device.state = "SUPERSEDED"
        await _refresh_run_counts(session, execution.run_id)
        await session.commit()
        raise HTTPException(
            status_code=409,
            detail="This dispatch was superseded by a newer device assignment",
        )

    job.state = "QUEUED"
    job.attempts = 0
    job.not_before = datetime.now(timezone.utc)
    job.last_error = None
    job.last_error_code = None
    job.lease_token = None
    job.lease_operation = None
    job.lease_expires_at = None
    execution.state = "QUEUED"
    run_device = await session.get(DeploymentRunDevice, (execution.run_id, device_id))
    if run_device is not None:
        run_device.state = "QUEUED"
    await _refresh_run_counts(session, execution.run_id)
    await session.commit()
    return MessageResponse(message="Deployment dispatch queued for retry")


@deployment_management.get(
    "/executions/{run_id}", response_model=DeploymentRunDetailResponse
)
async def get_deployment_run(
    run_id: str,
    device_read: ABACDeviceListFilterResult = Depends(ABACDeviceListFilter(Device.READ)),
    endpoint_read: ABACDeviceListFilterResult = Depends(
        ABACDeviceListFilter(Device.ENDPOINT_READ)
    ),
    session: AsyncSession = Depends(get_db),
    _=_config_read,
):
    try:
        run_uuid = UUID(run_id)
    except ValueError as error:
        raise HTTPException(status_code=404, detail="Deployment run not found") from error

    row = (
        await session.execute(_run_query().where(DeploymentRun.id == run_uuid))
    ).first()
    if row is None:
        raise HTTPException(status_code=404, detail="Deployment run not found")
    run, deployment, deployment_version, rule, rule_version = row
    visible_ids = set(await _visible_device_ids(session, device_read, endpoint_read))
    run_devices = (
        await session.execute(
            select(
                DeploymentRunDevice,
                DeviceModel.device_data,
                DeviceType.label,
            )
            .join(DeviceModel, DeploymentRunDevice.device_id == DeviceModel.device_id)
            .join(DeviceType, DeviceModel.type_id == DeviceType.type_id)
            .where(
                DeploymentRunDevice.run_id == run_uuid,
                DeploymentRunDevice.device_id.in_(visible_ids),
            )
            .order_by(DeploymentRunDevice.device_id)
        )
    ).all() if visible_ids else []
    if not run_devices:
        raise HTTPException(status_code=404, detail="Deployment run not found")

    # Execution rows can have different assignment IDs over successive runs;
    # narrow the join to each target device to pair the correct generation.
    execution_generations = {
        device_id: generation
        for device_id, generation in (
            await session.execute(
                select(DeviceDeployment.device_id, DeploymentExecution.generation)
                .join(
                    DeploymentExecution,
                    DeploymentExecution.device_deployment_id == DeviceDeployment.id,
                )
                .where(
                    DeploymentExecution.run_id == run_uuid,
                    DeviceDeployment.device_id.in_(visible_ids),
                )
            )
        ).all()
    } if visible_ids else {}

    devices = [
        DeploymentRunDeviceResponse(
            device_id=run_device.device_id,
            device_type=device_type,
            metadata=dict(metadata or {}),
            state=run_device.state,
            generation=execution_generations.get(run_device.device_id, 1),
        )
        for run_device, metadata, device_type in run_devices
    ]
    counts = {
        "total_devices": len(devices),
        "succeeded": sum(
            1
            for device in devices
            if device.state.upper() in {"APPLIED", "SUCCESS", "SUCCEEDED", "COMPLETED"}
        ),
        "failed": sum(
            1
            for device in devices
            if device.state.upper() in {"FAILED", "ERROR", "SUPERSEDED"}
        ),
        "queued": sum(1 for device in devices if device.state.upper() == "QUEUED"),
        "pending": sum(
            1
            for device in devices
            if device.state.upper()
            not in {
                "APPLIED",
                "SUCCESS",
                "SUCCEEDED",
                "COMPLETED",
                "FAILED",
                "ERROR",
                "SUPERSEDED",
                "QUEUED",
            }
        ),
    }
    return DeploymentRunDetailResponse(
        **_run_response(
            run, deployment, deployment_version, rule, rule_version, counts
        ).model_dump(),
        devices=devices,
    )


@deployment_management.get(
    "/history", response_model=DeploymentHistoryResponse
)
async def list_deployment_history(
    limit: int = 50,
    offset: int = 0,
    status: str | None = None,
    deployment_id: int | None = None,
    search: str | None = None,
    device_read: ABACDeviceListFilterResult = Depends(ABACDeviceListFilter(Device.READ)),
    endpoint_read: ABACDeviceListFilterResult = Depends(
        ABACDeviceListFilter(Device.ENDPOINT_READ)
    ),
    session: AsyncSession = Depends(get_db),
    _=_config_read,
):
    limit = min(max(limit, 1), 200)
    offset = max(offset, 0)
    visible_ids = await _visible_device_ids(session, device_read, endpoint_read)
    if not visible_ids:
        return DeploymentHistoryResponse(items=[], total=0, limit=limit, offset=offset)

    query = (
        select(
            DeploymentExecution,
            DeploymentRun,
            DeviceModel,
            DeviceType.label,
            Deployment,
            DeploymentVersion,
        )
        .join(DeploymentRun, DeploymentExecution.run_id == DeploymentRun.id)
        .join(
            DeviceDeployment,
            DeploymentExecution.device_deployment_id == DeviceDeployment.id,
        )
        .join(DeviceModel, DeviceDeployment.device_id == DeviceModel.device_id)
        .join(DeviceType, DeviceModel.type_id == DeviceType.type_id)
        .join(Deployment, DeploymentRun.deployment_id == Deployment.id)
        .join(
            DeploymentVersion,
            DeploymentExecution.deployment_version_id == DeploymentVersion.id,
        )
        .where(DeviceModel.device_id.in_(visible_ids))
    )
    if status:
        query = query.where(func.upper(DeploymentExecution.state) == status.upper())
    if deployment_id is not None:
        query = query.where(DeploymentRun.deployment_id == deployment_id)
    if search:
        pattern = f"%{search.strip()}%"
        query = query.where(
            DeviceModel.device_id.ilike(pattern)
            | Deployment.name.ilike(pattern)
            | func.cast(DeploymentRun.id, Text).ilike(pattern)
        )

    total = await session.scalar(
        select(func.count()).select_from(query.subquery())
    ) or 0
    records = (
        await session.execute(
            query.order_by(DeploymentExecution.triggered_at.desc())
            .limit(limit)
            .offset(offset)
        )
    ).all()
    return DeploymentHistoryResponse(
        items=[
            DeploymentHistoryItem(
                id=execution.id,
                run_id=run.id,
                device_id=device.device_id,
                device_type=device_type,
                metadata=dict(device.device_data or {}),
                deployment_id=deployment.id,
                deployment_name=deployment.name,
                deployment_type=deployment.deployment_type,
                deployment_version=deployment_version.version,
                generation=execution.generation,
                state=execution.state,
                triggered_at=execution.triggered_at,
                trigger_source=execution.trigger_source,
            )
            for execution, run, device, device_type, deployment, deployment_version in records
        ],
        total=total,
        limit=limit,
        offset=offset,
    )
