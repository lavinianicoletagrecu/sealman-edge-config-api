from fastapi import Depends, Request
from fastapi import HTTPException
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from routers.base_api_router import BaseAPIRouter
from authorization.abac_permission_check import (
    ABACDeviceListFilter,
    ABACDeviceListFilterResult,
    ABACPermissionCheck,
)
from authorization.permission_types import Device
from db.models.device import Device as DeviceModel
from db.models.device_type import DeviceType
from db.repos.device import DeviceRepository
from db.repos.device_template import DeviceTemplateRepository
from db.session import get_db, get_repository
from .routes.get_devices import get_devices, populate_cache_from_iot_hub_query
from .routes.get_devices_with_data import get_devices_with_data
from .routes.create_device import create_device
from .routes.delete_device import delete_device
from .routes.get_device_meta_values import get_device_meta_values
from authorization.abac_permission_check import ABACDeviceCreateCheck
from .routes.get_device import get_device
from routers.general.schemas import DeviceDetailResponse
from .schemas import (
    DeviceMaintenanceModeListItem,
    DeviceMaintenanceModeResponse,
    DeviceMaintenanceModeUpdate,
)

devices = BaseAPIRouter(prefix="/devices", tags=["Devices"])


@devices.get("/maintenance-mode", response_model=list[DeviceMaintenanceModeListItem])
async def list_device_maintenance_modes(
    abac_context: ABACDeviceListFilterResult = Depends(ABACDeviceListFilter(Device.READ)),
    session: AsyncSession = Depends(get_db),
):
    rows = (
        await session.execute(
            select(DeviceModel, DeviceType.label)
            .join(DeviceType, DeviceModel.type_id == DeviceType.type_id)
            .order_by(DeviceModel.device_id)
        )
    ).all()
    return [
        DeviceMaintenanceModeListItem(
            device_id=device.device_id,
            device_type=device_type,
            metadata=dict(device.device_data or {}),
            maintenance_mode=device.maintenance_mode,
        )
        for device, device_type in rows
        if abac_context["filter_device"](dict(device.device_data or {}))
    ]


@devices.get(
    "/{device_id}/maintenance-mode", response_model=DeviceMaintenanceModeResponse
)
async def get_device_maintenance_mode(
    device_id: str,
    session: AsyncSession = Depends(get_db),
    _=Depends(ABACPermissionCheck(Device.READ, device_path="device_id")),
):
    device = await session.get(DeviceModel, device_id)
    if device is None:
        raise HTTPException(status_code=404, detail="Device not found")
    return DeviceMaintenanceModeResponse(
        device_id=device.device_id,
        maintenance_mode=device.maintenance_mode,
    )


@devices.patch(
    "/{device_id}/maintenance-mode", response_model=DeviceMaintenanceModeResponse
)
async def set_device_maintenance_mode(
    device_id: str,
    body: DeviceMaintenanceModeUpdate,
    session: AsyncSession = Depends(get_db),
    _=Depends(ABACPermissionCheck(Device.METADATA_WRITE, device_path="device_id")),
):
    result = await session.execute(
        update(DeviceModel)
        .where(DeviceModel.device_id == device_id)
        .values(maintenance_mode=body.maintenance_mode)
        .returning(DeviceModel.device_id, DeviceModel.maintenance_mode)
    )
    row = result.one_or_none()
    if row is None:
        raise HTTPException(status_code=404, detail="Device not found")
    await session.commit()
    return DeviceMaintenanceModeResponse(
        device_id=row.device_id,
        maintenance_mode=row.maintenance_mode,
    )


@devices.get("")
async def get_devices_route(
    request: Request,
    abac_context: ABACDeviceListFilterResult = Depends(ABACDeviceListFilter(Device.READ)),
    repo: DeviceRepository = Depends(get_repository(DeviceRepository)),
):
    return await get_devices(
        repo=repo,
        filter_device=abac_context["filter_device"],
        query_params=request.query_params,
    )

@devices.get("/with-data")
async def get_devices_with_data_route(
    request: Request,
    device_read: ABACDeviceListFilterResult = Depends(ABACDeviceListFilter(Device.READ)),
    endpoint_read: ABACDeviceListFilterResult = Depends(ABACDeviceListFilter(Device.ENDPOINT_READ)),
    repo: DeviceRepository = Depends(get_repository(DeviceRepository)),
):
    def visible(device_data: dict) -> bool:
        return device_read["filter_device"](device_data) and endpoint_read["filter_device"](device_data)

    return await get_devices_with_data(
        repo=repo,
        query_params=request.query_params,
        is_unrestricted=device_read["is_unrestricted"] and endpoint_read["is_unrestricted"],
        filter_device=visible,
    )

@devices.put("/{device_id}")
async def create_device_route(
    device_id: str,
    request: Request,
    repo: DeviceRepository = Depends(get_repository(DeviceRepository)),
    template_repo: DeviceTemplateRepository = Depends(get_repository(DeviceTemplateRepository)),
    _=Depends(ABACDeviceCreateCheck(Device.CREATE)),
):
    body = await request.json()
    return await create_device(device_id, body, repo, template_repo)


@devices.delete("/{device_id}")
async def delete_device_route(
    device_id: str,
    repo: DeviceRepository = Depends(get_repository(DeviceRepository)),
    _=Depends(ABACPermissionCheck(Device.DELETE, device_path="device_id")),
):
    await delete_device(device_id, repo)
    return {"status": "deleted"}


@devices.get("/meta-values")
async def get_device_meta_values_route(
    abac_context: ABACDeviceListFilterResult = Depends(ABACDeviceListFilter(Device.READ)),
    repo: DeviceRepository = Depends(get_repository(DeviceRepository)),
):
    return await get_device_meta_values(repo, abac_context["filter_device"])


@devices.get("/{device_id}", response_model=DeviceDetailResponse)
async def get_device_route(
    device_id: str,
    repo: DeviceRepository = Depends(get_repository(DeviceRepository)),
    _=Depends(ABACPermissionCheck(Device.READ, device_path="device_id")),
):
    return await get_device(device_id, repo)
