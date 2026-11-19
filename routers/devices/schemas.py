from pydantic import BaseModel
from typing import Any


class DeviceMaintenanceModeUpdate(BaseModel):
    maintenance_mode: bool


class DeviceMaintenanceModeResponse(BaseModel):
    device_id: str
    maintenance_mode: bool


class DeviceMaintenanceModeListItem(DeviceMaintenanceModeResponse):
    device_type: str
    metadata: dict[str, Any]
