"""IoT Edge desired-state serialization and IoT Hub twin operations."""

import json
import logging
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

from async_requests import client
from constants import IOT_HUB_NAME
from exceptions import APIError
from helper import encode_iothub_id, get_iothub_auth_headers

_API_VERSION = "2021-04-12"
_TWIN_TIMEOUT_SECONDS = 30
_log = logging.getLogger("EdgeConfigAPI")


class IoTHubDeploymentError(Exception):
    def __init__(
        self,
        status_code: int,
        retry_after: float | None = None,
        iot_hub_error_code: int | None = None,
    ):
        self.status_code = status_code
        self.retry_after = retry_after
        self.iot_hub_error_code = iot_hub_error_code
        detail = (
            f", IoT Hub error code {iot_hub_error_code}"
            if iot_hub_error_code is not None
            else ""
        )
        super().__init__(f"IoT Hub request failed (HTTP {status_code}{detail})")


def _env_properties(values: dict[str, str]) -> dict[str, dict[str, str]]:
    return {name: {"value": value} for name, value in values.items()}


def _module_settings(image: str, create_options: dict[str, Any]) -> dict[str, str]:
    return {
        "image": image,
        "createOptions": json.dumps(
            create_options, separators=(",", ":"), sort_keys=True
        ),
    }


def build_module_twin_desired(definition: dict[str, Any]) -> tuple[dict, dict]:
    """Build the edgeAgent and edgeHub desired properties from a saved IoT Edge definition."""
    runtime = definition["runtime"]
    edge_agent = runtime["edge_agent"]
    edge_hub = runtime["edge_hub"]

    system_modules: dict[str, dict[str, Any]] = {
        "edgeAgent": {
            "type": "docker",
            "settings": _module_settings(
                edge_agent["image"], edge_agent.get("create_options", {})
            ),
            "env": _env_properties(edge_agent.get("env", {})),
        },
        "edgeHub": {
            "type": "docker",
            "status": "running",
            "restartPolicy": "always",
            "settings": _module_settings(
                edge_hub["image"], edge_hub.get("create_options", {})
            ),
            "env": _env_properties(edge_hub.get("env", {})),
            "startupOrder": 0,
        },
    }

    modules: dict[str, dict[str, Any]] = {}
    for module in definition.get("modules", []):
        desired_module: dict[str, Any] = {
            "version": "1.1",
            "type": "docker",
            "status": module["status"],
            "restartPolicy": module["restart_policy"],
            "settings": _module_settings(
                module["image"], module.get("create_options", {})
            ),
            "env": _env_properties(module.get("env", {})),
        }
        if module.get("startup_order") is not None:
            desired_module["startupOrder"] = module["startup_order"]
        if module.get("image_pull_policy"):
            desired_module["settings"]["imagePullPolicy"] = module["image_pull_policy"]
        modules[module["name"]] = desired_module

    edge_agent_desired = {
        "schemaVersion": "1.1",
        "runtime": {
            "type": "docker",
            "settings": {
                "minDockerVersion": "v1.25",
                "loggingOptions": "",
            },
        },
        "systemModules": system_modules,
        "modules": modules,
    }
    edge_hub_desired = {
        "schemaVersion": "1.1",
        "storeAndForwardConfiguration": {
            "timeToLiveSecs": edge_hub.get("store_and_forward_ttl_secs", 7200)
        },
        "routes": runtime.get("routes", {}),
    }
    return edge_agent_desired, edge_hub_desired


def _twin_url(device_id: str, module_id: str) -> str:
    if not IOT_HUB_NAME:
        raise RuntimeError("IOT_HUB_NAME is not configured")
    return (
        f"https://{IOT_HUB_NAME}/twins/{encode_iothub_id(device_id)}"
        f"/modules/{encode_iothub_id(module_id)}?api-version={_API_VERSION}"
    )


def _retry_after_seconds(response: httpx.Response) -> float | None:
    value = response.headers.get("Retry-After")
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
        except (TypeError, ValueError, OverflowError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        return max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds())


def _check_response(
    response: httpx.Response,
    device_id: str,
    resource: str,
    action: str,
) -> None:
    if response.status_code >= 400:
        retry_after = _retry_after_seconds(response)
        iot_hub_error_code = None
        try:
            body = response.json()
            for _ in range(2):
                if not isinstance(body, dict):
                    break
                code = body.get("errorCode")
                if isinstance(code, int):
                    iot_hub_error_code = code
                    break
                message = body.get("Message") or body.get("message")
                if not isinstance(message, str):
                    break
                try:
                    body = json.loads(message)
                except ValueError:
                    break
        except ValueError:
            pass
        _log.error(
            "IoT Hub %s %s failed device=%s status=%d hub_error_code=%s retry_after_seconds=%s",
            resource,
            action,
            device_id,
            response.status_code,
            iot_hub_error_code,
            retry_after,
        )
        raise IoTHubDeploymentError(
            response.status_code, retry_after, iot_hub_error_code
        )


def _response_json(
    response: httpx.Response,
    device_id: str,
    module_id: str,
    action: str,
) -> dict[str, Any]:
    try:
        body = response.json()
    except ValueError as error:
        _log.error(
            "IoT Hub module twin %s returned invalid JSON device=%s module=%s status=%d",
            action,
            device_id,
            module_id,
            response.status_code,
        )
        raise IoTHubDeploymentError(
            502, _retry_after_seconds(response)
        ) from error
    if not isinstance(body, dict):
        _log.error(
            "IoT Hub module twin %s returned an unexpected response device=%s module=%s status=%d",
            action,
            device_id,
            module_id,
            response.status_code,
        )
        raise IoTHubDeploymentError(502)
    return body


async def _get_twin(device_id: str, module_id: str) -> dict[str, Any]:
    response = await client.get(
        _twin_url(device_id, module_id),
        headers=get_iothub_auth_headers(),
        timeout=_TWIN_TIMEOUT_SECONDS,
    )
    _check_response(response, device_id, "module twin", "read")
    twin = _response_json(response, device_id, module_id, "read")
    if not twin.get("etag") and response.headers.get("ETag"):
        twin["etag"] = response.headers["ETag"]
    return twin


async def send_deployment(
    device_id: str, definition: dict[str, Any]
) -> int:
    try:
        edge_agent_desired, edge_hub_desired = build_module_twin_desired(definition)
    except (KeyError, TypeError, ValueError) as error:
        raise APIError(
            "Saved IoT Edge definition is invalid and cannot be dispatched",
            status_code=422,
        ) from error

    modules_content = {
        "$edgeAgent": {"properties.desired": edge_agent_desired},
        "$edgeHub": {"properties.desired": edge_hub_desired},
    }
    for module in definition.get("modules", []):
        desired_properties = module.get("desired_properties", {})
        if desired_properties:
            modules_content[module["name"]] = {
                "properties.desired": desired_properties
            }
    configuration_content = {"modulesContent": modules_content}
    url = (
        f"https://{IOT_HUB_NAME}/devices/{encode_iothub_id(device_id)}"
        f"/applyConfigurationContent?api-version={_API_VERSION}"
    )
    response = await client.post(
        url,
        json=configuration_content,
        headers={
            **get_iothub_auth_headers(),
            "Content-Type": "application/json",
        },
        timeout=_TWIN_TIMEOUT_SECONDS,
    )
    _check_response(response, device_id, "configuration content", "apply")
    _log.info(
        "IoT Hub accepted configuration content device=%s status=%d",
        device_id,
        response.status_code,
    )

    edge_agent_twin = await _get_twin(device_id, "$edgeAgent")
    properties = edge_agent_twin.get("properties")
    desired = properties.get("desired") if isinstance(properties, dict) else None
    desired_version = desired.get("$version") if isinstance(desired, dict) else None
    if not isinstance(desired_version, int):
        _log.error(
            "IoT Hub edgeAgent twin response has no desired version device=%s",
            device_id,
        )
        raise IoTHubDeploymentError(502)
    return desired_version


async def get_edge_agent_reported_status(device_id: str) -> dict[str, Any]:
    twin = await _get_twin(device_id, "$edgeAgent")
    properties = twin.get("properties", {}) or {}
    if not isinstance(properties, dict):
        return {"desired_version": None, "last_desired_status": None}
    desired = properties.get("desired", {}) or {}
    reported = properties.get("reported", {}) or {}
    if not isinstance(desired, dict) or not isinstance(reported, dict):
        return {
            "desired_version": None,
            "last_desired_version": None,
            "last_desired_status": None,
            "desired_system_modules": {},
            "desired_modules": {},
            "reported_system_modules": {},
            "reported_modules": {},
        }
    status = reported.get("lastDesiredStatus")
    desired_system_modules = desired.get("systemModules")
    desired_modules = desired.get("modules")
    reported_system_modules = reported.get("systemModules")
    reported_modules = reported.get("modules")
    return {
        "desired_version": desired.get("$version"),
        "last_desired_version": reported.get("lastDesiredVersion"),
        "last_desired_status": status if isinstance(status, dict) else None,
        "desired_system_modules": (
            desired_system_modules if isinstance(desired_system_modules, dict) else {}
        ),
        "desired_modules": desired_modules if isinstance(desired_modules, dict) else {},
        "reported_system_modules": (
            reported_system_modules if isinstance(reported_system_modules, dict) else {}
        ),
        "reported_modules": reported_modules if isinstance(reported_modules, dict) else {},
    }
