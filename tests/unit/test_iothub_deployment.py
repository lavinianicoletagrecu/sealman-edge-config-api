import json

import httpx
from unittest.mock import AsyncMock
from urllib.parse import urlparse

from exceptions import APIError
import routers.deployment_management.iothub_deployment as iothub_deployment
from routers.deployment_management.iothub_deployment import (
    _retry_after_seconds,
    build_module_twin_desired,
    get_edge_agent_reported_status,
    send_deployment,
)


def test_build_module_twin_desired_converts_saved_iot_edge_definition():
    edge_agent, edge_hub = build_module_twin_desired(
        {
            "runtime": {
                "edge_agent": {
                    "image": "mcr.microsoft.com/azureiotedge-agent:1.5",
                    "env": {"LOG_LEVEL": "info"},
                    "create_options": {"HostConfig": {"NetworkMode": "host"}},
                },
                "edge_hub": {
                    "image": "mcr.microsoft.com/azureiotedge-hub:1.5",
                    "store_and_forward_ttl_secs": 3600,
                },
                "routes": {"upstream": "FROM /messages/* INTO $upstream"},
            },
            "modules": [
                {
                    "name": "sample",
                    "image": "example/sample:1.0",
                    "env": {"SETTING": "value"},
                    "create_options": {"HostConfig": {"Privileged": True}},
                    "desired_properties": {"setting": "value"},
                    "restart_policy": "always",
                    "status": "running",
                    "image_pull_policy": "on-create",
                    "startup_order": 10,
                }
            ],
        }
    )

    assert edge_agent["schemaVersion"] == "1.1"
    assert edge_agent["runtime"]["settings"] == {
        "minDockerVersion": "v1.25",
        "loggingOptions": "",
    }
    assert edge_agent["systemModules"]["edgeAgent"]["env"] == {
        "LOG_LEVEL": {"value": "info"}
    }
    assert edge_agent["systemModules"]["edgeAgent"]["settings"]["createOptions"] == (
        '{"HostConfig":{"NetworkMode":"host"}}'
    )
    assert "properties.desired" not in edge_agent["modules"]["sample"]
    assert edge_agent["modules"]["sample"]["version"] == "1.1"
    assert edge_agent["modules"]["sample"]["startupOrder"] == 10
    assert edge_agent["modules"]["sample"]["imagePullPolicy"] == "on-create"
    assert "imagePullPolicy" not in edge_agent["modules"]["sample"]["settings"]
    assert edge_hub["storeAndForwardConfiguration"]["timeToLiveSecs"] == 3600
    assert edge_hub["routes"]["upstream"] == "FROM /messages/* INTO $upstream"


def test_private_registry_credentials_are_added_only_to_edge_agent_runtime(monkeypatch):
    monkeypatch.setattr(
        iothub_deployment, "DEPLOYMENT_REGISTRY_HOST", "geaeureka.azurecr.io"
    )
    monkeypatch.setattr(
        iothub_deployment, "DEPLOYMENT_REGISTRY_USERNAME", "registry-user"
    )
    monkeypatch.setattr(
        iothub_deployment, "DEPLOYMENT_REGISTRY_PASSWORD", "registry-password"
    )

    edge_agent, _ = build_module_twin_desired(
        {
            "runtime": {
                "edge_agent": {"image": "mcr.microsoft.com/agent:1.5"},
                "edge_hub": {"image": "mcr.microsoft.com/hub:1.5"},
            },
            "modules": [
                {
                    "name": "private",
                    "image": "geaeureka.azurecr.io/conncheck:2.0.3",
                    "status": "running",
                    "restart_policy": "always",
                }
            ],
        }
    )

    assert edge_agent["runtime"]["settings"]["registryCredentials"] == {
        "deployment-registry": {
            "address": "geaeureka.azurecr.io",
            "username": "registry-user",
            "password": "registry-password",
        }
    }


def test_private_registry_credentials_are_not_added_for_public_images(monkeypatch):
    monkeypatch.setattr(
        iothub_deployment, "DEPLOYMENT_REGISTRY_HOST", "geaeureka.azurecr.io"
    )
    monkeypatch.setattr(
        iothub_deployment, "DEPLOYMENT_REGISTRY_USERNAME", "registry-user"
    )
    monkeypatch.setattr(
        iothub_deployment, "DEPLOYMENT_REGISTRY_PASSWORD", "registry-password"
    )

    edge_agent, _ = build_module_twin_desired(
        {
            "runtime": {
                "edge_agent": {"image": "mcr.microsoft.com/agent:1.5"},
                "edge_hub": {"image": "mcr.microsoft.com/hub:1.5"},
            },
            "modules": [],
        }
    )

    assert "registryCredentials" not in edge_agent["runtime"]["settings"]


def test_private_registry_image_requires_configured_credentials(monkeypatch):
    monkeypatch.setattr(
        iothub_deployment, "DEPLOYMENT_REGISTRY_HOST", "geaeureka.azurecr.io"
    )
    monkeypatch.setattr(iothub_deployment, "DEPLOYMENT_REGISTRY_USERNAME", "")
    monkeypatch.setattr(iothub_deployment, "DEPLOYMENT_REGISTRY_PASSWORD", "")

    try:
        build_module_twin_desired(
            {
                "runtime": {
                    "edge_agent": {"image": "mcr.microsoft.com/agent:1.5"},
                    "edge_hub": {"image": "mcr.microsoft.com/hub:1.5"},
                },
                "modules": [
                    {
                        "name": "private",
                        "image": "geaeureka.azurecr.io/conncheck:2.0.3",
                        "status": "running",
                        "restart_policy": "always",
                    }
                ],
            }
        )
    except APIError as error:
        assert error.status_code == 503
    else:
        raise AssertionError("Expected missing private registry credentials to fail")


def test_create_options_are_stringified_json_and_preserve_large_environment_value():
    large_value = "x" * 2603
    options = {"Env": [f"CONFIG={large_value}"]}

    settings = iothub_deployment._module_settings("example/module:1", options)

    assert json.loads(settings["createOptions"]) == options
    assert len(settings["createOptions"].encode()) > 2600


def test_create_options_reject_non_json_numbers():
    try:
        iothub_deployment._module_settings("example/module:1", {"value": float("nan")})
    except APIError as error:
        assert error.status_code == 422
    else:
        raise AssertionError("Expected non-JSON CreateOptions values to fail")


def test_retry_after_seconds_is_preserved_without_shortening():
    response = httpx.Response(429, headers={"Retry-After": "240"})

    assert _retry_after_seconds(response) == 240


def test_retry_after_http_date_is_supported():
    response = httpx.Response(
        429,
        headers={"Retry-After": "Tue, 17 Nov 2026 10:04:45 GMT"},
    )

    assert _retry_after_seconds(response) is not None


async def test_send_deployment_applies_configuration_content_then_reads_version(monkeypatch):
    monkeypatch.setattr(iothub_deployment, "IOT_HUB_NAME", "hub.test")
    monkeypatch.setattr(
        iothub_deployment, "get_iothub_auth_headers", lambda: {"Authorization": "test"}
    )
    post = AsyncMock(
        return_value=httpx.Response(
            204, request=httpx.Request("POST", "https://hub.test")
        )
    )
    get = AsyncMock(
        return_value=httpx.Response(
            200,
            json={
                "properties": {
                    "desired": {"$version": 9},
                    "reported": {"lastDesiredStatus": {"code": 200}},
                }
            },
            request=httpx.Request("GET", "https://hub.test"),
        )
    )
    monkeypatch.setattr(iothub_deployment.client, "get", get)
    monkeypatch.setattr(iothub_deployment.client, "post", post)

    desired_version = await send_deployment("device-1", {"runtime": {
        "edge_agent": {"image": "agent:1", "env": {}, "create_options": {}},
        "edge_hub": {
            "image": "hub:1",
            "env": {},
            "create_options": {},
            "store_and_forward_ttl_secs": 7200,
        },
        "routes": {},
    }, "modules": [
        {
            "name": "sample",
            "image": "example/sample:1.0",
            "env": {},
            "create_options": {},
            "desired_properties": {"setting": "value"},
            "restart_policy": "always",
            "status": "running",
        }
    ]})

    assert desired_version == 9
    assert urlparse(post.await_args.args[0]).path == (
        "/devices/device-1/applyConfigurationContent"
    )
    assert post.await_args.kwargs["headers"]["Authorization"] == "test"
    content = post.await_args.kwargs["json"]["modulesContent"]
    assert set(content) == {"$edgeAgent", "$edgeHub", "sample"}
    edge_agent = content["$edgeAgent"]["properties.desired"]
    assert edge_agent["schemaVersion"] == "1.1"
    assert edge_agent["runtime"]["settings"]["minDockerVersion"] == "v1.25"
    assert edge_agent["systemModules"]["edgeAgent"]["settings"]["createOptions"] == "{}"
    assert "properties.desired" not in edge_agent["modules"]["sample"]
    assert content["sample"]["properties.desired"] == {"setting": "value"}
    assert content["$edgeHub"]["properties.desired"]["schemaVersion"] == "1.1"
    assert urlparse(get.await_args.args[0]).path == (
        "/twins/device-1/modules/%24edgeAgent"
    )


async def test_edge_agent_reported_status_reads_root_version_and_module_states(monkeypatch):
    monkeypatch.setattr(iothub_deployment, "IOT_HUB_NAME", "hub.test")
    monkeypatch.setattr(
        iothub_deployment, "get_iothub_auth_headers", lambda: {"Authorization": "test"}
    )
    get = AsyncMock(
        return_value=httpx.Response(
            200,
            json={
                "properties": {
                    "desired": {
                        "$version": 9,
                        "systemModules": {"edgeAgent": {"type": "docker"}},
                        "modules": {"sample": {"status": "running"}},
                    },
                    "reported": {
                        "lastDesiredVersion": 9,
                        "lastDesiredStatus": {"code": 200},
                        "systemModules": {
                            "edgeAgent": {"runtimeStatus": "running"}
                        },
                        "modules": {"sample": {"runtimeStatus": "running"}},
                    },
                }
            },
            request=httpx.Request("GET", "https://hub.test"),
        )
    )
    monkeypatch.setattr(iothub_deployment.client, "get", get)

    status = await get_edge_agent_reported_status("device-1")

    assert status["desired_version"] == 9
    assert status["last_desired_version"] == 9
    assert status["last_desired_status"] == {"code": 200}
    assert status["desired_system_modules"] == {
        "edgeAgent": {"type": "docker"}
    }
    assert status["reported_modules"] == {
        "sample": {"runtimeStatus": "running"}
    }
