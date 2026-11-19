import httpx
import pytest
from unittest.mock import AsyncMock

import routers.deployment_management.registry_catalog as registry_catalog


def _response(status: int, *, json: dict | None = None, headers: dict | None = None):
    return httpx.Response(
        status,
        json=json,
        headers=headers,
        request=httpx.Request("GET", "https://registry.test"),
    )


@pytest.fixture(autouse=True)
def registry_config(monkeypatch):
    monkeypatch.setattr(registry_catalog, "DEPLOYMENT_REGISTRY_HOST", "registry.test")
    monkeypatch.setattr(registry_catalog, "DEPLOYMENT_REGISTRY_USERNAME", "registry-user")
    monkeypatch.setattr(registry_catalog, "DEPLOYMENT_REGISTRY_PASSWORD", "test-secret")


async def test_list_repositories_authenticates_and_fetches_all_pages(monkeypatch):
    monkeypatch.setattr(registry_catalog, "_PAGE_SIZE", 2)
    challenge = (
        'Bearer realm="https://registry.test/oauth2/token",'
        'service="registry.test",scope="registry:catalog:*"'
    )
    get = AsyncMock(
        side_effect=[
            _response(401, headers={"WWW-Authenticate": challenge}),
            _response(200, json={"access_token": "temporary-token"}),
            _response(200, json={"repositories": ["vendor/zeta", "vendor/alpha"]}),
            _response(200, json={"repositories": ["vendor/beta"]}),
        ]
    )
    monkeypatch.setattr(registry_catalog.client, "get", get)

    result = await registry_catalog.list_repositories()

    assert result == {
        "registry": "registry.test",
        "repositories": ["vendor/alpha", "vendor/beta", "vendor/zeta"],
    }
    assert get.await_args_list[1].kwargs["auth"] == (
        "registry-user",
        "test-secret",
    )
    assert get.await_args_list[2].kwargs["headers"] == {
        "Authorization": "Bearer temporary-token"
    }
    assert get.await_args_list[3].kwargs["params"]["last"] == "vendor/alpha"


async def test_list_repository_tags_quotes_path_and_extracts_tag_names(monkeypatch):
    challenge = (
        'Bearer realm="https://registry.test/oauth2/token",'
        'service="registry.test",scope="repository:team/sensor:metadata_read"'
    )
    get = AsyncMock(
        side_effect=[
            _response(401, headers={"WWW-Authenticate": challenge}),
            _response(200, json={"token": "temporary-token"}),
            _response(
                200,
                json={"tags": [{"name": "2.0"}, {"name": "1.0"}]},
            ),
        ]
    )
    monkeypatch.setattr(registry_catalog.client, "get", get)

    result = await registry_catalog.list_repository_tags("team/sensor")

    assert result == {"repository": "team/sensor", "tags": ["1.0", "2.0"]}
    assert "/acr/v1/team/sensor/_tags" in get.await_args_list[0].args[0]


async def test_list_repository_tags_rejects_invalid_repository_names():
    with pytest.raises(registry_catalog.RegistryCatalogError) as error:
        await registry_catalog.list_repository_tags("../other")

    assert error.value.status_code == 422


async def test_registry_authentication_failure_does_not_expose_credentials(monkeypatch):
    challenge = (
        'Bearer realm="https://registry.test/oauth2/token",'
        'service="registry.test",scope="registry:catalog:*"'
    )
    get = AsyncMock(
        side_effect=[
            _response(401, headers={"WWW-Authenticate": challenge}),
            _response(401),
        ]
    )
    monkeypatch.setattr(registry_catalog.client, "get", get)

    with pytest.raises(registry_catalog.RegistryCatalogError) as error:
        await registry_catalog.list_repositories()

    assert error.value.status_code == 502
    assert "test-secret" not in str(error.value)
