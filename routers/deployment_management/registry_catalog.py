"""Read-only Azure Container Registry repository and tag browsing."""

import logging
import re
from urllib.parse import quote

import httpx

from async_requests import client
from constants import (
    DEPLOYMENT_REGISTRY_HOST,
    DEPLOYMENT_REGISTRY_PASSWORD,
    DEPLOYMENT_REGISTRY_USERNAME,
)

_API_VERSION = "2021-07-01"
_PAGE_SIZE = 1000
_MAX_PAGES = 100
_TIMEOUT_SECONDS = 20
_log = logging.getLogger("EdgeConfigAPI")


class RegistryCatalogError(Exception):
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        super().__init__(message)


def _registry_base_url() -> str:
    host = DEPLOYMENT_REGISTRY_HOST.strip().lower()
    if not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?", host):
        raise RegistryCatalogError(503, "Deployment image registry is not configured")
    if not DEPLOYMENT_REGISTRY_USERNAME or not DEPLOYMENT_REGISTRY_PASSWORD:
        raise RegistryCatalogError(
            503, "Deployment image registry credentials are not configured"
        )
    return f"https://{host}"


def _bearer_challenge(value: str | None) -> dict[str, str] | None:
    if not value:
        return None
    scheme, _, parameters = value.partition(" ")
    if scheme.lower() != "bearer":
        return None
    return {
        key.lower(): item
        for key, item in re.findall(r'([A-Za-z]+)="([^"]*)"', parameters)
    }


class RegistryCatalogClient:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self._token: str | None = None

    async def _get(
        self,
        url: str,
        *,
        params: dict[str, str | int] | None = None,
        headers: dict[str, str] | None = None,
        auth: tuple[str, str] | None = None,
    ) -> httpx.Response:
        try:
            return await client.get(
                url,
                params=params,
                headers=headers,
                auth=auth,
                timeout=_TIMEOUT_SECONDS,
            )
        except httpx.RequestError as error:
            _log.warning("Registry request failed (%s)", type(error).__name__)
            raise RegistryCatalogError(502, "Registry is temporarily unavailable") from error

    async def get_json(self, path: str, params: dict[str, str | int]) -> dict:
        url = f"{self.base_url}{path}"
        headers = (
            {"Authorization": f"Bearer {self._token}"} if self._token else {}
        )
        response = await self._get(url, params=params, headers=headers)
        if response.status_code == 401:
            challenge = _bearer_challenge(response.headers.get("WWW-Authenticate"))
            if challenge is None:
                _log.warning("Registry request was unauthorized without a bearer challenge")
                raise RegistryCatalogError(502, "Registry authentication was rejected")
            realm = challenge.get("realm", "")
            service = challenge.get("service", "")
            scope = challenge.get("scope", "")
            if not realm.startswith(self.base_url + "/") or not service:
                _log.error("Registry returned an unexpected authentication endpoint")
                raise RegistryCatalogError(502, "Registry authentication is unavailable")
            token_response = await self._get(
                realm,
                params={"service": service, "scope": scope},
                auth=(DEPLOYMENT_REGISTRY_USERNAME, DEPLOYMENT_REGISTRY_PASSWORD),
            )
            if token_response.status_code in {401, 403}:
                _log.warning(
                    "Registry credential exchange was rejected (HTTP %d)",
                    token_response.status_code,
                )
                raise RegistryCatalogError(
                    502, "Registry credentials were rejected or lack catalog access"
                )
            if token_response.is_error:
                _log.error(
                    "Registry credential exchange failed (HTTP %d)",
                    token_response.status_code,
                )
                raise RegistryCatalogError(502, "Registry authentication failed")
            try:
                token_body = token_response.json()
            except ValueError as error:
                raise RegistryCatalogError(
                    502, "Registry returned an invalid token response"
                ) from error
            token = token_body.get("access_token") or token_body.get("token")
            if not isinstance(token, str) or not token:
                raise RegistryCatalogError(502, "Registry returned no access token")
            self._token = token
            response = await self._get(
                url,
                params=params,
                headers={"Authorization": f"Bearer {self._token}"},
            )
        if response.status_code in {401, 403}:
            _log.warning("Registry catalog access was denied (HTTP %d)", response.status_code)
            raise RegistryCatalogError(
                502, "Registry credentials do not allow browsing this catalog"
            )
        if response.is_error:
            _log.error("Registry catalog request failed (HTTP %d)", response.status_code)
            raise RegistryCatalogError(502, "Registry catalog request failed")
        try:
            body = response.json()
        except ValueError as error:
            raise RegistryCatalogError(
                502, "Registry returned an invalid catalog response"
            ) from error
        if not isinstance(body, dict):
            raise RegistryCatalogError(502, "Registry returned an invalid catalog response")
        return body


async def _list_values(path: str, key: str) -> list[str]:
    base_url = _registry_base_url()
    registry = RegistryCatalogClient(base_url)
    values: list[str] = []
    last = ""
    for _ in range(_MAX_PAGES):
        params: dict[str, str | int] = {"api-version": _API_VERSION, "n": _PAGE_SIZE}
        if last:
            params["last"] = last
        body = await registry.get_json(path, params)
        page = body.get(key)
        if not isinstance(page, list) or any(
            not isinstance(item, (str, dict)) for item in page
        ):
            raise RegistryCatalogError(502, "Registry returned an invalid catalog response")
        page_values = [
            item if isinstance(item, str) else item.get("name")
            for item in page
        ]
        if any(not isinstance(item, str) or not item for item in page_values):
            raise RegistryCatalogError(502, "Registry returned an invalid catalog response")
        values.extend(page_values)
        if len(page) < _PAGE_SIZE:
            return sorted(set(values), key=str.casefold)
        next_last = page_values[-1]
        if next_last == last:
            break
        last = next_last
    _log.error("Registry catalog exceeded the maximum number of pages")
    raise RegistryCatalogError(502, "Registry catalog is too large to browse")


async def list_repositories() -> dict[str, object]:
    repositories = await _list_values("/acr/v1/_catalog", "repositories")
    return {
        "registry": DEPLOYMENT_REGISTRY_HOST.strip().lower(),
        "repositories": repositories,
    }


async def list_repository_tags(repository: str) -> dict[str, object]:
    segments = repository.split("/")
    if (
        not repository
        or any(
            not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]*", segment)
            or segment in {".", ".."}
            for segment in segments
        )
    ):
        raise RegistryCatalogError(422, "Invalid registry repository name")
    path = f"/acr/v1/{quote(repository, safe='/')}/_tags"
    tags = await _list_values(path, "tags")
    return {"repository": repository, "tags": tags}
