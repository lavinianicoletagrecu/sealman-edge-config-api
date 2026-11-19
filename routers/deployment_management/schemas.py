from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class EdgeRuntimeComponent(BaseModel):
    image: str = Field(min_length=1, max_length=1000)
    env: dict[str, str] = Field(default_factory=dict)
    create_options: dict[str, Any] = Field(default_factory=dict)


class EdgeHubComponent(EdgeRuntimeComponent):
    store_and_forward_ttl_secs: int = Field(default=7200, ge=0)


class EdgeRuntime(BaseModel):
    edge_agent: EdgeRuntimeComponent = Field(
        default_factory=lambda: EdgeRuntimeComponent(
            image="mcr.microsoft.com/azureiotedge-agent:1.5"
        )
    )
    edge_hub: EdgeHubComponent = Field(
        default_factory=lambda: EdgeHubComponent(
            image="mcr.microsoft.com/azureiotedge-hub:1.5"
        )
    )
    routes: dict[str, str] = Field(default_factory=dict)


class EdgeModule(BaseModel):
    name: str = Field(min_length=1, max_length=128)
    image: str = Field(min_length=1, max_length=1000)
    env: dict[str, str] = Field(default_factory=dict)
    create_options: dict[str, Any] = Field(default_factory=dict)
    desired_properties: dict[str, Any] = Field(default_factory=dict)
    restart_policy: Literal["always", "on-failure", "on-unhealthy", "never"] = "always"
    status: Literal["running", "stopped"] = "running"
    image_pull_policy: Literal["on-create", "never"] = "on-create"
    startup_order: int | None = Field(default=None, ge=0)

    @field_validator("name")
    @classmethod
    def strip_name(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("Module name must not be blank")
        return value


class IoTEdgeDefinition(BaseModel):
    runtime: EdgeRuntime = Field(default_factory=EdgeRuntime)
    modules: list[EdgeModule] = Field(default_factory=list, max_length=100)


class ComposeService(BaseModel):
    name: str = Field(min_length=1, max_length=128)
    image: str = Field(min_length=1, max_length=1000)
    version: str = Field(default="", max_length=128)
    env: dict[str, str] = Field(default_factory=dict)
    ports: dict[str, str] = Field(default_factory=dict)
    volumes: dict[str, str] = Field(default_factory=dict)
    restart: str = Field(default="always", max_length=128)
    command: str = Field(default="", max_length=10000)
    networks: dict[str, str] = Field(default_factory=dict)
    depends_on: dict[str, str] = Field(default_factory=dict)

    @field_validator("name")
    @classmethod
    def strip_name(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("Service name must not be blank")
        return value


class ComposeDefinition(BaseModel):
    services: list[ComposeService] = Field(default_factory=list, max_length=100)


class DeploymentWrite(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    deployment_type: Literal["IOTEDGE", "COMPOSE"]
    description: str | None = Field(default=None, max_length=2000)

    @field_validator("name")
    @classmethod
    def strip_name(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("Name must not be blank")
        return value

    @field_validator("description")
    @classmethod
    def strip_description(cls, value: str | None) -> str | None:
        return value.strip() or None if value is not None else None


class DeploymentResponse(BaseModel):
    id: int
    name: str
    deployment_type: str
    description: str | None
    created_at: datetime
    latest_version: int | None
    version_count: int


class DeploymentVersionWrite(BaseModel):
    change_note: str = Field(min_length=1, max_length=2000)
    definition: dict[str, Any]

    @field_validator("change_note")
    @classmethod
    def strip_change_note(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("Change note must not be blank")
        return value


class DeploymentVersionResponse(BaseModel):
    id: int
    version: int
    content_hash: str
    change_note: str
    created_at: datetime
    created_by: str | None = None
    definition: dict[str, Any]


class TargetCondition(BaseModel):
    key: str = Field(min_length=1, max_length=200)
    values: list[str] = Field(default_factory=list, max_length=100)
    operator: Literal["AND", "OR"] = "AND"


class TargetRuleWrite(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    expression: list[TargetCondition] = Field(default_factory=list, max_length=50)
    excluded_device_ids: list[str] = Field(default_factory=list, max_length=10000)

    @field_validator("name")
    @classmethod
    def strip_name(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("Name must not be blank")
        return value


class TargetRuleVersionResponse(BaseModel):
    id: int
    version: int
    expression: list[TargetCondition]
    excluded_device_ids: list[str]
    created_at: datetime
    created_by: str | None


class TargetRuleResponse(BaseModel):
    id: int
    name: str
    updated_at: datetime
    version: TargetRuleVersionResponse
    matching_devices: int
    used_deployment: str | None = None
    used_version: int | None = None
    deployment_count: int = 0
    is_synced: bool | None = None


class TargetDevice(BaseModel):
    device_id: str
    device_type: str
    metadata: dict[str, Any]
    endpoints: list[dict[str, Any]]
    services: list[str]


class TargetRuleValuesResponse(BaseModel):
    values: dict[str, list[str]]
    devices: list[TargetDevice]


class DeploymentRunPreviewRequest(BaseModel):
    deployment_version_id: int = Field(gt=0)
    rule_id: int = Field(gt=0)


class DeploymentRunPreviewResponse(BaseModel):
    deployment_id: int
    deployment_name: str
    deployment_type: str
    deployment_version_id: int
    deployment_version: int
    rule_id: int
    rule_name: str
    rule_version_id: int
    rule_version: int
    matching_devices: list[TargetDevice]


class DeploymentRunWrite(BaseModel):
    deployment_version_id: int = Field(gt=0)
    rule_version_id: int = Field(gt=0)
    idempotency_key: UUID


class DeploymentRunDeviceResponse(BaseModel):
    device_id: str
    device_type: str
    metadata: dict[str, Any]
    state: str
    generation: int


class DeploymentRunResponse(BaseModel):
    id: UUID
    deployment_id: int
    deployment_name: str
    deployment_type: str
    deployment_version_id: int
    deployment_version: int
    rule_id: int
    rule_name: str
    rule_version_id: int
    rule_version: int
    triggered_at: datetime
    total_devices: int
    succeeded: int
    failed: int
    pending: int
    queued: int
    state: str


class DeploymentRunDetailResponse(DeploymentRunResponse):
    devices: list[DeploymentRunDeviceResponse]


class DeploymentRunListResponse(BaseModel):
    items: list[DeploymentRunResponse]
    total: int
    limit: int
    offset: int


class DeploymentHistoryItem(BaseModel):
    id: UUID
    run_id: UUID
    device_id: str
    device_type: str
    metadata: dict[str, Any]
    deployment_id: int
    deployment_name: str
    deployment_type: str
    deployment_version: int
    generation: int
    state: str
    triggered_at: datetime
    trigger_source: str


class DeploymentHistoryResponse(BaseModel):
    items: list[DeploymentHistoryItem]
    total: int
    limit: int
    offset: int


class MessageResponse(BaseModel):
    message: str


class RegistryRepositoriesResponse(BaseModel):
    registry: str
    repositories: list[str]


class RegistryTagsResponse(BaseModel):
    repository: str
    tags: list[str]
