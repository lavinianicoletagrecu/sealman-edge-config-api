"""Durable IoT Edge deployment dispatch worker."""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import logging
import random
from typing import Any
from uuid import UUID, uuid4

import httpx
from sqlalchemy import case, exists, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from constants import IOT_HUB_NAME
from db.models.deployment import (
    DeploymentDispatchJob,
    DeploymentExecution,
    DeploymentRun,
    DeploymentRunDevice,
    DeploymentVersion,
    DeviceDeployment,
)
from db.session import AsyncSessionLocal
from exceptions import APIError
from .iothub_deployment import (
    IoTHubDeploymentError,
    get_edge_agent_reported_status,
    send_deployment,
)

_log = logging.getLogger("EdgeConfigAPI")
_LEASE_DURATION = timedelta(seconds=180)
_MAX_BACKOFF_SECONDS = 900
_RECONCILE_INTERVAL_SECONDS = 15
_ERROR_RETRY_INTERVAL_SECONDS = 300
_missing_hub_warning_logged = False


@dataclass(frozen=True)
class ClaimedJob:
    job_id: UUID
    execution_id: UUID
    run_id: UUID
    device_id: str
    expected_generation: int
    lease_token: UUID
    operation: str
    attempts: int
    max_attempts: int
    definition: dict[str, Any] | None
    desired_twin_version: int | None


async def _refresh_run_counts(
    session: AsyncSession, run_id: UUID
) -> None:
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
    run.succeeded = sum(1 for state in states if state.upper() == "APPLIED")
    run.failed = sum(
        1 for state in states if state.upper() in {"FAILED", "SUPERSEDED"}
    )
    queued = sum(1 for state in states if state.upper() == "QUEUED")
    run.pending = len(states) - run.succeeded - run.failed - queued


async def _set_execution_state(
    session: AsyncSession,
    job: DeploymentDispatchJob,
    state: str,
) -> None:
    execution = await session.get(DeploymentExecution, job.execution_id)
    if execution is None:
        return
    execution.state = state
    run_device = await session.get(
        DeploymentRunDevice, (execution.run_id, job.device_id)
    )
    if run_device is not None:
        run_device.state = state
    await _refresh_run_counts(session, execution.run_id)


async def _assignment_is_current(
    session: AsyncSession, job: DeploymentDispatchJob
) -> bool:
    current_generation = await session.scalar(
        select(DeviceDeployment.assignment_generation)
        .where(DeviceDeployment.id == job.device_deployment_id)
        .with_for_update()
    )
    return current_generation == job.expected_generation


async def _claim_next_job() -> ClaimedJob | None:
    now = datetime.now(timezone.utc)
    other_job = aliased(DeploymentDispatchJob)
    eligible = or_(
        (
            (DeploymentDispatchJob.state == "QUEUED")
            & (DeploymentDispatchJob.not_before <= now)
        ),
        (
            (DeploymentDispatchJob.state == "SUBMITTED")
            & (DeploymentDispatchJob.not_before <= now)
        ),
        (
            (DeploymentDispatchJob.state == "LEASED")
            & (DeploymentDispatchJob.lease_expires_at <= now)
        ),
    )

    async with AsyncSessionLocal() as session:
        try:
            row = (
                await session.execute(
                    select(
                        DeploymentDispatchJob,
                        DeploymentExecution,
                        DeviceDeployment.assignment_generation,
                        DeploymentVersion.definition,
                    )
                    .join(
                        DeploymentExecution,
                        DeploymentExecution.id
                        == DeploymentDispatchJob.execution_id,
                    )
                    .join(
                        DeviceDeployment,
                        DeviceDeployment.id
                        == DeploymentDispatchJob.device_deployment_id,
                    )
                    .join(
                        DeploymentVersion,
                        DeploymentVersion.id
                        == DeploymentExecution.deployment_version_id,
                    )
                    .where(
                        eligible,
                        ~exists(
                            select(1).where(
                                other_job.device_id
                                == DeploymentDispatchJob.device_id,
                                other_job.state == "LEASED",
                                other_job.id != DeploymentDispatchJob.id,
                            )
                        ),
                    )
                    .order_by(
                        case(
                            (DeploymentDispatchJob.state == "QUEUED", 0),
                            else_=1,
                        ),
                        DeploymentDispatchJob.created_at,
                    )
                    .with_for_update(
                        of=(DeploymentDispatchJob, DeviceDeployment),
                        skip_locked=True,
                    )
                    .limit(1)
                )
            ).first()
            if row is None:
                await session.rollback()
                return None

            job, execution, current_generation, serialized_definition = row
            if current_generation != job.expected_generation:
                _log.warning(
                    "Deployment job superseded before claim job=%s run=%s device=%s expected_generation=%s current_generation=%s",
                    job.id,
                    execution.run_id,
                    job.device_id,
                    job.expected_generation,
                    current_generation,
                )
                job.state = "SUPERSEDED"
                job.lease_token = None
                job.lease_operation = None
                job.lease_expires_at = None
                await _set_execution_state(session, job, "SUPERSEDED")
                await session.commit()
                return None

            if (
                job.state == "LEASED"
                and job.lease_operation == "DISPATCH"
                and job.attempts >= job.max_attempts
            ):
                job.state = "FAILED"
                job.lease_token = None
                job.lease_operation = None
                job.lease_expires_at = None
                job.last_error = "Dispatch lease expired after the final allowed attempt"
                await _set_execution_state(session, job, "FAILED")
                await session.commit()
                _log.error(
                    "Deployment dispatch failed after lease expiry job=%s run=%s device=%s attempts=%d max_attempts=%d",
                    job.id,
                    execution.run_id,
                    job.device_id,
                    job.attempts,
                    job.max_attempts,
                )
                return None

            operation = (
                job.lease_operation
                if job.state == "LEASED"
                else "RECONCILE"
                if job.state == "SUBMITTED"
                else "DISPATCH"
            )
            definition = None
            if operation == "DISPATCH":
                try:
                    definition = json.loads(serialized_definition)
                except (json.JSONDecodeError, TypeError):
                    job.state = "FAILED"
                    job.last_error = "Saved deployment definition is invalid JSON"
                    await _set_execution_state(session, job, "FAILED")
                    await session.commit()
                    _log.exception(
                        "Deployment dispatch failed because saved definition is invalid job=%s run=%s device=%s",
                        job.id,
                        execution.run_id,
                        job.device_id,
                    )
                    return None

            token = uuid4()
            job.state = "LEASED"
            job.lease_token = token
            job.lease_operation = operation
            job.lease_expires_at = now + _LEASE_DURATION
            if operation == "DISPATCH":
                job.attempts += 1
                await _set_execution_state(session, job, "DISPATCHING")
            await session.commit()

            claim = ClaimedJob(
                job_id=job.id,
                execution_id=execution.id,
                run_id=execution.run_id,
                device_id=job.device_id,
                expected_generation=job.expected_generation,
                lease_token=token,
                operation=operation,
                attempts=job.attempts,
                max_attempts=job.max_attempts,
                definition=definition,
                desired_twin_version=job.desired_twin_version,
            )
            _log.info(
                "Claimed deployment job=%s execution=%s run=%s device=%s operation=%s attempt=%d/%d generation=%d",
                claim.job_id,
                claim.execution_id,
                claim.run_id,
                claim.device_id,
                claim.operation,
                claim.attempts,
                claim.max_attempts,
                claim.expected_generation,
            )
            return claim
        except IntegrityError:
            await session.rollback()
            _log.debug("Deployment job claim skipped because another worker claimed it")
            return None


def _retry_delay(attempt: int, retry_after: float | None = None) -> float:
    backoff = min(2 ** max(0, attempt - 1), _MAX_BACKOFF_SECONDS)
    jittered_backoff = random.uniform(backoff, min(backoff * 1.25, _MAX_BACKOFF_SECONDS))
    return max(retry_after or 0, jittered_backoff)


async def _release_for_retry(
    claim: ClaimedJob,
    error: str,
    status_code: int | None,
    delay: float,
) -> None:
    async with AsyncSessionLocal() as session:
        job = await session.get(DeploymentDispatchJob, claim.job_id)
        if job is None or job.lease_token != claim.lease_token:
            return
        if not await _assignment_is_current(session, job):
            job.state = "SUPERSEDED"
            job.lease_token = None
            job.lease_operation = None
            job.lease_expires_at = None
            await _set_execution_state(session, job, "SUPERSEDED")
            await session.commit()
            _log.warning(
                "Deployment job superseded before retry job=%s execution=%s run=%s device=%s",
                claim.job_id,
                claim.execution_id,
                claim.run_id,
                claim.device_id,
            )
            return
        job.last_error = error
        job.last_error_code = status_code
        job.lease_token = None
        job.lease_operation = None
        job.lease_expires_at = None
        job.not_before = datetime.now(timezone.utc) + timedelta(seconds=delay)
        if claim.operation == "RECONCILE":
            job.state = "SUBMITTED"
        elif job.attempts >= job.max_attempts:
            job.state = "FAILED"
            await _set_execution_state(session, job, "FAILED")
        else:
            job.state = "QUEUED"
            await _set_execution_state(session, job, "QUEUED")
        await session.commit()
        if job.state == "FAILED":
            _log.error(
                "Deployment dispatch exhausted retries job=%s execution=%s run=%s device=%s attempt=%d/%d status=%s error=%s",
                claim.job_id,
                claim.execution_id,
                claim.run_id,
                claim.device_id,
                claim.attempts,
                claim.max_attempts,
                status_code,
                error,
            )
        else:
            _log.warning(
                "Deployment job scheduled for retry job=%s execution=%s run=%s device=%s operation=%s attempt=%d/%d status=%s delay_seconds=%.1f error=%s",
                claim.job_id,
                claim.execution_id,
                claim.run_id,
                claim.device_id,
                claim.operation,
                claim.attempts,
                claim.max_attempts,
                status_code,
                delay,
                error,
            )


async def _mark_failed(
    claim: ClaimedJob,
    error: str,
    status_code: int | None,
) -> None:
    async with AsyncSessionLocal() as session:
        job = await session.get(DeploymentDispatchJob, claim.job_id)
        if job is None or job.lease_token != claim.lease_token:
            return
        if not await _assignment_is_current(session, job):
            job.state = "SUPERSEDED"
            job.lease_token = None
            job.lease_operation = None
            job.lease_expires_at = None
            await _set_execution_state(session, job, "SUPERSEDED")
            await session.commit()
            _log.warning(
                "Deployment failure ignored because assignment was superseded job=%s execution=%s run=%s device=%s error=%s",
                claim.job_id,
                claim.execution_id,
                claim.run_id,
                claim.device_id,
                error,
            )
            return
        job.state = "FAILED"
        job.last_error = error
        job.last_error_code = status_code
        job.lease_token = None
        job.lease_operation = None
        job.lease_expires_at = None
        await _set_execution_state(session, job, "FAILED")
        await session.commit()
        _log.error(
            "Deployment job failed job=%s execution=%s run=%s device=%s attempt=%d/%d status=%s error=%s",
            claim.job_id,
            claim.execution_id,
            claim.run_id,
            claim.device_id,
            claim.attempts,
            claim.max_attempts,
            status_code,
            error,
        )


async def _mark_submitted(claim: ClaimedJob, desired_twin_version: int) -> None:
    async with AsyncSessionLocal() as session:
        job = await session.get(DeploymentDispatchJob, claim.job_id)
        if job is None or job.lease_token != claim.lease_token:
            return
        if not await _assignment_is_current(session, job):
            job.state = "SUPERSEDED"
            job.lease_token = None
            job.lease_operation = None
            job.lease_expires_at = None
            await _set_execution_state(session, job, "SUPERSEDED")
            await session.commit()
            _log.warning(
                "Submitted deployment superseded before status update job=%s execution=%s run=%s device=%s twin_version=%d",
                claim.job_id,
                claim.execution_id,
                claim.run_id,
                claim.device_id,
                desired_twin_version,
            )
            return
        job.state = "SUBMITTED"
        job.desired_twin_version = desired_twin_version
        job.submitted_at = datetime.now(timezone.utc)
        job.not_before = datetime.now(timezone.utc) + timedelta(
            seconds=_RECONCILE_INTERVAL_SECONDS
        )
        job.last_error = None
        job.last_error_code = None
        job.lease_token = None
        job.lease_operation = None
        job.lease_expires_at = None
        await _set_execution_state(session, job, "SUBMITTED")
        await session.commit()
        _log.info(
            "Deployment submitted to IoT Hub; awaiting Edge Agent report job=%s execution=%s run=%s device=%s desired_twin_version=%d",
            claim.job_id,
            claim.execution_id,
            claim.run_id,
            claim.device_id,
            desired_twin_version,
        )


async def _release_after_reconciliation(
    claim: ClaimedJob,
    state: str,
    detail: str | None = None,
    delay: float = _RECONCILE_INTERVAL_SECONDS,
) -> None:
    async with AsyncSessionLocal() as session:
        job = await session.get(DeploymentDispatchJob, claim.job_id)
        if job is None or job.lease_token != claim.lease_token:
            return
        if not await _assignment_is_current(session, job):
            state = "SUPERSEDED"
            detail = "The device assignment has a newer generation"
        job.state = state
        job.lease_token = None
        job.lease_operation = None
        job.lease_expires_at = None
        if state == "SUBMITTED":
            job.not_before = datetime.now(timezone.utc) + timedelta(
                seconds=delay
            )
        if detail is not None:
            job.last_error = detail
        elif state == "APPLIED":
            job.last_error = None
            job.last_error_code = None
        if state in {"APPLIED", "FAILED", "SUPERSEDED"}:
            await _set_execution_state(session, job, state)
        await session.commit()
        if state == "APPLIED":
            _log.info(
                "Deployment applied according to Edge Agent report job=%s execution=%s run=%s device=%s desired_twin_version=%s",
                claim.job_id,
                claim.execution_id,
                claim.run_id,
                claim.device_id,
                claim.desired_twin_version,
            )
        elif state == "FAILED":
            _log.error(
                "Edge Agent reported deployment failure job=%s execution=%s run=%s device=%s desired_twin_version=%s detail=%s",
                claim.job_id,
                claim.execution_id,
                claim.run_id,
                claim.device_id,
                claim.desired_twin_version,
                detail,
            )
        elif state == "SUPERSEDED":
            _log.warning(
                "Deployment superseded during reconciliation job=%s execution=%s run=%s device=%s detail=%s",
                claim.job_id,
                claim.execution_id,
                claim.run_id,
                claim.device_id,
                detail,
            )
        elif detail:
            _log.info(
                "Deployment reconciliation deferred job=%s execution=%s run=%s device=%s delay_seconds=%.1f detail=%s",
                claim.job_id,
                claim.execution_id,
                claim.run_id,
                claim.device_id,
                delay,
                detail,
            )


async def _dispatch(claim: ClaimedJob) -> None:
    if claim.definition is None:
        await _mark_failed(claim, "Saved deployment definition is unavailable", None)
        return
    _log.info(
        "Dispatching IoT Edge deployment job=%s execution=%s run=%s device=%s attempt=%d/%d generation=%d",
        claim.job_id,
        claim.execution_id,
        claim.run_id,
        claim.device_id,
        claim.attempts,
        claim.max_attempts,
        claim.expected_generation,
    )
    try:
        desired_twin_version = await send_deployment(
            claim.device_id, claim.definition
        )
    except IoTHubDeploymentError as error:
        retryable = (
            error.status_code in {408, 429}
            or error.status_code >= 500
            or error.status_code == 412
        )
        message = str(error)
        if retryable:
            delay = _retry_delay(claim.attempts, error.retry_after)
            _log.warning(
                "IoT Hub rejected deployment request job=%s execution=%s run=%s device=%s status=%d retry_after_seconds=%s retry_delay_seconds=%.1f",
                claim.job_id,
                claim.execution_id,
                claim.run_id,
                claim.device_id,
                error.status_code,
                error.retry_after,
                delay,
            )
            await _release_for_retry(
                claim,
                message,
                error.status_code,
                delay,
            )
        else:
            _log.error(
                "IoT Hub rejected deployment request permanently job=%s execution=%s run=%s device=%s status=%d",
                claim.job_id,
                claim.execution_id,
                claim.run_id,
                claim.device_id,
                error.status_code,
            )
            await _mark_failed(claim, message, error.status_code)
    except httpx.RequestError as error:
        _log.warning(
            "IoT Hub transport error job=%s execution=%s run=%s device=%s error_type=%s",
            claim.job_id,
            claim.execution_id,
            claim.run_id,
            claim.device_id,
            type(error).__name__,
        )
        await _release_for_retry(
            claim,
            "IoT Hub transport error",
            None,
            _retry_delay(claim.attempts),
        )
    except APIError as error:
        _log.error(
            "Deployment definition could not be dispatched job=%s execution=%s run=%s device=%s status=%s error=%s",
            claim.job_id,
            claim.execution_id,
            claim.run_id,
            claim.device_id,
            error.status_code,
            error,
        )
        await _mark_failed(claim, str(error), error.status_code)
    except (ValueError, TypeError, KeyError) as error:
        _log.exception(
            "Deployment definition validation failed job=%s execution=%s run=%s device=%s error_type=%s",
            claim.job_id,
            claim.execution_id,
            claim.run_id,
            claim.device_id,
            type(error).__name__,
        )
        await _mark_failed(
            claim, f"Invalid saved IoT Edge definition: {type(error).__name__}", 422
        )
    else:
        await _mark_submitted(claim, desired_twin_version)


async def _reconcile(claim: ClaimedJob) -> None:
    _log.debug(
        "Checking Edge Agent deployment report job=%s execution=%s run=%s device=%s expected_twin_version=%s",
        claim.job_id,
        claim.execution_id,
        claim.run_id,
        claim.device_id,
        claim.desired_twin_version,
    )
    try:
        twin_status = await get_edge_agent_reported_status(claim.device_id)
    except IoTHubDeploymentError as error:
        retry_after = error.retry_after
        delay = max(
            _ERROR_RETRY_INTERVAL_SECONDS,
            retry_after or 0,
        )
        _log.warning(
            "Could not read Edge Agent report job=%s execution=%s run=%s device=%s status=%d retry_after_seconds=%s reschedule_seconds=%.1f",
            claim.job_id,
            claim.execution_id,
            claim.run_id,
            claim.device_id,
            error.status_code,
            retry_after,
            delay,
        )
        await _release_after_reconciliation(
            claim,
            "SUBMITTED",
            f"IoT Hub status read failed (HTTP {error.status_code})",
            delay=delay,
        )
        return
    except httpx.RequestError as error:
        _log.warning(
            "IoT Hub status read transport error job=%s execution=%s run=%s device=%s error_type=%s",
            claim.job_id,
            claim.execution_id,
            claim.run_id,
            claim.device_id,
            type(error).__name__,
        )
        await _release_after_reconciliation(
            claim, "SUBMITTED", "IoT Hub status is temporarily unavailable"
        )
        return

    status = twin_status.get("last_desired_status") or {}
    reported_version = twin_status.get("last_desired_version")
    expected_version = claim.desired_twin_version
    code = status.get("code")
    _log.debug(
        "Read Edge Agent deployment report job=%s execution=%s run=%s device=%s desired_twin_version=%s reported_twin_version=%s reported_code=%s",
        claim.job_id,
        claim.execution_id,
        claim.run_id,
        claim.device_id,
        twin_status.get("desired_version"),
        reported_version,
        code,
    )
    if not isinstance(reported_version, int) or expected_version is None:
        await _release_after_reconciliation(
            claim, "SUBMITTED", "Edge Agent has not reported a desired version yet"
        )
        return
    twin_desired_version = twin_status.get("desired_version")
    if isinstance(twin_desired_version, int) and twin_desired_version > expected_version:
        await _release_after_reconciliation(
            claim,
            "SUPERSEDED",
            "A newer edgeAgent desired twin version is current",
        )
        return
    if reported_version > expected_version:
        await _release_after_reconciliation(
            claim,
            "SUPERSEDED",
            "A newer edgeAgent desired twin version is reported",
        )
        return
    if reported_version < expected_version:
        await _release_after_reconciliation(
            claim,
            "SUBMITTED",
            f"Edge Agent reports version {reported_version}; waiting for version {expected_version}",
        )
        return

    if code == 200:
        desired_modules = {
            **twin_status.get("desired_system_modules", {}),
            **twin_status.get("desired_modules", {}),
        }
        reported_modules = {
            **twin_status.get("reported_system_modules", {}),
            **twin_status.get("reported_modules", {}),
        }
        pending_modules = {}
        for name, desired_module in desired_modules.items():
            expected_status = (
                desired_module.get("status", "running")
                if isinstance(desired_module, dict)
                else "running"
            )
            reported_module = reported_modules.get(name)
            reported_status = (
                reported_module.get("runtimeStatus")
                if isinstance(reported_module, dict)
                else None
            )
            if reported_status != expected_status:
                pending_modules[name] = {
                    "expected": expected_status,
                    "reported": reported_status,
                }
        if pending_modules:
            await _release_after_reconciliation(
                claim,
                "SUBMITTED",
                f"Edge Agent accepted desired version {expected_version}; waiting for module runtime states {pending_modules}",
            )
            return
        await _release_after_reconciliation(claim, "APPLIED")
    elif isinstance(code, int) and code >= 400:
        await _release_after_reconciliation(
            claim,
            "FAILED",
            f"IoT Edge agent reported desired-state error (code {code})",
        )
    else:
        await _release_after_reconciliation(claim, "SUBMITTED")


async def process_deployment_dispatch_jobs() -> None:
    """Claim and process a small batch; each claim commits before network I/O."""
    global _missing_hub_warning_logged
    if not IOT_HUB_NAME:
        if not _missing_hub_warning_logged:
            _log.warning(
                "IOT_HUB_NAME is not configured; durable deployment jobs remain queued"
            )
            _missing_hub_warning_logged = True
        return
    for _ in range(10):
        claim = await _claim_next_job()
        if claim is None:
            break
        if claim.operation == "RECONCILE":
            await _reconcile(claim)
        else:
            await _dispatch(claim)
