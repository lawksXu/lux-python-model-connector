"""Scoped human attestation; attachment is evidence, never another provider POST."""

import argparse
import asyncio
import hashlib
import os
import secrets
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Annotated, Literal, Self

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import Field, StrictInt, StrictStr, model_validator

from lux_model_connector.byteplus import (
    ProviderSubmissionPayload,
    SeedreamReceipt,
    SeedreamRequest,
)
from lux_model_connector.byteplus_adapters import normalize_seedream_receipt
from lux_model_connector.generation_plan import (
    NormalizedPreparedSubmission,
    NormalizedReceipt,
    ProviderBinding,
)
from lux_model_connector.protocol import (
    CanonicalUuid,
    FrozenModel,
    canonical_json,
)
from lux_model_connector.store import ConnectorStore, StoreConflict

if TYPE_CHECKING:
    from lux_model_connector.store import ExecutionWork


def digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


@dataclass(frozen=True)
class ReconciliationSettings:
    enabled: bool = False
    bearer_token: str = field(default="", repr=False)
    operator_id: str = ""
    allowed_accounts: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.enabled and (
            not self.bearer_token.strip()
            or not self.operator_id.strip()
            or not self.allowed_accounts
            or any(not account.strip() for account in self.allowed_accounts)
        ):
            raise ValueError(
                "Reconciliation requires token, trusted operator and account scope"
            )

    @classmethod
    def from_environment(cls) -> ReconciliationSettings:
        enabled = os.environ.get(
            "LUX_CONNECTOR_RECONCILIATION_ENABLED", "false"
        ).lower()
        if enabled not in ("true", "false"):
            raise ValueError("Reconciliation enabled flag must be true or false")
        return cls(
            enabled=enabled == "true",
            bearer_token=os.environ.get("LUX_CONNECTOR_RECONCILIATION_TOKEN", ""),
            operator_id=os.environ.get("LUX_CONNECTOR_RECONCILIATION_OPERATOR", ""),
            allowed_accounts=tuple(
                account.strip()
                for account in os.environ.get(
                    "LUX_CONNECTOR_RECONCILIATION_ACCOUNTS", ""
                ).split(",")
                if account.strip()
            ),
        )


class OriginalProviderBinding(FrozenModel):
    execution_id: Annotated[StrictInt, Field(gt=0)]
    user_id: CanonicalUuid
    session_id: CanonicalUuid
    task_id: CanonicalUuid
    operation_id: CanonicalUuid
    kind: Literal["IMAGE", "VIDEO"]
    provider_id: Literal["byteplus"]
    account_ref: StrictStr
    model_id: StrictStr
    provider_binding: ProviderBinding | None = None
    request_hash: Annotated[StrictStr, Field(pattern=r"^[a-f0-9]{64}$")]
    submission_hash: Annotated[StrictStr, Field(pattern=r"^[a-f0-9]{64}$")]


class OriginalProviderEvidence(FrozenModel):
    binding: OriginalProviderBinding
    external_id: (
        Annotated[StrictStr, Field(pattern=r"^[A-Za-z0-9_-]{1,1024}$")] | None
    ) = None
    image_receipt: SeedreamReceipt | None = None
    evidence_reference: Annotated[StrictStr, Field(min_length=1, max_length=2048)]
    reason: Annotated[StrictStr, Field(min_length=1, max_length=2048)]

    @model_validator(mode="after")
    def original_kind(self) -> Self:
        if (
            self.binding.kind == "VIDEO"
            and (self.external_id is None or self.image_receipt is not None)
        ) or (
            self.binding.kind == "IMAGE"
            and (self.external_id is not None or self.image_receipt is None)
        ):
            raise ValueError(
                "Exactly the original kind of provider evidence is required"
            )
        if not self.reason.strip() or not self.evidence_reference.strip():
            raise ValueError("Original evidence reference and reason are required")
        return self


class OriginalEvidenceView(FrozenModel):
    binding: OriginalProviderBinding
    status: str
    external_id: str | None
    has_receipt: bool


class OriginalEvidencePage(FrozenModel):
    executions: tuple[OriginalEvidenceView, ...]


def original_view(work: ExecutionWork) -> OriginalEvidenceView:
    prepared = work.submission_payload
    if prepared is None:
        raise ValueError("Original prepared submission is required")
    execution = work.execution
    normalized = isinstance(prepared, NormalizedPreparedSubmission)
    return OriginalEvidenceView(
        binding=OriginalProviderBinding(
            execution_id=execution.id,
            user_id=execution.user_id,
            session_id=execution.session_id,
            task_id=execution.task_id,
            operation_id=execution.operation_id,
            kind=execution.kind,
            provider_id=prepared.binding.provider_id
            if normalized
            else prepared.provider_id,
            account_ref=prepared.account_ref,
            model_id=prepared.request.model_selection.model_id
            if normalized
            else prepared.request.model_id,
            provider_binding=prepared.binding if normalized else None,
            request_hash=digest(
                canonical_json(execution.input_generation_payload.request.root)
            ),
            submission_hash=digest(canonical_json(prepared.model_dump(mode="json"))),
        ),
        status=execution.status,
        external_id=work.provider_external_id,
        has_receipt=work.provider_receipt is not None,
    )


def original_image_receipt(
    prepared: ProviderSubmissionPayload | NormalizedPreparedSubmission,
    receipt: SeedreamReceipt,
) -> SeedreamReceipt | NormalizedReceipt:
    if not isinstance(prepared, NormalizedPreparedSubmission):
        if receipt.model is not None and receipt.model != prepared.request.model_id:
            raise ValueError("Original image model differs")
        return receipt
    request = SeedreamRequest.model_validate(prepared.provider_payload)
    if receipt.model is not None and receipt.model != request.model_id:
        raise ValueError("Original image model differs")
    return normalize_seedream_receipt(request, receipt)


def create_reconciliation_router(
    settings: ReconciliationSettings,
    store_provider: Callable[[], ConnectorStore | None],
) -> APIRouter:
    router = APIRouter(
        prefix="/internal/provider-reconciliation", include_in_schema=False
    )

    def authenticated(
        authorization: Annotated[str | None, Header()] = None,
    ) -> ConnectorStore:
        if not settings.enabled:
            raise HTTPException(404, "Not found")
        supplied = (
            authorization.removeprefix("Bearer ")
            if authorization and authorization.startswith("Bearer ")
            else ""
        )
        if not secrets.compare_digest(
            supplied.encode(), settings.bearer_token.encode()
        ):
            raise HTTPException(401, "Unauthorized")
        store = store_provider()
        if store is None:
            raise HTTPException(503, "Connector persistence unavailable")
        return store

    @router.get("")
    async def unresolved(
        store: ConnectorStore = Depends(authenticated),
    ) -> OriginalEvidencePage:
        views = await asyncio.to_thread(
            store.unresolved_provider_evidence, settings.allowed_accounts
        )
        return OriginalEvidencePage(executions=views)

    @router.get("/{user_id}/{task_id}")
    async def detail(
        user_id: str,
        task_id: str,
        store: ConnectorStore = Depends(authenticated),
    ) -> OriginalEvidenceView:
        work = await asyncio.to_thread(store.reconciliation_work, user_id, task_id)
        if (
            work is None
            or work.submission_payload is None
            or work.submission_payload.account_ref not in settings.allowed_accounts
        ):
            raise HTTPException(404, "Execution not found")
        return original_view(work)

    @router.post("/{user_id}/{task_id}/original-receipt")
    async def attach(
        user_id: str,
        task_id: str,
        evidence: OriginalProviderEvidence,
        store: ConnectorStore = Depends(authenticated),
    ) -> OriginalEvidenceView:
        if evidence.binding.user_id != user_id or evidence.binding.task_id != task_id:
            raise HTTPException(409, "Original execution binding differs")
        try:
            return await asyncio.to_thread(
                store.attach_provider_evidence,
                evidence,
                settings.operator_id,
                settings.allowed_accounts,
            )
        except StoreConflict, ValueError:
            raise HTTPException(
                409, "Original evidence conflicts with saved execution"
            ) from None

    return router


def main() -> None:
    """Explicit additive upgrade for an existing connector-owned database."""
    from lux_model_connector.store import ConnectorStore, MySQLSettings

    parser = argparse.ArgumentParser(
        description="Upgrade the owned Connector schema additively"
    )
    parser.add_argument("--upgrade-schema", action="store_true", required=True)
    parser.parse_args()
    dsn = os.environ.get("LUX_CONNECTOR_MYSQL_DSN")
    if not dsn:
        parser.error("LUX_CONNECTOR_MYSQL_DSN is required")
    ConnectorStore(MySQLSettings.from_dsn(dsn)).initialize()
    print("Connector owned schema validated and upgraded to version 3")


if __name__ == "__main__":
    main()
