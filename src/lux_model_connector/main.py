"""ASGI lifecycle for durable delivery, optional provider work, and retained media."""

import asyncio
import hashlib
import os
import shutil
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from dataclasses import dataclass
from typing import Literal
from urllib.parse import quote, urlsplit

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from starlette.types import Receive, Scope, Send

from lux_model_connector.asset_client import JavaAssetLocator
from lux_model_connector.byteplus import BytePlusClient, BytePlusSettings
from lux_model_connector.byteplus_adapters import byteplus_registry
from lux_model_connector.delivery import (
    DeliveryWorker,
    GenerationTransport,
    KafkaTransport,
)
from lux_model_connector.execution import ProviderWorker
from lux_model_connector.reconciliation import (
    ReconciliationSettings,
    create_reconciliation_router,
)
from lux_model_connector.results import (
    ResultPreservationError,
    S3Object,
    S3ResultStore,
    S3Settings,
)
from lux_model_connector.store import ConnectorStore, MySQLSettings


@dataclass(frozen=True)
class ServiceSettings:
    mysql: MySQLSettings | None = None
    kafka_bootstrap_servers: str | None = None
    kafka_group_id: str = "lux-model-connector-v2"
    byteplus: BytePlusSettings | None = None
    results: S3Settings | None = None
    result_base_url: str | None = None
    backend_url: str | None = None
    provider_execution_enabled: bool = False
    provider_poll_seconds: float = 5
    reconciliation: ReconciliationSettings = ReconciliationSettings()

    def __post_init__(self) -> None:
        if self.reconciliation.enabled and self.mysql is None:
            raise ValueError("Reconciliation requires MySQL persistence")
        if self.backend_url is not None:
            parsed = urlsplit(self.backend_url)
            if (
                parsed.scheme not in ("http", "https")
                or not parsed.hostname
                or parsed.username
                or parsed.password
                or parsed.query
                or parsed.fragment
            ):
                raise ValueError(
                    "Java backend URL must be HTTP(S) without credentials or query"
                )
        if self.kafka_bootstrap_servers is not None:
            if not self.mysql or not self.kafka_bootstrap_servers.strip():
                raise ValueError("Kafka reception requires MySQL persistence")
        if not self.kafka_group_id.strip():
            raise ValueError("Kafka group ID must be nonblank")
        if self.results is not None and (
            self.mysql is None or not self.result_base_url
        ):
            raise ValueError("Retained media requires MySQL and a stable connector URL")
        if self.provider_execution_enabled and not (
            self.mysql and self.byteplus and self.results and self.result_base_url
        ):
            raise ValueError(
                "Provider execution requires MySQL, API credentials and result storage"
            )
        if not 0 < self.provider_poll_seconds <= 300:
            raise ValueError("Provider polling interval must be positive and bounded")

    @classmethod
    def from_environment(cls) -> ServiceSettings:
        dsn = os.environ.get("LUX_CONNECTOR_MYSQL_DSN")
        key = os.environ.get("LUX_BYTEPLUS_API_KEY")
        bucket = os.environ.get("LUX_CONNECTOR_S3_BUCKET")
        enabled = os.environ.get(
            "LUX_CONNECTOR_PROVIDER_EXECUTION_ENABLED", "false"
        ).lower()
        if enabled not in ("true", "false"):
            raise ValueError("Provider execution flag must be true or false")
        return cls(
            mysql=MySQLSettings.from_dsn(dsn) if dsn else None,
            reconciliation=ReconciliationSettings.from_environment(),
            kafka_bootstrap_servers=os.environ.get("LUX_KAFKA_BOOTSTRAP_SERVERS"),
            kafka_group_id=os.environ.get(
                "LUX_CONNECTOR_KAFKA_GROUP_ID", "lux-model-connector-v2"
            ),
            byteplus=BytePlusSettings(
                api_key=key,
                account_ref=os.environ.get(
                    "LUX_BYTEPLUS_ACCOUNT_REF", "byteplus-primary"
                ),
            )
            if key
            else None,
            results=S3Settings(
                bucket=bucket,
                region_name=os.environ.get("LUX_CONNECTOR_S3_REGION", "us-east-1"),
                endpoint_url=os.environ.get("LUX_CONNECTOR_S3_ENDPOINT_URL"),
                access_key_id=os.environ.get("LUX_CONNECTOR_S3_ACCESS_KEY_ID", ""),
                secret_access_key=os.environ.get(
                    "LUX_CONNECTOR_S3_SECRET_ACCESS_KEY", ""
                ),
                session_token=os.environ.get("LUX_CONNECTOR_S3_SESSION_TOKEN"),
                prefix=os.environ.get("LUX_CONNECTOR_S3_PREFIX", "connector-results"),
            )
            if bucket
            else None,
            result_base_url=os.environ.get("LUX_CONNECTOR_RESULT_BASE_URL"),
            backend_url=os.environ.get("LUX_BACKEND_URL"),
            provider_execution_enabled=enabled == "true",
            provider_poll_seconds=float(
                os.environ.get("LUX_CONNECTOR_PROVIDER_POLL_SECONDS", "5")
            ),
        )


class HealthResponse(BaseModel):
    status: Literal["ok"] = "ok"


async def _stream_result(object: S3Object, checksum: str):
    digest = hashlib.sha256()
    transferred = 0
    while chunk := await asyncio.to_thread(object.body.read, 65536):
        transferred += len(chunk)
        if transferred > object.byte_length:
            raise ResultPreservationError("Retained object length changed")
        digest.update(chunk)
        yield chunk
    if transferred != object.byte_length or digest.hexdigest() != checksum:
        raise ResultPreservationError("Retained object content changed")


class RetainedResultResponse(StreamingResponse):
    """Own the opened object even if sending headers fails before iteration."""

    def __init__(self, object: S3Object, checksum: str, filename: str) -> None:
        self._object = object
        super().__init__(
            _stream_result(object, checksum),
            media_type=object.media_type,
            headers={
                "Content-Length": str(object.byte_length),
                "Cache-Control": "no-store",
                "X-Content-Type-Options": "nosniff",
                "Content-Disposition": "attachment; filename*=UTF-8''"
                + quote(filename),
            },
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            await asyncio.to_thread(self._object.body.close)


def create_app(
    settings: ServiceSettings | None = None,
    transport: GenerationTransport | None = None,
    provider_client: BytePlusClient | None = None,
    result_store: S3ResultStore | None = None,
) -> FastAPI:
    resolved = settings if settings is not None else ServiceSettings.from_environment()
    if transport is not None and resolved.mysql is None:
        raise ValueError("Command transport requires MySQL persistence")

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        application.state.store = None
        application.state.worker = None
        application.state.execution_worker = None
        application.state.result_store = None
        active_transport = transport
        tasks: list[asyncio.Task] = []
        async with AsyncExitStack() as resources:
            try:
                if resolved.mysql is not None:
                    store = ConnectorStore(
                        resolved.mysql, result_base_url=resolved.result_base_url
                    )
                    await asyncio.to_thread(store.initialize)
                    application.state.store = store
                    if resolved.provider_execution_enabled:
                        await asyncio.to_thread(
                            store.validate_provider_capacity,
                            resolved.byteplus.max_response_bytes,
                        )
                    active_results = result_store
                    if active_results is None and resolved.results is not None:
                        download_http = await resources.enter_async_context(
                            httpx.AsyncClient(follow_redirects=False, timeout=30)
                        )
                        active_results = await asyncio.to_thread(
                            S3ResultStore, resolved.results, download_http
                        )
                        resources.callback(active_results.s3_client.close)
                    application.state.result_store = active_results
                    if active_transport is None and resolved.kafka_bootstrap_servers:
                        active_transport = KafkaTransport(
                            resolved.kafka_bootstrap_servers, resolved.kafka_group_id
                        )
                    if active_transport is not None:
                        await active_transport.start()
                        delivery = asyncio.create_task(
                            DeliveryWorker(store, active_transport).run()
                        )
                        application.state.worker = delivery
                        tasks.append(delivery)
                    if resolved.provider_execution_enabled:
                        if shutil.which("ffprobe") is None:
                            raise ValueError(
                                "Normalized BytePlus execution requires ffprobe"
                            )
                        asset_locator = None
                        if resolved.backend_url is not None:
                            backend_http = await resources.enter_async_context(
                                httpx.AsyncClient(
                                    base_url=resolved.backend_url,
                                    follow_redirects=False,
                                    timeout=30,
                                )
                            )
                            asset_locator = JavaAssetLocator(backend_http)
                        active_client = provider_client
                        if active_client is None:
                            provider_http = await resources.enter_async_context(
                                httpx.AsyncClient(follow_redirects=False)
                            )
                            active_client = BytePlusClient(
                                resolved.byteplus, provider_http
                            )
                        execution = asyncio.create_task(
                            ProviderWorker(
                                store,
                                active_client,
                                active_results,
                                interval_seconds=resolved.provider_poll_seconds,
                                adapters=byteplus_registry(
                                    active_client, active_results
                                ),
                                asset_locator=asset_locator,
                            ).run()
                        )
                        application.state.execution_worker = execution
                        tasks.append(execution)
                yield
            finally:
                try:
                    for task in tasks:
                        task.cancel()
                    for task in tasks:
                        with suppress(asyncio.CancelledError):
                            await task
                finally:
                    if active_transport is not None:
                        await active_transport.close()

    application = FastAPI(
        title="Lux Model Connector", version="0.1.0", lifespan=lifespan
    )

    application.include_router(
        create_reconciliation_router(
            resolved.reconciliation, lambda: getattr(application.state, "store", None)
        )
    )

    @application.get("/health", tags=["health"])
    async def health() -> HealthResponse:
        return HealthResponse()

    @application.get("/internal/generation-results/{token}", include_in_schema=False)
    async def download_result(token: str):
        store = getattr(application.state, "store", None)
        results = getattr(application.state, "result_store", None)
        if store is None or results is None:
            raise HTTPException(
                status_code=503, detail="Retained media is not configured"
            )
        saved = await asyncio.to_thread(store.download_result, token)
        if saved is None:
            raise HTTPException(status_code=404, detail="Result not found")
        try:
            object = await asyncio.to_thread(results.read, saved)
        except ResultPreservationError:
            raise HTTPException(
                status_code=503, detail="Retained media is temporarily unavailable"
            ) from None
        return RetainedResultResponse(object, saved.sha256, saved.filename)

    return application


app = create_app()
