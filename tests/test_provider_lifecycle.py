"""Configured lifecycle with real MySQL and mocked BytePlus/object boundaries."""

import asyncio

import httpx
import pytest
from test_results import PNG, FakeS3
from test_results import settings as s3_settings
from test_store import TASK, USER, submit

from lux_model_connector.byteplus import BytePlusClient, BytePlusSettings
from lux_model_connector.main import ServiceSettings, create_app
from lux_model_connector.results import S3ResultStore


def test_provider_enablement_requires_complete_explicit_configuration():
    with pytest.raises(ValueError):
        ServiceSettings(provider_execution_enabled=True)
    with pytest.raises(ValueError):
        ServiceSettings(results=s3_settings())
    provider = BytePlusSettings(api_key="private-offline-key")
    config = ServiceSettings(byteplus=provider)
    assert not config.provider_execution_enabled
    assert "private-offline-key" not in repr(config)
    assert "storage-private-secret" not in repr(s3_settings())


def test_environment_does_not_enable_generation_with_credentials_only(monkeypatch):
    monkeypatch.setenv("LUX_BYTEPLUS_API_KEY", "private-offline-key")
    monkeypatch.delenv("LUX_CONNECTOR_PROVIDER_EXECUTION_ENABLED", raising=False)
    settings = ServiceSettings.from_environment()
    assert settings.byteplus is not None
    assert not settings.provider_execution_enabled
    monkeypatch.setenv("LUX_CONNECTOR_PROVIDER_EXECUTION_ENABLED", "yes")
    with pytest.raises(ValueError):
        ServiceSettings.from_environment()


def test_configured_image_execution_and_private_object_download(mysql_settings):
    async def scenario():
        requests = []

        def image(request):
            requests.append(request)
            return httpx.Response(
                200, json={"data": [{"url": "https://provider-output.test/a.png"}]}
            )

        async with (
            httpx.AsyncClient(transport=httpx.MockTransport(image)) as provider_http,
            httpx.AsyncClient(
                transport=httpx.MockTransport(
                    lambda _: httpx.Response(200, content=PNG)
                )
            ) as download_http,
        ):
            provider_settings = BytePlusSettings(api_key="offline-key")
            object_settings = s3_settings()
            client = BytePlusClient(provider_settings, provider_http)
            results = S3ResultStore(object_settings, download_http, FakeS3())
            settings = ServiceSettings(
                mysql=mysql_settings,
                byteplus=provider_settings,
                results=object_settings,
                result_base_url="http://connector.test",
                provider_execution_enabled=True,
                provider_poll_seconds=0.01,
            )
            app = create_app(settings, provider_client=client, result_store=results)
            async with app.router.lifespan_context(app):
                record = submit(
                    request={"model_id": "upstream-new-model", "prompt": "cat"}
                )
                await asyncio.to_thread(app.state.store.receive, record)
                async with asyncio.timeout(3):
                    while (
                        await asyncio.to_thread(app.state.store.execution, USER, TASK)
                    ).status != "SUCCEEDED":
                        await asyncio.sleep(0.01)
                execution = await asyncio.to_thread(
                    app.state.store.execution, USER, TASK
                )
                source = execution.outcome.sources[0]
                assert len(requests) == 1
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app),
                    base_url="http://connector.test",
                ) as http:
                    response = await http.get(source.url)
                    assert response.status_code == 200
                    assert response.content == PNG
                    assert response.headers["content-type"] == "image/png"
                    assert response.headers["cache-control"] == "no-store"
                    missing = await http.get("/internal/generation-results/" + "0" * 64)
                    assert missing.status_code == 404
                assert not app.state.execution_worker.done()
            assert app.state.execution_worker.done()

    asyncio.run(scenario())


def test_credentials_configured_worker_still_disabled(mysql_settings):
    async def scenario():
        config = ServiceSettings(
            mysql=mysql_settings, byteplus=BytePlusSettings(api_key="not-used")
        )
        app = create_app(config)
        async with app.router.lifespan_context(app):
            assert app.state.execution_worker is None
            await asyncio.to_thread(
                app.state.store.receive,
                submit(request={"model_id": "some-id", "prompt": "cat"}),
            )
            assert app.state.store.execution(USER, TASK).status == "ACCEPTED"

    asyncio.run(scenario())


def test_result_body_closes_when_header_send_disconnects():
    import io

    from starlette.requests import ClientDisconnect

    from lux_model_connector.main import RetainedResultResponse
    from lux_model_connector.results import S3Object

    async def scenario():
        body = io.BytesIO(PNG)
        response = RetainedResultResponse(
            S3Object(body, "image/png", len(PNG)), "0" * 64, "0.png"
        )

        async def send(message):
            raise OSError("disconnected before headers")

        async def receive():
            return {"type": "http.disconnect"}

        with pytest.raises(ClientDisconnect):
            await response(
                {"type": "http", "asgi": {"spec_version": "2.4"}}, receive, send
            )
        assert body.closed

    asyncio.run(scenario())
