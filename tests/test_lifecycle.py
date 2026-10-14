import asyncio
from dataclasses import replace

import pytest

from lux_model_connector.main import ServiceSettings, create_app


class Transport:
    def __init__(self, fail=False):
        self.started = self.closed = False
        self.fail = fail

    async def start(self):
        self.started = True
        if self.fail:
            raise RuntimeError("startup")

    async def poll(self):
        await asyncio.sleep(0.01)
        return None

    async def publish(self, publication):
        pass

    async def commit(self, record):
        pass

    async def close(self):
        self.closed = True


def test_default_lifespan_health_only():
    async def scenario():
        app = create_app(ServiceSettings())
        async with app.router.lifespan_context(app):
            assert app.state.store is None

    asyncio.run(scenario())


def test_kafka_requires_mysql():
    with pytest.raises(ValueError):
        ServiceSettings(kafka_bootstrap_servers="localhost:9092")
    with pytest.raises(ValueError):
        create_app(ServiceSettings(), Transport())


def test_configured_lifespan_and_shutdown(mysql_settings):
    async def scenario():
        transport = Transport()
        app = create_app(ServiceSettings(mysql=mysql_settings), transport)
        async with app.router.lifespan_context(app):
            assert transport.started
            assert app.state.store is not None
            assert not app.state.worker.done()
        assert transport.closed
        assert app.state.worker.done()

    asyncio.run(scenario())


def test_startup_failure_closes_transport(mysql_settings):
    async def scenario():
        transport = Transport(fail=True)
        app = create_app(ServiceSettings(mysql=mysql_settings), transport)
        with pytest.raises(RuntimeError, match="startup"):
            async with app.router.lifespan_context(app):
                pytest.fail("startup must fail")
        assert transport.closed

    asyncio.run(scenario())


def test_storage_startup_failure_closes_injected_transport(mysql_settings):
    async def scenario():
        transport = Transport()
        settings = replace(mysql_settings, database="lux_connector_test_missing")
        app = create_app(ServiceSettings(mysql=settings), transport)
        with pytest.raises(Exception):
            async with app.router.lifespan_context(app):
                pytest.fail("database must fail")
        assert transport.closed

    asyncio.run(scenario())
