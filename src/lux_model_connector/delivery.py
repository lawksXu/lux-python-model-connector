"""Kafka delivery acknowledges durable receipts and republishes saved event bytes."""

import asyncio
import logging
from typing import Protocol

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.errors import CommitFailedError

from lux_model_connector.store import (
    COMMAND_TOPIC,
    ConnectorStore,
    InboundRecord,
    Publication,
)

MAX_KAFKA_RECORD_BYTES = 32 * 1024 * 1024
MAX_KAFKA_FETCH_BYTES = 64 * 1024 * 1024
_LOGGER = logging.getLogger(__name__)


class OwnershipLost(RuntimeError):
    """The durably received record belongs to another Kafka partition owner."""


class GenerationTransport(Protocol):
    async def start(self) -> None: ...
    async def poll(self) -> InboundRecord | None: ...
    async def commit(self, record: InboundRecord) -> None: ...
    async def publish(self, publication: Publication) -> None: ...
    async def close(self) -> None: ...


class KafkaTransport:
    def __init__(self, bootstrap_servers: str, group_id: str) -> None:
        self._consumer = AIOKafkaConsumer(
            COMMAND_TOPIC,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_records=1,
            max_poll_interval_ms=300000,
            max_partition_fetch_bytes=MAX_KAFKA_RECORD_BYTES,
            fetch_max_bytes=MAX_KAFKA_FETCH_BYTES,
        )
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            acks="all",
            enable_idempotence=True,
            request_timeout_ms=10000,
            max_request_size=MAX_KAFKA_RECORD_BYTES,
        )

    async def start(self) -> None:
        # Lifespan closes both clients if either startup operation fails.
        async with asyncio.timeout(20):
            await self._producer.start()
            await self._consumer.start()

    async def poll(self) -> InboundRecord | None:
        try:
            async with asyncio.timeout(0.2):
                record = await self._consumer.getone()
        except TimeoutError:
            return None
        return InboundRecord(
            record.topic, record.partition, record.offset, record.key, record.value
        )

    async def commit(self, record: InboundRecord) -> None:
        partition = TopicPartition(record.topic, record.partition)
        if partition not in self._consumer.assignment():
            raise OwnershipLost("Command partition ownership changed")
        try:
            async with asyncio.timeout(10):
                await self._consumer.commit({partition: record.offset + 1})
        except CommitFailedError as error:
            # Broker membership can change after the local assignment check.
            raise OwnershipLost("Command partition ownership changed") from error

    async def publish(self, publication: Publication) -> None:
        async with asyncio.timeout(10):
            await self._producer.send_and_wait(
                publication.topic, key=publication.key, value=publication.value
            )

    async def close(self) -> None:
        try:
            async with asyncio.timeout(10):
                await self._consumer.stop()
        finally:
            async with asyncio.timeout(10):
                await self._producer.stop()


class DeliveryWorker:
    def __init__(self, store: ConnectorStore, transport: GenerationTransport) -> None:
        self._store = store
        self._transport = transport
        self._record: InboundRecord | None = None

    async def step(self) -> None:
        receive_error: Exception | None = None
        try:
            if self._record is None:
                self._record = await self._transport.poll()
            if self._record is not None:
                # Keep this offset pending until persistence and commit both succeed.
                await asyncio.to_thread(self._store.receive, self._record)
                try:
                    await self._transport.commit(self._record)
                except OwnershipLost:
                    self._record = None
                else:
                    self._record = None
        except Exception as error:
            receive_error = error

        # A deferred command acknowledgment must not starve durable event delivery.
        publication = await asyncio.to_thread(self._store.claim_publication)
        if publication is not None:
            try:
                await self._transport.publish(publication)
            except Exception as error:
                error_type = type(error).__name__
                await asyncio.to_thread(
                    self._store.publication_failed, publication, error_type
                )
                _LOGGER.warning("Generation publication deferred (%s)", error_type)
            else:
                await asyncio.to_thread(self._store.publication_delivered, publication)
        if receive_error is not None:
            raise receive_error

    async def run(self) -> None:
        # The app lifespan owns transport startup and cleanup, including failures.
        while True:
            try:
                await self.step()
            except Exception as error:
                _LOGGER.warning(
                    "Generation delivery deferred (%s)", type(error).__name__
                )
                await asyncio.sleep(1)
            await asyncio.sleep(0.05)
