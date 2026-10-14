import asyncio
import threading
from collections import deque
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from lux_model_connector.store import InboundRecord, Publication


def inbound(offset: int = 0, value: bytes = b"submit") -> InboundRecord:
    from lux_model_connector.store import InboundRecord

    return InboundRecord(
        topic="lux.generation.commands.v2",
        partition=0,
        offset=offset,
        key=b"owner:task",
        value=value,
    )


def publication() -> Publication:
    from lux_model_connector.store import Publication

    return Publication(
        event_id="8a0b55bd-67b8-46a3-aaef-b22c9a8cec16",
        topic="lux.generation.events.v2",
        key=b"owner:task",
        value=b'{ "original" : "\xc3\xa9", "sequence_no" : 1 }',
        lease_token="original-claim",
    )


class StoreDouble:
    def __init__(self, pending: Publication | None = None) -> None:
        self.pending = pending
        self.receive_failures = 0
        self.received: list[InboundRecord] = []
        self.diagnosed: list[InboundRecord] = []
        self.delivered: list[Publication] = []
        self.failed: list[tuple[Publication, str]] = []
        self.thread_ids: list[int] = []

    def receive(self, record: InboundRecord) -> None:
        self.thread_ids.append(threading.get_ident())
        if self.receive_failures:
            self.receive_failures -= 1
            raise OSError("database unavailable")
        self.received.append(record)
        if record.value == b"invalid":
            self.diagnosed.append(record)

    def claim_publication(self) -> Publication | None:
        self.thread_ids.append(threading.get_ident())
        return self.pending

    def publication_delivered(self, claimed: Publication) -> None:
        self.thread_ids.append(threading.get_ident())
        self.delivered.append(claimed)
        self.pending = None

    def publication_failed(self, claimed: Publication, error: str) -> None:
        self.thread_ids.append(threading.get_ident())
        self.failed.append((claimed, error))


class TransportDouble:
    def __init__(self, store: StoreDouble, records: list[InboundRecord]) -> None:
        self.store = store
        self.records: deque[InboundRecord] = deque(records)
        self.commit_errors: deque[Exception] = deque()
        self.publish_errors: deque[Exception] = deque()
        self.committed: list[InboundRecord] = []
        self.published: list[Publication] = []
        self.poll_count = 0
        self.closed = False

    async def start(self) -> None:
        pass

    async def poll(self) -> InboundRecord | None:
        self.poll_count += 1
        return self.records.popleft() if self.records else None

    async def commit(self, record: InboundRecord) -> None:
        assert record in self.store.received
        if record.value == b"invalid":
            assert record in self.store.diagnosed
        if self.commit_errors:
            raise self.commit_errors.popleft()
        self.committed.append(record)

    async def publish(self, claimed: Publication) -> None:
        self.published.append(claimed)
        if self.publish_errors:
            raise self.publish_errors.popleft()

    async def close(self) -> None:
        self.closed = True


@pytest.mark.parametrize("failure", ["receipt", "commit"])
def test_failed_receipt_or_commit_retains_offset_and_still_publishes(failure):
    async def scenario():
        from lux_model_connector.delivery import DeliveryWorker

        first, second = inbound(), inbound(1)
        outgoing = publication()
        store = StoreDouble(outgoing)
        transport = TransportDouble(store, [first, second])
        if failure == "receipt":
            store.receive_failures = 1
        else:
            transport.commit_errors.append(OSError("broker unavailable"))
        worker = DeliveryWorker(store, transport)

        with pytest.raises(OSError):
            await worker.step()
        assert transport.committed == []
        assert transport.published == [outgoing]
        assert store.delivered == [outgoing]

        await worker.step()
        assert transport.poll_count == 1
        assert transport.committed == [first]
        await worker.step()
        assert transport.committed == [first, second]

    asyncio.run(scenario())


def test_diagnosed_invalid_input_is_durable_before_offset_commit():
    async def scenario():
        from lux_model_connector.delivery import DeliveryWorker

        record = inbound(value=b"invalid")
        store = StoreDouble()
        transport = TransportDouble(store, [record])
        await DeliveryWorker(store, transport).step()
        assert transport.committed == [record]
        assert store.diagnosed == [record]

    asyncio.run(scenario())


def test_ownership_loss_releases_pending_offset_and_does_not_block_publication():
    async def scenario():
        from lux_model_connector.delivery import DeliveryWorker, OwnershipLost

        first, second = inbound(), inbound(1)
        outgoing = publication()
        store = StoreDouble(outgoing)
        transport = TransportDouble(store, [first, second])
        transport.commit_errors.append(OwnershipLost("partition revoked"))
        worker = DeliveryWorker(store, transport)
        await worker.step()
        await worker.step()
        assert transport.committed == [second]
        assert transport.published == [outgoing]
        assert transport.poll_count == 2

    asyncio.run(scenario())


def test_uncertain_publication_retries_saved_bytes_without_reencoding():
    async def scenario():
        from lux_model_connector.delivery import DeliveryWorker

        outgoing = publication()
        store = StoreDouble(outgoing)
        transport = TransportDouble(store, [])
        transport.publish_errors.append(TimeoutError("uncertain broker ack"))
        worker = DeliveryWorker(store, transport)
        await worker.step()
        await worker.step()
        assert transport.published == [outgoing, outgoing]
        assert all(attempt.value == outgoing.value for attempt in transport.published)
        assert store.failed == [(outgoing, "TimeoutError")]
        assert store.delivered == [outgoing]

    asyncio.run(scenario())


def test_sync_store_calls_run_outside_the_event_loop_thread():
    async def scenario():
        from lux_model_connector.delivery import DeliveryWorker

        loop_thread = threading.get_ident()
        store = StoreDouble(publication())
        transport = TransportDouble(store, [inbound()])
        await DeliveryWorker(store, transport).step()
        assert len(store.thread_ids) == 3
        assert all(identity != loop_thread for identity in store.thread_ids)

    asyncio.run(scenario())


def test_run_cancellation_leaves_transport_cleanup_to_lifespan():
    async def scenario():
        from lux_model_connector.delivery import DeliveryWorker

        store = StoreDouble()
        transport = TransportDouble(store, [])
        task = asyncio.create_task(DeliveryWorker(store, transport).run())
        async with asyncio.timeout(1):
            while transport.poll_count == 0:
                await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not transport.closed

    asyncio.run(scenario())


def test_kafka_options_and_commit_publish_preserve_transport_contract(monkeypatch):
    async def scenario():
        from aiokafka import TopicPartition
        from aiokafka.errors import CommitFailedError

        from lux_model_connector import delivery

        class ConsumerDouble:
            def __init__(self, *topics, **options):
                self.topics = topics
                self.options = options
                self.owned = {TopicPartition("lux.generation.commands.v2", 0)}
                self.committed = []
                self.commit_error = None

            async def start(self):
                pass

            async def stop(self):
                pass

            def assignment(self):
                return self.owned

            async def commit(self, offsets):
                if self.commit_error is not None:
                    raise self.commit_error
                self.committed.append(offsets)

        class ProducerDouble:
            def __init__(self, **options):
                self.options = options
                self.sent = []

            async def start(self):
                pass

            async def stop(self):
                pass

            async def send_and_wait(self, topic, **payload):
                self.sent.append((topic, payload))

        monkeypatch.setattr(delivery, "AIOKafkaConsumer", ConsumerDouble)
        monkeypatch.setattr(delivery, "AIOKafkaProducer", ProducerDouble)
        transport = delivery.KafkaTransport("localhost:9092", "connector-test")
        assert transport._consumer.topics == ("lux.generation.commands.v2",)
        options = transport._consumer.options
        assert options["enable_auto_commit"] is False
        assert options["max_poll_records"] == 1
        assert options["max_partition_fetch_bytes"] == 32 * 1024 * 1024
        assert options["fetch_max_bytes"] == 64 * 1024 * 1024
        options = transport._producer.options
        assert options["acks"] == "all"
        assert options["enable_idempotence"] is True
        assert options["max_request_size"] == 32 * 1024 * 1024

        record = inbound(7)
        await transport.commit(record)
        assert transport._consumer.committed == [
            {TopicPartition(record.topic, record.partition): 8}
        ]
        transport._consumer.owned = set()
        with pytest.raises(delivery.OwnershipLost):
            await transport.commit(record)
        assert len(transport._consumer.committed) == 1

        transport._consumer.owned = {TopicPartition(record.topic, record.partition)}
        transport._consumer.commit_error = CommitFailedError()
        with pytest.raises(delivery.OwnershipLost):
            await transport.commit(record)
        assert len(transport._consumer.committed) == 1

        outgoing = publication()
        await transport.publish(outgoing)
        assert transport._producer.sent == [
            (outgoing.topic, {"key": outgoing.key, "value": outgoing.value})
        ]
        await transport.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("operation", ["start", "poll", "commit", "publish", "close"])
def test_kafka_waits_are_bounded(operation, monkeypatch):
    async def scenario():
        from aiokafka import TopicPartition

        from lux_model_connector import delivery

        original_timeout = asyncio.timeout
        producer_stopped = False

        class ConsumerDouble:
            def __init__(self, *topics, **options):
                pass

            async def start(self):
                if operation == "start":
                    await asyncio.Event().wait()

            async def getone(self):
                await asyncio.Event().wait()

            def assignment(self):
                return {TopicPartition("lux.generation.commands.v2", 0)}

            async def commit(self, offsets):
                await asyncio.Event().wait()

            async def stop(self):
                if operation == "close":
                    await asyncio.Event().wait()

        class ProducerDouble:
            def __init__(self, **options):
                pass

            async def start(self):
                pass

            async def send_and_wait(self, topic, **payload):
                await asyncio.Event().wait()

            async def stop(self):
                nonlocal producer_stopped
                producer_stopped = True

        def short_timeout(delay):
            assert 0 < delay <= 30
            return original_timeout(0.001)

        monkeypatch.setattr(delivery, "AIOKafkaConsumer", ConsumerDouble)
        monkeypatch.setattr(delivery, "AIOKafkaProducer", ProducerDouble)
        monkeypatch.setattr(delivery.asyncio, "timeout", short_timeout)
        transport = delivery.KafkaTransport("localhost:9092", "connector-test")
        if operation == "poll":
            assert await transport.poll() is None
            return
        with pytest.raises(TimeoutError):
            if operation == "start":
                await transport.start()
            elif operation == "commit":
                await transport.commit(inbound())
            elif operation == "publish":
                await transport.publish(publication())
            else:
                await transport.close()
        if operation == "close":
            assert producer_stopped

    asyncio.run(scenario())
