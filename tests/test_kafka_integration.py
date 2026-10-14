"""Opt-in real Kafka/MySQL acceptance on the dedicated test services only."""

import asyncio
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from conftest import TestServices as IsolatedServices

from lux_model_connector.protocol import (
    ArchivedGenerationRequest,
    GenerationEvent,
    GenerationSubmit,
    GenerationSuccessOutcome,
    ResultSource,
    TaskStatusQuery,
    canonical_json,
)
from lux_model_connector.store import (
    COMMAND_TOPIC,
    EVENT_TOPIC,
    ConnectorStore,
    MySQLSettings,
)


@pytest.fixture
def kafka_servers() -> str:
    path = os.environ.get("LUX_CONNECTOR_TEST_CONFIG")
    if path is None:
        pytest.skip("Set LUX_CONNECTOR_TEST_CONFIG for isolated Kafka integration")
    services = IsolatedServices(**json.loads(Path(path).read_text()))
    if not services.kafka:
        pytest.skip("Dedicated Kafka bootstrap servers are not configured")
    return services.kafka


def _receipt_count(store: ConnectorStore, key: bytes) -> int:
    with store.connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM kafka_receipt WHERE original_key=%s", (key,)
        )
        return cursor.fetchone()[0]


def _assert_saved_events(
    store: ConnectorStore, user_id: str, task_id: str, observations: list[bytes]
) -> None:
    with store.connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT envelope,published_at FROM outbox_event "
            "WHERE user_id=%s AND task_id=%s ORDER BY sequence_no",
            (user_id, task_id),
        )
        rows = cursor.fetchall()
        assert [row[0].encode() for row in rows] == observations
        assert all(row[1] is not None for row in rows)
        cursor.execute(
            "SELECT COUNT(*) FROM command_receipt WHERE partition_key=%s",
            (f"{user_id}:{task_id}".encode(),),
        )
        assert cursor.fetchone()[0] == 2


def test_real_kafka_admission_restart_query_and_terminal_publication(
    kafka_servers: str, mysql_settings: MySQLSettings
) -> None:
    async def scenario() -> None:
        from lux_model_connector.delivery import DeliveryWorker, KafkaTransport

        user_id, session_id, task_id, operation_id = (str(uuid4()) for _ in range(4))
        group_id = "lux-connector-integration-" + uuid4().hex
        key = f"{user_id}:{task_id}".encode()
        timestamp = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        request = ArchivedGenerationRequest(
            {
                "model_id": "archive-only-test",
                "prompt": " 雪\n",
                "seed": 10**80,
                "extension": [None, False, 1.0, {"explicit_null": None}],
            }
        )
        command = GenerationSubmit(
            schema_version=2,
            event_id=str(uuid4()),
            event_type="SUBMIT",
            occurred_at=timestamp,
            user_id=user_id,
            session_id=session_id,
            task_id=task_id,
            operation_id=operation_id,
            kind="IMAGE",
            request=request,
        )
        command_bytes = canonical_json(command.model_dump(mode="json")).encode()
        store = ConnectorStore(mysql_settings)
        await asyncio.to_thread(store.initialize)
        transport = KafkaTransport(kafka_servers, group_id)
        producer = AIOKafkaProducer(
            bootstrap_servers=kafka_servers,
            client_id="lux-test-producer-" + uuid4().hex,
            acks="all",
            enable_idempotence=True,
            max_request_size=32 * 1024 * 1024,
        )
        observer = AIOKafkaConsumer(
            EVENT_TOPIC,
            bootstrap_servers=kafka_servers,
            client_id="lux-test-observer-" + uuid4().hex,
            group_id="lux-event-observer-" + uuid4().hex,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_partition_fetch_bytes=32 * 1024 * 1024,
            fetch_max_bytes=64 * 1024 * 1024,
        )
        observations: list[bytes] = []

        async def observe_available() -> None:
            records = await observer.getmany(timeout_ms=100)
            for partition_records in records.values():
                for record in partition_records:
                    if record.key == key:
                        decoded = GenerationEvent.model_validate_json(record.value)
                        assert decoded.user_id == user_id
                        assert decoded.session_id == session_id
                        assert decoded.task_id == task_id
                        assert decoded.event_type == "GENERATION_EVENT"
                        observations.append(record.value)

        try:
            async with asyncio.timeout(45):
                await producer.start()
                await observer.start()
                await transport.start()
                worker = DeliveryWorker(store, transport)
                original = await producer.send_and_wait(
                    COMMAND_TOPIC, key=key, value=command_bytes
                )
                duplicate = await producer.send_and_wait(
                    COMMAND_TOPIC,
                    partition=original.partition,
                    key=key,
                    value=command_bytes,
                )
                while await asyncio.to_thread(_receipt_count, store, key) != 2:
                    await worker.step()
                    await observe_available()
                while len(observations) < 1:
                    await worker.step()
                    await observe_available()
                assert len(observations) == 1
                assert (
                    GenerationEvent.model_validate_json(observations[0]).sequence_no
                    == 1
                )
                execution = await asyncio.to_thread(store.execution, user_id, task_id)
                assert execution is not None
                assert execution.status == "ACCEPTED"
                assert (
                    execution.input_generation_payload.request.model_dump()
                    == request.model_dump()
                )
                assert execution.next_event_sequence == 2
                committed = await transport._consumer.committed(
                    TopicPartition(COMMAND_TOPIC, duplicate.partition)
                )
                assert committed == duplicate.offset + 1

                # Restart retains the schema/group and committed duplicate offsets.
                await transport.close()
                restarted = ConnectorStore(mysql_settings)
                await asyncio.to_thread(restarted.initialize)
                transport = KafkaTransport(kafka_servers, group_id)
                await transport.start()
                worker = DeliveryWorker(restarted, transport)
                restored = await asyncio.to_thread(
                    restarted.execution, user_id, task_id
                )
                assert restored == execution
                query = TaskStatusQuery(
                    schema_version=2,
                    event_id=str(uuid4()),
                    event_type="TASK_STATUS_QUERY",
                    occurred_at=timestamp,
                    user_id=user_id,
                    session_id=session_id,
                    task_id=task_id,
                    last_observed_sequence_no=1,
                )
                query_receipt = await producer.send_and_wait(
                    COMMAND_TOPIC,
                    partition=original.partition,
                    key=key,
                    value=canonical_json(query.model_dump(mode="json")).encode(),
                )
                while len(observations) < 2:
                    await worker.step()
                    await observe_available()
                decoded = [
                    GenerationEvent.model_validate_json(raw) for raw in observations
                ]
                assert [value.sequence_no for value in decoded] == [1, 2]
                assert all(value.connector_status == "ACCEPTED" for value in decoded)
                assert all(value.outcome is None for value in decoded)
                assert decoded[0].event_id != decoded[1].event_id
                assert await asyncio.to_thread(_receipt_count, restarted, key) == 3
                committed = await transport._consumer.committed(
                    TopicPartition(COMMAND_TOPIC, query_receipt.partition)
                )
                assert committed == query_receipt.offset + 1
                restored = await asyncio.to_thread(
                    restarted.execution, user_id, task_id
                )
                assert restored is not None
                assert restored.id == execution.id
                assert restored.status == "ACCEPTED"
                assert restored.next_event_sequence == 3
                # Supply a test terminal fact; neither provider nor source is accessed.
                success = GenerationSuccessOutcome(
                    status="SUCCEEDED",
                    sources=(
                        ResultSource(
                            url="https://example.org/result.png?test=opaque",
                            filename="雪.png",
                            media_type="image/png",
                        ),
                    ),
                )
                await asyncio.to_thread(
                    restarted.transition, user_id, task_id, "RUNNING"
                )
                await asyncio.to_thread(
                    restarted.transition, user_id, task_id, "SUCCEEDED", success
                )
                while len(observations) < 4:
                    await worker.step()
                    await observe_available()
                decoded = [
                    GenerationEvent.model_validate_json(raw) for raw in observations
                ]
                assert [value.sequence_no for value in decoded] == [1, 2, 3, 4]
                assert [value.connector_status for value in decoded] == [
                    "ACCEPTED",
                    "ACCEPTED",
                    "RUNNING",
                    "SUCCEEDED",
                ]
                assert decoded[2].outcome is None
                assert isinstance(decoded[3].outcome, GenerationSuccessOutcome)
                assert decoded[3].outcome == success
                terminal = await asyncio.to_thread(
                    restarted.execution, user_id, task_id
                )
                assert terminal is not None
                assert terminal.status == "SUCCEEDED"
                assert terminal.outcome == success
                assert terminal.terminal_event_id == decoded[3].event_id
                await asyncio.to_thread(
                    _assert_saved_events, restarted, user_id, task_id, observations
                )
        finally:
            try:
                await transport.close()
            finally:
                try:
                    await observer.stop()
                finally:
                    await producer.stop()

    asyncio.run(scenario())
