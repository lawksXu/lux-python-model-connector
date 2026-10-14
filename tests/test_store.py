"""Real MySQL tests of admission, atomic state/events, and replay recovery."""

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from uuid import uuid4

import pymysql
import pytest

from lux_model_connector.protocol import GenerationSuccessOutcome, ResultSource
from lux_model_connector.store import (
    COMMAND_TOPIC,
    ConnectorStore,
    InboundRecord,
    MySQLSettings,
    StoreConflict,
)

USER, SESSION, TASK, OPERATION = (str(uuid4()) for _ in range(4))


def submit(offset=0, **changes):
    payload = dict(
        schema_version=2,
        event_id=str(uuid4()),
        event_type="SUBMIT",
        occurred_at="2026-10-12T00:00:00Z",
        user_id=USER,
        session_id=SESSION,
        task_id=TASK,
        operation_id=OPERATION,
        kind="IMAGE",
        request={"model_id": "future-model", "prompt": "猫", "seed": 10**80},
    )
    payload.update(changes)
    return InboundRecord(
        COMMAND_TOPIC,
        0,
        offset,
        f"{USER}:{payload['task_id']}".encode(),
        json.dumps(payload, ensure_ascii=False).encode(),
    )


def query(offset, watermark=0, **changes):
    payload = dict(
        schema_version=2,
        event_id=str(uuid4()),
        event_type="TASK_STATUS_QUERY",
        occurred_at="2026-10-12T00:00:00Z",
        user_id=USER,
        session_id=SESSION,
        task_id=TASK,
        last_observed_sequence_no=watermark,
    )
    payload.update(changes)
    return InboundRecord(
        COMMAND_TOPIC,
        0,
        offset,
        f"{USER}:{payload['task_id']}".encode(),
        json.dumps(payload).encode(),
    )


def success():
    return GenerationSuccessOutcome(
        status="SUCCEEDED",
        sources=(
            ResultSource(
                url="https://example.org/a.png",
                filename="猫.png",
                media_type="image/png",
            ),
        ),
    )


def drain(store):
    publications = []
    while publication := store.claim_publication():
        publications.append(publication)
        assert store.publication_delivered(publication)
    return publications


def test_restart_reuses_admission_and_preserves_archive(store, mysql_settings):
    record = submit()
    store.receive(record)
    restarted = ConnectorStore(mysql_settings)
    restarted.initialize()
    restarted.receive(replace(record, offset=1))
    restarted.receive(submit(2))
    execution = restarted.execution(USER, TASK)
    assert execution.id > 0
    assert execution.status == "ACCEPTED"
    assert execution.input_generation_payload.request.root["seed"] == 10**80
    assert execution.next_event_sequence == 2
    assert len(drain(restarted)) == 1


@pytest.mark.parametrize(
    "change",
    [
        dict(request={"prompt": "changed"}),
        dict(task_id=str(uuid4())),
        dict(operation_id=str(uuid4())),
        dict(session_id=str(uuid4())),
    ],
)
def test_identity_conflicts_diagnosed_without_changing_original(store, change):
    store.receive(submit())
    conflict = submit(1, **change)
    store.receive(conflict)
    assert store.receipt_error(conflict) == "IDENTITY_CONFLICT"
    assert store.execution(USER, TASK).status == "ACCEPTED"
    assert len(drain(store)) == 1


def test_duplicate_event_changed_body_and_wrong_key_are_durable(store):
    original = submit()
    store.receive(original)
    body = json.loads(original.value)
    body["request"] = {"prompt": "different"}
    altered = replace(original, offset=1, value=json.dumps(body).encode())
    store.receive(altered)
    assert store.receipt_error(altered) == "EVENT_CONFLICT"
    wrong_key = replace(submit(2), key=b"wrong")
    store.receive(wrong_key)
    assert store.receipt_error(wrong_key) == "INVALID_COMMAND"
    assert len(drain(store)) == 1


@pytest.mark.parametrize(
    "raw",
    [b'{"broken":', b"\xff", None, b'{"event_type":"SUBMIT","event_type":"SUBMIT"}'],
)
def test_invalid_ingress_diagnosed_before_ack(store, raw):
    record = replace(submit(), value=raw)
    store.receive(record)
    store.receive(record)
    assert store.receipt_error(record) == "INVALID_COMMAND"
    assert store.execution(USER, TASK) is None


def test_query_returns_current_status_and_owns_sequence(store):
    store.receive(submit())
    record = query(1, watermark=999)
    store.receive(record)
    store.receive(replace(record, offset=2))
    assert [json.loads(p.value)["sequence_no"] for p in drain(store)] == [1, 2]
    unknown = query(3, task_id=str(uuid4()))
    store.receive(unknown)
    assert store.receipt_error(unknown) == "UNKNOWN_TASK"


def test_terminal_commit_idempotent_immutable_and_replay_same_event(store):
    store.receive(submit())
    store.transition(USER, TASK, "RUNNING")
    store.transition(USER, TASK, "SUCCEEDED", success())
    terminal = store.execution(USER, TASK)
    store.transition(USER, TASK, "SUCCEEDED", success())
    with pytest.raises(StoreConflict):
        store.transition(USER, TASK, "RUNNING")
    events = drain(store)
    assert [json.loads(p.value)["connector_status"] for p in events] == [
        "ACCEPTED",
        "RUNNING",
        "SUCCEEDED",
    ]
    assert terminal.terminal_event_id == events[-1].event_id
    store.replay_terminal(USER, TASK)
    replay = store.claim_publication()
    assert replay.value == events[-1].value
    assert replay.event_id == events[-1].event_id
    assert not store.publication_delivered(events[-1])
    assert store.publication_delivered(replay)


def test_claim_expiry_order_and_stale_ack(store):
    store.receive(submit())
    store.transition(USER, TASK, "RUNNING")
    first = store.claim_publication()
    assert store.claim_publication() is None
    with store.connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            "UPDATE outbox_event SET lease_until=UTC_TIMESTAMP(6)-INTERVAL 1 "
            "SECOND WHERE event_id=%s",
            (first.event_id,),
        )
        connection.commit()
    reclaimed = store.claim_publication()
    assert reclaimed.event_id == first.event_id
    assert not store.publication_delivered(first)
    assert store.publication_delivered(reclaimed)
    assert json.loads(store.claim_publication().value)["sequence_no"] == 2


def test_concurrent_terminal_commits_allocate_one_event(store):
    store.receive(submit())
    store.transition(USER, TASK, "RUNNING")
    with ThreadPoolExecutor(max_workers=4) as workers:
        list(
            workers.map(
                lambda _: store.transition(USER, TASK, "SUCCEEDED", success()), range(8)
            )
        )
    assert store.execution(USER, TASK).next_event_sequence == 4
    assert len(drain(store)) == 3


def test_database_failure_rolls_back_receipt_execution_and_event(store):
    with store.connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            "CREATE TRIGGER fail_outbox BEFORE INSERT ON outbox_event FOR EACH "
            "ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced failure'"
        )
        connection.commit()
    record = submit()
    with pytest.raises(pymysql.MySQLError):
        store.receive(record)
    assert store.execution(USER, TASK) is None
    with store.connection() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM kafka_receipt")
        assert cursor.fetchone()[0] == 0
        cursor.execute("DROP TRIGGER fail_outbox")
    store.receive(record)
    assert len(drain(store)) == 1


def test_foreign_schema_rejected_without_ddl(mysql_settings):
    store = ConnectorStore(mysql_settings)
    with store.connection() as connection, connection.cursor() as cursor:
        cursor.execute("CREATE TABLE java_business (id BIGINT PRIMARY KEY)")
    with pytest.raises(StoreConflict):
        store.initialize()
    with store.connection() as connection, connection.cursor() as cursor:
        cursor.execute("SHOW TABLES")
        assert cursor.fetchall() == (("java_business",),)


def test_dsn_validation_and_secret_redaction():
    settings = MySQLSettings.from_dsn(
        "mysql://connector:secret%21@localhost:3306/lux_connector?ssl_ca=%2Ftmp%2Fca.pem"
    )
    assert settings.password == "secret!"
    assert settings.ssl_ca == "/tmp/ca.pem"
    assert "secret" not in repr(settings)
    for dsn in (
        "sqlite:///test",
        "mysql://root@host/java?unknown=1",
        "mysql://u:p@host/a-b",
    ):
        with pytest.raises(ValueError):
            MySQLSettings.from_dsn(dsn)


def test_execution_lifecycle_timestamps(store):
    store.receive(submit())
    store.transition(USER, TASK, "RUNNING")
    store.transition(USER, TASK, "SUCCEEDED", success())
    with store.connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT created_at,started_at,finished_at FROM generation_execution"
        )
        created, started, finished = cursor.fetchone()
        assert created <= started <= finished


@pytest.mark.parametrize(
    "alteration",
    [
        "ALTER TABLE generation_execution DROP INDEX execution_operation",
        "ALTER TABLE outbox_event MODIFY envelope VARCHAR(2000) NOT NULL",
        "ALTER TABLE generation_execution MODIFY status "
        "ENUM('accepted','running','succeeded','failed') NOT NULL",
    ],
)
def test_schema_drift_refused_on_restart(store, mysql_settings, alteration):
    with store.connection() as connection, connection.cursor() as cursor:
        cursor.execute(alteration)
    with pytest.raises(StoreConflict):
        ConnectorStore(mysql_settings).initialize()


def test_publication_failure_keeps_original_and_backoff_blocks_later(store):
    store.receive(submit())
    store.transition(USER, TASK, "RUNNING")
    original = store.claim_publication()
    assert store.publication_failed(original, "TimeoutError")
    assert not store.publication_delivered(original)
    assert store.claim_publication() is None
    with store.connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            "UPDATE outbox_event SET next_attempt_at=UTC_TIMESTAMP(6) "
            "WHERE event_id=%s",
            (original.event_id,),
        )
        connection.commit()
    retry = store.claim_publication()
    assert retry.value == original.value
    assert retry.lease_token != original.lease_token
    assert store.publication_delivered(retry)
    assert json.loads(store.claim_publication().value)["sequence_no"] == 2


def test_terminal_database_failure_retains_running_and_sequence(store):
    store.receive(submit())
    store.transition(USER, TASK, "RUNNING")
    with store.connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            "CREATE TRIGGER fail_terminal BEFORE INSERT ON outbox_event "
            "FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='forced failure'"
        )
    with pytest.raises(pymysql.MySQLError):
        store.transition(USER, TASK, "SUCCEEDED", success())
    execution = store.execution(USER, TASK)
    assert execution.status == "RUNNING"
    assert execution.outcome is None
    assert execution.terminal_event_id is None
    assert execution.next_event_sequence == 3
    assert len(drain(store)) == 2


@pytest.mark.parametrize("duplicate", ["operation", "event", "offset"])
def test_concurrent_admission_allocates_one_execution_and_event(store, duplicate):
    original = submit()
    records = [
        submit(offset)
        if duplicate == "operation"
        else replace(original, offset=offset)
        if duplicate == "event"
        else original
        for offset in range(8)
    ]
    with ThreadPoolExecutor(max_workers=4) as workers:
        list(workers.map(store.receive, records))
    assert store.execution(USER, TASK).next_event_sequence == 2
    assert len(drain(store)) == 1


def test_status_query_uses_original_terminal_outcome(store):
    store.receive(submit())
    store.transition(USER, TASK, "RUNNING")
    store.transition(USER, TASK, "SUCCEEDED", success())
    original_id = store.execution(USER, TASK).terminal_event_id
    store.receive(query(1))
    events = drain(store)
    assert (
        json.loads(events[-1].value)["outcome"]
        == json.loads(events[-2].value)["outcome"]
    )
    assert json.loads(events[-1].value)["sequence_no"] == 4
    assert store.execution(USER, TASK).terminal_event_id == original_id
