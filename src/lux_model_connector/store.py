"""Connector-owned MySQL transactions; broker calls never run inside them."""

import hashlib
import json
import re
import secrets
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from importlib.resources import files
from typing import TYPE_CHECKING, Literal
from urllib.parse import parse_qs, unquote, urlsplit
from uuid import uuid4

import pymysql
from pydantic import TypeAdapter

from lux_model_connector.byteplus import (
    ProviderSubmissionPayload,
    SeedanceReceipt,
    SeedreamReceipt,
)
from lux_model_connector.generation_plan import (
    AdapterRegistration,
    NormalizedPreparedSubmission,
    NormalizedReceipt,
)
from lux_model_connector.protocol import (
    MAX_SEQUENCE_NO,
    ConnectorExecutionStatus,
    GenerationError,
    GenerationEvent,
    GenerationFailureOutcome,
    GenerationIdentity,
    GenerationOutcome,
    GenerationSubmit,
    GenerationSuccessOutcome,
    InputGenerationPayload,
    ResultSource,
    TaskStatusQuery,
    canonical_json,
    decode_command,
    encode_event,
)
from lux_model_connector.results import StoredResult

if TYPE_CHECKING:
    from lux_model_connector.reconciliation import (
        OriginalEvidenceView,
        OriginalProviderEvidence,
    )


COMMAND_TOPIC = "lux.generation.commands.v2"
EVENT_TOPIC = "lux.generation.events.v2"
_OWNER = "lux-model-connector"
_OUTCOME = TypeAdapter(GenerationOutcome)
_EXECUTION_COLUMNS = (
    "id,user_id,session_id,task_id,operation_id,kind,input_generation_payload,"
    "status,outcome,terminal_event_id,next_event_sequence"
)


def _column_type(value: str) -> str:
    # ENUM labels are case-sensitive protocol facts even with a CI collation.
    name, delimiter, arguments = value.partition("(")
    return name.lower() + delimiter + arguments


class StoreConflict(RuntimeError):
    """An ownership, schema, or immutable-fact conflict requires diagnosis."""


@dataclass(frozen=True)
class MySQLSettings:
    host: str
    port: int
    user: str
    password: str = field(repr=False)
    database: str
    ssl_ca: str | None = None

    def __post_init__(self) -> None:
        if not self.host or not self.user or not 1 <= self.port <= 65535:
            raise ValueError("MySQL host, user, and valid port are required")
        if not re.fullmatch(r"[A-Za-z0-9_]{1,64}", self.database):
            raise ValueError("Dedicated MySQL schema name is required")

    @classmethod
    def from_dsn(cls, dsn: str) -> MySQLSettings:
        parsed = urlsplit(dsn)
        if parsed.scheme != "mysql" or parsed.fragment:
            raise ValueError("Expected mysql:// DSN")
        options = parse_qs(parsed.query, keep_blank_values=True)
        if set(options) - {"ssl_ca"} or any(len(v) != 1 for v in options.values()):
            raise ValueError("Unsupported MySQL DSN option")
        return cls(
            host=parsed.hostname or "",
            port=parsed.port or 3306,
            user=unquote(parsed.username or ""),
            password=unquote(parsed.password or ""),
            database=unquote(parsed.path.removeprefix("/")),
            ssl_ca=options.get("ssl_ca", [None])[0],
        )


@dataclass(frozen=True)
class InboundRecord:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None


@dataclass(frozen=True)
class Publication:
    event_id: str
    topic: str
    key: bytes
    value: bytes
    lease_token: str


@dataclass(frozen=True)
class ExecutionRecord:
    id: int
    user_id: str
    session_id: str
    task_id: str
    operation_id: str
    kind: Literal["IMAGE", "VIDEO"]
    input_generation_payload: InputGenerationPayload
    status: ConnectorExecutionStatus
    outcome: GenerationOutcome | None
    terminal_event_id: str | None
    next_event_sequence: int

    @classmethod
    def from_row(cls, row: tuple) -> ExecutionRecord:
        return cls(
            *row[:6],
            InputGenerationPayload.model_validate_json(row[6]),
            row[7],
            _OUTCOME.validate_json(row[8]) if row[8] is not None else None,
            row[9],
            row[10],
        )


@dataclass(frozen=True)
class SchemaColumn:
    name: str
    sql_type: str
    nullable: bool
    collation: str | None
    default: str | None
    auto_increment: bool


@dataclass(frozen=True)
class ExecutionWork:
    execution: ExecutionRecord
    submission_payload: ProviderSubmissionPayload | NormalizedPreparedSubmission | None
    submission_started_at: datetime | None
    provider_external_id: str | None
    provider_receipt: SeedreamReceipt | SeedanceReceipt | NormalizedReceipt | None

    @classmethod
    def from_row(cls, row: tuple) -> ExecutionWork:
        execution = ExecutionRecord.from_row(row[:11])
        receipt_type = SeedreamReceipt if execution.kind == "IMAGE" else SeedanceReceipt
        return cls(
            execution,
            (
                NormalizedPreparedSubmission.model_validate_json(row[11])
                if json.loads(row[11]).get("payload_type") == "normalized_prepared_v1"
                else ProviderSubmissionPayload.model_validate_json(row[11])
            )
            if row[11]
            else None,
            row[12],
            row[13],
            (
                NormalizedReceipt.model_validate_json(row[14])
                if (
                    json.loads(row[14]).get("receipt_type") == "normalized_receipt_v1"
                    or (
                        row[11]
                        and json.loads(row[11]).get("payload_type")
                        == "normalized_prepared_v1"
                    )
                )
                else receipt_type.model_validate_json(row[14])
            )
            if row[14]
            else None,
        )


@dataclass(frozen=True)
class ExecutionClaim:
    work: ExecutionWork
    token: str


_WORK_COLUMNS = (
    _EXECUTION_COLUMNS + ",submission_payload,submission_started_at,"
    "provider_external_id,provider_receipt"
)


class ConnectorStore:
    def __init__(
        self,
        settings: MySQLSettings,
        result_base_url: str | None = None,
    ) -> None:
        self.settings = settings
        self.result_base_url = result_base_url.rstrip("/") if result_base_url else None
        if self.result_base_url:
            parsed_base = urlsplit(self.result_base_url)
            if parsed_base.query or parsed_base.fragment:
                raise ValueError(
                    "Result gateway base cannot contain a query or fragment"
                )
            ResultSource(
                url=self.result_base_url + "/result",
                filename="result",
                media_type="application/octet-stream",
            )

    @contextmanager
    def connection(self) -> Iterator[pymysql.Connection]:
        # A fresh connection per transaction also permits independent worker threads.
        connection = pymysql.connect(
            host=self.settings.host,
            port=self.settings.port,
            user=self.settings.user,
            password=self.settings.password,
            database=self.settings.database,
            charset="utf8mb4",
            autocommit=False,
            connect_timeout=5,
            read_timeout=10,
            write_timeout=10,
            ssl_ca=self.settings.ssl_ca,
            ssl_verify_cert=bool(self.settings.ssl_ca),
            ssl_verify_identity=bool(self.settings.ssl_ca),
        )
        try:
            with connection.cursor() as cursor:
                cursor.execute("SET time_zone = '+00:00'")
                cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
            yield connection
        finally:
            connection.close()

    def initialize(self) -> None:
        script = files("lux_model_connector").joinpath("schema.sql").read_text()
        statements = [part.strip() for part in script.split(";") if part.strip()]
        expected = set(re.findall(r"CREATE TABLE IF NOT EXISTS (\w+)", script))
        with self.connection() as connection, connection.cursor() as cursor:
            lock = (
                "lux-connector-schema:"
                + hashlib.sha256(self.settings.database.encode()).hexdigest()[:32]
            )
            cursor.execute("SELECT GET_LOCK(%s,10)", (lock,))
            if cursor.fetchone()[0] != 1:
                raise StoreConflict("Schema initialization lock unavailable")
            try:
                cursor.execute("SHOW TABLES")
                existing = {row[0] for row in cursor.fetchall()}
                if existing - expected or (
                    existing and "connector_schema_version" not in existing
                ):
                    raise StoreConflict("Refusing foreign schema")
                if existing:
                    cursor.execute(
                        "SELECT owner,version FROM connector_schema_version WHERE "
                        "singleton=1"
                    )
                    marker = cursor.fetchone()
                    if (
                        marker is None
                        or marker[0] != _OWNER
                        or marker[1] not in (0, 1, 2, 3)
                    ):
                        raise StoreConflict("Unsupported connector schema")
                    if marker[1] == 3 and existing != expected:
                        raise StoreConflict("Incomplete connector schema")
                else:
                    cursor.execute(statements[0])
                    cursor.execute(
                        "INSERT INTO connector_schema_version VALUES (1,%s,0)",
                        (_OWNER,),
                    )
                    connection.commit()
                    marker = (_OWNER, 0)
                # Validate owned existing structures before any additive DDL. A v1
                # migration may have stopped between the two nullable column additions.
                additions = {
                    "execution_lease_token": (
                        "CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL"
                    ),
                    "execution_lease_until": "DATETIME(6) NULL",
                }
                if marker[1] == 1 and expected - existing - {
                    "generation_result",
                    "provider_reconciliation_audit",
                }:
                    raise StoreConflict("Incomplete v1 connector schema")
                for statement in statements:
                    name = re.search(r"EXISTS (\w+)", statement).group(1)
                    if name in existing:
                        missing = (
                            frozenset(additions)
                            if marker[1] < 2 and name == "generation_execution"
                            else frozenset()
                        )
                        self._validate_table(cursor, statement, allowed_missing=missing)
                if marker[1] < 2:
                    for statement in statements[1:]:
                        cursor.execute(statement)
                    cursor.execute("SHOW COLUMNS FROM generation_execution")
                    columns = {row[0] for row in cursor.fetchall()}
                    for name, definition in additions.items():
                        if name not in columns:
                            cursor.execute(
                                f"ALTER TABLE generation_execution "
                                f"ADD COLUMN {name} {definition}"
                            )
                if marker[1] < 3:
                    for statement in statements:
                        if (
                            "CREATE TABLE IF NOT EXISTS provider_reconciliation_audit"
                            in statement
                        ):
                            cursor.execute(statement)
                for statement in statements:
                    self._validate_table(cursor, statement)
                cursor.execute(
                    "UPDATE connector_schema_version SET version=3 WHERE singleton=1"
                )
                connection.commit()
            finally:
                cursor.execute("SELECT RELEASE_LOCK(%s)", (lock,))

    def _validate_table(
        self,
        cursor: pymysql.cursors.Cursor,
        statement: str,
        allowed_missing: frozenset[str] = frozenset(),
    ) -> None:
        name = re.search(r"EXISTS (\w+)", statement).group(1)
        definitions = []
        primary: tuple[str, ...] = ()
        for line in statement.splitlines():
            matched = re.fullmatch(
                r"    (\w+) (ENUM\([^)]*\)|\w+(?:\([^)]*\))?)(.*),?",
                line,
            )
            if matched is None or matched[1] in {
                "PRIMARY",
                "UNIQUE",
                "FOREIGN",
                "CHECK",
                "KEY",
            }:
                continue
            column, sql_type, rest = matched.groups()
            if "PRIMARY KEY" in rest:
                primary = (column,)
            collation = re.search(r"COLLATE (\w+)", rest)
            default = re.search(r"DEFAULT ([^,]+)", rest)
            definitions.append(
                SchemaColumn(
                    column,
                    _column_type(sql_type),
                    not ("NOT NULL" in rest or "PRIMARY KEY" in rest),
                    collation[1] if collation else None,
                    default[1].strip().lower() if default else None,
                    "AUTO_INCREMENT" in rest,
                )
            )
        cursor.execute(f"SHOW FULL COLUMNS FROM `{name}`")
        actual = {
            row[0]: SchemaColumn(
                row[0],
                _column_type(row[1]),
                row[3] == "YES",
                row[2],
                str(row[5]).lower() if row[5] is not None else None,
                "auto_increment" in row[6],
            )
            for row in cursor.fetchall()
        }
        expected_columns = {column.name for column in definitions}
        if (
            set(actual) - expected_columns
            or expected_columns - set(actual) - allowed_missing
        ):
            raise StoreConflict("Connector schema columns differ")
        for column in definitions:
            if column.name not in actual:
                continue
            observed = actual[column.name]
            if (
                column.sql_type != observed.sql_type
                or column.nullable != observed.nullable
                or column.default != observed.default
                or column.auto_increment != observed.auto_increment
                or (column.collation and column.collation != observed.collation)
            ):
                raise StoreConflict("Connector schema column definition differs")
        table_primary = re.search(r"PRIMARY KEY \(([^)]+)\)", statement)
        if table_primary:
            primary = tuple(part.strip() for part in table_primary[1].split(","))
        required: dict[str, tuple[str, ...]] = {"PRIMARY": primary}
        for index in re.finditer(r"UNIQUE KEY (\w+) \(([^)]+)\)", statement):
            required[index[1]] = tuple(part.strip() for part in index[2].split(","))
        cursor.execute(f"SHOW INDEX FROM `{name}`")
        unique: dict[str, list[str]] = {}
        for row in cursor.fetchall():
            if row[1] == 0:
                unique.setdefault(row[2], []).append(row[4])
        if any(tuple(unique.get(key, [])) != value for key, value in required.items()):
            raise StoreConflict("Connector schema unique index differs")
        cursor.execute(
            "SELECT ENGINE FROM information_schema.TABLES WHERE "
            "TABLE_SCHEMA=%s AND TABLE_NAME=%s",
            (self.settings.database, name),
        )
        if cursor.fetchone()[0] != "InnoDB":
            raise StoreConflict("Connector schema requires transactional tables")

    def execution(self, user_id: str, task_id: str) -> ExecutionRecord | None:
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                f"SELECT {_EXECUTION_COLUMNS} FROM generation_execution WHERE "
                f"user_id=%s AND task_id=%s",
                (user_id, task_id),
            )
            row = cursor.fetchone()
            return ExecutionRecord.from_row(row) if row else None

    def receive(self, record: InboundRecord) -> None:
        error: str | None = None
        command = None
        try:
            if record.topic != COMMAND_TOPIC or record.value is None:
                raise ValueError("Unsupported topic or missing value")
            command = decode_command(record.value)
            if record.key != f"{command.user_id}:{command.task_id}".encode():
                raise ValueError("Incorrect partition key")
        except ValueError, UnicodeError, RecursionError:
            error = "INVALID_COMMAND"
        with self.connection() as connection, connection.cursor() as cursor:
            # No-op upserts lock duplicates exclusively; INSERT IGNORE takes
            # shared locks that deadlock when concurrent receivers upgrade.
            cursor.execute(
                "INSERT INTO kafka_receipt(topic,partition_no,offset_no,"
                "original_key,original_value) VALUES (%s,%s,%s,%s,%s) ON "
                "DUPLICATE KEY UPDATE offset_no=offset_no",
                (
                    record.topic,
                    record.partition,
                    record.offset,
                    record.key,
                    record.value,
                ),
            )
            if cursor.rowcount == 0:
                cursor.execute(
                    "SELECT original_key,original_value FROM kafka_receipt WHERE "
                    "topic=%s AND partition_no=%s AND offset_no=%s FOR UPDATE",
                    (record.topic, record.partition, record.offset),
                )
                if cursor.fetchone() != (record.key, record.value):
                    raise StoreConflict("Physical Kafka receipt changed")
                connection.commit()
                return
            if error is None:
                payload = canonical_json(
                    command.model_dump(mode="json", exclude_unset=True)
                )
                cursor.execute(
                    "INSERT INTO command_receipt(topic,event_id,command_payload,"
                    "partition_key) VALUES (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE "
                    "event_id=event_id",
                    (record.topic, command.event_id, payload, record.key),
                )
                if cursor.rowcount == 0:
                    cursor.execute(
                        "SELECT command_payload,partition_key,error_code FROM "
                        "command_receipt WHERE topic=%s AND event_id=%s FOR UPDATE",
                        (record.topic, command.event_id),
                    )
                    saved_payload, saved_key, saved_error = cursor.fetchone()
                    error = (
                        saved_error
                        if (saved_payload, saved_key) == (payload, record.key)
                        else "EVENT_CONFLICT"
                    )
                else:
                    error = self._apply(cursor, command)
                    cursor.execute(
                        "UPDATE command_receipt SET error_code=%s WHERE topic=%s AND "
                        "event_id=%s",
                        (error, record.topic, command.event_id),
                    )
            cursor.execute(
                "UPDATE kafka_receipt SET event_id=%s,error_code=%s WHERE "
                "topic=%s AND partition_no=%s AND offset_no=%s",
                (
                    command.event_id if command else None,
                    error,
                    record.topic,
                    record.partition,
                    record.offset,
                ),
            )
            connection.commit()

    def _apply(
        self,
        cursor: pymysql.cursors.Cursor,
        command: GenerationSubmit | TaskStatusQuery,
    ) -> str | None:
        if isinstance(command, GenerationSubmit):
            archive = InputGenerationPayload(
                identity=GenerationIdentity(
                    user_id=command.user_id,
                    session_id=command.session_id,
                    task_id=command.task_id,
                    operation_id=command.operation_id,
                ),
                kind=command.kind,
                request=command.request,
            )
            payload = canonical_json(archive.model_dump(mode="json"))
            cursor.execute(
                "INSERT INTO generation_execution(user_id,session_id,task_id,"
                "operation_id,kind,input_generation_payload,status) VALUES "
                "(%s,%s,%s,%s,%s,%s,'ACCEPTED') ON DUPLICATE KEY UPDATE id=id",
                (
                    command.user_id,
                    command.session_id,
                    command.task_id,
                    command.operation_id,
                    command.kind,
                    payload,
                ),
            )
            inserted = cursor.rowcount == 1
            cursor.execute(
                f"SELECT {_EXECUTION_COLUMNS} FROM generation_execution WHERE "
                f"user_id=%s AND (task_id=%s OR operation_id=%s) ORDER BY id FOR "
                f"UPDATE",
                (command.user_id, command.task_id, command.operation_id),
            )
            rows = cursor.fetchall()
            if (
                len(rows) != 1
                or canonical_json(
                    ExecutionRecord.from_row(
                        rows[0]
                    ).input_generation_payload.model_dump(mode="json")
                )
                != payload
            ):
                return "IDENTITY_CONFLICT"
            if inserted:
                self._event(cursor, ExecutionRecord.from_row(rows[0]), "ACCEPTED", None)
        else:
            cursor.execute(
                f"SELECT {_EXECUTION_COLUMNS} FROM generation_execution WHERE "
                f"user_id=%s AND task_id=%s FOR UPDATE",
                (command.user_id, command.task_id),
            )
            row = cursor.fetchone()
            if row is None:
                return "UNKNOWN_TASK"
            execution = ExecutionRecord.from_row(row)
            if execution.session_id != command.session_id:
                return "IDENTITY_CONFLICT"
            self._event(cursor, execution, execution.status, execution.outcome)
        return None

    def _event(
        self,
        cursor: pymysql.cursors.Cursor,
        execution: ExecutionRecord,
        status: ConnectorExecutionStatus,
        outcome: GenerationOutcome | None,
    ) -> str:
        if execution.next_event_sequence >= MAX_SEQUENCE_NO:
            raise StoreConflict("Event sequence exhausted")
        event = GenerationEvent(
            event_id=str(uuid4()),
            occurred_at=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            user_id=execution.user_id,
            session_id=execution.session_id,
            task_id=execution.task_id,
            sequence_no=execution.next_event_sequence,
            connector_status=status,
            outcome=outcome,
        )
        envelope = encode_event(event)
        cursor.execute(
            "INSERT INTO outbox_event(event_id,execution_id,user_id,"
            "task_id,sequence_no,topic,partition_key,envelope) VALUES "
            "(%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                event.event_id,
                execution.id,
                execution.user_id,
                execution.task_id,
                event.sequence_no,
                EVENT_TOPIC,
                f"{execution.user_id}:{execution.task_id}".encode(),
                envelope,
            ),
        )
        cursor.execute(
            "UPDATE generation_execution SET "
            "next_event_sequence=next_event_sequence+1,"
            "updated_at=UTC_TIMESTAMP(6) WHERE id=%s",
            (execution.id,),
        )
        return event.event_id

    def transition(
        self,
        user_id: str,
        task_id: str,
        status: ConnectorExecutionStatus,
        outcome: GenerationOutcome | None = None,
    ) -> None:
        # Validate the complete immutable outcome before acquiring an execution lock.
        if status not in ("ACCEPTED", "RUNNING", "SUCCEEDED", "FAILED"):
            raise StoreConflict("Unknown execution state")
        if status in ("SUCCEEDED", "FAILED"):
            if outcome is None or outcome.status != status:
                raise StoreConflict("Terminal outcome must match status")
            outcome = _OUTCOME.validate_python(outcome)
        elif outcome is not None:
            raise StoreConflict("Progress cannot carry an outcome")
        encoded = (
            canonical_json(outcome.model_dump(mode="json", exclude_unset=True))
            if outcome
            else None
        )
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                f"SELECT {_EXECUTION_COLUMNS} FROM generation_execution WHERE "
                f"user_id=%s AND task_id=%s FOR UPDATE",
                (user_id, task_id),
            )
            row = cursor.fetchone()
            if row is None:
                raise StoreConflict("Unknown execution")
            execution = ExecutionRecord.from_row(row)
            self._transition_locked(cursor, execution, status, outcome, encoded)
            connection.commit()

    def _transition_locked(
        self,
        cursor: pymysql.cursors.Cursor,
        execution: ExecutionRecord,
        status: ConnectorExecutionStatus,
        outcome: GenerationOutcome | None,
        encoded: str | None,
    ) -> None:
        if status == execution.status:
            existing = (
                canonical_json(
                    execution.outcome.model_dump(mode="json", exclude_unset=True)
                )
                if execution.outcome
                else None
            )
            if existing != encoded:
                raise StoreConflict("Immutable outcome conflict")
            return
        allowed = {
            "ACCEPTED": {"RUNNING", "FAILED"},
            "RUNNING": {"SUCCEEDED", "FAILED"},
        }
        if status not in allowed.get(execution.status, set()):
            raise StoreConflict("Illegal execution transition")
        event_id = self._event(cursor, execution, status, outcome)
        cursor.execute(
            "UPDATE generation_execution SET status=%s,outcome=%s,"
            "terminal_event_id=%s,updated_at=UTC_TIMESTAMP(6),"
            "started_at=CASE WHEN %s='RUNNING' THEN COALESCE(started_at,"
            "UTC_TIMESTAMP(6)) ELSE started_at END,finished_at=CASE WHEN "
            "%s IN ('SUCCEEDED','FAILED') THEN UTC_TIMESTAMP(6) ELSE "
            "finished_at END WHERE id=%s",
            (
                status,
                encoded,
                event_id if outcome else None,
                status,
                status,
                execution.id,
            ),
        )

    def claim_publication(self) -> Publication | None:
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute("""
                SELECT event_id,topic,partition_key,envelope FROM outbox_event e
                WHERE published_at IS NULL AND next_attempt_at<=UTC_TIMESTAMP(6)
                AND (lease_until IS NULL OR lease_until<=UTC_TIMESTAMP(6))
                AND NOT EXISTS (SELECT 1 FROM outbox_event prior
                    WHERE prior.user_id=e.user_id
                    AND prior.task_id=e.task_id
                    AND prior.sequence_no<e.sequence_no
                    AND prior.published_at IS NULL)
                ORDER BY created_at,event_id LIMIT 1 FOR UPDATE SKIP LOCKED""")
            row = cursor.fetchone()
            if row is None:
                return None
            token = str(uuid4())
            cursor.execute(
                "UPDATE outbox_event SET lease_token=%s,"
                "lease_until=UTC_TIMESTAMP(6)+INTERVAL 30 SECOND,"
                "attempts=attempts+1 WHERE event_id=%s",
                (token, row[0]),
            )
            connection.commit()
            return Publication(row[0], row[1], row[2], row[3].encode(), token)

    def publication_delivered(self, publication: Publication) -> bool:
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "UPDATE outbox_event SET published_at=UTC_TIMESTAMP(6),"
                "lease_token=NULL,lease_until=NULL,last_error=NULL WHERE "
                "event_id=%s AND lease_token=%s AND "
                "lease_until>UTC_TIMESTAMP(6)",
                (publication.event_id, publication.lease_token),
            )
            changed = cursor.rowcount == 1
            connection.commit()
            return changed

    def publication_failed(self, publication: Publication, error: str) -> bool:
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "UPDATE outbox_event SET lease_token=NULL,lease_until=NULL,"
                "last_error=%s,next_attempt_at=UTC_TIMESTAMP(6)+INTERVAL "
                "LEAST(60,POW(2,LEAST(attempts,6))) SECOND WHERE event_id=%s "
                "AND lease_token=%s AND lease_until>UTC_TIMESTAMP(6)",
                (error[:128], publication.event_id, publication.lease_token),
            )
            changed = cursor.rowcount == 1
            connection.commit()
            return changed

    def replay_terminal(self, user_id: str, task_id: str) -> None:
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT terminal_event_id FROM generation_execution WHERE "
                "user_id=%s AND task_id=%s FOR UPDATE",
                (user_id, task_id),
            )
            row = cursor.fetchone()
            if row is None or row[0] is None:
                raise StoreConflict("No original terminal event")
            cursor.execute(
                "UPDATE outbox_event SET published_at=NULL,lease_token=NULL,"
                "lease_until=NULL,next_attempt_at=UTC_TIMESTAMP(6) WHERE "
                "event_id=%s",
                (row[0],),
            )
            connection.commit()

    def receipt_error(self, record: InboundRecord) -> str | None:
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT error_code FROM kafka_receipt WHERE topic=%s AND "
                "partition_no=%s AND offset_no=%s",
                (record.topic, record.partition, record.offset),
            )
            row = cursor.fetchone()
            return row[0] if row else None

    def claim_execution(
        self,
        account_ref: str | None = None,
        *,
        normalized_routes: tuple[AdapterRegistration, ...] = (),
    ) -> ExecutionClaim | None:
        eligibility = (
            "(%s IS NULL OR submission_payload IS NULL OR "
            "(JSON_EXTRACT(submission_payload,'$.payload_type') IS NULL AND "
            "JSON_UNQUOTE(JSON_EXTRACT(submission_payload,'$.account_ref'))=%s)"
        )
        parameters: list[str | None] = [account_ref, account_ref]
        for route in normalized_routes:
            eligibility += (
                " OR (JSON_UNQUOTE(JSON_EXTRACT(submission_payload,'$.payload_type'))="
                "'normalized_prepared_v1' AND "
                "JSON_UNQUOTE(JSON_EXTRACT(submission_payload,'$.account_ref'))=%s AND "
                "JSON_EXTRACT(submission_payload,'$.binding')=CAST(%s AS JSON) AND "
                "JSON_EXTRACT(submission_payload,'$.request.provider_binding')="
                "CAST(%s AS JSON) AND "
                "JSON_EXTRACT(submission_payload,'$.request.model_selection')="
                "CAST(%s AS JSON) AND kind=%s AND "
                "JSON_UNQUOTE(JSON_EXTRACT(submission_payload,"
                "'$.request.parameters.generation_mode'))=%s AND "
                "JSON_EXTRACT(submission_payload,'$.request.schema_version')=1)"
            )
            binding = canonical_json(route.binding.model_dump(mode="json"))
            parameters.extend(
                (
                    route.account_ref,
                    binding,
                    binding,
                    canonical_json(route.model_selection.model_dump(mode="json")),
                    route.media_type,
                    route.media_type,
                )
            )
        eligibility += ") "
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                f"SELECT {_WORK_COLUMNS} FROM generation_execution "
                "WHERE status IN ('ACCEPTED','RUNNING') "
                "AND submission_started_at IS NULL AND provider_receipt IS NULL "
                "AND provider_external_id IS NULL "
                "AND (execution_lease_until IS NULL "
                "OR execution_lease_until<=UTC_TIMESTAMP(6)) "
                "AND " + eligibility + "ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED",
                tuple(parameters),
            )
            row = cursor.fetchone()
            if not row:
                return None
            work = ExecutionWork.from_row(row)
            token = str(uuid4())
            cursor.execute(
                "UPDATE generation_execution SET execution_lease_token=%s,"
                "execution_lease_until=UTC_TIMESTAMP(6)+INTERVAL 900 SECOND "
                "WHERE id=%s",
                (token, work.execution.id),
            )
            connection.commit()
            return ExecutionClaim(work, token)

    def _claimed_work(
        self, cursor: pymysql.cursors.Cursor, claim: ExecutionClaim
    ) -> ExecutionWork | None:
        cursor.execute(
            f"SELECT {_WORK_COLUMNS} FROM generation_execution "
            "WHERE id=%s AND execution_lease_token=%s "
            "AND execution_lease_until>UTC_TIMESTAMP(6) FOR UPDATE",
            (claim.work.execution.id, claim.token),
        )
        row = cursor.fetchone()
        return ExecutionWork.from_row(row) if row else None

    def prepare_submission(
        self,
        claim: ExecutionClaim,
        payload: ProviderSubmissionPayload | NormalizedPreparedSubmission,
    ) -> bool:
        encoded = canonical_json(payload.model_dump(mode="json"))
        with self.connection() as connection, connection.cursor() as cursor:
            work = self._claimed_work(cursor, claim)
            if (
                not work
                or work.submission_started_at
                or work.execution.status not in ("ACCEPTED", "RUNNING")
            ):
                return False
            if payload.kind != work.execution.kind:
                raise StoreConflict("Submission kind differs from accepted request")
            if work.submission_payload:
                if (
                    canonical_json(work.submission_payload.model_dump(mode="json"))
                    != encoded
                ):
                    raise StoreConflict("Prepared submission is immutable")
            else:
                cursor.execute(
                    "UPDATE generation_execution SET submission_payload=%s WHERE id=%s",
                    (encoded, work.execution.id),
                )
            if work.execution.status == "ACCEPTED":
                self._transition_locked(cursor, work.execution, "RUNNING", None, None)
            connection.commit()
            return True

    def begin_submission(self, claim: ExecutionClaim) -> bool:
        # Persist first; neither lease expiry nor missing evidence permits
        # a second POST.
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "UPDATE generation_execution SET "
                "submission_started_at=UTC_TIMESTAMP(6),"
                "updated_at=UTC_TIMESTAMP(6) WHERE id=%s AND "
                "status='RUNNING' AND submission_payload IS NOT NULL AND "
                "submission_started_at IS NULL AND execution_lease_token=%s "
                "AND execution_lease_until>UTC_TIMESTAMP(6)",
                (claim.work.execution.id, claim.token),
            )
            changed = cursor.rowcount == 1
            connection.commit()
            return changed

    def release_execution(self, claim: ExecutionClaim) -> bool:
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "UPDATE generation_execution SET execution_lease_token=NULL,"
                "execution_lease_until=NULL WHERE id=%s AND "
                "execution_lease_token=%s",
                (claim.work.execution.id, claim.token),
            )
            changed = cursor.rowcount == 1
            connection.commit()
            return changed

    def renew_execution(self, claim: ExecutionClaim) -> bool:
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "UPDATE generation_execution SET "
                "execution_lease_until=UTC_TIMESTAMP(6)+INTERVAL 900 SECOND "
                "WHERE id=%s AND status='RUNNING' AND "
                "execution_lease_token=%s AND "
                "execution_lease_until>UTC_TIMESTAMP(6)",
                (claim.work.execution.id, claim.token),
            )
            changed = cursor.rowcount == 1
            connection.commit()
            return changed

    def fail_execution(self, claim: ExecutionClaim, error: GenerationError) -> bool:
        with self.connection() as connection, connection.cursor() as cursor:
            work = self._claimed_work(cursor, claim)
            if (
                not work
                or work.submission_started_at
                or work.execution.status not in ("ACCEPTED", "RUNNING")
            ):
                return False
            if error.stage != "VALIDATION":
                raise StoreConflict("Preparation failure requires validation diagnosis")
            outcome = GenerationFailureOutcome(status="FAILED", error=error)
            self._transition_locked(
                cursor,
                work.execution,
                "FAILED",
                outcome,
                canonical_json(outcome.model_dump(mode="json", exclude_unset=True)),
            )
            cursor.execute(
                "UPDATE generation_execution SET execution_lease_token=NULL,"
                "execution_lease_until=NULL WHERE id=%s",
                (work.execution.id,),
            )
            connection.commit()
            return True

    def reconciliation_work(self, user_id: str, task_id: str) -> ExecutionWork | None:
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                f"SELECT {_WORK_COLUMNS} FROM generation_execution "
                "WHERE user_id=%s AND task_id=%s",
                (user_id, task_id),
            )
            row = cursor.fetchone()
            return ExecutionWork.from_row(row) if row else None

    def unresolved_provider_evidence(
        self, accounts: tuple[str, ...]
    ) -> tuple[OriginalEvidenceView, ...]:
        from lux_model_connector.reconciliation import original_view

        if not accounts:
            return ()
        placeholders = ",".join("%s" for _ in accounts)
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                f"SELECT {_WORK_COLUMNS} FROM generation_execution "
                "WHERE status='RUNNING' AND submission_started_at IS NOT NULL "
                "AND submission_payload IS NOT NULL AND provider_external_id "
                "IS NULL AND provider_receipt IS NULL "
                "AND JSON_UNQUOTE(JSON_EXTRACT(submission_payload,'$.account_ref')) "
                f"IN ({placeholders}) ORDER BY id LIMIT 128",
                accounts,
            )
            return tuple(
                original_view(work)
                for row in cursor.fetchall()
                if (work := ExecutionWork.from_row(row)).submission_payload.account_ref
                in accounts
            )

    def attach_provider_evidence(
        self,
        evidence: OriginalProviderEvidence,
        operator: str,
        accounts: tuple[str, ...],
    ) -> OriginalEvidenceView:
        from lux_model_connector.reconciliation import (
            digest,
            original_image_receipt,
            original_view,
        )

        if not operator.strip():
            raise StoreConflict("Trusted operator is required")
        binding = evidence.binding
        encoded_evidence = canonical_json(evidence.model_dump(mode="json"))
        evidence_digest = digest(encoded_evidence)
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                f"SELECT {_WORK_COLUMNS} FROM generation_execution "
                "WHERE user_id=%s AND task_id=%s FOR UPDATE",
                (binding.user_id, binding.task_id),
            )
            row = cursor.fetchone()
            if row is None:
                raise StoreConflict("Unknown original execution")
            work = ExecutionWork.from_row(row)
            if (
                work.submission_payload is None
                or original_view(work).binding != binding
                or binding.account_ref not in accounts
            ):
                raise StoreConflict("Original submission binding differs")
            cursor.execute(
                "SELECT evidence_digest FROM provider_reconciliation_audit "
                "WHERE execution_id=%s",
                (work.execution.id,),
            )
            audit = cursor.fetchone()
            if audit is not None:
                if audit[0] != evidence_digest:
                    raise StoreConflict("Original evidence is immutable")
                connection.commit()
                return original_view(work)
            if (
                work.execution.status != "RUNNING"
                or work.submission_started_at is None
                or work.provider_external_id is not None
                or work.provider_receipt is not None
            ):
                raise StoreConflict(
                    "Only unresolved original submission evidence can be attached"
                )
            receipt = (
                original_image_receipt(work.submission_payload, evidence.image_receipt)
                if evidence.image_receipt is not None
                else None
            )
            encoded_receipt = (
                canonical_json(
                    receipt.model_dump(
                        mode="json",
                        exclude_unset=not isinstance(receipt, NormalizedReceipt),
                    )
                )
                if receipt
                else None
            )
            if not self._save_provider_receipt_locked(
                cursor, work, evidence.external_id, receipt, encoded_receipt
            ):
                raise StoreConflict("Original evidence was not saved")
            cursor.execute(
                "INSERT INTO provider_reconciliation_audit "
                "(execution_id,operator_id,evidence_digest,attachment) "
                "VALUES (%s,%s,%s,%s)",
                (work.execution.id, operator, evidence_digest, encoded_evidence),
            )
            cursor.execute(
                f"SELECT {_WORK_COLUMNS} FROM generation_execution WHERE id=%s",
                (work.execution.id,),
            )
            view = original_view(ExecutionWork.from_row(cursor.fetchone()))
            connection.commit()
            return view

    def save_provider_receipt(
        self,
        user_id: str,
        task_id: str,
        external_id: str | None,
        receipt: SeedreamReceipt | SeedanceReceipt | NormalizedReceipt | None,
    ) -> bool:
        encoded = (
            canonical_json(
                receipt.model_dump(
                    mode="json",
                    exclude_unset=not isinstance(receipt, NormalizedReceipt),
                )
            )
            if receipt
            else None
        )
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                f"SELECT {_WORK_COLUMNS} FROM generation_execution "
                "WHERE user_id=%s AND task_id=%s FOR UPDATE",
                (user_id, task_id),
            )
            row = cursor.fetchone()
            if not row:
                raise StoreConflict("Unknown provider execution")
            work = ExecutionWork.from_row(row)
            result = self._save_provider_receipt_locked(
                cursor, work, external_id, receipt, encoded
            )
            connection.commit()
            return result

    def _save_provider_receipt_locked(
        self,
        cursor: pymysql.cursors.Cursor,
        work: ExecutionWork,
        external_id: str | None,
        receipt: SeedreamReceipt | SeedanceReceipt | NormalizedReceipt | None,
        encoded: str | None,
    ) -> bool:
        if work.execution.status != "RUNNING":
            return False
        if not work.submission_payload or work.submission_started_at is None:
            raise StoreConflict("Receipt requires a marked original submission")
        if external_id is not None:
            if work.execution.kind != "VIDEO" or not re.fullmatch(
                r"[A-Za-z0-9_-]{1,1024}", external_id
            ):
                raise StoreConflict("Invalid external video task ID")
            if work.provider_external_id not in (None, external_id):
                raise StoreConflict("Original external ID is immutable")
        if receipt is not None:
            expected_type = (
                NormalizedReceipt
                if isinstance(work.submission_payload, NormalizedPreparedSubmission)
                else SeedreamReceipt
                if work.execution.kind == "IMAGE"
                else SeedanceReceipt
            )
            if not isinstance(receipt, expected_type):
                raise StoreConflict("Receipt kind differs")
            if isinstance(receipt, SeedanceReceipt):
                if receipt.id != (external_id or work.provider_external_id):
                    raise StoreConflict("Video receipt task differs")
                if receipt.status in ("queued", "running"):
                    return False
                if receipt.status == "succeeded":
                    content = receipt.content
                    requested_last_frame = (
                        work.submission_payload.request.return_last_frame
                    )
                    if (
                        not content
                        or not content.video_url
                        or (requested_last_frame and not content.last_frame_url)
                    ):
                        return False
            if (
                work.provider_receipt
                and canonical_json(
                    work.provider_receipt.model_dump(
                        mode="json",
                        exclude_unset=not isinstance(receipt, NormalizedReceipt),
                    )
                )
                != encoded
            ):
                if isinstance(receipt, SeedanceReceipt):
                    return False
                raise StoreConflict("Original provider receipt is immutable")
        first_evidence = (
            work.provider_external_id is None and external_id is not None
        ) or (work.provider_receipt is None and receipt is not None)
        cursor.execute(
            "UPDATE generation_execution SET "
            "provider_external_id=COALESCE(provider_external_id,%s),"
            "provider_receipt=COALESCE(provider_receipt,%s),"
            "updated_at=UTC_TIMESTAMP(6),execution_lease_token=CASE WHEN "
            "%s THEN NULL ELSE execution_lease_token END,"
            "execution_lease_until=CASE WHEN %s THEN NULL ELSE "
            "execution_lease_until END WHERE id=%s",
            (
                external_id,
                encoded,
                first_evidence,
                first_evidence,
                work.execution.id,
            ),
        )
        return True

    def save_submission_failure(
        self, user_id: str, task_id: str, error: GenerationError
    ) -> bool:
        outcome = GenerationFailureOutcome(status="FAILED", error=error)
        encoded = canonical_json(outcome.model_dump(mode="json", exclude_unset=True))
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                f"SELECT {_WORK_COLUMNS} FROM generation_execution "
                "WHERE user_id=%s AND task_id=%s FOR UPDATE",
                (user_id, task_id),
            )
            row = cursor.fetchone()
            if not row:
                raise StoreConflict("Unknown provider execution")
            work = ExecutionWork.from_row(row)
            if work.execution.status == "FAILED":
                return (
                    canonical_json(
                        work.execution.outcome.model_dump(
                            mode="json", exclude_unset=True
                        )
                    )
                    == encoded
                )
            if work.execution.status != "RUNNING":
                return False
            if (
                error.stage != "PROVIDER"
                or not work.submission_payload
                or not work.submission_started_at
                or work.provider_receipt
                or work.provider_external_id
            ):
                raise StoreConflict(
                    "Rejection does not match a marked original submission"
                )
            self._transition_locked(cursor, work.execution, "FAILED", outcome, encoded)
            cursor.execute(
                "UPDATE generation_execution SET execution_lease_token=NULL,"
                "execution_lease_until=NULL WHERE id=%s",
                (work.execution.id,),
            )
            connection.commit()
            return True

    def scan_provider_work(
        self, after_id: int = 0, limit: int = 32
    ) -> tuple[ExecutionWork, ...]:
        if not 1 <= limit <= 128 or after_id < 0:
            raise ValueError("Invalid provider scan page")
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                f"SELECT {_WORK_COLUMNS} FROM generation_execution WHERE id>%s "
                "AND status='RUNNING' AND submission_payload IS NOT NULL "
                "AND (provider_external_id IS NOT NULL "
                "OR provider_receipt IS NOT NULL OR (submission_started_at IS NOT NULL "
                "AND JSON_UNQUOTE("
                "JSON_EXTRACT(submission_payload,'$.payload_type'))="
                "'normalized_prepared_v1')) "
                "ORDER BY id LIMIT %s",
                (after_id, limit),
            )
            return tuple(ExecutionWork.from_row(row) for row in cursor.fetchall())

    def claim_result(self, work: ExecutionWork) -> ExecutionClaim | None:
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                f"SELECT {_WORK_COLUMNS} FROM generation_execution WHERE id=%s "
                "AND status='RUNNING' AND provider_receipt IS NOT NULL "
                "AND (execution_lease_until IS NULL "
                "OR execution_lease_until<=UTC_TIMESTAMP(6)) "
                "FOR UPDATE",
                (work.execution.id,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            current = ExecutionWork.from_row(row)
            token = str(uuid4())
            cursor.execute(
                "UPDATE generation_execution SET execution_lease_token=%s,"
                "execution_lease_until=UTC_TIMESTAMP(6)+INTERVAL 900 SECOND "
                "WHERE id=%s",
                (token, current.execution.id),
            )
            connection.commit()
            return ExecutionClaim(current, token)

    def _saved_results(
        self, cursor: pymysql.cursors.Cursor, execution_id: int
    ) -> tuple[StoredResult, ...]:
        cursor.execute(
            "SELECT result_payload FROM generation_result WHERE "
            "execution_id=%s ORDER BY source_index",
            (execution_id,),
        )
        return tuple(
            StoredResult.model_validate_json(row[0]) for row in cursor.fetchall()
        )

    def saved_results(self, user_id: str, task_id: str) -> tuple[StoredResult, ...]:
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT id FROM generation_execution WHERE user_id=%s AND task_id=%s",
                (user_id, task_id),
            )
            row = cursor.fetchone()
            return self._saved_results(cursor, row[0]) if row else ()

    def save_result(self, claim: ExecutionClaim, result: StoredResult) -> bool:
        encoded = canonical_json(result.model_dump(mode="json"))
        with self.connection() as connection, connection.cursor() as cursor:
            work = self._claimed_work(cursor, claim)
            if (
                not work
                or work.execution.status != "RUNNING"
                or not work.provider_receipt
            ):
                return False
            cursor.execute(
                "SELECT result_payload FROM generation_result WHERE "
                "execution_id=%s AND source_index=%s FOR UPDATE",
                (work.execution.id, result.source_index),
            )
            row = cursor.fetchone()
            if row:
                if row[0] != encoded:
                    raise StoreConflict("Preserved result is immutable")
            else:
                cursor.execute(
                    "INSERT INTO generation_result(execution_id,source_index,"
                    "result_payload,download_token) VALUES (%s,%s,%s,%s)",
                    (
                        work.execution.id,
                        result.source_index,
                        encoded,
                        secrets.token_hex(32),
                    ),
                )
            connection.commit()
            return True

    def _result_sources(
        self, cursor: pymysql.cursors.Cursor, execution_id: int
    ) -> tuple[ResultSource, ...]:
        if self.result_base_url is None:
            raise StoreConflict("Stable connector result URL is not configured")
        cursor.execute(
            "SELECT source_index,result_payload,download_token FROM "
            "generation_result WHERE execution_id=%s ORDER BY "
            "source_index",
            (execution_id,),
        )
        rows = cursor.fetchall()
        if (
            not rows
            or len(rows) > 64
            or tuple(row[0] for row in rows) != tuple(range(len(rows)))
        ):
            raise StoreConflict("Preserved source manifest is incomplete")
        return tuple(
            ResultSource(
                url=f"{self.result_base_url}/internal/generation-results/{row[2]}",
                filename=StoredResult.model_validate_json(row[1]).filename,
                media_type=StoredResult.model_validate_json(row[1]).media_type,
            )
            for row in rows
        )

    def result_sources(self, claim: ExecutionClaim) -> tuple[ResultSource, ...]:
        with self.connection() as connection, connection.cursor() as cursor:
            work = self._claimed_work(cursor, claim)
            if not work or work.execution.status != "RUNNING":
                raise StoreConflict("Result claim expired")
            return self._result_sources(cursor, work.execution.id)

    def _expected_result_count(self, work: ExecutionWork) -> int:
        receipt = work.provider_receipt
        if isinstance(receipt, NormalizedReceipt):
            if receipt.status != "SUCCEEDED" or len(receipt.outputs) != 1:
                raise StoreConflict("Normalized success requires exactly one file")
            return 1
        if isinstance(receipt, SeedreamReceipt):
            if (
                receipt.error
                or not receipt.data
                or any(image.error for image in receipt.data)
            ):
                raise StoreConflict("Image generation did not complete every output")
            return len(receipt.data)
        if (
            isinstance(receipt, SeedanceReceipt)
            and receipt.status == "succeeded"
            and receipt.content
            and receipt.content.video_url
        ):
            if (
                work.submission_payload.request.return_last_frame
                and not receipt.content.last_frame_url
            ):
                raise StoreConflict("Requested last frame is missing")
            return 1 + int(receipt.content.last_frame_url is not None)
        raise StoreConflict("Provider success has not been confirmed")

    def validate_provider_capacity(self, max_response_bytes: int) -> None:
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT @@max_allowed_packet")
            if cursor.fetchone()[0] < 2 * max_response_bytes + 1024 * 1024:
                raise StoreConflict(
                    "MySQL max_allowed_packet cannot retain provider responses; "
                    "configure twice the response budget plus 1 MiB"
                )

    def complete_execution(
        self, claim: ExecutionClaim, outcome: GenerationOutcome
    ) -> bool:
        outcome = _OUTCOME.validate_python(outcome)
        encoded = canonical_json(outcome.model_dump(mode="json", exclude_unset=True))
        with self.connection() as connection, connection.cursor() as cursor:
            work = self._claimed_work(cursor, claim)
            if (
                not work
                or work.execution.status != "RUNNING"
                or not work.provider_receipt
            ):
                return False
            if isinstance(outcome, GenerationSuccessOutcome):
                sources = self._result_sources(cursor, work.execution.id)
                if outcome.sources != sources or len(
                    sources
                ) != self._expected_result_count(work):
                    raise StoreConflict(
                        "Success differs from complete preserved result manifest"
                    )
            self._transition_locked(
                cursor, work.execution, outcome.status, outcome, encoded
            )
            cursor.execute(
                "UPDATE generation_execution SET execution_lease_token=NULL,"
                "execution_lease_until=NULL WHERE id=%s",
                (work.execution.id,),
            )
            connection.commit()
            return True

    def download_result(self, token: str) -> StoredResult | None:
        if not re.fullmatch(r"[0-9a-f]{64}", token):
            return None
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT r.result_payload FROM generation_result r JOIN "
                "generation_execution e ON e.id=r.execution_id WHERE "
                "r.download_token=%s AND e.status='SUCCEEDED'",
                (token,),
            )
            row = cursor.fetchone()
            return StoredResult.model_validate_json(row[0]) if row else None
