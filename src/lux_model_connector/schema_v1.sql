CREATE TABLE IF NOT EXISTS connector_schema_version (
    singleton TINYINT PRIMARY KEY,
    owner VARCHAR(64) NOT NULL,
    version INT NOT NULL,
    CHECK (singleton = 1)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS generation_execution (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    session_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    task_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    operation_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    kind ENUM('IMAGE','VIDEO') NOT NULL,
    input_generation_payload LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    status ENUM('ACCEPTED','RUNNING','SUCCEEDED','FAILED') NOT NULL,
    submission_payload LONGTEXT NULL,
    submission_started_at DATETIME(6) NULL,
    provider_external_id VARCHAR(1024) NULL,
    provider_receipt LONGTEXT NULL,
    outcome LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL,
    terminal_event_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL,
    next_event_sequence BIGINT NOT NULL DEFAULT 1,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    started_at DATETIME(6) NULL,
    finished_at DATETIME(6) NULL,
    UNIQUE KEY execution_task (user_id, task_id),
    UNIQUE KEY execution_operation (user_id, operation_id),
    CHECK (JSON_VALID(input_generation_payload)),
    CHECK (outcome IS NULL OR JSON_VALID(outcome)),
    CHECK (next_event_sequence > 0),
    CHECK ((status IN ('SUCCEEDED','FAILED')) = (outcome IS NOT NULL)),
    CHECK ((status IN ('SUCCEEDED','FAILED')) = (terminal_event_id IS NOT NULL))
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS command_receipt (
    topic VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    event_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    command_payload LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    partition_key VARBINARY(256) NOT NULL,
    error_code VARCHAR(64) NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (topic, event_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS kafka_receipt (
    topic VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    partition_no INT NOT NULL,
    offset_no BIGINT NOT NULL,
    original_key LONGBLOB NULL,
    original_value LONGBLOB NULL,
    event_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL,
    error_code VARCHAR(64) NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (topic, partition_no, offset_no)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS outbox_event (
    event_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    execution_id BIGINT NOT NULL,
    user_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    task_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    sequence_no BIGINT NOT NULL,
    topic VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    partition_key VARBINARY(256) NOT NULL,
    envelope LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    attempts BIGINT NOT NULL DEFAULT 0,
    next_attempt_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    last_error VARCHAR(128) NULL,
    lease_token CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL,
    lease_until DATETIME(6) NULL,
    published_at DATETIME(6) NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    UNIQUE KEY execution_sequence (user_id, task_id, sequence_no),
    KEY publication_due (published_at, next_attempt_at),
    FOREIGN KEY (execution_id) REFERENCES generation_execution(id),
    CHECK (sequence_no > 0),
    CHECK (JSON_VALID(envelope))
) ENGINE=InnoDB;
