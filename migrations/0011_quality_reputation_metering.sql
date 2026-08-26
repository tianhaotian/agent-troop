-- M9：Verifier 质量事实、信誉投影与内部权威计量。

BEGIN;

CREATE TABLE IF NOT EXISTS quality_records (
    artifact_id        TEXT PRIMARY KEY REFERENCES artifacts(id) ON DELETE CASCADE,
    mission_id         TEXT NOT NULL REFERENCES missions(id) ON DELETE CASCADE,
    subtask_id         TEXT REFERENCES subtasks(id) ON DELETE SET NULL,
    producer_agent_id  TEXT REFERENCES agents(id) ON DELETE SET NULL,
    producer_platform  TEXT NOT NULL DEFAULT '',
    attempt            INT NOT NULL DEFAULT 0,
    layers             JSONB NOT NULL DEFAULT '{}',
    score              DOUBLE PRECISION NOT NULL CHECK (score >= 0 AND score <= 1),
    confidence         DOUBLE PRECISION NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    verdict            TEXT NOT NULL CHECK (verdict IN ('accepted','rework','rejected')),
    failure_class      TEXT NOT NULL DEFAULT '',
    rubric             TEXT NOT NULL DEFAULT '',
    context_hash       TEXT NOT NULL DEFAULT '',
    verified_by        JSONB NOT NULL DEFAULT '[]',
    created_at         TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_quality_mission ON quality_records(mission_id, created_at);
CREATE INDEX IF NOT EXISTS idx_quality_producer ON quality_records(producer_agent_id, created_at);

CREATE TABLE IF NOT EXISTS reputation_signals (
    id          TEXT PRIMARY KEY,
    agent_id    TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    skill       TEXT NOT NULL,
    signal      JSONB NOT NULL,
    event_ref   TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_reputation_signals_agent_skill
    ON reputation_signals(agent_id, skill, created_at);

CREATE TABLE IF NOT EXISTS reputations (
    agent_id           TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    skill              TEXT NOT NULL,
    success_alpha      DOUBLE PRECISION NOT NULL DEFAULT 2,
    success_beta       DOUBLE PRECISION NOT NULL DEFAULT 2,
    quality_ewma       DOUBLE PRECISION NOT NULL DEFAULT 0.5,
    quality_samples    DOUBLE PRECISION NOT NULL DEFAULT 0,
    reliability_alpha  DOUBLE PRECISION NOT NULL DEFAULT 2,
    reliability_beta   DOUBLE PRECISION NOT NULL DEFAULT 2,
    latency_ewma_ms    DOUBLE PRECISION NOT NULL DEFAULT 0,
    cost_efficiency    DOUBLE PRECISION NOT NULL DEFAULT 0,
    samples            DOUBLE PRECISION NOT NULL DEFAULT 0,
    updated_at         TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (agent_id, skill)
);

CREATE TABLE IF NOT EXISTS meter_records (
    id           TEXT PRIMARY KEY,
    mission_id   TEXT NOT NULL REFERENCES missions(id) ON DELETE CASCADE,
    subtask_id   TEXT REFERENCES subtasks(id) ON DELETE SET NULL,
    agent_id     TEXT REFERENCES agents(id) ON DELETE SET NULL,
    resource     TEXT NOT NULL,
    quantity     DOUBLE PRECISION NOT NULL CHECK (quantity >= 0),
    unit         TEXT NOT NULL,
    trust        TEXT NOT NULL CHECK (trust IN ('authoritative','self_reported')),
    price_book   TEXT NOT NULL,
    unit_price   DOUBLE PRECISION NOT NULL CHECK (unit_price >= 0),
    credits      DOUBLE PRECISION NOT NULL CHECK (credits >= 0),
    metadata     JSONB NOT NULL DEFAULT '{}',
    recorded_at  TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_meter_mission ON meter_records(mission_id, recorded_at, id);
CREATE INDEX IF NOT EXISTS idx_meter_agent ON meter_records(agent_id, recorded_at);

COMMIT;
