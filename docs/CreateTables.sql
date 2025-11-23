-- ============================================================================
-- JobSync Database Schema Creation Script
-- ============================================================================
--
-- Creates all required tables for JobSync coordination system.
-- This script matches exactly what the application creates via schema.py.
--
-- NOTE: The default table prefix is 'sync_'. If you customized 'appname' in
--       CoordinationConfig, replace 'sync_' with your prefix throughout this
--       file (e.g., 'myapp_').
--
-- Usage:
--   Create all tables in new database:
--     psql -U postgres -d jobsync -f CreateTables.sql
--
--   Customize prefix before running:
--     sed 's/sync_/myapp_/g' CreateTables.sql | psql -U postgres -d jobsync
--
-- Safety:
--   All statements use "IF NOT EXISTS" - safe to run multiple times.
--   Will not drop existing tables or modify existing data.
--
-- TABLE REFERENCE:
--   sync_node           - Worker registration and heartbeat
--   sync_checkpoint     - Job execution checkpoints
--   sync_audit          - Completed task tracking
--   sync_claim          - Tasks claimed by workers
--   sync_token          - Token ownership assignments
--   sync_lock           - Task pinning to specific nodes (by task_id)
--   sync_leader_lock    - Current leader election lock
--   sync_rebalance_lock - Rebalancing coordination lock
--   sync_rebalance      - Token rebalancing audit log
--
-- ============================================================================

\set ON_ERROR_STOP on

\echo ''
\echo '╔══════════════════════════════════════════════════════════╗'
\echo '║          JobSync Database Schema Creation                ║'
\echo '╚══════════════════════════════════════════════════════════╝'
\echo ''

-- ============================================================================
-- CORE TABLES
-- ============================================================================
-- These tables support basic job tracking regardless of coordination mode

\echo 'Creating core tables...'
\echo ''

-- Node registration and heartbeat tracking
\echo '  → sync_node (worker registration and heartbeat)'
CREATE TABLE IF NOT EXISTS sync_node (
    name varchar NOT NULL,
    created_on timestamp with time zone NOT NULL,
    last_heartbeat timestamp with time zone,
    PRIMARY KEY (name)
);

CREATE INDEX IF NOT EXISTS idx_sync_node_heartbeat 
    ON sync_node(last_heartbeat);

-- Job execution checkpoints
\echo '  → sync_checkpoint (job execution checkpoints)'
CREATE TABLE IF NOT EXISTS sync_checkpoint (
    node varchar NOT NULL,
    created_on timestamp with time zone NOT NULL,
    PRIMARY KEY (node, created_on)
);

-- Completed task audit log
\echo '  → sync_audit (completed task tracking)'
CREATE TABLE IF NOT EXISTS sync_audit (
    created_on timestamp with time zone NOT NULL,
    node varchar NOT NULL,
    item varchar NOT NULL,
    date date NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sync_audit_date_item 
    ON sync_audit(date, item);

-- Task claim tracking
\echo '  → sync_claim (tasks claimed by workers)'
CREATE TABLE IF NOT EXISTS sync_claim (
    node varchar NOT NULL,
    item varchar NOT NULL,
    created_on timestamp with time zone NOT NULL,
    PRIMARY KEY (node, item)
);

\echo ''
\echo 'Core tables created successfully.'
\echo ''

-- ============================================================================
-- COORDINATION TABLES
-- ============================================================================
-- These tables enable distributed coordination and load balancing

\echo 'Creating coordination tables...'
\echo ''

-- Token ownership for task distribution
\echo '  → sync_token (token ownership assignments)'
CREATE TABLE IF NOT EXISTS sync_token (
    token_id integer NOT NULL,
    node varchar NOT NULL,
    assigned_at timestamp with time zone NOT NULL,
    version integer NOT NULL DEFAULT 1,
    PRIMARY KEY (token_id)
);

CREATE INDEX IF NOT EXISTS idx_sync_token_node 
    ON sync_token(node);

CREATE INDEX IF NOT EXISTS idx_sync_token_assigned 
    ON sync_token(assigned_at);

CREATE INDEX IF NOT EXISTS idx_sync_token_version 
    ON sync_token(version);

-- Task locking to specific node patterns
\echo '  → sync_lock (task pinning to specific nodes)'
CREATE TABLE IF NOT EXISTS sync_lock (
    task_id varchar NOT NULL,
    node_patterns jsonb NOT NULL,
    reason varchar,
    created_at timestamp with time zone NOT NULL,
    created_by varchar NOT NULL,
    expires_at timestamp with time zone,
    PRIMARY KEY (task_id)
);

CREATE INDEX IF NOT EXISTS idx_sync_lock_created_by 
    ON sync_lock(created_by);

CREATE INDEX IF NOT EXISTS idx_sync_lock_expires 
    ON sync_lock(expires_at) 
    WHERE expires_at IS NOT NULL;

-- Leader election lock (singleton table)
\echo '  → sync_leader_lock (leader election lock)'
CREATE TABLE IF NOT EXISTS sync_leader_lock (
    singleton integer PRIMARY KEY DEFAULT 1,
    node varchar NOT NULL,
    acquired_at timestamp with time zone NOT NULL,
    operation varchar NOT NULL,
    CHECK (singleton = 1)
);

-- Rebalancing coordination lock (singleton table)
\echo '  → sync_rebalance_lock (rebalancing coordination lock)'
CREATE TABLE IF NOT EXISTS sync_rebalance_lock (
    singleton integer PRIMARY KEY DEFAULT 1,
    in_progress boolean NOT NULL DEFAULT false,
    started_at timestamp with time zone,
    started_by varchar,
    CHECK (singleton = 1)
);

-- Initialize rebalance lock
\echo '  → Initializing sync_rebalance_lock'
INSERT INTO sync_rebalance_lock (singleton, in_progress)
VALUES (1, false)
ON CONFLICT (singleton) DO NOTHING;

-- Rebalancing audit log
\echo '  → sync_rebalance (token rebalancing audit log)'
CREATE TABLE IF NOT EXISTS sync_rebalance (
    id serial PRIMARY KEY,
    triggered_at timestamp with time zone NOT NULL,
    trigger_reason varchar NOT NULL,
    leader_node varchar NOT NULL,
    nodes_before integer NOT NULL,
    nodes_after integer NOT NULL,
    tokens_moved integer NOT NULL,
    duration_ms integer
);

CREATE INDEX IF NOT EXISTS idx_sync_rebalance_triggered 
    ON sync_rebalance(triggered_at DESC);

\echo ''
\echo 'Coordination tables created successfully.'
\echo ''

-- ============================================================================
-- VERIFICATION
-- ============================================================================

\echo '╔══════════════════════════════════════════════════════════╗'
\echo '║                Schema Verification                       ║'
\echo '╚══════════════════════════════════════════════════════════╝'
\echo ''

\echo 'Verifying all tables exist...'
\echo ''

SELECT 
    table_name,
    CASE 
        WHEN table_name IN (
            'sync_node', 'sync_checkpoint', 'sync_audit', 'sync_claim',
            'sync_token', 'sync_lock', 'sync_leader_lock', 
            'sync_rebalance_lock', 'sync_rebalance'
        ) THEN '✓'
        ELSE '✗'
    END as status
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name LIKE 'sync_%'
ORDER BY table_name;

\echo ''
\echo 'Schema creation complete!'
\echo ''
\echo 'Next steps:'
\echo '  1. Verify tables were created: \dt sync_*'
\echo '  2. Check indexes: \di sync_*'
\echo '  3. Start your JobSync workers'
\echo ''
