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
 \set
on_error_stop on \echo '' \echo '╔══════════════════════════════════════════════════════════╗' \echo '║          JobSync Database Schema Creation                ║' \echo '╚══════════════════════════════════════════════════════════╝' \echo '' -- ============================================================================
-- CORE TABLES
-- ============================================================================
-- These tables support basic job tracking regardless of coordination mode
 \echo 'Creating core tables...' \echo '' -- Node registration and heartbeat tracking
\echo '  → sync_node (worker registration and heartbeat)'
create table if not exists sync_node (name varchar not null, created_on timestamp with time zone not null, last_heartbeat timestamp with time zone, primary key (name));


create index if not exists idx_sync_node_heartbeat on sync_node(last_heartbeat);

-- Job execution checkpoints
\echo '  → sync_checkpoint (job execution checkpoints)'
create table if not exists sync_checkpoint (node varchar not null, created_on timestamp with time zone not null, primary key (node, created_on));

-- Completed task audit log
\echo '  → sync_audit (completed task tracking)'
create table if not exists sync_audit (created_on timestamp with time zone not null, node varchar not null, task_id varchar not null, date date not null);


create index if not exists idx_sync_audit_date_task_id on sync_audit(date, task_id);

-- Task claim tracking
\echo '  → sync_claim (tasks claimed by workers)'
create table if not exists sync_claim (node varchar not null, task_id varchar not null, created_on timestamp with time zone not null, primary key (node, task_id));

\echo '' \echo 'Core tables created successfully.' \echo '' -- ============================================================================
-- COORDINATION TABLES
-- ============================================================================
-- These tables enable distributed coordination and load balancing
 \echo 'Creating coordination tables...' \echo '' -- Token ownership for task distribution
\echo '  → sync_token (token ownership assignments)'
create table if not exists sync_token (token_id integer not null, node varchar not null, assigned_at timestamp with time zone not null, version integer not null default 1, primary key (token_id));


create index if not exists idx_sync_token_node on sync_token(node);


create index if not exists idx_sync_token_assigned on sync_token(assigned_at);


create index if not exists idx_sync_token_version on sync_token(version);

-- Task locking to specific node patterns
\echo '  → sync_lock (task pinning to specific nodes)'
create table if not exists sync_lock (task_id varchar not null, node_patterns jsonb not null, reason varchar, created_at timestamp with time zone not null, created_by varchar not null, expires_at timestamp with time zone, primary key (task_id));


create index if not exists idx_sync_lock_created_by on sync_lock(created_by);


create index if not exists idx_sync_lock_expires on sync_lock(expires_at)
where expires_at is not null;

-- Leader election lock (singleton table)
\echo '  → sync_leader_lock (leader election lock)'
create table if not exists sync_leader_lock (singleton integer primary key default 1, node varchar not null, acquired_at timestamp with time zone not null, operation varchar not null, check (singleton = 1));

-- Rebalancing coordination lock (singleton table)
\echo '  → sync_rebalance_lock (rebalancing coordination lock)'
create table if not exists sync_rebalance_lock (singleton integer primary key default 1, in_progress boolean not null default false, started_at timestamp with time zone, started_by varchar, check (singleton = 1));

-- Initialize rebalance lock
\echo '  → Initializing sync_rebalance_lock'
insert into sync_rebalance_lock (singleton, in_progress)
values (1, false) on conflict (singleton) do nothing;

-- Rebalancing audit log
\echo '  → sync_rebalance (token rebalancing audit log)'
create table if not exists sync_rebalance (id serial primary key, triggered_at timestamp with time zone not null, trigger_reason varchar not null, leader_node varchar not null, nodes_before integer not null, nodes_after integer not null, tokens_moved integer not null, duration_ms integer);


create index if not exists idx_sync_rebalance_triggered on sync_rebalance(triggered_at desc);

\echo '' \echo 'Coordination tables created successfully.' \echo '' -- ============================================================================
-- VERIFICATION
-- ============================================================================
 \echo '╔══════════════════════════════════════════════════════════╗' \echo '║                Schema Verification                       ║' \echo '╚══════════════════════════════════════════════════════════╝' \echo '' \echo 'Verifying all tables exist...' \echo ''
select
    table_name,
    case
        when table_name IN (
                                'sync_node',
                                'sync_checkpoint',
                                'sync_audit',
                                'sync_claim',
                                'sync_token',
                                'sync_lock',
                                'sync_leader_lock',
                                'sync_rebalance_lock',
                                'sync_rebalance') then '✓'
        else '✗'
    end as status
from information_schema.tables
where table_schema = 'public'
    and table_name LIKE 'sync_%'
order by table_name;

\echo '' \echo 'Schema creation complete!' \echo '' \echo 'Next steps:' \echo '  1. Verify tables were created: \dt sync_*' \echo '  2. Check indexes: \di sync_*' \echo '  3. Start your JobSync workers' \echo ''
