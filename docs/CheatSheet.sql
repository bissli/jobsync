-- ============================================================================
-- JobSync Cluster Monitoring Cheatsheet
-- ============================================================================
--
-- Quick reference for checking cluster health and status.
-- All queries use the default 'sync_' table prefix.
--
-- NOTE: If you customized 'appname' in CoordinationConfig, replace 'sync_'
--       with your prefix (e.g., 'myapp_') throughout these queries.
--
-- Usage:
--   Run the entire file:
--     psql -U postgres -d jobsync -f Cheatsheet.sql
--
--   Or copy individual queries to run them separately.
--
-- TABLE REFERENCE:
--   sync_node           - Worker registration and heartbeat
--   sync_token          - Token ownership assignments
--   sync_leader_lock    - Current leader election lock
--   sync_rebalance      - Token rebalancing audit log
--   sync_rebalance_lock - Rebalancing coordination lock
--   sync_lock           - Task pinning to specific nodes (by task_id)
--   sync_claim          - Tasks claimed by workers
--   sync_audit          - Completed task tracking
--   sync_check          - Task processing checkpoints
--   sync_checkpoint     - Job execution checkpoints
--
-- ============================================================================

-- Configure psql for better output
\set QUIET 1

-- Do not show query execution time
\timing off

-- Automatically use expanded display for wide results
\x auto

-- Use Unicode borders for tables
\pset linestyle unicode

-- Show NULL values clearly
\pset null '∅'

-- Format numbers with thousand separators
\pset numericlocale on

-- Disable footer (row count)
\pset footer off

\set QUIET 0

\echo ''
\echo ''
\echo '╔══════════════════════════════════════════════════════════╗'
\echo '║                   QUICK DIAGNOSTICS                      ║'
\echo '╚══════════════════════════════════════════════════════════╝'
\echo ''
\echo 'One-line summary of entire cluster state:'
\echo ''
SELECT 
    (SELECT COUNT(*) FROM sync_node WHERE last_heartbeat > NOW() - INTERVAL '15 seconds') as active_nodes,
    (SELECT COUNT(*) FROM sync_token) as total_tokens,
    (SELECT COUNT(*) FROM sync_lock) as active_locks,
    (SELECT COUNT(*) FROM sync_rebalance WHERE triggered_at > NOW() - INTERVAL '1 hour') as rebalances_1hr,
    (SELECT COUNT(DISTINCT item) FROM sync_claim WHERE created_on::date = CURRENT_DATE) as tasks_claimed_today,
    (SELECT COUNT(DISTINCT item) FROM sync_audit WHERE date = CURRENT_DATE) as tasks_completed_today;

\echo ''
\echo ''
\echo ''
\echo ''
\echo ''
\echo '╔══════════════════════════════════════════════════════════╗'
\echo '║                    CLUSTER HEALTH                        ║'
\echo '╚══════════════════════════════════════════════════════════╝'
\echo ''
\echo 'Worker processes registered with the cluster:'
\echo 'Shows registration time, last heartbeat, and health status'
\echo ''
SELECT 
    name,
    created_on,
    last_heartbeat,
    COALESCE(EXTRACT(EPOCH FROM (NOW() - last_heartbeat))::INTEGER, 0) as seconds_since_heartbeat,
    CASE 
        WHEN last_heartbeat > NOW() - INTERVAL '15 seconds' THEN '✓ HEALTHY'
        WHEN last_heartbeat > NOW() - INTERVAL '30 seconds' THEN '⚠ LAGGING'
        ELSE '✗ DEAD'
    END as status
FROM sync_node
ORDER BY created_on;

\echo ''
\echo 'Node Count Summary:'
SELECT 
    COUNT(*) as active_nodes,
    COUNT(*) FILTER (WHERE last_heartbeat > NOW() - INTERVAL '15 seconds') as healthy_nodes,
    COUNT(*) FILTER (WHERE last_heartbeat <= NOW() - INTERVAL '15 seconds') as dead_nodes
FROM sync_node;

\echo ''
\echo ''
\echo ''
\echo ''
\echo ''
\echo '╔══════════════════════════════════════════════════════════╗'
\echo '║                  TOKEN DISTRIBUTION                      ║'
\echo '╚══════════════════════════════════════════════════════════╝'
\echo ''
\echo 'Assigns ownership of token IDs to nodes:'
\echo 'Each task hashes to exactly one token, determining which node processes it'
\echo ''
SELECT 
    node,
    COUNT(*) as token_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) as percent,
    MIN(token_id) as min_token,
    MAX(token_id) as max_token,
    version
FROM sync_token
GROUP BY node, version
ORDER BY token_count DESC;

\echo ''
\echo 'Distribution Balance Analysis:'
\echo '(Imbalance should be <100 tokens for balanced cluster)'
WITH stats AS (
    SELECT COUNT(*) as token_count
    FROM sync_token
    GROUP BY node
)
SELECT 
    MIN(token_count) as min_tokens,
    MAX(token_count) as max_tokens,
    MAX(token_count) - MIN(token_count) as imbalance,
    ROUND(AVG(token_count)::numeric, 1) as avg_tokens,
    ROUND(STDDEV(token_count)::numeric, 1) as stddev_tokens,
    CASE 
        WHEN MAX(token_count) - MIN(token_count) < 100 THEN '✓ BALANCED'
        WHEN MAX(token_count) - MIN(token_count) < 500 THEN '⚠ SLIGHT IMBALANCE'
        ELSE '✗ IMBALANCED'
    END as status
FROM stats;

\echo ''
\echo ''
\echo ''
\echo ''
\echo ''
\echo '╔══════════════════════════════════════════════════════════╗'
\echo '║                     LEADER STATUS                        ║'
\echo '╚══════════════════════════════════════════════════════════╝'
\echo ''
\echo 'Manages cluster coordination and rebalancing:'
\echo 'Leader is the oldest registered node with active heartbeat'
\echo ''
SELECT 
    name as leader_node,
    created_on,
    last_heartbeat,
    EXTRACT(EPOCH FROM (NOW() - last_heartbeat))::INTEGER as seconds_since_heartbeat
FROM sync_node
WHERE last_heartbeat > NOW() - INTERVAL '15 seconds'
ORDER BY created_on, name
LIMIT 1;

\echo ''
\echo 'Leader Lock Status:'
\echo '(Shows if leader is currently performing token distribution)'
SELECT 
    node as leader,
    acquired_at,
    EXTRACT(EPOCH FROM (NOW() - acquired_at))::INTEGER as held_for_seconds
FROM sync_leader_lock;

\echo ''
\echo ''
\echo ''
\echo ''
\echo ''
\echo '╔══════════════════════════════════════════════════════════╗'
\echo '║                   TASK PROCESSING                        ║'
\echo '╚══════════════════════════════════════════════════════════╝'
\echo ''
\echo 'Tasks claimed by each node and completion tracking:'
\echo ''
SELECT 
    node,
    COUNT(*) as tasks_claimed,
    MIN(created_on) as first_claim,
    MAX(created_on) as last_claim,
    ROUND((COUNT(*)::NUMERIC / NULLIF(EXTRACT(EPOCH FROM (MAX(created_on) - MIN(created_on) + INTERVAL '1 second')), 0)) * 60, 1) 
        as claims_per_minute
FROM sync_claim
WHERE created_on::date = CURRENT_DATE
GROUP BY node
ORDER BY tasks_claimed DESC;

\echo ''
\echo 'Task Completion Rate (Today):'
\echo '(Claims are transient, cleaned on node exit; audit records are permanent)'
WITH claimed AS (
    SELECT COUNT(DISTINCT item) as count
    FROM sync_claim
    WHERE created_on::date = CURRENT_DATE
),
completed AS (
    SELECT COUNT(DISTINCT item) as count
    FROM sync_audit
    WHERE date = CURRENT_DATE
)
SELECT 
    claimed.count as claimed_tasks,
    completed.count as completed_tasks,
    CASE 
        WHEN claimed.count > 0 THEN claimed.count - completed.count
        ELSE NULL
    END as incomplete_tasks,
    ROUND(100.0 * completed.count / NULLIF(claimed.count, 0), 1) as completion_percent,
    CASE 
        WHEN claimed.count = 0 AND completed.count > 0 THEN '✓ NODES EXITED CLEANLY'
        WHEN claimed.count = 0 THEN '- NO ACTIVITY'
        WHEN claimed.count = completed.count THEN '✓ ALL COMPLETE'
        WHEN completed.count::float / NULLIF(claimed.count, 0) > 0.9 THEN '⚠ MOSTLY COMPLETE'
        ELSE '✗ MANY INCOMPLETE'
    END as status
FROM claimed, completed;

\echo ''
\echo 'Incomplete Tasks (First 20):'
\echo '(Tasks claimed but not yet completed - empty when nodes exit cleanly)'
SELECT 
    c.item as task_id,
    c.node,
    c.created_on as claimed_at,
    ROUND((EXTRACT(EPOCH FROM (NOW() - c.created_on))/60)::NUMERIC, 1) as minutes_ago
FROM sync_claim c
LEFT JOIN sync_audit a ON c.item = a.item AND a.date = CURRENT_DATE
WHERE c.created_on::date = CURRENT_DATE
  AND a.item IS NULL
ORDER BY c.created_on
LIMIT 20;

\echo ''
\echo ''
\echo ''
\echo ''
\echo ''
\echo '╔══════════════════════════════════════════════════════════╗'
\echo '║                      TASK LOCKS                          ║'
\echo '╚══════════════════════════════════════════════════════════╝'
\echo ''
\echo 'Pins specific tasks to specific node patterns:'
\echo 'Used for resource requirements (GPU, high memory, data locality)'
\echo ''
SELECT 
    node_patterns::text as patterns,
    COUNT(*) as locked_token_count,
    STRING_AGG(DISTINCT created_by, ', ') as created_by_nodes,
    MIN(created_at) as first_created,
    MAX(created_at) as last_created,
    reason
FROM sync_lock
GROUP BY node_patterns, reason
ORDER BY locked_token_count DESC;

\echo ''
\echo 'Lock Pattern Matching (First 20 Tasks):'
\echo '(Verify locked tasks are assigned to matching nodes)'
\echo '(node_patterns is JSONB array - patterns tried in order)'
WITH lock_patterns AS (
    SELECT 
        l.task_id,
        l.node_patterns::text as patterns_text,
        jsonb_array_elements_text(l.node_patterns) as pattern,
        l.reason,
        l.created_by
    FROM sync_lock l
)
SELECT 
    task_id,
    patterns_text,
    reason,
    created_by
FROM lock_patterns
ORDER BY task_id
LIMIT 20;

\echo ''
\echo 'Orphaned Locks:'
\echo '(Locks from nodes no longer in cluster - may need cleanup)'
SELECT 
    l.task_id,
    l.node_patterns,
    l.created_by,
    l.reason,
    l.created_at,
    ROUND((EXTRACT(EPOCH FROM (NOW() - l.created_at))/3600)::NUMERIC, 1) as age_hours
FROM sync_lock l
LEFT JOIN sync_node n ON l.created_by = n.name
WHERE n.name IS NULL 
   OR n.last_heartbeat < NOW() - INTERVAL '1 hour'
ORDER BY l.created_at;

\echo ''
\echo ''
\echo ''
\echo ''
\echo ''
\echo '╔══════════════════════════════════════════════════════════╗'
\echo '║                 REBALANCING HISTORY                      ║'
\echo '╚══════════════════════════════════════════════════════════╝'
\echo ''
\echo 'Audit log of token redistribution events:'
\echo 'Triggered when nodes join, leave, or fail'
\echo ''
SELECT 
    triggered_at,
    trigger_reason,
    leader_node,
    nodes_before,
    nodes_after,
    tokens_moved,
    duration_ms
FROM sync_rebalance
ORDER BY triggered_at DESC
LIMIT 10;

\echo ''
\echo 'Rebalance Frequency (Last Hour):'
\echo '(Should be <5 per hour in stable cluster)'
SELECT 
    COUNT(*) as rebalances_last_hour,
    CASE 
        WHEN COUNT(*) = 0 THEN '✓ STABLE'
        WHEN COUNT(*) <= 5 THEN '⚠ SOME ACTIVITY'
        ELSE '✗ UNSTABLE'
    END as status
FROM sync_rebalance
WHERE triggered_at > NOW() - INTERVAL '1 hour';

\echo ''
\echo 'Rebalances by Hour (Last 24 Hours):'
SELECT 
    DATE_TRUNC('hour', triggered_at) as hour,
    COUNT(*) as rebalance_count,
    STRING_AGG(trigger_reason, ', ' ORDER BY triggered_at) as reasons
FROM sync_rebalance
WHERE triggered_at > NOW() - INTERVAL '24 hours'
GROUP BY hour
ORDER BY hour DESC;
