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
\set quiet 1 -- Do not show query execution time
\timing off -- Automatically use expanded display for wide results
\x auto -- Use Unicode borders for tables
\pset linestyle unicode -- Show NULL values clearly
\pset null '∅' -- Format numbers with thousand separators
\pset numericlocale on -- Disable footer (row count)
\pset footer off \set quiet 0 \echo '' \echo '' \echo '╔══════════════════════════════════════════════════════════╗' \echo '║                   QUICK DIAGNOSTICS                      ║' \echo '╚══════════════════════════════════════════════════════════╝' \echo '' \echo 'One-line summary of entire cluster state:' \echo ''
select

    (select count(*)
     from sync_node
     where last_heartbeat > now() - INTERVAL '15 seconds') as active_nodes,

    (select count(*)
     from sync_token) as total_tokens,

    (select count(*)
     from sync_lock) as active_locks,

    (select count(*)
     from sync_rebalance
     where triggered_at > now() - INTERVAL '1 hour') as rebalances_1hr,

    (select count(distinct task_id)
     from sync_claim
     where created_on::date = current_date) as tasks_claimed_today,

    (select count(distinct task_id)
     from sync_audit
     where date = current_date) as tasks_completed_today;

\echo '' \echo '' \echo '' \echo '' \echo '' \echo '╔══════════════════════════════════════════════════════════╗' \echo '║                    CLUSTER HEALTH                        ║' \echo '╚══════════════════════════════════════════════════════════╝' \echo '' \echo 'Worker processes registered with the cluster:' \echo 'Shows registration time, last heartbeat, and health status' \echo ''
select
    name,
    created_on,
    last_heartbeat,
    coalesce(extract(epoch
                     from (now() - last_heartbeat))::INTEGER, 0) as seconds_since_heartbeat,
    case
        when last_heartbeat > now() - INTERVAL '15 seconds' then '✓ HEALTHY'
        when last_heartbeat > now() - INTERVAL '30 seconds' then '⚠ LAGGING'
        else '✗ DEAD'
    end as status
from sync_node
order by created_on;

\echo '' \echo 'Node Count Summary:'
select
    count(*) as active_nodes,
    count(*) filter (
                     where last_heartbeat > now() - INTERVAL '15 seconds') as healthy_nodes,
    count(*) filter (
                     where last_heartbeat <= now() - INTERVAL '15 seconds') as dead_nodes
from sync_node;

\echo '' \echo '' \echo '' \echo '' \echo '' \echo '╔══════════════════════════════════════════════════════════╗' \echo '║                  TOKEN DISTRIBUTION                      ║' \echo '╚══════════════════════════════════════════════════════════╝' \echo '' \echo 'Assigns ownership of token IDs to nodes:' \echo 'Each task hashes to exactly one token, determining which node processes it' \echo ''
select
    node,
    count(*) as token_count,
    round(100.0 * count(*) / sum(count(*)) over (), 1) as percent,
    min(token_id) as min_token,
    max(token_id) as max_token,
    version
from sync_token
group by
    node,
    version
order by token_count desc;

\echo '' \echo 'Distribution Balance Analysis:' \echo '(Imbalance should be <100 tokens for balanced cluster)' with stats as
    (select count(*) as token_count
     from sync_token
     group by node)
select
    min(token_count) as min_tokens,
    max(token_count) as max_tokens,
    max(token_count) - min(token_count) as imbalance,
    round(avg(token_count)::numeric, 1) as avg_tokens,
    round(stddev(token_count)::numeric, 1) as stddev_tokens,
    case
        when max(token_count) - min(token_count) < 100 then '✓ BALANCED'
        when max(token_count) - min(token_count) < 500 then '⚠ SLIGHT IMBALANCE'
        else '✗ IMBALANCED'
    end as status
from stats;

\echo '' \echo '' \echo '' \echo '' \echo '' \echo '╔══════════════════════════════════════════════════════════╗' \echo '║                     LEADER STATUS                        ║' \echo '╚══════════════════════════════════════════════════════════╝' \echo '' \echo 'Manages cluster coordination and rebalancing:' \echo 'Leader is the oldest registered node with active heartbeat' \echo ''
select
    name as leader_node,
    created_on,
    last_heartbeat,
    extract(epoch
            from (now() - last_heartbeat))::INTEGER as seconds_since_heartbeat
from sync_node
where last_heartbeat > now() - INTERVAL '15 seconds'
order by
    created_on,
    name
limit 1;

\echo '' \echo 'Leader Lock Status:' \echo '(Shows if leader is currently performing token distribution)'
select
    node as leader,
    acquired_at,
    extract(epoch
            from (now() - acquired_at))::INTEGER as held_for_seconds
from sync_leader_lock;

\echo '' \echo '' \echo '' \echo '' \echo '' \echo '╔══════════════════════════════════════════════════════════╗' \echo '║                   TASK PROCESSING                        ║' \echo '╚══════════════════════════════════════════════════════════╝' \echo '' \echo 'Tasks claimed by each node and completion tracking:' \echo ''
select
    node,
    count(*) as tasks_claimed,
    min(created_on) as first_claim,
    max(created_on) as last_claim,
    round((count(*)::NUMERIC / nullif(extract(epoch
                                              from (max(created_on) - min(created_on) + INTERVAL '1 second')), 0)) * 60, 1) as claims_per_minute
from sync_claim
where created_on::date = current_date
group by node
order by tasks_claimed desc;

\echo '' \echo 'Task Completion Rate (Today):' \echo '(Claims are transient, cleaned on node exit; audit records are permanent)' with
    claimed as
    (select count(distinct task_id) as count
     from sync_claim
     where created_on::date = current_date ),
    completed as
    (select count(distinct task_id) as count
     from sync_audit
     where date = current_date )
select
    claimed.count as claimed_tasks,
    completed.count as completed_tasks,
    case
        when claimed.count > 0 then claimed.count - completed.count
        else null
    end as incomplete_tasks,
    round(100.0 * completed.count / nullif(claimed.count, 0), 1) as completion_percent,
    case
        when claimed.count = 0
             and completed.count > 0 then '✓ NODES EXITED CLEANLY'
        when claimed.count = 0 then '- NO ACTIVITY'
        when claimed.count = completed.count then '✓ ALL COMPLETE'
        when completed.count::float / nullif(claimed.count, 0) > 0.9 then '⚠ MOSTLY COMPLETE'
        else '✗ MANY INCOMPLETE'
    end as status
from
    claimed,
    completed;

\echo '' \echo 'Incomplete Tasks (First 20):' \echo '(Tasks claimed but not yet completed - empty when nodes exit cleanly)'
select
    c.task_id,
    c.node,
    c.created_on as claimed_at,
    round((extract(epoch
                   from (now() - c.created_on))/60)::NUMERIC, 1) as minutes_ago
from sync_claim c
left join sync_audit a on c.task_id = a.task_id
and a.date = current_date
where c.created_on::date = current_date
    and a.task_id is null
order by c.created_on
limit 20;

\echo '' \echo '' \echo '' \echo '' \echo '' \echo '╔══════════════════════════════════════════════════════════╗' \echo '║                      TASK LOCKS                          ║' \echo '╚══════════════════════════════════════════════════════════╝' \echo '' \echo 'Pins specific tasks to specific node patterns:' \echo 'Used for resource requirements (GPU, high memory, data locality)' \echo ''
select
    node_patterns::text as patterns,
    count(*) as locked_token_count,
    string_agg(distinct created_by, ', ') as created_by_nodes,
    min(created_at) as first_created,
    max(created_at) as last_created,
    reason
from sync_lock
group by
    node_patterns,
    reason
order by locked_token_count desc;

\echo '' \echo 'Lock Pattern Matching (First 20 Tasks):' \echo '(Verify locked tasks are assigned to matching nodes)' \echo '(node_patterns is JSONB array - patterns tried in order)' with lock_patterns as
    (select
         l.task_id,
         l.node_patterns::text as patterns_text,
         jsonb_array_elements_text(l.node_patterns) as pattern,
         l.reason,
         l.created_by
     from sync_lock l)
select
    task_id,
    patterns_text,
    reason,
    created_by
from lock_patterns
order by task_id
limit 20;

\echo '' \echo 'Orphaned Locks:' \echo '(Locks from nodes no longer in cluster - may need cleanup)'
select
    l.task_id,
    l.node_patterns,
    l.created_by,
    l.reason,
    l.created_at,
    round((extract(epoch
                   from (now() - l.created_at))/3600)::NUMERIC, 1) as age_hours
from sync_lock l
left join sync_node n on l.created_by = n.name
where n.name is null
    or n.last_heartbeat < now() - INTERVAL '1 hour'
order by l.created_at;

\echo '' \echo '' \echo '' \echo '' \echo '' \echo '╔══════════════════════════════════════════════════════════╗' \echo '║                 REBALANCING HISTORY                      ║' \echo '╚══════════════════════════════════════════════════════════╝' \echo '' \echo 'Audit log of token redistribution events:' \echo 'Triggered when nodes join, leave, or fail' \echo ''
select
    triggered_at,
    trigger_reason,
    leader_node,
    nodes_before,
    nodes_after,
    tokens_moved,
    duration_ms
from sync_rebalance
order by triggered_at desc
limit 10;

\echo '' \echo 'Rebalance Frequency (Last Hour):' \echo '(Should be <5 per hour in stable cluster)'
select
    count(*) as rebalances_last_hour,
    case
        when count(*) = 0 then '✓ STABLE'
        when count(*) <= 5 then '⚠ SOME ACTIVITY'
        else '✗ UNSTABLE'
    end as status
from sync_rebalance
where triggered_at > now() - INTERVAL '1 hour';

\echo '' \echo 'Rebalances by Hour (Last 24 Hours):'
select
    date_trunc('hour', triggered_at) as hour,
    count(*) as rebalance_count,
    string_agg(trigger_reason, ', '
               order by triggered_at) as reasons
from sync_rebalance
where triggered_at > now() - INTERVAL '24 hours'
group by hour
order by hour desc;
