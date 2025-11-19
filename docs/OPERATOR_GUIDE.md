# Operator Guide - Hybrid Coordination System

## Table of Contents

1. [Operational Overview](#operational-overview)
2. [Deployment Procedures](#deployment-procedures)
3. [Monitoring and Alerting](#monitoring-and-alerting)
4. [Troubleshooting](#troubleshooting)
5. [Emergency Procedures](#emergency-procedures)
6. [Database Maintenance](#database-maintenance)
7. [Performance Tuning](#performance-tuning)

## Operational Overview

### System Components

**Nodes**: Worker processes that register, heartbeat, and claim tasks
**Leader**: One node (oldest) that manages cluster coordination
**Tokens**: Ownership units that determine task claiming rights
**Database**: PostgreSQL tables that store cluster state

### Normal Operation Flow

1. Nodes start and register in `sync_node` table
2. Leader is elected (oldest node by `created_on`)
3. Leader distributes tokens evenly across nodes
4. All nodes heartbeat every 5 seconds
5. Nodes claim tasks based on token ownership
6. Leader monitors for dead nodes every 10 seconds
7. On membership changes, leader triggers rebalancing
8. Nodes refresh tokens every 30 seconds

### Key Metrics to Monitor

- **Active node count**: Should match expected cluster size
- **Token distribution balance**: All nodes should have ~equal tokens
- **Heartbeat lag**: No node should have heartbeat >10 seconds old
- **Rebalance frequency**: Should be rare (<1 per hour in stable cluster)
- **Task claiming rate**: Should be consistent across nodes
- **Leader stability**: Leader should not change frequently
- **Callback performance**: Token callbacks should complete in <1 second

## Deployment Procedures

### Initial Deployment (New Cluster)

**Prerequisites**:
- PostgreSQL database accessible from all nodes
- Network connectivity between nodes
- Configuration values set (see config.py)

**Steps**:

1. **Verify database connectivity**:
```bash
psql -U postgres -d jobsync -c "SELECT version();"
```

2. **Verify tables created** (tables are created automatically on first node startup):
```sql
SELECT tablename FROM pg_tables WHERE tablename LIKE 'sync_%';
-- Should see: sync_node, sync_token, sync_lock, sync_leader_lock,
--              sync_rebalance_lock, sync_rebalance
```

3. **Start first node** (tables will be created automatically):
```bash
export SYNC_COORDINATION_ENABLED=true
export SYNC_TOTAL_TOKENS=10000
python worker.py --node-name worker-01
```

   The first node will automatically create all required tables via `ensure_database_ready()`.

4. **Verify tables were created**:
```sql
SELECT tablename FROM pg_tables WHERE tablename LIKE 'sync_%';
-- Should see: sync_node, sync_token, sync_lock, sync_leader_lock,
--              sync_rebalance_lock, sync_rebalance, sync_audit,
--              sync_claim, sync_checkpoint
```

5. **Verify first node registered**:
```sql
SELECT * FROM sync_node;
-- Should see worker-01 with recent last_heartbeat
```

6. **Start additional nodes**:
```bash
# Terminal 2
python worker.py --node-name worker-02

# Terminal 3
python worker.py --node-name worker-03
```

7. **Verify token distribution**:
```sql
SELECT node, COUNT(*) as token_count
FROM sync_token
GROUP BY node;
-- Should see balanced distribution
```

8. **Monitor logs** for coordination activity:
```
INFO: worker-01 elected as leader
INFO: Distributed 10000 tokens across 3 nodes
INFO: worker-01 received 3333 tokens (version 1)
INFO: worker-02 received 3333 tokens (version 1)
INFO: worker-03 received 3334 tokens (version 1)
```

### Rolling Update (Zero-Downtime)

**Goal**: Update code without stopping task processing.

**Steps**:

1. **Start new nodes with updated code**:
```bash
# New version nodes
python worker_v2.py --node-name worker-v2-01 &
python worker_v2.py --node-name worker-v2-02 &
```

2. **Wait for new nodes to join cluster** (check logs or database):
```sql
SELECT name, last_heartbeat FROM sync_node
WHERE name LIKE 'worker-v2%';
```

3. **Verify token distribution updated**:
```sql
SELECT node, COUNT(*) as tokens FROM sync_token GROUP BY node;
-- Should see tokens distributed across all 5 nodes (3 old + 2 new)
```

4. **Gracefully stop old nodes** (SIGTERM, not SIGKILL):
```bash
kill -TERM $(pgrep -f worker_v1.py)
```

5. **Wait for leader to detect and rebalance** (~30-60 seconds):
```sql
SELECT * FROM sync_rebalance ORDER BY triggered_at DESC LIMIT 1;
-- Should see recent rebalance event
```

6. **Verify remaining nodes have all tokens**:
```sql
SELECT SUM(COUNT(*)) as total_tokens FROM sync_token;
-- Should still be 10000
```

### Scaling Up (Adding Nodes)

**Steps**:

1. **Start new nodes**:
```bash
for i in {4..6}; do
  python worker.py --node-name worker-0$i &
done
```

2. **Verify nodes registered**:
```sql
SELECT name FROM sync_node ORDER BY created_on;
```

3. **Wait for automatic rebalancing** (~30-60 seconds)

4. **Verify balanced distribution**:
```sql
SELECT node, COUNT(*) as tokens,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) as pct
FROM sync_token
GROUP BY node
ORDER BY tokens DESC;
-- Each node should have ~16.7% (100/6)
```

### Scaling Down (Removing Nodes)

**Steps**:

1. **Choose nodes to remove** (prefer non-leaders to avoid failover)

2. **Gracefully stop nodes**:
```bash
kill -TERM $(pgrep -f worker-04)
kill -TERM $(pgrep -f worker-05)
```

3. **Wait for leader to detect deaths** (~15-30 seconds):
```sql
SELECT name FROM sync_node
WHERE last_heartbeat < NOW() - INTERVAL '15 seconds';
```

4. **Wait for automatic rebalancing** (~30-60 seconds):
```sql
SELECT * FROM sync_rebalance ORDER BY triggered_at DESC LIMIT 1;
```

5. **Verify tokens redistributed**:
```sql
SELECT node, COUNT(*) FROM sync_token GROUP BY node;
-- Remaining nodes should have more tokens
```

6. **Optional: Clean up dead node records**:
```sql
DELETE FROM sync_node WHERE name IN ('worker-04', 'worker-05');
```

## Monitoring and Alerting

### Critical Metrics

#### 1. Active Node Count

**Query**:
```sql
SELECT COUNT(*) as active_nodes
FROM sync_node
WHERE last_heartbeat > NOW() - INTERVAL '15 seconds';
```

**Alert**: Active node count != expected cluster size

**Action**: Investigate missing nodes (crashed, network issue, resource starvation)

#### 2. Heartbeat Lag

**Query**:
```sql
SELECT name,
       EXTRACT(EPOCH FROM (NOW() - last_heartbeat)) as seconds_since_heartbeat
FROM sync_node
WHERE last_heartbeat > NOW() - INTERVAL '1 minute'
ORDER BY last_heartbeat;
```

**Alert**: Any node with >10 seconds lag

**Action**: Check node health, CPU, network connectivity

#### 3. Token Distribution Balance

**Query**:
```sql
SELECT node, COUNT(*) as tokens,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct
FROM sync_token
GROUP BY node
ORDER BY tokens DESC;
```

**Alert**: Any node with <10% or >50% of tokens (for >3 node cluster)

**Action**: Check for locked tokens, verify rebalancing is working

#### 4. Rebalance Frequency

**Query**:
```sql
SELECT COUNT(*) as rebalances_last_hour
FROM sync_rebalance
WHERE triggered_at > NOW() - INTERVAL '1 hour';
```

**Alert**: >5 rebalances in 1 hour

**Action**: Indicates cluster instability - check node crashes, network issues

#### 5. Leader Stability

**Query**:
```sql
SELECT name as current_leader
FROM sync_node
WHERE last_heartbeat > NOW() - INTERVAL '15 seconds'
ORDER BY created_on, name
LIMIT 1;
```

**Alert**: Leader changed in last 5 minutes (check previous value)

**Action**: Investigate why previous leader died

#### 6. Locked Token Assignment

**Query**:
```sql
SELECT l.node_pattern, COUNT(*) as locked_tokens,
       STRING_AGG(DISTINCT t.node, ', ') as assigned_to
FROM sync_lock l
JOIN sync_token t ON l.token_id = t.token_id
GROUP BY l.node_pattern;
```

**Alert**: Locked tokens assigned to wrong node pattern

**Action**: Manual intervention required - rebalance or fix lock patterns

### Recommended Dashboards

#### Dashboard 1: Cluster Overview

- Active node count (gauge)
- Token distribution (bar chart per node)
- Current leader (text)
- Rebalances in last 24h (line chart)

#### Dashboard 2: Node Health

- Per-node heartbeat age (gauge per node)
- Per-node token count (gauge per node)
- Per-node task claiming rate (line chart)

#### Dashboard 3: Performance

- Token distribution time (histogram)
- Rebalance trigger lag (time from death to rebalance)
- Task claiming latency (can_claim_task duration)

### Sample Prometheus Metrics

```python
# In your worker code
from prometheus_client import Gauge, Counter, Histogram

cluster_size = Gauge('jobsync_cluster_size', 'Number of active nodes')
my_token_count = Gauge('jobsync_my_tokens', 'Tokens owned by this node', ['node_name'])
is_leader = Gauge('jobsync_is_leader', 'Whether this node is leader', ['node_name'])
tasks_claimed = Counter('jobsync_tasks_claimed', 'Tasks claimed by this node', ['node_name'])
rebalance_duration = Histogram('jobsync_rebalance_seconds', 'Time to complete rebalance')

# Update metrics periodically
with Job('worker-01', 'postgres', config) as job:
    cluster_size.set(len(job.get_active_nodes()))
    my_token_count.labels(node_name='worker-01').set(len(job._my_tokens))
    is_leader.labels(node_name='worker-01').set(1 if job.am_i_leader() else 0)
```

## Troubleshooting

### Problem: Node starts but doesn't claim any tasks

**Symptoms**:
- Node registered in `sync_node`
- Node heartbeat is current
- `can_claim_task()` returns False for all tasks
- Log shows "0 tokens" or "Cannot claim task"

**Diagnosis**:
```sql
-- Check token ownership
SELECT COUNT(*) FROM sync_token WHERE node = 'worker-01';
-- Should be >0

-- Check token version
SELECT DISTINCT version FROM sync_token;
-- Should match across all nodes

-- Check if locks exclude this node
SELECT * FROM sync_lock;
```

**Possible causes**:
1. **Node joined after token distribution**: Wait for next rebalance cycle
2. **Node name doesn't match lock pattern**: All tokens may be locked to other patterns
3. **Token version mismatch**: Node has stale token cache

**Solutions**:
1. Wait 30-60 seconds for rebalance
2. Check lock patterns - may need to adjust or remove locks
3. Restart node to refresh token cache
4. Manually trigger rebalance by stopping/starting another node

### Problem: Tasks processed multiple times

**Symptoms**:
- Same task ID appears in `sync_claim` multiple times
- Duplicate work detected in application logs
- Data corruption from double-processing

**Diagnosis**:
```sql
-- Find duplicate claims
SELECT task_id, node, COUNT(*) as claims
FROM sync_claim
GROUP BY task_id, node
HAVING COUNT(*) > 1;

-- Check if coordination is enabled
SELECT * FROM sync_token LIMIT 1;
-- If empty, coordination not active
```

**Possible causes**:
1. **Coordination disabled**: `SYNC_COORDINATION_ENABLED=false`
2. **skip_sync=True**: Bypassing coordination
3. **Network partition**: Two leaders operating independently
4. **Database replication lag**: Nodes seeing different token assignments

**Solutions**:
1. Verify coordination enabled in config
2. Check Job constructor - ensure `skip_sync=False`
3. Check network connectivity between nodes
4. If using read replicas, ensure leader writes go to primary

### Problem: Constant rebalancing

**Symptoms**:
- `sync_rebalance` table filling rapidly
- Log shows "Rebalancing triggered" every minute
- Token version incrementing rapidly

**Diagnosis**:
```sql
-- Rebalance frequency
SELECT COUNT(*),
       MAX(triggered_at) - MIN(triggered_at) as time_span
FROM sync_rebalance
WHERE triggered_at > NOW() - INTERVAL '10 minutes';

-- Recent rebalance reasons
SELECT triggered_at, reason, nodes_before, nodes_after, tokens_moved
FROM sync_rebalance
ORDER BY triggered_at DESC
LIMIT 10;
```

**Possible causes**:
1. **Nodes crashing repeatedly**: Check application logs
2. **Heartbeat timeout too short**: Network latency causing false deaths
3. **Leader instability**: Leader dying repeatedly
**Solutions**:
1. Fix application crashes (OOM, exceptions, etc.)
2. Increase heartbeat timeout:
   ```bash
   export SYNC_HEARTBEAT_TIMEOUT=30
   export SYNC_DEAD_NODE_INTERVAL=15
   ```
3. Ensure leader node is stable (adequate resources)

**Note**: Clock skew between nodes is not an issue - the system uses database timestamps,
not local node clocks.

### Problem: Leader lock timeout

**Symptoms**:
- Error: "Failed to acquire leader lock"
- No token distribution happening
- New nodes join but don't get tokens

**Diagnosis**:
```sql
-- Check leader lock
SELECT * FROM sync_leader_lock;

-- Check how long lock has been held
SELECT acquired_at, acquired_by,
       EXTRACT(EPOCH FROM (NOW() - acquired_at)) as seconds_held
FROM sync_leader_lock;
```

**Possible causes**:
1. **Previous leader crashed mid-distribution**: Lock not released
2. **Distribution taking too long**: Leader still working (>30 seconds)
3. **Database deadlock**: Transaction holding lock indefinitely

**Solutions**:

**If lock is stale (>5 minutes old)**:
```sql
-- CAUTION: Only do this if certain no leader is active
DELETE FROM sync_leader_lock WHERE acquired_at < NOW() - INTERVAL '5 minutes';
```

**If distribution is in progress** (lock <1 minute old):
- Wait for distribution to complete
- If it doesn't complete, check leader node logs for errors

**Prevention**:
- Increase leader lock timeout for large clusters:
  ```bash
  export SYNC_LEADER_LOCK_TIMEOUT=60
  ```

### Problem: Uneven token distribution

**Symptoms**:
- Some nodes have 2x more tokens than others
- Task load is imbalanced
- Some nodes idle while others overloaded

**Diagnosis**:
```sql
-- Token distribution
SELECT node, COUNT(*) as tokens,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) as pct
FROM sync_token
GROUP BY node
ORDER BY tokens DESC;

-- Check locked tokens
SELECT node_pattern, COUNT(*) as locked_count
FROM sync_lock
GROUP BY node_pattern;
```

**Possible causes**:
1. **Locked tokens creating imbalance**: Intentional for special nodes
2. **Recent node join/leave**: Rebalance in progress
3. **Dead node not cleaned up**: Tokens still assigned to dead node

**Solutions**:

**If due to locks** (intentional):
- No action needed - locks are working as designed
- Adjust lock patterns if imbalance is too severe

**If due to recent changes**:
- Wait for next rebalance cycle (30-60 seconds)

**If due to dead node**:
```sql
-- Find dead nodes with tokens
SELECT t.node, n.last_heartbeat, COUNT(*) as tokens
FROM sync_token t
LEFT JOIN sync_node n ON t.node = n.name
WHERE n.last_heartbeat < NOW() - INTERVAL '1 minute' OR n.name IS NULL
GROUP BY t.node, n.last_heartbeat;

-- Manually trigger cleanup (leader will do this automatically)
DELETE FROM sync_node WHERE last_heartbeat < NOW() - INTERVAL '1 hour';
```

### Problem: Orphaned locks from decommissioned nodes

**Symptoms**:
- Locks exist but creator node no longer in cluster
- Tasks permanently pinned to non-existent node pattern
- Token distribution skewed by locks to dead nodes

**Diagnosis**:
```sql
-- Find orphaned locks
SELECT 
    l.token_id,
    l.node_pattern,
    l.created_by,
    l.created_at,
    NOW() - l.created_at as age
FROM sync_lock l
LEFT JOIN sync_node n ON l.created_by = n.name
WHERE n.name IS NULL
ORDER BY l.created_at;
```

**Possible causes**:
1. **Node decommissioned without clearing locks**: Developer forgot to clear locks
2. **clear_existing_locks not used**: Static locks from node that no longer exists
3. **Manual lock creation**: Locks created via SQL, not through API

**Solutions**:

**Option 1: Use Job API (recommended)**:
```python
from jobsync import Job

with Job('admin-node', 'production', config) as job:
    # Clear locks from specific decommissioned node
    job.clear_locks_by_creator('old-worker-01')
    
    # Or review all locks first
    locks = job.list_locks()
    for lock in locks:
        if should_remove(lock):
            job.clear_locks_by_creator(lock['created_by'])
```

**Option 2: Direct SQL (if Job unavailable)**:
```sql
-- Clear locks from specific creator
DELETE FROM sync_lock WHERE created_by = 'old-worker-01';

-- Or clear all orphaned locks (no creator node exists)
DELETE FROM sync_lock
WHERE created_by NOT IN (SELECT name FROM sync_node);
```

**Prevention**:
- Use `clear_existing_locks=True` for dynamic lock logic
- Document which nodes create which locks
- Include lock cleanup in node decommission procedures

### Problem: Locks not being cleared between runs

**Symptoms**:
- Lock table growing over time
- Stale lock patterns accumulating
- Tasks pinned to old patterns

**Diagnosis**:
```sql
-- Count locks by creator
SELECT created_by, COUNT(*) as lock_count,
       MIN(created_at) as oldest,
       MAX(created_at) as newest
FROM sync_lock
GROUP BY created_by
ORDER BY lock_count DESC;

-- Find duplicate locks (same node creating multiple times)
SELECT created_by, COUNT(DISTINCT created_at::date) as run_dates
FROM sync_lock
GROUP BY created_by
HAVING COUNT(DISTINCT created_at::date) > 1;
```

**Possible causes**:
1. **clear_existing_locks=False with dynamic logic**: Locks accumulate each run
2. **lock_provider creating duplicate locks**: Same token locked multiple times
3. **Multiple processes with same node_name**: Locks created by parallel processes

**Solutions**:

**If dynamic lock logic**:
```python
# Change from False to True
with Job(..., lock_provider=my_locks, clear_existing_locks=True) as job:
    pass
```

**If lock_provider needs fixing**:
```python
def fixed_lock_provider(job):
    # Don't register same task multiple times
    tasks = set(get_tasks_needing_locks())  # Use set to deduplicate
    locks = [(task_id, pattern, reason) for task_id in tasks]
    job.register_task_locks_bulk(locks)
```

**Emergency cleanup**:
```python
with Job('admin', 'production', config) as job:
    # Nuclear option - clear everything
    job.clear_all_locks()
    
    # Then re-register correct locks
    register_correct_locks(job)
```

### Problem: Callbacks taking too long

**Symptoms**:
- Log shows "on_tokens_added completed in 5000ms"
- Warning: "Callback took >1000ms"
- Coordination appears slow or unresponsive
- Rebalancing takes longer than expected

**Diagnosis**:
```python
# Check application logs for callback timing
grep "on_tokens_added completed" app.log | tail -20
grep "on_tokens_removed completed" app.log | tail -20

# Look for slow callback warnings
grep "Slow callback detected" app.log
```

**Possible causes**:
1. **Synchronous heavy work in callback**: Loading data, making API calls, etc.
2. **Too many tasks per token**: Callback processing large token sets
3. **Blocking operations**: Synchronous I/O without threading

**Solutions**:

**Delegate heavy work to background threads**:
```python
def on_tokens_added(token_ids: set[int]):
    # Quick acknowledgment
    logger.info(f'Received {len(token_ids)} tokens')
    
    # Delegate heavy work
    def background_work():
        for token_id in token_ids:
            expensive_operation(token_id)
    
    thread = threading.Thread(target=background_work)
    thread.daemon = True
    thread.start()
```

**Batch operations efficiently**:
```python
def on_tokens_added(token_ids: set[int]):
    # Collect all work first
    all_tasks = []
    for token_id in token_ids:
        tasks = job.get_task_ids_for_token(token_id, all_task_ids)
        all_tasks.extend(tasks)
    
    # Process in bulk
    start_bulk_subscriptions(all_tasks)
```

**Monitor callback performance**:
```python
import time

def on_tokens_added(token_ids: set[int]):
    start = time.time()
    try:
        process_tokens(token_ids)
    finally:
        duration_ms = int((time.time() - start) * 1000)
        if duration_ms > 1000:
            logger.warning(f'Callback took {duration_ms}ms for {len(token_ids)} tokens')
```

### Problem: Callbacks failing with exceptions

**Symptoms**:
- Log shows "on_tokens_added callback failed: ..."
- Resources not started/stopped correctly
- Inconsistent state between tokens and active resources

**Diagnosis**:
```bash
# Check for callback exceptions
grep "callback failed" app.log

# Check if resources are leaked
# (app-specific - check your resource tracking)
```

**Possible causes**:
1. **Unhandled exceptions in callback**: Code doesn't handle errors
2. **Partial failures**: Some resources fail to start/stop
3. **Resource conflicts**: Multiple callbacks trying to use same resource

**Solutions**:

**Wrap callback logic in try/except**:
```python
def on_tokens_added(token_ids: set[int]):
    for token_id in token_ids:
        try:
            start_processing(token_id)
        except Exception as e:
            logger.error(f'Failed to process token {token_id}: {e}')
            # Continue with other tokens
```

**Track resource state**:
```python
class ResourceTracker:
    def __init__(self):
        self.active = {}
        self.failed = {}
    
    def on_tokens_added(self, token_ids: set[int]):
        for token_id in token_ids:
            try:
                resource = start_resource(token_id)
                self.active[token_id] = resource
            except Exception as e:
                logger.error(f'Failed to start token {token_id}: {e}')
                self.failed[token_id] = str(e)
```

**Implement retry logic**:
```python
def on_tokens_added(token_ids: set[int]):
    for token_id in token_ids:
        for attempt in range(3):
            try:
                start_processing(token_id)
                break
            except Exception as e:
                if attempt == 2:
                    logger.error(f'Failed after 3 attempts for token {token_id}: {e}')
                else:
                    time.sleep(1)
```

### Problem: Node health check failing

**Symptoms**:
- `am_i_healthy()` returns False
- Log shows "Heartbeat failed" or "Node unhealthy"
- Node still processing tasks

**Diagnosis**:
```python
with Job('worker-01', 'postgres', config) as job:
    print(f"Last heartbeat sent: {job._last_heartbeat_sent}")
    print(f"Heartbeat interval: {job._heartbeat_interval}")
    print(f"Heartbeat timeout: {job._heartbeat_timeout}")
    print(f"Healthy: {job.am_i_healthy()}")
```

```sql
-- Check database heartbeat
SELECT name, last_heartbeat,
       EXTRACT(EPOCH FROM (NOW() - last_heartbeat)) as seconds_ago
FROM sync_node
WHERE name = 'worker-01';
```

**Possible causes**:
1. **Heartbeat thread crashed**: Thread exception not logged
2. **Database connection lost**: Writes failing silently
3. **High CPU/resource contention**: Thread not getting scheduled
4. **Clock skew**: Node's clock is wrong

**Solutions**:
1. Check application logs for thread exceptions
2. Verify database connectivity: `psql -U postgres -d jobsync`
3. Check CPU usage: `top` - reduce load if needed
4. Verify system clock: `date` - sync with NTP if wrong
5. Restart node to recreate heartbeat thread

## Emergency Procedures

### Emergency: All nodes claiming all tasks (coordination failure)

**Impact**: HIGH - Duplicate processing, data corruption

**Immediate action**:
1. **Stop all nodes immediately**:
   ```bash
   pkill -TERM -f worker.py
   ```

2. **Verify coordination tables exist**:
   ```sql
   \dt sync_*
   ```
   If missing, tables will be created automatically on next node startup with coordination enabled.

3. **Check configuration**:
   ```bash
   grep SYNC_COORDINATION_ENABLED .env
   # Should be 'true'
   ```

4. **Restart one node with coordination enabled**:
   ```bash
   export SYNC_COORDINATION_ENABLED=true
   python worker.py --node-name worker-01
   ```

5. **Verify token distribution**:
   ```sql
   SELECT COUNT(*) FROM sync_token WHERE node = 'worker-01';
   ```

6. **If working, restart other nodes**

### Emergency: Leader node crashed, no new leader

**Impact**: MEDIUM - No rebalancing, but existing nodes continue working

**Symptoms**:
- No node in `sync_leader_lock`
- Rebalancing not happening
- New nodes not getting tokens

**Immediate action**:
1. **Verify current leader is dead**:
   ```sql
   SELECT name as supposed_leader
   FROM sync_node
   ORDER BY created_on, name
   LIMIT 1;

   -- Check if that node is alive
   SELECT last_heartbeat FROM sync_node WHERE name = 'supposed_leader';
   ```

2. **Clear leader lock if stale**:
   ```sql
   DELETE FROM sync_leader_lock
   WHERE acquired_at < NOW() - INTERVAL '2 minutes';
   ```

3. **Wait for new leader election** (~30-60 seconds)

4. **Verify new leader**:
   ```sql
   SELECT acquired_by FROM sync_leader_lock;
   ```

5. **If no new leader after 2 minutes**:
   - Check all nodes' logs for errors
   - Restart all nodes if needed

### Emergency: Database partition (split-brain)

**Impact**: CRITICAL - Multiple leaders, duplicate processing

**Symptoms**:
- Two nodes both think they're leader
- Different token assignments in different parts of cluster
- Impossible - PostgreSQL prevents this with SERIALIZABLE isolation

**This should not happen** due to database ACID properties, but if suspected:

1. **Stop all nodes immediately**

2. **Check for multiple leader locks** (should be impossible):
   ```sql
   SELECT COUNT(*) FROM sync_leader_lock;
   -- Should be 0 or 1, never >1
   ```

3. **Check database connectivity from all nodes**

4. **Restart nodes one at a time**, verifying single leader

### Emergency: Disable coordination system

**Impact**: LOW - Coordination disabled, all nodes claim all tasks (legacy behavior)

**Steps**:

1. **Stop all nodes**

2. **Option A: Keep tables, just disable coordination**:
   ```bash
   export SYNC_COORDINATION_ENABLED=false
   python worker.py --node-name worker-01
   ```
   Tables remain but are not used. Nodes operate independently (legacy behavior).

3. **Option B: Remove coordination tables** (if reverting permanently):
   ```sql
   DROP TABLE IF EXISTS sync_rebalance;
   DROP TABLE IF EXISTS sync_rebalance_lock;
   DROP TABLE IF EXISTS sync_leader_lock;
   DROP TABLE IF EXISTS sync_lock;
   DROP TABLE IF EXISTS sync_token;
   -- Keep sync_node, sync_checkpoint, sync_audit, sync_claim for basic functionality
   ```

4. **Deploy code with coordination disabled** (or old code version)

5. **Restart nodes**

6. **Verify coordination disabled**:
   - Check logs for "coordination mode" messages (should not appear)
   - All nodes should process all tasks (legacy behavior)
   - No entries in `sync_token` table

## Database Maintenance

### Cleaning Old Data

**Rebalance audit log**:
```sql
-- Keep last 30 days
DELETE FROM sync_rebalance
WHERE triggered_at < NOW() - INTERVAL '30 days';
```

**Dead node records**:
```sql
-- Remove nodes dead for >7 days
DELETE FROM sync_node
WHERE last_heartbeat < NOW() - INTERVAL '7 days';
```

**Orphaned locks** (tokens locked but no matching nodes):
```sql
-- Find orphaned locks
SELECT l.token_id, l.node_pattern, l.created_by
FROM sync_lock l
WHERE NOT EXISTS (
    SELECT 1 FROM sync_node n
    WHERE n.name LIKE l.node_pattern
    AND n.last_heartbeat > NOW() - INTERVAL '15 seconds'
);

-- Remove if safe
DELETE FROM sync_lock
WHERE token_id IN (...);
-- Then trigger rebalance
```

### Backup and Restore

**Backup coordination state**:
```bash
pg_dump -U postgres -d jobsync \
  -t sync_node -t sync_token -t sync_lock \
  -t sync_leader_lock -t sync_rebalance_lock -t sync_rebalance \
  > coordination_backup.sql
```

**Restore** (stops cluster - only do during maintenance window):
```bash
# 1. Stop all nodes
pkill -TERM -f worker.py

# 2. Restore tables
psql -U postgres -d jobsync < coordination_backup.sql

# 3. Clear locks
psql -U postgres -d jobsync <<EOF
DELETE FROM sync_leader_lock;
DELETE FROM sync_rebalance_lock;
EOF

# 4. Restart nodes
```

### Vacuum and Analyze

```sql
-- Regular maintenance
VACUUM ANALYZE sync_node;
VACUUM ANALYZE sync_token;
VACUUM ANALYZE sync_lock;
VACUUM ANALYZE sync_rebalance;
```

## Performance Tuning

### For High-Churn Environments

Nodes frequently joining/leaving (Kubernetes, spot instances):

```bash
export SYNC_HEARTBEAT_INTERVAL=3
export SYNC_HEARTBEAT_TIMEOUT=9
export SYNC_DEAD_NODE_INTERVAL=5
export SYNC_REBALANCE_INTERVAL=15
export SYNC_TOKEN_REFRESH_INITIAL=3
export SYNC_TOKEN_REFRESH_STEADY=10
```

### For Stable Clusters

Nodes rarely change:

```bash
export SYNC_HEARTBEAT_INTERVAL=10
export SYNC_HEARTBEAT_TIMEOUT=30
export SYNC_REBALANCE_INTERVAL=60
export SYNC_TOKEN_REFRESH_STEADY=60
```

### For Large Clusters (10+ nodes)

```bash
export SYNC_TOTAL_TOKENS=100000  # More tokens = better distribution
export SYNC_REBALANCE_INTERVAL=60  # Less frequent checks
```

### For Small Clusters (2-3 nodes)

```bash
export SYNC_TOTAL_TOKENS=1000  # Fewer tokens = faster rebalancing
```

### Database Optimization

**Indexes** (created by migration, verify):
```sql
-- Critical indexes
CREATE INDEX IF NOT EXISTS idx_sync_node_heartbeat ON sync_node(last_heartbeat);
CREATE INDEX IF NOT EXISTS idx_sync_token_node ON sync_token(node);
CREATE INDEX IF NOT EXISTS idx_sync_lock_token ON sync_lock(token_id);
```

**Connection pooling** (for high node count):
```python
# In config
postgres.pool_size = 10  # Max connections per node
postgres.pool_timeout = 30
```

**Query optimization**:
```sql
-- Ensure queries use indexes
EXPLAIN ANALYZE
SELECT * FROM sync_node WHERE last_heartbeat > NOW() - INTERVAL '15 seconds';
-- Should show "Index Scan" not "Seq Scan"
```

## Best Practices

1. **Always use graceful shutdown** (SIGTERM, not SIGKILL)
2. **Monitor rebalance frequency** - should be rare in stable cluster
3. **Set alerts on heartbeat lag** - early warning of node issues
4. **Keep token pool large** - default 10,000 is good for most cases
5. **Test coordination in staging** - same node count and timing as production
6. **Use descriptive node names** - include hostname, region, instance ID
7. **Document lock patterns** - explain why specific tasks locked to specific nodes
8. **Use clear_existing_locks=True for dynamic locks** - prevents stale lock accumulation
9. **Review lock table periodically** - watch for orphaned locks from decommissioned nodes
10. **Clean up dead nodes periodically** - prevent table bloat
11. **Vacuum coordination tables weekly** - maintain performance
12. **Back up before major changes** - can restore if issues arise
13. **Keep callbacks fast** - delegate heavy work to background threads (<1s completion)
14. **Monitor callback performance** - log timing and alert on slow callbacks
15. **Handle callback exceptions** - don't let one token failure stop others

## Contact and Escalation

For issues not covered in this guide:

1. Check application logs for detailed error messages
2. Review recent changes (code deploys, config changes, infrastructure)
3. Consult development team with:
   - Symptoms and timeline
   - Database state (node count, token distribution, rebalance history)
   - Relevant log excerpts
   - Steps already attempted

## Appendix: Quick Reference Commands

```bash
# Check cluster health
psql -U postgres -d jobsync -c "SELECT name, last_heartbeat FROM sync_node WHERE last_heartbeat > NOW() - INTERVAL '15 seconds';"

# Check token distribution
psql -U postgres -d jobsync -c "SELECT node, COUNT(*) FROM sync_token GROUP BY node;"

# Check current leader
psql -U postgres -d jobsync -c "SELECT name FROM sync_node WHERE last_heartbeat > NOW() - INTERVAL '15 seconds' ORDER BY created_on, name LIMIT 1;"

# Check recent rebalances
psql -U postgres -d jobsync -c "SELECT * FROM sync_rebalance ORDER BY triggered_at DESC LIMIT 5;"

# Clear stale leader lock (DANGEROUS)
psql -U postgres -d jobsync -c "DELETE FROM sync_leader_lock WHERE acquired_at < NOW() - INTERVAL '5 minutes';"

# Stop all workers
pkill -TERM -f worker.py

# Disable coordination (stop nodes first)
export SYNC_COORDINATION_ENABLED=false
# Or drop coordination tables:
# psql -U postgres -d jobsync -c "DROP TABLE IF EXISTS sync_rebalance, sync_rebalance_lock, sync_leader_lock, sync_lock, sync_token;"
```
