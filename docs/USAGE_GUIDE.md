# Usage Guide - Hybrid Coordination System

## Table of Contents

1. [Overview](#overview)
2. [Basic Job Creation](#basic-job-creation)
3. [Coordination Configuration](#coordination-configuration)
4. [Task Locking and Pinning](#task-locking-and-pinning)
5. [Task Processing Patterns](#task-processing-patterns)
6. [Health Monitoring](#health-monitoring)
7. [Database Queries](#database-queries)
8. [Best Practices](#best-practices)

## Overview

The JobSync coordination system provides distributed task processing with automatic load balancing. This guide shows how to use the system in your applications.

**Key Concepts:**
- **Nodes**: Worker processes that register and process tasks
- **Tokens**: Ownership units that determine which tasks a node can claim
- **Leader**: One node that manages cluster coordination and rebalancing
- **Locks**: Optional pinning of specific tasks to specific nodes

**Common Imports:**

```python
from jobsync import Job, Task, CoordinationConfig
import database as db
from datetime import datetime, timedelta
```

## Basic Job Creation

### Simple Job Without Locks

```python
def process_daily_tasks():
    """Basic job processing with coordination.
    """
    with Job(
        node_name='worker-01',
        site='production',
        config=config,
        wait_on_enter=120,  # Wait for cluster formation and token distribution
        skip_db_init=False
    ) as job:
        # Job automatically:
        # - Registers node with heartbeat
        # - Elects leader
        # - Distributes tokens (if leader)
        # - Reads token assignments
        
        # Get all tasks for today
        tasks = get_pending_tasks()
        
        for task_id in tasks:
            task = Task(task_id)
            
            # Only process if this task belongs to our tokens
            if job.can_claim_task(task):
                job.add_task(task)
                process_task(task_id)
        
        # Mark tasks complete
        job.write_audit()
```

### Job with Date-Based Processing

```python
from datetime import datetime

def process_historical_date(date: datetime):
    """Process tasks for a specific date.
    """
    with Job(
        node_name='worker-01',
        site='production',
        config=config,
        date=date,  # Process specific date
        wait_on_enter=120
    ) as job:
        # Get tasks for this specific date
        sql = "SELECT task_id FROM tasks WHERE date = %s AND status = 'pending'"
        tasks = db.select(job._cn, sql, date)
        
        for row in tasks.to_dict('records'):
            task = Task(row['task_id'])
            
            if job.can_claim_task(task):
                job.add_task(task)
                result = compute_result(row['task_id'])
                save_result(row['task_id'], result)
        
        job.write_audit()
```

## Coordination Configuration

### Using CoordinationConfig

The `CoordinationConfig` dataclass provides fine-grained control over coordination behavior.

**Configuration Priority:**
1. `coordination_config` parameter (highest)
2. `config.sync.coordination` settings
3. Built-in defaults (lowest)

### Basic Customization

```python
from jobsync import CoordinationConfig

def process_with_custom_config():
    """Customize coordination timing and behavior.
    """
    coord_config = CoordinationConfig(
        enabled=True,
        total_tokens=5000,                    # Fewer tokens for smaller clusters
        heartbeat_interval_sec=10,            # Slower heartbeat (default: 5)
        heartbeat_timeout_sec=30,             # More tolerant timeout (default: 15)
        rebalance_check_interval_sec=60,      # Less frequent rebalancing (default: 30)
        dead_node_check_interval_sec=20,      # Slower dead node detection (default: 10)
        token_refresh_initial_interval_sec=2, # Faster initial refresh (default: 5)
        token_refresh_steady_interval_sec=60, # Slower steady refresh (default: 30)
        locks_enabled=True,
        leader_lock_timeout_sec=60,
        health_check_interval_sec=60
    )
    
    with Job(
        node_name='worker-01',
        site='production',
        config=config,
        wait_on_enter=180,
        coordination_config=coord_config
    ) as job:
        # Process with custom settings
        process_tasks(job)
```

### Disabling Coordination

```python
def process_without_coordination():
    """Run without coordination (legacy mode).
    
    All tasks are processed by this node.
    """
    coord_config = CoordinationConfig(enabled=False)
    
    with Job(
        node_name='worker-01',
        site='production',
        config=config,
        coordination_config=coord_config
    ) as job:
        # All tasks processed by this node (no token filtering)
        for task_id in get_all_tasks():
            process_task(task_id)
```

### Performance Tuning for Different Scenarios

**Large Cluster (10+ nodes):**
```python
coord_config = CoordinationConfig(
    total_tokens=50000,                   # More tokens for better distribution
    heartbeat_interval_sec=3,             # Faster heartbeat
    heartbeat_timeout_sec=10,             # Shorter timeout
    rebalance_check_interval_sec=15,      # More responsive
    dead_node_check_interval_sec=5,       # Faster cleanup
    token_refresh_steady_interval_sec=10  # Frequent updates
)
```

**Stable Cluster (rarely changes):**
```python
coord_config = CoordinationConfig(
    heartbeat_interval_sec=10,            # Slower heartbeat
    heartbeat_timeout_sec=30,             # More tolerant
    rebalance_check_interval_sec=60,      # Less frequent checks
    token_refresh_steady_interval_sec=60  # Slower updates
)
```

**High-Churn Environment (Kubernetes, spot instances):**
```python
coord_config = CoordinationConfig(
    heartbeat_interval_sec=3,
    heartbeat_timeout_sec=9,
    dead_node_check_interval_sec=5,
    rebalance_check_interval_sec=15,
    token_refresh_initial_interval_sec=3,
    token_refresh_steady_interval_sec=10
)
```

### Configuration via Config Module

```python
# Alternative: Configure via config module
# In your config.py:
import os
from libb import Setting

Setting.unlock()

sync = Setting()
sync.coordination.enabled = True
sync.coordination.total_tokens = 5000
sync.coordination.heartbeat_interval_sec = 10
sync.coordination.heartbeat_timeout_sec = 30

Setting.lock()

# Then use without coordination_config parameter:
with Job(
    node_name='worker-01',
    site='production',
    config=config  # Reads from config.sync.coordination
) as job:
    process_tasks(job)
```

### CoordinationConfig Parameters Reference

| Parameter                            | Default   | Description                         |
| -----------                          | --------- | -------------                       |
| `enabled`                            | `True`    | Enable/disable coordination         |
| `total_tokens`                       | `10000`   | Total tokens in pool                |
| `heartbeat_interval_sec`             | `5`       | Seconds between heartbeats          |
| `heartbeat_timeout_sec`              | `15`      | Seconds before node considered dead |
| `rebalance_check_interval_sec`       | `30`      | Leader checks for changes           |
| `dead_node_check_interval_sec`       | `10`      | Leader checks for dead nodes        |
| `token_refresh_initial_interval_sec` | `5`       | Follower refresh (first 5 min)      |
| `token_refresh_steady_interval_sec`  | `30`      | Follower refresh (steady)           |
| `locks_enabled`                      | `True`    | Enable task locking                 |
| `leader_lock_timeout_sec`            | `30`      | Leader lock timeout                 |
| `health_check_interval_sec`          | `30`      | Health check frequency              |

## Task Locking and Pinning

### Single Lock Registration

```python
def process_with_lock():
    """Register a single task lock during job initialization.
    """
    def register_locks(job):
        # Pin task 12345 to node 'worker-01'
        job.register_task_lock(
            task_id=12345,
            node_pattern='worker-01',
            reason='high_memory_required',
            expires_in_days=7  # Optional expiration
        )
    
    with Job(
        node_name='worker-01',
        site='production',
        config=config,
        lock_provider=register_locks  # Callback for lock registration
    ) as job:
        # Locks registered before token distribution
        # Only worker-01 can process task 12345
        
        for task_id in get_task_list():
            task = Task(task_id)
            if job.can_claim_task(task):
                job.add_task(task)
                process_task(task_id)
```

### Bulk Lock Registration

```python
def process_with_bulk_locks():
    """Register multiple locks efficiently.
    """
    def register_locks(job):
        # Get tasks requiring special resources from database
        sql = """
        SELECT task_id, required_node 
        FROM tasks 
        WHERE date = %s AND required_node IS NOT NULL
        """
        tasks = db.select(job._cn, sql, job.date)
        
        # Build lock list
        locks = []
        for task in tasks.to_dict('records'):
            if task['required_node']:
                # required_node could be: 'worker-01,worker-02'
                for node in task['required_node'].split(','):
                    locks.append((
                        task['task_id'],
                        node.strip(),
                        'resource_requirement'
                    ))
        
        # Register all locks in one batch
        if locks:
            job.register_task_locks_bulk(locks)
    
    with Job(
        node_name='worker-01',
        site='production',
        config=config,
        lock_provider=register_locks
    ) as job:
        process_tasks(job)
```

### Pattern-Based Locks

```python
def register_pattern_locks(job):
    """Use SQL LIKE patterns for flexible node matching.
    """
    # Exact match
    job.register_task_lock(
        task_id=100,
        node_pattern='prod-node-01',
        reason='exact_node'
    )
    
    # Prefix pattern (SQL LIKE 'prod-%')
    job.register_task_lock(
        task_id=200,
        node_pattern='prod-%',
        reason='any_prod_node'
    )
    
    # Suffix pattern (SQL LIKE '%-gpu')
    job.register_task_lock(
        task_id=300,
        node_pattern='%-gpu',
        reason='gpu_required'
    )
    
    # Contains pattern (SQL LIKE '%special%')
    job.register_task_lock(
        task_id=400,
        node_pattern='%special%',
        reason='special_nodes_only'
    )
```

### Lock Lifecycle Management

**Dynamic Locks (Changes Each Run)**:

```python
def process_with_dynamic_locks():
    """Lock logic changes based on current state.
    
    Use clear_existing_locks=True to get a clean slate each run.
    """
    def register_current_locks(job):
        # Lock logic based on today's data
        heavy_queries = get_todays_heavy_queries()  # Different each run
        locks = [(task_id, 'high-mem-%', 'heavy_query') 
                 for task_id in heavy_queries]
        
        if locks:
            job.register_task_locks_bulk(locks)
    
    with Job(
        node_name='worker-01',
        site='production',
        config=config,
        lock_provider=register_current_locks,
        clear_existing_locks=True  # Clear old locks before registering new ones
    ) as job:
        for task_id in get_all_tasks():
            if job.can_claim_task(Task(task_id)):
                process_task(task_id)
```

**Static Locks (Persistent Across Runs)**:

```python
def process_with_static_locks():
    """Lock logic is consistent across runs.
    
    Use clear_existing_locks=False (default) to preserve locks.
    """
    def register_permanent_locks(job):
        # Always pin customer X to east region
        job.register_task_lock('customer-X', 'node-east-%', 'data_locality')
        job.register_task_lock('customer-Y', 'node-west-%', 'data_locality')
    
    with Job(
        node_name='worker-east-01',
        site='production',
        config=config,
        lock_provider=register_permanent_locks,
        clear_existing_locks=False  # Preserve existing locks (default)
    ) as job:
        for task_id in get_all_tasks():
            if job.can_claim_task(Task(task_id)):
                process_task(task_id)
```

**Manual Lock Management**:

```python
def manage_locks_manually():
    """Use lock management APIs for operational control.
    """
    with Job('worker-01', 'production', config) as job:
        # List all current locks
        locks = job.list_locks()
        for lock in locks:
            print(f"Token {lock['token_id']}: {lock['node_pattern']} "
                  f"(created by {lock['created_by']})")
        
        # Clear locks from decommissioned node
        removed = job.clear_locks_by_creator('old-worker-01')
        logger.info(f'Removed {removed} locks from old-worker-01')
        
        # Nuclear option: clear all locks
        if need_complete_reset:
            total = job.clear_all_locks()
            logger.warning(f'Cleared ALL {total} locks from system')
        
        # Process tasks
        for task_id in get_all_tasks():
            if job.can_claim_task(Task(task_id)):
                process_task(task_id)
```

**When to Use clear_existing_locks**:

| Scenario | Setting | Reason |
| -------- | ------- | ------ |
| Lock logic changes daily | `True` | Prevent accumulation of stale locks |
| GPU availability changes | `True` | Locks reflect current GPU node availability |
| Customer assignment changes | `True` | Update customer-to-region mappings |
| Permanent resource constraints | `False` | Keep consistent lock patterns |
| Development/testing | `True` | Clean state for each test run |

**Lock Management Best Practices**:

```python
def production_lock_pattern():
    """Best practices for production lock management.
    """
    def register_locks(job):
        # Query current state from database
        sql = """
        SELECT task_id, required_resource 
        FROM task_metadata 
        WHERE date = %s AND required_resource IS NOT NULL
        """
        requirements = db.select(job._cn, sql, job.date)
        
        locks = []
        for row in requirements.to_dict('records'):
            # Map requirements to node patterns
            if row['required_resource'] == 'gpu':
                locks.append((row['task_id'], '%-gpu', 'gpu_required'))
            elif row['required_resource'] == 'high_memory':
                locks.append((row['task_id'], 'high-mem-%', 'memory_required'))
        
        if locks:
            job.register_task_locks_bulk(locks)
            logger.info(f'Registered {len(locks)} locks for {job.date}')
    
    with Job(
        node_name='worker-01',
        site='production',
        config=config,
        lock_provider=register_locks,
        clear_existing_locks=True  # Dynamic logic - clear on each run
    ) as job:
        # Check lock state for debugging
        current_locks = job.list_locks()
        logger.debug(f'Active locks: {len(current_locks)}')
        
        process_tasks(job)
```

### Resource-Based Lock Example

```python
def process_gpu_tasks():
    """Lock GPU-intensive tasks to GPU-enabled nodes.
    """
    def register_gpu_locks(job):
        # Get tasks requiring GPU
        sql = """
        SELECT task_id FROM tasks 
        WHERE requires_gpu = true AND date = %s
        """
        gpu_tasks = db.select(job._cn, sql, job.date)
        
        locks = []
        for row in gpu_tasks.to_dict('records'):
            # Lock to any node with 'gpu' in its name
            locks.append((
                row['task_id'],
                '%gpu%',
                'gpu_required'
            ))
        
        if locks:
            job.register_task_locks_bulk(locks)
    
    with Job(
        node_name='worker-gpu-01',  # GPU-enabled node
        site='production',
        config=config,
        lock_provider=register_gpu_locks
    ) as job:
        # Only GPU tasks will be assigned to this node
        for task_id in get_task_list():
            task = Task(task_id)
            if job.can_claim_task(task):
                job.add_task(task)
                process_gpu_task(task_id)
```

## Task Processing Patterns

### Basic Task Loop

```python
def simple_task_processing():
    """Basic pattern for processing tasks.
    """
    with Job(
        node_name='worker-01',
        site='production',
        config=config,
        wait_on_enter=120
    ) as job:
        # Get all potential tasks
        all_tasks = get_all_tasks()
        
        for task_id in all_tasks:
            task = Task(task_id)
            
            # Only process tasks that belong to our tokens
            if job.can_claim_task(task):
                job.add_task(task)
                
                # Do the actual work
                result = process_task(task_id)
                
                # Save result to database
                save_result(task_id, result)
        
        # Mark tasks complete
        job.write_audit()
```

### With Incomplete Task Recovery

```python
def get_incomplete_tasks(job) -> list[Task]:
    """Get tasks that were claimed but never completed.
    
    Returns tasks claimed >5 minutes ago with no audit entry.
    """
    sql = f'''
    SELECT DISTINCT c.item
    FROM {job._tables["Claim"]} c
    LEFT JOIN {job._tables["Audit"]} a 
        ON c.item = a.item AND a.date = %s
    WHERE a.item IS NULL
    AND c.created_on < %s
    '''
    cutoff = datetime.now() - timedelta(minutes=5)
    incomplete = db.select(job._cn, sql, job.date, cutoff)
    
    return [Task(row['item']) for row in incomplete.to_dict('records')]

def process_with_recovery():
    """Process tasks with recovery of incomplete work.
    """
    with Job(
        node_name='worker-01',
        site='production',
        config=config,
        wait_on_enter=120
    ) as job:
        # First, recover any incomplete tasks from dead nodes
        incomplete_tasks = get_incomplete_tasks(job)
        if incomplete_tasks:
            logger.warning(f'Found {len(incomplete_tasks)} incomplete tasks, reprocessing')
            for task in incomplete_tasks:
                if job.can_claim_task(task):
                    job.add_task(task)
                    process_task(task.id)
        
        # Then process new tasks normally
        for task_id in get_new_tasks():
            task = Task(task_id)
            if job.can_claim_task(task):
                job.add_task(task)
                process_task(task_id)
        
        job.write_audit()
```

### Idempotent Task Processing

```python
def process_task_idempotent(task_id: int, cn):
    """Process task in an idempotent way.
    
    Safe to call multiple times - won't duplicate results.
    """
    # Check if already completed
    sql = "SELECT COUNT(*) FROM results WHERE task_id = %s"
    if db.select_scalar(cn, sql, task_id) > 0:
        logger.debug(f'Task {task_id} already completed, skipping')
        return
    
    # Process with transaction for atomicity
    with cn.transaction():
        # Do the work
        result = compute_expensive_result(task_id)
        
        # Save result (with UNIQUE constraint on task_id)
        sql = """
        INSERT INTO results (task_id, result, completed_at)
        VALUES (%s, %s, %s)
        ON CONFLICT (task_id) DO NOTHING
        """
        db.execute(cn, sql, task_id, result, datetime.now())

def process_idempotent_tasks():
    """Process tasks with idempotent operations.
    """
    with Job(
        node_name='worker-01',
        site='production',
        config=config,
        wait_on_enter=120
    ) as job:
        for task_id in get_tasks():
            if job.can_claim_task(Task(task_id)):
                # Safe to retry if node dies mid-processing
                process_task_idempotent(task_id, job._cn)
```

### Batch Processing Pattern

```python
def process_in_batches():
    """Process tasks in batches for efficiency.
    """
    with Job(
        node_name='worker-01',
        site='production',
        config=config,
        wait_on_enter=120
    ) as job:
        all_tasks = get_all_tasks()
        batch = []
        batch_size = 100
        
        for task_id in all_tasks:
            task = Task(task_id)
            
            if job.can_claim_task(task):
                job.add_task(task)
                batch.append(task_id)
                
                # Process batch when full
                if len(batch) >= batch_size:
                    process_batch(batch)
                    batch = []
        
        # Process remaining tasks
        if batch:
            process_batch(batch)
        
        job.write_audit()
```

### Continuous Processing Loop

```python
def continuous_processing():
    """Continuously process tasks until none remain.
    """
    with Job(
        node_name='worker-01',
        site='production',
        config=config,
        wait_on_enter=120
    ) as job:
        iteration = 0
        
        while True:
            iteration += 1
            logger.info(f'Starting iteration {iteration}')
            
            # Get pending tasks
            tasks = get_pending_tasks(job.date)
            
            processed_count = 0
            for task_id in tasks:
                task = Task(task_id)
                
                if job.can_claim_task(task):
                    job.add_task(task)
                    process_task(task_id)
                    processed_count += 1
            
            # Write audit after each iteration
            if processed_count > 0:
                job.write_audit()
                logger.info(f'Processed {processed_count} tasks')
            
            # Exit if no tasks found
            if processed_count == 0:
                logger.info('No more tasks to process')
                break
            
            # Brief delay before next iteration
            time.sleep(5)
```

## Health Monitoring

### Load Balancer Health Check

```python
from flask import Flask, jsonify

app = Flask(__name__)
job_instance = None  # Set during application startup

@app.route('/health/ready')
def health_ready():
    """Readiness check for load balancer.
    
    Returns 200 if node is healthy and processing tasks.
    """
    if job_instance and job_instance.am_i_healthy():
        return jsonify({
            'status': 'ready',
            'node': job_instance.node_name,
            'token_count': len(job_instance._my_tokens)
        }), 200
    else:
        return jsonify({'status': 'not_ready'}), 503

@app.route('/health/live')
def health_live():
    """Liveness check for load balancer.
    
    Returns 200 if process is alive.
    """
    return jsonify({'status': 'alive'}), 200

def main():
    """Main application with health checks.
    """
    global job_instance
    
    with Job(
        node_name='worker-01',
        site='production',
        config=config,
        wait_on_enter=120
    ) as job:
        job_instance = job
        
        # Start Flask in background thread
        from threading import Thread
        health_thread = Thread(
            target=lambda: app.run(host='0.0.0.0', port=8080)
        )
        health_thread.daemon = True
        health_thread.start()
        
        # Process tasks
        process_tasks(job)
```

### Manual Health Check

```python
def check_node_health(job: Job) -> dict:
    """Manual health check for debugging.
    """
    return {
        'node_name': job.node_name,
        'is_healthy': job.am_i_healthy(),
        'is_leader': job.am_i_leader(),
        'token_count': len(job._my_tokens),
        'token_version': job._token_version,
        'last_heartbeat': job._last_heartbeat_sent,
        'active_nodes': len(job.get_active_nodes()),
        'coordination_enabled': job._coordination_enabled
    }

def process_with_health_logging():
    """Log health status during processing.
    """
    with Job(
        node_name='worker-01',
        site='production',
        config=config,
        wait_on_enter=120
    ) as job:
        # Log initial health
        health = check_node_health(job)
        logger.info(f'Node health: {health}')
        
        # Process tasks
        process_tasks(job)
        
        # Log final health
        health = check_node_health(job)
        logger.info(f'Final health: {health}')
```

## Database Queries

### Cluster State Queries

**Note:** Replace `sync_` with your custom prefix if using `config.sync.sql.appname`.

**Active Nodes:**
```sql
SELECT 
    name,
    created_on,
    last_heartbeat,
    NOW() - last_heartbeat as time_since_heartbeat,
    CASE 
        WHEN last_heartbeat > NOW() - INTERVAL '15 seconds' THEN 'ACTIVE'
        ELSE 'DEAD'
    END as status
FROM sync_node
ORDER BY created_on;
```

**Token Distribution:**
```sql
SELECT 
    node,
    COUNT(*) as token_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) as pct,
    MIN(token_id) as min_token,
    MAX(token_id) as max_token
FROM sync_token
GROUP BY node
ORDER BY token_count DESC;
```

**Token Distribution Balance:**
```sql
WITH stats AS (
    SELECT 
        node,
        COUNT(*) as token_count
    FROM sync_token
    GROUP BY node
)
SELECT 
    MAX(token_count) - MIN(token_count) as max_imbalance,
    AVG(token_count) as avg_tokens,
    STDDEV(token_count) as stddev_tokens
FROM stats;
```

**Current Leader:**
```sql
SELECT 
    name as leader_node,
    created_on,
    last_heartbeat
FROM sync_node
WHERE last_heartbeat > NOW() - INTERVAL '15 seconds'
ORDER BY created_on
LIMIT 1;
```

### Lock Queries

**View All Locks:**
```sql
SELECT 
    node_pattern,
    COUNT(*) as locked_token_count,
    reason,
    expires_at,
    CASE 
        WHEN expires_at IS NULL THEN 'never'
        WHEN expires_at < NOW() THEN 'EXPIRED'
        ELSE 'active'
    END as status,
    STRING_AGG(DISTINCT created_by, ',') as registered_by
FROM sync_lock
GROUP BY node_pattern, reason, expires_at
ORDER BY locked_token_count DESC;
```

**Orphaned Locks (by dead creator node)**:
```sql
SELECT 
    l.token_id,
    l.node_pattern,
    l.created_by,
    l.reason,
    l.created_at,
    NOW() - l.created_at as orphaned_duration
FROM sync_lock l
LEFT JOIN sync_node n ON l.created_by = n.name
WHERE n.name IS NULL  -- Creator node no longer exists
   OR n.last_heartbeat < NOW() - INTERVAL '1 hour'  -- Creator dead
ORDER BY l.created_at;
```

**Incorrectly Assigned Locks**:
```sql
-- Locks assigned to nodes that don't match the pattern
SELECT 
    l.token_id,
    l.node_pattern,
    t.node as actual_node,
    l.reason,
    l.created_by
FROM sync_lock l
JOIN sync_token t ON l.token_id = t.token_id
WHERE NOT (t.node LIKE l.node_pattern)
ORDER BY l.token_id;
```

### Rebalance History

**Recent Rebalances:**
```sql
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
```

**Rebalance Frequency:**
```sql
SELECT 
    DATE_TRUNC('hour', triggered_at) as hour,
    COUNT(*) as rebalance_count
FROM sync_rebalance
WHERE triggered_at > NOW() - INTERVAL '24 hours'
GROUP BY hour
ORDER BY hour DESC;
```

### Task Processing Queries

**Tasks by Node:**
```sql
SELECT 
    node,
    COUNT(*) as task_count,
    MIN(created_on) as first_claim,
    MAX(created_on) as last_claim
FROM sync_claim
WHERE created_on::date = CURRENT_DATE
GROUP BY node
ORDER BY task_count DESC;
```

**Incomplete Tasks:**
```sql
SELECT 
    c.item,
    c.node,
    c.created_on,
    NOW() - c.created_on as age
FROM sync_claim c
LEFT JOIN sync_audit a ON c.item = a.item AND a.date = CURRENT_DATE
WHERE c.created_on::date = CURRENT_DATE
AND a.item IS NULL
ORDER BY c.created_on;
```

**Task Completion Rate:**
```sql
WITH claimed AS (
    SELECT COUNT(DISTINCT item) as claimed_count
    FROM sync_claim
    WHERE created_on::date = CURRENT_DATE
),
audited AS (
    SELECT COUNT(DISTINCT item) as audited_count
    FROM sync_audit
    WHERE date = CURRENT_DATE
)
SELECT 
    c.claimed_count,
    a.audited_count,
    c.claimed_count - a.audited_count as incomplete_count,
    ROUND(100.0 * a.audited_count / NULLIF(c.claimed_count, 0), 1) as completion_pct
FROM claimed c, audited a;
```

### Maintenance Queries

**Remove Stale Nodes:**
```sql
-- Remove nodes with heartbeats older than 1 hour
DELETE FROM sync_node 
WHERE last_heartbeat < NOW() - INTERVAL '1 hour';
```

**Clean Orphaned Tokens:**
```sql
-- Remove tokens assigned to non-existent nodes
DELETE FROM sync_token 
WHERE node NOT IN (SELECT name FROM sync_node);
```

**Clean Old Rebalance Records:**
```sql
-- Keep last 30 days
DELETE FROM sync_rebalance
WHERE triggered_at < NOW() - INTERVAL '30 days';
```

**Clean Orphaned Locks:**
```sql
-- Remove locks created by nodes that no longer exist
DELETE FROM sync_lock
WHERE created_by IN (
    SELECT l.created_by
    FROM sync_lock l
    LEFT JOIN sync_node n ON l.created_by = n.name
    WHERE n.name IS NULL
);
```

**Clean Expired Locks:**
```sql
-- Remove expired locks (normally done automatically)
DELETE FROM sync_lock
WHERE expires_at < NOW();
```

## Best Practices

### 1. Always Use Graceful Shutdown

```python
import signal
import sys

job_instance = None

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully.
    """
    logger.info(f'Received signal {signum}, shutting down gracefully')
    if job_instance:
        job_instance.__exit__(None, None, None)
    sys.exit(0)

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

with Job(...) as job:
    job_instance = job
    process_tasks(job)
```

### 2. Use Appropriate Wait Times

```python
# Default: 120 seconds for coordination mode
with Job(..., wait_on_enter=120) as job:
    pass

# Shorter for small clusters (2-3 nodes)
with Job(..., wait_on_enter=60) as job:
    pass

# Longer for large clusters (10+ nodes)
with Job(..., wait_on_enter=180) as job:
    pass
```

### 3. Implement Idempotent Processing

```python
def process_task_safely(task_id: int, cn):
    """Always make task processing idempotent.
    """
    # Check if already done
    if is_completed(task_id, cn):
        return
    
    # Use transaction for atomicity
    with cn.transaction():
        result = compute_result(task_id)
        save_result(task_id, result)
```

### 4. Use Bulk Lock Registration

```python
# GOOD: Bulk registration
def register_locks(job):
    locks = [(task_id, node, reason) for task_id in get_special_tasks()]
    job.register_task_locks_bulk(locks)

# BAD: Individual registration in loop
def register_locks(job):
    for task_id in get_special_tasks():
        job.register_task_lock(task_id, node, reason)  # Slow!
```

### 5. Monitor Rebalance Frequency

```python
def check_rebalance_frequency(cn):
    """Alert if rebalancing too frequently.
    """
    sql = """
    SELECT COUNT(*) as rebalance_count
    FROM sync_rebalance
    WHERE triggered_at > NOW() - INTERVAL '1 hour'
    """
    count = db.select_scalar(cn, sql)
    
    if count > 5:
        logger.warning(f'High rebalance frequency: {count} in last hour')
        # Alert operations team
```

### 6. Use Descriptive Node Names

```python
# GOOD: Descriptive names
node_name = f'prod-worker-{region}-{instance_id}'

# BAD: Generic names
node_name = 'worker-1'
```

### 7. Document Lock Reasons

```python
def register_locks(job):
    """Register locks with clear reasons.
    """
    # GPU requirement
    job.register_task_lock(
        task_id=100,
        node_pattern='%-gpu',
        reason='requires_nvidia_v100'
    )
    
    # Memory requirement
    job.register_task_lock(
        task_id=200,
        node_pattern='high-mem-%',
        reason='requires_64gb_ram'
    )
    
    # Data locality
    job.register_task_lock(
        task_id=300,
        node_pattern='us-east-%',
        reason='data_in_us_east_s3'
    )
```

### 8. Manage Lock Lifecycle

```python
def manage_lock_lifecycle():
    """Use clear_existing_locks for dynamic lock logic.
    """
    def dynamic_locks(job):
        # Lock logic that changes based on current conditions
        heavy_tasks = get_current_heavy_tasks()
        locks = [(t, 'high-mem-%', 'heavy') for t in heavy_tasks]
        job.register_task_locks_bulk(locks)
    
    # Clear old locks before registering new ones
    with Job('worker-01', 'production', config,
             lock_provider=dynamic_locks,
             clear_existing_locks=True) as job:
        process_tasks(job)

def cleanup_orphaned_locks():
    """Clean up locks from decommissioned nodes.
    """
    with Job('admin', 'production', config) as job:
        # List all locks to review
        locks = job.list_locks()
        for lock in locks:
            if lock['created_by'].startswith('old-'):
                job.clear_locks_by_creator(lock['created_by'])
```

### 8. Implement Health Checks

```python
def main():
    """Always implement health checks for production.
    """
    with Job(...) as job:
        # Start health check endpoint
        start_health_server(job)
        
        # Process tasks
        while True:
            if not job.am_i_healthy():
                logger.error('Node unhealthy, exiting')
                break
            
            process_tasks(job)
```

### 9. Log Meaningful Information

```python
def process_tasks_with_logging(job):
    """Use logging effectively.
    """
    logger.info(f'Starting processing: {len(job._my_tokens)} tokens assigned')
    
    processed = 0
    for task_id in get_tasks():
        if job.can_claim_task(Task(task_id)):
            process_task(task_id)
            processed += 1
    
    logger.info(f'Completed {processed} tasks')
    logger.debug(f'Token version: {job._token_version}')
```

### 10. Use clear_existing_locks for Dynamic Locks

```python
def process_with_dynamic_locks():
    """Always use clear_existing_locks=True when lock logic changes.
    """
    def register_locks(job):
        # This logic changes each run based on current state
        tasks_needing_gpu = query_gpu_requirements(job.date)
        locks = [(t, '%-gpu', 'gpu') for t in tasks_needing_gpu]
        job.register_task_locks_bulk(locks)
    
    with Job(
        node_name='worker-01',
        site='production',
        config=config,
        lock_provider=register_locks,
        clear_existing_locks=True  # IMPORTANT: prevents stale lock accumulation
    ) as job:
        process_tasks(job)
```

### 11. Test in Staging First

```python
def test_coordination(config):
    """Test coordination with same node count as production.
    """
    # Use staging config with production-like settings
    coord_config = CoordinationConfig(
        total_tokens=10000,  # Same as production
        heartbeat_interval_sec=5,  # Same as production
        # ... other production settings
    )
    
    with Job(
        node_name='staging-worker-01',
        site='staging',
        config=config,
        coordination_config=coord_config
    ) as job:
        # Test with production-like workload
        test_task_processing(job)
```

---

**Document Version**: 1.0  
**Last Updated**: 2024-01-18
