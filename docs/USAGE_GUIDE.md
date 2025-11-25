# Usage Guide - JobSync

## Table of Contents

1. [Overview](#overview)
2. [Basic Usage](#basic-usage)
3. [Configuration](#configuration)
4. [Task Locking](#task-locking)
5. [Long-Running Tasks](#long-running-tasks)
6. [Monitoring](#monitoring)
7. [Best Practices](#best-practices)

## Overview

JobSync coordinates multiple worker processes so each task is processed exactly once. Workers automatically balance load and handle failures.

**Key Concepts:**
- **Nodes**: Worker processes that register and process tasks
- **Tokens**: Ownership units (default 10,000) that determine which tasks a node can claim
- **Leader**: Oldest node that manages cluster coordination
- **Locks**: Optional pinning of specific tasks to specific nodes

**Common Imports:**

```python
from jobsync import Job, Task, CoordinationConfig
```

## Basic Usage

### Simple Example

```python
from jobsync import Job, Task, CoordinationConfig

coord_config = CoordinationConfig(
    host='localhost',
    dbname='jobsync',
    user='postgres',
    password='postgres',
    appname='myapp_',  # Prefix for database tables
    minimum_nodes=3    # Wait for 3 nodes before processing
)

with Job('worker-01', coordination_config=coord_config, 
         wait_on_enter=60) as job:  # 60s grace period for cluster formation
    for task_id in get_pending_tasks():
        task = Task(task_id)
        
        # Only process if this task belongs to our tokens
        if job.can_claim_task(task):
            job.add_task(task)
            process_task(task_id)
    
    job.write_audit()
```

**Note for Batch Workloads**: If all tasks exist upfront (not streaming), set `minimum_nodes` to your expected cluster size and use an appropriate `wait_on_enter` grace period. The full grace period always completes to allow all nodes time to register, ensuring fair distribution - otherwise early-starting nodes claim all tasks before late joiners arrive.

### Without Coordination

For single-node deployments or testing:

```python
with Job('worker-01', coordination_config=None) as job:
    # All tasks processed by this node
    for task_id in get_all_tasks():
        process_task(task_id)
```

## Configuration

### CoordinationConfig Parameters

All timing parameters have sensible defaults. Only customize when needed.

```python
coord_config = CoordinationConfig(
    # Database (required)
    host='localhost',
    port=5432,
    dbname='jobsync',
    user='postgres',
    password='postgres',
    appname='myapp_',  # Table prefix
    
    # Cluster coordination
    total_tokens=10000,             # Default: 10000 (lower for testing: 1000-5000)
    minimum_nodes=1,                # Wait for N nodes before distribution
    
    # Timing (optional tuning)
    heartbeat_interval_sec=5,       # Default: 5
    heartbeat_timeout_sec=15,       # Default: 15
    rebalance_check_interval_sec=30 # Default: 30
)
```

**Critical for Batch Workloads**: Set `minimum_nodes` to your expected cluster size (e.g., 7 for a 7-node deployment) and use `wait_on_enter=60` (or higher for slow startups). The full grace period always completes to allow all nodes time to register, preventing early nodes from claiming all tasks before the cluster forms.

**Common Tuning Scenarios:**

| Scenario | total_tokens | minimum_nodes | wait_on_enter | heartbeat_interval_sec | heartbeat_timeout_sec |
|----------|--------------|---------------|---------------|------------------------|----------------------|
| Default (2-10 nodes) | 10000 | 1 | 120 | 5 | 15 |
| Batch workload (all tasks upfront) | 10000 | N (cluster size) | 60-120 | 5 | 15 |
| Large cluster (10+ nodes) | 50000 | N (cluster size) | 120 | 3 | 10 |
| Stable cluster | 10000 | 1 | 120 | 10 | 30 |
| High churn (K8s/spot) | 10000 | 1 | 60 | 3 | 9 |
| Testing/dev | 1000-5000 | 1 | 30 | 5 | 15 |

### Choosing Token Count

**What Are Tokens?**

Tokens are the fundamental unit of work distribution in JobSync. Each task is hashed to a specific token, and nodes claim tasks based on which tokens they own.

**The Core Principle: Sampling Theory**

Higher token counts reduce variance in task distribution across nodes:
- With **10,000 tokens** and 600 tasks: Each node samples from ~1,428 tokens, providing stable distribution
- With **600 tokens** and 600 tasks: Each node samples from ~86 tokens, creating high variance

**The Formula**

For production workloads, use a 10:1 ratio of tokens to tasks:

```
total_tokens = max(expected_tasks × 10, expected_nodes × 100, 10000)
```

**Lookup Table (Recommended Tokens by Task Count and Node Count)**

| Expected Tasks | 2 Nodes | 5 Nodes | 10 Nodes | 20 Nodes | 50 Nodes |
| -------------: | ------: | ------: | -------: | -------: | -------: |
| 1,000          | 10,000  | 10,000  | 10,000   | 10,000   | 10,000   |
| 2,500          | 25,000  | 25,000  | 25,000   | 25,000   | 25,000   |
| 5,000          | 50,000  | 50,000  | 50,000   | 50,000   | 50,000   |
| 7,500          | 75,000  | 75,000  | 75,000   | 75,000   | 75,000   |
| 10,000         | 100,000 | 100,000 | 100,000  | 100,000  | 100,000  |
| 50,000         | 500,000 | 500,000 | 500,000  | 500,000  | 500,000  |

**Key Insight**: With a 10:1 token-to-task ratio, node count has minimal impact on the recommended token count. The task count dominates the calculation in all practical scenarios.

**When to Use Lower Token Counts**

You can use fewer tokens (1,000-5,000) for:
- **Testing/development environments**: Reduces database rows for easier inspection
- **Very stable workloads**: Where task distribution variance is acceptable
- **Single-node deployments**: Token count is irrelevant without coordination

**When to Use Higher Token Counts**

Always use 10,000+ tokens for:
- **Production deployments**: Ensures consistent task distribution
- **Batch workloads**: Where uneven distribution is immediately visible
- **Not sure**: Use default 10,000 tokens

**Performance Considerations**

- **More tokens = better balance** but with diminishing returns beyond 10:1 ratio
- **Database impact**: Token count = row count in `sync_token` table
- **Default (10,000)** handles up to 10 nodes with excellent distribution
- **Maximum (1,000,000)** for very large deployments (20+ nodes, 100,000+ tasks)

### Hash Function Selection

JobSync uses consistent hashing to map tasks to tokens. Three algorithms are available:

```python
coord_config = CoordinationConfig(
    # ... other parameters
    hash_function='double_sha256'  # Options: 'md5', 'sha256', 'double_sha256'
)
```

**Available Options:**
- **`double_sha256`** (default): Best distribution quality, recommended for production
- **`sha256`**: Good distribution, moderate variance
- **`md5`**: Fastest but higher variance, not recommended for production

**When to Change:**
- Default (`double_sha256`) is recommended for all production workloads
- Only change if you have specific performance requirements or constraints
- See docstrings in `src/jobsync/client.py` for detailed performance characteristics

**Impact:** Hash function determines which token owns each task. Changing this after locks are registered will invalidate existing lock mappings, as task IDs will hash to different tokens.

## Task Locking

Lock specific tasks to specific nodes using pattern matching with ordered fallback support.

### Basic Lock Usage

```python
def register_gpu_locks(job):
    """Lock GPU tasks to GPU-enabled nodes.
    """
    gpu_tasks = get_gpu_task_ids()
    
    # Single pattern
    locks = [(task_id, '%-gpu', 'requires_gpu') for task_id in gpu_tasks]
    
    job.register_locks_bulk(locks)

with Job('worker-gpu-01', coordination_config=coord_config,
         lock_provider=register_gpu_locks) as job:
    for task_id in get_all_tasks():
        if job.can_claim_task(Task(task_id)):
            process_task(task_id)
```

### Fallback Patterns

```python
def register_locks_with_fallback(job):
    """Use ordered fallback for high availability.
    """
    # Try primary GPU first, then any GPU node
    job.register_lock(
        task_id=100,
        node_patterns=['gpu-primary-01', '%-gpu'],
        reason='prefer_primary'
    )
    
    # Regional fallback
    job.register_lock(
        task_id=200,
        node_patterns=['us-east-1-%', 'us-east-%', 'us-%'],
        reason='data_locality'
    )
```

### Lock Lifecycle

**Dynamic locks** (logic changes each run):

```python
def register_locks(job):
    heavy_tasks = get_todays_heavy_tasks()
    locks = [(t, 'high-mem-%', 'heavy') for t in heavy_tasks]
    job.register_locks_bulk(locks)

with Job('worker-01', coordination_config=coord_config,
         lock_provider=register_locks,
         clear_existing_locks=True) as job:  # Clear stale locks
    process_tasks(job)
```

**Static locks** (persistent):

```python
def register_locks(job):
    job.register_lock('customer-X', 'east-%', 'data_locality')

with Job('worker-01', coordination_config=coord_config,
         lock_provider=register_locks,
         clear_existing_locks=False) as job:  # Keep existing
    process_tasks(job)
```

## Long-Running Tasks

For WebSockets, subscriptions, or continuous processing, use the `on_rebalance` callback:

```python
def on_rebalance():
    """Called when cluster membership changes or tokens are rebalanced.
    
    Re-evaluate which tasks this node should be processing and update
    subscriptions, connections, or other long-running resources accordingly.
    """
    current_tokens = job.my_tokens
    # Update your subscriptions/connections based on new token ownership
    logger.info(f'Rebalance: now own {len(current_tokens)} tokens')

with Job('worker-01', coordination_config=coord_config,
         on_rebalance=on_rebalance) as job:
    while not shutdown:
        time.sleep(1)
```

**Callback rules:**
- Called on initial token assignment and whenever cluster membership changes
- Must complete quickly (<1 second) or delegate work to background threads
- Always handle exceptions - don't let callback failures crash the node
- Use `job.my_tokens` to determine current token ownership

## Monitoring

### Health Checks

```python
from flask import Flask, jsonify

@app.route('/health/ready')
def health():
    if job.am_i_healthy():
        return jsonify({'status': 'ready'}), 200
    return jsonify({'status': 'not_ready'}), 503
```

### Useful SQL Queries

See [Cheatsheet.sql](Cheatsheet.sql) for a complete reference.

**Active nodes:**
```sql
SELECT COUNT(*) FROM sync_node 
WHERE last_heartbeat > NOW() - INTERVAL '15 seconds';
```

**Token distribution:**
```sql
SELECT node, COUNT(*) as tokens FROM sync_token GROUP BY node;
```

**Current leader:**
```sql
SELECT name FROM sync_node 
WHERE last_heartbeat > NOW() - INTERVAL '15 seconds'
ORDER BY created_on LIMIT 1;
```

## Best Practices

1. **Use graceful shutdown** - Always handle SIGTERM properly
2. **Make processing idempotent** - Tasks might be retried if node fails
3. **Use bulk lock registration** - More efficient than individual calls
4. **Use descriptive node names** - Include region, hostname, or instance ID
5. **Document lock reasons** - Explain why tasks are pinned
6. **Use clear_existing_locks=True for dynamic locks** - Prevents stale locks
7. **Keep on_rebalance callback fast** - Delegate heavy work to background threads
8. **Handle callback exceptions** - Don't let callback failures crash the node
9. **Monitor rebalance frequency** - >5/hour indicates instability
10. **Test in staging first** - Use same node count and timing as production

---

For operational procedures, troubleshooting, and production maintenance, see the [Operator Guide](OPERATOR_GUIDE.md).
