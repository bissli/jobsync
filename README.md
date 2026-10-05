# JobSync - Distributed Task Coordination

A Python library for coordinating distributed task processing across multiple worker nodes with automatic load balancing, failover, and zero-downtime deployments.

## Overview

JobSync coordinates multiple worker processes so each task is processed exactly once, even when workers come and go. It uses PostgreSQL for state management and provides:

- **Automatic load balancing** - Tasks distributed evenly across all active workers
- **Zero-downtime deployments** - Workers join and leave without stopping processing
- **Automatic failover** - When workers die, their tasks are reassigned
- **Task pinning** - Locks pin specific tasks to specific workers (e.g., GPU tasks to GPU nodes)
- **Health monitoring** - Built-in health checks for load balancers

## Quick Start

### Installation

```bash
pip install jobsync
```

### Basic Example

```python
from jobsync import Job, Task, CoordinationConfig

# Configure coordination
coord_config = CoordinationConfig(
    host='localhost',
    dbname='jobsync',
    user='postgres',
    password='postgres',
    appname='myapp_'  # Prefix for database tables
)

# Each worker runs this code
with Job('worker-01', coordination_config=coord_config) as job:
    for item_id in get_pending_items():
        task = Task(item_id)
        
        # Only process if this task belongs to this worker
        if job.can_claim_task(task):
            job.add_task(task)
            process_item(item_id)
```

### Long-Running Task Example

```python
from jobsync import Job, CoordinationConfig

# For subscriptions, WebSockets, or continuous processing
def on_rebalance():
    """Called when cluster membership changes.
    
    The callback re-evaluates which tasks this node should process and
    updates long-running connections or subscriptions accordingly.
    """
    current_tokens = job.my_tokens
    # Update subscriptions/connections from token ownership
    logger.info(f'Rebalance: now own {len(current_tokens)} tokens')

coord_config = CoordinationConfig(
    host='localhost',
    dbname='jobsync',
    user='postgres',
    password='postgres',
    appname='myapp_'
)

with Job('worker-01', coordination_config=coord_config,
         on_rebalance=on_rebalance) as job:
    # The callback reports cluster changes
    while not shutdown:
        time.sleep(1)
```

Multiple workers coordinate automatically:

```bash
# Terminal 1
python worker.py --node-name worker-01

# Terminal 2  
python worker.py --node-name worker-02

# Terminal 3
python worker.py --node-name worker-03
```

Each worker processes different tasks - no duplicate work.

## Design

**Token-Based Distribution**: JobSync divides a pool of 10,000 tokens evenly across all active workers. Each task hashes consistently to exactly one token, so each worker knows which tasks it owns.

**Leader-Based Coordination**: The oldest worker becomes the leader and monitors cluster health. When workers join or leave, the leader redistributes tokens to maintain balance.

**Consistent Hashing**: Tasks always hash to the same token, so the same task always goes to the same worker (until the cluster changes).

**Lifecycle**:
1. Workers register and send heartbeat every 5 seconds
2. Leader is elected (oldest worker by registration time)
3. Leader distributes tokens evenly across all workers
4. Workers claim only tasks that hash to their tokens
5. Leader monitors for dead workers and triggers rebalancing
6. When leader dies, next oldest worker becomes leader

## Configuration

`CoordinationConfig` holds the database connection and coordination settings:

```python
from jobsync import CoordinationConfig

coord_config = CoordinationConfig(
    host='localhost',
    port=5432,
    dbname='jobsync',
    user='postgres',
    password='postgres',
    appname='myapp_',      # Prefix for database tables (e.g., myapp_node, myapp_token)
    total_tokens=10000,    # Optional: customize token pool size
    hash_function='double_sha256'  # Optional: 'md5', 'sha256', or 'double_sha256' (default, recommended)
)
```

The **[Usage Guide](docs/USAGE_GUIDE.md#coordination-configuration)** covers the configuration options in detail, and the **[Operator Guide](docs/OPERATOR_GUIDE.md#performance-tuning)** covers tuning parameters.

## Usage Examples

### Basic Task Processing

```python
from jobsync import Job, Task, CoordinationConfig

coord_config = CoordinationConfig(
    host='localhost',
    dbname='jobsync',
    user='postgres',
    password='postgres',
    appname='myapp_'
)

with Job('worker-01', coordination_config=coord_config) as job:
    for item_id in get_pending_items():
        task = Task(item_id)
        if job.can_claim_task(task):
            job.add_task(task)
            process_item(item_id)
    job.write_audit()
```

### Task Pinning (GPU tasks on GPU workers)

```python
from jobsync import Job, Task, CoordinationConfig

def register_gpu_locks(job):
    gpu_tasks = get_gpu_task_ids()
    locks = [(task_id, '%gpu%', 'requires_gpu') for task_id in gpu_tasks]
    job.register_locks_bulk(locks)

coord_config = CoordinationConfig(
    host='localhost',
    dbname='jobsync',
    user='postgres',
    password='postgres',
    appname='myapp_'
)

with Job('worker-gpu-01', coordination_config=coord_config,
         lock_provider=register_gpu_locks) as job:
    for task_id in get_all_tasks():
        if job.can_claim_task(Task(task_id)):
            process_gpu_task(task_id)
```

The **[Usage Guide](docs/USAGE_GUIDE.md)** holds complete examples including WebSocket subscriptions, Kafka consumers, ETL pipelines, and more.

## Key Features

### Automatic Load Balancing and Failover

- **Token-based distribution** - 10,000 tokens evenly distributed across all active workers
- **Consistent hashing** - Each task always hashes to the same token
- **Automatic rebalancing** - When workers join/leave, tokens are redistributed with minimal movement
- **Leader-based coordination** - Oldest worker manages cluster state and triggers rebalancing
- **Zero-downtime deployments** - New workers start and receive tokens, then old workers stop

### Task Locking and Pinning

Locks pin specific tasks to specific workers using SQL LIKE patterns with ordered fallback support. Locks are registered by `task_id` and stored in the database. During distribution, each `task_id` is hashed to a `token_id`, and lock patterns determine which node receives that token.

```python
from jobsync import Job, CoordinationConfig

def register_locks(job):
    gpu_tasks = get_gpu_task_ids()
    
    # Ordered fallback: try primary GPU first, then any GPU node
    locks = [
        (task_id, ['gpu-primary-01', '%-gpu'], 'requires_gpu')
        for task_id in gpu_tasks
    ]
    job.register_locks_bulk(locks)

coord_config = CoordinationConfig(
    host='localhost',
    dbname='jobsync',
    user='postgres',
    password='postgres',
    appname='myapp_'
)

with Job('worker-gpu-01', coordination_config=coord_config,
         lock_provider=register_locks,
         clear_existing_locks=True) as job:
    process_tasks(job)
```

**Use cases:**
- GPU tasks pinned to GPU-enabled workers
- High-memory tasks routed to large-memory nodes
- Data locality (tasks process data in the same region)
- Resource-aware scheduling

The **[Usage Guide](docs/USAGE_GUIDE.md#task-locking)** covers the complete lock API including pattern matching, fallback chains, expiration, and lifecycle management.

### Health Monitoring and Custom Configuration

**Health checks for load balancers:**
```python
from flask import Flask, jsonify
from jobsync import Job, CoordinationConfig

@app.route('/health/ready')
def health():
    if job.am_i_healthy():
        return jsonify({'status': 'ready'}), 200
    return jsonify({'status': 'not_ready'}), 503
```

**Coordination tuning:**
```python
coord_config = CoordinationConfig(
    host='localhost',
    dbname='jobsync',
    user='postgres',
    password='postgres',
    appname='myapp_',
    total_tokens=50000,                   # More tokens for large clusters
    minimum_nodes=2,                      # Wait for 2 nodes before distribution
    heartbeat_interval_sec=3,             # Faster heartbeat
    heartbeat_timeout_sec=9,              # Quicker failure detection
    rebalance_check_interval_sec=15       # More responsive rebalancing
)
```

The **[Usage Guide](docs/USAGE_GUIDE.md#coordination-configuration)** and **[Operator Guide](docs/OPERATOR_GUIDE.md#performance-tuning)** hold tuning recommendations.

## Documentation

### Guides

**[Usage Guide](docs/USAGE_GUIDE.md)** - Developer reference with examples, configuration, and patterns

**[Operator Guide](docs/OPERATOR_GUIDE.md)** - Deployment, monitoring, and troubleshooting

**[SQL Cheatsheet](docs/CheatSheet.sql)** - Common monitoring queries


## Monitoring

Cluster health check:

```sql
-- Quick health check
SELECT COUNT(*) as active_nodes FROM sync_node
WHERE last_heartbeat > NOW() - INTERVAL '15 seconds';
```

The **[Operator Guide](docs/OPERATOR_GUIDE.md#monitoring)** covers metrics, alerts, and troubleshooting.

## Performance

- **Memory**: <1 MB per worker
- **Database load**: <10 queries/minute per worker
- **Scalability**: 2-50 workers
- **Rebalancing**: Only ~14% of tokens move when worker fails
- **Distribution speed**: 100-500ms for 10,000 tokens

## Testing

```bash
# Test dependencies
pip install pytest pytest-cov

# All tests
pytest tests/

# Coverage
pytest --cov=jobsync tests/

# One test suite
pytest tests/test_unit.py -v
```

## Requirements

- Python 3.11+
- PostgreSQL 12+
- Required packages: psycopg (psycopg3), sqlalchemy (installed automatically)

## License

MIT

## Support

- **Issues**: https://github.com/bissli/jobsync/issues
- **Documentation**: the `docs/` folder
- **Examples**: the `tests/` folder
