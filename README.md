# JobSync - Distributed Task Coordination

A Python library for coordinating distributed task processing across multiple worker nodes with automatic load balancing, failover, and zero-downtime deployments.

## What It Does

JobSync coordinates multiple worker processes so each task is processed exactly once, even when workers come and go. It uses PostgreSQL for state management and provides:

- **Automatic load balancing** - Tasks distributed evenly across all active workers
- **Zero-downtime deployments** - Add/remove workers without stopping processing
- **Automatic failover** - When workers die, their tasks are automatically reassigned
- **Task pinning** - Lock specific tasks to specific workers (e.g., GPU tasks to GPU nodes)
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
active_subscriptions = {}

def on_tokens_added(token_ids: set[int]):
    """Start subscriptions for newly assigned tokens."""
    for token_id in token_ids:
        task_ids = job.get_task_ids_for_token(token_id, all_task_ids)
        for task_id in task_ids:
            subscription = start_websocket_subscription(task_id)
            active_subscriptions[task_id] = subscription

def on_tokens_removed(token_ids: set[int]):
    """Stop subscriptions for removed tokens."""
    for token_id in token_ids:
        task_ids = job.get_task_ids_for_token(token_id, all_task_ids)
        for task_id in task_ids:
            if task_id in active_subscriptions:
                active_subscriptions[task_id].close()
                del active_subscriptions[task_id]

coord_config = CoordinationConfig(
    host='localhost',
    dbname='jobsync',
    user='postgres',
    password='postgres',
    appname='myapp_'
)

with Job('worker-01', coordination_config=coord_config,
         on_tokens_added=on_tokens_added,
         on_tokens_removed=on_tokens_removed) as job:
    # Callbacks handle starting/stopping subscriptions
    while not shutdown:
        time.sleep(1)
```

Run multiple workers - they automatically coordinate:

```bash
# Terminal 1
python worker.py --node-name worker-01

# Terminal 2  
python worker.py --node-name worker-02

# Terminal 3
python worker.py --node-name worker-03
```

Each worker processes different tasks - no duplicate work.

## How It Works

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

Configure database connection and coordination settings using `CoordinationConfig`:

```python
from jobsync import CoordinationConfig

coord_config = CoordinationConfig(
    host='localhost',
    port=5432,
    dbname='jobsync',
    user='postgres',
    password='postgres',
    appname='myapp_',  # Prefix for database tables (e.g., myapp_node, myapp_token)
    total_tokens=10000  # Optional: customize token pool size
)
```

**See [Usage Guide](docs/USAGE_GUIDE.md#coordination-configuration)** for detailed configuration options and **[Operator Guide](docs/OPERATOR_GUIDE.md#performance-tuning)** for tuning parameters.

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

### Task Pinning (Lock GPU tasks to GPU workers)

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

**See [Usage Guide](docs/USAGE_GUIDE.md)** for complete examples including WebSocket subscriptions, Kafka consumers, ETL pipelines, and more.

## Key Features

### Automatic Load Balancing and Failover

- **Token-based distribution** - 10,000 tokens evenly distributed across all active workers
- **Consistent hashing** - Each task always hashes to the same token
- **Automatic rebalancing** - When workers join/leave, tokens are redistributed with minimal movement
- **Leader-based coordination** - Oldest worker manages cluster state and triggers rebalancing
- **Zero-downtime deployments** - Add new workers, wait for tokens, then stop old workers

### Task Locking and Pinning

Lock specific tasks to specific workers using SQL LIKE patterns with ordered fallback support. Locks are registered by `task_id` and stored in the database. During distribution, each `task_id` is hashed to a `token_id`, and lock patterns determine which node receives that token.

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
- Pin GPU tasks to GPU-enabled workers
- Route high-memory tasks to large-memory nodes
- Ensure data locality (tasks process data in same region)
- Resource-aware scheduling

**See [Usage Guide](docs/USAGE_GUIDE.md#task-locking-and-pinning)** for complete lock API including pattern matching, fallback chains, expiration, and lifecycle management.

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

**Fine-tune coordination:**
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

**See [Usage Guide](docs/USAGE_GUIDE.md#coordination-configuration)** and **[Operator Guide](docs/OPERATOR_GUIDE.md#performance-tuning)** for tuning recommendations.

## Documentation

### 📚 Documentation

**[Usage Guide](docs/USAGE_GUIDE.md)** - Developer reference with examples, configuration, and patterns

**[Operator Guide](docs/OPERATOR_GUIDE.md)** - Deployment, monitoring, and troubleshooting

**[SQL Cheatsheet](docs/Cheatsheet.sql)** - Common monitoring queries


## Monitoring

Check cluster health:

```sql
-- Quick health check
SELECT COUNT(*) as active_nodes FROM sync_node
WHERE last_heartbeat > NOW() - INTERVAL '15 seconds';
```

**See [Operator Guide](docs/OPERATOR_GUIDE.md#monitoring)** for metrics, alerts, and troubleshooting.

## Performance

- **Memory**: <1 MB per worker
- **Database load**: <10 queries/minute per worker
- **Scalability**: Tested with 2-50 workers
- **Rebalancing**: Only ~14% of tokens move when worker fails
- **Distribution speed**: 100-500ms for 10,000 tokens

## Testing

```bash
# Install test dependencies
pip install pytest pytest-cov

# Run all tests
pytest tests/

# Run with coverage
pytest --cov=jobsync tests/

# Run specific test suite
pytest tests/test_coordination.py -v
```

## Requirements

- Python 3.10+
- PostgreSQL 12+
- Required packages: psycopg (psycopg3), sqlalchemy (installed automatically)

## License

MIT

## Support

- **Issues**: https://github.com/bissli/jobsync/issues
- **Documentation**: See `docs/` folder
- **Examples**: See `tests/` folder
