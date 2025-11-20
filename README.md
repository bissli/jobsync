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
from jobsync import Job, Task, config

# Each worker runs this code
with Job('worker-01', config) as job:
    for item_id in get_pending_items():
        task = Task(item_id)
        
        # Only process if this task belongs to this worker
        if job.can_claim_task(task):
            job.add_task(task)
            process_item(item_id)
```

### Long-Running Task Example

```python
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

with Job('worker-01', config,
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

Set environment variables for PostgreSQL connection:

```bash
export SYNC_SQL_HOST=localhost
export SYNC_SQL_DATABASE=jobsync
export SYNC_SQL_USERNAME=postgres
export SYNC_SQL_PASSWORD=postgres
```

Optional coordination settings (defaults work for most cases):

```bash
export SYNC_TOTAL_TOKENS=10000          # Token pool size (default: 10000)
```

**See [Usage Guide](docs/USAGE_GUIDE.md#coordination-configuration)** for detailed configuration options and **[Operator Guide](docs/OPERATOR_GUIDE.md#performance-tuning)** for tuning parameters.

## Usage Examples

### Basic Task Processing

```python
from jobsync import Job, Task

with Job('worker-01', config) as job:
    for item_id in get_pending_items():
        task = Task(item_id)
        if job.can_claim_task(task):
            job.add_task(task)
            process_item(item_id)
    job.write_audit()
```

### Task Pinning (Lock GPU tasks to GPU workers)

```python
def register_gpu_locks(job):
    gpu_tasks = get_gpu_task_ids()
    locks = [(task_id, '%gpu%', 'requires_gpu') for task_id in gpu_tasks]
    job.register_locks_bulk(locks)

with Job('worker-gpu-01', config, 
         lock_provider=register_gpu_locks) as job:
    for task_id in get_all_tasks():
        if job.can_claim_task(Task(task_id)):
            process_gpu_task(task_id)
```

**See [Usage Guide](docs/USAGE_GUIDE.md)** for complete examples including WebSocket subscriptions, Kafka consumers, ETL pipelines, and more.

## Key Features

### Lock Lifecycle Management

Control how locks are managed across runs:

```python
# Dynamic locks (changes each run)
def dynamic_lock_provider(job):
    heavy_tasks = get_current_heavy_tasks()  # Different each day
    locks = [(t, 'high-mem-%', 'heavy') for t in heavy_tasks]
    job.register_task_locks_bulk(locks)

with Job('worker', 'prod', config,
         lock_provider=dynamic_lock_provider,
         clear_existing_locks=True) as job:  # Clean old locks
    process_tasks(job)

# Manual lock management
with Job('admin', config) as job:
    locks = job.list_locks()  # Review all locks
    job.clear_locks_by_creator('old-node')  # Clean up specific node
    job.clear_all_locks()  # Nuclear option
```

### Zero-Downtime Deployments

Start new workers with updated code, then stop old workers:

```bash
# Start new version
python worker_v2.py --node-name worker-v2-01 &
python worker_v2.py --node-name worker-v2-02 &

# Wait for them to join and get tokens (30-60 seconds)

# Stop old version
kill -TERM $(pgrep -f worker_v1.py)
```

The leader automatically rebalances tokens as workers join and leave.


### Task Pinning

Lock specific tasks to specific workers using patterns (supports single pattern or ordered fallback list):

```python
def register_locks(job):
    gpu_tasks = get_gpu_task_ids()
    locks = [
        # Ordered fallback: try gpu-01 first, then any GPU node
        (task_id, ['gpu-01', '%-gpu'], 'requires_gpu')
        for task_id in gpu_tasks
    ]
    # Simple case: single pattern string also accepted
    # locks.append((task_id, '%-gpu', 'requires_gpu'))
    job.register_locks_bulk(locks)

with Job('worker-gpu-01', config, 
         lock_provider=register_locks) as job:
    for task_id in get_all_tasks():
        if job.can_claim_task(Task(task_id)):
            process_gpu_task(task_id)
```

**See [Usage Guide](docs/USAGE_GUIDE.md#task-locking-and-pinning)** for complete lock API reference including bulk registration, pattern matching, fallback patterns, and lifecycle management.

### Health Monitoring

Integrate with load balancers:

```python
from flask import Flask, jsonify

app = Flask(__name__)

@app.route('/health/ready')
def health():
    if job.am_i_healthy():
        return jsonify({'status': 'ready'}), 200
    return jsonify({'status': 'not_ready'}), 503

with Job('worker-01', config) as job:
    app.run(host='0.0.0.0', port=8080)
```

### Custom Configuration

Fine-tune coordination behavior:

```python
from jobsync import CoordinationConfig

config = CoordinationConfig(
    total_tokens=50000,                   # More tokens for large clusters
    heartbeat_interval_sec=3,             # Faster heartbeat
    heartbeat_timeout_sec=9,              # Quicker failure detection
    rebalance_check_interval_sec=15       # More responsive rebalancing
)

with Job('worker-01', base_config, coordination_config=config) as job:
    process_tasks(job)
```

## Documentation

### 📚 Documentation Overview

This README provides a high-level overview and quick start. For detailed information:

**[Usage Guide](docs/USAGE_GUIDE.md)** - Complete developer reference:
- API documentation with code examples
- Long-running task patterns (WebSockets, Kafka, subscriptions)  
- Task locking with fallback patterns
- Configuration options and tuning
- Best practices and common patterns

**[Operator Guide](docs/OPERATOR_GUIDE.md)** - Operations reference:
- Deployment and scaling procedures
- Monitoring, alerting, and dashboards
- Troubleshooting guide with diagnostics
- Emergency procedures and recovery
- Database maintenance and tuning


## Monitoring

Check cluster health:

```sql
-- Quick health check
SELECT COUNT(*) as active_nodes FROM sync_node
WHERE last_heartbeat > NOW() - INTERVAL '15 seconds';
```

**Quick Reference**: See [Cheatsheet.sql](docs/Cheatsheet.sql) for a printable collection of common monitoring queries.

**See [Operator Guide](docs/OPERATOR_GUIDE.md#monitoring-and-alerting)** for:
- Complete monitoring setup with alerting rules
- Prometheus/Grafana integration examples
- Diagnostic SQL queries
- Troubleshooting procedures

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

- Python 3.8+
- PostgreSQL 12+
- Required packages: psycopg2, pandas (installed automatically)

## License

MIT

## Support

- **Issues**: https://github.com/yourorg/jobsync/issues
- **Documentation**: See `docs/` folder
- **Examples**: See `tests/` folder
