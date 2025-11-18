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
with Job('worker-01', 'production', config) as job:
    for item_id in get_pending_items():
        task = Task(item_id)
        
        # Only process if this task belongs to this worker
        if job.can_claim_task(task):
            job.add_task(task)
            process_item(item_id)
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

### Database Connection

Set environment variables for PostgreSQL:

```bash
export SYNC_SQL_HOST=localhost
export SYNC_SQL_DATABASE=jobsync
export SYNC_SQL_USERNAME=postgres
export SYNC_SQL_PASSWORD=postgres
export SYNC_SQL_PORT=5432
```

### Coordination Settings

```bash
# Core settings
export SYNC_COORDINATION_ENABLED=true
export SYNC_TOTAL_TOKENS=10000

# Timing controls
export SYNC_HEARTBEAT_INTERVAL=5        # How often workers heartbeat
export SYNC_HEARTBEAT_TIMEOUT=15        # When worker is considered dead
export SYNC_REBALANCE_INTERVAL=30       # How often leader checks for changes
export SYNC_TOKEN_REFRESH_STEADY=30     # How often workers refresh tokens
```

**See [Operator Guide](docs/OPERATOR_GUIDE.md#performance-tuning)** for tuning these values for different cluster sizes and environments.

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
with Job('admin', 'prod', config) as job:
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

Lock specific tasks to specific workers using patterns:

```python
def register_locks(job):
    # Pin GPU tasks to GPU workers
    gpu_tasks = get_gpu_task_ids()
    locks = [(task_id, '%gpu%', 'requires_gpu') for task_id in gpu_tasks]
    job.register_task_locks_bulk(locks)

# Static locks (persist across runs)
with Job('worker-gpu-01', 'production', config, 
         lock_provider=register_locks, 
         clear_existing_locks=False) as job:
    for task_id in get_all_tasks():
        if job.can_claim_task(Task(task_id)):
            process_gpu_task(task_id)

# Dynamic locks (changes each run)
with Job('worker-01', 'production', config,
         lock_provider=register_locks,
         clear_existing_locks=True) as job:  # Clean slate each run
    for task_id in get_all_tasks():
        if job.can_claim_task(Task(task_id)):
            process_task(task_id)
```

**Lock Management APIs**:

```python
# List current locks
locks = job.list_locks()
for lock in locks:
    print(f"Token {lock['token_id']} -> {lock['node_pattern']}")

# Clear locks from specific node
job.clear_locks_by_creator('old-worker-01')

# Clear all locks (nuclear option)
job.clear_all_locks()
```

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

with Job('worker-01', 'production', config) as job:
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

with Job('worker-01', 'production', base_config, coordination_config=config) as job:
    process_tasks(job)
```

## Documentation

### 📚 Comprehensive Guides

**[Usage Guide](docs/USAGE_GUIDE.md)** - Complete developer guide:
- Job creation and configuration
- Task locking patterns (GPU, memory, data locality)
- Processing patterns (batch, idempotent, continuous)
- Health monitoring and diagnostics
- Best practices and common patterns
- Full CoordinationConfig reference

**[Operator Guide](docs/OPERATOR_GUIDE.md)** - Complete operations guide:
- Deployment procedures (initial, rolling updates, scaling)
- Monitoring and alerting (metrics, dashboards, queries)
- Troubleshooting common issues (with SQL queries)
- Emergency procedures (split-brain, leader failures)
- Database maintenance and performance tuning
- Configuration for different environments

## Real-World Examples

### Example 1: ETL Pipeline

```python
def run_daily_etl():
    """Process today's data across multiple workers.
    """
    with Job('etl-worker-01', 'production', config, wait_on_enter=120) as job:
        # Get all source files for today
        files = list_s3_files(today())
        
        for file_path in files:
            task = Task(file_path)
            
            if job.can_claim_task(task):
                job.add_task(task)
                data = extract_from_s3(file_path)
                transformed = transform_data(data)
                load_to_warehouse(transformed)
        
        job.write_audit()
```

### Example 2: GPU Task Processing

```python
def process_gpu_tasks():
    """Lock GPU-intensive tasks to GPU workers.
    """
    def register_gpu_locks(job):
        sql = "SELECT task_id FROM tasks WHERE requires_gpu = true"
        gpu_tasks = db.select(job._cn, sql)
        locks = [(row['task_id'], '%gpu%', 'gpu_required') for row in gpu_tasks.to_dict('records')]
        if locks:
            job.register_task_locks_bulk(locks)
    
    with Job('worker-gpu-01', 'production', config, lock_provider=register_gpu_locks) as job:
        for task_id in get_pending_tasks():
            if job.can_claim_task(Task(task_id)):
                run_model_inference(task_id)
```

### Example 3: Continuous Processing

```python
def continuous_processor():
    """Process tasks continuously until queue empty.
    """
    with Job('worker-01', 'production', config, wait_on_enter=120) as job:
        while True:
            pending = get_pending_tasks()
            if not pending:
                break
                
            for task_id in pending:
                if job.can_claim_task(Task(task_id)):
                    job.add_task(task_id)
                    process_task(task_id)
            
            job.write_audit()
            time.sleep(5)
```

## Monitoring

### Key Queries

**Active Workers**:
```sql
SELECT name, last_heartbeat, NOW() - last_heartbeat as lag
FROM sync_node
WHERE last_heartbeat > NOW() - INTERVAL '15 seconds'
ORDER BY created_on;
```

**Token Distribution**:
```sql
SELECT node, COUNT(*) as tokens,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) as pct
FROM sync_token
GROUP BY node;
```

**Current Leader**:
```sql
SELECT name FROM sync_node
WHERE last_heartbeat > NOW() - INTERVAL '15 seconds'
ORDER BY created_on LIMIT 1;
```

**See [Operator Guide](docs/OPERATOR_GUIDE.md#monitoring-and-alerting)** for complete monitoring setup with alerting rules and dashboards.

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
