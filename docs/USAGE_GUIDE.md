# Usage Guide - Hybrid Coordination System

## Table of Contents

1. [Overview](#overview)
2. [Basic Job Creation](#basic-job-creation)
3. [Coordination Configuration](#coordination-configuration)
4. [Long-Running Tasks and Callbacks](#long-running-tasks-and-callbacks)
5. [Task Locking and Pinning](#task-locking-and-pinning)
6. [Task Processing Patterns](#task-processing-patterns)
7. [Health Monitoring](#health-monitoring)
8. [Database Queries](#database-queries)
9. [Best Practices](#best-practices)

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
        config=config,
        wait_on_enter=120  # Wait for cluster formation and token distribution
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
    """Run without coordination.
    
    All tasks are processed by this node without token-based filtering.
    Useful for single-node deployments or during troubleshooting.
    """
    coord_config = CoordinationConfig(enabled=False)
    
    with Job(
        node_name='worker-01',
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
from types import SimpleNamespace

sync = SimpleNamespace(
    coordination=SimpleNamespace(
        enabled=True,
        total_tokens=5000,
        heartbeat_interval_sec=10,
        heartbeat_timeout_sec=30,
        # ... other settings
    )
)

# Then use without coordination_config parameter:
with Job(
    node_name='worker-01',
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

## Long-Running Tasks and Callbacks

### Overview

JobSync supports long-running tasks like WebSocket subscriptions, message queue consumers, or continuous data streams through token ownership callbacks. These callbacks notify your application when token ownership changes, allowing you to start/stop persistent resources automatically.

**When to Use Callbacks:**
- WebSocket or streaming data connections
- Message queue subscriptions (Kafka, RabbitMQ, etc.)
- Background threads processing specific tasks
- Cache warming/invalidation based on ownership
- Database connection pools per task set

**Callback Lifecycle:**

```
Node Startup:
├── Register node
├── Token distribution
└── on_tokens_added(initial_tokens)  ← Start initial subscriptions

During Rebalancing:
├── on_tokens_removed(removed_tokens)  ← Always called first
└── on_tokens_added(added_tokens)      ← Then called (may overlap)

Node Shutdown:
└── __exit__ cleanup  ← Application responsible for cleanup
```

**Callback Contract:**
1. **Thread Safety Required**: Callbacks run in background threads (heartbeat or refresh thread)
2. **Keep Fast**: Callbacks should complete in <1 second or delegate work to other threads
3. **Exception Handling**: Exceptions are logged but don't stop coordination
4. **No Blocking**: Don't make synchronous calls that could block the coordination thread
5. **Order Guarantee**: `on_tokens_removed` always called before `on_tokens_added` during rebalancing

### Basic Callback Usage

```python
def simple_callback_example():
    """Basic token ownership callbacks.
    """
    def handle_tokens_added(token_ids: set[int]):
        """Called when tokens are assigned to this node.
        
        Args:
            token_ids: Set of token IDs now owned by this node
        """
        logger.info(f'Received {len(token_ids)} new tokens')
        
        # Get all task IDs that hash to these tokens
        for token_id in token_ids:
            task_ids = job.get_task_ids_for_token(token_id, all_possible_tasks)
            for task_id in task_ids:
                start_processing(task_id)
    
    def handle_tokens_removed(token_ids: set[int]):
        """Called when tokens are removed from this node.
        
        Args:
            token_ids: Set of token IDs no longer owned by this node
        """
        logger.info(f'Lost {len(token_ids)} tokens')
        
        for token_id in token_ids:
            task_ids = job.get_task_ids_for_token(token_id, all_possible_tasks)
            for task_id in task_ids:
                stop_processing(task_id)
    
    with Job(
        node_name='worker-01',
        config=config,
        on_tokens_added=handle_tokens_added,
        on_tokens_removed=handle_tokens_removed
    ) as job:
        # Callbacks handle token changes
        # Keep worker alive
        while not shutdown_requested():
            time.sleep(1)
```

### WebSocket Subscription Management

```python
import threading
import websocket
from collections import defaultdict

class WebSocketManager:
    """Manage WebSocket connections based on token ownership.
    """
    
    def __init__(self, job, all_symbols):
        self.job = job
        self.all_symbols = all_symbols
        self.connections = {}
        self.threads = {}
        self.lock = threading.Lock()
        self.shutdown = False
    
    def on_tokens_added(self, token_ids: set[int]):
        """Start WebSocket connections for assigned tokens.
        """
        with self.lock:
            for token_id in token_ids:
                symbols = self.job.get_task_ids_for_token(token_id, self.all_symbols)
                
                for symbol in symbols:
                    if symbol in self.connections:
                        continue
                    
                    try:
                        ws = websocket.WebSocketApp(
                            f'wss://stream.example.com/quotes/{symbol}',
                            on_message=lambda ws, msg: self._handle_message(symbol, msg),
                            on_error=lambda ws, err: self._handle_error(symbol, err),
                            on_close=lambda ws: self._handle_close(symbol)
                        )
                        
                        self.connections[symbol] = ws
                        
                        thread = threading.Thread(
                            target=ws.run_forever,
                            name=f'ws-{symbol}'
                        )
                        thread.daemon = True
                        thread.start()
                        self.threads[symbol] = thread
                        
                        logger.info(f'Started WebSocket for {symbol}')
                    except Exception as e:
                        logger.error(f'Failed to start WebSocket for {symbol}: {e}')
                
                logger.info(f'Managing {len(self.connections)} WebSocket connections')
    
    def on_tokens_removed(self, token_ids: set[int]):
        """Stop WebSocket connections for removed tokens.
        """
        with self.lock:
            for token_id in token_ids:
                symbols = self.job.get_task_ids_for_token(token_id, self.all_symbols)
                
                for symbol in symbols:
                    if symbol in self.connections:
                        try:
                            self.connections[symbol].close()
                            del self.connections[symbol]
                            if symbol in self.threads:
                                del self.threads[symbol]
                            logger.info(f'Closed WebSocket for {symbol}')
                        except Exception as e:
                            logger.error(f'Error closing WebSocket for {symbol}: {e}')
    
    def _handle_message(self, symbol, message):
        """Process incoming WebSocket message.
        """
        try:
            data = json.loads(message)
            process_quote_update(symbol, data)
        except Exception as e:
            logger.error(f'Error processing message for {symbol}: {e}')
    
    def _handle_error(self, symbol, error):
        """Handle WebSocket error.
        """
        logger.error(f'WebSocket error for {symbol}: {error}')
    
    def _handle_close(self, symbol):
        """Handle WebSocket closure.
        """
        logger.warning(f'WebSocket closed for {symbol}')
        
        with self.lock:
            if symbol in self.connections and not self.shutdown:
                logger.info(f'Reconnecting WebSocket for {symbol}')
                del self.connections[symbol]
    
    def close_all(self):
        """Close all WebSocket connections (for shutdown).
        """
        self.shutdown = True
        with self.lock:
            for symbol, ws in list(self.connections.items()):
                try:
                    ws.close()
                except Exception:
                    pass
            self.connections.clear()
            self.threads.clear()

def run_websocket_worker():
    """Run worker managing WebSocket subscriptions.
    """
    all_symbols = get_all_symbols()
    
    with Job('ws-worker-01', 'production', config) as job:
        manager = WebSocketManager(job, all_symbols)
        
        with Job(
            node_name='ws-worker-01',
            config=config,
            on_tokens_added=manager.on_tokens_added,
            on_tokens_removed=manager.on_tokens_removed
        ) as job:
            try:
                while not shutdown_requested():
                    time.sleep(1)
            finally:
                manager.close_all()
```

### Message Queue Consumer Management

```python
import threading
from kafka import KafkaConsumer

class KafkaConsumerManager:
    """Manage Kafka consumers based on token ownership.
    """
    
    def __init__(self, job, all_topics):
        self.job = job
        self.all_topics = all_topics
        self.consumers = {}
        self.threads = {}
        self.lock = threading.Lock()
        self.shutdown = False
    
    def on_tokens_added(self, token_ids: set[int]):
        """Start Kafka consumers for assigned tokens.
        """
        with self.lock:
            for token_id in token_ids:
                topics = self.job.get_task_ids_for_token(token_id, self.all_topics)
                
                for topic in topics:
                    if topic in self.consumers:
                        continue
                    
                    try:
                        consumer = KafkaConsumer(
                            topic,
                            bootstrap_servers=['kafka:9092'],
                            group_id='my-consumer-group',
                            auto_offset_reset='latest'
                        )
                        
                        self.consumers[topic] = consumer
                        
                        thread = threading.Thread(
                            target=self._consume_messages,
                            args=(topic, consumer),
                            name=f'kafka-{topic}'
                        )
                        thread.daemon = True
                        thread.start()
                        self.threads[topic] = thread
                        
                        logger.info(f'Started Kafka consumer for {topic}')
                    except Exception as e:
                        logger.error(f'Failed to start consumer for {topic}: {e}')
                
                logger.info(f'Managing {len(self.consumers)} Kafka consumers')
    
    def on_tokens_removed(self, token_ids: set[int]):
        """Stop Kafka consumers for removed tokens.
        """
        with self.lock:
            for token_id in token_ids:
                topics = self.job.get_task_ids_for_token(token_id, self.all_topics)
                
                for topic in topics:
                    if topic in self.consumers:
                        try:
                            self.consumers[topic].close()
                            del self.consumers[topic]
                            if topic in self.threads:
                                del self.threads[topic]
                            logger.info(f'Closed Kafka consumer for {topic}')
                        except Exception as e:
                            logger.error(f'Error closing consumer for {topic}: {e}')
    
    def _consume_messages(self, topic, consumer):
        """Consume messages from Kafka topic.
        """
        while not self.shutdown and topic in self.consumers:
            try:
                for message in consumer:
                    if self.shutdown or topic not in self.consumers:
                        break
                    
                    process_kafka_message(topic, message)
            except Exception as e:
                logger.error(f'Error consuming from {topic}: {e}')
                time.sleep(5)
    
    def close_all(self):
        """Close all Kafka consumers (for shutdown).
        """
        self.shutdown = True
        with self.lock:
            for topic, consumer in list(self.consumers.items()):
                try:
                    consumer.close()
                except Exception:
                    pass
            self.consumers.clear()
            self.threads.clear()

def run_kafka_worker():
    """Run worker managing Kafka consumers.
    """
    all_topics = ['topic-1', 'topic-2', 'topic-3', 'topic-4']
    
    with Job('kafka-worker-01', 'production', config) as job:
        manager = KafkaConsumerManager(job, all_topics)
        
        with Job(
            node_name='kafka-worker-01',
            config=config,
            on_tokens_added=manager.on_tokens_added,
            on_tokens_removed=manager.on_tokens_removed
        ) as job:
            try:
                while not shutdown_requested():
                    time.sleep(1)
            finally:
                manager.close_all()
```

### Background Thread Management

```python
import threading
import queue

class BackgroundProcessor:
    """Manage background processing threads per token.
    """
    
    def __init__(self, job, all_task_ids):
        self.job = job
        self.all_task_ids = all_task_ids
        self.threads = {}
        self.queues = {}
        self.lock = threading.Lock()
        self.shutdown = False
    
    def on_tokens_added(self, token_ids: set[int]):
        """Start processing threads for assigned tokens.
        """
        with self.lock:
            for token_id in token_ids:
                if token_id in self.threads:
                    continue
                
                task_queue = queue.Queue()
                self.queues[token_id] = task_queue
                
                thread = threading.Thread(
                    target=self._process_token,
                    args=(token_id, task_queue),
                    name=f'processor-{token_id}'
                )
                thread.daemon = True
                thread.start()
                self.threads[token_id] = thread
                
                task_ids = self.job.get_task_ids_for_token(token_id, self.all_task_ids)
                for task_id in task_ids:
                    task_queue.put(task_id)
                
                logger.info(f'Started thread for token {token_id} with {len(task_ids)} tasks')
    
    def on_tokens_removed(self, token_ids: set[int]):
        """Stop processing threads for removed tokens.
        """
        with self.lock:
            for token_id in token_ids:
                if token_id in self.queues:
                    self.queues[token_id].put(None)
                    del self.queues[token_id]
                
                if token_id in self.threads:
                    del self.threads[token_id]
                
                logger.info(f'Stopped thread for token {token_id}')
    
    def _process_token(self, token_id, task_queue):
        """Process tasks for a specific token.
        """
        while not self.shutdown:
            try:
                task_id = task_queue.get(timeout=1)
                
                if task_id is None:
                    break
                
                process_long_running_task(task_id)
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f'Error processing token {token_id}: {e}')
    
    def close_all(self):
        """Stop all processing threads (for shutdown).
        """
        self.shutdown = True
        with self.lock:
            for token_id in list(self.queues.keys()):
                self.queues[token_id].put(None)
            self.queues.clear()
            self.threads.clear()

def run_background_processor():
    """Run worker with background processing threads.
    """
    all_task_ids = get_all_task_ids()
    
    with Job('processor-01', 'production', config) as job:
        processor = BackgroundProcessor(job, all_task_ids)
        
        with Job(
            node_name='processor-01',
            config=config,
            on_tokens_added=processor.on_tokens_added,
            on_tokens_removed=processor.on_tokens_removed
        ) as job:
            try:
                while not shutdown_requested():
                    time.sleep(1)
            finally:
                processor.close_all()
```

### Cache Management Based on Ownership

```python
from functools import lru_cache
import threading

class TokenBasedCache:
    """Manage cache warming/invalidation based on token ownership.
    """
    
    def __init__(self, job, all_customer_ids):
        self.job = job
        self.all_customer_ids = all_customer_ids
        self.cached_data = {}
        self.lock = threading.Lock()
    
    def on_tokens_added(self, token_ids: set[int]):
        """Warm cache for newly assigned tokens.
        """
        with self.lock:
            for token_id in token_ids:
                customer_ids = self.job.get_task_ids_for_token(token_id, self.all_customer_ids)
                
                for customer_id in customer_ids:
                    if customer_id not in self.cached_data:
                        self.cached_data[customer_id] = load_customer_data(customer_id)
                
                logger.info(f'Warmed cache for {len(customer_ids)} customers in token {token_id}')
    
    def on_tokens_removed(self, token_ids: set[int]):
        """Invalidate cache for removed tokens.
        """
        with self.lock:
            for token_id in token_ids:
                customer_ids = self.job.get_task_ids_for_token(token_id, self.all_customer_ids)
                
                for customer_id in customer_ids:
                    if customer_id in self.cached_data:
                        del self.cached_data[customer_id]
                
                logger.info(f'Invalidated cache for {len(customer_ids)} customers in token {token_id}')
    
    def get_data(self, customer_id):
        """Get cached data for customer.
        """
        with self.lock:
            if customer_id not in self.cached_data:
                self.cached_data[customer_id] = load_customer_data(customer_id)
            return self.cached_data[customer_id]

def run_cached_processor():
    """Run worker with token-based caching.
    """
    all_customer_ids = get_all_customers()
    
    with Job('cached-worker-01', 'production', config) as job:
        cache = TokenBasedCache(job, all_customer_ids)
        
        with Job(
            node_name='cached-worker-01',
            config=config,
            on_tokens_added=cache.on_tokens_added,
            on_tokens_removed=cache.on_tokens_removed
        ) as job:
            while not shutdown_requested():
                for customer_id in get_pending_requests():
                    if job.can_claim_task(Task(customer_id)):
                        data = cache.get_data(customer_id)
                        process_request(customer_id, data)
                
                time.sleep(1)
```

### Helper Methods for Callback Implementation

```python
def token_ownership_helpers():
    """Demonstrate helper methods for working with token ownership.
    """
    with Job('worker-01', config) as job:
        # Get all task IDs currently owned by this node
        all_task_ids = ['task-1', 'task-2', 'task-3', ...]
        my_tasks = job.get_my_task_ids(all_task_ids)
        logger.info(f'I own {len(my_tasks)} tasks')
        
        # Get task IDs for a specific token
        token_id = 100
        tasks_in_token = job.get_task_ids_for_token(token_id, all_task_ids)
        logger.info(f'Token {token_id} contains {len(tasks_in_token)} tasks')
        
        # Check which token a task hashes to
        task_id = 'customer-12345'
        token = job.task_to_token(task_id)
        logger.info(f'Task {task_id} hashes to token {token}')
        
        # Check if we own this task
        can_process = job.can_claim_task(Task(task_id))
        logger.info(f'Can process {task_id}: {can_process}')
        
        # Get current token ownership info
        my_tokens = job.my_tokens  # Read-only copy
        token_version = job.token_version
        logger.info(f'Own {len(my_tokens)} tokens at version {token_version}')
```

### Callback Best Practices

**1. Delegate Heavy Work to Background Threads:**

```python
def on_tokens_added(token_ids: set[int]):
    """Don't block the coordination thread!"""
    # BAD: Synchronous heavy work
    for token_id in token_ids:
        load_heavy_data(token_id)  # Blocks coordination!
    
    # GOOD: Delegate to worker thread
    def background_work():
        for token_id in token_ids:
            load_heavy_data(token_id)
    
    thread = threading.Thread(target=background_work)
    thread.daemon = True
    thread.start()
```

**2. Handle Exceptions Gracefully:**

```python
def on_tokens_added(token_ids: set[int]):
    """Always handle exceptions in callbacks.
    """
    for token_id in token_ids:
        try:
            start_subscription(token_id)
        except Exception as e:
            logger.error(f'Failed to start subscription for token {token_id}: {e}')
            # Continue processing other tokens
```

**3. Use Locks for Shared State:**

```python
class SafeCallbackHandler:
    """Thread-safe callback handler.
    """
    
    def __init__(self):
        self.active_resources = {}
        self.lock = threading.Lock()
    
    def on_tokens_added(self, token_ids: set[int]):
        with self.lock:
            for token_id in token_ids:
                self.active_resources[token_id] = start_resource(token_id)
    
    def on_tokens_removed(self, token_ids: set[int]):
        with self.lock:
            for token_id in token_ids:
                if token_id in self.active_resources:
                    self.active_resources[token_id].close()
                    del self.active_resources[token_id]
```

**4. Track Callback Performance:**

```python
import time

def on_tokens_added(token_ids: set[int]):
    """Monitor callback performance.
    """
    start = time.time()
    
    try:
        for token_id in token_ids:
            start_processing(token_id)
        
        duration_ms = int((time.time() - start) * 1000)
        logger.info(f'on_tokens_added completed in {duration_ms}ms for {len(token_ids)} tokens')
        
        if duration_ms > 1000:
            logger.warning(f'Callback took >{duration_ms}ms - consider delegating work')
    
    except Exception as e:
        logger.error(f'Callback failed: {e}')
```

**5. Handle Overlapping Token Sets During Rebalancing:**

```python
class OverlapAwareHandler:
    """Handle token changes that may overlap.
    """
    
    def __init__(self):
        self.active_tokens = set()
        self.lock = threading.Lock()
    
    def on_tokens_removed(self, token_ids: set[int]):
        """Always called first during rebalancing.
        """
        with self.lock:
            for token_id in token_ids:
                if token_id in self.active_tokens:
                    stop_processing(token_id)
                    self.active_tokens.discard(token_id)
    
    def on_tokens_added(self, token_ids: set[int]):
        """Called after on_tokens_removed during rebalancing.
        """
        with self.lock:
            for token_id in token_ids:
                if token_id not in self.active_tokens:
                    start_processing(token_id)
                    self.active_tokens.add(token_id)
```

## Task Locking and Pinning

### Single Lock Registration

```python
def process_with_lock():
    """Register a single task lock during job initialization.
    """
    def register_locks(job):
        # Pin task 12345 to node 'worker-01'
        job.register_lock(
            task_id=12345,
            node_patterns='worker-01',  # Single pattern
            reason='high_memory_required',
            expires_in_days=7  # Optional expiration
        )
    
    with Job(
        node_name='worker-01',
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
    """Register multiple locks efficiently with fallback support.
    """
    def register_locks(job):
        # Get tasks requiring special resources from database
        sql = """
        SELECT task_id, required_node 
        FROM tasks 
        WHERE date = %s AND required_node IS NOT NULL
        """
        tasks = db.select(job._cn, sql, job.date)
        
        locks = []
        for task in tasks.to_dict('records'):
            if task['required_node']:
                nodes = [n.strip() for n in task['required_node'].split(',')]
                locks.append((task['task_id'], nodes, 'resource_requirement'))
        
        if locks:
            job.register_locks_bulk(locks)
    
    with Job(
        node_name='worker-01',
        config=config,
        lock_provider=register_locks
    ) as job:
        process_tasks(job)
```

### Pattern-Based Locks

```python
def register_pattern_locks(job):
    """Use SQL LIKE patterns for flexible node matching with fallback.
    """
    # Exact match
    job.register_lock(
        task_id=100,
        node_patterns='prod-node-01',
        reason='exact_node'
    )
    
    # Prefix pattern (SQL LIKE 'prod-%')
    job.register_lock(
        task_id=200,
        node_patterns='prod-%',
        reason='any_prod_node'
    )
    
    # Suffix pattern (SQL LIKE '%-gpu')
    job.register_lock(
        task_id=300,
        node_patterns='%-gpu',
        reason='gpu_required'
    )
    
    # Contains pattern (SQL LIKE '%special%')
    job.register_lock(
        task_id=400,
        node_patterns='%special%',
        reason='special_nodes_only'
    )
    
    # Multiple patterns with ordered fallback
    job.register_lock(
        task_id=500,
        node_patterns=['prod-gpu-01', 'prod-gpu-%', '%-gpu'],
        reason='prefer_primary_gpu_with_fallback'
    )
    
    # Regional fallback pattern
    job.register_lock(
        task_id=600,
        node_patterns=['us-east-1-%', 'us-east-%', 'us-%'],
        reason='regional_data_locality'
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
    with Job('worker-01', config) as job:
        # List all current locks
        locks = job.list_locks()
        for lock in locks:
            patterns = lock['node_patterns']  # List of patterns
            patterns_str = ' -> '.join(patterns)  # Show fallback chain
            print(f"Token {lock['token_id']}: {patterns_str} "
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

| Scenario                       | Setting | Reason                                    |
| ---------                      | ------- | ------                                    |
| Lock logic changes daily       | `True`  | Prevent accumulation of stale locks       |
| GPU availability changes       | `True`  | Locks reflect current GPU availability    |
| Customer assignment changes    | `True`  | Update customer-to-region mappings        |
| Permanent resource constraints | `False` | Keep consistent lock patterns             |
| Development/testing            | `True`  | Clean state for each test run             |

**Lock Management Best Practices**:

```python
def production_lock_pattern():
    """Best practices for production lock management with fallback.
    """
    def register_locks(job):
        # Query current state from database
        sql = """
        SELECT task_id, required_resource, priority 
        FROM task_metadata 
        WHERE date = %s AND required_resource IS NOT NULL
        """
        requirements = db.select(job._cn, sql, job.date)
        
        locks = []
        for row in requirements.to_dict('records'):
            # Map requirements to node patterns with fallback
            if row['required_resource'] == 'gpu':
                if row['priority'] == 'high':
                    locks.append((row['task_id'], ['gpu-primary', '%-gpu'], 'high_priority_gpu'))
                else:
                    locks.append((row['task_id'], '%-gpu', 'gpu_required'))
            elif row['required_resource'] == 'high_memory':
                locks.append((row['task_id'], ['high-mem-01', 'high-mem-%'], 'memory_required'))
        
        if locks:
            job.register_locks_bulk(locks)
            logger.info(f'Registered {len(locks)} locks for {job.date}')
    
    with Job(
        node_name='worker-01',
        config=config,
        lock_provider=register_locks,
        clear_existing_locks=True  # Dynamic logic - clear on each run
    ) as job:
        # Check lock state for debugging
        current_locks = job.list_locks()
        logger.debug(f'Active locks: {len(current_locks)}')
        
        process_tasks(job)
```

### Fallback Pattern Support

The lock system supports ordered fallback patterns for high availability. Patterns are stored as JSONB arrays in the database (even single patterns become single-element arrays).

```python
def register_locks_with_fallback(job):
    """Register locks with ordered fallback patterns.
    
    During token distribution, the system tries patterns in order:
    1. Try first pattern - if any active node matches, lock to it
    2. If no match, try second pattern
    3. Continue until a match is found
    4. If no patterns match, issue warning and treat as unlocked
    
    Storage: node_patterns stored as JSONB array ['pattern1', 'pattern2', ...]
    """
    # Example 1: Prefer primary, fall back to pool
    job.register_lock(
        task_id=100,
        node_patterns=['gpu-primary-01', 'gpu-pool-%'],
        reason='prefer_primary'
    )
    # Distribution: Tries 'gpu-primary-01' first
    #              If not active, tries any node matching 'gpu-pool-%'
    
    # Example 2: Regional fallback with widening scope
    job.register_lock(
        task_id=200,
        node_patterns=['us-east-1a-%', 'us-east-1-%', 'us-east-%', 'us-%'],
        reason='data_locality'
    )
    # Distribution: Tries specific AZ -> region -> coast -> country
    
    # Example 3: Resource tier fallback
    job.register_lock(
        task_id=300,
        node_patterns=['xlarge-mem-%', 'large-mem-%', 'medium-mem-%'],
        reason='memory_tiering'
    )
    # Distribution: Tries largest available memory tier
    
    # Example 4: Single pattern (no fallback)
    job.register_lock(
        task_id=400,
        node_patterns='special-node-only',  # String converts to single-element list
        reason='exact_match_required'
    )
    # Distribution: Only matches 'special-node-only' exactly
```

**Fallback Behavior:**
- Patterns are tried in the order provided
- First matching pattern wins
- If no patterns match any active nodes, a warning is logged and the token is treated as unlocked
- JSONB storage preserves pattern order
- Pattern matching uses SQL LIKE semantics (`%` = wildcard)

**When to Use Fallback Patterns:**
- **High availability**: Prefer primary node but allow failover to replicas
- **Resource tiers**: Try optimal resources first, degrade gracefully
- **Geographic locality**: Prefer local resources, expand scope as needed
- **Load shedding**: Route to specific nodes when available, else general pool

**Best Practices:**
- Order patterns from most specific to least specific
- Keep fallback lists short (2-4 patterns typical)
- Document why each pattern is in the list
- Monitor fallback usage (check which patterns actually match)

### Resource-Based Lock Example

```python
def process_gpu_tasks():
    """Lock GPU-intensive tasks to GPU-enabled nodes with fallback.
    """
    def register_gpu_locks(job):
        # Get tasks requiring GPU
        sql = """
        SELECT task_id, priority FROM tasks 
        WHERE requires_gpu = true AND date = %s
        """
        gpu_tasks = db.select(job._cn, sql, job.date)
        
        locks = []
        for row in gpu_tasks.to_dict('records'):
            if row['priority'] == 'high':
                # High priority: prefer primary GPU node, fall back to any GPU node
                locks.append((
                    row['task_id'],
                    ['gpu-primary', '%-gpu'],
                    'high_priority_gpu'
                ))
            else:
                # Normal priority: any GPU node
                locks.append((
                    row['task_id'],
                    '%-gpu',
                    'gpu_required'
                ))
        
        if locks:
            job.register_locks_bulk(locks)
    
    with Job(
        node_name='worker-gpu-01',  # GPU-enabled node
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

### Overview

This section provides SQL queries useful for debugging and development. For production monitoring and operations, see the [Operator Guide](OPERATOR_GUIDE.md#appendix-quick-reference-sql).

**💡 TIP**: For a quick reference of the most common monitoring queries, see [Cheatsheet.sql](Cheatsheet.sql).

**Note:** Replace `sync_` with your custom prefix if using `config.sync.sql.appname`.

### Development Queries

**Token Distribution with Details:**
```sql
SELECT 
    node,
    COUNT(*) as token_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) as pct,
    MIN(token_id) as min_token,
    MAX(token_id) as max_token,
    version
FROM sync_token
GROUP BY node, version
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

### Lock Queries (Development)

**View All Locks with Details:**
```sql
-- node_patterns is JSONB array (even for single patterns)
SELECT 
    node_patterns::text as patterns,
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
GROUP BY node_patterns, reason, expires_at
ORDER BY locked_token_count DESC;
```

**Locks with Token Assignments:**
```sql
-- Show locks and which nodes actually own the tokens
-- node_patterns is JSONB array of ordered fallback patterns
SELECT 
    l.token_id,
    l.node_patterns::text as patterns,
    t.node as actual_node,
    l.reason,
    l.created_by,
    l.created_at
FROM sync_lock l
JOIN sync_token t ON l.token_id = t.token_id
ORDER BY l.token_id;
```

**Note:** For production monitoring and cleanup of orphaned locks, see [Operator Guide - Troubleshooting](OPERATOR_GUIDE.md#problem-orphaned-locks-from-decommissioned-nodes).

### Task Processing Queries (Development)

**Tasks Claimed by Node:**
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

**Task Completion Rate by Node:**
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

**Note:** For production database maintenance procedures, see [Operator Guide - Database Maintenance](OPERATOR_GUIDE.md#database-maintenance).

**Quick cleanup during development:**
```sql
-- Clean test data
DELETE FROM sync_node WHERE name LIKE 'test-%';
DELETE FROM sync_claim WHERE created_on < NOW() - INTERVAL '7 days';
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
    job.register_locks_bulk(locks)

# BAD: Individual registration in loop
def register_locks(job):
    for task_id in get_special_tasks():
        job.register_lock(task_id, node, reason)  # Slow!
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
    """Register locks with clear reasons and fallback patterns.
    """
    # GPU requirement with fallback
    job.register_lock(
        task_id=100,
        node_patterns=['gpu-v100-%', '%-gpu'],
        reason='requires_nvidia_v100_preferred'
    )
    
    # Memory requirement with fallback
    job.register_lock(
        task_id=200,
        node_patterns=['high-mem-01', 'high-mem-%'],
        reason='requires_64gb_ram'
    )
    
    # Data locality with regional fallback
    job.register_lock(
        task_id=300,
        node_patterns=['us-east-1-%', 'us-east-%', 'us-%'],
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
        job.register_locks_bulk(locks)
    
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
            patterns = lock['node_patterns']  # List of patterns
            print(f"Token {lock['token_id']}: {patterns} by {lock['created_by']}")
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

### 10. Keep Callbacks Fast

```python
def process_with_fast_callbacks():
    """Ensure callbacks complete quickly.
    """
    def on_tokens_added(token_ids: set[int]):
        # BAD: Synchronous heavy work
        for token_id in token_ids:
            expensive_operation(token_id)  # Blocks coordination thread!
        
        # GOOD: Quick work only
        logger.info(f'Received {len(token_ids)} tokens')
        
        # GOOD: Delegate to background thread
        threading.Thread(target=lambda: heavy_work(token_ids)).start()
    
    with Job('worker-01', config,
             on_tokens_added=on_tokens_added) as job:
        process_tasks(job)
```

### 11. Use clear_existing_locks for Dynamic Locks

```python
def process_with_dynamic_locks():
    """Always use clear_existing_locks=True when lock logic changes.
    """
    def register_locks(job):
        # This logic changes each run based on current state
        tasks_needing_gpu = query_gpu_requirements(job.date)
        locks = [(t, '%-gpu', 'gpu') for t in tasks_needing_gpu]
        job.register_locks_bulk(locks)
    
    with Job(
        node_name='worker-01',
        config=config,
        lock_provider=register_locks,
        clear_existing_locks=True  # IMPORTANT: prevents stale lock accumulation
    ) as job:
        process_tasks(job)
```

### 12. Monitor Callback Performance

```python
def monitor_callbacks():
    """Track callback execution time in production.
    """
    import time
    
    def on_tokens_added(token_ids: set[int]):
        start = time.time()
        try:
            for token_id in token_ids:
                start_processing(token_id)
            
            duration_ms = int((time.time() - start) * 1000)
            logger.info(f'Callback completed in {duration_ms}ms')
            
            if duration_ms > 1000:
                logger.warning(f'Slow callback detected: {duration_ms}ms')
        except Exception as e:
            logger.error(f'Callback failed: {e}')
    
    with Job('worker-01', config,
             on_tokens_added=on_tokens_added) as job:
        process_tasks(job)
```

### 13. Test in Staging First

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
        config=config,
        coordination_config=coord_config
    ) as job:
        # Test with production-like workload
        test_task_processing(job)
```

---

**Document Version**: 1.0  
**Last Updated**: 2024-01-18
