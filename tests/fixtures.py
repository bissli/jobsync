"""Shared test fixtures, utilities, and helpers.

This module provides common test infrastructure to avoid duplication across test files.
Includes config builders, object factories, wait helpers, assertion helpers, and database utilities.

USE THIS FILE FOR:
- Creating reusable test fixtures and utilities
- Adding new wait/assertion helpers used by multiple test files
- Database setup/teardown utilities
- Test data factories
"""
import contextlib
import datetime
import functools
import json
import logging
import threading
import time
from collections.abc import Hashable
from dataclasses import replace

import pytest
from sqlalchemy import text

from jobsync import schema
from jobsync.client import CoordinationConfig, Job, JobState, Task

logger = logging.getLogger(__name__)


# ============================================================================
# CONFIG BUILDERS - Create test configurations
# ============================================================================

def get_coordination_config(**overrides) -> CoordinationConfig:
    """Create CoordinationConfig with test-optimized values (alias: test_config).
    """
    return test_config(**overrides)


def test_config(**overrides) -> CoordinationConfig:
    """Create CoordinationConfig with test-optimized values.

    Fast intervals for quick tests. Use overrides for specific test needs.

    Args:
        **overrides: Override specific config values

    Returns
        CoordinationConfig suitable for testing

    Usage:
        config = get_coordination_config()
        config = get_coordination_config(total_tokens=50, heartbeat_interval_sec=0.5)
    """
    defaults = {
        'minimum_nodes': 1,
        'heartbeat_interval_sec': 1,
        'heartbeat_timeout_sec': 3,
        'health_check_interval_sec': 2,
        'dead_node_check_interval_sec': 2,
        'rebalance_check_interval_sec': 2,
    }
    defaults.update(overrides)
    return CoordinationConfig(**defaults)


def get_state_driven_config(**overrides) -> CoordinationConfig:
    """Create CoordinationConfig for state machine testing.

    Returns standard config suitable for state-driven tests.
    """
    return test_config(**overrides)


def get_structure_test_config(**overrides) -> CoordinationConfig:
    """Create CoordinationConfig for schema structure testing.

    Returns standard config suitable for structure tests.
    """
    return test_config(**overrides)


# ============================================================================
# CLUSTER MANAGEMENT - Start/stop coordinated clusters
# ============================================================================

@contextlib.contextmanager
def cluster(postgres, *node_names, **config_overrides):
    """Start cluster of nodes in parallel with automatic cleanup.

    Args:
        postgres: postgres fixture
        *node_names: Node names to create
        **config_overrides: Optional CoordinationConfig overrides

    Yields
        List of Job instances (already started)

    Usage:
        with cluster(postgres, 'node1', 'node2', 'node3', total_tokens=100) as nodes:
            assert wait_for_cluster_running(nodes)
            # ...test logic...
        # All nodes auto-cleanup on exit
    """
    config = test_config(**config_overrides)
    jobs = [create_job(name, postgres, coordination_config=config, wait_on_enter=15)
            for name in node_names]

    threads = [threading.Thread(target=job.__enter__) for job in jobs]
    for i, t in enumerate(threads):
        t.start()
        if i < len(threads) - 1:
            time.sleep(0.1)

    for t in threads:
        t.join()

    try:
        yield jobs
    finally:
        for job in jobs:
            with contextlib.suppress(Exception):
                job.__exit__(None, None, None)


# ============================================================================
# CONNECTION HELPERS - Build connection strings
# ============================================================================

def get_test_connection_string() -> str:
    """Build connection string for tests.

    Returns connection string suitable for SQLAlchemy Engine or Job.
    Used by multiprocessing workers that can't access postgres fixture.
    """
    return 'postgresql+psycopg://postgres:postgres@localhost:5432/jobsync'


def find_task_ids_covering_all_tokens(total_tokens: int, max_search: int = 10000) -> list[Hashable]:
    """Find task IDs that hash to all token IDs.

    Searches for task IDs that collectively cover all token_ids from 0 to total_tokens-1.
    Useful for tests that need to lock all tokens.

    Args:
        total_tokens: Total number of tokens to cover
        max_search: Maximum candidate task IDs to search

    Returns
        List of task IDs (one per token, in token ID order)

    Raises
        RuntimeError: If cannot find covering set within max_search

    Usage:
        task_ids = find_task_ids_covering_all_tokens(20)
        for task_id in task_ids:
            insert_lock(postgres, tables, task_id, ['pattern'], created_by='test')
    """
    from jobsync.client import task_to_token

    token_to_task = {}
    for candidate in range(max_search):
        token_id = task_to_token(candidate, total_tokens)
        if token_id not in token_to_task:
            token_to_task[token_id] = candidate
        if len(token_to_task) == total_tokens:
            break

    if len(token_to_task) < total_tokens:
        raise RuntimeError(
            f'Could not find task IDs covering all {total_tokens} tokens '
            f'(found {len(token_to_task)} after searching {max_search} candidates)'
        )

    return [token_to_task[i] for i in range(total_tokens)]


# ============================================================================
# TEST UTILITIES - Reusable test helpers
# ============================================================================

def simulate_node_crash(node: Job, cleanup: bool = False) -> None:
    """Simulate node crash by stopping threads without database cleanup.

    This leaves the node row in the database with a stale heartbeat,
    allowing the leader to detect it as dead and trigger rebalancing.

    Use this instead of __exit__() when testing dead node detection.

    Args:
        node: Job instance to crash
        cleanup: If True, also dispose resources (use when node won't be rejoined)
    """
    node._shutdown_event.set()
    for monitor in node._monitors.values():
        if monitor.thread and monitor.thread.is_alive():
            monitor.thread.join(timeout=5)

    if cleanup:
        try:
            node._cleanup()
        except Exception:
            pass
        try:
            node.db.dispose()
        except Exception:
            pass


class CallbackTracker:
    """Thread-safe callback tracker for testing token ownership callbacks.

    Usage:
        tracker = CallbackTracker()
        job = create_job('node1', postgres,
                        on_tokens_added=tracker.on_tokens_added,
                        on_tokens_removed=tracker.on_tokens_removed)
        with job:
            assert wait_for(lambda: len(tracker.added_calls) >= 1)
    """

    def __init__(self):
        self.added_calls = []
        self.removed_calls = []
        self.call_order = []
        self.lock = threading.Lock()

    def on_tokens_added(self, token_ids: set[int]):
        """Track on_tokens_added calls.
        """
        with self.lock:
            self.added_calls.append(token_ids.copy())
            self.call_order.append(('added', token_ids.copy()))
            logger.info(f'on_tokens_added called with {len(token_ids)} tokens')

    def on_tokens_removed(self, token_ids: set[int]):
        """Track on_tokens_removed calls.
        """
        with self.lock:
            self.removed_calls.append(token_ids.copy())
            self.call_order.append(('removed', token_ids.copy()))
            logger.info(f'on_tokens_removed called with {len(token_ids)} tokens')

    def get_total_added(self) -> set[int]:
        """Get all tokens ever added.
        """
        with self.lock:
            result = set()
            for tokens in self.added_calls:
                result.update(tokens)
            return result

    def get_total_removed(self) -> set[int]:
        """Get all tokens ever removed.
        """
        with self.lock:
            result = set()
            for tokens in self.removed_calls:
                result.update(tokens)
            return result

    def reset(self):
        """Reset all tracking.
        """
        with self.lock:
            self.added_calls.clear()
            self.removed_calls.clear()
            self.call_order.clear()


# ============================================================================
# FACTORIES - Create test objects with sensible defaults
# ============================================================================

@pytest.fixture(scope='module')
def shared_unit_test_job(postgres):
    """Shared job for unit tests that don't modify state.

    Module-scoped to avoid recreation. Only for tests checking read-only
    operations like task_to_token(), pattern matching, etc.
    """
    coord_config = get_coordination_config()
    job = create_job('unit-test-shared', postgres, coordination_config=coord_config, wait_on_enter=0)
    return job


@pytest.fixture
def callback_tracker():
    """Fixture that provides CallbackTracker instance.

    Usage:
        def test_callbacks(postgres, callback_tracker):
            job = create_job('node1', postgres,
                           on_tokens_added=callback_tracker.on_tokens_added)
    """
    return CallbackTracker()


def create_job(
    node_name: str,
    postgres,
    coordination_config: CoordinationConfig = None,
    wait_on_enter: int = 0,
    **kwargs
) -> Job:
    """Create Job with test defaults.

    Args:
        node_name: Unique node identifier
        postgres: pytest postgres fixture
        coordination_config: Optional CoordinationConfig to enable coordination (None disables)
        wait_on_enter: Seconds to wait for cluster formation.
                      Use 0 for unit tests (no coordination needed).
                      Use 2-5 for simple integration tests.
                      Use 10-15 for complex multi-node cluster tests.
        **kwargs: Additional Job() parameters

    Returns
        Configured Job instance (not entered)

    Usage:
        # Simple test with coordination
        coord_cfg = get_coordination_config()
        job = create_job('node1', postgres, coordination_config=coord_cfg)

        # Disable coordination
        job = create_job('node1', postgres, coordination_config=None)

        # Custom coordination settings
        coord_cfg = CoordinationConfig(total_tokens=50, heartbeat_interval_sec=1)
        job = create_job('node1', postgres, coordination_config=coord_cfg)
    """
    if coordination_config is not None:
        url = postgres.url
        coordination_config = replace(
            coordination_config,
            host=url.host,
            port=url.port,
            dbname=url.database,
            user=url.username,
            password=url.password
        )

    return Job(
        node_name,
        coordination_config=coordination_config,
        wait_on_enter=wait_on_enter,
        **kwargs
    )


def create_task(task_id: Hashable, name: str = None) -> Task:
    """Create Task with default name.

    Args:
        task_id: Unique task identifier

    Returns
        Task instance with name 'task-{task_id}'
    """
    name = name or f'task-{task_id}'
    return Task(task_id, name)


# ============================================================================
# WAIT HELPERS - Poll for conditions with timeout
#
# PREFER wait_for_condition() or wait_for() FOR SIMPLE CONDITIONS:
#   assert wait_for(lambda: len(job.my_tokens) >= 10)
#   assert wait_for(lambda: job.am_i_healthy())
#   assert wait_for(lambda: job._shutdown_event.is_set())
#
# Use specialized helpers only for complex multi-step operations.
# ============================================================================

def wait_for_condition(
    condition: callable,
    timeout_sec: float = 5.0,
    check_interval: float = 0.1
) -> bool:
    """Wait for arbitrary condition function to return True.

    Args:
        condition: Function that returns bool
        timeout_sec: Maximum wait time
        check_interval: How often to check

    Returns
        True if condition met, False if timeout

    Usage:
        assert wait_for_condition(lambda: len(my_list) > 0, timeout_sec=5)
        assert wait_for(lambda: job.am_i_healthy())  # Using alias
    """
    start = time.time()
    while time.time() - start < timeout_sec:
        try:
            if condition():
                return True
        except Exception:
            pass
        time.sleep(check_interval)
    return False


def wait_for(condition: callable, timeout_sec: float = 5.0) -> bool:
    """Shorter alias for wait_for_condition().

    Usage:
        assert wait_for(lambda: job.am_i_healthy())
        assert wait_for(lambda: len(job.my_tokens) >= 10, timeout_sec=15)
        assert wait_for(lambda: job._shutdown_event.is_set())
    """
    return wait_for_condition(condition, timeout_sec)


def wait_for_leader_election(
    job: Job,
    expected_leader: str = None,
    timeout_sec: float = 10.0,
    check_interval: float = 0.2
) -> str:
    """Wait for leader to be elected, optionally verify expected leader.

    Args:
        job: Job instance to check
        expected_leader: Optional expected leader name
        timeout_sec: Maximum wait time
        check_interval: How often to check

    Returns
        Leader name if elected (and matches expected if provided)

    Raises
        TimeoutError: If timeout reached

    Usage:
        leader = wait_for_leader_election(job, expected_leader='node1')
    """
    start = time.time()
    last_leader = None

    while time.time() - start < timeout_sec:
        try:
            leader = job.cluster.elect_leader()
            last_leader = leader
            if expected_leader is None or leader == expected_leader:
                return leader
        except Exception:
            pass
        time.sleep(check_interval)

    if expected_leader is not None:
        raise TimeoutError(
            f'Leader election timeout: expected {expected_leader}, '
            f'got {last_leader} after {timeout_sec}s'
        )

    raise TimeoutError(f'Leader election timeout after {timeout_sec}s')


def wait_for_state(
    job: Job,
    state: JobState,
    timeout_sec: float = 10.0,
    check_interval: float = 0.1
) -> bool:
    """Wait for job to reach specific state.

    Args:
        job: Job instance to check
        state: Expected state
        timeout_sec: Maximum wait time
        check_interval: How often to check

    Returns
        True if state reached, False if timeout
    """
    start = time.time()
    while time.time() - start < timeout_sec:
        if job.state_machine.state == state:
            return True
        time.sleep(check_interval)
    return False


def wait_for_running_state(
    job: Job,
    timeout_sec: float = 5.0,
    check_interval: float = 0.1
) -> bool:
    """Wait for job to reach either RUNNING_LEADER or RUNNING_FOLLOWER state.

    Args:
        job: Job instance to check
        timeout_sec: Maximum wait time
        check_interval: How often to check

    Returns
        True if either running state reached, False if timeout

    Usage:
        assert wait_for_running_state(job, timeout_sec=10)
    """
    running_states = {JobState.RUNNING_LEADER, JobState.RUNNING_FOLLOWER}
    return any(wait_for_state(job, state, timeout_sec, check_interval) for state in running_states)


def wait_for_cluster_running(
    nodes: list[Job],
    leader_name: str = None,
    timeout_sec: float = 10.0
) -> bool:
    """Wait for all nodes in cluster to reach running states.

    Args:
        nodes: List of Job instances
        leader_name: Optional expected leader name
        timeout_sec: Maximum wait time per node

    Returns
        True if all nodes reach running states, False if timeout

    Usage:
        assert wait_for_cluster_running(nodes, leader_name='node1')
    """
    for node in nodes:
        if leader_name:
            expected = JobState.RUNNING_LEADER if node.node_name == leader_name else JobState.RUNNING_FOLLOWER
            if not wait_for_state(node, expected, timeout_sec):
                return False
        else:
            if not wait_for_running_state(node, timeout_sec):
                return False
    return True


def wait_for_rebalance(
    postgres,
    tables: dict,
    min_count: int = 1,
    timeout_sec: float = 20.0,
    check_interval: float = 0.3
) -> bool:
    """Wait for rebalance events to be logged (complex multi-step DB query).

    Args:
        postgres: postgres fixture
        tables: Table name dict from get_table_names()
        min_count: Minimum number of rebalance events
        timeout_sec: Maximum wait time
        check_interval: How often to check

    Returns
        True if condition met, False if timeout
    """
    start = time.time()
    while time.time() - start < timeout_sec:
        with postgres.connect() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM {tables["Rebalance"]}
                WHERE triggered_at > NOW() - INTERVAL '{int(timeout_sec)} seconds'
            """))
            if result.scalar() >= min_count:
                return True
        time.sleep(check_interval)
    return False


def wait_for_no_leader_lock(
    postgres,
    tables: dict,
    timeout_sec: float = 5.0,
    check_interval: float = 0.1
) -> bool:
    """Wait for leader lock to be released (DB query helper).

    Args:
        postgres: postgres fixture
        tables: Table name dict from get_table_names()
        timeout_sec: Maximum wait time
        check_interval: How often to check

    Returns
        True if no lock held, False if timeout
    """
    return wait_for_condition(
        lambda: _check_no_leader_lock(postgres, tables),
        timeout_sec,
        check_interval
    )


def _check_no_leader_lock(postgres, tables: dict) -> bool:
    """Check if leader lock exists (helper for wait_for_no_leader_lock).
    """
    with postgres.connect() as conn:
        result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["LeaderLock"]}'))
        return result.scalar() == 0


def wait_for_dead_node_removal(
    postgres,
    tables: dict,
    node_name: str,
    timeout_sec: float = 20.0,
    check_interval: float = 0.3
) -> bool:
    """Wait for dead node to be removed from Node table.

    Args:
        postgres: postgres fixture
        tables: Table name dict from get_table_names()
        node_name: Name of node to wait for removal
        timeout_sec: Maximum wait time
        check_interval: How often to check

    Returns
        True if node removed, False if timeout
    """
    start = time.time()
    while time.time() - start < timeout_sec:
        with postgres.connect() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM {tables["Node"]} WHERE name = :name
            """), {'name': node_name})
            if result.scalar() == 0:
                return True
        time.sleep(check_interval)
    return False


def wait_for_token_sync(
    nodes: list[Job],
    expected_total: int,
    check_cache: bool = True,
    timeout_sec: float = 10.0,
    check_interval: float = 0.2
) -> bool:
    """Wait for nodes to sync tokens with database.

    Args:
        nodes: List of Job instances
        expected_total: Expected total tokens across all nodes
        check_cache: If True, verify cached tokens match DB (use after rebalance)
        timeout_sec: Maximum wait time
        check_interval: How often to check

    Returns
        True if synced, False if timeout

    Usage:
        # Just check DB totals:
        assert wait_for_token_sync(nodes, 100, check_cache=False)

        # Verify cache also synced (after rebalance):
        assert wait_for_token_sync(nodes, 100, check_cache=True)
    """
    start = time.time()
    while time.time() - start < timeout_sec:
        all_synced = True
        total = 0

        for node in nodes:
            db_tokens, db_version = node.tokens.get_my_tokens_versioned()

            if check_cache:
                cached_tokens = node.tokens.my_tokens
                cached_version = node.tokens.token_version
                if db_version != cached_version or db_tokens != cached_tokens:
                    all_synced = False
                    break

            total += len(db_tokens)

        if all_synced and total == expected_total:
            return True

        time.sleep(check_interval)

    return False


def wait_for_all_nodes_token_sync(
    nodes: list[Job],
    expected_total: int,
    timeout_sec: float = 10.0,
    check_interval: float = 0.2
) -> bool:
    """Wait for all nodes to sync tokens to expected total (DB only, no cache check).

    Deprecated: Use wait_for_token_sync(nodes, total, check_cache=False) instead.
    """
    return wait_for_token_sync(nodes, expected_total, check_cache=False, timeout_sec=timeout_sec, check_interval=check_interval)


def wait_for_cached_tokens_sync(
    nodes: list[Job],
    expected_total: int,
    timeout_sec: float = 10.0,
    check_interval: float = 0.2
) -> bool:
    """Wait for nodes' cached token sets to sync with database.

    Deprecated: Use wait_for_token_sync(nodes, total, check_cache=True) instead.
    """
    return wait_for_token_sync(nodes, expected_total, check_cache=True, timeout_sec=timeout_sec, check_interval=check_interval)


def wait_for_shutdown(node: Job, timeout_sec: float = 5.0, check_interval: float = 0.1) -> bool:
    """Wait for all monitor threads to stop.

    Args:
        node: Job instance with monitors to check
        timeout_sec: Maximum time to wait
        check_interval: Time between checks

    Returns
        True if all threads stopped within timeout, False otherwise
    """
    def all_threads_stopped():
        return all(
            not m.thread or not m.thread.is_alive()
            for m in node._monitors.values()
        )

    return wait_for(all_threads_stopped, timeout_sec=timeout_sec)


# ============================================================================
# ASSERTION HELPERS - Common test assertions
# ============================================================================

def assert_token_distribution_balanced(
    jobs: list[Job],
    total_tokens: int,
    tolerance: float = 0.15
) -> None:
    """Assert tokens are evenly distributed across jobs.

    Args:
        jobs: List of Job instances
        total_tokens: Expected total token count
        tolerance: Acceptable deviation (0.15 = 15%)

    Raises
        AssertionError: If distribution is imbalanced

    Usage:
        assert_token_distribution_balanced([job1, job2, job3], total_tokens=100)
    """
    node_count = len(jobs)
    if node_count == 0:
        raise ValueError('No jobs provided')

    expected_per_node = total_tokens / node_count
    min_acceptable = int(expected_per_node * (1 - tolerance))
    max_acceptable = int(expected_per_node * (1 + tolerance))

    actual_total = 0
    for job in jobs:
        token_count = len(job.my_tokens)
        actual_total += token_count

        assert min_acceptable <= token_count <= max_acceptable, (
            f'{job.node_name} has {token_count} tokens, '
            f'expected {min_acceptable}-{max_acceptable} '
            f'(target: {expected_per_node:.1f})'
        )

    assert actual_total == total_tokens, (
        f'Total tokens {actual_total} != expected {total_tokens}'
    )


def assert_monitors_running(
    job: Job,
    monitor_names: list[str] = None
) -> None:
    """Assert specified monitors are running.

    Args:
        job: Job instance to check
        monitor_names: Optional list of monitor name substrings to check
                      (if None, checks all monitors are running)

    Raises
        AssertionError: If any monitor not running
    """
    if monitor_names is None:
        for monitor in job._monitors.values():
            assert monitor.thread is not None, (
                f'Monitor {monitor.name} has no thread'
            )
            assert monitor.thread.is_alive(), (
                f'Monitor {monitor.name} thread is not alive'
            )
    else:
        for name_pattern in monitor_names:
            found = False
            for monitor in job._monitors.values():
                if name_pattern in monitor.name:
                    found = True
                    assert monitor.thread is not None, (
                        f'Monitor {monitor.name} has no thread'
                    )
                    assert monitor.thread.is_alive(), (
                        f'Monitor {monitor.name} thread is not alive'
                    )

            assert found, f'No monitor found matching pattern: {name_pattern}'


def assert_monitors_stopped(
    job: Job,
    monitor_names: list[str] = None
) -> None:
    """Assert specified monitors are stopped.

    Args:
        job: Job instance to check
        monitor_names: Optional list of monitor name substrings to check

    Raises
        AssertionError: If any monitor still running
    """
    if monitor_names is None:
        for monitor in job._monitors.values():
            if monitor.thread and monitor.thread.is_alive():
                raise AssertionError(
                    f'Monitor {monitor.name} is still running'
                )
    else:
        for name_pattern in monitor_names:
            for monitor in job._monitors.values():
                if name_pattern in monitor.name and monitor.thread and monitor.thread.is_alive():
                    raise AssertionError(
                        f'Monitor {monitor.name} is still running'
                    )


# ============================================================================
# DATABASE HELPERS - Setup/query utilities
# ============================================================================

def get_fresh_token_count(node: Job) -> int:
    """Get current token count directly from database (bypasses cache).

    Args:
        node: Job instance to query

    Returns
        Current token count for this node
    """
    tokens, _ = node.tokens.get_my_tokens_versioned()
    return len(tokens)


def clean_tables(*table_names):
    """Decorator to clear tables before test execution.

    Args:
        *table_names: Table keys to clear (e.g., 'Node', 'Token', 'Lock')

    Usage:
        @clean_tables('Node', 'Token', 'Lock')
        def test_something(postgres):
            # Tables already cleaned
            ...

        # Also works on class methods:
        @clean_tables('Node')
        def test_method(self, postgres):
            ...
    """
    def decorator(func: callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Handle both regular functions and class methods
            # pytest passes fixtures as kwargs, so postgres is usually in kwargs

            # Get postgres from kwargs or args
            postgres = kwargs.get('postgres')
            if postgres is None:
                # Check if it's in args (class method with postgres as positional)
                if args and hasattr(args[0], '__class__') and not isinstance(args[0], dict):
                    # Class method: args = (self, postgres, ...) - but pytest uses kwargs
                    if len(args) >= 2:
                        postgres = args[1]
                elif args:
                    # Regular function: args = (postgres, ...)
                    postgres = args[0]

            if postgres is None:
                raise TypeError(f"{func.__name__}() missing required 'postgres' fixture")

            tables = schema.get_table_names(test_config().appname)
            clear_tables(postgres, tables, list(table_names))
            return func(*args, **kwargs)
        return wrapper
    return decorator


def clear_tables(postgres, tables: dict, table_names: list[str]) -> None:
    """Clear specified tables for testing.

    Args:
        postgres: postgres fixture
        tables: Table name dict from get_table_names()
        table_names: List of table keys to clear (e.g., ['Lock', 'Node'])
    """
    with postgres.connect() as conn:
        for table_key in table_names:
            table_name = tables[table_key]
            conn.execute(text(f'DELETE FROM {table_name}'))
        conn.commit()


def delete_rows(postgres, tables: dict, table_key: str, where_clause: str, params: dict = None) -> int:
    """Delete rows from table with optional WHERE clause.

    Args:
        postgres: postgres fixture
        tables: Table name dict from get_table_names()
        table_key: Table key to delete from (e.g., 'Node', 'Lock')
        where_clause: WHERE clause without 'WHERE' keyword (e.g., 'name = :name')
        params: Optional parameters dict for WHERE clause (e.g., {'name': 'node1'})

    Returns
        Number of rows deleted

    Usage:
        delete_rows(postgres, tables, 'Node', 'name = :name', {'name': 'node1'})
    """
    table_name = tables[table_key]
    sql = f'DELETE FROM {table_name} WHERE {where_clause}'

    with postgres.connect() as conn:
        result = conn.execute(text(sql), params or {})
        rows_deleted = result.rowcount
        conn.commit()

    return rows_deleted


def insert_active_node(
    postgres,
    tables: dict,
    node_name: str,
    created_on: datetime.datetime = None
) -> None:
    """Insert active node with current heartbeat for testing.

    Args:
        postgres: postgres fixture
        tables: Table name dict from get_table_names()
        node_name: Name for node
        created_on: Optional creation timestamp (defaults to now)
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    if created_on is None:
        created_on = now

    with postgres.connect() as conn:
        conn.execute(text(f"""
            INSERT INTO {tables["Node"]} (name, created_on, last_heartbeat)
            VALUES (:name, :created_on, :heartbeat)
        """), {
            'name': node_name,
            'created_on': created_on,
            'heartbeat': now
        })
        conn.commit()


def insert_stale_node(
    postgres,
    tables: dict,
    node_name: str,
    heartbeat_age_seconds: int = 30
) -> None:
    """Insert node with old heartbeat for testing dead node detection.

    Args:
        postgres: postgres fixture
        tables: Table name dict from get_table_names()
        node_name: Name for dead node
        heartbeat_age_seconds: How old the heartbeat should be
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    stale_heartbeat = now - datetime.timedelta(seconds=heartbeat_age_seconds)

    with postgres.connect() as conn:
        conn.execute(text(f"""
            INSERT INTO {tables["Node"]} (name, created_on, last_heartbeat)
            VALUES (:name, :created_on, :heartbeat)
        """), {
            'name': node_name,
            'created_on': now,
            'heartbeat': stale_heartbeat
        })
        conn.commit()


def insert_lock(
    postgres,
    tables: dict,
    task_id: str | int,
    patterns: list[str],
    created_by: str = 'test',
    expires_at: datetime.datetime = None,
    reason: str = 'test lock',
    raw_patterns: str = None
) -> None:
    """Insert lock record for testing.

    Args:
        postgres: postgres fixture
        tables: Table name dict
        task_id: Task identifier to lock
        patterns: Node patterns for lock
        created_by: Creator node name
        expires_at: Optional expiration timestamp
        reason: Lock reason/description
        raw_patterns: Raw JSON string for patterns (for testing invalid JSON)
    """
    now = datetime.datetime.now(datetime.timezone.utc)

    patterns_value = raw_patterns if raw_patterns is not None else json.dumps(patterns)

    with postgres.connect() as conn:
        conn.execute(text(f"""
            INSERT INTO {tables["Lock"]}
            (task_id, node_patterns, reason, created_at, created_by, expires_at)
            VALUES (:task_id, :patterns, :reason, :created_at, :created_by, :expires_at)
        """), {
            'task_id': str(task_id),
            'patterns': patterns_value,
            'reason': reason,
            'created_at': now,
            'created_by': created_by,
            'expires_at': expires_at
        })
        conn.commit()


def insert_inst(postgres, tables: dict, item: str, done: bool = False) -> None:
    """Insert test task record into Inst table.

    Args:
        postgres: postgres fixture
        tables: Table name dict
        item: Task item identifier
        done: Task completion status
    """
    with postgres.connect() as conn:
        conn.execute(text(f'INSERT INTO {tables["Inst"]} (item, done) VALUES (:item, :done)'),
                    {'item': item, 'done': done})
        conn.commit()


def insert_leader_lock(postgres, tables: dict, node: str, operation: str, acquired_at: datetime.datetime = None) -> None:
    """Insert leader lock record for testing.

    Args:
        postgres: postgres fixture
        tables: Table name dict
        node: Node holding the lock
        operation: Operation description
        acquired_at: Lock acquisition timestamp (defaults to now)
    """
    if acquired_at is None:
        acquired_at = datetime.datetime.now(datetime.timezone.utc)

    with postgres.connect() as conn:
        conn.execute(text(f"""
            INSERT INTO {tables["LeaderLock"]} (singleton, node, acquired_at, operation)
            VALUES (1, :node, :acquired_at, :operation)
        """), {
            'node': node,
            'acquired_at': acquired_at,
            'operation': operation
        })
        conn.commit()


def insert_token(
    postgres,
    tables: dict,
    token_id: int,
    node: str,
    assigned_at: datetime.datetime = None,
    version: int = 1
) -> None:
    """Insert token assignment record for testing.

    Args:
        postgres: postgres fixture
        tables: Table name dict
        token_id: Token identifier
        node: Node assigned to token
        assigned_at: Assignment timestamp (defaults to now)
        version: Token version
    """
    if assigned_at is None:
        assigned_at = datetime.datetime.now(datetime.timezone.utc)

    with postgres.connect() as conn:
        conn.execute(text(f"""
            INSERT INTO {tables["Token"]} (token_id, node, assigned_at, version)
            VALUES (:token_id, :node, :assigned_at, :version)
        """), {
            'token_id': token_id,
            'node': node,
            'assigned_at': assigned_at,
            'version': version
        })
        conn.commit()


def get_token_assignments(
    postgres,
    tables: dict
) -> dict[int, str]:
    """Get current token assignments from database.

    Args:
        postgres: postgres fixture
        tables: Table name dict

    Returns
        Dict mapping token_id -> node_name
    """
    with postgres.connect() as conn:
        result = conn.execute(
            text(f'SELECT token_id, node FROM {tables["Token"]}')
        )
        return {row[0]: row[1] for row in result}


# ============================================================================
# PATTERN MATCHERS - For mock usage
# ============================================================================

def exact_match(node: str, pattern: str) -> bool:
    """Pattern matcher that only matches exact strings.

    Used for testing distribution algorithm without SQL LIKE complexity.
    """
    return node == pattern


def wildcard_match(node: str, pattern: str) -> bool:
    """Pattern matcher supporting % wildcard (SQL LIKE style).

    Used for testing distribution algorithm with pattern matching.
    """
    if pattern == node:
        return True
    if '%' not in pattern:
        return False
    if pattern.endswith('%'):
        return node.startswith(pattern[:-1])
    if pattern.startswith('%'):
        return node.endswith(pattern[1:])
    return False
