"""Config builders, job factories, wait and assert helpers, and row inserts.
"""
import contextlib
import datetime
import functools
import json
import logging
import threading
import time
from collections.abc import Callable, Hashable, Iterator
from dataclasses import replace
from typing import Any

import pytest
from sqlalchemy import Engine, text

from jobsync import schema
from jobsync.client import CoordinationConfig, Job, JobState, Task
from jobsync.client import task_to_token

logger = logging.getLogger(__name__)


# --- Config builders ---

def get_coordination_config(**overrides: Any) -> CoordinationConfig:
    """Same as test_config(): CoordinationConfig with test-optimized values.
    """
    return test_config(**overrides)


def test_config(**overrides: Any) -> CoordinationConfig:
    """CoordinationConfig with short intervals for fast tests.

    Parameters
    ----------
    **overrides : Any
        CoordinationConfig fields that replace the test defaults.

    Returns
    -------
    CoordinationConfig
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


# The test_ prefix makes pytest collect this helper as a test in every
# module that star-imports fixtures.
test_config.__test__ = False


def get_state_driven_config(**overrides: Any) -> CoordinationConfig:
    """Same as test_config(), named for state machine tests.
    """
    return test_config(**overrides)


def get_structure_test_config(**overrides: Any) -> CoordinationConfig:
    """Same as test_config(), named for schema structure tests.
    """
    return test_config(**overrides)


# --- Cluster management ---

@contextlib.contextmanager
def cluster(
    postgres: Engine,
    *node_names: str,
    **config_overrides: Any
) -> Iterator[list[Job]]:
    """Start cluster of nodes in parallel with automatic cleanup.

    Parameters
    ----------
    postgres : Engine
        The postgres fixture.
    *node_names : str
        Node names to create, entered in this order 0.1s apart.
    **config_overrides : Any
        CoordinationConfig overrides on top of test_config().

    Yields
    ------
    list[Job]
        Entered jobs, in node_names order. Every job is exited on leaving
        the block.

    Raises
    ------
    RuntimeError
        A job's __enter__ raised. The other jobs are exited first.
    """
    config = test_config(**config_overrides)
    jobs = [
        create_job(name, postgres, coordination_config=config, wait_on_enter=15)
        for name in node_names
        ]

    enter_errors = []

    def enter(job: Job) -> None:
        try:
            job.__enter__()
        except Exception as exc:
            enter_errors.append((job.node_name, exc))

    threads = [threading.Thread(target=enter, args=(job,)) for job in jobs]
    for i, t in enumerate(threads):
        t.start()
        if i < len(threads) - 1:
            time.sleep(0.1)

    for t in threads:
        t.join()

    try:
        if enter_errors:
            raise RuntimeError(f'cluster nodes failed to enter: {enter_errors}')
        yield jobs
    finally:
        for job in jobs:
            with contextlib.suppress(Exception):
                job.__exit__(None, None, None)


def find_task_ids_covering_all_tokens(
    config: CoordinationConfig,
    max_search: int = 10000
) -> list[Hashable]:
    """One task id per token, each hashing to its token under config.

    Parameters
    ----------
    config : CoordinationConfig
        Supplies total_tokens and hash_function.
    max_search : int, default 10000
        Candidate ids tried, 0 through max_search - 1.

    Returns
    -------
    list[Hashable]
        Integer task ids. Entry i hashes to token i.

    Raises
    ------
    RuntimeError
        The candidates leave a token uncovered.
    """
    total_tokens = config.total_tokens
    hash_function = config.hash_function

    token_to_task = {}
    for candidate in range(max_search):
        token_id = task_to_token(candidate, total_tokens, hash_function)
        if token_id not in token_to_task:
            token_to_task[token_id] = candidate
        if len(token_to_task) == total_tokens:
            break

    if len(token_to_task) < total_tokens:
        raise RuntimeError(
            f'Could not find task IDs covering all {total_tokens} tokens '
            f'(found {len(token_to_task)} after searching {max_search} candidates)')

    return [token_to_task[i] for i in range(total_tokens)]


# --- Test utilities ---

def simulate_node_crash(node: Job, cleanup: bool = False) -> None:
    """Stop a node's monitor threads without the shutdown cleanup.

    Parameters
    ----------
    node : Job
        Entered job to crash. Its Node row stays, with a heartbeat that goes
        stale, so the leader detects it as dead. The caller still exits it.
    cleanup : bool, default False
        Also run the node's table cleanup (its Node, Claim and Check rows go)
        and dispose its engine, so it leaves at once and is never seen as
        dead.
    """
    node._shutdown_event.set()
    for monitor in list(node._monitors.values()):
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
    """Records each on_rebalance call, safe across threads.

    Attributes
    ----------
    rebalance_calls : list[float]
        Epoch seconds of each on_rebalance call, oldest first.
    lock : threading.Lock
        Guards rebalance_calls.
    """

    def __init__(self) -> None:
        self.rebalance_calls = []
        self.lock = threading.Lock()

    def on_rebalance(self) -> None:
        """Record the call time in rebalance_calls.
        """
        with self.lock:
            self.rebalance_calls.append(time.time())
            logger.info('on_rebalance called')

    def reset(self) -> None:
        """Empty rebalance_calls.
        """
        with self.lock:
            self.rebalance_calls.clear()


# --- Factories ---

@pytest.fixture(scope='module')
def shared_unit_test_job(postgres: Engine) -> Job:
    """Unentered job shared by every test in a module.

    Returns
    -------
    Job
        Built with wait_on_enter=0. Only for tests that change no state,
        since later tests in the module reuse it.
    """
    coord_config = get_coordination_config()
    job = create_job(
        'unit-test-shared',
        postgres,
        coordination_config=coord_config,
        wait_on_enter=0)
    return job


@pytest.fixture
def callback_tracker() -> CallbackTracker:
    """A fresh CallbackTracker.
    """
    return CallbackTracker()


def create_job(
    node_name: str,
    postgres: Engine,
    coordination_config: CoordinationConfig = None,
    wait_on_enter: int = 0,
    **kwargs: Any
) -> Job:
    """Unentered Job pointed at the test database.

    Parameters
    ----------
    node_name : str
        Unique node identifier.
    postgres : Engine
        The postgres fixture. Its URL replaces the host, port, dbname, user
        and password in coordination_config.
    coordination_config : CoordinationConfig, default None
        None runs the job without coordination.
    wait_on_enter : int, default 0
        Seconds __enter__ waits for the cluster to form: 0 for unit tests,
        2-5 for simple integration tests, 10-15 for multi-node clusters.
    **kwargs : Any
        Passed to Job().

    Returns
    -------
    Job
    """
    if coordination_config is not None:
        url = postgres.url
        coordination_config = replace(
            coordination_config,
            host=url.host,
            port=url.port,
            dbname=url.database,
            user=url.username,
            password=url.password)

    return Job(
        node_name,
        coordination_config=coordination_config,
        wait_on_enter=wait_on_enter,
        **kwargs)


def create_task(task_id: Hashable, name: str = None) -> Task:
    """Task for task_id, named name or else 'task-{task_id}'.
    """
    name = name or f'task-{task_id}'
    return Task(task_id, name)


# --- Wait helpers ---

def wait_for_condition(
    condition: callable,
    timeout_sec: float = 5.0,
    check_interval: float = 0.1
) -> bool:
    """Poll condition until it returns a truthy value or time runs out.

    Parameters
    ----------
    condition : callable
        Called with no arguments. An exception it raises counts as False.
    timeout_sec : float, default 5.0
        Seconds to keep polling.
    check_interval : float, default 0.1
        Seconds between calls.

    Returns
    -------
    bool
        True once condition holds, False on timeout.
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
    """wait_for_condition() with the default check_interval.
    """
    return wait_for_condition(condition, timeout_sec)


def wait_for_leader_election(
    job: Job,
    expected_leader: str = None,
    timeout_sec: float = 10.0,
    check_interval: float = 0.2
) -> str:
    """Poll job.cluster.elect_leader() until a leader is elected.

    Parameters
    ----------
    job : Job
        Job whose cluster is polled.
    expected_leader : str, default None
        Keep polling until this node leads. None accepts any leader.
    timeout_sec : float, default 10.0
        Seconds to keep polling.
    check_interval : float, default 0.2
        Seconds between polls.

    Returns
    -------
    str
        Name of the elected leader.

    Raises
    ------
    TimeoutError
        No leader, or a leader other than expected_leader, within
        timeout_sec. An exception from elect_leader counts as no leader.
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
            f'got {last_leader} after {timeout_sec}s')

    raise TimeoutError(f'Leader election timeout after {timeout_sec}s')


def wait_for_state(
    job: Job,
    state: JobState,
    timeout_sec: float = 10.0,
    check_interval: float = 0.1
) -> bool:
    """Poll until the job's state machine is in state.

    Parameters
    ----------
    job : Job
        Job to watch.
    state : JobState
        State to wait for.
    timeout_sec : float, default 10.0
        Seconds to keep polling.
    check_interval : float, default 0.1
        Seconds between checks.

    Returns
    -------
    bool
        True once state is reached, False on timeout.
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

    Parameters
    ----------
    job : Job
        Job instance to check.
    timeout_sec : float, default 5.0
        Maximum wait for either state, in seconds.
    check_interval : float, default 0.1
        Seconds between checks.

    Returns
    -------
    bool
        True once the job is in either running state, False on timeout.
    """
    return wait_for_condition(job.state_machine.is_running, timeout_sec, check_interval)


def wait_for_cluster_running(
    nodes: list[Job],
    leader_name: str = None,
    timeout_sec: float = 10.0
) -> bool:
    """Wait for every node to reach a running state, one node at a time.

    Parameters
    ----------
    nodes : list[Job]
        Nodes to wait on, in order.
    leader_name : str, default None
        Node that must reach RUNNING_LEADER. Every other node must reach
        RUNNING_FOLLOWER. None accepts either running state.
    timeout_sec : float, default 10.0
        Seconds allowed per node, so the whole wait can run to
        len(nodes) * timeout_sec.

    Returns
    -------
    bool
        True once all nodes are running, False at the first node to time
        out.
    """
    for node in nodes:
        if leader_name:
            expected = (
                JobState.RUNNING_LEADER if node.node_name == leader_name
                else JobState.RUNNING_FOLLOWER)
            if not wait_for_state(node, expected, timeout_sec):
                return False
        else:
            if not wait_for_running_state(node, timeout_sec):
                return False
    return True


def wait_for_rebalance(
    postgres: Engine,
    tables: dict,
    min_count: int = 1,
    timeout_sec: float = 20.0,
    check_interval: float = 0.3
) -> bool:
    """Poll the Rebalance table until enough recent events are logged.

    Parameters
    ----------
    postgres : Engine
        The postgres fixture.
    tables : dict
        Table name dict from get_table_names().
    min_count : int, default 1
        Events required. Each poll counts only events triggered within the
        last timeout_sec seconds.
    timeout_sec : float, default 20.0
        Seconds to keep polling, and the age window above, truncated to
        whole seconds.
    check_interval : float, default 0.3
        Seconds between polls.

    Returns
    -------
    bool
        True once min_count is reached, False on timeout.
    """
    sql = f"""
select count(*) from {tables["Rebalance"]}
where triggered_at > now() - interval '{int(timeout_sec)} seconds'
"""
    start = time.time()
    while time.time() - start < timeout_sec:
        with postgres.connect() as conn:
            result = conn.execute(text(sql))
            if result.scalar() >= min_count:
                return True
        time.sleep(check_interval)
    return False


def wait_for_no_leader_lock(
    postgres: Engine,
    tables: dict,
    timeout_sec: float = 5.0,
    check_interval: float = 0.1
) -> bool:
    """Poll until the LeaderLock table is empty.

    Parameters
    ----------
    postgres : Engine
        The postgres fixture.
    tables : dict
        Table name dict from get_table_names().
    timeout_sec : float, default 5.0
        Seconds to keep polling.
    check_interval : float, default 0.1
        Seconds between polls.

    Returns
    -------
    bool
        True once no lock is held, False on timeout.
    """
    def no_leader_lock() -> bool:
        with postgres.connect() as conn:
            result = conn.execute(text(f'select count(*) from {tables["LeaderLock"]}'))
            return result.scalar() == 0

    return wait_for_condition(no_leader_lock, timeout_sec, check_interval)


def wait_for_dead_node_removal(
    postgres: Engine,
    tables: dict,
    node_name: str,
    timeout_sec: float = 20.0,
    check_interval: float = 0.3
) -> bool:
    """Poll until node_name has no row in the Node table.

    Parameters
    ----------
    postgres : Engine
        The postgres fixture.
    tables : dict
        Table name dict from get_table_names().
    node_name : str
        Node whose row must go.
    timeout_sec : float, default 20.0
        Seconds to keep polling.
    check_interval : float, default 0.3
        Seconds between polls.

    Returns
    -------
    bool
        True once the row is gone, False on timeout.
    """
    start = time.time()
    while time.time() - start < timeout_sec:
        with postgres.connect() as conn:
            result = conn.execute(
                text(f'select count(*) from {tables["Node"]} where name = :name'),
                {'name': node_name})
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
    """Poll until the nodes' token rows in the database total expected_total.

    Parameters
    ----------
    nodes : list[Job]
        Nodes whose token rows are counted.
    expected_total : int
        Token count the nodes must hold between them.
    check_cache : bool, default True
        Also require each node's cached tokens and version to match its
        database rows. Pass True after a rebalance.
    timeout_sec : float, default 10.0
        Seconds to keep polling.
    check_interval : float, default 0.2
        Seconds between polls.

    Returns
    -------
    bool
        True once synced, False on timeout.
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
    """Deprecated alias of wait_for_token_sync(..., check_cache=False).
    """
    return wait_for_token_sync(
        nodes,
        expected_total,
        check_cache=False,
        timeout_sec=timeout_sec,
        check_interval=check_interval)


def wait_for_cached_tokens_sync(
    nodes: list[Job],
    expected_total: int,
    timeout_sec: float = 10.0,
    check_interval: float = 0.2
) -> bool:
    """Deprecated alias of wait_for_token_sync(..., check_cache=True).
    """
    return wait_for_token_sync(
        nodes,
        expected_total,
        check_cache=True,
        timeout_sec=timeout_sec,
        check_interval=check_interval)


def wait_for_shutdown(
    node: Job,
    timeout_sec: float = 5.0,
    check_interval: float = 0.1
) -> bool:
    """Wait for every monitor thread of node to stop.

    Parameters
    ----------
    node : Job
        Job whose monitor threads are checked.
    timeout_sec : float, default 5.0
        Seconds to keep polling.
    check_interval : float, default 0.1
        Ignored. Polling runs every 0.1 seconds.

    Returns
    -------
    bool
        True once every thread has stopped, False on timeout.
    """
    def all_threads_stopped() -> bool:
        return all(
            not m.thread or not m.thread.is_alive()
            for m in node._monitors.values())

    return wait_for(all_threads_stopped, timeout_sec=timeout_sec)


# --- Assertion helpers ---

def assert_token_distribution_balanced(
    jobs: list[Job],
    total_tokens: int,
    tolerance: float = 0.15
) -> None:
    """Assert jobs split total_tokens evenly, within tolerance.

    Parameters
    ----------
    jobs : list[Job]
        Jobs whose cached my_tokens are counted.
    total_tokens : int
        Token count the jobs must hold between them, exactly.
    tolerance : float, default 0.15
        Allowed deviation from the even share, as a fraction (0.15 is 15%).
        Both bounds truncate to int.

    Raises
    ------
    AssertionError
        A job's count is outside the bounds, or the counts do not sum to
        total_tokens.
    ValueError
        jobs is empty.
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
    """Assert the named monitors have live threads.

    Parameters
    ----------
    job : Job
        Job whose monitors are checked.
    monitor_names : list[str], default None
        Substrings of monitor names. Each must match at least one monitor,
        and every match must be alive. None checks every monitor.

    Raises
    ------
    AssertionError
        A matched monitor has no thread or a dead one, or a substring
        matches no monitor.
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
    """Assert the named monitors have no live thread.

    Parameters
    ----------
    job : Job
        Job whose monitors are checked.
    monitor_names : list[str], default None
        Substrings of monitor names. A substring that matches no monitor
        passes. None checks every monitor.

    Raises
    ------
    AssertionError
        A matched monitor's thread is alive.
    """
    if monitor_names is None:
        for monitor in job._monitors.values():
            if monitor.thread and monitor.thread.is_alive():
                raise AssertionError(
                    f'Monitor {monitor.name} is still running')
    else:
        for name_pattern in monitor_names:
            for monitor in job._monitors.values():
                if (name_pattern in monitor.name
                    and monitor.thread
                    and monitor.thread.is_alive()):
                    raise AssertionError(
                        f'Monitor {monitor.name} is still running')


# --- Database helpers ---

def get_fresh_token_count(node: Job) -> int:
    """Token count for node read from the database, bypassing its cache.
    """
    tokens, _ = node.tokens.get_my_tokens_versioned()
    return len(tokens)


def clean_tables(*table_names: str) -> Callable:
    """Decorator that empties the named tables before the test runs.

    Parameters
    ----------
    *table_names : str
        Keys of get_table_names(), e.g. 'Node', 'Token', 'Lock'.

    Returns
    -------
    Callable
        Decorator for a test function or method that takes the postgres
        fixture. The wrapped test raises TypeError when it gets none.
    """
    def decorator(func: callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            postgres = kwargs.get('postgres')
            if postgres is None:
                if (args and hasattr(args[0], '__class__')
                    and not isinstance(args[0], dict)):
                    if len(args) >= 2:
                        postgres = args[1]
                elif args:
                    postgres = args[0]

            if postgres is None:
                raise TypeError(f"{func.__name__}() missing required 'postgres' fixture")

            tables = schema.get_table_names(test_config().appname)
            clear_tables(postgres, tables, list(table_names))
            return func(*args, **kwargs)
        return wrapper
    return decorator


def clear_tables(postgres: Engine, tables: dict, table_names: list[str]) -> None:
    """Delete every row from the named tables in one transaction.

    Parameters
    ----------
    postgres : Engine
        The postgres fixture.
    tables : dict
        Table name dict from get_table_names().
    table_names : list[str]
        Keys into tables, e.g. ['Lock', 'Node'].
    """
    with postgres.connect() as conn:
        for table_key in table_names:
            table_name = tables[table_key]
            conn.execute(text(f'delete from {table_name}'))
        conn.commit()


def delete_rows(
    postgres: Engine,
    tables: dict,
    table_key: str,
    where_clause: str,
    params: dict = None
) -> int:
    """Delete the rows of one table that match where_clause.

    Parameters
    ----------
    postgres : Engine
        The postgres fixture.
    tables : dict
        Table name dict from get_table_names().
    table_key : str
        Key into tables, e.g. 'Node'.
    where_clause : str
        SQL condition without the WHERE keyword, e.g. 'name = :name'.
        It goes into the statement as written.
    params : dict, default None
        Bind values for where_clause, e.g. {'name': 'node1'}.

    Returns
    -------
    int
        Rows deleted.
    """
    table_name = tables[table_key]
    sql = f'delete from {table_name} where {where_clause}'

    with postgres.connect() as conn:
        result = conn.execute(text(sql), params or {})
        rows_deleted = result.rowcount
        conn.commit()

    return rows_deleted


def insert_active_node(
    postgres: Engine,
    tables: dict,
    node_name: str,
    created_on: datetime.datetime = None
) -> None:
    """Insert a Node row whose heartbeat is now.

    Parameters
    ----------
    postgres : Engine
        The postgres fixture.
    tables : dict
        Table name dict from get_table_names().
    node_name : str
        Name for the node.
    created_on : datetime.datetime, default None
        Creation time. None means now.
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    if created_on is None:
        created_on = now

    sql = f"""
insert into {tables["Node"]} (name, created_on, last_heartbeat)
values (:name, :created_on, :heartbeat)
"""
    with postgres.connect() as conn:
        conn.execute(text(sql), {
            'name': node_name,
            'created_on': created_on,
            'heartbeat': now,
            })
        conn.commit()


def insert_stale_node(
    postgres: Engine,
    tables: dict,
    node_name: str,
    heartbeat_age_seconds: int = 30
) -> None:
    """Insert a Node row created now with an old heartbeat.

    Parameters
    ----------
    postgres : Engine
        The postgres fixture.
    tables : dict
        Table name dict from get_table_names().
    node_name : str
        Name for the dead node.
    heartbeat_age_seconds : int, default 30
        Seconds before now of the heartbeat.
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    stale_heartbeat = now - datetime.timedelta(seconds=heartbeat_age_seconds)

    sql = f"""
insert into {tables["Node"]} (name, created_on, last_heartbeat)
values (:name, :created_on, :heartbeat)
"""
    with postgres.connect() as conn:
        conn.execute(text(sql), {
            'name': node_name,
            'created_on': now,
            'heartbeat': stale_heartbeat,
            })
        conn.commit()


def insert_lock(
    postgres: Engine,
    tables: dict,
    task_id: str | int,
    patterns: list[str],
    created_by: str = 'test',
    expires_at: datetime.datetime = None,
    reason: str = 'test lock',
    raw_patterns: str = None
) -> None:
    """Insert a Lock row created now.

    Parameters
    ----------
    postgres : Engine
        The postgres fixture.
    tables : dict
        Table name dict from get_table_names().
    task_id : str | int
        Task to lock, stored as str.
    patterns : list[str]
        Node patterns, stored as JSON.
    created_by : str, default 'test'
        Creator node name.
    expires_at : datetime.datetime, default None
        Expiry time. None stores NULL.
    reason : str, default 'test lock'
        Lock reason.
    raw_patterns : str, default None
        Stored in place of the JSON of patterns when given, e.g. invalid
        JSON.
    """
    now = datetime.datetime.now(datetime.timezone.utc)

    patterns_value = raw_patterns if raw_patterns is not None else json.dumps(patterns)

    sql = f"""
insert into {tables["Lock"]}
(task_id, node_patterns, reason, created_at, created_by, expires_at)
values (:task_id, :patterns, :reason, :created_at, :created_by, :expires_at)
"""
    with postgres.connect() as conn:
        conn.execute(text(sql), {
            'task_id': str(task_id),
            'patterns': patterns_value,
            'reason': reason,
            'created_at': now,
            'created_by': created_by,
            'expires_at': expires_at,
            })
        conn.commit()


def insert_inst(
    postgres: Engine,
    tables: dict,
    task_id: str,
    done: bool = False
) -> None:
    """Insert an Inst row.

    Parameters
    ----------
    postgres : Engine
        The postgres fixture.
    tables : dict
        Table name dict from get_table_names().
    task_id : str
        Stored in the item column.
    done : bool, default False
        Task completion status.
    """
    with postgres.connect() as conn:
        conn.execute(
            text(f'insert into {tables["Inst"]} (item, done) values (:task_id, :done)'),
            {'task_id': task_id, 'done': done})
        conn.commit()


def insert_leader_lock(
    postgres: Engine,
    tables: dict,
    node: str,
    operation: str,
    acquired_at: datetime.datetime = None
) -> None:
    """Insert the singleton LeaderLock row.

    Parameters
    ----------
    postgres : Engine
        The postgres fixture.
    tables : dict
        Table name dict from get_table_names().
    node : str
        Node holding the lock.
    operation : str
        Operation description.
    acquired_at : datetime.datetime, default None
        Acquisition time. None means now.
    """
    if acquired_at is None:
        acquired_at = datetime.datetime.now(datetime.timezone.utc)

    sql = f"""
insert into {tables["LeaderLock"]} (singleton, node, acquired_at, operation)
values (1, :node, :acquired_at, :operation)
"""
    with postgres.connect() as conn:
        conn.execute(text(sql), {
            'node': node,
            'acquired_at': acquired_at,
            'operation': operation,
            })
        conn.commit()


def insert_token(
    postgres: Engine,
    tables: dict,
    token_id: int,
    node: str,
    assigned_at: datetime.datetime = None,
    version: int = 1
) -> None:
    """Insert a Token row assigning token_id to node.

    Parameters
    ----------
    postgres : Engine
        The postgres fixture.
    tables : dict
        Table name dict from get_table_names().
    token_id : int
        Token to assign.
    node : str
        Node that owns the token.
    assigned_at : datetime.datetime, default None
        Assignment time. None means now.
    version : int, default 1
        Token version.
    """
    if assigned_at is None:
        assigned_at = datetime.datetime.now(datetime.timezone.utc)

    sql = f"""
insert into {tables["Token"]} (token_id, node, assigned_at, version)
values (:token_id, :node, :assigned_at, :version)
"""
    with postgres.connect() as conn:
        conn.execute(text(sql), {
            'token_id': token_id,
            'node': node,
            'assigned_at': assigned_at,
            'version': version,
            })
        conn.commit()


def get_token_assignments(
    postgres: Engine,
    tables: dict
) -> dict[int, str]:
    """Owner of every assigned token, read from the Token table.

    Parameters
    ----------
    postgres : Engine
        The postgres fixture.
    tables : dict
        Table name dict from get_table_names().

    Returns
    -------
    dict[int, str]
        Node name keyed by token_id.
    """
    with postgres.connect() as conn:
        result = conn.execute(
            text(f'select token_id, node from {tables["Token"]}'))
        return {row[0]: row[1] for row in result}


def get_rebalance_count(postgres: Engine, tables: dict, trigger_reason: str) -> int:
    """Number of distributions logged with trigger_reason, at any age.

    Parameters
    ----------
    postgres : Engine
        The postgres fixture.
    tables : dict
        Table name dict from get_table_names().
    trigger_reason : str
        Exact reason, e.g. 'initial_distribution', 'dead_nodes',
        'membership_change'.

    Returns
    -------
    int
        Matching rows in the Rebalance table.
    """
    with postgres.connect() as conn:
        result = conn.execute(
            text(f'select count(*) from {tables["Rebalance"]} where trigger_reason = :reason'),
            {'reason': trigger_reason})
        return result.scalar()


# --- Pattern matchers ---

def exact_match(node: str, pattern: str) -> bool:
    """True when node equals pattern, with no wildcards.
    """
    return node == pattern


def wildcard_match(node: str, pattern: str) -> bool:
    """Approximate SQL LIKE with one leading or trailing % wildcard.

    Parameters
    ----------
    node : str
        Node name to test.
    pattern : str
        Exact name, 'prefix%' or '%suffix'. With % at both ends, the
        leading one is literal. A % anywhere else matches only the
        identical string.

    Returns
    -------
    bool
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
