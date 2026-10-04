"""Job synchronization manager with state machine-based coordination.
"""
import contextlib
import datetime
import functools
import hashlib
import inspect
import json
import logging
import re
import threading
import time
from collections import deque
from collections.abc import Hashable, Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait as futures_wait
from copy import deepcopy
from dataclasses import dataclass
from enum import Enum
from functools import total_ordering
from types import TracebackType
from typing import Any, Self

from sqlalchemy import Connection, CursorResult, create_engine, text

from jobsync.schema import ensure_database_ready, get_table_names

logger = logging.getLogger(__name__)

__all__ = ['Job', 'Task', 'LockNotAcquired', 'CoordinationConfig', 'RebalanceEvent']


# --- Event system ---

@dataclass
class CoordinationEvent:
    """Simple event for monitor-to-job communication.
    """
    type: str
    data: dict
    timestamp: float = None

    def __post_init__(self) -> None:
        if self.timestamp is None:
            self.timestamp = time.time()


@dataclass
class RebalanceEvent:
    """Token-ownership change delivered to an on_rebalance callback.

    Attributes
    ----------
    is_initial : bool
        True for this node's first token assignment.
    token_version : int
        Distribution version after the change.
    tokens_added : int
        Count of tokens gained.
    tokens_removed : int
        Count of tokens lost. Always 0 when is_initial is True.
    """
    is_initial: bool
    token_version: int
    tokens_added: int
    tokens_removed: int


class EventQueue:
    """Thread-safe event queue for monitors to publish to Job.
    """

    def __init__(self, history_size: int = 100) -> None:
        """Empty queue with a bounded history.

        Parameters
        ----------
        history_size : int, default 100
            Most events kept in history. The oldest drop first.
        """
        self._events = []
        self._history = deque(maxlen=history_size)
        self._lock = threading.Lock()

    def publish(self, event_type: str, data: dict = None) -> None:
        """Append an event for the next consume_all.

        Parameters
        ----------
        event_type : str
            Type the Job dispatches on, such as 'node_unhealthy'.
        data : dict, optional
            Event payload. None becomes an empty dict.
        """
        with self._lock:
            self._events.append(CoordinationEvent(event_type, data or {}))

    def consume_all(self) -> list[CoordinationEvent]:
        """Drain the queue, moving its events into history.

        Returns
        -------
        list[CoordinationEvent]
            Pending events, oldest first. Empty when none are pending.
        """
        with self._lock:
            events = self._events[:]
            self._history.extend(events)
            self._events.clear()
            return events

    def get_history(self, limit: int = None) -> list[CoordinationEvent]:
        """Consumed and pending events, oldest first.

        Parameters
        ----------
        limit : int, optional
            Keep only the most recent limit events. None keeps all. Zero
            or a negative value returns an empty list.

        Returns
        -------
        list[CoordinationEvent]
            History followed by pending events, oldest first.
        """
        with self._lock:
            all_events = list(self._history) + self._events
            if limit is not None:
                return all_events[-limit:] if limit > 0 else []
            return all_events[:]


# --- Utility functions ---

@dataclass
class CoordinationConfig:
    """Coordination and database settings for a Job.

    Attributes
    ----------
    total_tokens : int, default 10000
        Tokens that tasks hash onto. Every node must use the same value.
    minimum_nodes : int, default 1
        Live nodes required when the enter wait ends. Fewer makes
        Job.__enter__ raise TimeoutError.
    heartbeat_interval_sec : int, default 5
        Seconds between heartbeats.
    heartbeat_timeout_sec : int, default 15
        Seconds without a heartbeat before a node counts as dead.
    rebalance_check_interval_sec : int, default 30
        Seconds between the leader's live node counts.
    dead_node_check_interval_sec : int, default 10
        Seconds between the leader's dead node sweeps.
    token_refresh_initial_interval_sec : int, default 5
        Seconds between token polls during the first 300 seconds after
        start or after the last token change.
    token_refresh_steady_interval_sec : int, default 30
        Seconds between token polls after those 300 seconds.
    token_distribution_timeout_sec : int, default 60
        Seconds wait_for_distribution waits by default. Job.__enter__
        waits twice this.
    leader_lock_timeout_sec : int, default 30
        Seconds to keep retrying the leader lock.
    health_check_interval_sec : int, default 30
        Seconds between health checks.
    stale_leader_lock_age_sec : int, default 300
        Seconds a leader lock may be held before a waiter force-releases
        it.
    stale_rebalance_lock_age_sec : int, default 300
        Seconds a rebalance lock may be held before a waiter
        force-releases it.
    hash_function : str, default 'double_sha256'
        'md5', 'sha256' or 'double_sha256'. Every node must use the same
        value.
    host : str, default 'localhost'
        Database host.
    port : int, default 5432
        Database port.
    dbname : str, default 'jobsync'
        Database name.
    user : str, default 'postgres'
        Login role.
    password : str, default 'postgres'
        Login password.
    appname : str, default 'sync_'
        Prefix of every table name.
    """
    total_tokens: int = 10000
    minimum_nodes: int = 1
    heartbeat_interval_sec: int = 5
    heartbeat_timeout_sec: int = 15
    rebalance_check_interval_sec: int = 30
    dead_node_check_interval_sec: int = 10
    token_refresh_initial_interval_sec: int = 5
    token_refresh_steady_interval_sec: int = 30
    token_distribution_timeout_sec: int = 60
    leader_lock_timeout_sec: int = 30
    health_check_interval_sec: int = 30
    stale_leader_lock_age_sec: int = 300
    stale_rebalance_lock_age_sec: int = 300
    hash_function: str = 'double_sha256'

    host: str = 'localhost'
    port: int = 5432
    dbname: str = 'jobsync'
    user: str = 'postgres'
    password: str = 'postgres'
    appname: str = 'sync_'


def build_connection_string(
    host: str,
    port: int,
    dbname: str,
    user: str,
    password: str
) -> str:
    """SQLAlchemy URL for PostgreSQL through the psycopg driver.

    Parameters
    ----------
    host : str
        Database host.
    port : int
        Database port.
    dbname : str
        Database name.
    user : str
        Login role.
    password : str
        Login password. Inserted without URL escaping, so a password
        holding '@', ':' or '/' yields a broken URL.

    Returns
    -------
    str
        'postgresql+psycopg://user:password@host:port/dbname'.
    """
    return (
        f'postgresql+psycopg://{user}:{password}'
        f'@{host}:{port}/{dbname}'
    )


def retry_with_backoff(
    max_attempts: int = 5,
    base_delay: float = 1.0,
    operation_name: str = None
) -> callable:
    """Decorator that retries a call on any exception, with backoff.

    Parameters
    ----------
    max_attempts : int, default 5
        Total calls, the first included. The last failure is re-raised.
    base_delay : float, default 1.0
        Seconds before the first retry. Doubles on each retry.
    operation_name : str, optional
        Name in the retry warning. None uses the function name.

    Returns
    -------
    callable
        Decorator to apply to the function.
    """
    def decorator(func: callable) -> callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            name = operation_name or func.__name__
            last_exception = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt == max_attempts:
                        raise
                    delay = base_delay * (2 ** (attempt - 1))
                    logger.warning(f'{name} attempt {attempt}/{max_attempts} failed: {e}, retrying in {delay:.1f}s')
                    time.sleep(delay)
            raise last_exception
        return wrapper
    return decorator


def log_duration(operation_name: str = None) -> callable:
    """Decorator that logs a call's wall time at INFO, in milliseconds.

    Parameters
    ----------
    operation_name : str, optional
        Name in the log line. None uses the function name.

    Returns
    -------
    callable
        Decorator to apply to the function.
    """
    def decorator(func: callable) -> callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            name = operation_name or func.__name__
            start = time.time()
            result = func(*args, **kwargs)
            duration_ms = int((time.time() - start) * 1000)
            logger.info(f'{name} completed in {duration_ms}ms')
            return result
        return wrapper
    return decorator


def ensure_timezone_aware(
    dt: datetime.datetime,
    name: str = 'datetime'
) -> datetime.datetime:
    """Return dt after checking that it carries a UTC offset.

    Parameters
    ----------
    dt : datetime.datetime
        Datetime to check.
    name : str, default 'datetime'
        Label for dt in the error message.

    Returns
    -------
    datetime.datetime
        dt, unchanged.

    Raises
    ------
    ValueError
        dt is naive: it has no tzinfo, or its tzinfo gives no offset.
    """
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        raise ValueError(f'{name} must be timezone-aware (has tzinfo), got naive datetime: {dt}')
    return dt


def task_to_token_md5(task_id: Hashable, total_tokens: int) -> int:
    """Token in [0, total_tokens) from the MD5 digest of str(task_id).

    Spreads tasks less evenly than the two SHA-256 variants.
    """
    task_str = str(task_id)
    hash_obj = hashlib.md5(task_str.encode())
    hash_int = int(hash_obj.hexdigest(), 16)
    token_id = hash_int % total_tokens
    return token_id


def task_to_token_sha256(task_id: Hashable, total_tokens: int) -> int:
    """Token in [0, total_tokens) from one SHA-256 pass over str(task_id).

    Clusters sequential task ids more than task_to_token_double_sha256.
    """
    task_str = str(task_id)
    hash_obj = hashlib.sha256(task_str.encode())
    hash_bytes = hash_obj.digest()[:8]
    hash_int = int.from_bytes(hash_bytes, byteorder='big')
    token_id = hash_int % total_tokens
    return token_id


def task_to_token_double_sha256(task_id: Hashable, total_tokens: int) -> int:
    """Token in [0, total_tokens) from two SHA-256 passes over str(task_id).

    Spreads sequential task ids the most evenly of the three hash
    functions, and is the CoordinationConfig default.
    """
    task_str = str(task_id)
    hash_obj = hashlib.sha256(task_str.encode())
    hash_obj = hashlib.sha256(hash_obj.digest())
    hash_bytes = hash_obj.digest()[:8]
    hash_int = int.from_bytes(hash_bytes, byteorder='big')
    token_id = hash_int % total_tokens
    return token_id


HASH_FUNCTIONS = {
    'md5': task_to_token_md5,
    'sha256': task_to_token_sha256,
    'double_sha256': task_to_token_double_sha256,
    }


def task_to_token(
    task_id: Hashable,
    total_tokens: int,
    hash_function: str = 'md5'
) -> int:
    """Token for task_id under the named hash function.

    Parameters
    ----------
    task_id : Hashable
        Task identifier. Hashed through str(), so 1 and '1' share a token.
    total_tokens : int
        Token count.
    hash_function : str, default 'md5'
        'md5', 'sha256' or 'double_sha256'. The default differs from
        CoordinationConfig.hash_function, so pass the Job's setting to
        match its token map.

    Returns
    -------
    int
        Token id in [0, total_tokens).

    Raises
    ------
    ValueError
        hash_function names no known hash function.
    """
    if hash_function not in HASH_FUNCTIONS:
        raise ValueError(f'Unknown hash function: {hash_function}. Valid options: {list(HASH_FUNCTIONS.keys())}')
    return HASH_FUNCTIONS[hash_function](task_id, total_tokens)


def matches_pattern(node_name: str, pattern: str) -> bool:
    """Check if node_name matches SQL LIKE pattern.
    """
    if node_name == pattern:
        return True

    regex_parts = []
    for ch in pattern:
        if ch == '%':
            regex_parts.append('.*')
        elif ch == '_':
            regex_parts.append('.')
        elif ch in r'\.^$*+?{}[]|()':
            regex_parts.append('\\' + ch)
        else:
            regex_parts.append(ch)

    regex_pattern = '^' + ''.join(regex_parts) + '$'
    return re.match(regex_pattern, node_name) is not None


def find_nodes_matching_patterns(
    patterns: str | list[str],
    active_nodes: list[str],
    pattern_matcher: callable
) -> list[str]:
    """Find nodes matching lock patterns, trying fallback patterns in order.
    """
    if isinstance(patterns, str):
        patterns = [patterns]

    for pattern in patterns:
        matches = [n for n in active_nodes if pattern_matcher(n, pattern)]
        if matches:
            return matches

    return []


def categorize_tokens_by_locks(
    total_tokens: int,
    active_nodes: list[str],
    locked_tokens: dict[int, str | list[str]],
    pattern_matcher: callable
) -> tuple[list[int], dict[int, list[str]], list[int]]:
    """Split token ids 0 to total_tokens - 1 by lock state.

    Parameters
    ----------
    total_tokens : int
        Token count.
    active_nodes : list[str]
        Names of live nodes.
    locked_tokens : dict[int, str | list[str]]
        Pattern, or fallback patterns in order, per locked token id.
    pattern_matcher : callable
        (node_name, pattern) -> bool.

    Returns
    -------
    distributable : list[int]
        Unlocked token ids, ascending.
    locked : dict[int, list[str]]
        Eligible nodes per locked token, from the first of its patterns
        that matches a live node.
    blocked : list[int]
        Locked token ids that no pattern matches. They stay unassigned.
    """
    distributable = []
    locked = {}
    blocked = []

    for token_id in range(total_tokens):
        if token_id not in locked_tokens:
            distributable.append(token_id)
        else:
            matching_nodes = find_nodes_matching_patterns(
                locked_tokens[token_id],
                active_nodes,
                pattern_matcher)
            if matching_nodes:
                locked[token_id] = matching_nodes
            else:
                blocked.append(token_id)

    return distributable, locked, blocked


def count_assignment_changes(
    current_assignments: dict[int, str],
    new_assignments: dict[int, str]
) -> int:
    """Count tokens that changed assignment.
    """
    return sum(
        1 for tid, node in new_assignments.items()
        if current_assignments.get(tid) != node)


def compute_minimal_move_distribution(
    total_tokens: int,
    active_nodes: list[str],
    current_assignments: dict[int, str],
    locked_tokens: dict[int, str | list[str]],
    pattern_matcher: callable
) -> tuple[dict[int, str], int]:
    """Token-to-node map that moves the fewest tokens and honors locks.

    Unlocked tokens spread evenly over active_nodes. A locked token goes
    to a node its patterns match. A token no pattern matches stays
    unassigned.

    Parameters
    ----------
    total_tokens : int
        Token count. Token ids run 0 to total_tokens - 1.
    active_nodes : list[str]
        Names of live nodes. Empty returns ({}, 0).
    current_assignments : dict[int, str]
        Present owner per token id. An owner outside active_nodes counts
        as no owner.
    locked_tokens : dict[int, str | list[str]]
        Pattern, or fallback patterns in order, per locked token id.
    pattern_matcher : callable
        (node_name, pattern) -> bool.

    Returns
    -------
    new_assignments : dict[int, str]
        Owner per assigned token id. Unmatched locked tokens are absent.
    moves : int
        Tokens whose owner differs from current_assignments.
    """
    def assign_locked_token(
        token_id: int,
        eligible_nodes: list[str],
        assigned_so_far: dict
    ) -> str:
        """Current owner when eligible, else the least-loaded eligible node.
        """
        current_owner = current_assignments.get(token_id)
        if current_owner and current_owner in eligible_nodes:
            return current_owner

        load_counts = dict.fromkeys(eligible_nodes, 0)
        for node in assigned_so_far.values():
            if node in load_counts:
                load_counts[node] += 1

        return min(eligible_nodes, key=lambda n: (load_counts[n], n))

    def assign_unlocked_token(
        token_id: int,
        receivers: deque,
        node_loads: dict,
        node_targets: dict
    ) -> str:
        """Assign unlocked token minimizing movement, respecting targets.
        """
        current_owner = current_assignments.get(token_id)
        is_current_active = current_owner in node_loads
        is_over_target = (
            is_current_active
            and node_loads[current_owner] > node_targets[current_owner])

        if is_current_active and not (is_over_target and receivers):
            return current_owner

        if receivers:
            receiver, deficit = receivers[0]
            node_loads[receiver] += 1
            receivers[0] = (receiver, deficit - 1)
            if receivers[0][1] <= 0:
                receivers.popleft()

            if is_current_active:
                node_loads[current_owner] -= 1

            return receiver

        return current_owner if is_current_active else active_nodes[0]

    if not active_nodes:
        return {}, 0

    distributable, locked_with_nodes, blocked = categorize_tokens_by_locks(
        total_tokens,
        active_nodes,
        locked_tokens,
        pattern_matcher)

    total_distributable = len(distributable)
    target_per_node = total_distributable // len(active_nodes)
    remainder = total_distributable % len(active_nodes)

    node_targets = {}
    for i, node in enumerate(sorted(active_nodes)):
        target = target_per_node + (1 if i < remainder else 0)
        node_targets[node] = target

    node_loads = dict.fromkeys(active_nodes, 0)
    for token_id in distributable:
        owner = current_assignments.get(token_id)
        if owner in node_loads:
            node_loads[owner] += 1

    receivers = deque()
    for i, (node, current_load) in enumerate(sorted(node_loads.items())):
        target = target_per_node + (1 if i < remainder else 0)
        deficit = target - current_load
        if deficit > 0:
            receivers.append((node, deficit))

    new_assignments = {}
    for token_id, eligible_nodes in locked_with_nodes.items():
        new_assignments[token_id] = assign_locked_token(
            token_id,
            eligible_nodes,
            new_assignments)

    needs_rebalancing = any(
        node_loads[node] > node_targets[node] for node in active_nodes)
    token_order = reversed(distributable) if needs_rebalancing else distributable

    for token_id in token_order:
        new_assignments[token_id] = assign_unlocked_token(
            token_id,
            receivers,
            node_loads,
            node_targets)

    moves = count_assignment_changes(current_assignments, new_assignments)

    return new_assignments, moves


# --- Exceptions ---

class LockNotAcquired(Exception):
    """Raised when a coordination lock cannot be acquired.
    """


# --- Task model ---

@total_ordering
class Task:
    """Unit of work, compared and ordered by id alone.
    """

    def __init__(self, id: Hashable, name: str = None) -> None:
        """Initialize a task with unique identifier and optional name.
        """
        assert isinstance(id, Hashable), f'Id {id} must be hashable'
        self.id = id
        self.name = name

    def __repr__(self) -> str:
        """Return string representation of task.
        """
        return f'id:{self.id},name:{self.name}'

    def __eq__(self, other: object) -> bool:
        """Check equality based on task id.
        """
        if not isinstance(other, Task):
            return NotImplemented
        return self.id == other.id

    def __lt__(self, other: Any) -> bool:
        """Compare tasks based on id for sorting.
        """
        if not isinstance(other, Task):
            return NotImplemented
        return self.id < other.id

    def __gt__(self, other: Any) -> bool:
        """Compare tasks based on id for sorting.
        """
        if not isinstance(other, Task):
            return NotImplemented
        return self.id > other.id


def extract_task_id(task: Task | Hashable) -> Hashable:
    """task.id for a Task, else task itself.
    """
    return task.id if isinstance(task, Task) else task


# --- Pure state machine ---

class JobState(Enum):
    """Job lifecycle states.
    """
    INITIALIZING = 'initializing'
    CLUSTER_FORMING = 'cluster_forming'
    ELECTING = 'electing'
    DISTRIBUTING = 'distributing'
    RUNNING_FOLLOWER = 'running_follower'
    RUNNING_LEADER = 'running_leader'
    ERROR = 'error'
    SHUTTING_DOWN = 'shutting_down'


class JobStateMachine:
    """State machine for managing job lifecycle transitions.
    """

    def __init__(self) -> None:
        """Initialize state machine with transition graph.
        """
        self.state = JobState.INITIALIZING
        self._on_enter_callbacks = {}
        self._on_exit_callbacks = {}
        self._valid_transitions = {}
        self._lock = threading.RLock()
        self._setup_transition_graph()

    def _setup_transition_graph(self) -> None:
        """Define valid state transitions.
        """
        self._add_transition(JobState.INITIALIZING, JobState.CLUSTER_FORMING)
        self._add_transition(JobState.INITIALIZING, JobState.ERROR)
        self._add_transition(JobState.CLUSTER_FORMING, JobState.ELECTING)
        self._add_transition(JobState.CLUSTER_FORMING, JobState.ERROR)
        self._add_transition(JobState.ELECTING, JobState.DISTRIBUTING)
        self._add_transition(JobState.ELECTING, JobState.ERROR)
        self._add_transition(JobState.DISTRIBUTING, JobState.RUNNING_LEADER)
        self._add_transition(JobState.DISTRIBUTING, JobState.RUNNING_FOLLOWER)
        self._add_transition(JobState.DISTRIBUTING, JobState.ERROR)
        self._add_transition(JobState.RUNNING_FOLLOWER, JobState.RUNNING_LEADER)
        self._add_transition(JobState.RUNNING_LEADER, JobState.RUNNING_FOLLOWER)
        self._add_transition(JobState.RUNNING_LEADER, JobState.SHUTTING_DOWN)
        self._add_transition(JobState.RUNNING_FOLLOWER, JobState.SHUTTING_DOWN)
        self._add_transition(JobState.ERROR, JobState.SHUTTING_DOWN)
        self._add_transition(JobState.INITIALIZING, JobState.SHUTTING_DOWN)
        self._add_transition(JobState.CLUSTER_FORMING, JobState.SHUTTING_DOWN)
        self._add_transition(JobState.ELECTING, JobState.SHUTTING_DOWN)
        self._add_transition(JobState.DISTRIBUTING, JobState.SHUTTING_DOWN)

    def _add_transition(self, from_state: JobState, to_state: JobState) -> None:
        """Allow the move from from_state to to_state.
        """
        if from_state not in self._valid_transitions:
            self._valid_transitions[from_state] = set()
        self._valid_transitions[from_state].add(to_state)

    def on_enter(self, state: JobState, callback: callable) -> None:
        """Run callback each time the machine enters state.

        Parameters
        ----------
        state : JobState
            State to watch.
        callback : callable
            Zero-argument callable. A second registration for the same
            state replaces the first.
        """
        self._on_enter_callbacks[state] = callback

    def on_exit(self, state: JobState, callback: callable) -> None:
        """Run callback each time the machine leaves state.

        Parameters
        ----------
        state : JobState
            State to watch.
        callback : callable
            Zero-argument callable. A second registration for the same
            state replaces the first.
        """
        self._on_exit_callbacks[state] = callback

    def transition_to(self, new_state: JobState) -> bool:
        """Move to new_state when the transition graph allows it.

        The exit and enter callbacks run under a reentrant lock, so a
        callback may call transition_to itself.

        Parameters
        ----------
        new_state : JobState
            Target state. The current state counts as a valid target and
            runs no callback.

        Returns
        -------
        bool
            True when the move ran or new_state is the current state.
            False, with an error log, for a move the graph forbids.
        """
        with self._lock:
            if self.state == new_state:
                return True

            valid_next_states = self._valid_transitions.get(self.state, set())
            if new_state not in valid_next_states:
                logger.error(f'Invalid transition: {self.state.value} -> {new_state.value}')
                return False

            logger.info(f'State transition: {self.state.value} -> {new_state.value}')

            if self.state in self._on_exit_callbacks:
                self._on_exit_callbacks[self.state]()

            self.state = new_state

            if new_state in self._on_enter_callbacks:
                self._on_enter_callbacks[new_state]()

            return True

    def is_leader(self) -> bool:
        """True in RUNNING_LEADER.
        """
        return self.state == JobState.RUNNING_LEADER

    def is_follower(self) -> bool:
        """True in RUNNING_FOLLOWER.
        """
        return self.state == JobState.RUNNING_FOLLOWER

    def is_running(self) -> bool:
        """True in RUNNING_LEADER or RUNNING_FOLLOWER.
        """
        return self.state in {JobState.RUNNING_LEADER, JobState.RUNNING_FOLLOWER}

    def is_initializing(self) -> bool:
        """True in INITIALIZING, CLUSTER_FORMING, ELECTING or DISTRIBUTING.
        """
        return self.state in {
            JobState.INITIALIZING,
            JobState.CLUSTER_FORMING,
            JobState.ELECTING,
            JobState.DISTRIBUTING,
            }

    def is_error(self) -> bool:
        """True in ERROR.
        """
        return self.state == JobState.ERROR

    def can_claim_task(self) -> bool:
        """True in RUNNING_LEADER or RUNNING_FOLLOWER, the claiming states.
        """
        return self.state in {JobState.RUNNING_LEADER, JobState.RUNNING_FOLLOWER}


# --- Service layer ---

class DatabaseContext:
    """Manages database engine, connections, and table names.
    """

    def __init__(self, coord_config: CoordinationConfig) -> None:
        """Build a pooled engine and the table names for one database.

        Parameters
        ----------
        coord_config : CoordinationConfig
            Supplies connection settings and the table name prefix.
        """
        connection_string = build_connection_string(
            coord_config.host,
            coord_config.port,
            coord_config.dbname,
            coord_config.user,
            coord_config.password)
        self.engine = create_engine(
            connection_string,
            pool_pre_ping=True,
            pool_size=10,
            max_overflow=5)
        self.tables = get_table_names(coord_config.appname)

    def execute(self, sql: str, params: dict = None) -> CursorResult:
        """Run one statement on its own connection and commit it.

        Parameters
        ----------
        sql : str
            Statement with :name bind parameters.
        params : dict, optional
            Bind values. None binds nothing.

        Returns
        -------
        CursorResult
            Result of the statement, as Connection.execute returns it.
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            conn.commit()
            return result

    def query(self, sql: str, params: dict = None) -> list:
        """All rows a query returns. The connection never commits.

        Parameters
        ----------
        sql : str
            Query with :name bind parameters.
        params : dict, optional
            Bind values. None binds nothing.

        Returns
        -------
        list
            Row objects, in query order.
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            return list(result)

    def dispose(self) -> None:
        """Dispose of engine resources.
        """
        with contextlib.suppress(Exception):
            self.engine.dispose()


class ClusterCoordinator:
    """Handles node registration, heartbeats, and leader election.
    """

    def __init__(
        self,
        node_name: str,
        db: DatabaseContext,
        coord_config: CoordinationConfig,
        created_on: datetime.datetime
    ) -> None:
        """Bind registration, heartbeat and election to one node.

        Parameters
        ----------
        node_name : str
            This node's name, unique across the cluster.
        db : DatabaseContext
            Shared database context.
        coord_config : CoordinationConfig
            Supplies heartbeat and monitor intervals.
        created_on : datetime.datetime
            Registration time, timezone-aware. The oldest live node leads.
        """
        self.node_name = node_name
        self.db = db
        self.heartbeat_interval = coord_config.heartbeat_interval_sec
        self.heartbeat_timeout = coord_config.heartbeat_timeout_sec
        self.health_check_interval = coord_config.health_check_interval_sec
        self.dead_node_check_interval = coord_config.dead_node_check_interval_sec
        self.rebalance_check_interval = coord_config.rebalance_check_interval_sec
        self.created_on = created_on
        self.last_heartbeat_sent = None
        self._heartbeat_monitor = None
        self._health_monitor = None

    @property
    def active_nodes_sql(self) -> str:
        """SQL query for fetching active nodes.
        """
        return f"""
select name, created_on, last_heartbeat
from {self.db.tables["Node"]}
where last_heartbeat > now() - interval '{self.heartbeat_timeout} seconds'
order by name asc
"""

    @retry_with_backoff()
    def register(self) -> None:
        """Register node with initial heartbeat.
        """
        self.last_heartbeat_sent = datetime.datetime.now(datetime.timezone.utc)
        sql = f"""
insert into {self.db.tables["Node"]} (name, created_on, last_heartbeat)
values (:name, :created_on, :heartbeat)
on conflict (name) do update
set last_heartbeat = excluded.last_heartbeat
"""
        self.db.execute(
            sql,
            {
                'name': self.node_name,
                'created_on': self.created_on,
                'heartbeat': self.last_heartbeat_sent,
                })
        logger.info(f'Node {self.node_name} registered with heartbeat')

    def elect_leader(self) -> str:
        """Name of the live node with the oldest created_on.

        Returns
        -------
        str
            Leader node name. A tie on created_on goes to the lowest name.

        Raises
        ------
        RuntimeError
            No node has sent a heartbeat within heartbeat_timeout.
        """
        sql = f"""
select name, created_on, last_heartbeat
from {self.db.tables["Node"]}
where last_heartbeat > now() - interval '{self.heartbeat_timeout} seconds'
order by created_on asc, name asc
"""
        nodes = self.db.query(sql)

        if not nodes:
            raise RuntimeError('No active nodes for leader election')

        return nodes[0][0]

    def get_active_nodes(self) -> list[dict]:
        """Nodes that sent a heartbeat within heartbeat_timeout.

        Returns
        -------
        list[dict]
            One dict per node with keys 'name', 'created_on' and
            'last_heartbeat', ordered by name.
        """
        result = self.db.query(self.active_nodes_sql)
        return [
            {'name': row[0], 'created_on': row[1], 'last_heartbeat': row[2]}
            for row in result
            ]

    def get_dead_nodes(self) -> list[str]:
        """Names of nodes with no heartbeat within heartbeat_timeout.

        Returns
        -------
        list[str]
            Node names, nodes that never sent a heartbeat included.
        """
        sql = f"""
select name
from {self.db.tables["Node"]}
where last_heartbeat <= now() - interval '{self.heartbeat_timeout} seconds' or last_heartbeat is null
"""
        result = self.db.query(sql)
        return [row[0] for row in result]

    def am_i_healthy(self) -> bool:
        """Whether this node's heartbeat is fresh and the database answers.

        Returns
        -------
        bool
            False when the last heartbeat this node sent is older than
            heartbeat_timeout, or a test query fails. Never raises.
        """
        try:
            if self.last_heartbeat_sent:
                now = datetime.datetime.now(datetime.timezone.utc)
                age_seconds = (now - self.last_heartbeat_sent).total_seconds()
                if age_seconds > self.heartbeat_timeout:
                    return False
            with self.db.engine.connect() as conn:
                conn.execute(text('select 1'))
            return True
        except Exception:
            return False

    def cleanup(self) -> None:
        """Remove node from Node table.
        """
        try:
            sql = f'delete from {self.db.tables["Node"]} where name = :name'
            self.db.execute(sql, {'name': self.node_name})
            logger.debug(f'Cleared {self.node_name} from {self.db.tables["Node"]}')
        except Exception as e:
            logger.debug(f'Failed to cleanup node {self.node_name}: {e}')


class TokenDistributor:
    """Handles token distribution and assignment tracking.
    """

    def __init__(
        self,
        node_name: str,
        db: DatabaseContext,
        coord_config: CoordinationConfig
    ) -> None:
        """Track one node's tokens, and distribute tokens as leader.

        Parameters
        ----------
        node_name : str
            This node's name.
        db : DatabaseContext
            Shared database context.
        coord_config : CoordinationConfig
            Supplies token count, refresh intervals and timeouts.
        """
        self.node_name = node_name
        self.db = db
        self.total_tokens = coord_config.total_tokens
        self.minimum_nodes = coord_config.minimum_nodes
        self.token_refresh_initial = coord_config.token_refresh_initial_interval_sec
        self.token_refresh_steady = coord_config.token_refresh_steady_interval_sec
        self.token_distribution_timeout = coord_config.token_distribution_timeout_sec
        self.heartbeat_timeout = coord_config.heartbeat_timeout_sec
        self.my_tokens = set()
        self.token_version = 0
        self.on_rebalance = None
        self._callback_executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix='rebalance-callback')
        self._pending_callbacks = []
        self._executor_shutdown = False

    def distribute(
        self,
        lock_manager: 'LockManager',
        cluster: ClusterCoordinator,
        trigger_reason: str = 'distribution'
    ) -> None:
        """Rewrite the token table with a minimal-move distribution.

        With no live node it logs an error and leaves the table as is.

        Parameters
        ----------
        lock_manager : LockManager
            Source of the active task locks.
        cluster : ClusterCoordinator
            Source of the live node list.
        trigger_reason : str, default 'distribution'
            Cause, such as 'initial_distribution', 'dead_nodes' or
            'membership_change'. Stored in the rebalance audit row.

        Raises
        ------
        ValueError
            The computed map holds more than total_tokens entries, or
            names a node that is not live.
        """
        start_time = time.time()

        active_nodes = cluster.get_active_nodes()

        active_node_names = [n['name'] for n in active_nodes]
        nodes_count = len(active_node_names)

        logger.info(f'Token distribution starting: {nodes_count} active nodes')

        if nodes_count == 0:
            logger.error('No active nodes for token distribution')
            return

        with self.db.engine.connect() as conn:
            sql = f'select token_id, node from {self.db.tables["Token"]}'
            result = conn.execute(text(sql))
            current_assignments = {row[0]: row[1] for row in result}

        logger.debug(
            f'Current assignments: {len(current_assignments)} tokens across '
            f'{len(set(current_assignments.values()))} nodes')

        locked_tokens = lock_manager.get_active_locks()

        for token_id, patterns in locked_tokens.items():
            matching_found = False
            for pattern in patterns:
                matching_nodes = [
                    n for n in active_node_names if matches_pattern(n, pattern)
                    ]
                if matching_nodes:
                    matching_found = True
                    logger.debug(
                        f'Token {token_id} locked to pattern "{pattern}" '
                        f'(matches {len(matching_nodes)} nodes)')
                    break

            if not matching_found:
                logger.warning(
                    f'Token {token_id} lock failed: patterns {patterns} '
                    f'matched no active nodes {active_node_names}')

        new_assignments, tokens_moved = compute_minimal_move_distribution(
            self.total_tokens,
            active_node_names,
            current_assignments,
            locked_tokens,
            matches_pattern)

        logger.debug(
            f'Computed new assignments: {len(new_assignments)} tokens across '
            f'{len(set(new_assignments.values()))} nodes, {tokens_moved} moves')

        if len(new_assignments) > self.total_tokens:
            logger.error(
                f'Token distribution produced {len(new_assignments)} assignments '
                f'but total_tokens={self.total_tokens}')
            raise ValueError(f'Invalid token distribution: {len(new_assignments)} > {self.total_tokens}')

        assigned_nodes = set(new_assignments.values())
        invalid_nodes = assigned_nodes - set(active_node_names)
        if invalid_nodes:
            logger.error(f'Token distribution assigned to non-active nodes: {invalid_nodes}')
            raise ValueError(f'Tokens assigned to inactive nodes: {invalid_nodes}')

        missing_tokens = set(range(self.total_tokens)) - set(new_assignments)
        if missing_tokens:
            logger.error(
                'Token distribution incomplete: '
                f'{len(missing_tokens)}/{self.total_tokens} tokens unowned '
                f'(e.g. {sorted(missing_tokens)[:10]}) - tasks hashing to them '
                'are unprocessed until a matching node joins')

        with self.db.engine.connect() as conn:
            result = conn.execute(text(f'select max(version) from {self.db.tables["Token"]}'))
            current_version = result.scalar() or 0
            new_version = current_version + 1

            logger.debug(
                'Applying token distribution atomically '
                '(DELETE+INSERT within transaction, brief gap acceptable)')
            conn.execute(text(f'delete from {self.db.tables["Token"]}'))

            if new_assignments:
                insert_values = [
                    {'token_id': tid, 'node': node, 'version': new_version}
                    for tid, node in new_assignments.items()
                    ]
                sql = f"""
insert into {self.db.tables["Token"]} (token_id, node, assigned_at, version)
values (:token_id, :node, now(), :version)
"""
                conn.execute(text(sql), insert_values)

            conn.commit()

        duration_ms = int((time.time() - start_time) * 1000)
        self._log_rebalance(
            trigger_reason,
            len(active_node_names),
            len(active_node_names),
            tokens_moved,
            duration_ms)

        logger.info(
            f'Token distribution complete: {len(new_assignments)} tokens across '
            f'{nodes_count} nodes, {tokens_moved} moved, v{new_version}, '
            f'{duration_ms}ms')

    def get_my_tokens_versioned(self) -> tuple[set[int], int]:
        """Token ids this node owns, with their distribution version.

        Returns
        -------
        tokens : set[int]
            Owned token ids. Empty when the node owns none.
        version : int
            Distribution version. 0 when the node owns no token.
        """
        sql = f'select token_id, version from {self.db.tables["Token"]} where node = :node'
        records = self.db.query(sql, {'node': self.node_name})

        if not records:
            return set(), 0

        tokens = {row[0] for row in records}
        version = records[0][1] if records else 0
        logger.debug(f'Node {self.node_name} owns {len(tokens)} tokens, version {version}')
        return tokens, version

    @log_duration('wait_for_distribution')
    def wait_for_distribution(
        self,
        timeout_sec: int = None,
        check_interval: float = 0.5
    ) -> None:
        """Block until the token table assigns this node at least one token.

        Parameters
        ----------
        timeout_sec : int, optional
            Seconds to wait. None uses token_distribution_timeout.
        check_interval : float, default 0.5
            Seconds between polls.

        Raises
        ------
        TimeoutError
            No token arrives within timeout_sec.
        """
        if timeout_sec is None:
            timeout_sec = self.token_distribution_timeout

        start = time.time()

        while time.time() - start < timeout_sec:
            sql = f"""
select count(*)
from {self.db.tables["Token"]}
where node = :node
"""
            result = self.db.query(sql, {'node': self.node_name})
            my_token_count = result[0][0] if result else 0

            if my_token_count > 0:
                logger.info(f'Token distribution complete: {my_token_count} tokens assigned to {self.node_name}')
                return

            time.sleep(check_interval)

        raise TimeoutError(f'Token distribution did not complete for {self.node_name} within {timeout_sec}s')

    def invoke_callback(self, callback: callable, event: RebalanceEvent) -> None:
        """Queue callback on the single-worker callback pool.

        After shutdown_callbacks the callback is skipped with a warning.

        Parameters
        ----------
        callback : callable
            Called with event when it takes a positional argument, else
            with no argument. An exception it raises is logged, never
            raised.
        event : RebalanceEvent
            Token-ownership change to report.
        """
        if self._executor_shutdown:
            logger.warning('on_rebalance not invoked - executor already shutdown')
            return

        try:
            params = inspect.signature(callback).parameters.values()
            pass_event = any(
                p.kind in {p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD, p.VAR_POSITIONAL}
                for p in params)
        except (ValueError, TypeError):
            pass_event = False

        def _run_callback() -> None:
            try:
                start = time.time()
                if pass_event:
                    callback(event)
                else:
                    callback()
                duration_ms = int((time.time() - start) * 1000)
                logger.info(
                    f'on_rebalance completed in {duration_ms}ms '
                    f'(initial={event.is_initial}, +{event.tokens_added} '
                    f'-{event.tokens_removed}, v{event.token_version})')
            except Exception as e:
                logger.error(f'on_rebalance callback failed: {e}')

        future = self._callback_executor.submit(_run_callback)
        self._pending_callbacks.append(future)

    def shutdown_callbacks(self, wait: bool = True, timeout: int = 10) -> None:
        """Stop the callback pool and refuse new callbacks.

        Parameters
        ----------
        wait : bool, default True
            Wait for pending callbacks before the pool shuts down.
        timeout : int, default 10
            Seconds to wait for pending callbacks. When wait is True, the
            pool shutdown that follows still waits for a running callback
            without a limit.
        """
        self._executor_shutdown = True

        pending_count = len(self._pending_callbacks)
        if pending_count > 0:
            logger.info(f'Waiting for {pending_count} pending callbacks to complete (timeout: {timeout}s)...')

        if wait and self._pending_callbacks:
            futures_wait(self._pending_callbacks, timeout=timeout)

            completed = sum(1 for f in self._pending_callbacks if f.done())
            logger.debug(f'Callback shutdown complete: {completed}/{pending_count} callbacks completed')

        self._callback_executor.shutdown(wait=wait)
        self._pending_callbacks.clear()

    def _log_rebalance(
        self,
        reason: str,
        nodes_before: int,
        nodes_after: int,
        tokens_moved: int,
        duration_ms: int
    ) -> None:
        """Insert one row into the rebalance audit table.

        Parameters
        ----------
        reason : str
            Trigger reason.
        nodes_before : int
            Live node count before the rebalance.
        nodes_after : int
            Live node count after the rebalance.
        tokens_moved : int
            Tokens whose owner changed.
        duration_ms : int
            Rebalance wall time, in milliseconds.
        """
        sql = f"""
insert into {self.db.tables["Rebalance"]}
(triggered_at, trigger_reason, leader_node, nodes_before, nodes_after, tokens_moved, duration_ms)
values (now(), :reason, :leader, :before, :after, :moved, :duration)
"""
        self.db.execute(
            sql,
            {
                'reason': reason,
                'leader': self.node_name,
                'before': nodes_before,
                'after': nodes_after,
                'moved': tokens_moved,
                'duration': duration_ms,
                })


class LockManager:
    """Handles task locking and coordination locks.
    """

    def __init__(
        self,
        node_name: str,
        db: DatabaseContext,
        coord_config: CoordinationConfig
    ) -> None:
        """Bind task locks and coordination locks to one node.

        Parameters
        ----------
        node_name : str
            This node's name, stored as the creator of its locks.
        db : DatabaseContext
            Shared database context.
        coord_config : CoordinationConfig
            Supplies token count, hash function and lock timeouts.
        """
        self.node_name = node_name
        self.db = db
        self.total_tokens = coord_config.total_tokens
        self.hash_function = coord_config.hash_function
        self.leader_lock_timeout = coord_config.leader_lock_timeout_sec
        self.stale_leader_lock_age = coord_config.stale_leader_lock_age_sec
        self.stale_rebalance_lock_age = coord_config.stale_rebalance_lock_age_sec

    def _lock_upsert_sql(self) -> str:
        """Build the upsert SQL statement for the Lock table.
        """
        return f"""
insert into {self.db.tables["Lock"]}
(task_id, node_patterns, reason, created_at, created_by, expires_at)
values (:task_id, :patterns, :reason, now(), :created_by, :expires_at)
on conflict (task_id) do update
set node_patterns = excluded.node_patterns,
    reason = excluded.reason,
    created_at = excluded.created_at,
    created_by = excluded.created_by,
    expires_at = excluded.expires_at
"""

    def _build_lock_row(
        self,
        task_id: Hashable,
        node_patterns: str | list[str],
        reason: str,
        expires_at: datetime.datetime | None
    ) -> dict:
        """Build a parameter row for the Lock upsert SQL.
        """
        if isinstance(node_patterns, str):
            node_patterns = [node_patterns]
        return {
            'task_id': str(task_id),
            'patterns': json.dumps(node_patterns),
            'reason': reason,
            'created_by': self.node_name,
            'expires_at': expires_at,
            }

    def register_lock(
        self,
        task_id: Hashable,
        node_patterns: str | list[str],
        reason: str = None,
        expires_in_days: int = None
    ) -> None:
        """Pin a task to nodes matching a pattern, replacing its old lock.

        Parameters
        ----------
        task_id : Hashable
            Task to pin, never a token id. Stored as str(task_id), and
            hashed to a token at each distribution.
        node_patterns : str | list[str]
            SQL LIKE pattern, or fallback patterns tried in order.
        reason : str, optional
            Free text stored with the lock.
        expires_in_days : int, optional
            Days until the lock expires. None or 0 never expires.
        """
        expires_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
            days=expires_in_days) if expires_in_days else None

        row = self._build_lock_row(task_id, node_patterns, reason, expires_at)
        self.db.execute(self._lock_upsert_sql(), row)

        logger.debug(f'Registered lock: task {task_id} -> patterns {row["patterns"]}')

    @log_duration('register_locks_bulk')
    def register_locks_bulk(
        self,
        locks: list[tuple[Hashable, str | list[str], str]]
    ) -> None:
        """Pin many tasks in one transaction, as register_lock does.

        Parameters
        ----------
        locks : list[tuple[Hashable, str | list[str], str]]
            (task_id, node_patterns, reason) per lock. These locks never
            expire. An empty list does nothing.
        """
        if not locks:
            return

        rows = [
            self._build_lock_row(task_id, node_patterns, reason, None)
            for task_id, node_patterns, reason in locks
            ]

        sql = self._lock_upsert_sql()
        with self.db.engine.connect() as conn:
            for row in rows:
                conn.execute(text(sql), row)
            conn.commit()

        logger.debug(f'Registered {len(locks)} locks')

    def clear_locks_by_creator(self, creator: str) -> int:
        """Delete every lock one node created.

        Parameters
        ----------
        creator : str
            Node name stored as the lock's creator.

        Returns
        -------
        int
            Locks deleted.
        """
        sql = f'delete from {self.db.tables["Lock"]} where created_by = :creator'
        result = self.db.execute(sql, {'creator': creator})
        rows = result.rowcount
        logger.info(f'Cleared {rows} locks created by {creator}')
        return rows

    def clear_all_locks(self) -> int:
        """Delete every lock, whatever its creator.

        Returns
        -------
        int
            Locks deleted.
        """
        sql = f'delete from {self.db.tables["Lock"]}'
        result = self.db.execute(sql)
        rows = result.rowcount
        logger.warning(f'Cleared ALL {rows} locks from system')
        return rows

    def list_locks(self) -> list[dict]:
        """Unexpired locks, newest first.

        Returns
        -------
        list[dict]
            One dict per lock with keys task_id, node_patterns, reason,
            created_at, created_by and expires_at.
        """
        sql = f"""
select task_id, node_patterns, reason, created_at, created_by, expires_at
from {self.db.tables["Lock"]}
where expires_at is null or expires_at > now()
order by created_at desc
"""
        result = self.db.query(sql)
        return [dict(row._mapping) for row in result]

    def get_active_locks(self) -> dict[int, list[str]]:
        """Fallback patterns per locked token, after purging expired locks.

        Each call hashes the stored task ids with the current total_tokens,
        so a lock stays valid when total_tokens changes between runs.

        Returns
        -------
        dict[int, list[str]]
            Ordered patterns per token id. A lock whose patterns fail to
            parse is skipped with a warning.
        """
        with self.db.engine.connect() as conn:
            sql = f"""
delete from {self.db.tables["Lock"]}
where expires_at is not null and expires_at < now()
"""
            conn.execute(text(sql))

            sql = f'select task_id, node_patterns from {self.db.tables["Lock"]}'
            result = conn.execute(text(sql))
            records = [dict(row._mapping) for row in result]

            locked_tokens = {}
            for row in records:
                try:
                    patterns = row['node_patterns']
                    if isinstance(patterns, str):
                        patterns = json.loads(patterns)
                    if not isinstance(patterns, list):
                        logger.warning(
                            f'Invalid lock pattern for task {row["task_id"]}: '
                            f'expected list, got {type(patterns).__name__}')
                        continue
                    token_id = task_to_token(
                        row['task_id'],
                        self.total_tokens,
                        self.hash_function)
                    locked_tokens[token_id] = patterns
                except (json.JSONDecodeError, TypeError, KeyError) as e:
                    logger.warning(f'Failed to parse lock pattern for task {row["task_id"]}: {e}')
                    continue

            conn.commit()
            return locked_tokens

    @contextlib.contextmanager
    def acquire_leader_lock(self, operation: str) -> Iterator[None]:
        """Hold the cluster-wide leader lock for the body of a with block.

        Parameters
        ----------
        operation : str
            Label stored with the lock and used in log lines.

        Raises
        ------
        LockNotAcquired
            The lock stayed out of reach for leader_lock_timeout seconds.
        """
        if not self._try_acquire_leader_lock(operation):
            raise LockNotAcquired(f'Another leader is performing {operation}')
        try:
            yield
        finally:
            self._release_leader_lock()

    @contextlib.contextmanager
    def acquire_rebalance_lock(self, started_by: str) -> Iterator[None]:
        """Hold the cluster-wide rebalance lock for the body of a with block.

        Parameters
        ----------
        started_by : str
            Component name stored as the holder.

        Raises
        ------
        LockNotAcquired
            Another holder has the lock. There is no retry.
        """
        if not self._try_acquire_rebalance_lock(started_by):
            raise LockNotAcquired('Rebalance already in progress')
        try:
            yield
        finally:
            self._release_rebalance_lock()

    def _try_acquire_leader_lock(self, operation: str) -> bool:
        """Insert the leader lock row, retrying until leader_lock_timeout.

        Parameters
        ----------
        operation : str
            Label stored with the lock.

        Returns
        -------
        bool
            True once acquired. False after the timeout.
        """
        start_time = time.time()
        while time.time() - start_time < self.leader_lock_timeout:
            try:
                sql = f"""
insert into {self.db.tables["LeaderLock"]} (singleton, node, acquired_at, operation)
values (1, :node, :acquired_at, :operation)
on conflict (singleton) do nothing
"""
                result = self.db.execute(
                    sql,
                    {
                        'node': self.node_name,
                        'acquired_at': datetime.datetime.now(datetime.timezone.utc),
                        'operation': operation,
                        })

                if result.rowcount > 0:
                    logger.info(f'Leader lock acquired by {self.node_name} for {operation}')
                    return True

                with self.db.engine.connect() as conn:
                    if self._check_and_clear_stale_lock(
                        conn,
                        self.db.tables['LeaderLock'],
                        self.stale_leader_lock_age,
                        'leader'):
                        continue

                time.sleep(0.5)

            except Exception as e:
                logger.error(f'Leader lock acquisition failed: {e}')
                time.sleep(0.5)

        logger.warning(f'Leader lock acquisition timeout for {self.node_name}')
        return False

    def _release_leader_lock(self) -> None:
        """Release leader lock.
        """
        try:
            sql = f'delete from {self.db.tables["LeaderLock"]} where node = :node'
            self.db.execute(sql, {'node': self.node_name})
            logger.debug(f'Leader lock released by {self.node_name}')
        except Exception as e:
            logger.error(f'Leader lock release failed: {e}')

    def _try_acquire_rebalance_lock(self, started_by: str) -> bool:
        """Set the rebalance flag once, after clearing a stale holder.

        Parameters
        ----------
        started_by : str
            Component name stored as the holder.

        Returns
        -------
        bool
            True when this call set the flag. False when it is held.
        """
        with self.db.engine.connect() as conn:
            self._check_and_clear_stale_lock(
                conn,
                self.db.tables['RebalanceLock'],
                self.stale_rebalance_lock_age,
                'rebalance')

        sql = f"""
update {self.db.tables["RebalanceLock"]}
set in_progress = true, started_at = :started_at, started_by = :started_by
where singleton = 1 and in_progress = false
"""
        result = self.db.execute(
            sql,
            {
                'started_at': datetime.datetime.now(datetime.timezone.utc),
                'started_by': started_by,
                })

        success = result.rowcount > 0

        if success:
            logger.debug(f'Rebalance lock acquired by {started_by}')
        else:
            logger.debug('Rebalance lock already held')

        return success

    def _release_rebalance_lock(self) -> None:
        """Release rebalance lock.
        """
        sql = f"""
update {self.db.tables["RebalanceLock"]}
set in_progress = false, started_at = null, started_by = null
where singleton = 1
"""
        self.db.execute(sql)
        logger.debug('Rebalance lock released')

    def _check_and_clear_stale_lock(
        self,
        conn: Connection,
        table: str,
        age_threshold: int,
        lock_type: str
    ) -> bool:
        """Force-release a lock held longer than age_threshold.

        Parameters
        ----------
        conn : Connection
            Open connection. Committed when a lock is cleared.
        table : str
            Lock table name.
        age_threshold : int
            Seconds a hold may last before it counts as stale.
        lock_type : str
            'leader' for the leader lock table. Any other value treats
            table as the rebalance lock table.

        Returns
        -------
        bool
            True when a stale lock was cleared.
        """
        if lock_type == 'leader':
            sql = f"""
select node, extract(epoch from (now() - acquired_at)) as age_seconds
from {table}
where singleton = 1
"""
        else:
            sql = f"""
select started_by, extract(epoch from (now() - started_at)) as age_seconds
from {table}
where singleton = 1 and in_progress = true
"""

        result = conn.execute(text(sql))
        lock_row = result.first()

        if lock_row and lock_row[1] is not None and lock_row[1] > age_threshold:
            holder = lock_row[0]
            age_seconds = lock_row[1]
            logger.warning(f'Stale {lock_type} lock detected (age: {age_seconds}s, holder: {holder}), forcing release')

            if lock_type == 'leader':
                conn.execute(text(f'delete from {table} where singleton = 1'))
            else:
                sql = f"""
update {table}
set in_progress = false, started_at = null, started_by = null
where singleton = 1
"""
                conn.execute(text(sql))
            conn.commit()
            return True

        return False


class TaskManager:
    """Handles task claiming and audit logging.
    """

    def __init__(
        self,
        node_name: str,
        db: DatabaseContext,
        date: datetime.date,
        total_tokens: int,
        hash_function: str
    ) -> None:
        """Bind task claiming and auditing to one node and date.

        Parameters
        ----------
        node_name : str
            This node's name.
        db : DatabaseContext
            Shared database context.
        date : datetime.date
            Processing date written to audit rows.
        total_tokens : int
            Token count for task hashing.
        hash_function : str
            Hash function name for task hashing, as task_to_token takes.
        """
        self.node_name = node_name
        self.db = db
        self.date = date
        self.total_tokens = total_tokens
        self.hash_function = hash_function
        self._tasks = []

    def can_claim(self, task: Task | Hashable, my_tokens: set[int]) -> bool:
        """Whether this node owns the token that task hashes to.

        Parameters
        ----------
        task : Task | Hashable
            Task or task id.
        my_tokens : set[int]
            Token ids this node owns.

        Returns
        -------
        bool
            True when the task's token is in my_tokens.
        """
        task_id = extract_task_id(task)
        token_id = task_to_token(task_id, self.total_tokens, self.hash_function)
        can_claim = token_id in my_tokens

        if not can_claim:
            logger.debug(f'Cannot claim task {task_id} (token {token_id} not owned by {self.node_name})')

        return can_claim

    def add_task(self, task: Task | Hashable) -> None:
        """Queue task, stamped with the current UTC time, for write_audit.
        """
        self._tasks.append((task, datetime.datetime.now(datetime.timezone.utc)))

    def set_claim(self, task: Task | Hashable) -> None:
        """Record a claim row for each task, keeping any existing claim.

        Parameters
        ----------
        task : Task | Hashable
            Task or task id, or an iterable of them. A str counts as one
            task id, but any other iterable, a tuple id included, counts
            as a batch.
        """
        sql = f"""
insert into {self.db.tables["Claim"]} (node, task_id, created_on)
values (:node, :task_id, :created_on)
on conflict do nothing
"""
        with self.db.engine.connect() as conn:
            if isinstance(task, Iterable) and not isinstance(task, str):
                rows = [
                    {
                        'node': self.node_name,
                        'created_on': datetime.datetime.now(datetime.timezone.utc),
                        'task_id': str(extract_task_id(i)),
                        }
                    for i in task
                    ]
                for row in rows:
                    conn.execute(text(sql), row)
            else:
                task_id = extract_task_id(task)
                conn.execute(
                    text(sql),
                    {
                        'node': self.node_name,
                        'task_id': str(task_id),
                        'created_on': datetime.datetime.now(datetime.timezone.utc),
                        })
            conn.commit()

    def write_audit(self) -> None:
        """Write accumulated tasks to audit table.
        """
        if not self._tasks:
            return

        with self.db.engine.connect() as conn:
            for task, _ in self._tasks:
                task_id = extract_task_id(task)
                sql = f"""
insert into {self.db.tables["Audit"]} (date, node, task_id, created_on)
values (:date, :node, :task_id, now())
"""
                conn.execute(
                    text(sql),
                    {
                        'date': self.date,
                        'node': self.node_name,
                        'task_id': str(task_id),
                        })
            conn.commit()

        logger.debug(f'Flushed {len(self._tasks)} task to {self.db.tables["Audit"]}')
        self._tasks.clear()

    def get_audit(self) -> list[dict]:
        """Audit rows for this manager's date, across all nodes.

        Returns
        -------
        list[dict]
            One dict per row with keys 'node' and 'task_id'.
        """
        sql = f'select node, task_id from {self.db.tables["Audit"]} where date = :date and task_id is not null'
        result = self.db.query(sql, {'date': self.date})
        return [{'node': row[0], 'task_id': row[1]} for row in result]

    def cleanup(self) -> None:
        """Cleanup Check and Claim tables.
        """
        try:
            with self.db.engine.connect() as conn:
                sql = f'delete from {self.db.tables["Check"]} where node = :node'
                conn.execute(text(sql), {'node': self.node_name})

                sql = f'delete from {self.db.tables["Claim"]} where node = :node'
                conn.execute(text(sql), {'node': self.node_name})

                conn.commit()
            logger.debug(f'Cleaned {self.node_name} from Check and Claim tables')
        except Exception as e:
            logger.debug(f'Failed to cleanup tasks for {self.node_name}: {e}')


# --- Monitors ---

class Monitor:
    """Base class for background monitoring threads.
    """

    def __init__(
        self,
        name: str,
        interval: float,
        shutdown_event: threading.Event
    ) -> None:
        """Configure a monitor whose thread has not started.

        Parameters
        ----------
        name : str
            Thread name and log label.
        interval : float
            Seconds between checks.
        shutdown_event : threading.Event
            Ends the loop once set.
        """
        self.name = name
        self.interval = interval
        self.shutdown_event = shutdown_event
        self.thread = None
        self._stop_requested = False

    def start(self) -> None:
        """Start the monitor thread.
        """
        self.thread = threading.Thread(
            target=self._run,
            daemon=True,
            name=self.name)
        self.thread.start()
        logger.info(f'{self.name} monitor started')

    def stop(self) -> None:
        """Request the monitor to stop its thread.
        """
        self._stop_requested = True

    def _run(self) -> None:
        """Main monitoring loop.
        """
        while not self.shutdown_event.is_set() and not self._stop_requested:
            try:
                self.check()
            except Exception as e:
                logger.error(f'{self.name} monitor error: {e}', exc_info=True)
                time.sleep(1.0)
                continue

            if self.shutdown_event.wait(timeout=self.interval):
                break

    def check(self) -> None:
        """Perform monitoring check - to be implemented by subclasses.
        """
        raise NotImplementedError


class HeartbeatMonitor(Monitor):
    """Sends periodic heartbeats to maintain node registration.
    """

    def __init__(
        self,
        cluster: ClusterCoordinator,
        shutdown_event: threading.Event
    ) -> None:
        """Heartbeat for the cluster's node, at its heartbeat interval.

        Parameters
        ----------
        cluster : ClusterCoordinator
            Supplies the node, database and interval.
        shutdown_event : threading.Event
            Ends the loop once set.
        """
        super().__init__(
            f'heartbeat-{cluster.node_name}',
            cluster.heartbeat_interval,
            shutdown_event)
        self.cluster = cluster
        self.db = cluster.db
        self.node_name = cluster.node_name

    def check(self) -> None:
        """Send heartbeat update.
        """
        sql = f"""
update {self.db.tables["Node"]}
set last_heartbeat = :heartbeat
where name = :name
"""
        heartbeat_time = datetime.datetime.now(datetime.timezone.utc)
        self.db.execute(sql, {'heartbeat': heartbeat_time, 'name': self.node_name})
        self.cluster.last_heartbeat_sent = heartbeat_time
        logger.debug(f'Heartbeat sent by {self.node_name}')


class HealthMonitor(Monitor):
    """Checks one node's heartbeat age and database connectivity.
    """

    def __init__(
        self,
        cluster: ClusterCoordinator,
        event_queue: EventQueue,
        shutdown_event: threading.Event
    ) -> None:
        """Health checks for the cluster's node, at its check interval.

        Parameters
        ----------
        cluster : ClusterCoordinator
            Supplies the node, database, interval and heartbeat timeout.
        event_queue : EventQueue
            Receives 'node_unhealthy'.
        shutdown_event : threading.Event
            Ends the loop once set.
        """
        super().__init__(
            f'health-{cluster.node_name}',
            cluster.health_check_interval,
            shutdown_event)
        self.cluster = cluster
        self.event_queue = event_queue

    def check(self) -> None:
        """Check heartbeat age and database connectivity.
        """
        if self.cluster.last_heartbeat_sent:
            now = datetime.datetime.now(datetime.timezone.utc)
            age_seconds = (now - self.cluster.last_heartbeat_sent).total_seconds()
            if age_seconds > self.cluster.heartbeat_timeout:
                logger.error(
                    f'Heartbeat thread appears dead (age: {age_seconds:.1f}s > '
                    f'timeout: {self.cluster.heartbeat_timeout}s)')
                self.event_queue.publish(
                    'node_unhealthy',
                    {'reason': 'heartbeat_timeout', 'age_seconds': age_seconds})
                return

        with self.cluster.db.engine.connect() as conn:
            conn.execute(text('select 1'))
            conn.rollback()


class TokenRefreshMonitor(Monitor):
    """Detects token reassignments and invokes callbacks for followers.
    """

    def __init__(
        self,
        node_name: str,
        db: DatabaseContext,
        tokens: TokenDistributor,
        state_machine: JobStateMachine,
        event_queue: EventQueue,
        shutdown_event: threading.Event,
        cluster: 'ClusterCoordinator'
    ) -> None:
        """Poll this node's tokens and leadership.

        Parameters
        ----------
        node_name : str
            This node's name.
        db : DatabaseContext
            Shared database context.
        tokens : TokenDistributor
            Holds my_tokens, token_version and on_rebalance. Updated in
            place.
        state_machine : JobStateMachine
            Read to tell leader from follower.
        event_queue : EventQueue
            Receives 'leadership_promoted' and 'leadership_demoted'.
        shutdown_event : threading.Event
            Ends the loop once set.
        cluster : ClusterCoordinator
            Supplies heartbeat_timeout.
        """
        super().__init__(
            f'token-refresh-{node_name}',
            tokens.token_refresh_initial,
            shutdown_event)
        self.node_name = node_name
        self.db = db
        self.tokens = tokens
        self.state_machine = state_machine
        self.event_queue = event_queue
        self.cluster = cluster
        self.start_time = time.time()
        self.check_count = 0
        self.initial_callback_sent = False

    def check(self) -> None:
        """Publish leadership changes and pass token changes to on_rebalance.
        """
        elapsed = time.time() - self.start_time
        self.interval = (
            self.tokens.token_refresh_initial
            if elapsed < 300
            else self.tokens.token_refresh_steady)

        try:
            with self.db.engine.connect() as conn:
                sql = f"""
select name from {self.db.tables["Node"]}
where last_heartbeat > now() - interval '{self.cluster.heartbeat_timeout} seconds'
order by created_on asc, name asc
"""
                result = conn.execute(text(sql))
                nodes = list(result)
                elected_leader = nodes[0][0] if nodes else None

            state = self.state_machine.state
            if elected_leader == self.node_name and state == JobState.RUNNING_FOLLOWER:
                logger.info('Detected leader promotion')
                self.event_queue.publish('leadership_promoted', {})
            elif elected_leader != self.node_name and state == JobState.RUNNING_LEADER:
                logger.warning('Detected leader demotion (older node rejoined)')
                self.event_queue.publish('leadership_demoted', {})
        except Exception as e:
            logger.warning(f'Failed to check leader status: {e}')

        with self.db.engine.connect() as conn:
            sql = f'select token_id, version from {self.db.tables["Token"]} where node = :node'
            result = conn.execute(text(sql), {'node': self.node_name})
            records = [dict(row._mapping) for row in result]

            if not records:
                new_tokens, new_version = set(), 0
            else:
                new_tokens = {row['token_id'] for row in records}
                new_version = records[0]['version'] if records else 0

            if not self.initial_callback_sent and new_tokens:
                logger.info(f'Initial token assignment: {len(new_tokens)} tokens, v{new_version}')
                self.tokens.my_tokens = new_tokens
                self.tokens.token_version = new_version
                if self.tokens.on_rebalance:
                    event = RebalanceEvent(
                        is_initial=True,
                        token_version=new_version,
                        tokens_added=len(new_tokens),
                        tokens_removed=0)
                    self.tokens.invoke_callback(self.tokens.on_rebalance, event)
                self.initial_callback_sent = True
                self.start_time = time.time()
            elif new_version != self.tokens.token_version:
                added = new_tokens - self.tokens.my_tokens
                removed = self.tokens.my_tokens - new_tokens

                logger.warning(
                    f'Token rebalance detected: v{self.tokens.token_version}->v{new_version}, '
                    f'+{len(added)} -{len(removed)} tokens')

                self.tokens.my_tokens = new_tokens
                self.tokens.token_version = new_version

                if self.tokens.on_rebalance:
                    event = RebalanceEvent(
                        is_initial=False,
                        token_version=new_version,
                        tokens_added=len(added),
                        tokens_removed=len(removed))
                    self.tokens.invoke_callback(self.tokens.on_rebalance, event)

                self.start_time = time.time()

            self.check_count += 1
            if self.check_count % 10 == 0:
                logger.debug(
                    f'Token refresh #{self.check_count}: '
                    f'{len(self.tokens.my_tokens)} tokens, '
                    f'v{self.tokens.token_version}')


class DeadNodeMonitor(Monitor):
    """Leader-only monitor that detects and removes dead nodes.
    """

    def __init__(
        self,
        node_name: str,
        db: DatabaseContext,
        cluster: ClusterCoordinator,
        locks: LockManager,
        event_queue: EventQueue,
        shutdown_event: threading.Event
    ) -> None:
        """Sweep dead nodes at the cluster's dead node interval.

        Parameters
        ----------
        node_name : str
            This node's name.
        db : DatabaseContext
            Shared database context.
        cluster : ClusterCoordinator
            Supplies the interval and heartbeat timeout.
        locks : LockManager
            Supplies the rebalance lock held during a sweep.
        event_queue : EventQueue
            Receives 'dead_nodes_detected'.
        shutdown_event : threading.Event
            Ends the loop once set.
        """
        super().__init__(
            f'dead-node-{node_name}',
            cluster.dead_node_check_interval,
            shutdown_event)
        self.node_name = node_name
        self.db = db
        self.cluster = cluster
        self.locks = locks
        self.event_queue = event_queue

    def check(self) -> None:
        """Delete dead nodes, their claims and locks, then publish them.
        """
        sql = f"""
select name
from {self.db.tables["Node"]}
where last_heartbeat <= now() - interval '{self.cluster.heartbeat_timeout} seconds' or last_heartbeat is null
"""

        with self.db.engine.connect() as conn:
            result = conn.execute(text(sql))
            dead_nodes = [row[0] for row in result]
            conn.rollback()

        if dead_nodes:
            logger.warning(f'Detected dead nodes: {dead_nodes}')
            tables = self.db.tables
            try:
                with self.locks.acquire_rebalance_lock('dead_node_monitor'):
                    with self.db.engine.connect() as conn:
                        for node in dead_nodes:
                            conn.execute(
                                text(f'delete from {tables["Node"]} where name = :name'),
                                {'name': node})
                            conn.execute(
                                text(f'delete from {tables["Claim"]} where node = :node'),
                                {'node': node})
                            conn.execute(
                                text(f'delete from {tables["Lock"]} where created_by = :node'),
                                {'node': node})
                            logger.info(f'Removed dead node and cleaned up locks: {node}')
                        conn.commit()
                self.event_queue.publish('dead_nodes_detected', {'nodes': dead_nodes})
            except LockNotAcquired:
                logger.debug('Rebalance already in progress, skipping dead node cleanup')


class RebalanceMonitor(Monitor):
    """Leader-only monitor that triggers rebalancing on membership changes.
    """

    def __init__(
        self,
        node_name: str,
        db: DatabaseContext,
        cluster: ClusterCoordinator,
        event_queue: EventQueue,
        shutdown_event: threading.Event,
        initial_node_count: int = None
    ) -> None:
        """Watch the live node count at the cluster's rebalance interval.

        Parameters
        ----------
        node_name : str
            This node's name.
        db : DatabaseContext
            Shared database context.
        cluster : ClusterCoordinator
            Supplies the interval and the live node query.
        event_queue : EventQueue
            Receives 'membership_changed'.
        shutdown_event : threading.Event
            Ends the loop once set.
        initial_node_count : int, optional
            Live node count at distribution time, the baseline for the
            first check. None queries the current count.
        """
        super().__init__(
            f'rebalance-{node_name}',
            cluster.rebalance_check_interval,
            shutdown_event)
        self.node_name = node_name
        self.db = db
        self.cluster = cluster
        self.event_queue = event_queue

        if initial_node_count is not None:
            self.last_node_count = initial_node_count
            logger.info(f'RebalanceMonitor initialized with distribution-time node count: {initial_node_count}')
        else:
            with self.db.engine.connect() as conn:
                result = conn.execute(text(self.cluster.active_nodes_sql))
                self.last_node_count = len(list(result))
            logger.info(f'RebalanceMonitor initialized with current node count: {self.last_node_count}')

    def check(self) -> None:
        """Publish 'membership_changed' when the live node count changes.
        """
        result = self.db.query(self.cluster.active_nodes_sql)
        current_count = len(result)

        logger.debug(f'Rebalance check: last={self.last_node_count}, current={current_count}')

        if current_count != self.last_node_count:
            logger.info(f'Node count changed: {self.last_node_count} -> {current_count}')
            self.event_queue.publish(
                'membership_changed',
                {
                    'previous_count': self.last_node_count,
                    'current_count': current_count,
                    })
            self.last_node_count = current_count


class CoordinationMonitor(Monitor):
    """Periodically processes coordination events.
    """

    def __init__(self, job: 'Job', interval: float = 1.0) -> None:
        """Drain the job's event queue every interval seconds.

        Parameters
        ----------
        job : Job
            Job whose events to process. Its shutdown event ends the loop.
        interval : float, default 1.0
            Seconds between checks.
        """
        super().__init__('coordination', interval, job._shutdown_event)
        self.job = job

    def check(self) -> None:
        """Process pending coordination events.
        """
        self.job._coordinate_state_transitions()


# --- Job class ---

class Job:
    """Job synchronization manager with state machine-based coordination.

    Lifecycle Phases:
    1. INITIALIZING: Configure instance variables
    2. CLUSTER_FORMING: Register node and start heartbeat
    3. ELECTING: Elect leader (oldest node wins)
    4. DISTRIBUTING: Leader distributes tokens
    5. RUNNING_LEADER/RUNNING_FOLLOWER: Normal operation
    6. SHUTTING_DOWN: Stop threads, cleanup database
    """

    def __init__(
        self,
        node_name: str,
        coordination_config: CoordinationConfig = None,
        date: datetime.date | datetime.datetime = None,
        wait_on_enter: int = 120,
        wait_on_exit: int = 0,
        lock_provider: callable = None,
        clear_existing_locks: bool = False,
        on_rebalance: callable = None,
    ) -> None:
        """Set up one node, and its database tables when coordinated.

        Parameters
        ----------
        node_name : str
            This node's name, unique across the cluster.
        coordination_config : CoordinationConfig, optional
            Coordination and database settings. None runs standalone,
            with no database access.
        date : datetime.date | datetime.datetime, optional
            Processing date for audit rows. None uses today's local date.
            A datetime contributes only its date.
        wait_on_enter : int, default 120
            Seconds __enter__ waits for the cluster to form, taken as
            int(abs(wait_on_enter)).
        wait_on_exit : int, default 0
            Seconds __exit__ sleeps before cleanup, taken as
            int(abs(wait_on_exit)).
        lock_provider : callable, optional
            Called with this Job during __enter__ to register task locks.
        clear_existing_locks : bool, default False
            Delete the locks this node created before lock_provider runs.
        on_rebalance : callable, optional
            Called on each token-ownership change: the initial assignment,
            a node joining or leaving, a rebalance. Receives a
            RebalanceEvent when it takes a positional argument, else no
            argument. Never called in standalone mode.

        Raises
        ------
        TypeError
            date is not None, a date or a datetime.
        """
        self.node_name = node_name
        self._wait_on_enter = int(abs(wait_on_enter))
        self._wait_on_exit = int(abs(wait_on_exit))
        self._nodes = [{'name': self.node_name}]

        if date is None:
            date = datetime.datetime.now().date()
        elif isinstance(date, datetime.datetime):
            date = date.date()
        elif not isinstance(date, datetime.date):
            raise TypeError(f'date must be None, datetime.date, or datetime.datetime, got {type(date).__name__}')

        self._created_on = ensure_timezone_aware(
            datetime.datetime.now(datetime.timezone.utc),
            'node created_on')
        self._shutdown_event = threading.Event()
        self._lock_provider = lock_provider
        self._clear_existing_locks = clear_existing_locks
        self._event_queue = EventQueue()

        self._coordination_enabled = coordination_config is not None

        if coordination_config is not None:
            coord_cfg = coordination_config
            self.db = DatabaseContext(coord_cfg)
            self.cluster = ClusterCoordinator(
                node_name,
                self.db,
                coord_cfg,
                self._created_on)
            self.tokens = TokenDistributor(node_name, self.db, coord_cfg)
            self.locks = LockManager(node_name, self.db, coord_cfg)
            self.tasks = TaskManager(
                node_name,
                self.db,
                date,
                coord_cfg.total_tokens,
                coord_cfg.hash_function)

            self.tokens.on_rebalance = on_rebalance

            ensure_database_ready(self.db.engine, coord_cfg.appname)
        else:
            self.db = None
            self.cluster = None
            self.tokens = None
            self.locks = None
            self.tasks = None

        self.state_machine = JobStateMachine()

        self._monitors = {}
        self._elected_leader = None
        self._nodes_at_distribution = None

        machine = self.state_machine
        machine.on_enter(JobState.CLUSTER_FORMING, self._on_enter_cluster_forming)
        machine.on_enter(JobState.ELECTING, self._on_enter_electing)
        machine.on_enter(JobState.DISTRIBUTING, self._on_enter_distributing)
        machine.on_enter(JobState.RUNNING_LEADER, self._on_enter_running_leader)
        machine.on_exit(JobState.RUNNING_LEADER, self._on_exit_running_leader)
        machine.on_enter(JobState.RUNNING_FOLLOWER, self._on_enter_running_follower)
        machine.on_enter(JobState.SHUTTING_DOWN, self._on_enter_shutting_down)

        if self._coordination_enabled:
            coord_monitor = CoordinationMonitor(self, interval=1.0)
            self._start_monitor('coordination', coord_monitor)

        if not self._coordination_enabled and on_rebalance:
            logger.warning('on_rebalance callback provided but coordination_enabled=False - callback will never fire')

    def _start_monitor(self, name: str, monitor: Monitor) -> None:
        """Start monitor under name, unless a monitor of that name exists.
        """
        if name in self._monitors:
            logger.debug(f'Monitor {name} already running')
            return
        self._monitors[name] = monitor
        monitor.start()
        logger.info(f'Started monitor: {name}')

    def _stop_monitor(self, name: str) -> None:
        """Ask the monitor under name to stop, and forget it.
        """
        if name not in self._monitors:
            return
        self._monitors[name].stop()
        del self._monitors[name]
        logger.info(f'Stopped monitor: {name}')

    def _on_enter_cluster_forming(self) -> None:
        """Entry action for CLUSTER_FORMING state.
        """
        self.cluster.register()

        heartbeat_monitor = HeartbeatMonitor(self.cluster, self._shutdown_event)
        self._start_monitor('heartbeat', heartbeat_monitor)

        health_monitor = HealthMonitor(
            cluster=self.cluster,
            event_queue=self._event_queue,
            shutdown_event=self._shutdown_event)
        self._start_monitor('health', health_monitor)

        if self._lock_provider:
            if self._clear_existing_locks:
                logger.info(f'Clearing existing locks created by {self.node_name}')
                self.locks.clear_locks_by_creator(self.node_name)
            logger.info('Invoking lock_provider callback')
            self._lock_provider(self)

    def _on_enter_electing(self) -> None:
        """Entry action for ELECTING state.
        """
        try:
            self._elected_leader = self.cluster.elect_leader()
            logger.info(f'Leader election complete: {self._elected_leader}')
        except Exception as e:
            logger.error(f'Failed to elect leader: {e}')
            self.state_machine.transition_to(JobState.ERROR)

    def _on_enter_distributing(self) -> None:
        """Entry action for DISTRIBUTING state.
        """
        try:
            if self._elected_leader is None:
                raise RuntimeError('No leader elected before distribution')

            if self._elected_leader == self.node_name:
                logger.info('This node is the leader, performing token distribution')

                nodes_before_distribution = self.cluster.get_active_nodes()
                self._nodes_at_distribution = len(nodes_before_distribution)
                logger.info(f'Node count at distribution: {self._nodes_at_distribution}')
                self._distribute_tokens_safe('initial_distribution')
            else:
                logger.info(f'Follower node detected, waiting for leader {self._elected_leader} to distribute tokens')
        except Exception:
            self.state_machine.transition_to(JobState.ERROR)
            raise

    def _on_enter_running_leader(self) -> None:
        """Entry action for RUNNING_LEADER state.
        """
        logger.info('Starting leader monitoring threads...')

        token_refresh = TokenRefreshMonitor(
            node_name=self.node_name,
            db=self.db,
            tokens=self.tokens,
            state_machine=self.state_machine,
            event_queue=self._event_queue,
            shutdown_event=self._shutdown_event,
            cluster=self.cluster)
        self._start_monitor('token_refresh', token_refresh)

        dead_node_monitor = DeadNodeMonitor(
            node_name=self.node_name,
            db=self.db,
            cluster=self.cluster,
            locks=self.locks,
            event_queue=self._event_queue,
            shutdown_event=self._shutdown_event)
        self._start_monitor('dead_node', dead_node_monitor)

        rebalance_monitor = RebalanceMonitor(
            node_name=self.node_name,
            db=self.db,
            cluster=self.cluster,
            event_queue=self._event_queue,
            shutdown_event=self._shutdown_event,
            initial_node_count=self._nodes_at_distribution)
        self._start_monitor('rebalance', rebalance_monitor)

    def _on_exit_running_leader(self) -> None:
        """Exit action for RUNNING_LEADER state.
        """
        logger.info('Stopping leader monitoring threads...')
        self._stop_monitor('dead_node')
        self._stop_monitor('rebalance')

    def _on_enter_running_follower(self) -> None:
        """Entry action for RUNNING_FOLLOWER state.
        """
        token_refresh = TokenRefreshMonitor(
            node_name=self.node_name,
            db=self.db,
            tokens=self.tokens,
            state_machine=self.state_machine,
            event_queue=self._event_queue,
            shutdown_event=self._shutdown_event,
            cluster=self.cluster)
        self._start_monitor('token_refresh', token_refresh)

    def _on_enter_shutting_down(self) -> None:
        """Entry action for SHUTTING_DOWN state.
        """
        self._shutdown_event.set()

    def _coordinate_state_transitions(self) -> None:
        """Central coordinator for all state transitions.
        """
        events = self._event_queue.consume_all()

        if events:
            logger.info(f'Processing {len(events)} coordination events: {[e.type for e in events]}')

        for event in events:
            logger.info(
                f'Event: {event.type}',
                extra={
                    'event_type': event.type,
                    'event_data': event.data,
                    'current_state': self.state_machine.state.value,
                    'node_name': self.node_name,
                    })

            if event.type == 'node_unhealthy':
                logger.error(f'Node unhealthy: {event.data}')
                self.state_machine.transition_to(JobState.ERROR)
                self.state_machine.transition_to(JobState.SHUTTING_DOWN)
                return

            elif event.type == 'shutdown_requested':
                logger.info('Shutdown requested')
                if self.state_machine.state != JobState.SHUTTING_DOWN:
                    self.state_machine.transition_to(JobState.SHUTTING_DOWN)
                return

            elif event.type == 'leadership_promoted':
                if self.state_machine.is_running():
                    logger.warning(f'{self.node_name} promoted to leader')
                    self.state_machine.transition_to(JobState.RUNNING_LEADER)

            elif event.type == 'leadership_demoted':
                if self.state_machine.is_running():
                    logger.warning(f'{self.node_name} demoted from leader')
                    self.state_machine.transition_to(JobState.RUNNING_FOLLOWER)

            elif event.type == 'dead_nodes_detected':
                if self.state_machine.is_leader():
                    logger.info(f'Dead nodes: {event.data.get("nodes")}, rebalancing')
                    self._distribute_tokens_safe('dead_nodes')

            elif event.type == 'membership_changed':
                if self.state_machine.is_leader():
                    data = event.data
                    logger.info(f'Membership: {data.get("previous_count")} → {data.get("current_count")}')
                    self._distribute_tokens_safe('membership_change')

    def _wait_for_enter_time_and_minimum_nodes(self) -> bool:
        """Wait out wait_on_enter, then count the live nodes.

        Returns
        -------
        bool
            True when at least minimum_nodes are live. False on shutdown
            or with too few nodes.
        """
        target_nodes = self.tokens.minimum_nodes
        logger.info(
            f'Waiting {self._wait_on_enter}s for cluster formation grace period '
            f'(target: {target_nodes} nodes)...')
        start_wait = time.time()

        while time.time() - start_wait < self._wait_on_enter:
            if self._shutdown_event.wait(timeout=1.0):
                logger.info('Shutdown requested during cluster wait')
                return False

        self._nodes = self.cluster.get_active_nodes()
        logger.info(
            'Cluster formation grace period complete after '
            f'{self._wait_on_enter}s with {len(self._nodes)} node(s) '
            f'(target: {target_nodes})')
        logger.info(f'Active nodes: {[n["name"] for n in self._nodes]}')

        return not len(self._nodes) < target_nodes

    def __enter__(self) -> Self:
        """Enter context and perform coordination setup.
        """
        if not self._coordination_enabled:
            logger.info(f'Starting {self.node_name} in standalone mode (no coordination)')
            return self

        logger.info(f'Starting {self.node_name} in coordination mode')

        try:
            if not self.state_machine.transition_to(JobState.CLUSTER_FORMING):
                raise RuntimeError('Invalid state transition to CLUSTER_FORMING')

            if not self._wait_for_enter_time_and_minimum_nodes():
                if not self._shutdown_event.is_set():
                    raise TimeoutError(
                        f'Minimum nodes ({self.tokens.minimum_nodes}) not reached '
                        f'after {self._wait_on_enter}s grace period '
                        f'(only {len(self._nodes)} node(s) present)')
                return self

            if not self.state_machine.transition_to(JobState.ELECTING):
                raise RuntimeError('Invalid state transition to ELECTING')

            if not self.state_machine.transition_to(JobState.DISTRIBUTING):
                raise RuntimeError('Invalid state transition to DISTRIBUTING')

            if self.state_machine.state == JobState.ERROR:
                raise RuntimeError('Failed to complete token distribution')

            follower_timeout = 2 * self.tokens.token_distribution_timeout
            logger.info(f'Waiting for token distribution (timeout: {follower_timeout}s)...')
            self.tokens.wait_for_distribution(timeout_sec=follower_timeout)

            self.tokens.my_tokens, self.tokens.token_version = (
                self.tokens.get_my_tokens_versioned())
            logger.info(
                f'Node {self.node_name} assigned {len(self.tokens.my_tokens)} '
                f'tokens, version {self.tokens.token_version}')

            elected_leader = self.cluster.elect_leader()
            is_leader = elected_leader == self.node_name

            if is_leader:
                self.state_machine.transition_to(JobState.RUNNING_LEADER)
            else:
                self.state_machine.transition_to(JobState.RUNNING_FOLLOWER)

        except Exception as e:
            logger.error(f'Initialization failed: {e}', exc_info=True)
            self.state_machine.transition_to(JobState.ERROR)
            raise

        return self

    def __exit__(
        self,
        exc_ty: type[BaseException] | None,
        exc_val: BaseException | None,
        tb: TracebackType | None
    ) -> None:
        """Exit context and cleanup coordination resources.
        """
        logger.debug(f'Exiting {self.node_name} context')

        if exc_ty:
            if any(klass.__name__ == 'SessionError' for klass in exc_ty.__mro__):
                logger.debug(f'{self.node_name} context exited with session error: {exc_val}')
            else:
                logger.error(exc_val)

        if self._coordination_enabled:
            if self.state_machine.state != JobState.SHUTTING_DOWN:
                self.state_machine.transition_to(JobState.SHUTTING_DOWN)

            for name, monitor in list(self._monitors.items()):
                if monitor.thread and monitor.thread.is_alive():
                    logger.debug(f'Joining {name} thread...')
                    monitor.thread.join(timeout=10)
                    if monitor.thread.is_alive():
                        logger.warning(f'{name} thread did not stop within timeout')

            if self.tokens:
                self.tokens.shutdown_callbacks(wait=True, timeout=10)

            if self._wait_on_exit:
                logger.debug(f'Sleeping {self._wait_on_exit} seconds...')
                time.sleep(self._wait_on_exit)

            self._cleanup()

        if self.db is not None:
            self.db.dispose()

    @property
    def nodes(self) -> list[dict]:
        """Safely expose internal nodes.
        """
        return deepcopy(self._nodes)

    @property
    def my_tokens(self) -> set[int]:
        """Get current token IDs owned by this node (read-only copy).
        """
        if self.tokens is not None:
            return self.tokens.my_tokens.copy()
        return set()

    @property
    def token_version(self) -> int:
        """Get current token distribution version.
        """
        if self.tokens is not None:
            return self.tokens.token_version
        return 0

    def set_claim(self, task: Task | Hashable) -> None:
        """Record a claim row for each task, keeping any existing claim.

        Parameters
        ----------
        task : Task | Hashable
            Task or task id, or an iterable of them. A str counts as one
            task id, but any other iterable, a tuple id included, counts
            as a batch. Ignored in standalone mode.
        """
        if self.tasks is not None:
            self.tasks.set_claim(task)

    def add_task(self, task: Task | Hashable) -> None:
        """Queue task for the audit and claim it, when this node owns it.

        Parameters
        ----------
        task : Task | Hashable
            Task or task id. Dropped without error when this node cannot
            claim it, and always in standalone mode.
        """
        if self.tasks is None:
            return

        if self._coordination_enabled and not self.can_claim_task(task):
            task_id = extract_task_id(task)
            logger.debug(f'Task {task_id} rejected (token not owned)')
            return

        self.tasks.add_task(task)
        self.set_claim(task)

    def write_audit(self) -> None:
        """Write accumulated tasks to audit table.
        """
        if self.tasks is not None:
            self.tasks.write_audit()

    def get_audit(self) -> list[dict]:
        """Audit rows for this job's date, across all nodes.

        Returns
        -------
        list[dict]
            One dict per row with keys 'node' and 'task_id'. Empty in
            standalone mode.
        """
        if not self._coordination_enabled:
            return []
        return self.tasks.get_audit()

    def can_claim_task(self, task: Task | Hashable) -> bool:
        """Whether this node may claim task now.

        Parameters
        ----------
        task : Task | Hashable
            Task or task id.

        Returns
        -------
        bool
            True in standalone mode. Otherwise True only in a running
            state and when this node owns the task's token.
        """
        if not self._coordination_enabled:
            return True

        if not self.state_machine.can_claim_task():
            task_id = extract_task_id(task)
            logger.debug(f'Cannot claim task {task_id} in state {self.state_machine.state.value}')
            return False

        return self.tasks.can_claim(task, self.tokens.my_tokens)

    def task_to_token(self, task_id: Hashable) -> int:
        """Token for task_id under this job's token count and hash function.

        Parameters
        ----------
        task_id : Hashable
            Task identifier.

        Returns
        -------
        int
            Token id. 0 in standalone mode.
        """
        if self.tokens is None or self.tasks is None:
            return 0
        return task_to_token(
            task_id,
            self.tokens.total_tokens,
            self.tasks.hash_function)

    def register_lock(
        self,
        task_id: Hashable,
        node_patterns: str | list[str],
        reason: str = None,
        expires_in_days: int = None
    ) -> None:
        """Pin a task to nodes matching a pattern, replacing its old lock.

        Parameters
        ----------
        task_id : Hashable
            Task to pin, never a token id.
        node_patterns : str | list[str]
            SQL LIKE pattern, or fallback patterns tried in order.
        reason : str, optional
            Free text stored with the lock.
        expires_in_days : int, optional
            Days until the lock expires. None or 0 never expires.
        """
        if self.locks is not None:
            self.locks.register_lock(task_id, node_patterns, reason, expires_in_days)

    def register_locks_bulk(
        self,
        locks: list[tuple[Hashable, str | list[str], str]]
    ) -> None:
        """Pin many tasks in one transaction, as register_lock does.

        Parameters
        ----------
        locks : list[tuple[Hashable, str | list[str], str]]
            (task_id, node_patterns, reason) per lock. These locks never
            expire.
        """
        if self.locks is not None:
            self.locks.register_locks_bulk(locks)

    def clear_locks_by_creator(self, creator: str) -> int:
        """Delete every lock one node created.

        Parameters
        ----------
        creator : str
            Node name stored as the lock's creator.

        Returns
        -------
        int
            Locks deleted. 0 in standalone mode.
        """
        if self.locks is not None:
            return self.locks.clear_locks_by_creator(creator)
        return 0

    def clear_all_locks(self) -> int:
        """Delete every lock, whatever its creator.

        Returns
        -------
        int
            Locks deleted. 0 in standalone mode.
        """
        if self.locks is not None:
            return self.locks.clear_all_locks()
        return 0

    def list_locks(self) -> list[dict]:
        """Unexpired locks, newest first.

        Returns
        -------
        list[dict]
            One dict per lock, as LockManager.list_locks returns. Empty in
            standalone mode.
        """
        if not self._coordination_enabled:
            return []
        return self.locks.list_locks()

    def get_coordination_status(self) -> dict:
        """Snapshot of coordination state for debugging.

        Returns
        -------
        dict
            {'coordination_enabled': False} in standalone mode. Otherwise
            also the node name, state, leadership, token count and
            version, live node count, last 20 events, last heartbeat and
            monitor names.
        """
        if not self._coordination_enabled:
            return {'coordination_enabled': False}

        return {
            'coordination_enabled': True,
            'node_name': self.node_name,
            'state': self.state_machine.state.value,
            'is_leader': self.state_machine.is_leader(),
            'my_tokens': len(self.tokens.my_tokens),
            'token_version': self.tokens.token_version,
            'total_tokens': self.tokens.total_tokens,
            'active_nodes': len(self.cluster.get_active_nodes()),
            'recent_events': [
                {'type': e.type, 'timestamp': e.timestamp, 'data': e.data}
                for e in self._event_queue.get_history(limit=20)
                ],
            'last_heartbeat': self.cluster.last_heartbeat_sent,
            'monitors': list(self._monitors.keys()),
            }

    def am_i_healthy(self) -> bool:
        """Whether this node's heartbeat is fresh and the database answers.

        Returns
        -------
        bool
            True in standalone mode. Otherwise as
            ClusterCoordinator.am_i_healthy.
        """
        if not self._coordination_enabled:
            return True
        return self.cluster.am_i_healthy()

    def am_i_leader(self) -> bool:
        """Whether the database names this node the oldest live node.

        Returns
        -------
        bool
            False in standalone mode and outside the running states,
            without a query. When the query fails, the leader state the
            state machine holds.
        """
        if not self._coordination_enabled:
            return False

        if not self.state_machine.is_running():
            return False

        try:
            elected_leader = self.cluster.elect_leader()
            return elected_leader == self.node_name
        except Exception as e:
            logger.warning(f'Failed to query leader status: {e}, maintaining current state')
            return self.state_machine.is_leader()

    def get_active_nodes(self) -> list[dict]:
        """Nodes that sent a heartbeat within heartbeat_timeout.

        Returns
        -------
        list[dict]
            As ClusterCoordinator.get_active_nodes. Empty in standalone
            mode.
        """
        if not self._coordination_enabled:
            return []
        return self.cluster.get_active_nodes()

    def get_dead_nodes(self) -> list[str]:
        """Names of nodes with no heartbeat within heartbeat_timeout.

        Returns
        -------
        list[str]
            Node names. Empty in standalone mode.
        """
        if not self._coordination_enabled:
            return []
        return self.cluster.get_dead_nodes()

    def can_distribute_tokens(self) -> bool:
        """True in DISTRIBUTING or RUNNING_LEADER.
        """
        return self.state_machine.state in {
            JobState.DISTRIBUTING,
            JobState.RUNNING_LEADER,
            }

    def _distribute_tokens_safe(self, trigger_reason: str = 'distribution') -> None:
        """Distribute tokens under the rebalance lock and the leader lock.

        Never raises. A lock held elsewhere skips the distribution with a
        debug log. Any other failure is logged as an error, and while
        initializing it moves the state machine to ERROR.

        Parameters
        ----------
        trigger_reason : str, default 'distribution'
            Cause, stored in the rebalance audit row.
        """
        if self.locks is None or self.tokens is None or self.cluster is None:
            return

        try:
            # Every node takes the rebalance lock first, then the leader
            # lock, so two nodes never deadlock.
            with self.locks.acquire_rebalance_lock('token_distribution'):
                with self.locks.acquire_leader_lock('distribute'):
                    self.tokens.distribute(self.locks, self.cluster, trigger_reason)
        except LockNotAcquired as e:
            logger.debug(f'Could not acquire lock for token distribution: {e}')
        except Exception as e:
            logger.error(f'Token distribution failed: {e}', exc_info=True)
            if self.state_machine.is_initializing():
                self.state_machine.transition_to(JobState.ERROR)

    def _cleanup(self) -> None:
        """Cleanup tables and write audit log.
        """
        if not self._coordination_enabled or self.db is None:
            return

        cleanup_steps = [
            ('audit', lambda: self.tasks and self.tasks.write_audit()),
            ('cluster', lambda: self.cluster and self.cluster.cleanup()),
            ('tasks', lambda: self.tasks and self.tasks.cleanup()),
            ]

        for cleanup_name, cleanup_func in cleanup_steps:
            try:
                cleanup_func()
            except Exception as e:
                logger.error(f'{cleanup_name} cleanup failed: {e}')
