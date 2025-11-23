"""Job synchronization manager with state machine-based coordination.
"""
import contextlib
import datetime
import functools
import hashlib
import json
import logging
import re
import threading
import time
from collections import deque
from collections.abc import Hashable, Iterable
from copy import deepcopy
from dataclasses import dataclass
from enum import Enum
from functools import total_ordering

from sqlalchemy import create_engine, text

from jobsync.schema import ensure_database_ready, get_table_names

logger = logging.getLogger(__name__)

__all__ = ['Job', 'Task', 'LockNotAcquired', 'CoordinationConfig']


# ============================================================
# EVENT SYSTEM
# ============================================================

@dataclass
class CoordinationEvent:
    """Simple event for monitor-to-job communication.
    """
    type: str
    data: dict
    timestamp: float = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = time.time()


class EventQueue:
    """Thread-safe event queue for monitors to publish to Job.
    """

    def __init__(self, history_size: int = 100):
        """Initialize event queue with thread lock and history buffer.

        Args:
            history_size: Maximum number of events to retain in history
        """
        self._events = []
        self._history = deque(maxlen=history_size)
        self._lock = threading.Lock()

    def publish(self, event_type: str, data: dict = None) -> None:
        """Publish event to queue.

        Args:
            event_type: Event type identifier
            data: Optional event data
        """
        with self._lock:
            self._events.append(CoordinationEvent(event_type, data or {}))

    def consume_all(self) -> list[CoordinationEvent]:
        """Consume and return all events, clearing the queue and storing in history.

        Returns
            List of events
        """
        with self._lock:
            events = self._events[:]
            self._history.extend(events)
            self._events.clear()
            return events

    def get_history(self, limit: int = None) -> list[CoordinationEvent]:
        """Get event history including both processed and unprocessed events.

        Args:
            limit: Optional limit on number of events (most recent if limited)

        Returns
            List of events (most recent first if limited)
        """
        with self._lock:
            all_events = list(self._history) + self._events
            if limit is not None:
                return all_events[-limit:] if limit > 0 else []
            return all_events[:]


# ============================================================
# UTILITY FUNCTIONS
# ============================================================

@dataclass
class CoordinationConfig:
    """Configuration for hybrid coordination system.

    All timing parameters are in seconds.
    Connection parameters for database access.
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

    host: str = 'localhost'
    port: int = 5432
    dbname: str = 'jobsync'
    user: str = 'postgres'
    password: str = 'postgres'
    appname: str = 'sync_'


def build_connection_string(host: str, port: int, dbname: str, user: str, password: str) -> str:
    """Build PostgreSQL connection string from parameters.
    """
    return (
        f'postgresql+psycopg://{user}:{password}'
        f'@{host}:{port}/{dbname}'
    )


def retry_with_backoff(max_attempts: int = 5, base_delay: float = 1.0, operation_name: str = None):
    """Decorator to retry function with exponential backoff on database errors.

    Args:
        max_attempts: Maximum retry attempts
        base_delay: Base delay in seconds (doubles each retry)
        operation_name: Name for logging (defaults to function name)

    Returns
        Decorated function
    """
    def decorator(func: callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            name = operation_name or func.__name__
            last_exception = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt == max_attempts:
                        logger.error(f'{name} failed after {max_attempts} attempts: {e}')
                        raise
                    delay = base_delay * (2 ** (attempt - 1))
                    logger.warning(f'{name} attempt {attempt}/{max_attempts} failed: {e}, retrying in {delay:.1f}s')
                    time.sleep(delay)
            raise last_exception
        return wrapper
    return decorator


def log_duration(operation_name: str = None):
    """Decorator to log method execution duration.

    Args:
        operation_name: Custom name for logging (defaults to function name)

    Returns
        Decorated function
    """
    def decorator(func: callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            name = operation_name or func.__name__
            start = time.time()
            result = func(*args, **kwargs)
            duration_ms = int((time.time() - start) * 1000)
            logger.info(f'{name} completed in {duration_ms}ms')
            return result
        return wrapper
    return decorator


def ensure_timezone_aware(dt: datetime.datetime, name: str = 'datetime') -> datetime.datetime:
    """Ensure datetime is timezone-aware.

    Args:
        dt: Datetime to check
        name: Name for error message

    Returns
        The datetime (unchanged if already aware)

    Raises
        ValueError: If datetime is naive
    """
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        raise ValueError(f'{name} must be timezone-aware (has tzinfo), got naive datetime: {dt}')
    return dt


def task_to_token(task_id: Hashable, total_tokens: int) -> int:
    """Hash task_id to token_id using consistent hashing.
    """
    task_str = str(task_id)
    hash_obj = hashlib.md5(task_str.encode())
    hash_int = int(hash_obj.hexdigest(), 16)
    token_id = hash_int % total_tokens
    return token_id


def matches_pattern(node_name: str, pattern: str) -> bool:
    """Check if node_name matches SQL LIKE pattern.
    """
    if node_name == pattern:
        return True

    # Convert SQL LIKE pattern to regex by processing character-by-character
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


def find_nodes_matching_patterns(patterns: str | list[str], active_nodes: list[str], pattern_matcher: callable) -> list[str]:
    """Find nodes matching lock patterns, trying each fallback pattern in order.
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
    """Categorize tokens based on lock constraints.

    Returns
        distributable: Tokens with no locks (normal distribution)
        locked: Tokens with locks that found matching nodes {token_id: [eligible_nodes]}
        blocked: Tokens with locks but no matching nodes (not distributed)
    """
    distributable = []
    locked = {}
    blocked = []

    for token_id in range(total_tokens):
        if token_id not in locked_tokens:
            distributable.append(token_id)
        else:
            matching_nodes = find_nodes_matching_patterns(
                locked_tokens[token_id], active_nodes, pattern_matcher)
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
    return sum(1 for tid, node in new_assignments.items()
               if current_assignments.get(tid) != node)


def compute_minimal_move_distribution(
    total_tokens: int,
    active_nodes: list[str],
    current_assignments: dict[int, str],
    locked_tokens: dict[int, str | list[str]],
    pattern_matcher: callable
) -> tuple[dict[int, str], int]:
    """Compute token distribution using minimal-move algorithm with lock constraints.

    Pure function that calculates optimal token distribution across nodes while:
    - Minimizing token movement (keeping existing assignments when possible)
    - Respecting locked token constraints
    - Balancing load evenly across all active nodes

    Args:
        total_tokens: Total number of tokens to distribute
        active_nodes: List of active node names
        current_assignments: Current token_id -> node_name mapping
        locked_tokens: Locked token_id -> node_pattern(s) mapping (single pattern or list of fallback patterns)
        pattern_matcher: Function(node_name, pattern) -> bool for pattern matching

    Returns
        Tuple of (new_assignments dict, tokens_moved count)
    """
    def assign_locked_token(token_id: int, eligible_nodes: list[str], assigned_so_far: dict) -> str:
        """Assign locked token to eligible node, preferring current owner or least-loaded node.
        """
        current_owner = current_assignments.get(token_id)
        if current_owner and current_owner in eligible_nodes:
            return current_owner

        load_counts = dict.fromkeys(eligible_nodes, 0)
        for node in assigned_so_far.values():
            if node in load_counts:
                load_counts[node] += 1

        return min(eligible_nodes, key=lambda n: (load_counts[n], n))

    def assign_unlocked_token(token_id: int, receivers: deque, node_loads: dict, target: int) -> str:
        """Assign unlocked token minimizing movement, respecting target distribution.
        """
        current_owner = current_assignments.get(token_id)
        is_current_active = current_owner in node_loads
        is_over_target = is_current_active and node_loads[current_owner] > target

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
        total_tokens, active_nodes, locked_tokens, pattern_matcher)

    total_distributable = len(distributable)
    target_per_node = total_distributable // len(active_nodes)
    remainder = total_distributable % len(active_nodes)

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

    needs_rebalance = any(node_loads[n] > target_per_node for n in active_nodes) and bool(receivers)
    iteration_order = sorted(distributable, reverse=needs_rebalance)

    new_assignments = {}
    for token_id, eligible_nodes in locked_with_nodes.items():
        new_assignments[token_id] = assign_locked_token(token_id, eligible_nodes, new_assignments)

    for token_id in iteration_order:
        new_assignments[token_id] = assign_unlocked_token(token_id, receivers, node_loads, target_per_node)

    moves = count_assignment_changes(current_assignments, new_assignments)

    return new_assignments, moves


# ============================================================
# EXCEPTIONS
# ============================================================

class LockNotAcquired(Exception):
    """Raised when a coordination lock cannot be acquired.
    """


# ============================================================
# TASK MODEL
# ============================================================

@total_ordering
class Task:

    def __init__(self, id: Hashable, name: str = None):
        """Initialize a task with unique identifier and optional name.
        """
        assert isinstance(id, Hashable), f'Id {id} must be hashable'
        self.id = id
        self.name = name

    def __repr__(self) -> str:
        """Return string representation of task.
        """
        return f'id:{self.id},name:{self.name}'

    def __eq__(self, other) -> bool:
        """Check equality based on task id.
        """
        if not isinstance(other, Task):
            return NotImplemented
        return self.id == other.id

    def __lt__(self, other) -> bool:
        """Compare tasks based on id for sorting.
        """
        if not isinstance(other, Task):
            return NotImplemented
        return self.id < other.id

    def __gt__(self, other) -> bool:
        """Compare tasks based on id for sorting.
        """
        if not isinstance(other, Task):
            return NotImplemented
        return self.id > other.id


# ============================================================
# PURE STATE MACHINE
# ============================================================

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

    Manages state transitions with validation and callbacks.
    Services are orchestrated by the Job class via registered callbacks.
    """

    def __init__(self):
        """Initialize state machine with transition graph.
        """
        self.state = JobState.INITIALIZING
        self._on_enter_callbacks = {}
        self._on_exit_callbacks = {}
        self._valid_transitions = {}
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
        """Register a valid transition.

        Args:
            from_state: Source state
            to_state: Target state
        """
        if from_state not in self._valid_transitions:
            self._valid_transitions[from_state] = set()
        self._valid_transitions[from_state].add(to_state)

    def on_enter(self, state: JobState, callback: callable) -> None:
        """Register callback when entering state.

        Args:
            state: State to monitor
            callback: Function to call on entry
        """
        self._on_enter_callbacks[state] = callback

    def on_exit(self, state: JobState, callback: callable) -> None:
        """Register callback when exiting state.

        Args:
            state: State to monitor
            callback: Function to call on exit
        """
        self._on_exit_callbacks[state] = callback

    def transition_to(self, new_state: JobState) -> bool:
        """Attempt transition with validation.

        Args:
            new_state: Target state

        Returns
            True if transition succeeded, False if invalid
        """
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
        """Check if in leader state.

        Returns
            True if in RUNNING_LEADER state
        """
        return self.state == JobState.RUNNING_LEADER

    def is_follower(self) -> bool:
        """Check if in follower state.

        Returns
            True if in RUNNING_FOLLOWER state
        """
        return self.state == JobState.RUNNING_FOLLOWER

    def is_running(self) -> bool:
        """Check if in running state (leader or follower).

        Returns
            True if in either RUNNING_LEADER or RUNNING_FOLLOWER state
        """
        return self.state in {JobState.RUNNING_LEADER, JobState.RUNNING_FOLLOWER}

    def is_initializing(self) -> bool:
        """Check if in initialization states.

        Returns
            True if in any initialization state
        """
        return self.state in {JobState.INITIALIZING, JobState.CLUSTER_FORMING, JobState.ELECTING, JobState.DISTRIBUTING}

    def is_error(self) -> bool:
        """Check if in error state.

        Returns
            True if in ERROR state
        """
        return self.state == JobState.ERROR

    def can_claim_task(self) -> bool:
        """Check if current state allows claiming tasks.

        Tasks can only be claimed when the node is in a running state
        (either as leader or follower).

        Returns
            True if in RUNNING_LEADER or RUNNING_FOLLOWER state
        """
        return self.state in {JobState.RUNNING_LEADER, JobState.RUNNING_FOLLOWER}


# ============================================================
# SERVICE LAYER
# ============================================================

class DatabaseContext:
    """Manages database engine, connections, and table names.
    """

    def __init__(self, coord_config: CoordinationConfig):
        """Initialize database context.

        Args:
            coord_config: Coordination configuration with connection parameters
        """
        connection_string = build_connection_string(
            coord_config.host, coord_config.port, coord_config.dbname, coord_config.user,
            coord_config.password)
        self.engine = create_engine(connection_string, pool_pre_ping=True, pool_size=10, max_overflow=5)
        self.tables = get_table_names(coord_config.appname)

    def execute(self, sql: str, params: dict = None):
        """Execute SQL statement with automatic commit.

        Args:
            sql: SQL statement to execute
            params: Optional parameters for the statement

        Returns
            Result proxy object
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            conn.commit()
            return result

    def query(self, sql: str, params: dict = None) -> list:
        """Execute query and return all rows.

        Args:
            sql: SQL query to execute
            params: Optional parameters for the query

        Returns
            List of row objects
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

    def __init__(self, node_name: str, db: DatabaseContext, coord_config: CoordinationConfig, created_on: datetime.datetime):
        """Initialize cluster coordinator.

        Args:
            node_name: This node's name
            db: Database context
            coord_config: Coordination configuration
            created_on: Node creation timestamp
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
        SELECT name, created_on, last_heartbeat
        FROM {self.db.tables["Node"]}
        WHERE last_heartbeat > NOW() - INTERVAL '{self.heartbeat_timeout} seconds'
        ORDER BY created_on ASC, name ASC
        """

    @retry_with_backoff()
    def register(self) -> None:
        """Register node with initial heartbeat.
        """
        self.last_heartbeat_sent = datetime.datetime.now(datetime.timezone.utc)
        sql = f"""
        INSERT INTO {self.db.tables["Node"]} (name, created_on, last_heartbeat)
        VALUES (:name, :created_on, :heartbeat)
        ON CONFLICT (name) DO UPDATE
        SET last_heartbeat = EXCLUDED.last_heartbeat
        """
        self.db.execute(sql, {
            'name': self.node_name,
            'created_on': self.created_on,
            'heartbeat': self.last_heartbeat_sent
        })
        logger.info(f'Node {self.node_name} registered with heartbeat')

    def elect_leader(self) -> str:
        """Elect leader as node with oldest registration timestamp.

        Returns
            Name of elected leader node
        """
        nodes = self.db.query(self.active_nodes_sql)

        if not nodes:
            raise RuntimeError('No active nodes for leader election')

        return nodes[0][0]

    def get_active_nodes(self) -> list[dict]:
        """Get list of active nodes.

        Returns
            List of node dicts with name, created_on, last_heartbeat
        """
        result = self.db.query(self.active_nodes_sql)
        return [{'name': row[0], 'created_on': row[1], 'last_heartbeat': row[2]} for row in result]

    def get_dead_nodes(self) -> list[str]:
        """Get list of dead nodes.

        Returns
            List of dead node names
        """
        sql = f"""
        SELECT name
        FROM {self.db.tables["Node"]}
        WHERE last_heartbeat <= NOW() - INTERVAL '{self.heartbeat_timeout} seconds' OR last_heartbeat IS NULL
        """
        result = self.db.query(sql)
        return [row[0] for row in result]

    def am_i_healthy(self) -> bool:
        """Check if node is healthy.

        Returns
            True if healthy, False otherwise
        """
        try:
            if self.last_heartbeat_sent:
                age_seconds = (datetime.datetime.now(datetime.timezone.utc) - self.last_heartbeat_sent).total_seconds()
                if age_seconds > self.heartbeat_timeout:
                    return False
            with self.db.engine.connect() as conn:
                conn.execute(text('SELECT 1'))
            return True
        except Exception:
            return False

    def cleanup(self) -> None:
        """Remove node from Node table.
        """
        try:
            sql = f'DELETE FROM {self.db.tables["Node"]} WHERE name = :name'
            self.db.execute(sql, {'name': self.node_name})
            logger.debug(f'Cleared {self.node_name} from {self.db.tables["Node"]}')
        except Exception as e:
            logger.debug(f'Failed to cleanup node {self.node_name}: {e}')


class TokenDistributor:
    """Handles token distribution and assignment tracking.
    """

    def __init__(self, node_name: str, db: DatabaseContext, coord_config: CoordinationConfig):
        """Initialize token distributor.

        Args:
            node_name: This node's name
            db: Database context
            coord_config: Coordination configuration
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
        self.on_tokens_added = None
        self.on_tokens_removed = None

    def distribute(self, lock_manager: 'LockManager', cluster: ClusterCoordinator) -> None:
        """Distribute tokens using minimal-move algorithm.

        Args:
            lock_manager: Lock manager for fetching active locks
            cluster: Cluster coordinator for active nodes
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
            sql = f'SELECT token_id, node FROM {self.db.tables["Token"]}'
            result = conn.execute(text(sql))
            current_assignments = {row[0]: row[1] for row in result}

        logger.debug(f'Current assignments: {len(current_assignments)} tokens across {len(set(current_assignments.values()))} nodes')

        locked_tokens = lock_manager.get_active_locks()

        for token_id, patterns in locked_tokens.items():
            matching_found = False
            for pattern in patterns:
                matching_nodes = [n for n in active_node_names if matches_pattern(n, pattern)]
                if matching_nodes:
                    matching_found = True
                    logger.debug(f'Token {token_id} locked to pattern "{pattern}" (matches {len(matching_nodes)} nodes)')
                    break

            if not matching_found:
                logger.warning(f'Token {token_id} lock failed: patterns {patterns} matched no active nodes {active_node_names}')

        new_assignments, tokens_moved = compute_minimal_move_distribution(
            self.total_tokens, active_node_names, current_assignments, locked_tokens, matches_pattern)

        logger.debug(f'Computed new assignments: {len(new_assignments)} tokens across {len(set(new_assignments.values()))} nodes, {tokens_moved} moves')

        if len(new_assignments) > self.total_tokens:
            logger.error(f'Token distribution produced {len(new_assignments)} assignments but total_tokens={self.total_tokens}')
            raise ValueError(f'Invalid token distribution: {len(new_assignments)} > {self.total_tokens}')

        assigned_nodes = set(new_assignments.values())
        invalid_nodes = assigned_nodes - set(active_node_names)
        if invalid_nodes:
            logger.error(f'Token distribution assigned to non-active nodes: {invalid_nodes}')
            raise ValueError(f'Tokens assigned to inactive nodes: {invalid_nodes}')

        with self.db.engine.connect() as conn:
            result = conn.execute(text(f'SELECT MAX(version) FROM {self.db.tables["Token"]}'))
            current_version = result.scalar() or 0
            new_version = current_version + 1

            logger.debug('Applying token distribution atomically (DELETE+INSERT within transaction, brief gap acceptable)')
            conn.execute(text(f'DELETE FROM {self.db.tables["Token"]}'))

            if new_assignments:
                insert_values = [
                    {'token_id': tid, 'node': node, 'version': new_version}
                    for tid, node in new_assignments.items()
                ]
                sql = f'INSERT INTO {self.db.tables["Token"]} (token_id, node, assigned_at, version) VALUES (:token_id, :node, NOW(), :version)'
                conn.execute(text(sql), insert_values)

            conn.commit()

        duration_ms = int((time.time() - start_time) * 1000)
        self._log_rebalance('distribution', len(active_node_names), len(active_node_names), tokens_moved, duration_ms)

        logger.info(f'Token distribution complete: {len(new_assignments)} tokens across {nodes_count} nodes, '
                    f'{tokens_moved} moved, v{new_version}, {duration_ms}ms')

    def get_my_tokens_versioned(self) -> tuple[set[int], int]:
        """Get token IDs and version assigned to this node.

        Returns
            Tuple of (token set, version)
        """
        sql = f'SELECT token_id, version FROM {self.db.tables["Token"]} WHERE node = :node'
        records = self.db.query(sql, {'node': self.node_name})

        if not records:
            return set(), 0

        tokens = {row[0] for row in records}
        version = records[0][1] if records else 0
        logger.debug(f'Node {self.node_name} owns {len(tokens)} tokens, version {version}')
        return tokens, version

    @log_duration('wait_for_distribution')
    def wait_for_distribution(self, timeout_sec: int = None, check_interval: float = 0.5) -> None:
        """Wait for initial token distribution to complete.

        Waits for tokens to be assigned to this specific node. For a late-joining node,
        this ensures we don't proceed until the leader has redistributed tokens to include
        this node.

        Args:
            timeout_sec: Maximum seconds to wait (defaults to config value)
            check_interval: How often to check for tokens
        """
        if timeout_sec is None:
            timeout_sec = self.token_distribution_timeout

        start = time.time()

        while time.time() - start < timeout_sec:
            sql = f"""
            SELECT COUNT(*)
            FROM {self.db.tables["Token"]}
            WHERE node = :node
            """
            result = self.db.query(sql, {'node': self.node_name})
            my_token_count = result[0][0] if result else 0

            if my_token_count > 0:
                logger.info(f'Token distribution complete: {my_token_count} tokens assigned to {self.node_name}')
                return

            time.sleep(check_interval)

        raise TimeoutError(f'Token distribution did not complete for {self.node_name} within {timeout_sec}s')

    def invoke_callback(self, callback: callable, token_ids: set[int], callback_name: str) -> None:
        """Invoke token callback with timing and error handling.

        Args:
            callback: Callback function to invoke
            token_ids: Set of token IDs to pass to callback
            callback_name: Name for logging
        """
        try:
            start = time.time()
            callback(token_ids)
            duration_ms = int((time.time() - start) * 1000)
            logger.info(f'{callback_name} completed in {duration_ms}ms for {len(token_ids)} tokens')
        except Exception as e:
            logger.error(f'{callback_name} callback failed: {e}')

    def _log_rebalance(self, reason: str, nodes_before: int, nodes_after: int, tokens_moved: int, duration_ms: int) -> None:
        """Log rebalance event to audit table.

        Args:
            reason: Rebalance trigger reason
            nodes_before: Node count before rebalance
            nodes_after: Node count after rebalance
            tokens_moved: Number of tokens moved
            duration_ms: Rebalance duration in milliseconds
        """
        sql = f"""
        INSERT INTO {self.db.tables["Rebalance"]}
        (triggered_at, trigger_reason, leader_node, nodes_before, nodes_after, tokens_moved, duration_ms)
        VALUES (NOW(), :reason, :leader, :before, :after, :moved, :duration)
        """
        self.db.execute(sql, {
            'reason': reason,
            'leader': self.node_name,
            'before': nodes_before,
            'after': nodes_after,
            'moved': tokens_moved,
            'duration': duration_ms
        })


class LockManager:
    """Handles task locking and coordination locks.
    """

    def __init__(self, node_name: str, db: DatabaseContext, coord_config: CoordinationConfig):
        """Initialize lock manager.

        Args:
            node_name: This node's name
            db: Database context
            coord_config: Coordination configuration
        """
        self.node_name = node_name
        self.db = db
        self.total_tokens = coord_config.total_tokens
        self.leader_lock_timeout = coord_config.leader_lock_timeout_sec
        self.stale_leader_lock_age = coord_config.stale_leader_lock_age_sec
        self.stale_rebalance_lock_age = coord_config.stale_rebalance_lock_age_sec

    def register_lock(self, task_id: Hashable, node_patterns: str | list[str], reason: str = None,
                      expires_in_days: int = None) -> None:
        """Register lock to pin task to specific node patterns.

        The task_id is stored in the database and later hashed to a token_id
        during distribution. The lock patterns are then applied to that token.

        Args:
            task_id: Task identifier to lock
            node_patterns: Single pattern or ordered list of fallback patterns
            reason: Human-readable reason for lock
            expires_in_days: Optional expiration in days
        """
        if isinstance(node_patterns, str):
            node_patterns = [node_patterns]

        expires_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
            days=expires_in_days) if expires_in_days else None

        sql = f"""
        INSERT INTO {self.db.tables["Lock"]}
        (task_id, node_patterns, reason, created_at, created_by, expires_at)
        VALUES (:task_id, :patterns, :reason, NOW(), :created_by, :expires_at)
        ON CONFLICT (task_id) DO UPDATE
        SET node_patterns = EXCLUDED.node_patterns,
            reason = EXCLUDED.reason,
            created_at = EXCLUDED.created_at,
            created_by = EXCLUDED.created_by,
            expires_at = EXCLUDED.expires_at
        """

        self.db.execute(sql, {
            'task_id': str(task_id),
            'patterns': json.dumps(node_patterns),
            'reason': reason,
            'created_by': self.node_name,
            'expires_at': expires_at
        })

        logger.debug(f'Registered lock: task {task_id} -> patterns {node_patterns}')

    @log_duration('register_locks_bulk')
    def register_locks_bulk(self, locks: list[tuple[Hashable, str | list[str], str]]) -> None:
        """Register multiple locks in batch.

        Each task_id is stored in the database and later hashed to a token_id
        during distribution. The lock patterns are then applied to those tokens.

        Args:
            locks: List of (task_id, node_patterns, reason) tuples
        """
        if not locks:
            return

        rows = []
        for task_id, node_patterns, reason in locks:
            if isinstance(node_patterns, str):
                node_patterns = [node_patterns]

            rows.append({
                'task_id': str(task_id),
                'patterns': json.dumps(node_patterns),
                'reason': reason,
                'created_by': self.node_name,
                'expires_at': None
            })

        sql = f"""
        INSERT INTO {self.db.tables["Lock"]}
        (task_id, node_patterns, reason, created_at, created_by, expires_at)
        VALUES (:task_id, :patterns, :reason, NOW(), :created_by, :expires_at)
        ON CONFLICT (task_id) DO UPDATE
        SET node_patterns = EXCLUDED.node_patterns,
            reason = EXCLUDED.reason,
            created_at = EXCLUDED.created_at,
            created_by = EXCLUDED.created_by,
            expires_at = EXCLUDED.expires_at
        """

        with self.db.engine.connect() as conn:
            for row in rows:
                conn.execute(text(sql), row)
            conn.commit()

        logger.debug(f'Registered {len(locks)} locks')

    def clear_locks_by_creator(self, creator: str) -> int:
        """Clear all locks created by specified node.

        Args:
            creator: Node name that created the locks

        Returns
            Number of locks removed
        """
        sql = f'DELETE FROM {self.db.tables["Lock"]} WHERE created_by = :creator'
        result = self.db.execute(sql, {'creator': creator})
        rows = result.rowcount
        logger.info(f'Cleared {rows} locks created by {creator}')
        return rows

    def clear_all_locks(self) -> int:
        """Clear all locks from system.

        Returns
            Number of locks removed
        """
        sql = f'DELETE FROM {self.db.tables["Lock"]}'
        result = self.db.execute(sql)
        rows = result.rowcount
        logger.warning(f'Cleared ALL {rows} locks from system')
        return rows

    def list_locks(self) -> list[dict]:
        """List all active locks.

        Returns
            List of lock records with task_id, patterns, and metadata
        """
        sql = f"""
        SELECT task_id, node_patterns, reason, created_at, created_by, expires_at
        FROM {self.db.tables["Lock"]}
        WHERE expires_at IS NULL OR expires_at > NOW()
        ORDER BY created_at DESC
        """
        result = self.db.query(sql)
        return [dict(row._mapping) for row in result]

    def get_active_locks(self) -> dict[int, list[str]]:
        """Get active locks as token_id -> patterns mapping.

        Dynamically hashes task_id to token_id using current total_tokens configuration.
        This allows locks to remain valid when total_tokens changes between runs.

        Returns
            Dictionary mapping token IDs to ordered pattern lists
        """
        with self.db.engine.connect() as conn:
            sql = f"""
            DELETE FROM {self.db.tables["Lock"]}
            WHERE expires_at IS NOT NULL AND expires_at < NOW()
            """
            conn.execute(text(sql))

            sql = f'SELECT task_id, node_patterns FROM {self.db.tables["Lock"]}'
            result = conn.execute(text(sql))
            records = [dict(row._mapping) for row in result]

            locked_tokens = {}
            for row in records:
                try:
                    patterns = row['node_patterns']
                    if isinstance(patterns, str):
                        patterns = json.loads(patterns)
                    if not isinstance(patterns, list):
                        logger.warning(f'Invalid lock pattern for task {row["task_id"]}: expected list, got {type(patterns).__name__}')
                        continue
                    token_id = task_to_token(row['task_id'], self.total_tokens)
                    locked_tokens[token_id] = patterns
                except (json.JSONDecodeError, TypeError, KeyError) as e:
                    logger.warning(f'Failed to parse lock pattern for task {row["task_id"]}: {e}')
                    continue

            conn.commit()
            return locked_tokens

    @contextlib.contextmanager
    def acquire_leader_lock(self, operation: str):
        """Context manager for leader lock acquisition.

        Args:
            operation: Operation name for logging

        Raises
            LockNotAcquired: If lock cannot be acquired
        """
        if not self._try_acquire_leader_lock(operation):
            raise LockNotAcquired(f'Another leader is performing {operation}')
        try:
            yield
        finally:
            self._release_leader_lock()

    @contextlib.contextmanager
    def acquire_rebalance_lock(self, started_by: str):
        """Context manager for rebalance lock acquisition.

        Args:
            started_by: Name of component starting rebalance

        Raises
            LockNotAcquired: If lock cannot be acquired
        """
        if not self._try_acquire_rebalance_lock(started_by):
            raise LockNotAcquired('Rebalance already in progress')
        try:
            yield
        finally:
            self._release_rebalance_lock()

    def _try_acquire_leader_lock(self, operation: str) -> bool:
        """Attempt to acquire leader lock with retry.

        Args:
            operation: Operation name

        Returns
            True if acquired, False otherwise
        """
        start_time = time.time()
        while time.time() - start_time < self.leader_lock_timeout:
            try:
                sql = f"""
                INSERT INTO {self.db.tables["LeaderLock"]} (singleton, node, acquired_at, operation)
                VALUES (1, :node, :acquired_at, :operation)
                ON CONFLICT (singleton) DO NOTHING
                """
                result = self.db.execute(sql, {
                    'node': self.node_name,
                    'acquired_at': datetime.datetime.now(datetime.timezone.utc),
                    'operation': operation
                })

                if result.rowcount > 0:
                    logger.info(f'Leader lock acquired by {self.node_name} for {operation}')
                    return True

                with self.db.engine.connect() as conn:
                    if self._check_and_clear_stale_lock(
                        conn, self.db.tables['LeaderLock'], self.stale_leader_lock_age, 'leader'
                    ):
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
            sql = f'DELETE FROM {self.db.tables["LeaderLock"]} WHERE node = :node'
            self.db.execute(sql, {'node': self.node_name})
            logger.debug(f'Leader lock released by {self.node_name}')
        except Exception as e:
            logger.error(f'Leader lock release failed: {e}')

    def _try_acquire_rebalance_lock(self, started_by: str) -> bool:
        """Attempt to acquire rebalance lock.

        Args:
            started_by: Component name

        Returns
            True if acquired, False otherwise
        """
        with self.db.engine.connect() as conn:
            self._check_and_clear_stale_lock(
                conn, self.db.tables['RebalanceLock'], self.stale_rebalance_lock_age, 'rebalance'
            )

        sql = f"""
        UPDATE {self.db.tables["RebalanceLock"]}
        SET in_progress = TRUE, started_at = :started_at, started_by = :started_by
        WHERE singleton = 1 AND in_progress = FALSE
        """
        result = self.db.execute(sql, {
            'started_at': datetime.datetime.now(datetime.timezone.utc),
            'started_by': started_by
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
        UPDATE {self.db.tables["RebalanceLock"]}
        SET in_progress = FALSE, started_at = NULL, started_by = NULL
        WHERE singleton = 1
        """
        self.db.execute(sql)
        logger.debug('Rebalance lock released')

    def _check_and_clear_stale_lock(self, conn, table: str, age_threshold: int, lock_type: str) -> bool:
        """Check for and clear stale locks.

        Args:
            conn: Database connection
            table: Table name
            age_threshold: Age threshold in seconds
            lock_type: Lock type name

        Returns
            True if stale lock cleared, False otherwise
        """
        if lock_type == 'leader':
            sql = f"""
            SELECT node, EXTRACT(EPOCH FROM (NOW() - acquired_at)) as age_seconds
            FROM {table}
            WHERE singleton = 1
            """
        else:
            sql = f"""
            SELECT started_by, EXTRACT(EPOCH FROM (NOW() - started_at)) as age_seconds
            FROM {table}
            WHERE singleton = 1 AND in_progress = TRUE
            """

        result = conn.execute(text(sql))
        lock_row = result.first()

        if lock_row and lock_row[1] is not None and lock_row[1] > age_threshold:
            holder = lock_row[0]
            age_seconds = lock_row[1]
            logger.warning(f'Stale {lock_type} lock detected (age: {age_seconds}s, holder: {holder}), forcing release')

            if lock_type == 'leader':
                conn.execute(text(f'DELETE FROM {table} WHERE singleton = 1'))
            else:
                conn.execute(text(f"""
                    UPDATE {table}
                    SET in_progress = FALSE, started_at = NULL, started_by = NULL
                    WHERE singleton = 1
                """))
            conn.commit()
            return True

        return False


class TaskManager:
    """Handles task claiming and audit logging.
    """

    def __init__(self, node_name: str, db: DatabaseContext, date: datetime.date, total_tokens: int):
        """Initialize task manager.

        Args:
            node_name: This node's name
            db: Database context
            date: Processing date
            total_tokens: Total token count for hashing
        """
        self.node_name = node_name
        self.db = db
        self.date = date
        self.total_tokens = total_tokens
        self._tasks = []

    def can_claim(self, task: Task, my_tokens: set[int]) -> bool:
        """Check if task can be claimed based on token ownership.

        Args:
            task: Task to check
            my_tokens: Set of tokens owned by this node

        Returns
            True if claimable, False otherwise
        """
        token_id = task_to_token(task.id, self.total_tokens)
        can_claim = token_id in my_tokens

        if not can_claim:
            logger.debug(f'Cannot claim task {task.id} (token {token_id} not owned by {self.node_name})')

        return can_claim

    def add_task(self, task: Task) -> None:
        """Add task to processing queue and set claim.

        Args:
            task: Task to add
        """
        self._tasks.append((task, datetime.datetime.now(datetime.timezone.utc)))
        self.set_claim(task.id)

    def set_claim(self, item) -> None:
        """Claim an item or items and publish to database.

        Args:
            item: Item ID or iterable of item IDs
        """
        with self.db.engine.connect() as conn:
            if isinstance(item, Iterable) and not isinstance(item, str):
                rows = [{'node': self.node_name, 'created_on': datetime.datetime.now(datetime.timezone.utc), 'item': str(i)} for i in item]
                for row in rows:
                    sql = f'INSERT INTO {self.db.tables["Claim"]} (node, item, created_on) VALUES (:node, :item, :created_on) ON CONFLICT DO NOTHING'
                    conn.execute(text(sql), row)
            else:
                sql = f'INSERT INTO {self.db.tables["Claim"]} (node, item, created_on) VALUES (:node, :item, :created_on) ON CONFLICT DO NOTHING'
                conn.execute(text(sql), {'node': self.node_name, 'item': str(item), 'created_on': datetime.datetime.now(datetime.timezone.utc)})
            conn.commit()

    def write_audit(self) -> None:
        """Write accumulated tasks to audit table.
        """
        if not self._tasks:
            return

        with self.db.engine.connect() as conn:
            for task, _ in self._tasks:
                sql = f'INSERT INTO {self.db.tables["Audit"]} (date, node, item, created_on) VALUES (:date, :node, :item, now())'
                conn.execute(text(sql), {
                    'date': self.date,
                    'node': self.node_name,
                    'item': str(task.id)
                })
            conn.commit()

        logger.debug(f'Flushed {len(self._tasks)} task to {self.db.tables["Audit"]}')
        self._tasks.clear()

    def get_audit(self) -> list[dict]:
        """Get audit records for current date.

        Returns
            List of audit records
        """
        sql = f'SELECT node, item FROM {self.db.tables["Audit"]} WHERE date = :date'
        result = self.db.query(sql, {'date': self.date})
        return [{'node': row[0], 'item': row[1]} for row in result]

    def cleanup(self) -> None:
        """Cleanup Check and Claim tables.
        """
        try:
            with self.db.engine.connect() as conn:
                sql = f'DELETE FROM {self.db.tables["Check"]} WHERE node = :node'
                conn.execute(text(sql), {'node': self.node_name})

                sql = f'DELETE FROM {self.db.tables["Claim"]} WHERE node = :node'
                conn.execute(text(sql), {'node': self.node_name})

                conn.commit()
            logger.debug(f'Cleaned {self.node_name} from Check and Claim tables')
        except Exception as e:
            logger.debug(f'Failed to cleanup tasks for {self.node_name}: {e}')


# ============================================================
# MONITORS
# ============================================================

class Monitor:
    """Base class for background monitoring threads.
    """

    def __init__(self, name: str, interval: float, shutdown_event: threading.Event):
        """Initialize monitor.

        Args:
            name: Monitor name
            interval: Check interval in seconds
            shutdown_event: Event to signal shutdown
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
            name=self.name
        )
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

    def __init__(self, cluster: ClusterCoordinator, shutdown_event: threading.Event):
        """Initialize heartbeat monitor.

        Args:
            cluster: Cluster coordinator
            shutdown_event: Shutdown event
        """
        super().__init__(f'heartbeat-{cluster.node_name}', cluster.heartbeat_interval, shutdown_event)
        self.cluster = cluster
        self.db = cluster.db
        self.node_name = cluster.node_name

    def check(self) -> None:
        """Send heartbeat update.
        """
        sql = f"""
        UPDATE {self.db.tables["Node"]}
        SET last_heartbeat = :heartbeat
        WHERE name = :name
        """
        heartbeat_time = datetime.datetime.now(datetime.timezone.utc)
        self.db.execute(sql, {'heartbeat': heartbeat_time, 'name': self.node_name})
        self.cluster.last_heartbeat_sent = heartbeat_time
        logger.debug(f'Heartbeat sent by {self.node_name}')


class HealthMonitor(Monitor):
    """Monitors node health by checking heartbeat age and database connectivity.
    """

    def __init__(self, cluster: ClusterCoordinator, event_queue: EventQueue, shutdown_event: threading.Event):
        """Initialize health monitor.

        Args:
            cluster: Cluster coordinator
            event_queue: Event queue for publishing events
            shutdown_event: Shutdown event
        """
        super().__init__(f'health-{cluster.node_name}', cluster.health_check_interval, shutdown_event)
        self.cluster = cluster
        self.event_queue = event_queue

    def check(self) -> None:
        """Check heartbeat age and database connectivity.
        """
        if self.cluster.last_heartbeat_sent:
            age_seconds = (datetime.datetime.now(datetime.timezone.utc) - self.cluster.last_heartbeat_sent).total_seconds()
            if age_seconds > self.cluster.heartbeat_timeout:
                logger.error(f'Heartbeat thread appears dead (age: {age_seconds:.1f}s > timeout: {self.cluster.heartbeat_timeout}s)')
                self.event_queue.publish('node_unhealthy', {'reason': 'heartbeat_timeout', 'age_seconds': age_seconds})
                return

        with self.cluster.db.engine.connect() as conn:
            conn.execute(text('SELECT 1'))
            conn.rollback()


class TokenRefreshMonitor(Monitor):
    """Detects token reassignments and invokes callbacks for followers.
    """

    def __init__(self, node_name: str, db: DatabaseContext, tokens: TokenDistributor,
                 state_machine: JobStateMachine, event_queue: EventQueue, shutdown_event: threading.Event,
                 cluster: 'ClusterCoordinator'):
        """Initialize token refresh monitor.

        Args:
            node_name: This node's name
            db: Database context
            tokens: Token distributor
            state_machine: State machine instance
            event_queue: Event queue for publishing events
            shutdown_event: Shutdown event
            cluster: Cluster coordinator for accessing heartbeat_timeout
        """
        super().__init__(
            f'token-refresh-{node_name}',
            tokens.token_refresh_initial,
            shutdown_event
        )
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
        """Check for token reassignments and invoke callbacks for leadership/token changes.
        """
        elapsed = time.time() - self.start_time
        self.interval = self.tokens.token_refresh_initial if elapsed < 300 else self.tokens.token_refresh_steady

        try:
            with self.db.engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT name FROM {self.db.tables["Node"]}
                    WHERE last_heartbeat > NOW() - INTERVAL '{self.cluster.heartbeat_timeout} seconds'
                    ORDER BY created_on ASC, name ASC
                """))
                nodes = list(result)
                elected_leader = nodes[0][0] if nodes else None

            if elected_leader == self.node_name and self.state_machine.state == JobState.RUNNING_FOLLOWER:
                logger.info('Detected leader promotion')
                self.event_queue.publish('leadership_promoted', {})
            elif elected_leader != self.node_name and self.state_machine.state == JobState.RUNNING_LEADER:
                logger.warning('Detected leader demotion (older node rejoined)')
                self.event_queue.publish('leadership_demoted', {})
        except Exception as e:
            logger.warning(f'Failed to check leader status: {e}')

        with self.db.engine.connect() as conn:
            sql = f'SELECT token_id, version FROM {self.db.tables["Token"]} WHERE node = :node'
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
                if self.tokens.on_tokens_added:
                    self.tokens.invoke_callback(self.tokens.on_tokens_added, new_tokens.copy(), 'on_tokens_added')
                self.initial_callback_sent = True
                self.start_time = time.time()
            elif new_version != self.tokens.token_version:
                added = new_tokens - self.tokens.my_tokens
                removed = self.tokens.my_tokens - new_tokens

                logger.warning(f'Token rebalance detected: v{self.tokens.token_version}->v{new_version}, '
                               f'+{len(added)} -{len(removed)} tokens')

                self.tokens.my_tokens = new_tokens
                self.tokens.token_version = new_version

                if removed and self.tokens.on_tokens_removed:
                    self.tokens.invoke_callback(self.tokens.on_tokens_removed, removed, 'on_tokens_removed')

                if added and self.tokens.on_tokens_added:
                    self.tokens.invoke_callback(self.tokens.on_tokens_added, added, 'on_tokens_added')

                self.start_time = time.time()

            self.check_count += 1
            if self.check_count % 10 == 0:
                logger.debug(f'Token refresh #{self.check_count}: {len(self.tokens.my_tokens)} tokens, v{self.tokens.token_version}')


class DeadNodeMonitor(Monitor):
    """Leader-only monitor that detects and removes dead nodes.
    """

    def __init__(self, node_name: str, db: DatabaseContext, cluster: ClusterCoordinator,
                 locks: LockManager, event_queue: EventQueue, shutdown_event: threading.Event):
        """Initialize dead node monitor.

        Args:
            node_name: This node's name
            db: Database context
            cluster: Cluster coordinator
            locks: Lock manager
            event_queue: Event queue for publishing events
            shutdown_event: Shutdown event
        """
        super().__init__(
            f'dead-node-{node_name}',
            cluster.dead_node_check_interval,
            shutdown_event
        )
        self.node_name = node_name
        self.db = db
        self.cluster = cluster
        self.locks = locks
        self.event_queue = event_queue

    def check(self) -> None:
        """Detect and remove dead nodes, invoke callback.
        """
        sql = f"""
        SELECT name
        FROM {self.db.tables["Node"]}
        WHERE last_heartbeat <= NOW() - INTERVAL '{self.cluster.heartbeat_timeout} seconds' OR last_heartbeat IS NULL
        """

        with self.db.engine.connect() as conn:
            result = conn.execute(text(sql))
            dead_nodes = [row[0] for row in result]
            conn.rollback()

        if dead_nodes:
            logger.warning(f'Detected dead nodes: {dead_nodes}')
            try:
                with self.locks.acquire_rebalance_lock('dead_node_monitor'):
                    with self.db.engine.connect() as conn:
                        for node in dead_nodes:
                            conn.execute(text(f'DELETE FROM {self.db.tables["Node"]} WHERE name = :name'), {'name': node})
                            conn.execute(text(f'DELETE FROM {self.db.tables["Claim"]} WHERE node = :node'), {'node': node})
                            conn.execute(text(f'DELETE FROM {self.db.tables["Lock"]} WHERE created_by = :node'), {'node': node})
                            logger.info(f'Removed dead node and cleaned up locks: {node}')
                        conn.commit()
                self.event_queue.publish('dead_nodes_detected', {'nodes': dead_nodes})
            except LockNotAcquired:
                logger.debug('Rebalance already in progress, skipping dead node cleanup')


class RebalanceMonitor(Monitor):
    """Leader-only monitor that triggers rebalancing on membership changes.
    """

    def __init__(self, node_name: str, db: DatabaseContext, cluster: ClusterCoordinator,
                 event_queue: EventQueue, shutdown_event: threading.Event, initial_node_count: int = None):
        """Initialize rebalance monitor.

        Args:
            node_name: This node's name
            db: Database context
            cluster: Cluster coordinator
            event_queue: Event queue for publishing events
            shutdown_event: Shutdown event
            initial_node_count: Node count at distribution time (if None, queries current count)
        """
        super().__init__(
            f'rebalance-{node_name}',
            cluster.rebalance_check_interval,
            shutdown_event
        )
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
        """Check for membership changes and invoke callback.
        """
        result = self.db.query(self.cluster.active_nodes_sql)
        current_count = len(result)

        logger.debug(f'Rebalance check: last={self.last_node_count}, current={current_count}')

        if current_count != self.last_node_count:
            logger.info(f'Node count changed: {self.last_node_count} -> {current_count}')
            self.event_queue.publish('membership_changed', {
                'previous_count': self.last_node_count,
                'current_count': current_count
            })
            self.last_node_count = current_count


class CoordinationMonitor(Monitor):
    """Periodically processes coordination events.
    """

    def __init__(self, job: 'Job', interval: float = 1.0):
        """Initialize coordination monitor.

        Args:
            job: Job instance to coordinate
            interval: Check interval in seconds
        """
        super().__init__('coordination', interval, job._shutdown_event)
        self.job = job

    def check(self) -> None:
        """Process pending coordination events.
        """
        self.job._coordinate_state_transitions()


# ============================================================
# JOB CLASS
# ============================================================

class Job:
    """Job synchronization manager with state machine-based coordination.

    Lifecycle Phases:
    1. INITIALIZING: Configure instance variables
    2. CLUSTER_FORMING: Register node and start heartbeat
    3. ELECTING: Elect leader (oldest node wins)
    4. DISTRIBUTING: Leader distributes tokens
    5. RUNNING_LEADER/RUNNING_FOLLOWER: Normal operation
    6. SHUTTING_DOWN: Stop threads, cleanup database

    State transitions are validated and callbacks handle service orchestration.
    Monitors publish events instead of directly triggering state changes.
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
        on_tokens_added: callable = None,
        on_tokens_removed: callable = None,
    ):
        """Initialize job synchronization manager.

        Args:
            node_name: Unique identifier for this node
            coordination_config: CoordinationConfig for coordination settings and database connection (None disables coordination)
            date: Processing date (defaults to today)
            wait_on_enter: Seconds to wait for cluster formation (default 120)
            wait_on_exit: Seconds to wait before cleanup (default 0)
            lock_provider: Callback(job) to register task locks during __enter__
            clear_existing_locks: If True, clear locks created by this node before invoking lock_provider
            on_tokens_added: Callback(token_ids: set[int]) invoked when tokens assigned
            on_tokens_removed: Callback(token_ids: set[int]) invoked when tokens removed
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

        self._created_on = ensure_timezone_aware(datetime.datetime.now(datetime.timezone.utc), 'node created_on')
        self._shutdown_event = threading.Event()
        self._lock_provider = lock_provider
        self._clear_existing_locks = clear_existing_locks
        self._event_queue = EventQueue()

        self._coordination_enabled = coordination_config is not None

        if coordination_config is not None:
            coord_cfg = coordination_config
            self.db = DatabaseContext(coord_cfg)
            self.cluster = ClusterCoordinator(node_name, self.db, coord_cfg, self._created_on)
            self.tokens = TokenDistributor(node_name, self.db, coord_cfg)
            self.locks = LockManager(node_name, self.db, coord_cfg)
            self.tasks = TaskManager(node_name, self.db, date, coord_cfg.total_tokens)

            self.tokens.on_tokens_added = on_tokens_added
            self.tokens.on_tokens_removed = on_tokens_removed

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

        self.state_machine.on_enter(JobState.CLUSTER_FORMING, self._on_enter_cluster_forming)
        self.state_machine.on_enter(JobState.ELECTING, self._on_enter_electing)
        self.state_machine.on_enter(JobState.DISTRIBUTING, self._on_enter_distributing)
        self.state_machine.on_enter(JobState.RUNNING_LEADER, self._on_enter_running_leader)
        self.state_machine.on_exit(JobState.RUNNING_LEADER, self._on_exit_running_leader)
        self.state_machine.on_enter(JobState.RUNNING_FOLLOWER, self._on_enter_running_follower)
        self.state_machine.on_enter(JobState.SHUTTING_DOWN, self._on_enter_shutting_down)

        if self._coordination_enabled:
            coord_monitor = CoordinationMonitor(self, interval=1.0)
            self._start_monitor('coordination', coord_monitor)

        if not self._coordination_enabled and (on_tokens_added or on_tokens_removed):
            logger.warning('Token callbacks provided but coordination_enabled=False - callbacks will never fire')

    def _start_monitor(self, name: str, monitor) -> None:
        """Start a monitor if not already running.

        Args:
            name: Monitor name
            monitor: Monitor instance
        """
        if name in self._monitors:
            logger.debug(f'Monitor {name} already running')
            return
        self._monitors[name] = monitor
        monitor.start()
        logger.info(f'Started monitor: {name}')

    def _stop_monitor(self, name: str) -> None:
        """Stop and remove a monitor.

        Args:
            name: Monitor name
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

        health_monitor = HealthMonitor(cluster=self.cluster,
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

        Performs leader election and stores the result for use in DISTRIBUTING state.
        """
        try:
            self._elected_leader = self.cluster.elect_leader()
            logger.info(f'Leader election complete: {self._elected_leader}')
        except Exception as e:
            logger.error(f'Failed to elect leader: {e}')
            self.state_machine.transition_to(JobState.ERROR)

    def _wait_for_minimum_nodes(self) -> bool:
        """Wait for minimum nodes before token distribution.

        Returns
            True if minimum nodes reached, False if shutdown requested

        Raises
            TimeoutError: If minimum nodes not reached within timeout
        """
        start = time.time()
        timeout = self.tokens.token_distribution_timeout
        while time.time() - start < timeout:
            active_count = len(self.cluster.get_active_nodes())
            if active_count >= self.tokens.minimum_nodes:
                logger.info(f'Minimum nodes reached: {active_count}/{self.tokens.minimum_nodes}')
                return True
            if self._shutdown_event.wait(timeout=1.0):
                logger.info('Shutdown requested during minimum nodes wait')
                return False
            logger.debug(f'Waiting for minimum nodes: {active_count}/{self.tokens.minimum_nodes}')

        raise TimeoutError(f'Minimum nodes ({self.tokens.minimum_nodes}) not reached within {timeout}s')

    def _on_enter_distributing(self) -> None:
        """Entry action for DISTRIBUTING state.

        Uses the leader elected in ELECTING state to perform token distribution.
        """
        try:
            if self._elected_leader is None:
                raise RuntimeError('No leader elected before distribution')

            if self._elected_leader == self.node_name:
                logger.info('This node is the leader, performing token distribution')

                if not self._wait_for_minimum_nodes():
                    return

                nodes_at_distribution = self.cluster.get_active_nodes()
                self._nodes_at_distribution = len(nodes_at_distribution)
                logger.info(f'Node count at distribution: {self._nodes_at_distribution}')
                self._distribute_tokens_safe()
            else:
                logger.info(f'Follower node detected, waiting for leader {self._elected_leader} to distribute tokens')
        except Exception as e:
            logger.error(f'Failed during token distribution: {e}')
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
            cluster=self.cluster
        )
        self._start_monitor('token_refresh', token_refresh)

        dead_node_monitor = DeadNodeMonitor(
            node_name=self.node_name,
            db=self.db,
            cluster=self.cluster,
            locks=self.locks,
            event_queue=self._event_queue,
            shutdown_event=self._shutdown_event
        )
        self._start_monitor('dead_node', dead_node_monitor)

        rebalance_monitor = RebalanceMonitor(
            node_name=self.node_name,
            db=self.db,
            cluster=self.cluster,
            event_queue=self._event_queue,
            shutdown_event=self._shutdown_event,
            initial_node_count=self._nodes_at_distribution
        )
        self._start_monitor('rebalance', rebalance_monitor)

    def _on_exit_running_leader(self) -> None:
        """Exit action for RUNNING_LEADER state.

        Stops leader-only monitors (DeadNodeMonitor, RebalanceMonitor).
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
            cluster=self.cluster
        )
        self._start_monitor('token_refresh', token_refresh)

    def _on_enter_shutting_down(self) -> None:
        """Entry action for SHUTTING_DOWN state.
        """
        self._shutdown_event.set()

    def _coordinate_state_transitions(self) -> None:
        """Central coordinator for all state transitions.

        Processes events from monitor event queue and executes transitions.
        """
        events = self._event_queue.consume_all()

        if events:
            logger.info(f'Processing {len(events)} coordination events: {[e.type for e in events]}')

        for event in events:
            logger.info(f'Event: {event.type}', extra={
                'event_type': event.type,
                'event_data': event.data,
                'current_state': self.state_machine.state.value,
                'node_name': self.node_name
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
                    self._distribute_tokens_safe()

            elif event.type == 'membership_changed':
                if self.state_machine.is_leader():
                    data = event.data
                    logger.info(f'Membership: {data.get("previous_count")} → {data.get("current_count")}')
                    self._distribute_tokens_safe()

    def __enter__(self):
        """Enter context and perform coordination setup.
        """
        if not self._coordination_enabled:
            logger.info(f'Starting {self.node_name} in standalone mode (no coordination)')
            return self

        logger.info(f'Starting {self.node_name} in coordination mode')

        try:
            if not self.state_machine.transition_to(JobState.CLUSTER_FORMING):
                logger.error('Failed to transition to CLUSTER_FORMING')
                raise RuntimeError('Invalid state transition to CLUSTER_FORMING')

            target_nodes = self.tokens.minimum_nodes
            logger.info(f'Waiting up to {self._wait_on_enter}s for cluster formation (target: {target_nodes} nodes)...')
            start_wait = time.time()
            while time.time() - start_wait < self._wait_on_enter:
                self._nodes = self.cluster.get_active_nodes()
                if len(self._nodes) >= target_nodes:
                    logger.info(f'Cluster formed with {len(self._nodes)} nodes (target: {target_nodes}), proceeding')
                    break
                if self._shutdown_event.wait(timeout=1.0):
                    logger.info('Shutdown requested during cluster wait')
                    return self
            else:
                logger.info(f'Cluster formation wait complete after {self._wait_on_enter}s with {len(self._nodes)} node(s) (target: {target_nodes})')
            logger.info(f'Active nodes: {[n["name"] for n in self._nodes]}')

            if not self.state_machine.transition_to(JobState.ELECTING):
                logger.error('Failed to transition to ELECTING')
                raise RuntimeError('Invalid state transition to ELECTING')

            if not self.state_machine.transition_to(JobState.DISTRIBUTING):
                logger.error('Failed to transition to DISTRIBUTING')
                raise RuntimeError('Invalid state transition to DISTRIBUTING')

            if self.state_machine.state == JobState.ERROR:
                logger.error('Node in ERROR state after DISTRIBUTING transition, skipping wait_for_distribution')
                raise RuntimeError('Failed to complete token distribution')

            logger.info('Waiting for token distribution...')
            self.tokens.wait_for_distribution()

            self.tokens.my_tokens, self.tokens.token_version = self.tokens.get_my_tokens_versioned()
            logger.info(f'Node {self.node_name} assigned {len(self.tokens.my_tokens)} tokens, version {self.tokens.token_version}')

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

    def __exit__(self, exc_ty, exc_val, tb):
        """Exit context and cleanup coordination resources.
        """
        logger.debug(f'Exiting {self.node_name} context')

        if exc_ty:
            logger.error(exc_val)

        if self._coordination_enabled:
            if self.state_machine.state != JobState.SHUTTING_DOWN:
                self.state_machine.transition_to(JobState.SHUTTING_DOWN)

            for name, monitor in self._monitors.items():
                if monitor.thread and monitor.thread.is_alive():
                    logger.debug(f'Joining {name} thread...')
                    monitor.thread.join(timeout=10)
                    if monitor.thread.is_alive():
                        logger.warning(f'{name} thread did not stop within timeout')

            if self._wait_on_exit:
                logger.debug(f'Sleeping {self._wait_on_exit} seconds...')
                time.sleep(self._wait_on_exit)

            self._cleanup()

        if self.db is not None:
            self.db.dispose()

    @property
    def nodes(self):
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

    def set_claim(self, item):
        """Claim an item or items and publish to database.

        Args:
            item: Item ID or iterable of item IDs
        """
        if self.tasks is not None:
            self.tasks.set_claim(item)

    def add_task(self, task: Task):
        """Add task to processing queue if claimable in coordination mode.

        Args:
            task: Task to add
        """
        if self.tasks is None:
            return

        if self._coordination_enabled and not self.can_claim_task(task):
            logger.debug(f'Task {task.id} rejected (token not owned)')
            return

        self.tasks.add_task(task)

    def write_audit(self) -> None:
        """Write accumulated tasks to audit table.
        """
        if self.tasks is not None:
            self.tasks.write_audit()

    def get_audit(self) -> list[dict]:
        """Get audit records for current date.

        Returns
            List of audit records
        """
        if not self._coordination_enabled:
            return []
        return self.tasks.get_audit()

    def can_claim_task(self, task: Task) -> bool:
        """Check if this node can claim the given task.

        Args:
            task: Task to check

        Returns
            True if claimable, False otherwise
        """
        if not self._coordination_enabled:
            return True

        if not self.state_machine.can_claim_task():
            logger.debug(f'Cannot claim task {task.id} in state {self.state_machine.state.value}')
            return False

        return self.tasks.can_claim(task, self.tokens.my_tokens)

    def task_to_token(self, task_id: Hashable) -> int:
        """Map task ID to token ID using consistent hashing.

        Convenience wrapper around module-level function for API consistency.

        Args:
            task_id: Task identifier

        Returns
            Token ID
        """
        if self.tokens is None:
            return 0
        return task_to_token(task_id, self.tokens.total_tokens)

    def get_task_ids_for_token(self, token_id: int, all_task_ids: list[Hashable]) -> list[Hashable]:
        """Get task IDs that hash to the given token.

        Args:
            token_id: Token ID to match
            all_task_ids: Complete list of possible task IDs

        Returns
            List of task IDs that hash to this token
        """
        if self.tokens is None:
            return []
        return [task_id for task_id in all_task_ids if task_to_token(task_id, self.tokens.total_tokens) == token_id]

    def get_my_task_ids(self, all_task_ids: list[Hashable]) -> list[Hashable]:
        """Get all task IDs owned by this node.

        Args:
            all_task_ids: Complete list of possible task IDs

        Returns
            List of task IDs this node currently owns
        """
        if self.tokens is None:
            return []
        return [task_id for task_id in all_task_ids if task_to_token(task_id, self.tokens.total_tokens) in self.tokens.my_tokens]

    def register_lock(self, task_id: Hashable, node_patterns: str | list[str], reason: str = None,
                      expires_in_days: int = None) -> None:
        """Register lock to pin task to specific node patterns.

        Args:
            task_id: Task identifier to lock
            node_patterns: Single pattern or ordered list of fallback patterns
            reason: Human-readable reason for lock
            expires_in_days: Optional expiration in days
        """
        if self.locks is not None:
            self.locks.register_lock(task_id, node_patterns, reason, expires_in_days)

    def register_locks_bulk(self, locks: list[tuple[Hashable, str | list[str], str]]) -> None:
        """Register multiple locks in batch.

        Args:
            locks: List of (task_id, node_patterns, reason) tuples
        """
        if self.locks is not None:
            self.locks.register_locks_bulk(locks)

    def clear_locks_by_creator(self, creator: str) -> int:
        """Clear all locks created by specified node.

        Args:
            creator: Node name that created the locks

        Returns
            Number of locks removed
        """
        if self.locks is not None:
            return self.locks.clear_locks_by_creator(creator)
        return 0

    def clear_all_locks(self) -> int:
        """Clear all locks from system.

        Returns
            Number of locks removed
        """
        if self.locks is not None:
            return self.locks.clear_all_locks()
        return 0

    def list_locks(self) -> list[dict]:
        """List all active locks.

        Returns
            List of lock records
        """
        if not self._coordination_enabled:
            return []
        return self.locks.list_locks()

    def get_coordination_status(self) -> dict:
        """Get current coordination state for debugging.

        Returns
            Dictionary with coordination status information
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
            'monitors': list(self._monitors.keys())
        }

    def am_i_healthy(self) -> bool:
        """Check if node is healthy.

        Returns
            True if healthy, False otherwise
        """
        if not self._coordination_enabled:
            return True
        return self.cluster.am_i_healthy()

    def am_i_leader(self) -> bool:
        """Check if this node should be the elected leader based on database state.

        During initialization states, returns False without database query.
        During running states, queries database to detect leadership changes.

        Returns
            True if this node should be leader (oldest active node), False otherwise
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
        """Get list of active nodes.

        Returns
            List of node dicts
        """
        if not self._coordination_enabled:
            return []
        return self.cluster.get_active_nodes()

    def get_dead_nodes(self) -> list[str]:
        """Get list of dead nodes.

        Returns
            List of dead node names
        """
        if not self._coordination_enabled:
            return []
        return self.cluster.get_dead_nodes()

    def can_distribute_tokens(self) -> bool:
        """Check if token distribution is allowed in current state.

        Returns
            True if in DISTRIBUTING or RUNNING_LEADER state
        """
        return self.state_machine.state in {
            JobState.DISTRIBUTING,
            JobState.RUNNING_LEADER
        }

    def _distribute_tokens_safe(self) -> None:
        """Leader distributes tokens with rebalance and leader lock protection.

        Acquires locks in order to prevent deadlock:
        1. rebalance_lock - coarse-grained "who's rebalancing"
        2. leader_lock - fine-grained "which leader is distributing"
        """
        if self.locks is None or self.tokens is None or self.cluster is None:
            return

        try:
            with self.locks.acquire_rebalance_lock('token_distribution'):
                with self.locks.acquire_leader_lock('distribute'):
                    self.tokens.distribute(self.locks, self.cluster)
        except LockNotAcquired as e:
            logger.debug(f'Could not acquire lock for token distribution: {e}')
        except Exception as e:
            logger.error(f'Token distribution failed: {e}', exc_info=True)
            if self.state_machine.is_initializing():
                logger.error('Distribution failed during initialization, transitioning to ERROR state')
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
