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
from collections.abc import Hashable, Iterable
from copy import deepcopy
from enum import Enum
from functools import total_ordering
from types import ModuleType

from sqlalchemy import create_engine, text

from jobsync.config import CoordinationConfig
from jobsync.schema import ensure_database_ready, get_table_names

logger = logging.getLogger(__name__)

__all__ = ['Job', 'Task', 'LockNotAcquired']


# ============================================================
# UTILITY FUNCTIONS
# ============================================================

def build_connection_string(config: ModuleType) -> str:
    """Build PostgreSQL connection string from config.
    """
    sql_config = config.sync.sql
    return (
        f'postgresql+psycopg://{sql_config.user}:{sql_config.passwd}'
        f'@{sql_config.host}:{sql_config.port}/{sql_config.dbname}'
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


def load_coordination_config(config: ModuleType, coordination_config: CoordinationConfig = None) -> dict:
    """Load and normalize coordination configuration from config or CoordinationConfig.
    """
    if coordination_config:
        return {
            'enabled': coordination_config.enabled,
            'total_tokens': coordination_config.total_tokens,
            'heartbeat_interval_sec': coordination_config.heartbeat_interval_sec,
            'heartbeat_timeout_sec': coordination_config.heartbeat_timeout_sec,
            'rebalance_check_interval_sec': coordination_config.rebalance_check_interval_sec,
            'dead_node_check_interval_sec': coordination_config.dead_node_check_interval_sec,
            'token_refresh_initial_interval_sec': coordination_config.token_refresh_initial_interval_sec,
            'token_refresh_steady_interval_sec': coordination_config.token_refresh_steady_interval_sec,
            'locks_enabled': coordination_config.locks_enabled,
            'leader_lock_timeout_sec': coordination_config.leader_lock_timeout_sec,
            'health_check_interval_sec': coordination_config.health_check_interval_sec,
            'stale_leader_lock_age_sec': coordination_config.stale_leader_lock_age_sec,
            'stale_rebalance_lock_age_sec': coordination_config.stale_rebalance_lock_age_sec,
        }

    coord = config.sync.coordination
    return {
        'enabled': getattr(coord, 'enabled', True),
        'total_tokens': getattr(coord, 'total_tokens', 10000),
        'heartbeat_interval_sec': getattr(coord, 'heartbeat_interval_sec', 5),
        'heartbeat_timeout_sec': getattr(coord, 'heartbeat_timeout_sec', 15),
        'rebalance_check_interval_sec': getattr(coord, 'rebalance_check_interval_sec', 30),
        'dead_node_check_interval_sec': getattr(coord, 'dead_node_check_interval_sec', 10),
        'token_refresh_initial_interval_sec': getattr(coord, 'token_refresh_initial_interval_sec', 5),
        'token_refresh_steady_interval_sec': getattr(coord, 'token_refresh_steady_interval_sec', 30),
        'locks_enabled': getattr(coord, 'locks_enabled', True),
        'leader_lock_timeout_sec': getattr(coord, 'leader_lock_timeout_sec', 30),
        'health_check_interval_sec': getattr(coord, 'health_check_interval_sec', 30),
        'stale_leader_lock_age_sec': getattr(coord, 'stale_leader_lock_age_sec', 300),
        'stale_rebalance_lock_age_sec': getattr(coord, 'stale_rebalance_lock_age_sec', 300),
    }


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


def compute_minimal_move_distribution(
    total_tokens: int,
    active_nodes: list[str],
    current_assignments: dict[int, str],
    locked_tokens: dict[int, str | list[str]],
    pattern_matcher: callable
) -> tuple[dict[int, str], int]:
    """Compute token distribution using minimal-move algorithm.

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
    nodes_count = len(active_nodes)

    if nodes_count == 0:
        return {}, 0

    locked_assignments = {}
    for token_id, patterns in locked_tokens.items():
        if isinstance(patterns, str):
            patterns = [patterns]
        for pattern in patterns:
            matching_nodes = [n for n in active_nodes if pattern_matcher(n, pattern)]
            if matching_nodes:
                locked_assignments[token_id] = matching_nodes[0]
                break

    unlocked_token_ids = [t for t in range(total_tokens) if t not in locked_tokens]
    target_per_node = len(unlocked_token_ids) // nodes_count
    remainder = len(unlocked_token_ids) % nodes_count

    node_token_counts = dict.fromkeys(active_nodes, 0)
    for token_id in unlocked_token_ids:
        if token_id in current_assignments and current_assignments[token_id] in active_nodes:
            node_token_counts[current_assignments[token_id]] += 1

    receivers = []
    for i, (node_name, count) in enumerate(sorted(node_token_counts.items())):
        target = target_per_node + (1 if i < remainder else 0)
        if count < target:
            receivers.append((node_name, target - count))

    has_over_quota_nodes = any(node_token_counts[node] > target_per_node for node in active_nodes)
    has_under_quota_nodes = len(receivers) > 0

    # Reverse iteration when rebalancing helps minimize token movement by processing
    # higher token IDs first, which tends to produce more stable reassignments
    if has_over_quota_nodes and has_under_quota_nodes:
        token_iteration_order = sorted(unlocked_token_ids, reverse=True)
    else:
        token_iteration_order = sorted(unlocked_token_ids)

    new_assignments = {}
    tokens_moved = 0

    for token_id in token_iteration_order:
        if token_id in locked_assignments:
            new_assignments[token_id] = locked_assignments[token_id]
        elif token_id in current_assignments and current_assignments[token_id] in active_nodes:
            current_owner = current_assignments[token_id]
            if node_token_counts[current_owner] > target_per_node and receivers:
                receiver, deficit = receivers[0]
                new_assignments[token_id] = receiver
                node_token_counts[current_owner] -= 1
                node_token_counts[receiver] += 1
                tokens_moved += 1
                receivers[0] = (receiver, deficit - 1)
                if receivers[0][1] <= 0:
                    receivers.pop(0)
            else:
                new_assignments[token_id] = current_owner
        elif receivers:
            receiver, deficit = receivers[0]
            new_assignments[token_id] = receiver
            node_token_counts[receiver] += 1
            tokens_moved += 1
            receivers[0] = (receiver, deficit - 1)
            if receivers[0][1] <= 0:
                receivers.pop(0)

    for token_id, node in locked_assignments.items():
        if token_id not in new_assignments:
            new_assignments[token_id] = node

    return new_assignments, tokens_moved


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
        return self.id == other.id

    def __lt__(self, other) -> bool:
        """Compare tasks based on id for sorting.
        """
        return self.id < other.id

    def __gt__(self, other) -> bool:
        """Compare tasks based on id for sorting.
        """
        return self.id > other.id


# ============================================================
# EVENT SYSTEM
# ============================================================

class StateEvent(Enum):
    """Events that can trigger state transitions.
    """
    LEADER_PROMOTED = 'leader_promoted'
    LEADER_DEMOTED = 'leader_demoted'
    DEAD_NODES_DETECTED = 'dead_nodes_detected'
    MEMBERSHIP_CHANGED = 'membership_changed'
    NODE_UNHEALTHY = 'node_unhealthy'
    SHUTDOWN_REQUESTED = 'shutdown_requested'
    DISTRIBUTION_FAILED = 'distribution_failed'
    INITIALIZATION_FAILED = 'initialization_failed'
    ROLE_DETERMINED = 'role_determined'


class EventBus:
    """Simple event bus for decoupling monitors from state machine.
    """

    def __init__(self):
        """Initialize event bus.
        """
        self._subscribers = {}

    def subscribe(self, event_type: StateEvent, callback: callable) -> None:
        """Subscribe to an event type.

        Args:
            event_type: Event to subscribe to
            callback: Function to call when event occurs
        """
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []
        self._subscribers[event_type].append(callback)

    def publish(self, event_type: StateEvent, data: dict = None) -> None:
        """Publish an event to all subscribers.

        Args:
            event_type: Event to publish
            data: Optional event data
        """
        if event_type in self._subscribers:
            for callback in self._subscribers[event_type]:
                try:
                    callback(data or {})
                except Exception as e:
                    logger.error(f'Event callback failed for {event_type}: {e}')


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
        """Check if tasks can be claimed in current state.

        Returns
            True if claiming allowed
        """
        return self.is_running()

    def can_distribute(self) -> bool:
        """Check if token distribution is allowed in current state.

        Returns
            True if in DISTRIBUTING or RUNNING_LEADER state
        """
        return self.state in {JobState.DISTRIBUTING, JobState.RUNNING_LEADER}


# ============================================================
# SERVICE LAYER
# ============================================================

class DatabaseContext:
    """Manages database engine, connections, and table names.
    """

    def __init__(self, connection_string: str, config: ModuleType):
        """Initialize database context.

        Args:
            connection_string: SQLAlchemy connection string
            config: Configuration module
        """
        self.engine = create_engine(connection_string, pool_pre_ping=True, pool_size=10, max_overflow=5)
        self.tables = get_table_names(config)

    def dispose(self) -> None:
        """Dispose of engine resources.
        """
        with contextlib.suppress(Exception):
            self.engine.dispose()


class ClusterCoordinator:
    """Handles node registration, heartbeats, and leader election.
    """

    def __init__(self, node_name: str, db: DatabaseContext, coord_config: dict, created_on: datetime.datetime):
        """Initialize cluster coordinator.

        Args:
            node_name: This node's name
            db: Database context
            coord_config: Coordination configuration dict
            created_on: Node creation timestamp
        """
        self.node_name = node_name
        self.db = db
        self.heartbeat_interval = coord_config['heartbeat_interval_sec']
        self.heartbeat_timeout = coord_config['heartbeat_timeout_sec']
        self.health_check_interval = coord_config['health_check_interval_sec']
        self.dead_node_check_interval = coord_config['dead_node_check_interval_sec']
        self.rebalance_check_interval = coord_config['rebalance_check_interval_sec']
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
        with self.db.engine.connect() as conn:
            conn.execute(text(sql), {
                'name': self.node_name,
                'created_on': self.created_on,
                'heartbeat': self.last_heartbeat_sent
            })
            conn.commit()
        logger.info(f'Node {self.node_name} registered with heartbeat')

    def start_heartbeat(self, shutdown_event: threading.Event) -> None:
        """Start heartbeat monitor thread.

        Args:
            shutdown_event: Event to signal shutdown
        """
        self._heartbeat_monitor = HeartbeatMonitor(self, shutdown_event)
        self._heartbeat_monitor.start()

    def start_health_monitor(self, job: 'Job', shutdown_event: threading.Event) -> None:
        """Start health monitor thread.

        Args:
            job: Parent Job instance
            shutdown_event: Event to signal shutdown
        """
        self._health_monitor = HealthMonitor(self, job, shutdown_event)
        self._health_monitor.start()

    def elect_leader(self) -> str:
        """Elect leader as node with oldest registration timestamp.

        Returns
            Name of elected leader node
        """
        with self.db.engine.connect() as conn:
            result = conn.execute(text(self.active_nodes_sql))
            nodes = list(result)

        if not nodes:
            raise RuntimeError('No active nodes for leader election')

        return nodes[0][0]

    def get_active_nodes(self) -> list[dict]:
        """Get list of active nodes.

        Returns
            List of node dicts with name, created_on, last_heartbeat
        """
        with self.db.engine.connect() as conn:
            result = conn.execute(text(self.active_nodes_sql))
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
        with self.db.engine.connect() as conn:
            result = conn.execute(text(sql))
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
            with self.db.engine.connect() as conn:
                conn.execute(text(sql), {'name': self.node_name})
                conn.commit()
            logger.debug(f'Cleared {self.node_name} from {self.db.tables["Node"]}')
        except Exception as e:
            logger.debug(f'Failed to cleanup node {self.node_name}: {e}')


class TokenDistributor:
    """Handles token distribution and assignment tracking.
    """

    def __init__(self, node_name: str, db: DatabaseContext, coord_config: dict):
        """Initialize token distributor.

        Args:
            node_name: This node's name
            db: Database context
            coord_config: Coordination configuration dict
        """
        self.node_name = node_name
        self.db = db
        self.total_tokens = coord_config['total_tokens']
        self.token_refresh_initial = coord_config['token_refresh_initial_interval_sec']
        self.token_refresh_steady = coord_config['token_refresh_steady_interval_sec']
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
                    logger.debug(f'Token {token_id} will be locked to pattern "{pattern}" (matches {len(matching_nodes)} nodes)')
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

            conn.execute(text(f'DELETE FROM {self.db.tables["Token"]}'))

            for tid, node in new_assignments.items():
                sql = f'INSERT INTO {self.db.tables["Token"]} (token_id, node, assigned_at, version) VALUES (:token_id, :node, NOW(), :version)'
                conn.execute(text(sql), {
                    'token_id': tid,
                    'node': node,
                    'version': new_version
                })

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
        with self.db.engine.connect() as conn:
            result = conn.execute(text(sql), {'node': self.node_name})
            records = list(result)

        if not records:
            return set(), 0

        tokens = {row[0] for row in records}
        version = records[0][1] if records else 0
        logger.debug(f'Node {self.node_name} owns {len(tokens)} tokens, version {version}')
        return tokens, version

    def wait_for_distribution(self, timeout_sec: int = 30, check_interval: float = 0.5) -> None:
        """Wait for initial token distribution to complete.

        Args:
            timeout_sec: Maximum seconds to wait
            check_interval: How often to check for tokens
        """
        start = time.time()

        while time.time() - start < timeout_sec:
            with self.db.engine.connect() as conn:
                result = conn.execute(text(f'SELECT COUNT(*) FROM {self.db.tables["Token"]}'))
                count = result.scalar()

            if count > 0:
                logger.info(f'Token distribution detected ({count} tokens)')
                return

            time.sleep(check_interval)

        raise TimeoutError(f'Token distribution did not complete within {timeout_sec}s')

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
        with self.db.engine.connect() as conn:
            conn.execute(text(sql), {
                'reason': reason,
                'leader': self.node_name,
                'before': nodes_before,
                'after': nodes_after,
                'moved': tokens_moved,
                'duration': duration_ms
            })
            conn.commit()


class LockManager:
    """Handles task locking and coordination locks.
    """

    def __init__(self, node_name: str, db: DatabaseContext, coord_config: dict):
        """Initialize lock manager.

        Args:
            node_name: This node's name
            db: Database context
            coord_config: Coordination configuration dict
        """
        self.node_name = node_name
        self.db = db
        self.total_tokens = coord_config['total_tokens']
        self.leader_lock_timeout = coord_config['leader_lock_timeout_sec']
        self.stale_leader_lock_age = coord_config['stale_leader_lock_age_sec']
        self.stale_rebalance_lock_age = coord_config['stale_rebalance_lock_age_sec']

    def register_lock(self, task_id: Hashable, node_patterns: str | list[str], reason: str = None,
                      expires_in_days: int = None) -> None:
        """Register lock to pin task to specific node patterns.

        Args:
            task_id: Task identifier to lock
            node_patterns: Single pattern or ordered list of fallback patterns
            reason: Human-readable reason for lock
            expires_in_days: Optional expiration in days
        """
        if isinstance(node_patterns, str):
            node_patterns = [node_patterns]

        token_id = task_to_token(task_id, self.total_tokens)
        expires_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
            days=expires_in_days) if expires_in_days else None

        sql = f"""
        INSERT INTO {self.db.tables["Lock"]}
        (token_id, node_patterns, reason, created_at, created_by, expires_at)
        VALUES (:token_id, :patterns, :reason, NOW(), :created_by, :expires_at)
        ON CONFLICT (token_id) DO UPDATE
        SET node_patterns = EXCLUDED.node_patterns,
            reason = EXCLUDED.reason,
            created_at = EXCLUDED.created_at,
            created_by = EXCLUDED.created_by,
            expires_at = EXCLUDED.expires_at
        """

        with self.db.engine.connect() as conn:
            conn.execute(text(sql), {
                'token_id': token_id,
                'patterns': json.dumps(node_patterns),
                'reason': reason,
                'created_by': self.node_name,
                'expires_at': expires_at
            })
            conn.commit()

        logger.debug(f'Registered lock: task {task_id} -> token {token_id} -> patterns {node_patterns}')

    def register_locks_bulk(self, locks: list[tuple[Hashable, str | list[str], str]]) -> None:
        """Register multiple locks in batch.

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
                'token_id': task_to_token(task_id, self.total_tokens),
                'patterns': json.dumps(node_patterns),
                'reason': reason,
                'created_by': self.node_name,
                'expires_at': None
            })

        sql = f"""
        INSERT INTO {self.db.tables["Lock"]}
        (token_id, node_patterns, reason, created_at, created_by, expires_at)
        VALUES (:token_id, :patterns, :reason, NOW(), :created_by, :expires_at)
        ON CONFLICT (token_id) DO UPDATE
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

        logger.info(f'Registered {len(locks)} locks in bulk')

    def clear_locks_by_creator(self, creator: str) -> int:
        """Clear all locks created by specified node.

        Args:
            creator: Node name that created the locks

        Returns
            Number of locks removed
        """
        sql = f'DELETE FROM {self.db.tables["Lock"]} WHERE created_by = :creator'
        with self.db.engine.connect() as conn:
            result = conn.execute(text(sql), {'creator': creator})
            conn.commit()
            rows = result.rowcount
        logger.info(f'Cleared {rows} locks created by {creator}')
        return rows

    def clear_all_locks(self) -> int:
        """Clear all locks from system.

        Returns
            Number of locks removed
        """
        sql = f'DELETE FROM {self.db.tables["Lock"]}'
        with self.db.engine.connect() as conn:
            result = conn.execute(text(sql))
            conn.commit()
            rows = result.rowcount
        logger.warning(f'Cleared ALL {rows} locks from system')
        return rows

    def list_locks(self) -> list[dict]:
        """List all active locks.

        Returns
            List of lock records
        """
        sql = f"""
        SELECT token_id, node_patterns, reason, created_at, created_by, expires_at
        FROM {self.db.tables["Lock"]}
        WHERE expires_at IS NULL OR expires_at > NOW()
        ORDER BY created_at DESC
        """
        with self.db.engine.connect() as conn:
            result = conn.execute(text(sql))
            return [dict(row._mapping) for row in result]

    def get_active_locks(self) -> dict[int, list[str]]:
        """Get active locks as token_id -> patterns mapping.

        Returns
            Dictionary mapping token IDs to ordered pattern lists
        """
        with self.db.engine.connect() as conn:
            sql = f"""
            DELETE FROM {self.db.tables["Lock"]}
            WHERE expires_at IS NOT NULL AND expires_at < NOW()
            """
            conn.execute(text(sql))

            sql = f'SELECT token_id, node_patterns FROM {self.db.tables["Lock"]}'
            result = conn.execute(text(sql))
            records = [dict(row._mapping) for row in result]

            locked_tokens = {}
            for row in records:
                try:
                    patterns = row['node_patterns']
                    if not isinstance(patterns, list):
                        logger.warning(f'Invalid lock pattern for token {row["token_id"]}: expected list, got {type(patterns).__name__}')
                        continue
                    locked_tokens[row['token_id']] = patterns
                except (json.JSONDecodeError, TypeError, KeyError) as e:
                    logger.warning(f'Failed to parse lock pattern for token {row["token_id"]}: {e}')
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
        """Attempt to acquire leader lock.

        Args:
            operation: Operation name

        Returns
            True if acquired, False otherwise
        """
        start_time = time.time()
        while time.time() - start_time < self.leader_lock_timeout:
            try:
                with self.db.engine.connect() as conn:
                    sql = f"""
                    INSERT INTO {self.db.tables["LeaderLock"]} (singleton, node, acquired_at, operation)
                    VALUES (1, :node, :acquired_at, :operation)
                    ON CONFLICT (singleton) DO NOTHING
                    """
                    result = conn.execute(text(sql), {
                        'node': self.node_name,
                        'acquired_at': datetime.datetime.now(datetime.timezone.utc),
                        'operation': operation
                    })
                    conn.commit()
                    rows_affected = result.rowcount

                    if rows_affected > 0:
                        logger.info(f'Leader lock acquired by {self.node_name} for {operation}')
                        return True

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
            with self.db.engine.connect() as conn:
                conn.execute(text(sql), {'node': self.node_name})
                conn.commit()
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
            result = conn.execute(text(sql), {
                'started_at': datetime.datetime.now(datetime.timezone.utc),
                'started_by': started_by
            })
            conn.commit()
            rows_affected = result.rowcount

        success = rows_affected > 0

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
        with self.db.engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()
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
        with self.db.engine.connect() as conn:
            result = conn.execute(text(sql), {'date': self.date})
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
        """Main monitoring loop - to be implemented by subclasses.
        """
        raise NotImplementedError

    def check(self, conn) -> None:
        """Perform monitoring check - to be implemented by subclasses.

        Args:
            conn: Active database connection
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

    def _run(self) -> None:
        """Main heartbeat loop.
        """
        conn = self.cluster.db.engine.connect()
        try:
            while not self.shutdown_event.is_set() and not self._stop_requested:
                try:
                    self.check(conn)
                except Exception as e:
                    logger.error(f'{self.name} monitor error: {e}', exc_info=True)
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = self.cluster.db.engine.connect()

                if self.shutdown_event.wait(timeout=self.interval):
                    break
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def check(self, conn) -> None:
        """Send heartbeat update.

        Args:
            conn: Database connection
        """
        sql = f"""
        UPDATE {self.cluster.db.tables["Node"]}
        SET last_heartbeat = :heartbeat
        WHERE name = :name
        """
        heartbeat_time = datetime.datetime.now(datetime.timezone.utc)
        conn.execute(text(sql), {'heartbeat': heartbeat_time, 'name': self.cluster.node_name})
        conn.commit()
        self.cluster.last_heartbeat_sent = heartbeat_time
        logger.debug(f'Heartbeat sent by {self.cluster.node_name}')


class HealthMonitor(Monitor):
    """Monitors node health by checking heartbeat age and database connectivity.
    """

    def __init__(self, cluster: ClusterCoordinator, job: 'Job', shutdown_event: threading.Event):
        """Initialize health monitor.

        Args:
            cluster: Cluster coordinator
            job: Parent Job instance
            shutdown_event: Shutdown event
        """
        super().__init__(f'health-{cluster.node_name}', cluster.health_check_interval, shutdown_event)
        self.cluster = cluster
        self.job = job

    def _run(self) -> None:
        """Main health check loop.
        """
        conn = self.cluster.db.engine.connect()
        try:
            while not self.shutdown_event.is_set() and not self._stop_requested:
                try:
                    self.check(conn)
                except Exception as e:
                    logger.error(f'{self.name} monitor error: {e}', exc_info=True)
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = self.cluster.db.engine.connect()

                if self.shutdown_event.wait(timeout=self.interval):
                    break
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def check(self, conn) -> None:
        """Check heartbeat age and database connectivity.

        Args:
            conn: Database connection
        """
        if self.cluster.last_heartbeat_sent:
            age_seconds = (datetime.datetime.now(datetime.timezone.utc) - self.cluster.last_heartbeat_sent).total_seconds()
            if age_seconds > self.cluster.heartbeat_timeout:
                logger.error(f'Heartbeat thread appears dead (age: {age_seconds:.1f}s > timeout: {self.cluster.heartbeat_timeout}s)')
                self.job.event_bus.publish(StateEvent.NODE_UNHEALTHY, {'reason': 'heartbeat_timeout', 'age_seconds': age_seconds})
                return

        conn.execute(text('SELECT 1'))
        conn.rollback()


class TokenRefreshMonitor(Monitor):
    """Detects token reassignments and invokes callbacks for followers.
    """

    def __init__(self, job: 'Job', shutdown_event: threading.Event):
        """Initialize token refresh monitor.

        Args:
            job: Parent Job instance
            shutdown_event: Shutdown event
        """
        super().__init__(
            f'token-refresh-{job.node_name}',
            job.tokens.token_refresh_initial,
            shutdown_event
        )
        self.job = job
        self.start_time = time.time()
        self.check_count = 0
        self.initial_callback_sent = False

    def _run(self) -> None:
        """Main token refresh loop.
        """
        conn = self.job.db.engine.connect()
        try:
            while not self.shutdown_event.is_set() and not self._stop_requested:
                try:
                    self.check(conn)
                except Exception as e:
                    logger.error(f'{self.name} monitor error: {e}', exc_info=True)
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = self.job.db.engine.connect()

                if self.shutdown_event.wait(timeout=self.interval):
                    break
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def check(self, conn) -> None:
        """Check for token reassignments and publish leader promotion/demotion events.

        Args:
            conn: Database connection
        """
        elapsed = time.time() - self.start_time
        self.interval = self.job.tokens.token_refresh_initial if elapsed < 300 else self.job.tokens.token_refresh_steady

        if self.job.am_i_leader() and self.job.state_machine.state == JobState.RUNNING_FOLLOWER:
            logger.info('Detected leader promotion, publishing event')
            self.job.event_bus.publish(StateEvent.LEADER_PROMOTED)
        elif not self.job.am_i_leader() and self.job.state_machine.state == JobState.RUNNING_LEADER:
            logger.warning('Detected leader demotion (older node rejoined), publishing event')
            self.job.event_bus.publish(StateEvent.LEADER_DEMOTED)

        sql = f'SELECT token_id, version FROM {self.job.db.tables["Token"]} WHERE node = :node'
        result = conn.execute(text(sql), {'node': self.job.node_name})
        records = [dict(row._mapping) for row in result]

        if not records:
            new_tokens, new_version = set(), 0
        else:
            new_tokens = {row['token_id'] for row in records}
            new_version = records[0]['version'] if records else 0

        if not self.initial_callback_sent and new_tokens and self.job.tokens.on_tokens_added:
            logger.info(f'Invoking initial on_tokens_added for {len(new_tokens)} tokens')
            self.job.tokens.my_tokens = new_tokens
            self.job.tokens.token_version = new_version
            self.job.tokens.invoke_callback(self.job.tokens.on_tokens_added, new_tokens.copy(), 'on_tokens_added')
            self.initial_callback_sent = True
            self.start_time = time.time()
        elif self.job.tokens.token_version > 0 and new_version != self.job.tokens.token_version:
            added = new_tokens - self.job.tokens.my_tokens
            removed = self.job.tokens.my_tokens - new_tokens

            logger.warning(f'Token rebalance detected: v{self.job.tokens.token_version}->v{new_version}, '
                           f'+{len(added)} -{len(removed)} tokens')

            self.job.tokens.my_tokens = new_tokens
            self.job.tokens.token_version = new_version

            if removed and self.job.tokens.on_tokens_removed:
                self.job.tokens.invoke_callback(self.job.tokens.on_tokens_removed, removed, 'on_tokens_removed')

            if added and self.job.tokens.on_tokens_added:
                self.job.tokens.invoke_callback(self.job.tokens.on_tokens_added, added, 'on_tokens_added')

            self.start_time = time.time()

        self.check_count += 1
        if self.check_count % 10 == 0:
            logger.debug(f'Token refresh #{self.check_count}: {len(self.job.tokens.my_tokens)} tokens, v{self.job.tokens.token_version}')


class DeadNodeMonitor(Monitor):
    """Leader-only monitor that detects and removes dead nodes.
    """

    def __init__(self, job: 'Job', shutdown_event: threading.Event):
        """Initialize dead node monitor.

        Args:
            job: Parent Job instance
            shutdown_event: Shutdown event
        """
        super().__init__(
            f'dead-node-{job.node_name}',
            job.cluster.dead_node_check_interval,
            shutdown_event
        )
        self.job = job

    def _run(self) -> None:
        """Main dead node detection loop.
        """
        conn = self.job.db.engine.connect()
        try:
            while not self.shutdown_event.is_set() and not self._stop_requested:
                try:
                    self.check(conn)
                except Exception as e:
                    logger.error(f'{self.name} monitor error: {e}', exc_info=True)
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = self.job.db.engine.connect()

                if self.shutdown_event.wait(timeout=self.interval):
                    break
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def check(self, conn) -> None:
        """Detect and remove dead nodes, publish event.

        Args:
            conn: Database connection
        """
        sql = f"""
        SELECT name
        FROM {self.job.db.tables["Node"]}
        WHERE last_heartbeat <= NOW() - INTERVAL '{self.job.cluster.heartbeat_timeout} seconds' OR last_heartbeat IS NULL
        """

        result = conn.execute(text(sql))
        dead_nodes = [row[0] for row in result]
        conn.rollback()

        if dead_nodes:
            logger.warning(f'Detected dead nodes: {dead_nodes}')
            try:
                with self.job.locks.acquire_rebalance_lock('dead_node_monitor'):
                    for node in dead_nodes:
                        conn.execute(text(f'DELETE FROM {self.job.db.tables["Node"]} WHERE name = :name'), {'name': node})
                        conn.execute(text(f'DELETE FROM {self.job.db.tables["Claim"]} WHERE node = :node'), {'node': node})
                        conn.execute(text(f'DELETE FROM {self.job.db.tables["Lock"]} WHERE created_by = :node'), {'node': node})
                        logger.info(f'Removed dead node and cleaned up locks: {node}')
                    conn.commit()
                self.job.event_bus.publish(StateEvent.DEAD_NODES_DETECTED, {'nodes': dead_nodes})
            except LockNotAcquired:
                logger.debug('Rebalance already in progress, skipping dead node cleanup')
                conn.rollback()


class RebalanceMonitor(Monitor):
    """Leader-only monitor that triggers rebalancing on membership changes.
    """

    def __init__(self, job: 'Job', shutdown_event: threading.Event):
        """Initialize rebalance monitor.

        Args:
            job: Parent Job instance
            shutdown_event: Shutdown event
        """
        super().__init__(
            f'rebalance-{job.node_name}',
            job.cluster.rebalance_check_interval,
            shutdown_event
        )
        self.job = job
        with self.job.db.engine.connect() as conn:
            result = conn.execute(text(self.job.cluster.active_nodes_sql))
            self.last_node_count = len(list(result))

    def _run(self) -> None:
        """Main rebalance monitoring loop.
        """
        conn = self.job.db.engine.connect()
        try:
            while not self.shutdown_event.is_set() and not self._stop_requested:
                try:
                    self.check(conn)
                except Exception as e:
                    logger.error(f'{self.name} monitor error: {e}', exc_info=True)
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = self.job.db.engine.connect()

                if self.shutdown_event.wait(timeout=self.interval):
                    break
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def check(self, conn) -> None:
        """Check for membership changes and publish event.

        Args:
            conn: Database connection
        """
        result = conn.execute(text(self.job.cluster.active_nodes_sql))
        current_count = len(list(result))
        conn.rollback()

        if current_count != self.last_node_count:
            logger.info(f'Node count changed: {self.last_node_count} -> {current_count}')
            self.job.event_bus.publish(StateEvent.MEMBERSHIP_CHANGED, {
                'previous_count': self.last_node_count,
                'current_count': current_count
            })
            self.last_node_count = current_count


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
        config: ModuleType,
        date: datetime.date | datetime.datetime = None,
        wait_on_enter: int = 120,
        wait_on_exit: int = 0,
        lock_provider: callable = None,
        clear_existing_locks: bool = False,
        coordination_config: CoordinationConfig = None,
        connection_string: str = None,
        on_tokens_added: callable = None,
        on_tokens_removed: callable = None,
    ):
        """Initialize job synchronization manager.

        Args:
            node_name: Unique identifier for this node
            config: Configuration module
            date: Processing date (defaults to today)
            wait_on_enter: Seconds to wait for cluster formation (default 120)
            wait_on_exit: Seconds to wait before cleanup (default 0)
            lock_provider: Callback(job) to register task locks during __enter__
            clear_existing_locks: If True, clear locks created by this node before invoking lock_provider
            coordination_config: Optional CoordinationConfig to override config module settings
            connection_string: Optional SQLAlchemy connection string
            on_tokens_added: Callback(token_ids: set[int]) invoked when tokens assigned
            on_tokens_removed: Callback(token_ids: set[int]) invoked when tokens removed
        """
        self.node_name = node_name
        self._wait_on_enter = int(abs(wait_on_enter))
        self._wait_on_exit = int(abs(wait_on_exit))
        self._nodes = [{'node': self.node_name}]

        if date is None:
            date = datetime.datetime.now().date()
        elif isinstance(date, datetime.datetime):
            date = date.date()
        elif not isinstance(date, datetime.date):
            raise TypeError(f'date must be None, datetime.date, or datetime.datetime, got {type(date).__name__}')

        self._config = config
        coord_cfg = load_coordination_config(config, coordination_config)
        self._coordination_enabled = coord_cfg['enabled']

        if connection_string is None:
            connection_string = build_connection_string(config)

        self._created_on = ensure_timezone_aware(datetime.datetime.now(datetime.timezone.utc), 'node created_on')
        self._shutdown_event = threading.Event()
        self._lock_provider = lock_provider
        self._clear_existing_locks = clear_existing_locks
        self._locks_enabled = coord_cfg['locks_enabled']

        self.db = DatabaseContext(connection_string, config)
        self.cluster = ClusterCoordinator(node_name, self.db, coord_cfg, self._created_on)
        self.tokens = TokenDistributor(node_name, self.db, coord_cfg)
        self.locks = LockManager(node_name, self.db, coord_cfg)
        self.tasks = TaskManager(node_name, self.db, date, coord_cfg['total_tokens'])

        self.tokens.on_tokens_added = on_tokens_added
        self.tokens.on_tokens_removed = on_tokens_removed

        self.event_bus = EventBus()
        self.state_machine = JobStateMachine()

        self._monitors = []

        self._register_state_callbacks()
        self._register_event_handlers()

        if not self._coordination_enabled and (on_tokens_added or on_tokens_removed):
            logger.warning('Token callbacks provided but coordination_enabled=False - callbacks will never fire')

        ensure_database_ready(self.db.engine, config, coordination_enabled=self._coordination_enabled)

    def _register_state_callbacks(self) -> None:
        """Register callbacks for state entry/exit actions.
        """
        self.state_machine.on_enter(JobState.CLUSTER_FORMING, self._on_enter_cluster_forming)
        self.state_machine.on_enter(JobState.ELECTING, self._on_enter_electing)
        self.state_machine.on_enter(JobState.DISTRIBUTING, self._on_enter_distributing)
        self.state_machine.on_enter(JobState.RUNNING_LEADER, self._on_enter_running_leader)
        self.state_machine.on_exit(JobState.RUNNING_LEADER, self._on_exit_running_leader)
        self.state_machine.on_enter(JobState.RUNNING_FOLLOWER, self._on_enter_running_follower)
        self.state_machine.on_enter(JobState.SHUTTING_DOWN, self._on_enter_shutting_down)

    def _register_event_handlers(self) -> None:
        """Register handlers for monitor events.
        """
        self.event_bus.subscribe(StateEvent.LEADER_PROMOTED, self._handle_leader_promoted)
        self.event_bus.subscribe(StateEvent.LEADER_DEMOTED, self._handle_leader_demoted)
        self.event_bus.subscribe(StateEvent.DEAD_NODES_DETECTED, self._handle_dead_nodes)
        self.event_bus.subscribe(StateEvent.MEMBERSHIP_CHANGED, self._handle_membership_changed)
        self.event_bus.subscribe(StateEvent.NODE_UNHEALTHY, self._handle_node_unhealthy)
        self.event_bus.subscribe(StateEvent.SHUTDOWN_REQUESTED, self._handle_shutdown_requested)
        self.event_bus.subscribe(StateEvent.DISTRIBUTION_FAILED, self._handle_distribution_failed)
        self.event_bus.subscribe(StateEvent.INITIALIZATION_FAILED, self._handle_initialization_failed)
        self.event_bus.subscribe(StateEvent.ROLE_DETERMINED, self._handle_role_determined)

    def _on_enter_cluster_forming(self) -> None:
        """Entry action for CLUSTER_FORMING state.
        """
        self.cluster.register()
        self.cluster.start_heartbeat(self._shutdown_event)
        self.cluster.start_health_monitor(self, self._shutdown_event)
        self._monitors.append(self.cluster._heartbeat_monitor)
        self._monitors.append(self.cluster._health_monitor)

        if self._lock_provider and self._locks_enabled:
            if self._clear_existing_locks:
                logger.info(f'Clearing existing locks created by {self.node_name}')
                self.locks.clear_locks_by_creator(self.node_name)
            logger.info('Invoking lock_provider callback')
            self._lock_provider(self)

    def _on_enter_electing(self) -> None:
        """Entry action for ELECTING state.
        """

    def _on_enter_distributing(self) -> None:
        """Entry action for DISTRIBUTING state.
        """
        self._distribute_tokens_safe()

    def _on_enter_running_leader(self) -> None:
        """Entry action for RUNNING_LEADER state.
        """
        logger.info('Starting leader monitoring threads...')
        if not any('token-refresh' in m.name for m in self._monitors):
            monitor = TokenRefreshMonitor(self, self._shutdown_event)
            self._monitors.append(monitor)
            monitor.start()
        self._monitors.append(DeadNodeMonitor(self, self._shutdown_event))
        self._monitors.append(RebalanceMonitor(self, self._shutdown_event))
        for monitor in self._monitors[-2:]:
            monitor.start()

    def _on_exit_running_leader(self) -> None:
        """Exit action for RUNNING_LEADER state.

        Stops leader-only monitors (DeadNodeMonitor, RebalanceMonitor).
        """
        logger.info('Stopping leader monitoring threads...')
        for monitor in self._monitors:
            if isinstance(monitor, (DeadNodeMonitor, RebalanceMonitor)):
                monitor.stop()

    def _on_enter_running_follower(self) -> None:
        """Entry action for RUNNING_FOLLOWER state.
        """
        if not any('token-refresh' in m.name for m in self._monitors):
            self._monitors.append(TokenRefreshMonitor(self, self._shutdown_event))
            self._monitors[-1].start()

    def _on_enter_shutting_down(self) -> None:
        """Entry action for SHUTTING_DOWN state.
        """
        self._shutdown_event.set()

    def _handle_leader_promoted(self, data: dict) -> None:
        """Handle leader promotion event from monitor.

        Args:
            data: Event data
        """
        if self.state_machine.is_follower():
            logger.warning(f'{self.node_name} promoted to leader')
            self.state_machine.transition_to(JobState.RUNNING_LEADER)

    def _handle_leader_demoted(self, data: dict) -> None:
        """Handle leader demotion event (older node rejoined).

        Args:
            data: Event data
        """
        if self.state_machine.is_leader():
            logger.warning(f'{self.node_name} demoted from leader (older node rejoined)')
            self.state_machine.transition_to(JobState.RUNNING_FOLLOWER)

    def _handle_dead_nodes(self, data: dict) -> None:
        """Handle dead nodes detected event.

        Args:
            data: Event data with 'nodes' list
        """
        if not self.state_machine.can_distribute():
            logger.debug('Cannot distribute in non-leader state')
            return
        self._distribute_tokens_safe()

    def _handle_membership_changed(self, data: dict) -> None:
        """Handle membership change event.

        Args:
            data: Event data with previous_count and current_count
        """
        if not self.state_machine.can_distribute():
            logger.debug('Cannot distribute in non-leader state')
            return
        self._distribute_tokens_safe()

    def _handle_node_unhealthy(self, data: dict) -> None:
        """Handle node unhealthy event.

        Args:
            data: Event data
        """
        logger.error('Node detected as unhealthy, triggering shutdown')
        if not self.state_machine.is_error():
            self.state_machine.transition_to(JobState.ERROR)
        self.state_machine.transition_to(JobState.SHUTTING_DOWN)

    def _handle_shutdown_requested(self, data: dict) -> None:
        """Handle shutdown request event.

        Args:
            data: Event data
        """
        logger.info('Shutdown requested')
        if self.state_machine.state != JobState.SHUTTING_DOWN:
            self.state_machine.transition_to(JobState.SHUTTING_DOWN)

    def _handle_distribution_failed(self, data: dict) -> None:
        """Handle distribution failure event.

        Args:
            data: Event data with error information
        """
        if self.state_machine.is_initializing():
            logger.error('Distribution failed during initialization, transitioning to ERROR state')
            self.state_machine.transition_to(JobState.ERROR)

    def _handle_initialization_failed(self, data: dict) -> None:
        """Handle initialization failure event.

        Args:
            data: Event data with error and phase information
        """
        error = data.get('error', 'unknown')
        phase = data.get('phase', 'unknown')
        logger.error(f'Initialization failed during {phase}: {error}')
        self.state_machine.transition_to(JobState.ERROR)

    def _handle_role_determined(self, data: dict) -> None:
        """Handle role determination event.

        Args:
            data: Event data with is_leader flag
        """
        if data.get('is_leader'):
            self.state_machine.transition_to(JobState.RUNNING_LEADER)
        else:
            self.state_machine.transition_to(JobState.RUNNING_FOLLOWER)

    def __enter__(self):
        """Enter context and perform coordination setup.
        """
        self._cleanup()

        if not self._coordination_enabled:
            return self

        logger.info(f'Starting {self.node_name} in coordination mode')

        try:
            if not self.state_machine.transition_to(JobState.CLUSTER_FORMING):
                logger.error('Failed to transition to CLUSTER_FORMING')
                self.event_bus.publish(StateEvent.INITIALIZATION_FAILED, {
                    'error': 'Invalid state transition to CLUSTER_FORMING',
                    'phase': 'state_transition'
                })
                return self

            logger.info(f'Waiting {self._wait_on_enter}s for cluster formation...')
            if self._shutdown_event.wait(timeout=self._wait_on_enter):
                logger.info('Shutdown requested during cluster wait')
                return self

            self._nodes = self.cluster.get_active_nodes()
            logger.info(f'Active nodes: {[n["name"] for n in self._nodes]}')

            if not self.state_machine.transition_to(JobState.ELECTING):
                logger.error('Failed to transition to ELECTING')
                self.event_bus.publish(StateEvent.INITIALIZATION_FAILED, {
                    'error': 'Invalid state transition to ELECTING',
                    'phase': 'electing'
                })
                return self

            if self.state_machine.is_error():
                return self

            if not self.state_machine.transition_to(JobState.DISTRIBUTING):
                logger.error('Failed to transition to DISTRIBUTING')
                self.event_bus.publish(StateEvent.INITIALIZATION_FAILED, {
                    'error': 'Invalid state transition to DISTRIBUTING',
                    'phase': 'distributing'
                })
                return self

            if self.state_machine.is_error():
                return self

            logger.info('Waiting for token distribution...')
            self.tokens.wait_for_distribution()

            self.tokens.my_tokens, self.tokens.token_version = self.tokens.get_my_tokens_versioned()
            logger.info(f'Node {self.node_name} assigned {len(self.tokens.my_tokens)} tokens, version {self.tokens.token_version}')

            # Determine role via event-driven pattern
            elected_leader = self.cluster.elect_leader()
            is_leader = elected_leader == self.node_name
            self.event_bus.publish(StateEvent.ROLE_DETERMINED, {
                'is_leader': is_leader,
                'elected_leader': elected_leader
            })

        except Exception as e:
            logger.error(f'Initialization failed: {e}', exc_info=True)
            self.event_bus.publish(StateEvent.INITIALIZATION_FAILED, {
                'error': str(e),
                'phase': 'initialization'
            })

        return self

    def __exit__(self, exc_ty, exc_val, tb):
        """Exit context and cleanup coordination resources.
        """
        logger.debug(f'Exiting {self.node_name} context')

        if exc_ty:
            logger.error(exc_val)

        if self._coordination_enabled:
            self.event_bus.publish(StateEvent.SHUTDOWN_REQUESTED)

            for monitor in self._monitors:
                if monitor.thread and monitor.thread.is_alive():
                    logger.debug(f'Joining {monitor.name} thread...')
                    monitor.thread.join(timeout=10)
                    if monitor.thread.is_alive():
                        logger.warning(f'{monitor.name} thread did not stop within timeout')

        if self._wait_on_exit:
            logger.debug(f'Sleeping {self._wait_on_exit} seconds...')
            time.sleep(self._wait_on_exit)

        self._cleanup()
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
        return self.tokens.my_tokens.copy()

    @property
    def token_version(self) -> int:
        """Get current token distribution version.
        """
        return self.tokens.token_version

    def set_claim(self, item):
        """Claim an item or items and publish to database.

        Args:
            item: Item ID or iterable of item IDs
        """
        self.tasks.set_claim(item)

    def add_task(self, task: Task):
        """Add task to processing queue if claimable in coordination mode.

        Args:
            task: Task to add
        """
        if self._coordination_enabled and not self.can_claim_task(task):
            logger.debug(f'Task {task.id} rejected (token not owned)')
            return

        self.tasks.add_task(task)

    def write_audit(self) -> None:
        """Write accumulated tasks to audit table.
        """
        self.tasks.write_audit()

    def get_audit(self) -> list[dict]:
        """Get audit records for current date.

        Returns
            List of audit records
        """
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

        if self.state_machine.is_error():
            logger.debug(f'Cannot claim task {task.id} in ERROR state')
            return False

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
        return task_to_token(task_id, self.tokens.total_tokens)

    def get_task_ids_for_token(self, token_id: int, all_task_ids: list[Hashable]) -> list[Hashable]:
        """Get task IDs that hash to the given token.

        Args:
            token_id: Token ID to match
            all_task_ids: Complete list of possible task IDs

        Returns
            List of task IDs that hash to this token
        """
        return [task_id for task_id in all_task_ids if task_to_token(task_id, self.tokens.total_tokens) == token_id]

    def get_my_task_ids(self, all_task_ids: list[Hashable]) -> list[Hashable]:
        """Get all task IDs owned by this node.

        Args:
            all_task_ids: Complete list of possible task IDs

        Returns
            List of task IDs this node currently owns
        """
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
        self.locks.register_lock(task_id, node_patterns, reason, expires_in_days)

    def register_locks_bulk(self, locks: list[tuple[Hashable, str | list[str], str]]) -> None:
        """Register multiple locks in batch.

        Args:
            locks: List of (task_id, node_patterns, reason) tuples
        """
        self.locks.register_locks_bulk(locks)

    def clear_locks_by_creator(self, creator: str) -> int:
        """Clear all locks created by specified node.

        Args:
            creator: Node name that created the locks

        Returns
            Number of locks removed
        """
        return self.locks.clear_locks_by_creator(creator)

    def clear_all_locks(self) -> int:
        """Clear all locks from system.

        Returns
            Number of locks removed
        """
        return self.locks.clear_all_locks()

    def list_locks(self) -> list[dict]:
        """List all active locks.

        Returns
            List of lock records
        """
        return self.locks.list_locks()

    def am_i_healthy(self) -> bool:
        """Check if node is healthy.

        Returns
            True if healthy, False otherwise
        """
        return self.cluster.am_i_healthy()

    def am_i_leader(self) -> bool:
        """Check if this node should be the elected leader based on database state.

        Returns
            True if this node should be leader (oldest active node), False otherwise
        """
        if not self._coordination_enabled:
            return False

        try:
            elected_leader = self.cluster.elect_leader()
            return elected_leader == self.node_name
        except Exception:
            return False

    def get_active_nodes(self) -> list[dict]:
        """Get list of active nodes.

        Returns
            List of node dicts
        """
        return self.cluster.get_active_nodes()

    def get_dead_nodes(self) -> list[str]:
        """Get list of dead nodes.

        Returns
            List of dead node names
        """
        return self.cluster.get_dead_nodes()

    def _distribute_tokens_safe(self) -> None:
        """Leader distributes tokens with rebalance and leader lock protection.

        Acquires locks in order to prevent deadlock:
        1. rebalance_lock - coarse-grained "who's rebalancing"
        2. leader_lock - fine-grained "which leader is distributing"
        """
        try:
            with self.locks.acquire_rebalance_lock('token_distribution'):
                with self.locks.acquire_leader_lock('distribute'):
                    self.tokens.distribute(self.locks, self.cluster)
        except LockNotAcquired as e:
            logger.debug(f'Could not acquire lock for token distribution: {e}')
        except Exception as e:
            logger.error(f'Token distribution failed: {e}', exc_info=True)
            self.event_bus.publish(StateEvent.DISTRIBUTION_FAILED, {'error': str(e)})

    def _cleanup(self) -> None:
        """Cleanup tables and write audit log.

        Task audit and cleanup always run regardless of coordination mode.
        Cluster cleanup only runs in coordination mode to avoid unnecessary DB operations.
        """
        self.tasks.write_audit()
        if self._coordination_enabled:
            self.cluster.cleanup()
        self.tasks.cleanup()
