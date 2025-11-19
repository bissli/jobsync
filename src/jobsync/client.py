"""Job synchronization manager with hybrid coordination support.
"""
import contextlib
import datetime
import enum
import hashlib
import logging
import re
import threading
import time
from collections.abc import Hashable, Iterable
from copy import deepcopy
from functools import total_ordering
from types import ModuleType

from sqlalchemy import create_engine, text

from jobsync.config import CoordinationConfig
from jobsync.schema import ensure_database_ready, get_table_names

logger = logging.getLogger(__name__)

__all__ = ['Job', 'Task']


class JobState(enum.Enum):
    """Job lifecycle states for tracking coordination progress.
    """
    INITIALIZING = 1
    WAITING_FOR_CLUSTER = 2
    ELECTING_LEADER = 3
    DISTRIBUTING_TOKENS = 4
    RUNNING = 5
    SHUTTING_DOWN = 6
    STOPPED = 7


def compute_minimal_move_distribution(
    total_tokens: int,
    active_nodes: list[str],
    current_assignments: dict[int, str],
    locked_tokens: dict[int, str],
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
        locked_tokens: Locked token_id -> node_pattern mapping
        pattern_matcher: Function(node_name, pattern) -> bool for pattern matching

    Returns
        Tuple of (new_assignments dict, tokens_moved count)
    """
    nodes_count = len(active_nodes)

    if nodes_count == 0:
        return {}, 0

    locked_assignments = {}
    for token_id, pattern in locked_tokens.items():
        matching_nodes = [n for n in active_nodes if pattern_matcher(n, pattern)]
        if matching_nodes:
            locked_assignments[token_id] = matching_nodes[0]

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


class Job:
    """Job sync manager with coordination support for distributed task processing.

    Lifecycle Phases:
    1. INITIALIZING (__init__): Configure instance variables
    2. WAITING_FOR_CLUSTER (__enter__):
       - Register node and start heartbeat
       - Wait for cluster formation (wait_on_enter seconds)
    3. ELECTING_LEADER: Current nodes elect leader (oldest node wins)
    4. DISTRIBUTING_TOKENS: Leader distributes tokens, followers wait
    5. RUNNING: Normal operation - process tasks via can_claim_task()
    6. SHUTTING_DOWN (__exit__): Stop threads, cleanup database
    7. STOPPED: All resources released

    Callback Lifecycle (Long-Running Task Support):
    - on_tokens_added(token_ids: set[int]): Called when tokens are assigned
      * Invoked during __enter__ for initial allocation
      * Invoked in refresh thread on rebalances
      * Use to start subscriptions/long-running tasks
    - on_tokens_removed(token_ids: set[int]): Called when tokens are removed
      * Invoked in refresh thread on rebalances
      * Always called BEFORE on_tokens_added in same rebalance
      * Use to stop subscriptions/long-running tasks

    Callback Requirements:
    - Callbacks run in background thread (heartbeat or refresh thread)
    - Keep callbacks fast (<1s) or delegate work to separate threads
    - Exceptions are logged but don't stop coordination
    - Applications must handle thread safety (use locks for shared state)

    Threading Model (all daemon threads):
    - heartbeat_thread: ALL nodes send periodic heartbeats
    - health_thread: ALL nodes monitor own health status
    - refresh_thread: FOLLOWERS watch for token reassignments
    - monitor_thread: LEADER ONLY - detects dead nodes
    - rebalance_thread: LEADER ONLY - rebalances on membership changes

    Leader vs Follower Responsibilities:
    - Leader: Distributes tokens, monitors dead nodes, triggers rebalancing
    - Follower: Watches for token reassignments, can become leader if elected
    """

    # ============================================================
    # INITIALIZATION
    # ============================================================

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
        """Initialize job sync manager with hybrid coordination support.

        Args:
            node_name: Unique identifier for this node
            config: Configuration module (coordination params read from config.sync.coordination if coordination_config not provided)
            date: Processing date (defaults to today)
            wait_on_enter: Seconds to wait for cluster formation (default 120)
            wait_on_exit: Seconds to wait before cleanup (default 0)
            lock_provider: Callback(job) to register task locks during __enter__
            clear_existing_locks: If True, clear locks created by this node before invoking lock_provider
            coordination_config: Optional CoordinationConfig to override config module settings
            connection_string: Optional SQLAlchemy connection string (for testing, defaults to building from config)
            on_tokens_added: Callback(token_ids: set[int]) invoked when tokens are assigned to this node
            on_tokens_removed: Callback(token_ids: set[int]) invoked when tokens are removed from this node
        """
        self._state = JobState.INITIALIZING
        self.node_name = node_name
        self._wait_on_enter = int(abs(wait_on_enter))
        self._wait_on_exit = int(abs(wait_on_exit))
        self._tasks = []
        self._nodes = [{'node': self.node_name}]

        if date is None:
            date = datetime.date.today()
        elif isinstance(date, datetime.datetime):
            date = date.date()
        elif not isinstance(date, datetime.date):
            raise TypeError(f'date must be None, datetime.date, or datetime.datetime, got {type(date).__name__}')
        self._date = datetime.date(date.year, date.month, date.day)

        self._config = config
        self._tables = get_table_names(config)

        if connection_string is None:
            connection_string = self._build_connection_string(config)
        self._engine = create_engine(connection_string, pool_pre_ping=True, pool_size=10, max_overflow=5)

        if coordination_config:
            self._coordination_enabled = coordination_config.enabled
            self._total_tokens = coordination_config.total_tokens
            self._heartbeat_interval = coordination_config.heartbeat_interval_sec
            self._heartbeat_timeout = coordination_config.heartbeat_timeout_sec
            self._rebalance_check_interval = coordination_config.rebalance_check_interval_sec
            self._dead_node_check_interval = coordination_config.dead_node_check_interval_sec
            self._token_refresh_initial = coordination_config.token_refresh_initial_interval_sec
            self._token_refresh_steady = coordination_config.token_refresh_steady_interval_sec
            self._locks_enabled = coordination_config.locks_enabled
            self._leader_lock_timeout = coordination_config.leader_lock_timeout_sec
            self._health_check_interval = coordination_config.health_check_interval_sec
        else:
            coord = config.sync.coordination
            self._coordination_enabled = getattr(coord, 'enabled', True)
            self._total_tokens = getattr(coord, 'total_tokens', 10000)
            self._heartbeat_interval = getattr(coord, 'heartbeat_interval_sec', 5)
            self._heartbeat_timeout = getattr(coord, 'heartbeat_timeout_sec', 15)
            self._rebalance_check_interval = getattr(coord, 'rebalance_check_interval_sec', 30)
            self._dead_node_check_interval = getattr(coord, 'dead_node_check_interval_sec', 10)
            self._token_refresh_initial = getattr(coord, 'token_refresh_initial_interval_sec', 5)
            self._token_refresh_steady = getattr(coord, 'token_refresh_steady_interval_sec', 30)
            self._locks_enabled = getattr(coord, 'locks_enabled', True)
            self._leader_lock_timeout = getattr(coord, 'leader_lock_timeout_sec', 30)
            self._health_check_interval = getattr(coord, 'health_check_interval_sec', 30)

        self._lock_provider = lock_provider
        self._clear_existing_locks = clear_existing_locks
        self._created_on = datetime.datetime.now(datetime.timezone.utc)

        self._my_tokens = set()
        self._token_version = 0
        self._shutdown = False
        self._shutdown_event = threading.Event()
        self._last_heartbeat_sent = None
        self._heartbeat_thread = None
        self._health_thread = None
        self._monitor_thread = None
        self._rebalance_thread = None
        self._refresh_thread = None

        self._stale_leader_lock_age = coordination_config.stale_leader_lock_age_sec if coordination_config else 300

        self._on_tokens_added = on_tokens_added
        self._on_tokens_removed = on_tokens_removed

        if not self._coordination_enabled and (on_tokens_added or on_tokens_removed):
            logger.warning('Token callbacks provided but coordination_enabled=False - callbacks will never fire')

        ensure_database_ready(self._engine, config, coordination_enabled=self._coordination_enabled)

    def _build_connection_string(self, config: ModuleType) -> str:
        """Build PostgreSQL connection string from config.
        """
        sql_config = config.sync.sql
        return (
            f'postgresql+psycopg://{sql_config.user}:{sql_config.passwd}'
            f'@{sql_config.host}:{sql_config.port}/{sql_config.dbname}'
        )

    # ============================================================
    # CONTEXT MANAGER LIFECYCLE
    # ============================================================

    def __enter__(self):
        """Enter context and perform coordination setup.

        Phases:
        1. Cleanup any stale state from previous runs
        2. Register this node and start heartbeat
        3. Invoke lock_provider callback (if provided)
        4. Wait for cluster formation (wait_on_enter seconds)
        5. Elect leader (oldest node by created_on timestamp)
        6. Leader distributes tokens, followers wait
        7. Read token assignments for this node
        8. Start monitoring threads (leader vs follower differ)
        """
        self._state = JobState.INITIALIZING
        self.__cleanup()

        if self._coordination_enabled:
            logger.info(f'Starting {self.node_name} in coordination mode')

            self._register_node()
            self._start_heartbeat_thread()
            self._start_health_monitor()
            if self._lock_provider and self._locks_enabled:
                if self._clear_existing_locks:
                    logger.info(f'Clearing existing locks created by {self.node_name}')
                    self.clear_locks_by_creator(self.node_name)
                logger.info('Invoking lock_provider callback')
                self._lock_provider(self)

            self._state = JobState.WAITING_FOR_CLUSTER
            logger.info(f'Waiting {self._wait_on_enter}s for cluster formation...')
            if self._shutdown_event.wait(timeout=self._wait_on_enter):
                logger.info('Shutdown requested during cluster wait')
                return self

            self._nodes = self.get_active_nodes()
            logger.info(f'Active nodes: {[n["name"] for n in self._nodes]}')

            self._state = JobState.ELECTING_LEADER
            leader = self._elect_leader()
            is_leader = (leader == self.node_name)
            logger.info(f'Leader elected: {leader}, am_i_leader={is_leader}')

            self._state = JobState.DISTRIBUTING_TOKENS
            if is_leader:
                logger.info('I am leader, distributing tokens...')
                self._distribute_tokens_safe()

            logger.info('Waiting for token distribution...')
            self._wait_for_token_distribution()

            self._my_tokens, self._token_version = self._get_my_tokens_versioned()
            logger.info(f'Node {self.node_name} assigned {len(self._my_tokens)} tokens, version {self._token_version}')

            if self._my_tokens and self._on_tokens_added:
                def invoke_initial_callback():
                    try:
                        logger.info(f'Invoking on_tokens_added for {len(self._my_tokens)} initial tokens')
                        start = time.time()
                        self._on_tokens_added(self._my_tokens.copy())
                        duration_ms = int((time.time() - start) * 1000)
                        logger.info(f'on_tokens_added completed in {duration_ms}ms')
                    except Exception as e:
                        logger.error(f'on_tokens_added callback failed during initialization: {e}')

                callback_thread = threading.Thread(
                    target=invoke_initial_callback,
                    daemon=True,
                    name=f'initial-callback-{self.node_name}'
                )
                callback_thread.start()
                callback_thread.join()

            self._start_token_refresh_thread()
            if is_leader:
                logger.info('Starting leader monitoring threads...')
                self._start_dead_node_monitor()
                self._start_rebalance_monitor()

            self._state = JobState.RUNNING

        return self

    def __exit__(self, exc_ty, exc_val, tb):
        """Exit context and cleanup coordination resources.

        Shutdown sequence:
        1. Set shutdown flag to stop all threads
        2. Join all background threads with timeout
        3. Optional wait_on_exit delay
        4. Cleanup database tables
        5. Close database connection
        """
        self._state = JobState.SHUTTING_DOWN
        logger.debug(f'Exiting {self.node_name} context')

        if exc_ty:
            logger.error(exc_val)
        if self._coordination_enabled:
            self._shutdown = True
            self._shutdown_event.set()
            threads = [
                ('heartbeat', self._heartbeat_thread),
                ('health', self._health_thread),
                ('refresh', self._refresh_thread),
                ('monitor', self._monitor_thread),
                ('rebalance', self._rebalance_thread),
            ]

            for name, thread in threads:
                if thread and thread.is_alive():
                    logger.debug(f'Joining {name} thread...')
                    thread.join(timeout=10)
                    if thread.is_alive():
                        logger.warning(f'{name} thread did not stop within timeout')

        if self._wait_on_exit:
            logger.debug(f'Sleeping {self._wait_on_exit} seconds...')
            time.sleep(self._wait_on_exit)

        self.__cleanup()

        with contextlib.suppress(Exception):
            self._engine.dispose()

    @property
    def nodes(self):
        """Safely expose internal nodes (for name matching, for example).
        """
        return deepcopy(self._nodes)

    @property
    def my_tokens(self) -> set[int]:
        """Get current token IDs owned by this node (read-only copy).
        """
        return self._my_tokens.copy()

    @property
    def token_version(self) -> int:
        """Get current token distribution version.
        """
        return self._token_version

    def set_claim(self, item):
        """Claim an item or items and publish to database.
        """
        with self._engine.connect() as conn:
            if isinstance(item, Iterable) and not isinstance(item, str):
                rows = [{'node': self.node_name, 'created_on': datetime.datetime.now(datetime.timezone.utc), 'item': str(i)} for i in item]
                for row in rows:
                    sql = f'INSERT INTO {self._tables["Claim"]} (node, item, created_on) VALUES (:node, :item, :created_on) ON CONFLICT DO NOTHING'
                    conn.execute(text(sql), row)
            else:
                sql = f'INSERT INTO {self._tables["Claim"]} (node, item, created_on) VALUES (:node, :item, :created_on) ON CONFLICT DO NOTHING'
                conn.execute(text(sql), {'node': self.node_name, 'item': str(item), 'created_on': datetime.datetime.now(datetime.timezone.utc)})
            conn.commit()

    def add_task(self, task: Task):
        """Add task to processing queue if claimable in coordination mode.
        """
        if self._coordination_enabled and not self.can_claim_task(task):
            logger.debug(f'Task {task.id} rejected (token not owned)')
            return

        self._tasks.append((task, datetime.datetime.now(datetime.timezone.utc)))
        if self._coordination_enabled:
            self.set_claim(task.id)

    def write_audit(self) -> None:
        """Write accumulated tasks to audit table.
        """
        if not self._tasks:
            return

        with self._engine.connect() as conn:
            for task, _ in self._tasks:
                sql = f'INSERT INTO {self._tables["Audit"]} (date, node, item, created_on) VALUES (:date, :node, :item, now())'
                conn.execute(text(sql), {
                    'date': self._date,
                    'node': self.node_name,
                    'item': str(task.id)
                })
            conn.commit()

        logger.debug(f'Flushed {len(self._tasks)} task to {self._tables["Audit"]}')
        self._tasks.clear()

    def get_audit(self) -> list[dict]:
        """Get audit records for current date.
        """
        sql = f'SELECT node, item FROM {self._tables["Audit"]} WHERE date = :date'
        with self._engine.connect() as conn:
            result = conn.execute(text(sql), {'date': self._date})
            return [{'node': row[0], 'item': row[1]} for row in result]

    def can_claim_task(self, task: Task) -> bool:
        """Check if this node can claim the given task based on token ownership.
        """
        if not self._coordination_enabled:
            return True

        token_id = self._task_to_token(task.id)
        can_claim = token_id in self._my_tokens

        if not can_claim:
            logger.debug(f'Cannot claim task {task.id} (token {token_id} not owned by {self.node_name})')

        return can_claim

    def task_to_token(self, task_id: Hashable) -> int:
        """Map task ID to token ID using consistent hashing.

        This is a pure function - same task_id always maps to same token_id.
        Applications can call this to build efficient lookup structures.
        """
        return self._task_to_token(task_id)

    def get_task_ids_for_token(self, token_id: int, all_task_ids: list[Hashable]) -> list[Hashable]:
        """Get task IDs that hash to the given token.

        Note: Performs linear scan. For large task sets, consider caching
        the result or building inverse lookup using task_to_token().

        Args:
            token_id: Token ID to match
            all_task_ids: Complete list of possible task IDs

        Returns
            List of task IDs that hash to this token
        """
        return [task_id for task_id in all_task_ids if self._task_to_token(task_id) == token_id]

    def get_my_task_ids(self, all_task_ids: list[Hashable]) -> list[Hashable]:
        """Get all task IDs owned by this node based on current token allocation.

        Args:
            all_task_ids: Complete list of possible task IDs

        Returns
            List of task IDs this node currently owns
        """
        return [task_id for task_id in all_task_ids if self._task_to_token(task_id) in self._my_tokens]

    # ============================================================
    # PUBLIC API - Lock Management
    # ============================================================

    def clear_locks_by_creator(self, creator: str) -> int:
        """Clear all locks created by specified node.

        Args:
            creator: Node name that created the locks

        Returns
            Number of locks removed
        """
        sql = f'DELETE FROM {self._tables["Lock"]} WHERE created_by = :creator'
        with self._engine.connect() as conn:
            result = conn.execute(text(sql), {'creator': creator})
            conn.commit()
            rows = result.rowcount
        logger.info(f'Cleared {rows} locks created by {creator}')
        return rows

    def clear_all_locks(self) -> int:
        """Clear all locks from the system (use with caution).

        Returns
            Number of locks removed
        """
        sql = f'DELETE FROM {self._tables["Lock"]}'
        with self._engine.connect() as conn:
            result = conn.execute(text(sql))
            conn.commit()
            rows = result.rowcount
        logger.warning(f'Cleared ALL {rows} locks from system')
        return rows

    def list_locks(self) -> list[dict]:
        """List all active (non-expired) locks.

        Returns
            List of lock records with token_id, node_pattern, reason, created_by, etc.
        """
        sql = f"""
        SELECT token_id, node_pattern, reason, created_at, created_by, expires_at
        FROM {self._tables["Lock"]}
        WHERE expires_at IS NULL OR expires_at > :now
        ORDER BY created_at DESC
        """
        with self._engine.connect() as conn:
            result = conn.execute(text(sql), {'now': datetime.datetime.now(datetime.timezone.utc)})
            return [dict(row._mapping) for row in result]

    def register_task_lock(self, task_id: Hashable, node_pattern: str, reason: str = None, expires_in_days: int = None) -> None:
        """Register a lock to pin task to specific node pattern."""
        token_id = self._task_to_token(task_id)
        expires_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=expires_in_days) if expires_in_days else None

        sql = f"""
        INSERT INTO {self._tables["Lock"]} (token_id, node_pattern, reason, created_at, created_by, expires_at)
        VALUES (:token_id, :pattern, :reason, :created_at, :created_by, :expires_at)
        ON CONFLICT (token_id) DO NOTHING
        """
        with self._engine.connect() as conn:
            conn.execute(text(sql), {
                'token_id': token_id,
                'pattern': node_pattern,
                'reason': reason,
                'created_at': datetime.datetime.now(datetime.timezone.utc),
                'created_by': self.node_name,
                'expires_at': expires_at
            })
            conn.commit()
        logger.debug(f'Registered lock: task {task_id} -> token {token_id} -> pattern "{node_pattern}"')

    def register_task_locks_bulk(self, locks: list[tuple[Hashable, str, str]]):
        """Register multiple locks in batch.

        Args:
            locks: List of (task_id, node_pattern, reason) tuples
        """
        if not locks:
            return
        rows = [
            {
                'token_id': self._task_to_token(task_id),
                'node_pattern': pattern,
                'reason': reason,
                'created_at': datetime.datetime.now(datetime.timezone.utc),
                'created_by': self.node_name,
                'expires_at': None
            }
            for task_id, pattern, reason in locks
        ]

        sql = f"""
        INSERT INTO {self._tables["Lock"]} (token_id, node_pattern, reason, created_at, created_by, expires_at)
        VALUES (:token_id, :node_pattern, :reason, :created_at, :created_by, :expires_at)
        ON CONFLICT (token_id) DO NOTHING
        """

        with self._engine.connect() as conn:
            for row in rows:
                conn.execute(text(sql), row)
            conn.commit()

        logger.info(f'Registered {len(locks)} locks in bulk')

    # ============================================================
    # PUBLIC API - Health & Status
    # ============================================================

    def am_i_healthy(self) -> bool:
        """Check if node is healthy with current heartbeat and database access.
        """
        try:
            if self._last_heartbeat_sent:
                age = datetime.datetime.now(datetime.timezone.utc) - self._last_heartbeat_sent
                if age.total_seconds() > self._heartbeat_timeout:
                    return False
            with self._engine.connect() as conn:
                conn.execute(text('SELECT 1'))
            return True
        except Exception:
            return False

    def am_i_leader(self) -> bool:
        """Check if this node is the elected leader.
        """
        try:
            leader = self._elect_leader()
            return leader == self.node_name
        except RuntimeError:
            return False

    def get_active_nodes(self) -> list[dict]:
        """Get list of active nodes (heartbeat within timeout threshold)."""
        cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=self._heartbeat_timeout)
        sql = f"""
        SELECT name, created_on, last_heartbeat
        FROM {self._tables["Node"]}
        WHERE last_heartbeat > :cutoff
        ORDER BY created_on ASC, name ASC
        """
        with self._engine.connect() as conn:
            result = conn.execute(text(sql), {'cutoff': cutoff})
            return [{'name': row[0], 'created_on': row[1], 'last_heartbeat': row[2]} for row in result]

    def get_dead_nodes(self) -> list[str]:
        """Get list of dead nodes (heartbeat older than timeout threshold)."""
        cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=self._heartbeat_timeout)
        sql = f"""
        SELECT name
        FROM {self._tables["Node"]}
        WHERE last_heartbeat <= :cutoff OR last_heartbeat IS NULL
        """
        with self._engine.connect() as conn:
            result = conn.execute(text(sql), {'cutoff': cutoff})
            return [row[0] for row in result]

    # ============================================================
    # NODE COORDINATION (ALL NODES)
    # ============================================================

    def _register_node(self) -> None:
        """Register node with initial heartbeat."""
        sql = f"""
        INSERT INTO {self._tables["Node"]} (name, created_on, last_heartbeat)
        VALUES (:name, :created_on, :heartbeat)
        ON CONFLICT (name) DO UPDATE
        SET last_heartbeat = EXCLUDED.last_heartbeat
        """
        with self._engine.connect() as conn:
            conn.execute(text(sql), {
                'name': self.node_name,
                'created_on': self._created_on,
                'heartbeat': datetime.datetime.now(datetime.timezone.utc)
            })
            conn.commit()
        logger.info(f'Node {self.node_name} registered with heartbeat')

    def _start_heartbeat_thread(self) -> None:
        """Start background thread that sends heartbeat every N seconds."""
        self._last_heartbeat_sent = datetime.datetime.now(datetime.timezone.utc)

        def heartbeat_loop():
            conn = self._engine.connect()
            try:
                while not self._shutdown:
                    try:
                        sql = f"""
                        UPDATE {self._tables["Node"]}
                        SET last_heartbeat = :heartbeat
                        WHERE name = :name
                        """
                        heartbeat_time = datetime.datetime.now(datetime.timezone.utc)
                        conn.execute(text(sql), {'heartbeat': heartbeat_time, 'name': self.node_name})
                        conn.commit()
                        self._last_heartbeat_sent = heartbeat_time
                        logger.debug(f'Heartbeat sent by {self.node_name}')
                    except Exception as e:
                        logger.error(f'Heartbeat failed for {self.node_name}: {e}')
                        try:
                            conn.close()
                        except Exception:
                            pass
                        conn = self._engine.connect()

                    if self._shutdown_event.wait(timeout=self._heartbeat_interval):
                        break
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

        self._heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True, name=f'heartbeat-{self.node_name}')
        self._heartbeat_thread.start()
        logger.info(f'Heartbeat thread started for {self.node_name}')

    def _start_health_monitor(self) -> None:
        """Start health monitoring thread that checks heartbeat and database connectivity."""
        def health_loop():
            conn = self._engine.connect()
            try:
                while not self._shutdown:
                    try:
                        if self._last_heartbeat_sent:
                            age_seconds = (datetime.datetime.now(datetime.timezone.utc) - self._last_heartbeat_sent).total_seconds()
                            if age_seconds > self._heartbeat_timeout:
                                logger.error(f'Heartbeat thread appears dead (>{self._heartbeat_timeout}s old)')
                        try:
                            conn.execute(text('SELECT 1'))
                        except Exception as e:
                            logger.error(f'Database connectivity test failed: {e}')
                            try:
                                conn.close()
                            except Exception:
                                pass
                            conn = self._engine.connect()

                    except Exception as e:
                        logger.error(f'Health monitor error: {e}')

                    if self._shutdown_event.wait(timeout=self._health_check_interval):
                        break
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

        self._health_thread = threading.Thread(target=health_loop, daemon=True, name=f'health-{self.node_name}')
        self._health_thread.start()
        logger.debug(f'Health monitor started for {self.node_name}')

    # ============================================================
    # LEADER COORDINATION (LEADER ONLY)
    # ============================================================

    def _elect_leader(self) -> str:
        """Elect leader as node with oldest registration timestamp.
        """
        active_nodes = self.get_active_nodes()
        if not active_nodes:
            raise RuntimeError('No active nodes for leader election')

        leader = active_nodes[0]['name']
        return leader

    def _acquire_leader_lock(self, operation: str) -> bool:
        """Acquire leader lock with timeout and stale lock recovery.
        """
        start_time = time.time()
        while time.time() - start_time < self._leader_lock_timeout:
            try:
                with self._engine.connect() as conn:
                    sql = f"""
                    INSERT INTO {self._tables["LeaderLock"]} (singleton, node, acquired_at, operation)
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

                    sql = f'SELECT node, acquired_at FROM {self._tables["LeaderLock"]} WHERE singleton = 1'
                    result = conn.execute(text(sql))
                    lock_row = result.first()

                    if lock_row:
                        acquired_at = lock_row[1]
                        age = datetime.datetime.now(datetime.timezone.utc) - acquired_at
                        if age.total_seconds() > self._stale_leader_lock_age:
                            logger.warning(f'Stale leader lock detected (age: {age.total_seconds()}s), forcing release')
                            conn.execute(text(f'DELETE FROM {self._tables["LeaderLock"]} WHERE singleton = 1'))
                            conn.commit()
                            continue

                time.sleep(0.5)

            except Exception as e:
                logger.error(f'Leader lock acquisition failed: {e}')
                time.sleep(0.5)

        logger.warning(f'Leader lock acquisition timeout for {self.node_name}')
        return False

    def _release_leader_lock(self) -> None:
        """Release leader lock."""
        try:
            sql = f'DELETE FROM {self._tables["LeaderLock"]} WHERE node = :node'
            with self._engine.connect() as conn:
                conn.execute(text(sql), {'node': self.node_name})
                conn.commit()
            logger.debug(f'Leader lock released by {self.node_name}')
        except Exception as e:
            logger.error(f'Leader lock release failed: {e}')

    def _distribute_tokens_safe(self, total_tokens: int = None) -> None:
        """Leader distributes tokens with leader lock protection."""
        total_tokens = total_tokens or self._total_tokens

        if not self._acquire_leader_lock('distribute'):
            logger.warning('Another leader is distributing tokens, skipping')
            return

        try:
            self._distribute_tokens_minimal_move(total_tokens)
        finally:
            self._release_leader_lock()

    def _get_active_locks(self) -> dict[int, str]:
        """Get active (non-expired) locks as token_id -> node_pattern mapping.

        Returns dictionary mapping token IDs to node patterns. Expired locks
        are removed as a side effect.
        """
        with self._engine.connect() as conn:
            sql = f'SELECT token_id, node_pattern, expires_at FROM {self._tables["Lock"]}'
            result = conn.execute(text(sql))
            records = [dict(row._mapping) for row in result]

            locked_tokens = {}
            current_time = datetime.datetime.now(datetime.timezone.utc)
            for row in records:
                expires_at = row['expires_at']
                if expires_at and expires_at < current_time:
                    conn.execute(text(f'DELETE FROM {self._tables["Lock"]} WHERE token_id = :token_id'),
                                {'token_id': row['token_id']})
                    continue
                locked_tokens[row['token_id']] = row['node_pattern']

            conn.commit()
            return locked_tokens

    def _distribute_tokens_minimal_move(self, total_tokens: int) -> None:
        """Distribute tokens using minimal-move algorithm."""
        start_time = time.time()
        active_nodes = self.get_active_nodes()
        active_node_names = [n["name"] for n in active_nodes]
        nodes_count = len(active_node_names)

        if nodes_count == 0:
            logger.error('No active nodes for token distribution')
            return

        with self._engine.connect() as conn:
            sql = f'SELECT token_id, node FROM {self._tables["Token"]}'
            result = conn.execute(text(sql))
            current_assignments = {row[0]: row[1] for row in result}

        locked_tokens = self._get_active_locks()

        for token_id, pattern in locked_tokens.items():
            matching_nodes = [n for n in active_node_names if self._matches_pattern(n, pattern)]
            if not matching_nodes:
                logger.warning(f'Token {token_id} locked to pattern "{pattern}" but no matching nodes found')

        new_assignments, tokens_moved = compute_minimal_move_distribution(
            total_tokens, active_node_names, current_assignments, locked_tokens, self._matches_pattern)

        with self._engine.connect() as conn:
            result = conn.execute(text(f'SELECT MAX(version) FROM {self._tables["Token"]}'))
            current_version = result.scalar() or 0
            new_version = current_version + 1

            conn.execute(text(f'DELETE FROM {self._tables["Token"]}'))

            assignment_time = datetime.datetime.now(datetime.timezone.utc)
            for tid, node in new_assignments.items():
                sql = f'INSERT INTO {self._tables["Token"]} (token_id, node, assigned_at, version) VALUES (:token_id, :node, :assigned_at, :version)'
                conn.execute(text(sql), {
                    'token_id': tid,
                    'node': node,
                    'assigned_at': assignment_time,
                    'version': new_version
                })

            conn.commit()

        duration_ms = int((time.time() - start_time) * 1000)
        self._log_rebalance('distribution', len(active_node_names), len(active_node_names), tokens_moved, duration_ms)

        logger.info(f'Token distribution complete: {len(new_assignments)} tokens across {nodes_count} nodes, '
                    f'{tokens_moved} tokens moved, v{new_version}, {duration_ms}ms')

    def _start_dead_node_monitor(self) -> None:
        """Start leader monitoring thread for dead node detection."""
        def monitor_loop():
            conn = self._engine.connect()
            try:
                while not self._shutdown:
                    try:
                        if not self.am_i_leader():
                            logger.debug('No longer leader, stopping dead node monitor')
                            break

                        cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=self._heartbeat_timeout)
                        sql = f"""
                        SELECT name
                        FROM {self._tables["Node"]}
                        WHERE last_heartbeat <= :cutoff OR last_heartbeat IS NULL
                        """

                        result = conn.execute(text(sql), {'cutoff': cutoff})
                        dead_nodes = [row[0] for row in result]

                        if dead_nodes:
                            logger.warning(f'Detected dead nodes: {dead_nodes}')
                            if self._acquire_rebalance_lock('dead_node_monitor'):
                                try:
                                    for node in dead_nodes:
                                        conn.execute(text(f'DELETE FROM {self._tables["Node"]} WHERE name = :name'),
                                                    {'name': node})
                                        conn.execute(text(f'DELETE FROM {self._tables["Claim"]} WHERE node = :node'),
                                                    {'node': node})
                                        logger.info(f'Removed dead node: {node}')
                                    conn.commit()
                                    self._distribute_tokens_safe()
                                finally:
                                    self._release_rebalance_lock()
                            else:
                                logger.debug('Rebalance already in progress, skipping dead node cleanup')

                    except Exception as e:
                        logger.error(f'Dead node monitor failed: {e}', exc_info=True)
                        try:
                            conn.close()
                        except Exception:
                            pass
                        conn = self._engine.connect()

                    if self._shutdown_event.wait(timeout=self._dead_node_check_interval):
                        break
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

        self._monitor_thread = threading.Thread(target=monitor_loop, daemon=True, name=f'dead-node-monitor-{self.node_name}')
        self._monitor_thread.start()
        logger.info('Dead node monitor started')

    def _acquire_rebalance_lock(self, started_by: str) -> bool:
        """Acquire rebalance lock."""
        sql = f"""
        UPDATE {self._tables["RebalanceLock"]}
        SET in_progress = TRUE, started_at = :started_at, started_by = :started_by
        WHERE singleton = 1 AND in_progress = FALSE
        """
        with self._engine.connect() as conn:
            result = conn.execute(text(sql), {'started_at': datetime.datetime.now(datetime.timezone.utc), 'started_by': started_by})
            conn.commit()
            rows_affected = result.rowcount

        success = rows_affected > 0

        if success:
            logger.debug(f'Rebalance lock acquired by {started_by}')
        else:
            logger.debug('Rebalance lock already held')

        return success

    def _release_rebalance_lock(self) -> None:
        """Release rebalance lock."""
        sql = f"""
        UPDATE {self._tables["RebalanceLock"]}
        SET in_progress = FALSE, started_at = NULL, started_by = NULL
        WHERE singleton = 1
        """
        with self._engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()
        logger.debug('Rebalance lock released')

    def _start_rebalance_monitor(self) -> None:
        """Start leader monitoring thread for membership changes."""
        def rebalance_loop():
            conn = self._engine.connect()
            try:
                cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=self._heartbeat_timeout)
                sql = f"""
                SELECT name, created_on, last_heartbeat
                FROM {self._tables["Node"]}
                WHERE last_heartbeat > :cutoff
                ORDER BY created_on ASC, name ASC
                """

                result = conn.execute(text(sql), {'cutoff': cutoff})
                last_node_count = len(list(result))

                while not self._shutdown:
                    try:
                        if not self.am_i_leader():
                            logger.debug('No longer leader, stopping rebalance monitor')
                            break

                        cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=self._heartbeat_timeout)
                        result = conn.execute(text(sql), {'cutoff': cutoff})
                        current_count = len(list(result))

                        if current_count != last_node_count:
                            logger.info(f'Node count changed: {last_node_count} -> {current_count}')

                            if self._acquire_rebalance_lock('membership_monitor'):
                                try:
                                    self._distribute_tokens_safe()
                                    last_node_count = current_count
                                finally:
                                    self._release_rebalance_lock()
                            else:
                                logger.debug('Rebalance already in progress, will retry next cycle')

                    except Exception as e:
                        logger.error(f'Rebalance monitor failed: {e}', exc_info=True)
                        try:
                            conn.close()
                        except Exception:
                            pass
                        conn = self._engine.connect()

                    if self._shutdown_event.wait(timeout=self._rebalance_check_interval):
                        break
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

        self._rebalance_thread = threading.Thread(target=rebalance_loop, daemon=True, name=f'rebalance-monitor-{self.node_name}')
        self._rebalance_thread.start()
        logger.info('Rebalance monitor started')

    # ============================================================
    # FOLLOWER COORDINATION (FOLLOWERS ONLY)
    # ============================================================

    def _start_token_refresh_thread(self) -> None:
        """Start follower thread that detects token reassignments."""
        def refresh_loop():
            conn = self._engine.connect()
            try:
                start_time = time.time()
                check_count = 0

                while not self._shutdown:
                    try:
                        elapsed = time.time() - start_time
                        check_interval = self._token_refresh_initial if elapsed < 300 else self._token_refresh_steady

                        sql = f'SELECT token_id, version FROM {self._tables["Token"]} WHERE node = :node'
                        result = conn.execute(text(sql), {'node': self.node_name})
                        records = [dict(row._mapping) for row in result]

                        if not records:
                            new_tokens, new_version = set(), 0
                        else:
                            new_tokens = {row['token_id'] for row in records}
                            new_version = records[0]['version'] if records else 0

                        if self._token_version > 0 and new_version != self._token_version:
                            added = new_tokens - self._my_tokens
                            removed = self._my_tokens - new_tokens

                            logger.warning(f'Token rebalance detected: v{self._token_version}->v{new_version}, '
                                           f'+{len(added)} -{len(removed)} tokens')

                            self._my_tokens = new_tokens
                            self._token_version = new_version

                            if removed and self._on_tokens_removed:
                                try:
                                    start = time.time()
                                    self._on_tokens_removed(removed)
                                    duration_ms = int((time.time() - start) * 1000)
                                    logger.info(f'on_tokens_removed completed in {duration_ms}ms for {len(removed)} tokens')
                                except Exception as e:
                                    logger.error(f'on_tokens_removed callback failed: {e}')

                            if added and self._on_tokens_added:
                                try:
                                    start = time.time()
                                    self._on_tokens_added(added)
                                    duration_ms = int((time.time() - start) * 1000)
                                    logger.info(f'on_tokens_added completed in {duration_ms}ms for {len(added)} tokens')
                                except Exception as e:
                                    logger.error(f'on_tokens_added callback failed: {e}')

                            start_time = time.time()

                        check_count += 1
                        if check_count % 10 == 0:
                            logger.debug(f'Token refresh #{check_count}: {len(self._my_tokens)} tokens, v{self._token_version}')

                    except Exception as e:
                        logger.error(f'Token refresh failed: {e}', exc_info=True)
                        try:
                            conn.close()
                        except Exception:
                            pass
                        conn = self._engine.connect()

                    if self._shutdown_event.wait(timeout=check_interval):
                        break
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

        self._refresh_thread = threading.Thread(target=refresh_loop, daemon=True, name=f'token-refresh-{self.node_name}')
        self._refresh_thread.start()
        logger.debug('Token refresh thread started')

    # ============================================================
    # TOKEN MANAGEMENT (INTERNAL)
    # ============================================================

    def _task_to_token(self, task_id: Hashable) -> int:
        """Hash task_id to token_id."""
        task_str = str(task_id)
        hash_obj = hashlib.md5(task_str.encode())
        hash_int = int(hash_obj.hexdigest(), 16)
        token_id = hash_int % self._total_tokens
        return token_id

    def _matches_pattern(self, node_name: str, pattern: str) -> bool:
        """Check if node_name matches SQL LIKE pattern.
        """
        if node_name == pattern:
            return True
        regex_pattern = pattern.replace('%', '<<PERCENT>>')
        regex_pattern = regex_pattern.replace('_', '<<UNDERSCORE>>')
        regex_pattern = re.escape(regex_pattern)
        regex_pattern = regex_pattern.replace('<<PERCENT>>', '.*')
        regex_pattern = regex_pattern.replace('<<UNDERSCORE>>', '.')
        regex_pattern = f'^{regex_pattern}$'

        return re.match(regex_pattern, node_name) is not None

    def _get_my_tokens(self) -> set[int]:
        """Get token IDs assigned to this node."""
        sql = f'SELECT token_id FROM {self._tables["Token"]} WHERE node = :node'
        with self._engine.connect() as conn:
            result = conn.execute(text(sql), {'node': self.node_name})
            tokens = {row[0] for row in result}
        logger.debug(f'Node {self.node_name} owns {len(tokens)} tokens')
        return tokens

    def _get_my_tokens_versioned(self) -> tuple[set[int], int]:
        """Get token IDs and version assigned to this node.
        """
        sql = f'SELECT token_id, version FROM {self._tables["Token"]} WHERE node = :node'
        with self._engine.connect() as conn:
            result = conn.execute(text(sql), {'node': self.node_name})
            records = [dict(row._mapping) for row in result]

        if not records:
            return set(), 0

        tokens = {row['token_id'] for row in records}
        version = records[0]['version'] if records else 0

        logger.debug(f'Node {self.node_name} owns {len(tokens)} tokens, version {version}')
        return tokens, version

    def _wait_for_token_distribution(self, timeout_sec: int = 30) -> None:
        """Wait for initial token distribution to complete."""
        start = time.time()
        while time.time() - start < timeout_sec:
            with self._engine.connect() as conn:
                result = conn.execute(text(f'SELECT COUNT(*) FROM {self._tables["Token"]}'))
                count = result.scalar()

            if count > 0:
                logger.info(f'Token distribution detected ({count} tokens)')
                return

            time.sleep(0.5)

        raise TimeoutError(f'Token distribution did not complete within {timeout_sec}s')

    def _log_rebalance(self, reason: str, nodes_before: int, nodes_after: int, tokens_moved: int, duration_ms: int) -> None:
        """Log rebalance event to audit table."""
        sql = f"""
        INSERT INTO {self._tables["Rebalance"]}
        (triggered_at, trigger_reason, leader_node, nodes_before, nodes_after, tokens_moved, duration_ms)
        VALUES (:triggered_at, :reason, :leader, :before, :after, :moved, :duration)
        """
        with self._engine.connect() as conn:
            conn.execute(text(sql), {
                'triggered_at': datetime.datetime.now(datetime.timezone.utc),
                'reason': reason,
                'leader': self.node_name,
                'before': nodes_before,
                'after': nodes_after,
                'moved': tokens_moved,
                'duration': duration_ms
            })
            conn.commit()

    # ============================================================
    # CLEANUP
    # ============================================================

    def __cleanup_node_table(self) -> None:
        """Remove node from Node table.
        """
        sql = f'DELETE FROM {self._tables["Node"]} WHERE name = :name'
        with self._engine.connect() as conn:
            conn.execute(text(sql), {'name': self.node_name})
            conn.commit()
        logger.debug(f'Cleared {self.node_name} from {self._tables["Node"]}')

    def __cleanup_check_table(self) -> None:
        """Remove node from Check table.
        """
        sql = f'DELETE FROM {self._tables["Check"]} WHERE node = :node'
        with self._engine.connect() as conn:
            conn.execute(text(sql), {'node': self.node_name})
            conn.commit()
        logger.debug(f'Cleaned {self.node_name} from {self._tables["Check"]}')

    def __cleanup_claim_table(self) -> None:
        """Remove node from Claim table.
        """
        sql = f'DELETE FROM {self._tables["Claim"]} WHERE node = :node'
        with self._engine.connect() as conn:
            conn.execute(text(sql), {'node': self.node_name})
            conn.commit()
        logger.debug(f'Cleaned {self.node_name} from {self._tables["Claim"]}')

    def __cleanup(self) -> None:
        """Cleanup tables and write audit log.
        """
        self.write_audit()
        self.__cleanup_node_table()
        self.__cleanup_check_table()
        self.__cleanup_claim_table()
