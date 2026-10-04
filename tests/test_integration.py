"""Integration tests for coordinated workflows and state management.

USE THIS FILE FOR:
- Integration tests requiring coordination between components
- State machine and event bus integration
- Callback and event handling
- Schema and database initialization
- Component interaction tests
"""
import datetime
import logging
import re
import threading
import time
from collections import Counter

import pytest
from fixtures import *  # noqa: F401, F403
from sqlalchemy import text

from jobsync import schema
from jobsync.client import CoordinationConfig, Job, JobState, Task
from jobsync.client import TokenRefreshMonitor

logger = logging.getLogger(__name__)


@pytest.fixture
def make_unentered_job(postgres):
    """Factory for coordinated Jobs the test never enters.

    Yields
    ------
    callable
        make(node_name, coordination_config) -> Job, built by create_job
        with wait_on_enter=0. Each job is exited at teardown, which stops
        the CoordinationMonitor thread Job.__init__ starts and disposes
        its engine.
    """
    jobs = []

    def make(node_name: str, coordination_config: CoordinationConfig) -> Job:
        job = create_job(node_name, postgres, coordination_config=coordination_config, wait_on_enter=0)
        jobs.append(job)
        return job

    yield make

    for job in jobs:
        job.__exit__(None, None, None)


@pytest.fixture
def token_refresh_gate(monkeypatch):
    """Skip every TokenRefreshMonitor check until the test sets the gate.

    Returns
    -------
    threading.Event
        Unset at start. While unset, TokenRefreshMonitor.check returns at
        once, so only Job.__enter__ can fill the token cache.
    """
    gate = threading.Event()
    original_check = TokenRefreshMonitor.check

    def gated_check(monitor: TokenRefreshMonitor) -> None:
        if gate.is_set():
            original_check(monitor)

    monkeypatch.setattr(TokenRefreshMonitor, 'check', gated_check)
    return gate


class TestLeaderElection:
    """Test leader election logic."""

    @clean_tables('Node')
    def test_oldest_node_elected(self, postgres, make_unentered_job):
        """Verify the node with the oldest created_on wins, whatever its name.

        Mutation: elect_leader ordered by name before created_on, or by
            created_on DESC.
        Oracle: node3 is inserted 20s old, older than node1 and node2, and
            sorts last by name.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)
        job = make_unentered_job('test', config)

        base_time = datetime.datetime.now(datetime.timezone.utc)

        insert_active_node(postgres, tables, 'node1', created_on=base_time - datetime.timedelta(seconds=5))
        insert_active_node(postgres, tables, 'node2', created_on=base_time - datetime.timedelta(seconds=10))
        insert_active_node(postgres, tables, 'node3', created_on=base_time - datetime.timedelta(seconds=20))

        leader = job.cluster.elect_leader()

        assert leader == 'node3', 'Oldest node should be elected leader'

    @clean_tables('Node')
    def test_name_tiebreaker(self, postgres, make_unentered_job):
        """Verify the alphabetically first name wins when created_on ties.

        Mutation: the name ASC tiebreak dropped from elect_leader or
            reversed to DESC.
        Oracle: three nodes share one created_on; node-a sorts first and
            is inserted second.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)
        job = make_unentered_job('test', config)

        same_time = datetime.datetime.now(datetime.timezone.utc)

        for name in ['node-c', 'node-a', 'node-b']:
            insert_active_node(postgres, tables, name, created_on=same_time)

        leader = job.cluster.elect_leader()

        assert leader == 'node-a', 'Alphabetically first node should win tiebreaker'

    @clean_tables('Node')
    def test_dead_nodes_filtered(self, postgres, make_unentered_job):
        """Verify a node with a stale heartbeat is skipped by leader election.

        Mutation: the last_heartbeat filter dropped from elect_leader.
        Oracle: stale node1 is inserted first, so it is older than node2
            and sorts first by name; only the heartbeat filter excludes it.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)
        job = make_unentered_job('test', config)

        insert_stale_node(postgres, tables, 'node1', heartbeat_age_seconds=30)
        insert_active_node(postgres, tables, 'node2')

        leader = job.cluster.elect_leader()

        assert leader == 'node2', 'Only alive nodes should be considered'


class TestCanClaimTask:
    """Test task claiming logic."""

    def test_can_claim_owned_token(self, make_unentered_job):
        """Verify a follower can claim a task whose token it owns.

        Mutation: TaskManager.can_claim tests 'not in my_tokens', or
            RUNNING_FOLLOWER dropped from JobStateMachine.can_claim_task.
        Oracle: the task is found by search to hash to token 5, the one
            owned token.
        """
        job = make_unentered_job('node1', get_coordination_config())

        token_id = 5
        job.tokens.my_tokens = {token_id}
        job.state_machine.state = JobState.RUNNING_FOLLOWER

        task_id = None
        for candidate in range(100000):
            if job.task_to_token(candidate) == token_id:
                task_id = candidate
                break
        assert task_id is not None, f'Could not find task_id mapping to token {token_id}'

        task = create_task(task_id)
        assert job.can_claim_task(task), 'Should be able to claim task with owned token'

    def test_cannot_claim_unowned_token(self, make_unentered_job):
        """Verify a follower cannot claim a task whose token it lacks.

        Mutation: can_claim checks a neighboring token (token_id + 1), or
            ignores the token set once the state allows claiming.
        Oracle: the node owns every token except the one task 123 hashes to.
        """
        coord_config = get_coordination_config()
        job = make_unentered_job('node1', coord_config)

        job.tokens.my_tokens = set(range(coord_config.total_tokens)) - {job.task_to_token(123)}
        job.state_machine.state = JobState.RUNNING_FOLLOWER

        task = create_task(123)
        assert not job.can_claim_task(task), 'Should not be able to claim task without token'


class TestTokenDistribution:
    """Test token distribution algorithm."""

    @clean_tables('Node')
    def test_even_distribution(self, postgres, make_unentered_job):
        """Verify a fresh distribution splits 99 tokens 33/33/33 across three nodes.

        Mutation: a receiver's deficit computed one short, so the leftover
            tokens fall through to the first node.
        Oracle: 99 / 3 = 33, hand-computed.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        current = datetime.datetime.now(datetime.timezone.utc)
        for i in range(1, 4):
            insert_active_node(postgres, tables, f'node{i}', created_on=current)

        job = make_unentered_job('node1', CoordinationConfig(total_tokens=99))

        job.tokens.distribute(job.locks, job.cluster)

        assignments = get_token_assignments(postgres, tables)
        assert sorted(assignments) == list(range(99)), 'Every token should be assigned'
        assert Counter(assignments.values()) == {'node1': 33, 'node2': 33, 'node3': 33}

    @clean_tables('Token', 'Lock', 'Node')
    def test_locked_tokens_respected(self, postgres, make_unentered_job):
        """Verify tokens locked to 'special-%' all go to the one matching node.

        Mutation: categorize_tokens_by_locks treats locked tokens as
            distributable.
        Oracle: unlocked, tokens 0-9 would go to node1, the first receiver
            by name; locked, all ten must go to special-alpha.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        current = datetime.datetime.now(datetime.timezone.utc)
        for name in ['node1', 'node2', 'special-alpha']:
            insert_active_node(postgres, tables, name, created_on=current)

        coord_config = CoordinationConfig(total_tokens=30)

        task_ids = find_task_ids_covering_all_tokens(coord_config)

        for token_id in range(10):
            insert_lock(postgres, tables, task_ids[token_id], ['special-%'], created_by='test')

        job = make_unentered_job('node1', coord_config)

        job.tokens.distribute(job.locks, job.cluster)

        assignments = get_token_assignments(postgres, tables)
        locked_correct = sum(1 for token_id in range(10) if assignments.get(token_id) == 'special-alpha')

        assert locked_correct == 10, f'All 10 locked tokens should be assigned to special-alpha (got {locked_correct})'

    @clean_tables('Lock', 'Token', 'Node')
    def test_locked_tokens_balanced_across_multiple_matching_nodes(self, postgres, make_unentered_job):
        """Verify locked tokens split evenly between two matching nodes, and unlocked ones across all five.

        Mutation: assign_locked_token returns eligible_nodes[0] instead of
            the least-loaded eligible node.
        Oracle: 20 locked tokens / 2 special nodes = 10 each; 30 unlocked
            tokens / 5 nodes = 6 each, hand-computed.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        current = datetime.datetime.now(datetime.timezone.utc)
        node_names = ['node1', 'node2', 'node3', 'special-alpha', 'special-beta']
        for name in node_names:
            insert_active_node(postgres, tables, name, created_on=current)

        coord_config = CoordinationConfig(total_tokens=50)

        task_ids = find_task_ids_covering_all_tokens(coord_config)
        for token_id in range(20):
            insert_lock(postgres, tables, task_ids[token_id], ['special-%'], created_by='test')

        job = make_unentered_job('node1', coord_config)

        job.tokens.distribute(job.locks, job.cluster)

        assignments = get_token_assignments(postgres, tables)
        locked_by_node = Counter(assignments[tid] for tid in range(20))
        unlocked_by_node = Counter(assignments[tid] for tid in range(20, 50))

        assert locked_by_node == {'special-alpha': 10, 'special-beta': 10}
        assert unlocked_by_node == dict.fromkeys(node_names, 6)

    @clean_tables('Node', 'Token')
    def test_same_nodes_always_get_same_tokens(self, postgres):
        """Verify a fresh cluster with the same node names gets the same token assignment.

        Mutation: compute_minimal_move_distribution shuffles the
            distributable tokens (random.shuffle) before assigning them.
        Oracle: run 1's assignment, from a cluster with the same three names.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        with cluster(postgres, 'alpha', 'beta', 'gamma', total_tokens=100) as nodes_run1:
            for node in nodes_run1:
                assert wait_for(lambda n=node: len(n.my_tokens) >= 20, timeout_sec=10)

            assert wait_for_cached_tokens_sync(nodes_run1, expected_total=100, timeout_sec=10)

            distribution_run1 = {node.node_name: sorted(node.my_tokens) for node in nodes_run1}

        clear_tables(postgres, tables, ['Node', 'Token', 'Rebalance'])

        with cluster(postgres, 'alpha', 'beta', 'gamma', total_tokens=100) as nodes_run2:
            for node in nodes_run2:
                assert wait_for(lambda n=node: len(n.my_tokens) >= 20, timeout_sec=10)

            assert wait_for_cached_tokens_sync(nodes_run2, expected_total=100, timeout_sec=10)

            distribution_run2 = {node.node_name: sorted(node.my_tokens) for node in nodes_run2}

        assert distribution_run1 == distribution_run2, 'Same node names should get the same tokens'


class TestNodesPropertyIsolation:
    """Test Job.nodes property deep copy behavior."""

    def test_nodes_returns_copy(self):
        """Verify each read of nodes returns a new list.

        Mutation: nodes returns self._nodes.
        Oracle: identity of two successive reads.
        """
        job = Job('node1')

        assert job.nodes is not job.nodes, 'Each call should return new copy'

    def test_modifying_returned_nodes_doesnt_affect_internal(self):
        """Verify editing a value in a returned node dict leaves the job's copy alone.

        Mutation: nodes returns a shallow copy (list(self._nodes)).
        Oracle: the node name given to Job.
        """
        job = Job('node1')

        job.nodes[0]['name'] = 'MODIFIED'

        assert job.nodes[0]['name'] == 'node1'

    def test_adding_to_returned_list_doesnt_affect_internal(self):
        """Verify appending to the returned list leaves the job's list alone.

        Mutation: nodes returns self._nodes.
        Oracle: a new Job lists one node, itself.
        """
        job = Job('node1')

        job.nodes.append({'name': 'fake-node'})

        assert job.nodes == [{'name': 'node1'}]

    def test_deep_copy_protects_nested_dicts(self):
        """Verify a key added to a returned node dict does not reach the job's copy.

        Mutation: nodes returns a shallow copy (list(self._nodes)).
        Oracle: a new Job's node entry, {'name': 'node1'}.
        """
        job = Job('node1')

        job.nodes[0]['new_key'] = 'new_value'

        assert job.nodes[0] == {'name': 'node1'}


class TestBasicCallbackInvocation:
    """Test basic callback invocation scenarios."""

    def test_on_rebalance_called_on_startup(self, postgres):
        """Verify on_rebalance runs once for the initial assignment and not again while ownership holds.

        Mutation: TokenRefreshMonitor never sets initial_callback_sent, so
            every refresh repeats the initial callback.
        Oracle: one ownership change, the initial assignment, so one call;
            the refresh interval is 1s, so a repeat lands inside the 3s window.
        """
        coord_cfg = get_coordination_config(token_refresh_initial_interval_sec=1)

        tracker = CallbackTracker()

        with create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=0,
                        on_rebalance=tracker.on_rebalance):
            assert wait_for(lambda: len(tracker.rebalance_calls) >= 1, timeout_sec=10), 'Callback should be invoked'
            assert not wait_for(lambda: len(tracker.rebalance_calls) >= 2, timeout_sec=3), \
                'on_rebalance should not repeat without an ownership change'

    def test_no_callbacks_without_coordination(self, caplog):
        """Verify a standalone job given on_rebalance warns that it will never run it.

        Mutation: the coordination check on the warning in Job.__init__
            inverted or dropped.
        Oracle: the warning text Job.__init__ logs for a standalone job
            given a callback.
        """
        tracker = CallbackTracker()

        with caplog.at_level(logging.WARNING), Job('node1', on_rebalance=tracker.on_rebalance):
            pass

        assert any('callback will never fire' in record.getMessage() for record in caplog.records)


class TestCallbackTiming:
    """Test callback timing and ordering guarantees."""

    def test_rebalance_called_during_membership_change(self, postgres):
        """Verify on_rebalance runs again on the leader when a second node joins.

        Mutation: TokenRefreshMonitor's version-change branch skips
            on_rebalance.
        Oracle: node1 owns every token before node2 joins, so the join must
            take tokens from it; the tracker is reset after the initial call.
        """
        coord_cfg = get_coordination_config()

        tracker = CallbackTracker()

        job1 = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10,
                          on_rebalance=tracker.on_rebalance)
        job1.__enter__()

        try:
            assert wait_for(lambda: len(job1.my_tokens) >= 30, timeout_sec=15)
            assert wait_for_running_state(job1, timeout_sec=5)

            assert wait_for(lambda: len(tracker.rebalance_calls) >= 1, timeout_sec=5), 'Initial callback should fire before reset'

            initial_tokens = job1.my_tokens.copy()
            tracker.reset()

            job2 = create_job('node2', postgres, coordination_config=coord_cfg, wait_on_enter=10)
            job2.__enter__()

            try:
                assert wait_for(lambda: len(job2.my_tokens) >= 1, timeout_sec=15)

                for node in [job1, job2]:
                    assert wait_for_running_state(node, timeout_sec=5)

                assert wait_for(lambda: len(tracker.rebalance_calls) >= 1, timeout_sec=10), \
                    'on_rebalance should be called after membership change'

                assert job1.my_tokens != initial_tokens, 'Tokens should have been rebalanced'

            finally:
                job2.__exit__(None, None, None)

        finally:
            job1.__exit__(None, None, None)


class TestCallbackExceptionHandling:
    """Test that callback exceptions don't break coordination."""

    def test_exception_in_on_rebalance(self, postgres):
        """Verify a raising on_rebalance runs once per ownership change and leaves the node running.

        Mutation: invoke_callback calls the callback inline with no
            try/except, so the raise escapes TokenRefreshMonitor.check before
            initial_callback_sent is set and the monitor repeats the call.
        Oracle: one ownership change, the initial assignment, so one call;
            the refresh interval is 1s, so a repeat lands inside the 3s window.
        """
        coord_cfg = get_coordination_config(token_refresh_initial_interval_sec=1)

        calls = []

        def failing_callback():
            calls.append(time.time())
            raise RuntimeError('Test exception in callback')

        with create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=0,
                        on_rebalance=failing_callback) as job:
            assert wait_for(lambda: len(calls) >= 1, timeout_sec=10), 'Callback should be invoked'
            assert not wait_for(lambda: len(calls) >= 2, timeout_sec=3), \
                'A raising callback should not be repeated without an ownership change'

            assert job.state_machine.state == JobState.RUNNING_LEADER
            assert job.am_i_healthy(), 'Node should be healthy despite callback exception'


class TestCallbackPerformance:
    """Test callback performance characteristics."""

    def test_callback_timing_logged(self, postgres, caplog):
        """Verify on_rebalance's duration is logged in milliseconds.

        Mutation: the 1000x scale factor dropped from duration_ms in
            invoke_callback.
        Oracle: the callback sleeps 0.1s, so the logged duration is at
            least 100ms.
        """
        coord_cfg = get_coordination_config()

        callback_done = threading.Event()

        def tracked_callback():
            time.sleep(0.1)
            callback_done.set()

        with caplog.at_level(logging.INFO), create_job('node1', postgres, coordination_config=coord_cfg,
                                                       wait_on_enter=0, on_rebalance=tracked_callback):
            assert callback_done.wait(timeout=15), 'Callback should be invoked'

        durations_ms = [
            int(match.group(1)) for record in caplog.records
            if (match := re.match(r'on_rebalance completed in (\d+)ms', record.getMessage()))
            ]

        assert len(durations_ms) == 1, f'Expected one on_rebalance timing line, got {durations_ms}'
        assert durations_ms[0] >= 100


class TestMonitorLifecycleManagement:
    """Test monitor lifecycle controlled by state callbacks."""

    @clean_tables('Node')
    def test_leader_exit_callback_stops_leader_monitors(self, postgres):
        """Verify demotion from RUNNING_LEADER stops DeadNodeMonitor and RebalanceMonitor.

        Mutation: _on_exit_running_leader drops the _stop_monitor call for
            dead_node or rebalance.
        Oracle: the monitor objects captured while leader, read right after
            the demoting transition.
        """
        config = get_state_driven_config()
        tables = schema.get_table_names(config.appname)

        now = datetime.datetime.now(datetime.timezone.utc)
        insert_active_node(postgres, tables, 'node1', created_on=now)

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=5)
        job.__enter__()

        try:
            assert wait_for_state(job, JobState.RUNNING_LEADER, timeout_sec=10)

            dead_node_monitor = next((m for m in job._monitors.values() if 'dead-node' in m.name), None)
            rebalance_monitor = next((m for m in job._monitors.values() if 'rebalance' in m.name), None)

            assert dead_node_monitor is not None, 'Should have dead node monitor'
            assert rebalance_monitor is not None, 'Should have rebalance monitor'
            assert not dead_node_monitor._stop_requested, 'Monitor should be running'
            assert not rebalance_monitor._stop_requested, 'Monitor should be running'

            job.state_machine.transition_to(JobState.RUNNING_FOLLOWER)

            assert dead_node_monitor._stop_requested, 'Dead node monitor should be stopped by exit callback'
            assert rebalance_monitor._stop_requested, 'Rebalance monitor should be stopped by exit callback'

        finally:
            job.__exit__(None, None, None)


class TestTableVerification:
    """Test table existence verification."""

    def test_verify_tables_exist_all_present(self, postgres):
        """Verify verify_tables_exist reports True for each of the nine tables once created.

        Mutation: the existence query is bound to the table key ('Node')
            instead of the prefixed table name.
        Oracle: ensure_database_ready has just created all nine tables.
        """
        config = get_structure_test_config()

        schema.ensure_database_ready(postgres, config.appname)

        status = schema.verify_tables_exist(postgres, config.appname)

        expected_tables = ['Node', 'Check', 'Audit', 'Claim', 'Token', 'Lock',
                           'LeaderLock', 'RebalanceLock', 'Rebalance']

        for table_key in expected_tables:
            assert status[table_key], f'{table_key} should exist'

    def test_verify_tables_exist_missing_coordination_tables(self, postgres):
        """Verify verify_tables_exist reports dropped coordination tables as missing.

        Mutation: the existence query matches any table in the schema
            instead of the named one, so every key reads True.
        Oracle: the five coordination tables are dropped just before the
            check; the four core tables are left in place.
        """
        config = get_structure_test_config()
        tables = schema.get_table_names(config.appname)

        with postgres.connect() as conn:
            for table in ['Token', 'Lock', 'LeaderLock', 'RebalanceLock', 'Rebalance']:
                conn.execute(text(f'DROP TABLE IF EXISTS {tables[table]}'))
            conn.commit()

        status = schema.verify_tables_exist(postgres, config.appname)

        for table_key in ['Node', 'Check', 'Audit', 'Claim']:
            assert status[table_key], f'Core table {table_key} should exist'

        for table_key in ['Token', 'Lock', 'LeaderLock', 'RebalanceLock', 'Rebalance']:
            assert status[table_key] is False, f'Coordination table {table_key} should not exist'

    def test_verify_tables_only_checks_requested_tables(self, postgres):
        """Verify verify_tables_exist reports exactly the nine jobsync tables.

        Mutation: 'Inst' added to, or a key dropped from, the table_keys list
            in verify_tables_exist.
        Oracle: the nine keys get_table_names defines, less Inst, which the
            test fixture creates but jobsync does not own.
        """
        config = get_structure_test_config()

        status = schema.verify_tables_exist(postgres, config.appname)

        assert set(status) == {'Node', 'Check', 'Audit', 'Claim', 'Token', 'Lock', 'LeaderLock', 'RebalanceLock', 'Rebalance'}


class TestCoreTableCreation:
    """Test core table creation."""

    def test_core_tables_created_when_missing(self, postgres):
        """Verify ensure_database_ready recreates dropped core tables.

        Mutation: ensure_database_ready skips _create_core_tables.
        Oracle: the four core tables are dropped just before the call.
        """
        config = get_structure_test_config()
        tables = schema.get_table_names(config.appname)

        with postgres.connect() as conn:
            for table in ['Node', 'Check', 'Audit', 'Claim']:
                conn.execute(text(f'DROP TABLE IF EXISTS {tables[table]}'))
            conn.commit()

        schema.ensure_database_ready(postgres, config.appname)

        with postgres.connect() as conn:
            for table in ['Node', 'Check', 'Audit', 'Claim']:
                result = conn.execute(text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'public'
                        AND table_name = :table_name
                    )
                """), {'table_name': tables[table]})
                exists = result.scalar()
                assert exists, f'{table} should be created'

    def test_core_tables_have_required_indexes(self, postgres):
        """Verify the Node table carries its last_heartbeat index.

        Mutation: the heartbeat index statement removed from
            _create_core_tables.
        Oracle: the index name idx_<Node table>_heartbeat.
        """
        config = get_structure_test_config()
        tables = schema.get_table_names(config.appname)

        schema.ensure_database_ready(postgres, config.appname)

        with postgres.connect() as conn:
            result = conn.execute(text("""
                SELECT indexname FROM pg_indexes
                WHERE tablename = :table_name
                AND schemaname = 'public'
            """), {'table_name': tables['Node']})
            indexes = [row[0] for row in result]

        expected_index = f'idx_{tables["Node"]}_heartbeat'
        assert expected_index in indexes, 'Node table should have heartbeat index'


class TestCoordinationTableCreation:
    """Test coordination table creation."""

    def test_coordination_tables_created_when_enabled(self, postgres):
        """Verify ensure_database_ready recreates dropped coordination tables.

        Mutation: ensure_database_ready skips _create_coordination_tables.
        Oracle: the five coordination tables are dropped just before the call.
        """
        config = get_structure_test_config()
        tables = schema.get_table_names(config.appname)

        with postgres.connect() as conn:
            for table in ['Token', 'Lock', 'LeaderLock', 'RebalanceLock', 'Rebalance']:
                conn.execute(text(f'DROP TABLE IF EXISTS {tables[table]}'))
            conn.commit()

        schema.ensure_database_ready(postgres, config.appname)

        with postgres.connect() as conn:
            for table in ['Token', 'Lock', 'LeaderLock', 'RebalanceLock', 'Rebalance']:
                result = conn.execute(text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'public'
                        AND table_name = :table_name
                    )
                """), {'table_name': tables[table]})
                exists = result.scalar()
                assert exists, f'{table} should be created'

    def test_coordination_tables_have_required_indexes(self, postgres):
        """Verify the Token and Lock tables carry their indexes.

        Mutation: an index statement removed from _create_coordination_tables.
        Oracle: the index names idx_<table>_<column> for Token and Lock.
        """
        config = get_structure_test_config()
        tables = schema.get_table_names(config.appname)

        schema.ensure_database_ready(postgres, config.appname)

        expected_indexes = {
            tables['Token']: [f'idx_{tables["Token"]}_node', f'idx_{tables["Token"]}_assigned', f'idx_{tables["Token"]}_version'],
            tables['Lock']: [f'idx_{tables["Lock"]}_created_by', f'idx_{tables["Lock"]}_expires'],
        }

        with postgres.connect() as conn:
            for table_name, expected in expected_indexes.items():
                result = conn.execute(text("""
                    SELECT indexname FROM pg_indexes
                    WHERE tablename = :table_name
                    AND schemaname = 'public'
                """), {'table_name': table_name})
                indexes = [row[0] for row in result]

                for expected_index in expected:
                    assert expected_index in indexes, f'{table_name} should have {expected_index} index'


class TestIdempotentInitialization:
    """Test that database initialization is idempotent."""

    def test_ensure_database_ready_is_idempotent(self, postgres):
        """Verify ensure_database_ready raises nothing on a database it already set up.

        Mutation: IF NOT EXISTS dropped from a CREATE TABLE or CREATE INDEX
            statement.
        Oracle: the postgres fixture has already run ensure_database_ready
            once.
        """
        config = get_structure_test_config()

        schema.ensure_database_ready(postgres, config.appname)
        schema.ensure_database_ready(postgres, config.appname)

    def test_rebalance_lock_initialized_only_once(self, postgres):
        """Verify repeated setup leaves exactly one RebalanceLock row.

        Mutation: the RebalanceLock seed insert dropped, or its
            ON CONFLICT DO NOTHING dropped.
        Oracle: the singleton table holds one row.
        """
        config = get_structure_test_config()
        tables = schema.get_table_names(config.appname)

        schema.ensure_database_ready(postgres, config.appname)
        schema.ensure_database_ready(postgres, config.appname)

        with postgres.connect() as conn:
            result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["RebalanceLock"]}'))
            count = result.scalar()

        assert count == 1, 'RebalanceLock should have exactly one row'


class TestJobInitialization:
    """Test Job automatically initializes database."""

    def test_job_creates_core_tables_automatically(self, postgres, make_unentered_job):
        """Verify building a coordinated Job recreates dropped core tables.

        Mutation: Job.__init__ skips ensure_database_ready.
        Oracle: the four core tables are dropped before the Job is built.
        """
        config = get_structure_test_config()
        tables = schema.get_table_names(config.appname)

        with postgres.connect() as conn:
            for table in ['Node', 'Check', 'Audit', 'Claim']:
                conn.execute(text(f'DROP TABLE IF EXISTS {tables[table]}'))
            conn.commit()

        make_unentered_job('test-node', config)

        with postgres.connect() as conn:
            for table in ['Node', 'Check', 'Audit', 'Claim']:
                result = conn.execute(text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'public'
                        AND table_name = :table_name
                    )
                """), {'table_name': tables[table]})
                exists = result.scalar()
                assert exists, f'{table} should be created by Job initialization'

    def test_job_creates_coordination_tables_when_enabled(self, postgres, make_unentered_job):
        """Verify building a coordinated Job recreates dropped coordination tables.

        Mutation: Job.__init__ skips ensure_database_ready.
        Oracle: the five coordination tables are dropped before the Job is
            built.
        """
        config = get_structure_test_config()
        tables = schema.get_table_names(config.appname)

        with postgres.connect() as conn:
            for table in ['Token', 'Lock', 'LeaderLock', 'RebalanceLock', 'Rebalance']:
                conn.execute(text(f'DROP TABLE IF EXISTS {tables[table]} CASCADE'))
            conn.commit()

        make_unentered_job('test-node', CoordinationConfig(total_tokens=100))

        with postgres.connect() as conn:
            for table in ['Token', 'Lock', 'LeaderLock', 'RebalanceLock', 'Rebalance']:
                result = conn.execute(text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'public'
                        AND table_name = :table_name
                    )
                """), {'table_name': tables[table]})
                exists = result.scalar()
                assert exists, f'{table} should be created when coordination enabled'


class TestLeadershipDuringInitialization:
    """Test leadership queries during initialization states."""

    @clean_tables('Node')
    def test_am_i_leader_false_during_initialization(self, postgres):
        """Verify am_i_leader() is False before __enter__ and True once the lone node runs.

        Mutation: the is_running guard dropped from am_i_leader, or the
            elected name compared with != node_name.
        Oracle: node1 is the only active node in the Node table, so an
            unguarded election names it leader while still INITIALIZING.
        """
        coord_config = get_coordination_config()
        tables = schema.get_table_names(coord_config.appname)

        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)

        now = datetime.datetime.now(datetime.timezone.utc)
        insert_active_node(postgres, tables, 'node1', created_on=now)

        assert job.state_machine.state == JobState.INITIALIZING
        assert not job.am_i_leader(), 'Should return False during INITIALIZING'

        job.__enter__()

        try:
            assert wait_for_running_state(job, timeout_sec=10)

            assert job.am_i_leader(), 'The only active node should be leader once running'

        finally:
            job.__exit__(None, None, None)

    def test_am_i_leader_no_database_fallback(self, make_unentered_job):
        """Verify am_i_leader() never runs an election while INITIALIZING.

        Mutation: the is_running guard in am_i_leader moved below the
            elect_leader call, or dropped.
        Oracle: a stub on cluster.elect_leader that records its calls.
        """
        job = make_unentered_job('node1', get_coordination_config())

        original_elect = job.cluster.elect_leader
        elect_called = [False]

        def track_elect():
            elect_called[0] = True
            return original_elect()

        job.cluster.elect_leader = track_elect

        result = job.am_i_leader()

        assert not result, 'Should return False during initialization'
        assert not elect_called[0], 'Should NOT call elect_leader as fallback'


class TestCallbackBlockingDetection:
    """Test that slow callbacks don't block coordination."""

    def test_slow_callback_does_not_block_token_version_detection(self, postgres):
        """Verify the token-refresh monitor caches a new token version while on_rebalance is held.

        Mutation: invoke_callback runs the callback inline on the
            token-refresh thread instead of submitting it to the executor.
        Oracle: a stub callback that blocks until released; the version the
            test's own distribute() writes must reach the cache meanwhile.
        """
        coord_cfg = get_coordination_config(token_refresh_initial_interval_sec=1)

        callback_started = threading.Event()
        release = threading.Event()

        def blocking_callback():
            callback_started.set()
            release.wait(timeout=60)

        job = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=0,
                         on_rebalance=blocking_callback)
        job.__enter__()

        try:
            assert callback_started.wait(timeout=10), 'Initial callback should start'

            initial_version = job.token_version
            job.tokens.distribute(job.locks, job.cluster)

            assert wait_for(lambda: job.token_version > initial_version, timeout_sec=10), \
                'New token version should be cached while the callback is still running'

        finally:
            release.set()
            job.__exit__(None, None, None)

    def test_slow_callback_does_not_block_leadership_change_detection(self, postgres):
        """Verify a follower is promoted after the leader dies while its on_rebalance is held.

        Mutation: invoke_callback runs the callback inline on the
            token-refresh thread, the only thread that detects promotion.
        Oracle: a stub callback that blocks until released; node2 is the
            only node left alive, so it must reach RUNNING_LEADER meanwhile.
        """
        coord_cfg = get_coordination_config(token_refresh_initial_interval_sec=1)

        callback_started = threading.Event()
        release = threading.Event()

        def blocking_callback():
            callback_started.set()
            release.wait(timeout=60)

        node1 = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=0)
        node2 = create_job('node2', postgres, coordination_config=coord_cfg, wait_on_enter=0,
                           on_rebalance=blocking_callback)

        node1.__enter__()

        try:
            node2.__enter__()

            try:
                assert wait_for_state(node2, JobState.RUNNING_FOLLOWER, timeout_sec=10)
                assert callback_started.wait(timeout=10), 'Initial callback should start'

                simulate_node_crash(node1, cleanup=True)

                assert wait_for_state(node2, JobState.RUNNING_LEADER, timeout_sec=15), \
                    'node2 should be promoted while its callback is still running'

            finally:
                release.set()
                node2.__exit__(None, None, None)

        finally:
            node1.__exit__(None, None, None)


class TestInitialCallbackRaceCondition:
    """Test race between __enter__ completion and initial callback."""

    def test_leader_tokens_available_before_callback(self, postgres, token_refresh_gate):
        """Verify a leader's token cache is full when __enter__ returns.

        Mutation: __enter__ skips loading my_tokens and leaves the cache to
            TokenRefreshMonitor's first pass.
        Oracle: a one-node cluster owns all total_tokens tokens; the gate
            holds TokenRefreshMonitor off until after the read.
        """
        coord_cfg = get_coordination_config(token_refresh_initial_interval_sec=1)

        callback_invoked = []

        def track_callback():
            callback_invoked.append(True)

        with create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=0,
                        on_rebalance=track_callback) as job:
            assert job.my_tokens == set(range(coord_cfg.total_tokens)), \
                'Leader should have tokens immediately after __enter__'

            token_refresh_gate.set()

            assert wait_for(lambda: len(callback_invoked) >= 1, timeout_sec=10), \
                'Callback should fire once the refresh monitor runs'

    def test_follower_tokens_available_before_callback(self, postgres, token_refresh_gate):
        """Verify a follower's token cache matches its Token rows when __enter__ returns.

        Mutation: __enter__ skips loading my_tokens and leaves the cache to
            TokenRefreshMonitor's first pass.
        Oracle: node2's rows in the Token table; the gate holds
            TokenRefreshMonitor off until after the read.
        """
        coord_cfg = get_coordination_config(token_refresh_initial_interval_sec=1)
        tables = schema.get_table_names(coord_cfg.appname)

        callback_invoked = []

        def track_callback():
            callback_invoked.append(True)

        job1 = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=0)
        job1.__enter__()

        try:
            assert wait_for_running_state(job1, timeout_sec=10)

            with create_job('node2', postgres, coordination_config=coord_cfg, wait_on_enter=0,
                            on_rebalance=track_callback) as job2:
                tokens_at_enter_exit = job2.my_tokens

                assert len(tokens_at_enter_exit) > 0, 'Follower should have tokens after __enter__'
                assignments = get_token_assignments(postgres, tables)
                assert tokens_at_enter_exit == {tid for tid, owner in assignments.items() if owner == 'node2'}

                token_refresh_gate.set()

                assert wait_for(lambda: len(callback_invoked) >= 1, timeout_sec=10), \
                    'Callback should fire once the refresh monitor runs'

        finally:
            job1.__exit__(None, None, None)

    @pytest.mark.usefixtures('token_refresh_gate')
    def test_user_code_can_rely_on_token_cache_immediately(self, postgres):
        """Verify a one-node cluster can claim a task as soon as __enter__ returns.

        Mutation: __enter__ returns before moving to a running state, or
            skips loading my_tokens.
        Oracle: a one-node cluster owns every token, so it can claim any
            task; the gate holds TokenRefreshMonitor off, so only __enter__
            can fill the cache.
        """
        coord_cfg = get_coordination_config()

        with create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=0) as job:
            assert job.can_claim_task(Task(42)), 'Token cache should be ready for claiming right after __enter__'


class TestCallbackExceptionDoesNotStopMonitor:
    """Test that callback exceptions don't stop the monitor thread."""

    def test_monitor_continues_after_callback_exception(self, postgres):
        """Verify a raising on_rebalance still gets exactly one call per token version, including later ones.

        Mutation: invoke_callback calls the callback inline with no
            try/except, so the raise escapes TokenRefreshMonitor.check and
            the monitor repeats the initial call.
        Oracle: the token versions job1 caches; the callback must see the
            post-join version, and no version twice.
        """
        coord_cfg = get_coordination_config(token_refresh_initial_interval_sec=1)

        versions_seen = []

        def failing_callback(event):
            versions_seen.append(event.token_version)
            raise RuntimeError('Test callback failure')

        job1 = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=0,
                          on_rebalance=failing_callback)
        job1.__enter__()

        try:
            assert wait_for(lambda: len(versions_seen) >= 1, timeout_sec=10), 'Initial callback should run'
            initial_version = job1.token_version

            job2 = create_job('node2', postgres, coordination_config=coord_cfg, wait_on_enter=0)
            job2.__enter__()

            try:
                assert wait_for(lambda: job1.token_version > initial_version, timeout_sec=15), \
                    'node1 should cache the post-join token version'
                assert wait_for(lambda: job1.token_version in versions_seen, timeout_sec=5), \
                    'Callback should run for the post-join version after raising on the initial one'
                assert len(versions_seen) == len(set(versions_seen)), \
                    f'Each token version should reach the callback once, got {versions_seen}'

            finally:
                job2.__exit__(None, None, None)

        finally:
            job1.__exit__(None, None, None)


class TestCallbackThreadPoolExecution:
    """Test ThreadPoolExecutor-based callback execution."""

    def test_callbacks_run_in_thread_pool(self, postgres):
        """Verify callbacks run on the rebalance-callback executor thread.

        Mutation: invoke_callback runs the callback inline on the
            token-refresh thread.
        Oracle: the executor's thread_name_prefix, 'rebalance-callback'.
        """
        coord_cfg = get_coordination_config()

        main_thread_id = threading.current_thread().ident
        callback_threads = []

        def track_thread_callback():
            callback_threads.append((threading.current_thread().ident, threading.current_thread().name))

        with create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=0,
                        on_rebalance=track_thread_callback):
            assert wait_for(lambda: len(callback_threads) >= 1, timeout_sec=15)

            thread_id, thread_name = callback_threads[0]

            assert thread_id != main_thread_id, 'Callback should not run in main thread'
            assert 'rebalance-callback' in thread_name, f'Callback should run in executor thread, got {thread_name}'

    def test_thread_pool_limits_concurrent_callbacks(self, postgres):
        """Verify a second on_rebalance does not start while the first is still running.

        Mutation: the callback executor built with max_workers=2.
        Oracle: a stub callback that blocks until released and records each
            start; one start while held, two after release.
        """
        coord_cfg = get_coordination_config(token_refresh_initial_interval_sec=1)

        callback_starts = []
        release = threading.Event()

        def blocking_callback():
            callback_starts.append(time.time())
            release.wait(timeout=60)

        job = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=0,
                         on_rebalance=blocking_callback)
        job.__enter__()

        try:
            assert wait_for(lambda: len(callback_starts) == 1, timeout_sec=10), 'Initial callback should start'

            initial_version = job.token_version
            job.tokens.distribute(job.locks, job.cluster)

            assert wait_for(lambda: job.token_version > initial_version, timeout_sec=10), \
                'Second token version should be cached, queuing a second callback'
            assert not wait_for(lambda: len(callback_starts) >= 2, timeout_sec=2), \
                'Second callback started while the first was still running'

            release.set()

            assert wait_for(lambda: len(callback_starts) == 2, timeout_sec=5), \
                'Queued callback should start once the first finishes'

        finally:
            release.set()
            job.__exit__(None, None, None)

    def test_shutdown_waits_for_pending_callbacks(self, postgres):
        """Verify __exit__ returns only after a running callback finishes.

        Mutation: __exit__ calls shutdown_callbacks(wait=False).
        Oracle: the callback appends 3s after it starts, and __exit__ is
            entered right after the start.
        """
        coord_cfg = get_coordination_config()

        callback_started = threading.Event()
        callback_completed = []

        def long_callback():
            callback_started.set()
            time.sleep(3)
            callback_completed.append(True)

        with create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=0,
                        on_rebalance=long_callback):
            assert callback_started.wait(timeout=15), 'Callback should start'

        assert callback_completed == [True], 'Callback should complete before shutdown finishes'

    def test_pending_callbacks_tracked(self, postgres):
        """Verify a running callback's future is held in _pending_callbacks.

        Mutation: invoke_callback does not append the future to
            _pending_callbacks.
        Oracle: one submitted callback, held running by the stub.
        """
        coord_cfg = get_coordination_config()

        callback_active = threading.Event()
        release = threading.Event()

        def blocking_callback():
            callback_active.set()
            release.wait(timeout=60)

        job = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=0,
                         on_rebalance=blocking_callback)
        job.__enter__()

        try:
            assert callback_active.wait(timeout=15), 'Callback should start'

            assert wait_for(lambda: len(job.tokens._pending_callbacks) == 1, timeout_sec=5), \
                'Future should be tracked in pending callbacks list'

        finally:
            release.set()
            job.__exit__(None, None, None)

    def test_shutdown_logs_pending_callback_count(self, postgres, caplog):
        """Verify shutdown logs how many callbacks it waits for.

        Mutation: the pending-count log in shutdown_callbacks guarded by
            pending_count > 1.
        Oracle: one submitted callback, the initial assignment's.
        """
        coord_cfg = get_coordination_config()

        callback_started = threading.Event()

        def slow_callback():
            callback_started.set()
            time.sleep(2)

        with caplog.at_level(logging.INFO), create_job('node1', postgres, coordination_config=coord_cfg,
                                                       wait_on_enter=0, on_rebalance=slow_callback):
            assert callback_started.wait(timeout=15), 'Callback should start'

        assert any(record.getMessage().startswith('Waiting for 1 pending callbacks') for record in caplog.records), \
            'Should log the pending callback count during shutdown'


if __name__ == '__main__':
    pytest.main(args=['-sx', __file__])
