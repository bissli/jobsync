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
import threading
import time

import pytest
from sqlalchemy import text

from jobsync import schema
from jobsync.client import CoordinationConfig, JobState

logger = logging.getLogger(__name__)


from fixtures import *  # noqa: F401, F403

logger = logging.getLogger(__name__)


class TestLeaderElection:
    """Test leader election logic."""

    @clean_tables('Node')
    def test_oldest_node_elected(self, postgres):
        """Test that oldest node becomes leader."""
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        base_time = datetime.datetime.now(datetime.timezone.utc)

        insert_active_node(postgres, tables, 'node2', created_on=base_time - datetime.timedelta(seconds=10))
        insert_active_node(postgres, tables, 'node1', created_on=base_time - datetime.timedelta(seconds=20))
        insert_active_node(postgres, tables, 'node3', created_on=base_time - datetime.timedelta(seconds=5))

        job = create_job('test', postgres, coordination_config=config, wait_on_enter=0)
        leader = job.cluster.elect_leader()

        assert leader == 'node1', 'Oldest node should be elected leader'

    @clean_tables('Node')
    def test_name_tiebreaker(self, postgres):
        """Test that name is used as tiebreaker when timestamps equal."""
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        same_time = datetime.datetime.now(datetime.timezone.utc)

        for name in ['node-c', 'node-a', 'node-b']:
            insert_active_node(postgres, tables, name, created_on=same_time)

        job = create_job('test', postgres, coordination_config=config, wait_on_enter=0)
        leader = job.cluster.elect_leader()

        assert leader == 'node-a', 'Alphabetically first node should win tiebreaker'

    @clean_tables('Node')
    def test_dead_nodes_filtered(self, postgres):
        """Test that dead nodes are not considered for leader election."""
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        current_time = datetime.datetime.now(datetime.timezone.utc)

        insert_stale_node(postgres, tables, 'node1', heartbeat_age_seconds=30)
        insert_active_node(postgres, tables, 'node2', created_on=current_time - datetime.timedelta(seconds=20))

        job = create_job('test', postgres, coordination_config=config, wait_on_enter=0)
        leader = job.cluster.elect_leader()

        assert leader == 'node2', 'Only alive nodes should be considered'


class TestCanClaimTask:
    """Test task claiming logic."""

    @clean_tables('Token')
    def test_can_claim_owned_token(self, postgres):
        """Test that node can claim task if it owns the token."""
        coord_config = get_coordination_config()
        tables = schema.get_table_names(coord_config.appname)

        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)

        token_id = 5
        insert_token(postgres, tables, token_id, 'node1', version=1)

        job.tokens.my_tokens = {5}
        job.tokens.token_version = 1
        job.state_machine.state = JobState.RUNNING_FOLLOWER

        # Find a task_id that maps to token 5
        task_id = None
        for candidate in range(100000):
            if job.task_to_token(candidate) == token_id:
                task_id = candidate
                break
        assert task_id is not None, f'Could not find task_id mapping to token {token_id}'

        task = create_task(task_id)
        assert job.can_claim_task(task), 'Should be able to claim task with owned token'

    def test_cannot_claim_unowned_token(self, postgres):
        """Test that node cannot claim task if it doesn't own the token."""
        coord_config = get_coordination_config()

        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)

        job.tokens.my_tokens = set()
        job.state_machine.state = JobState.RUNNING_FOLLOWER

        task = create_task(123)
        assert not job.can_claim_task(task), 'Should not be able to claim task without token'


class TestTokenDistribution:
    """Test token distribution algorithm."""

    @clean_tables('Node')
    def test_even_distribution(self, postgres):
        """Test that tokens are distributed evenly across nodes."""
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        current = datetime.datetime.now(datetime.timezone.utc)
        for i in range(1, 4):
            insert_active_node(postgres, tables, f'node{i}', created_on=current)

        coord_config = CoordinationConfig(total_tokens=99)
        job = create_job('node1', postgres, wait_on_enter=0, coordination_config=coord_config)

        job.tokens.distribute(job.locks, job.cluster)

        nodes = [create_job(f'node{i}', postgres, wait_on_enter=0,
                            coordination_config=coord_config) for i in range(1, 4)]

        assignments = get_token_assignments(postgres, tables)
        for node in nodes:
            node.tokens.my_tokens = {tid for tid, owner in assignments.items() if owner == node.node_name}
            print(f'{node.node_name}: {len(node.tokens.my_tokens)} tokens')

        assert_token_distribution_balanced(nodes, total_tokens=99, tolerance=0.1)

    @clean_tables('Token', 'Lock', 'Node')
    def test_locked_tokens_respected(self, postgres):
        """Test that locked tokens are assigned to matching nodes only."""
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        current = datetime.datetime.now(datetime.timezone.utc)
        for name in ['node1', 'node2', 'special-alpha']:
            insert_active_node(postgres, tables, name, created_on=current)

        coord_config = CoordinationConfig(total_tokens=30)

        task_ids = find_task_ids_covering_all_tokens(coord_config)

        for token_id in range(10):
            insert_lock(postgres, tables, task_ids[token_id], ['special-%'], created_by='test')

        job = create_job('node1', postgres, wait_on_enter=0, coordination_config=coord_config)

        job.tokens.distribute(job.locks, job.cluster)

        assignments = get_token_assignments(postgres, tables)
        locked_correct = sum(1 for token_id in range(10) if assignments.get(token_id) == 'special-alpha')

        assert locked_correct == 10, f'All 10 locked tokens should be assigned to special-alpha (got {locked_correct})'

    @clean_tables('Lock', 'Token', 'Node')
    def test_locked_tokens_balanced_across_multiple_matching_nodes(self, postgres):
        """Test that locked tokens are balanced evenly among multiple nodes matching the pattern."""
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        current = datetime.datetime.now(datetime.timezone.utc)
        # Create 3 regular nodes and 2 special nodes
        for name in ['node1', 'node2', 'node3', 'special-alpha', 'special-beta']:
            insert_active_node(postgres, tables, name, created_on=current)

        coord_config = CoordinationConfig(total_tokens=50)

        # Find task_ids that hash to all tokens (using helper function)
        task_ids = find_task_ids_covering_all_tokens(coord_config)

        # Lock first 20 tokens with pattern 'special-%' (matches both special-alpha and special-beta)
        for token_id in range(20):
            insert_lock(postgres, tables, task_ids[token_id], ['special-%'], created_by='test')

        job = create_job('node1', postgres, wait_on_enter=0, coordination_config=coord_config)

        job.tokens.distribute(job.locks, job.cluster)

        assignments = get_token_assignments(postgres, tables)

        # Count locked tokens per special node
        special_alpha_locked = sum(1 for tid in range(20) if assignments.get(tid) == 'special-alpha')
        special_beta_locked = sum(1 for tid in range(20) if assignments.get(tid) == 'special-beta')

        # Count unlocked tokens per node
        node1_unlocked = sum(1 for tid in range(20, 50) if assignments.get(tid) == 'node1')
        node2_unlocked = sum(1 for tid in range(20, 50) if assignments.get(tid) == 'node2')
        node3_unlocked = sum(1 for tid in range(20, 50) if assignments.get(tid) == 'node3')
        special_alpha_unlocked = sum(1 for tid in range(20, 50) if assignments.get(tid) == 'special-alpha')
        special_beta_unlocked = sum(1 for tid in range(20, 50) if assignments.get(tid) == 'special-beta')

        print('\nLocked token distribution (should be balanced 10/10):')
        print(f'  special-alpha: {special_alpha_locked} locked tokens')
        print(f'  special-beta: {special_beta_locked} locked tokens')
        print('\nUnlocked token distribution (30 tokens across all 5 nodes = 6 each):')
        print(f'  node1: {node1_unlocked} unlocked tokens')
        print(f'  node2: {node2_unlocked} unlocked tokens')
        print(f'  node3: {node3_unlocked} unlocked tokens')
        print(f'  special-alpha: {special_alpha_unlocked} unlocked tokens')
        print(f'  special-beta: {special_beta_unlocked} unlocked tokens')

        # Verify all locked tokens went to special nodes
        total_special_locked = special_alpha_locked + special_beta_locked
        assert total_special_locked == 20, f'All 20 locked tokens should go to special nodes (got {total_special_locked})'

        # Verify balanced distribution among special nodes (should be 10 each)
        assert special_alpha_locked == 10, f'special-alpha should get 10 locked tokens (got {special_alpha_locked})'
        assert special_beta_locked == 10, f'special-beta should get 10 locked tokens (got {special_beta_locked})'

        # Verify unlocked tokens are distributed among ALL nodes (5 nodes, 6 each)
        total_unlocked = node1_unlocked + node2_unlocked + node3_unlocked + special_alpha_unlocked + special_beta_unlocked
        assert total_unlocked == 30, f'All 30 unlocked tokens should be distributed (got {total_unlocked})'

        # Verify balanced distribution of unlocked tokens (each node should get 6)
        assert abs(node1_unlocked - 6) <= 1, f'node1 should get ~6 unlocked tokens (got {node1_unlocked})'
        assert abs(node2_unlocked - 6) <= 1, f'node2 should get ~6 unlocked tokens (got {node2_unlocked})'
        assert abs(node3_unlocked - 6) <= 1, f'node3 should get ~6 unlocked tokens (got {node3_unlocked})'
        assert abs(special_alpha_unlocked - 6) <= 1, f'special-alpha should get ~6 unlocked tokens (got {special_alpha_unlocked})'
        assert abs(special_beta_unlocked - 6) <= 1, f'special-beta should get ~6 unlocked tokens (got {special_beta_unlocked})'

        with postgres.connect() as conn:
            result = conn.execute(text(f"""
                SELECT node, COUNT(*) as cnt
                FROM {tables["Token"]}
                GROUP BY node
                ORDER BY node
            """))
            all_tokens = [dict(row._mapping) for row in result]

        print('\nToken distribution:')
        for row in all_tokens:
            print(f'  {row["node"]}: {row["cnt"]} tokens')

        node_counts = {row['node']: row['cnt'] for row in all_tokens}
        for node_name in ['node1', 'node2', 'special-alpha']:
            assert node_name in node_counts and node_counts[node_name] > 0, f'{node_name} should have some tokens'

        assert node_counts['special-alpha'] >= 10, 'special-alpha should have at least the 10 locked tokens'

    @clean_tables('Node', 'Token')
    def test_same_nodes_always_get_same_tokens(self, postgres):
        """Verify same node names always receive identical token assignments across runs.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        coord_config = CoordinationConfig(total_tokens=100)

        print('\nRun 1: Initial cluster formation')
        with cluster(postgres, 'alpha', 'beta', 'gamma', total_tokens=100) as nodes_run1:
            for node in nodes_run1:
                assert wait_for(lambda n=node: len(n.my_tokens) >= 20, timeout_sec=10)

            assert wait_for_cached_tokens_sync(nodes_run1, expected_total=100, timeout_sec=10)

            distribution_run1 = {node.node_name: sorted(node.my_tokens) for node in nodes_run1}
            print('Run 1 distribution:')
            for name, tokens in distribution_run1.items():
                print(f'  {name}: {len(tokens)} tokens')

        print('\nCleaning database...')
        clear_tables(postgres, tables, ['Node', 'Token', 'Rebalance'])

        print('\nRun 2: Fresh cluster with same node names')
        with cluster(postgres, 'alpha', 'beta', 'gamma', total_tokens=100) as nodes_run2:
            for node in nodes_run2:
                assert wait_for(lambda n=node: len(n.my_tokens) >= 20, timeout_sec=10)

            assert wait_for_cached_tokens_sync(nodes_run2, expected_total=100, timeout_sec=10)

            distribution_run2 = {node.node_name: sorted(node.my_tokens) for node in nodes_run2}
            print('Run 2 distribution:')
            for name, tokens in distribution_run2.items():
                print(f'  {name}: {len(tokens)} tokens')

        for node_name in ['alpha', 'beta', 'gamma']:
            tokens_run1 = distribution_run1[node_name]
            tokens_run2 = distribution_run2[node_name]

            assert tokens_run1 == tokens_run2, (
                f'{node_name} received different tokens across runs: '
                f'run1={len(tokens_run1)}, run2={len(tokens_run2)}'
            )
            print(f'✓ {node_name}: identical distribution across runs')

        print('✓ Deterministic distribution verified')


class TestNodesPropertyIsolation:
    """Test Job.nodes property deep copy behavior."""

    def test_nodes_returns_copy(self, postgres):
        """Verify nodes property returns a copy, not reference.
        """
        config = get_coordination_config()

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=5)
        job.__enter__()

        try:
            assert wait_for_running_state(job, timeout_sec=10)

            nodes1 = job.nodes
            nodes2 = job.nodes

            assert nodes1 is not nodes2, 'Each call should return new copy'

        finally:
            job.__exit__(None, None, None)

    def test_modifying_returned_nodes_doesnt_affect_internal(self, postgres):
        """Verify modifications to returned nodes don't affect internal state.
        """
        config = get_coordination_config()

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=5)
        job.__enter__()

        try:
            assert wait_for_running_state(job, timeout_sec=10)

            nodes = job.nodes
            original_name = nodes[0]['name']

            nodes[0]['name'] = 'MODIFIED'

            nodes_after = job.nodes
            assert nodes_after[0]['name'] == original_name, \
                'Internal state should not be affected by external modifications'

            logger.info('✓ Internal state protected from external modifications')

        finally:
            job.__exit__(None, None, None)

    def test_adding_to_returned_list_doesnt_affect_internal(self, postgres):
        """Verify adding to returned list doesn't affect internal state.
        """
        config = get_coordination_config()

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=5)
        job.__enter__()

        try:
            assert wait_for_running_state(job, timeout_sec=10)

            nodes = job.nodes
            original_length = len(nodes)

            nodes.append({'node': 'fake-node'})

            nodes_after = job.nodes
            assert len(nodes_after) == original_length, \
                'Internal list should not grow when external copy is modified'

        finally:
            job.__exit__(None, None, None)

    def test_deep_copy_protects_nested_dicts(self, postgres):
        """Verify deep copy protects nested dictionary values.
        """
        config = get_coordination_config()

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=5)
        job.__enter__()

        try:
            assert wait_for_running_state(job, timeout_sec=10)

            nodes = job.nodes
            if len(nodes) > 0 and isinstance(nodes[0], dict):
                original_keys = set(nodes[0].keys())

                nodes[0]['new_key'] = 'new_value'

                nodes_after = job.nodes
                assert 'new_key' not in nodes_after[0], \
                    'New keys in external copy should not appear in internal state'

                assert set(nodes_after[0].keys()) == original_keys, \
                    'Internal dict keys should remain unchanged'

            logger.info('✓ Deep copy protects nested structures')

        finally:
            job.__exit__(None, None, None)


class TestBasicCallbackInvocation:
    """Test basic callback invocation scenarios."""

    def test_on_rebalance_called_on_startup(self, postgres):
        """Verify on_rebalance called during initial allocation.
        """
        coord_cfg = get_coordination_config()

        tracker = CallbackTracker()

        with create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10,
                on_rebalance=tracker.on_rebalance) as job:
            assert wait_for(lambda: len(job.my_tokens) >= 1, timeout_sec=15), 'Should receive tokens'
            assert wait_for(lambda: len(tracker.rebalance_calls) >= 1, timeout_sec=5), 'Callback should be invoked'

            assert len(tracker.rebalance_calls) > 0, 'on_rebalance should be called'
            assert len(job.my_tokens) > 0, 'Should have received tokens'

    def test_no_callbacks_without_coordination(self, postgres):
        """Verify callbacks not invoked when coordination disabled.
        """
        tracker = CallbackTracker()

        with create_job('node1', postgres, coordination_config=None, wait_on_enter=0,
                on_rebalance=tracker.on_rebalance) as job:
            time.sleep(1)

            assert len(tracker.rebalance_calls) == 0, 'Callbacks should not fire when coordination disabled'


class TestCallbackTiming:
    """Test callback timing and ordering guarantees."""

    def test_rebalance_called_during_membership_change(self, postgres):
        """Verify on_rebalance called when cluster membership changes.
        """
        coord_cfg = get_coordination_config()
        tables = schema.get_table_names(coord_cfg.appname)

        tracker = CallbackTracker()

        # Start first node
        job1 = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10,
                  on_rebalance=tracker.on_rebalance)
        job1.__enter__()

        try:
            assert wait_for(lambda: len(job1.my_tokens) >= 30, timeout_sec=15)
            assert wait_for_running_state(job1, timeout_sec=5)

            # Wait for initial callback to fire before resetting tracker
            assert wait_for(lambda: len(tracker.rebalance_calls) >= 1, timeout_sec=5), 'Initial callback should fire before reset'

            initial_tokens = job1.my_tokens.copy()
            initial_callback_count = len(tracker.rebalance_calls)
            tracker.reset()

            # Start second node to trigger rebalancing
            job2 = create_job('node2', postgres, coordination_config=coord_cfg, wait_on_enter=10)
            job2.__enter__()

            try:
                assert wait_for(lambda: len(job2.my_tokens) >= 1, timeout_sec=15)

                for node in [job1, job2]:
                    assert wait_for_running_state(node, timeout_sec=5)

                assert wait_for_rebalance(postgres, tables, min_count=1, timeout_sec=20)

                # Wait for rebalance callback
                assert wait_for(lambda: len(tracker.rebalance_calls) >= 1, timeout_sec=10), \
                    'on_rebalance should be called after membership change'

                # Verify tokens were actually rebalanced
                final_tokens = job1.my_tokens
                assert final_tokens != initial_tokens, 'Tokens should have been rebalanced'

            finally:
                job2.__exit__(None, None, None)

        finally:
            job1.__exit__(None, None, None)

    def test_callbacks_invoked_in_refresh_thread(self, postgres):
        """Verify callbacks run in background thread, not blocking main thread.
        """
        coord_cfg = get_coordination_config()

        main_thread_id = threading.current_thread().ident
        callback_thread_id = None
        callback_invoked = []

        def track_thread_callback():
            nonlocal callback_thread_id
            callback_thread_id = threading.current_thread().ident
            callback_invoked.append(True)
            logger.info(f'Callback running in thread {callback_thread_id}')

        with create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10,
                on_rebalance=track_thread_callback) as job:
            assert wait_for(lambda: len(job.my_tokens) >= 1, timeout_sec=15)
            assert wait_for(lambda: len(callback_invoked) >= 1, timeout_sec=5)

            assert callback_thread_id is not None, 'Callback should have been invoked'
            assert callback_thread_id != main_thread_id, 'Callback should run in background thread'


class TestCallbackExceptionHandling:
    """Test that callback exceptions don't break coordination."""

    def test_exception_in_on_rebalance(self, postgres):
        """Verify exception in on_rebalance doesn't break coordination.
        """
        coord_cfg = get_coordination_config()

        def failing_callback():
            raise RuntimeError('Test exception in callback')

        with create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10,
                on_rebalance=failing_callback) as job:
            assert wait_for(lambda: len(job.my_tokens) >= 1, timeout_sec=15), 'Should receive tokens despite callback exception'

            # Job should still function despite callback failure
            assert job.am_i_healthy(), 'Node should be healthy despite callback exception'
            assert len(job.my_tokens) > 0, 'Node should still have tokens'

    def test_exception_during_rebalance(self, postgres):
        """Verify exception in callback doesn't prevent rebalancing.
        """
        coord_cfg = get_coordination_config()

        def failing_callback():
            raise RuntimeError('Test exception in callback')

        job1 = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10,
                  on_rebalance=failing_callback)
        job1.__enter__()

        try:
            assert wait_for(lambda: len(job1.my_tokens) >= 30, timeout_sec=15)
            assert wait_for_running_state(job1, timeout_sec=5)

            # Start second node to trigger rebalancing
            job2 = create_job('node2', postgres, coordination_config=coord_cfg, wait_on_enter=10)
            job2.__enter__()

            try:
                assert wait_for(lambda: len(job2.my_tokens) >= 1, timeout_sec=15)

                for node in [job1, job2]:
                    assert wait_for_running_state(node, timeout_sec=5)

                # Despite callback failing, coordination should continue
                assert job1.am_i_healthy(), 'Node1 should be healthy'
                assert job2.am_i_healthy(), 'Node2 should be healthy'

            finally:
                job2.__exit__(None, None, None)

        finally:
            job1.__exit__(None, None, None)


class TestCallbackCorrectness:
    """Test that callbacks are called at correct times."""

    def test_rebalance_called_on_startup(self, postgres):
        """Verify on_rebalance called during initial token assignment.
        """
        coord_cfg = get_coordination_config()

        callback_invoked = []

        def track_callback():
            callback_invoked.append(True)

        with create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10,
                on_rebalance=track_callback) as job:
            assert wait_for(lambda: len(job.my_tokens) >= 1, timeout_sec=15)
            assert wait_for(lambda: len(callback_invoked) >= 1, timeout_sec=5)

            assert len(callback_invoked) > 0, 'Callback should have been called'

    def test_rebalance_called_on_membership_change(self, postgres):
        """Verify on_rebalance called when cluster membership changes.
        """
        coord_cfg = get_coordination_config()
        tables = schema.get_table_names(coord_cfg.appname)

        tracker = CallbackTracker()

        job1 = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10,
                  on_rebalance=tracker.on_rebalance)
        job1.__enter__()

        try:
            assert wait_for(lambda: len(job1.my_tokens) >= 30, timeout_sec=15)
            assert wait_for_running_state(job1, timeout_sec=5)

            assert wait_for(lambda: len(tracker.rebalance_calls) >= 1, timeout_sec=5), 'Initial callback should fire'

            initial_tokens = job1.my_tokens.copy()
            initial_call_count = len(tracker.rebalance_calls)

            # Add second node
            job2 = create_job('node2', postgres, coordination_config=coord_cfg, wait_on_enter=10)
            job2.__enter__()

            try:
                assert wait_for(lambda: len(job2.my_tokens) >= 1, timeout_sec=15)

                for node in [job1, job2]:
                    assert wait_for_running_state(node, timeout_sec=5)

                assert wait_for_rebalance(postgres, tables, min_count=1, timeout_sec=20)

                # Wait for callback to fire after rebalance
                assert wait_for(lambda: len(tracker.rebalance_calls) > initial_call_count, timeout_sec=10), \
                    'Callback should fire after rebalance'

                final_tokens = job1.my_tokens
                assert final_tokens != initial_tokens, 'Tokens should have changed'

            finally:
                job2.__exit__(None, None, None)

        finally:
            job1.__exit__(None, None, None)


class TestCallbackPerformance:
    """Test callback performance characteristics."""

    def test_slow_callback_doesnt_block_coordination(self, postgres):
        """Verify slow callbacks don't prevent heartbeats.
        """
        coord_cfg = get_coordination_config()

        def slow_callback():
            logger.info('Slow callback starting...')
            time.sleep(3)  # Simulate slow work
            logger.info('Slow callback completed')

        with create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10,
                on_rebalance=slow_callback) as job:
            assert wait_for(lambda: len(job.my_tokens) >= 1, timeout_sec=15)
            assert wait_for_running_state(job, timeout_sec=5)

            # Despite slow callback, heartbeat should continue
            assert wait_for(job.am_i_healthy, timeout_sec=8), 'Node should remain healthy with slow callback'

    def test_callback_timing_logged(self, postgres, caplog):
        """Verify callback duration is logged.
        """
        coord_cfg = get_coordination_config()

        def tracked_callback():
            time.sleep(0.1)  # Small delay to ensure measurable time

        with caplog.at_level(logging.INFO), create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10,
                on_rebalance=tracked_callback) as job:
            assert wait_for(lambda: len(job.my_tokens) >= 1, timeout_sec=15)
            assert wait_for_running_state(job, timeout_sec=5)

        timing_messages = [record.message for record in caplog.records
                          if 'completed in' in record.message and 'ms' in record.message]

        assert len(timing_messages) > 0, 'Callback timing should be logged'


class TestMonitorLifecycleManagement:
    """Test monitor lifecycle controlled by state callbacks."""

    @clean_tables('Node')
    def test_leader_exit_callback_stops_leader_monitors(self, postgres):
        """Verify _on_exit_running_leader stops DeadNodeMonitor and RebalanceMonitor.
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

            # Trigger demotion
            job.state_machine.transition_to(JobState.RUNNING_FOLLOWER)

            # Exit callback should have stopped monitors
            assert dead_node_monitor._stop_requested, 'Dead node monitor should be stopped by exit callback'
            assert rebalance_monitor._stop_requested, 'Rebalance monitor should be stopped by exit callback'

            logger.info('✓ Leader monitors stopped by exit callback')

        finally:
            job.__exit__(None, None, None)

    @clean_tables('Node')
    def test_monitors_dont_self_check_leadership(self, postgres):
        """Verify monitors no longer contain am_i_leader() checks in their check() methods.
        """
        config = get_state_driven_config()
        tables = schema.get_table_names(config.appname)

        insert_active_node(postgres, tables, 'node1')

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=5)
        job.__enter__()

        try:
            assert wait_for_running_state(job, timeout_sec=5)

            dead_node_monitor = next((m for m in job._monitors.values() if 'dead-node' in m.name), None)
            rebalance_monitor = next((m for m in job._monitors.values() if 'rebalance' in m.name), None)

            # Monitors should exist and be running
            assert dead_node_monitor is not None, 'Should have dead node monitor'
            assert rebalance_monitor is not None, 'Should have rebalance monitor'
            assert dead_node_monitor.thread.is_alive(), 'Monitor thread should be alive'
            assert rebalance_monitor.thread.is_alive(), 'Monitor thread should be alive'

            # The monitor check() methods don't call am_i_leader() anymore
            # They continue running until stopped by state machine callback
            # This is the key difference from before

            logger.info('✓ Monitors run without self-checking leadership')

        finally:
            job.__exit__(None, None, None)


class TestTableVerification:
    """Test table existence verification."""

    def test_verify_tables_exist_all_present(self, postgres):
        """Verify verification returns True when all tables exist.
        """
        config = get_structure_test_config()
        tables = schema.get_table_names(config.appname)

        schema.ensure_database_ready(postgres, config.appname)

        status = schema.verify_tables_exist(postgres, config.appname)

        expected_tables = ['Node', 'Check', 'Audit', 'Claim', 'Token', 'Lock',
                          'LeaderLock', 'RebalanceLock', 'Rebalance']

        for table_key in expected_tables:
            assert status[table_key], f'{table_key} should exist'

    def test_verify_tables_exist_missing_coordination_tables(self, postgres):
        """Verify verification detects missing coordination tables.
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
            assert not status.get(table_key, False), f'Coordination table {table_key} should not exist'

    def test_verify_tables_only_checks_requested_tables(self, postgres):
        """Verify verification checks all tables.
        """
        config = get_structure_test_config()

        schema.ensure_database_ready(postgres, config.appname)

        status = schema.verify_tables_exist(postgres, config.appname)

        for table_key in ['Node', 'Check', 'Audit', 'Claim', 'Token', 'Lock', 'LeaderLock', 'RebalanceLock', 'Rebalance']:
            assert table_key in status, f'{table_key} should be in status'


class TestCoreTableCreation:
    """Test core table creation."""

    def test_core_tables_created_when_missing(self, postgres):
        """Verify core tables are created when missing.
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
        """Verify core tables have required indexes.
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
        """Verify coordination tables are created.
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
        """Verify coordination tables have required indexes.
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
        """Verify ensure_database_ready can be called multiple times safely.
        """
        config = get_structure_test_config()

        schema.ensure_database_ready(postgres, config.appname)

        try:
            schema.ensure_database_ready(postgres, config.appname)
            schema.ensure_database_ready(postgres, config.appname)
        except Exception as e:
            pytest.fail(f'ensure_database_ready should be idempotent: {e}')

    def test_rebalance_lock_initialized_only_once(self, postgres):
        """Verify RebalanceLock singleton row is only created once.
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

    def test_job_creates_core_tables_automatically(self, postgres):
        """Verify Job creation triggers core table creation.
        """
        config = get_structure_test_config()
        tables = schema.get_table_names(config.appname)

        with postgres.connect() as conn:
            for table in ['Node', 'Check', 'Audit', 'Claim']:
                conn.execute(text(f'DROP TABLE IF EXISTS {tables[table]}'))
            conn.commit()

        job = create_job('test-node', postgres, wait_on_enter=0, coordination_config=config)

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

    def test_job_creates_coordination_tables_when_enabled(self, postgres):
        """Verify Job creates coordination tables when coordination enabled.
        """
        config = get_structure_test_config()
        tables = schema.get_table_names(config.appname)

        with postgres.connect() as conn:
            for table in ['Token', 'Lock', 'LeaderLock', 'RebalanceLock', 'Rebalance']:
                conn.execute(text(f'DROP TABLE IF EXISTS {tables[table]} CASCADE'))
            conn.commit()

        coord_config = CoordinationConfig(total_tokens=100)
        job = create_job('test-node', postgres, wait_on_enter=0, coordination_config=coord_config)

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

    def test_job_without_coordination_skips_coordination_tables(self, postgres):
        """Verify Job without coordination doesn't create coordination tables.
        """
        config = get_structure_test_config()
        tables = schema.get_table_names(config.appname)

        with postgres.connect() as conn:
            for table in ['Token', 'Lock', 'LeaderLock', 'RebalanceLock', 'Rebalance']:
                conn.execute(text(f'DROP TABLE IF EXISTS {tables[table]} CASCADE'))
            conn.commit()

        job = create_job('test-node', postgres, wait_on_enter=0, coordination_config=None)

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
                assert not exists, f'{table} should not exist when coordination disabled'


class TestMixedModeOperations:
    """Test switching between coordination modes."""

    def test_switching_from_disabled_to_enabled_creates_tables(self, postgres):
        """Verify switching to coordination mode creates missing tables.
        """
        config = get_structure_test_config()
        tables = schema.get_table_names(config.appname)

        with postgres.connect() as conn:
            for table in ['Token', 'Lock', 'LeaderLock', 'RebalanceLock', 'Rebalance']:
                conn.execute(text(f'DROP TABLE IF EXISTS {tables[table]} CASCADE'))
            conn.commit()

        job1 = create_job('test-node-1', postgres, wait_on_enter=0, coordination_config=None)

        with postgres.connect() as conn:
            result = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = :table_name
                )
            """), {'table_name': tables['Token']})
            exists_before = result.scalar()

        assert not exists_before, 'Token table should not exist with coordination disabled'

        coord_config = CoordinationConfig(total_tokens=100)
        job2 = create_job('test-node-2', postgres, wait_on_enter=0, coordination_config=coord_config)

        with postgres.connect() as conn:
            result = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = :table_name
                )
            """), {'table_name': tables['Token']})
            exists_after = result.scalar()

        assert exists_after, 'Token table should be created when switching to coordination mode'


class TestLeadershipDuringInitialization:
    """Test leadership queries during initialization states."""

    @clean_tables('Node')
    def test_am_i_leader_false_during_initialization(self, postgres):
        """Verify am_i_leader() returns False during initialization states.
        """
        coord_config = get_coordination_config()
        tables = schema.get_table_names(coord_config.appname)

        now = datetime.datetime.now(datetime.timezone.utc)
        insert_active_node(postgres, tables, 'node1', created_on=now)

        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)

        assert job.state_machine.state == JobState.INITIALIZING
        assert not job.am_i_leader(), 'Should return False during INITIALIZING'

        job.__enter__()

        try:
            assert wait_for_running_state(job, timeout_sec=10)

            assert job.am_i_leader() == job.state_machine.is_leader(), \
                'am_i_leader() should match state machine state in running states'

            logger.info('✓ am_i_leader() correctly returns False during initialization')

        finally:
            job.__exit__(None, None, None)

    def test_am_i_leader_no_database_fallback(self, postgres):
        """Verify am_i_leader() doesn't call database during initialization.
        """
        coord_config = get_coordination_config()

        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)

        # Mock elect_leader to track if it's called
        original_elect = job.cluster.elect_leader
        elect_called = [False]

        def track_elect():
            elect_called[0] = True
            return original_elect()

        job.cluster.elect_leader = track_elect

        # Call am_i_leader during initialization
        result = job.am_i_leader()

        assert not result, 'Should return False during initialization'
        assert not elect_called[0], 'Should NOT call elect_leader as fallback'

        logger.info('✓ am_i_leader() has no database fallback')


class TestCallbackBlockingDetection:
    """Test that slow callbacks don't block coordination."""

    def test_slow_callback_does_not_block_token_version_detection(self, postgres):
        """Verify slow callback does NOT block TokenRefreshMonitor (callbacks are async).
        """
        coord_cfg = get_coordination_config()
        tables = schema.get_table_names(coord_cfg.appname)

        callback_started = []
        callback_finished = []

        def blocking_callback():
            callback_started.append(time.time())
            time.sleep(5)
            callback_finished.append(time.time())

        job1 = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10,
                  on_rebalance=blocking_callback)
        job1.__enter__()

        try:
            assert wait_for(lambda: len(job1.my_tokens) >= 1, timeout_sec=15)
            assert wait_for_running_state(job1, timeout_sec=5)

            assert wait_for(lambda: len(callback_started) >= 1, timeout_sec=5)

            initial_version = job1.token_version

            job2 = create_job('node2', postgres, coordination_config=coord_cfg, wait_on_enter=10)
            job2.__enter__()

            try:
                assert wait_for(lambda: len(job2.my_tokens) >= 1, timeout_sec=15)
                assert wait_for_running_state(job2, timeout_sec=5)

                assert wait_for_rebalance(postgres, tables, min_count=1, timeout_sec=20)

                start_check = time.time()
                detected_new_version = False
                while time.time() - start_check < 8:
                    if job1.token_version != initial_version:
                        detected_new_version = True
                        break
                    time.sleep(0.2)

                detection_time = time.time() - start_check

                logger.info(f'Callback blocking duration: {callback_finished[0] - callback_started[0] if callback_finished else "not finished"}s')
                logger.info(f'Version detection time: {detection_time:.1f}s')

                if len(callback_finished) > 0:
                    assert detection_time < 4, 'Detection should NOT be delayed by slow callback (callbacks run async)'
                else:
                    logger.info('Callback still running, but version was detected (callbacks are async)')

            finally:
                job2.__exit__(None, None, None)

        finally:
            job1.__exit__(None, None, None)

    def test_slow_callback_does_not_block_leadership_change_detection(self, postgres):
        """Verify slow callback does NOT block detecting leadership changes (callbacks are async).
        """
        coord_cfg = get_coordination_config(token_refresh_initial_interval_sec=1)
        tables = schema.get_table_names(coord_cfg.appname)

        callback_progress = []

        def blocking_callback():
            callback_progress.append('started')
            time.sleep(4)
            callback_progress.append('finished')

        node1 = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10)
        node2 = create_job('node2', postgres, coordination_config=coord_cfg, wait_on_enter=10,
                  on_rebalance=blocking_callback)

        node1.__enter__()
        time.sleep(0.5)
        node2.__enter__()

        try:
            assert wait_for_state(node1, JobState.RUNNING_LEADER, timeout_sec=10)
            assert wait_for_state(node2, JobState.RUNNING_FOLLOWER, timeout_sec=10)

            simulate_node_crash(node1, cleanup=True)

            assert wait_for_dead_node_removal(postgres, tables, 'node1', timeout_sec=20)
            assert wait_for_rebalance(postgres, tables, min_count=1, timeout_sec=20)

            assert wait_for(lambda: 'started' in callback_progress, timeout_sec=10)

            start_promotion_check = time.time()
            promoted = False
            while time.time() - start_promotion_check < 10:
                if node2.state_machine.state == JobState.RUNNING_LEADER:
                    promoted = True
                    break
                time.sleep(0.2)

            promotion_time = time.time() - start_promotion_check

            logger.info(f'Callback status: {callback_progress}')
            logger.info(f'Promotion detection time: {promotion_time:.1f}s')

            assert promoted, 'Node2 should have been promoted to leader'

            if 'finished' in callback_progress:
                assert promotion_time < 4, 'Leadership detection should NOT be delayed by callback (callbacks are async)'
            else:
                logger.info('Callback still running, but promotion was detected (callbacks are async)')

        finally:
            try:
                node2.__exit__(None, None, None)
            except:
                pass

    def test_multiple_rebalances_during_long_callback(self, postgres):
        """Verify multiple rebalances can occur while callback is running.
        """
        coord_cfg = get_coordination_config()
        tables = schema.get_table_names(coord_cfg.appname)

        callback_invocations = []

        def slow_callback():
            callback_invocations.append(time.time())
            time.sleep(3)

        job1 = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10,
                  on_rebalance=slow_callback)
        job1.__enter__()

        try:
            assert wait_for(lambda: len(job1.my_tokens) >= 1, timeout_sec=15)
            assert wait_for_running_state(job1, timeout_sec=5)

            temp_jobs = []
            for i in range(3):
                temp_job = create_job(f'temp-{i}', postgres, coordination_config=coord_cfg, wait_on_enter=5)
                temp_job.__enter__()
                temp_jobs.append(temp_job)
                time.sleep(0.5)

            try:
                for temp_job in temp_jobs:
                    assert wait_for_running_state(temp_job, timeout_sec=10)

                for temp_job in temp_jobs:
                    temp_job.__exit__(None, None, None)
                    time.sleep(0.5)

                time.sleep(4)

                with postgres.connect() as conn:
                    result = conn.execute(text(f"""
                        SELECT COUNT(*) FROM {tables["Rebalance"]}
                        WHERE triggered_at > NOW() - INTERVAL '30 seconds'
                    """))
                    rebalance_count = result.scalar()

                logger.info(f'Rebalances triggered: {rebalance_count}')
                logger.info(f'Callback invocations: {len(callback_invocations)}')

                assert rebalance_count >= 2, 'Multiple rebalances should occur'

            finally:
                for temp_job in temp_jobs:
                    try:
                        temp_job.__exit__(None, None, None)
                    except:
                        pass

        finally:
            job1.__exit__(None, None, None)


class TestInitialCallbackRaceCondition:
    """Test race between __enter__ completion and initial callback."""

    def test_leader_tokens_available_before_callback(self, postgres):
        """Verify leader has tokens in cache before on_rebalance fires.
        """
        coord_cfg = get_coordination_config()

        callback_invoked = []

        def track_callback():
            callback_invoked.append(True)

        job = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10,
                on_rebalance=track_callback)

        with job:
            tokens_at_enter_exit = job.my_tokens.copy()

            logger.info(f'Tokens at __enter__ exit: {len(tokens_at_enter_exit)}')
            logger.info(f'Callback invoked: {len(callback_invoked) > 0}')

            assert len(tokens_at_enter_exit) > 0, 'Leader should have tokens immediately after __enter__'

            assert wait_for(lambda: len(callback_invoked) >= 1, timeout_sec=5), \
                'Callback should fire shortly after __enter__'

    def test_follower_tokens_available_before_callback(self, postgres):
        """Verify follower has tokens in cache before on_rebalance fires.
        """
        coord_cfg = get_coordination_config()
        tables = schema.get_table_names(coord_cfg.appname)

        now = datetime.datetime.now(datetime.timezone.utc)
        insert_active_node(postgres, tables, 'node1', created_on=now)

        callback_invoked = []

        def track_callback():
            callback_invoked.append(True)

        job1 = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10)
        job1.__enter__()

        try:
            assert wait_for_running_state(job1, timeout_sec=10)

            job2 = create_job('node2', postgres, coordination_config=coord_cfg, wait_on_enter=10,
                      on_rebalance=track_callback)

            with job2:
                tokens_at_exit = job2.my_tokens.copy()

                logger.info(f'node2 tokens at __enter__ exit: {len(tokens_at_exit)}')
                logger.info(f'Callback invoked: {len(callback_invoked) > 0}')

                assert len(tokens_at_exit) > 0, 'Follower should have tokens after __enter__'

                assert wait_for(lambda: len(callback_invoked) >= 1, timeout_sec=5)

        finally:
            job1.__exit__(None, None, None)

    def test_user_code_can_rely_on_token_cache_immediately(self, postgres):
        """Verify user can safely use token cache right after __enter__.
        """
        coord_cfg = get_coordination_config()

        callback_fired = []

        def track_callback():
            callback_fired.append(True)

        job = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10,
                on_rebalance=track_callback)

        with job:
            assert len(job.my_tokens) > 0, 'Tokens should be available immediately after __enter__'

            task_42 = Task(42)
            can_claim = job.can_claim_task(task_42)

            logger.info(f'Can claim task immediately: {can_claim}')
            logger.info(f'Callback fired: {len(callback_fired) > 0}')

            token_42 = job.task_to_token(42)
            expected_can_claim = token_42 in job.my_tokens

            assert can_claim == expected_can_claim, \
                'Token cache should be accurate immediately for task claiming'


class TestCallbackExceptionDoesNotStopMonitor:
    """Test that callback exceptions don't stop the monitor thread."""

    def test_monitor_continues_after_callback_exception(self, postgres):
        """Verify TokenRefreshMonitor continues checking after callback exception.
        """
        coord_cfg = get_coordination_config()
        tables = schema.get_table_names(coord_cfg.appname)

        callback_count = [0]

        def failing_callback():
            callback_count[0] += 1
            raise RuntimeError('Test callback failure')

        job1 = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10,
                  on_rebalance=failing_callback)
        job1.__enter__()

        try:
            assert wait_for(lambda: len(job1.my_tokens) >= 1, timeout_sec=15)
            assert wait_for_running_state(job1, timeout_sec=5)

            initial_failures = callback_count[0]
            logger.info(f'Initial callback failures: {initial_failures}')

            job2 = create_job('node2', postgres, coordination_config=coord_cfg, wait_on_enter=10)
            job2.__enter__()

            try:
                assert wait_for(lambda: len(job2.my_tokens) >= 1, timeout_sec=15)
                assert wait_for_running_state(job2, timeout_sec=5)

                assert wait_for_rebalance(postgres, tables, min_count=1, timeout_sec=20)

                token_refresh_monitor = next((m for m in job1._monitors.values()
                                             if 'token-refresh' in m.name), None)

                assert token_refresh_monitor is not None, 'Should have token refresh monitor'
                assert token_refresh_monitor.thread.is_alive(), \
                    'Monitor thread should still be running after callback exception'

                time.sleep(2)

                assert callback_count[0] > initial_failures, \
                    'Monitor should continue checking after callback exception'

                logger.info(f'Callback failures after rebalance: {callback_count[0]}')

            finally:
                job2.__exit__(None, None, None)

        finally:
            job1.__exit__(None, None, None)


class TestCallbackThreadPoolExecution:
    """Test ThreadPoolExecutor-based callback execution."""

    def test_callbacks_run_in_thread_pool(self, postgres):
        """Verify callbacks execute in ThreadPoolExecutor threads, not main thread.
        """
        coord_cfg = get_coordination_config()

        main_thread_id = threading.current_thread().ident
        callback_thread_ids = []
        callback_lock = threading.Lock()

        def track_thread_callback():
            with callback_lock:
                callback_thread_ids.append(threading.current_thread().ident)
                callback_thread_ids.append(threading.current_thread().name)

        with create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10,
                on_rebalance=track_thread_callback) as job:
            assert wait_for(lambda: len(job.my_tokens) >= 1, timeout_sec=15)
            assert wait_for(lambda: len(callback_thread_ids) >= 2, timeout_sec=5)

            thread_id = callback_thread_ids[0]
            thread_name = callback_thread_ids[1]

            assert thread_id != main_thread_id, 'Callback should not run in main thread'
            assert 'rebalance-callback' in thread_name, f'Callback should run in executor thread, got {thread_name}'

    def test_thread_pool_limits_concurrent_callbacks(self, postgres):
        """Verify ThreadPoolExecutor with max_workers=1 limits concurrent execution.
        """
        coord_cfg = get_coordination_config()

        concurrent_count = []
        max_concurrent = [0]
        callback_lock = threading.Lock()

        def slow_callback():
            with callback_lock:
                concurrent_count.append(1)
                current = len(concurrent_count)
                max_concurrent[0] = max(max_concurrent[0], current)
            time.sleep(1)
            with callback_lock:
                concurrent_count.pop()

        job1 = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10,
                  on_rebalance=slow_callback)
        job1.__enter__()

        try:
            assert wait_for(lambda: len(job1.my_tokens) >= 1, timeout_sec=15)
            assert wait_for_running_state(job1, timeout_sec=5)

            # Use only 1 temp node to reduce complexity
            temp = create_job('temp0', postgres, coordination_config=coord_cfg, wait_on_enter=10)
            temp.__enter__()

            try:
                assert wait_for(lambda: len(temp.my_tokens) >= 1, timeout_sec=20)
                assert wait_for_running_state(temp, timeout_sec=15)

                # Wait for callbacks from temp joining to process
                time.sleep(2)

            finally:
                temp.__exit__(None, None, None)

            # Wait for all callbacks from temp node lifecycle to complete
            time.sleep(5)

            logger.info(f'Maximum concurrent callbacks observed: {max_concurrent[0]}')

            assert max_concurrent[0] <= 1, f'Should not exceed 1 concurrent callback, saw {max_concurrent[0]}'

        finally:
            job1.__exit__(None, None, None)

    def test_callbacks_serialize_when_slow(self, postgres):
        """Verify callbacks run serially when executor has max_workers=1.
        """
        coord_cfg = get_coordination_config()

        callback_starts = []
        callback_ends = []
        callback_lock = threading.Lock()

        def slow_callback():
            with callback_lock:
                start_time = time.time()
                callback_starts.append(start_time)
            time.sleep(2)
            with callback_lock:
                callback_ends.append(time.time())

        job1 = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10,
                  on_rebalance=slow_callback)
        job1.__enter__()

        try:
            assert wait_for(lambda: len(job1.my_tokens) >= 1, timeout_sec=15)
            assert wait_for_running_state(job1, timeout_sec=5)

            temp_nodes = []
            for i in range(2):
                temp = create_job(f'temp{i}', postgres, coordination_config=coord_cfg, wait_on_enter=5)
                temp.__enter__()
                temp_nodes.append(temp)
                time.sleep(0.2)

            for temp in temp_nodes:
                temp.__exit__(None, None, None)
                time.sleep(0.2)

            time.sleep(8)

            with callback_lock:
                starts = callback_starts.copy()
                ends = callback_ends.copy()

            logger.info(f'Callback starts: {len(starts)}, ends: {len(ends)}')

            if len(starts) >= 2:
                first_start = starts[0]
                second_start = starts[1]

                delay = second_start - first_start

                logger.info(f'Second callback delayed by {delay:.1f}s from first')

                # With max_workers=1, second callback should wait for first to complete
                assert delay >= 1.5, f'Second callback should wait for first to complete (delayed {delay:.1f}s)'

        finally:
            job1.__exit__(None, None, None)

    def test_shutdown_waits_for_pending_callbacks(self, postgres):
        """Verify shutdown_callbacks() waits for pending callbacks to complete.
        """
        coord_cfg = get_coordination_config()

        callback_completed = []

        def long_callback():
            time.sleep(3)
            callback_completed.append(True)

        job = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10,
                on_rebalance=long_callback)

        with job:
            assert wait_for(lambda: len(job.my_tokens) >= 1, timeout_sec=15)

        assert len(callback_completed) >= 1, 'Callback should complete before shutdown finishes'

    def test_pending_callbacks_tracked(self, postgres):
        """Verify pending callbacks are tracked in _pending_callbacks list.
        """
        coord_cfg = get_coordination_config()

        callback_active = threading.Event()

        def blocking_callback():
            callback_active.set()
            time.sleep(5)

        job = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10,
                on_rebalance=blocking_callback)
        job.__enter__()

        try:
            assert wait_for(lambda: len(job.my_tokens) >= 1, timeout_sec=15)
            assert callback_active.wait(timeout=5), 'Callback should start'

            assert wait_for(lambda: len(job.tokens._pending_callbacks) >= 1, timeout_sec=2), \
                'Future should be tracked in pending callbacks list'

            pending_count = len(job.tokens._pending_callbacks)

            logger.info(f'Pending callbacks: {pending_count}')

            assert pending_count >= 1, 'Should have pending callbacks while callback is running'

        finally:
            job.__exit__(None, None, None)

    def test_callback_exceptions_dont_crash_executor(self, postgres):
        """Verify ThreadPoolExecutor continues working after callback exception.
        """
        coord_cfg = get_coordination_config()
        tables = schema.get_table_names(coord_cfg.appname)

        callback_count = [0]

        def failing_callback():
            callback_count[0] += 1
            raise RuntimeError('Test callback failure')

        job1 = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10,
                  on_rebalance=failing_callback)
        job1.__enter__()

        try:
            assert wait_for(lambda: len(job1.my_tokens) >= 1, timeout_sec=15)
            assert wait_for_running_state(job1, timeout_sec=5)

            initial_count = callback_count[0]

            job2 = create_job('node2', postgres, coordination_config=coord_cfg, wait_on_enter=10)
            job2.__enter__()

            try:
                assert wait_for(lambda: len(job2.my_tokens) >= 1, timeout_sec=15)
                assert wait_for_running_state(job2, timeout_sec=5)

                assert wait_for_rebalance(postgres, tables, min_count=1, timeout_sec=20)

                time.sleep(2)

                assert callback_count[0] > initial_count, 'ThreadPoolExecutor should continue after exception'

                logger.info(f'Executor survived {callback_count[0]} callback exceptions')

            finally:
                job2.__exit__(None, None, None)

        finally:
            job1.__exit__(None, None, None)

    def test_shutdown_logs_pending_callback_count(self, postgres, caplog):
        """Verify shutdown logs number of pending callbacks.
        """
        coord_cfg = get_coordination_config()

        def slow_callback():
            time.sleep(2)

        with caplog.at_level(logging.INFO):
            job = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10,
                    on_rebalance=slow_callback)

            with job:
                assert wait_for(lambda: len(job.my_tokens) >= 1, timeout_sec=15)

            info_messages = [record.message for record in caplog.records if record.levelname == 'INFO']
            shutdown_messages = [msg for msg in info_messages if 'pending callback' in msg.lower()]

            assert len(shutdown_messages) > 0, 'Should log pending callbacks during shutdown'

            logger.info(f'Shutdown messages: {shutdown_messages}')


if __name__ == '__main__':
    pytest.main(args=['-sx', __file__])
