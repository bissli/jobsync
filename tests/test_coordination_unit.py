"""Unit tests for individual coordination methods.

Tests each coordination component in isolation.
"""
import datetime
import logging
import time
from types import SimpleNamespace

import config as test_config
import pytest
from asserts import assert_equal, assert_true
from sqlalchemy import text

from jobsync import schema
from jobsync.client import CoordinationConfig, Job, Task

logger = logging.getLogger(__name__)


def get_unit_test_config():
    """Create a config with coordination enabled for unit tests."""
    config = SimpleNamespace()
    config.postgres = test_config.postgres
    config.sync = SimpleNamespace(
        sql=SimpleNamespace(appname='sync_'),
        coordination=SimpleNamespace(
            enabled=True,
            heartbeat_interval_sec=5,
            heartbeat_timeout_sec=15,
            rebalance_check_interval_sec=30,
            dead_node_check_interval_sec=10,
            token_refresh_initial_interval_sec=5,
            token_refresh_steady_interval_sec=30,
            total_tokens=100,
            locks_enabled=True,
            lock_orphan_warning_hours=24,
            leader_lock_timeout_sec=30,
            health_check_interval_sec=30
        )
    )
    return config


class TestTaskToToken:
    """Test consistent hashing of tasks to tokens."""

    def test_consistent_hashing(self, postgres):
        """Verify task IDs consistently map to same token and handle various ID types."""
        config = get_unit_test_config()
        job = Job('node1', config, connection_string=postgres.url.render_as_string(hide_password=False))

        task_id = 'test-task-123'
        token1 = job._task_to_token(task_id)
        token2 = job._task_to_token(task_id)

        assert_equal(token1, token2, 'Same task should map to same token')
        assert_true(0 <= token1 < 100, 'Token should be in valid range')

        tokens = {job._task_to_token(f'task-{i}') for i in range(20)}
        assert_true(len(tokens) >= 15, 'Most tasks should map to different tokens')

        numeric_token = job._task_to_token(123)
        string_token = job._task_to_token('abc-def-ghi')
        assert_true(0 <= numeric_token < 100, 'Numeric ID should produce valid token')
        assert_true(0 <= string_token < 100, 'String ID should produce valid token')


class TestPatternMatching:
    """Test SQL LIKE pattern matching."""

    @pytest.mark.parametrize(('node_name', 'pattern', 'should_match'), [
        ('node1', 'node1', True),
        ('node1', 'node2', False),
        ('prod-alpha', 'prod-%', True),
        ('prod-beta', 'prod-%', True),
        ('test-alpha', 'prod-%', False),
        ('alpha-gpu', '%-gpu', True),
        ('beta-gpu', '%-gpu', True),
        ('alpha-cpu', '%-gpu', False),
        ('prod-special-001', '%special%', True),
        ('special', '%special%', True),
        ('prod-regular-001', '%special%', False),
        ('node1', 'node_', True),
        ('node2', 'node_', True),
        ('node10', 'node_', False),
    ])
    def test_pattern_matching(self, postgres, node_name, pattern, should_match):
        """Test SQL LIKE pattern matching with various patterns."""
        config = get_unit_test_config()
        job = Job('node1', config, connection_string=postgres.url.render_as_string(hide_password=False))

        result = job._matches_pattern(node_name, pattern)
        if should_match:
            assert_true(result, f'{node_name} should match pattern {pattern}')
        else:
            assert_equal(result, False, f'{node_name} should not match pattern {pattern}')


class TestLeaderElection:
    """Test leader election logic."""

    def test_oldest_node_elected(self, postgres):
        """Test that oldest node becomes leader."""
        config = get_unit_test_config()
        tables = schema.get_table_names(config)

        base_time = datetime.datetime.now(datetime.timezone.utc)
        with postgres.connect() as conn:
            conn.execute(text(f"""
                INSERT INTO {tables["Node"]} (name, created_on, last_heartbeat)
                VALUES (:name, :created_on, :heartbeat)
            """), {'name': 'node2', 'created_on': base_time - datetime.timedelta(seconds=10), 'heartbeat': base_time})

            conn.execute(text(f"""
                INSERT INTO {tables["Node"]} (name, created_on, last_heartbeat)
                VALUES (:name, :created_on, :heartbeat)
            """), {'name': 'node1', 'created_on': base_time - datetime.timedelta(seconds=20), 'heartbeat': base_time})

            conn.execute(text(f"""
                INSERT INTO {tables["Node"]} (name, created_on, last_heartbeat)
                VALUES (:name, :created_on, :heartbeat)
            """), {'name': 'node3', 'created_on': base_time - datetime.timedelta(seconds=5), 'heartbeat': base_time})
            conn.commit()

        job = Job('test', config, connection_string=postgres.url.render_as_string(hide_password=False))
        leader = job._elect_leader()

        assert_equal(leader, 'node1', 'Oldest node should be elected leader')

    def test_name_tiebreaker(self, postgres):
        """Test that name is used as tiebreaker when timestamps equal."""
        config = get_unit_test_config()
        tables = schema.get_table_names(config)

        same_time = datetime.datetime.now(datetime.timezone.utc)
        with postgres.connect() as conn:
            for name in ['node-c', 'node-a', 'node-b']:
                conn.execute(text(f"""
                    INSERT INTO {tables["Node"]} (name, created_on, last_heartbeat)
                    VALUES (:name, :created_on, :heartbeat)
                """), {'name': name, 'created_on': same_time, 'heartbeat': same_time})
            conn.commit()

        job = Job('test', config, connection_string=postgres.url.render_as_string(hide_password=False))
        leader = job._elect_leader()

        assert_equal(leader, 'node-a', 'Alphabetically first node should win tiebreaker')

    def test_dead_nodes_filtered(self, postgres):
        """Test that dead nodes are not considered for leader election."""
        config = get_unit_test_config()
        tables = schema.get_table_names(config)

        current_time = datetime.datetime.now(datetime.timezone.utc)
        stale_time = current_time - datetime.timedelta(seconds=30)

        with postgres.connect() as conn:
            conn.execute(text(f"""
                INSERT INTO {tables["Node"]} (name, created_on, last_heartbeat)
                VALUES (:name, :created_on, :heartbeat)
            """), {'name': 'node1', 'created_on': current_time - datetime.timedelta(seconds=30), 'heartbeat': stale_time})

            conn.execute(text(f"""
                INSERT INTO {tables["Node"]} (name, created_on, last_heartbeat)
                VALUES (:name, :created_on, :heartbeat)
            """), {'name': 'node2', 'created_on': current_time - datetime.timedelta(seconds=20), 'heartbeat': current_time})
            conn.commit()

        job = Job('test', config, connection_string=postgres.url.render_as_string(hide_password=False))
        leader = job._elect_leader()

        assert_equal(leader, 'node2', 'Only alive nodes should be considered')


class TestTokenDistribution:
    """Test token distribution algorithm."""

    def test_even_distribution(self, postgres):
        """Test that tokens are distributed evenly across nodes."""
        config = get_unit_test_config()
        tables = schema.get_table_names(config)

        current = datetime.datetime.now(datetime.timezone.utc)
        with postgres.connect() as conn:
            for i in range(1, 4):
                conn.execute(text(f"""
                    INSERT INTO {tables["Node"]} (name, created_on, last_heartbeat)
                    VALUES (:name, :created_on, :heartbeat)
                """), {'name': f'node{i}', 'created_on': current, 'heartbeat': current})
            conn.commit()

        coord_config = CoordinationConfig(total_tokens=99)
        job = Job('node1', config, connection_string=postgres.url.render_as_string(hide_password=False),
                 coordination_config=coord_config)

        job._distribute_tokens_minimal_move(99)

        for i in range(1, 4):
            with postgres.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM {tables["Token"]} WHERE node = :node
                """), {'node': f'node{i}'})
                count = result.scalar()
            print(f'node{i}: {count} tokens')
            assert_true(30 <= count <= 36, f'node{i} should have ~33 tokens (got {count})')

    def test_locked_tokens_respected(self, postgres):
        """Test that locked tokens are assigned to matching nodes only."""
        config = get_unit_test_config()
        tables = schema.get_table_names(config)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Token"]}'))
            conn.execute(text(f'DELETE FROM {tables["Lock"]}'))
            conn.execute(text(f'DELETE FROM {tables["Node"]}'))
            conn.commit()

        current = datetime.datetime.now(datetime.timezone.utc)
        with postgres.connect() as conn:
            for name in ['node1', 'node2', 'special-alpha']:
                conn.execute(text(f"""
                    INSERT INTO {tables["Node"]} (name, created_on, last_heartbeat)
                    VALUES (:name, :created_on, :heartbeat)
                """), {'name': name, 'created_on': current, 'heartbeat': current})

            for token_id in range(10):
                conn.execute(text(f"""
                    INSERT INTO {tables["Lock"]} (token_id, node_patterns, reason, created_at, created_by)
                    VALUES (:token_id, :patterns, :reason, :created_at, :created_by)
                """), {'token_id': token_id, 'patterns': '["special-%"]', 'reason': 'test', 'created_at': current, 'created_by': 'test'})
            conn.commit()

        coord_config = CoordinationConfig(total_tokens=30)
        job = Job('node1', config, connection_string=postgres.url.render_as_string(hide_password=False),
                 coordination_config=coord_config)

        job._distribute_tokens_minimal_move(30)

        locked_correct = 0
        for token_id in range(10):
            with postgres.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT node FROM {tables["Token"]} WHERE token_id = :token_id
                """), {'token_id': token_id})
                owner = result.scalar()
            if owner == 'special-alpha':
                locked_correct += 1

        assert_equal(locked_correct, 10,
                    f'All 10 locked tokens should be assigned to special-alpha (got {locked_correct})')

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
            assert_true(node_name in node_counts and node_counts[node_name] > 0,
                       f'{node_name} should have some tokens')

        assert_true(node_counts['special-alpha'] >= 10,
                   'special-alpha should have at least the 10 locked tokens')


class TestLockRegistration:
    """Test lock registration API."""

    def test_register_single_lock(self, postgres):
        """Test registering a single lock."""

        config = get_unit_test_config()
        tables = schema.get_table_names(config)

        job = Job('node1', config, connection_string=postgres.url.render_as_string(hide_password=False))

        task_id = 'task-123'
        job.register_lock(task_id, 'special-%', 'test reason')

        with postgres.connect() as conn:
            result = conn.execute(text(f"""
                SELECT node_patterns, reason, created_by
                FROM {tables["Lock"]}
                WHERE token_id = :token_id
            """), {'token_id': job._task_to_token(task_id)})
            lock = [dict(row._mapping) for row in result]

        assert_equal(len(lock), 1, 'Lock should be created')
        patterns = lock[0]['node_patterns']
        assert_equal(patterns, ['special-%'])
        assert_equal(lock[0]['reason'], 'test reason')
        assert_equal(lock[0]['created_by'], 'node1')

    def test_register_bulk_locks(self, postgres):
        """Test registering multiple locks in bulk."""
        config = get_unit_test_config()
        tables = schema.get_table_names(config)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Lock"]}'))
            conn.commit()

        job = Job('node1', config, connection_string=postgres.url.render_as_string(hide_password=False))

        locks = [
            ('task-1', 'pattern-1', 'reason-1'),
            ('task-2', 'pattern-2', 'reason-2'),
            ('task-3', 'pattern-3', 'reason-3'),
        ]

        job.register_locks_bulk(locks)

        with postgres.connect() as conn:
            result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Lock"]}'))
            lock_count = result.scalar()
        assert_equal(lock_count, 3, 'All 3 locks should be created')

    def test_lock_idempotency(self, postgres):
        """Test that registering same lock twice updates to latest patterns."""

        config = get_unit_test_config()
        tables = schema.get_table_names(config)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Lock"]}'))
            conn.commit()

        job = Job('node1', config, connection_string=postgres.url.render_as_string(hide_password=False))

        task_id = 'task-123'
        job.register_lock(task_id, 'pattern-1', 'reason-1')
        job.register_lock(task_id, 'pattern-2', 'reason-2')  # Different pattern, same token

        with postgres.connect() as conn:
            result = conn.execute(text(f"""
                SELECT node_patterns FROM {tables["Lock"]}
                WHERE token_id = :token_id
            """), {'token_id': job._task_to_token(task_id)})
            locks = [dict(row._mapping) for row in result]

        assert_equal(len(locks), 1, 'Should only have 1 lock (ON CONFLICT DO UPDATE)')
        patterns = locks[0]['node_patterns']
        assert_equal(patterns, ['pattern-2'], 'Second pattern should replace first (DO UPDATE)')


class TestCanClaimTask:
    """Test task claiming logic."""

    def test_can_claim_owned_token(self, postgres):
        """Test that node can claim task if it owns the token."""
        config = get_unit_test_config()
        tables = schema.get_table_names(config)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Token"]}'))
            conn.commit()

        job = Job('node1', config, connection_string=postgres.url.render_as_string(hide_password=False))
        job._coordination_enabled = True

        token_id = 5
        with postgres.connect() as conn:
            assigned_at = datetime.datetime.now(datetime.timezone.utc)
            conn.execute(text(f"""
                INSERT INTO {tables["Token"]} (token_id, node, assigned_at, version)
                VALUES (:token_id, :node, :assigned_at, :version)
            """), {'token_id': token_id, 'node': 'node1', 'assigned_at': assigned_at, 'version': 1})
            conn.commit()

        # Update job's token cache
        job._my_tokens = {5}

        # Find a task that hashes to token 5
        task = None
        for i in range(1000):
            if job._task_to_token(i) == 5:
                task = Task(i)
                break

        assert_true(task is not None, 'Should find a task that maps to token 5')
        assert_true(job.can_claim_task(task), 'Should be able to claim task with owned token')

    def test_cannot_claim_unowned_token(self, postgres):
        """Test that node cannot claim task if it doesn't own the token."""
        config = get_unit_test_config()

        job = Job('node1', config, connection_string=postgres.url.render_as_string(hide_password=False))
        job._coordination_enabled = True

        # Node has no tokens
        job._my_tokens = set()

        task = Task(123)
        assert_equal(job.can_claim_task(task), False,
                    'Should not be able to claim task without token')


class TestThreadShutdown:
    """Test thread shutdown behavior and responsiveness."""

    def test_fast_shutdown(self, postgres):
        """Verify shutdown completes quickly without thread timeouts.
        """
        config = get_unit_test_config()

        node = Job('node1', config, wait_on_enter=5, connection_string=postgres.url.render_as_string(hide_password=False))
        node.__enter__()

        time.sleep(2)

        start = time.time()
        node.__exit__(None, None, None)
        elapsed = time.time() - start

        assert_true(elapsed < 3, f'Shutdown took {elapsed:.1f}s (should be < 3s)')
        logger.info(f'✓ Shutdown completed in {elapsed:.1f}s')

    def test_all_threads_wake_on_shutdown(self, postgres):
        """Verify all thread types respond immediately to shutdown event.
        """
        config = get_unit_test_config()

        node = Job('node1', config, wait_on_enter=5, connection_string=postgres.url.render_as_string(hide_password=False))
        node.__enter__()
        time.sleep(1)

        assert_true(node._heartbeat_thread.is_alive(), 'Heartbeat thread should be running')
        assert_true(node._health_thread.is_alive(), 'Health thread should be running')
        assert_true(node._refresh_thread.is_alive(), 'Refresh thread should be running')

        node._shutdown = True
        node._shutdown_event.set()

        node._heartbeat_thread.join(timeout=1)
        node._health_thread.join(timeout=1)
        node._refresh_thread.join(timeout=1)

        assert_equal(node._heartbeat_thread.is_alive(), False, 'Heartbeat thread should stop')
        assert_equal(node._health_thread.is_alive(), False, 'Health thread should stop')
        assert_equal(node._refresh_thread.is_alive(), False, 'Refresh thread should stop')

        logger.info('✓ All threads responded to shutdown within 1 second')

    def test_leader_threads_shutdown(self, postgres):
        """Verify leader-specific threads respond to shutdown event.
        """
        config = get_unit_test_config()
        tables = schema.get_table_names(config)

        now = datetime.datetime.now(datetime.timezone.utc)
        with postgres.connect() as conn:
            conn.execute(text(f"""
                INSERT INTO {tables["Node"]} (name, created_on, last_heartbeat)
                VALUES (:name, :created_on, :heartbeat)
            """), {'name': 'node1', 'created_on': now, 'heartbeat': now})
            conn.commit()

        node = Job('node1', config, wait_on_enter=5, connection_string=postgres.url.render_as_string(hide_password=False))
        node.__enter__()

        time.sleep(2)

        assert_true(node._monitor_thread is not None, 'Leader should have monitor thread')
        assert_true(node._rebalance_thread is not None, 'Leader should have rebalance thread')
        assert_true(node._monitor_thread.is_alive(), 'Monitor thread should be running')
        assert_true(node._rebalance_thread.is_alive(), 'Rebalance thread should be running')

        node._shutdown = True
        node._shutdown_event.set()

        node._monitor_thread.join(timeout=2)
        node._rebalance_thread.join(timeout=2)

        assert_equal(node._monitor_thread.is_alive(), False, 'Monitor thread should stop')
        assert_equal(node._rebalance_thread.is_alive(), False, 'Rebalance thread should stop')

        logger.info('✓ Leader threads responded to shutdown within 2 seconds')

    def test_no_thread_timeout_warnings(self, postgres, caplog):
        """Verify no thread timeout warnings during normal shutdown.
        """
        config = get_unit_test_config()

        with caplog.at_level(logging.WARNING):
            node = Job('node1', config, wait_on_enter=5, connection_string=postgres.url.render_as_string(hide_password=False))
            node.__enter__()
            time.sleep(1)
            node.__exit__(None, None, None)

        warning_messages = [record.message for record in caplog.records if record.levelname == 'WARNING']
        timeout_warnings = [msg for msg in warning_messages if 'did not stop within timeout' in msg]

        assert_equal(len(timeout_warnings), 0,
                    f'Should have no thread timeout warnings, found: {timeout_warnings}')

        logger.info('✓ No thread timeout warnings during shutdown')

    def test_shutdown_with_long_intervals(self, postgres):
        """Verify shutdown is fast even with long check intervals.
        """
        config = get_unit_test_config()

        config.sync.coordination.heartbeat_interval_sec = 60
        config.sync.coordination.health_check_interval_sec = 60
        config.sync.coordination.token_refresh_steady_interval_sec = 60

        node = Job('node1', config, wait_on_enter=5, connection_string=postgres.url.render_as_string(hide_password=False))
        node.__enter__()

        time.sleep(1)

        start = time.time()
        node.__exit__(None, None, None)
        elapsed = time.time() - start

        assert_true(elapsed < 3,
                   f'Shutdown with long intervals took {elapsed:.1f}s (should be < 3s)')
        logger.info(f'✓ Shutdown with 60s intervals completed in {elapsed:.1f}s')


if __name__ == '__main__':
    pytest.main(args=['-sx', __file__])
