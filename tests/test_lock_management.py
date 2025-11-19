"""Unit tests for lock lifecycle management.

Tests lock creation, listing, and cleanup APIs.
"""
import logging
import threading
from datetime import timedelta
from types import SimpleNamespace

import config as test_config
import pendulum
import pytest
from asserts import assert_equal, assert_true
from sqlalchemy import text

from jobsync import schema
from jobsync.client import Job

logger = logging.getLogger(__name__)


def get_lock_test_config():
    """Create a config with coordination enabled for lock tests.
    """
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


class TestLockClearing:
    """Test lock clearing functionality."""

    def test_clear_locks_by_creator(self, postgres):
        """Verify clearing locks by creator removes only that creator's locks.
        """
        config = get_lock_test_config()
        tables = schema.get_table_names(config)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Lock"]}'))
            conn.commit()

        job = Job('node1', config, connection_string=postgres.url.render_as_string(hide_password=False))

        job.register_task_lock('task-1', 'pattern-1', 'reason-1')
        job.register_task_lock('task-2', 'pattern-2', 'reason-2')
        job.register_task_lock('task-3', 'pattern-3', 'reason-3')

        with postgres.connect() as conn:
            count_before = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Lock"]}')).scalar()
        assert_equal(count_before, 3, 'Should have 3 locks')

        removed = job.clear_locks_by_creator('node1')
        assert_equal(removed, 3, 'Should remove 3 locks')

        with postgres.connect() as conn:
            count_after = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Lock"]}')).scalar()
        assert_equal(count_after, 0, 'Should have 0 locks remaining')

    def test_clear_locks_by_creator_selective(self, postgres):
        """Verify clearing only removes specified creator's locks.
        """
        config = get_lock_test_config()
        tables = schema.get_table_names(config)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Lock"]}'))
            conn.commit()

        connection_string = postgres.url.render_as_string(hide_password=False)
        job1 = Job('node1', config, connection_string=connection_string)
        job2 = Job('node2', config, connection_string=connection_string)

        job1.register_task_lock('task-1', 'pattern-1', 'from node1')
        job1.register_task_lock('task-2', 'pattern-2', 'from node1')
        job2.register_task_lock('task-3', 'pattern-3', 'from node2')

        with postgres.connect() as conn:
            count_before = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Lock"]}')).scalar()
        assert_equal(count_before, 3, 'Should have 3 locks total')

        removed = job1.clear_locks_by_creator('node1')
        assert_equal(removed, 2, 'Should remove 2 locks from node1')

        with postgres.connect() as conn:
            result = conn.execute(text(f'SELECT created_by FROM {tables["Lock"]}'))
            remaining = [dict(row._mapping) for row in result]
        assert_equal(len(remaining), 1, 'Should have 1 lock remaining')
        assert_equal(remaining[0]['created_by'], 'node2', 'Remaining lock should be from node2')

    def test_clear_all_locks(self, postgres):
        """Verify clear_all_locks removes all locks regardless of creator.
        """
        config = get_lock_test_config()
        tables = schema.get_table_names(config)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Lock"]}'))
            conn.commit()

        connection_string = postgres.url.render_as_string(hide_password=False)
        job1 = Job('node1', config, connection_string=connection_string)
        job2 = Job('node2', config, connection_string=connection_string)

        job1.register_task_lock('task-1', 'pattern-1', 'from node1')
        job2.register_task_lock('task-2', 'pattern-2', 'from node2')

        with postgres.connect() as conn:
            count_before = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Lock"]}')).scalar()
        assert_equal(count_before, 2, 'Should have 2 locks')

        removed = job1.clear_all_locks()
        assert_equal(removed, 2, 'Should remove all locks')

        with postgres.connect() as conn:
            count_after = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Lock"]}')).scalar()
        assert_equal(count_after, 0, 'Should have 0 locks')


class TestLockListing:
    """Test lock listing functionality."""

    def test_list_locks_empty(self, postgres):
        """Verify list_locks returns empty list when no locks exist.
        """
        config = get_lock_test_config()
        tables = schema.get_table_names(config)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Lock"]}'))
            conn.commit()

        job = Job('node1', config, connection_string=postgres.url.render_as_string(hide_password=False))
        locks = job.list_locks()

        assert_equal(locks, [], 'Should return empty list')

    def test_list_locks_content(self, postgres):
        """Verify list_locks returns correct lock information.
        """
        config = get_lock_test_config()
        tables = schema.get_table_names(config)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Lock"]}'))
            conn.commit()

        job = Job('node1', config, connection_string=postgres.url.render_as_string(hide_password=False))

        job.register_task_lock('task-1', 'pattern-1', 'reason-1')
        job.register_task_lock('task-2', 'pattern-2', 'reason-2')

        locks = job.list_locks()
        assert_equal(len(locks), 2, 'Should have 2 locks')

        lock1 = next(l for l in locks if l['node_pattern'] == 'pattern-1')
        assert_equal(lock1['reason'], 'reason-1')
        assert_equal(lock1['created_by'], 'node1')
        assert_equal(lock1['expires_at'], None)

    def test_list_locks_filters_expired(self, postgres):
        """Verify list_locks filters out expired locks.
        """
        config = get_lock_test_config()
        tables = schema.get_table_names(config)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Lock"]}'))
            conn.commit()

        job = Job('node1', config, connection_string=postgres.url.render_as_string(hide_password=False))

        job.register_task_lock('task-1', 'pattern-1', 'not expired')
        job.register_task_lock('task-2', 'pattern-2', 'expires soon', expires_in_days=1)

        expired_time = pendulum.now() - timedelta(days=2)
        with postgres.connect() as conn:
            conn.execute(text(f"""
                INSERT INTO {tables["Lock"]} (token_id, node_pattern, reason, created_at, created_by, expires_at)
                VALUES (:token_id, :pattern, :reason, :created_at, :created_by, :expires_at)
            """), {
                'token_id': 999,
                'pattern': 'pattern-expired',
                'reason': 'already expired',
                'created_at': pendulum.now(),
                'created_by': 'node1',
                'expires_at': expired_time
            })
            conn.commit()

        locks = job.list_locks()

        patterns = [l['node_pattern'] for l in locks]
        assert_true('pattern-1' in patterns, 'Non-expired lock should be listed')
        assert_true('pattern-2' in patterns, 'Future-expiring lock should be listed')
        assert_true('pattern-expired' not in patterns, 'Expired lock should not be listed')


class TestClearExistingLocks:
    """Test clear_existing_locks parameter behavior."""

    def test_clear_existing_locks_true(self, postgres):
        """Verify clear_existing_locks=True clears this node's locks before lock_provider.
        """
        config = get_lock_test_config()
        tables = schema.get_table_names(config)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Lock"]}'))
            conn.commit()

        connection_string = postgres.url.render_as_string(hide_password=False)

        def first_lock_provider(job):
            job.register_task_lock('task-1', 'pattern-OLD', 'first run')
            job.register_task_lock('task-2', 'pattern-OLD', 'first run')

        job1 = Job('node1', config, connection_string=connection_string,
                   lock_provider=first_lock_provider, clear_existing_locks=False)
        job1._register_node()
        if job1._lock_provider and job1._locks_enabled:
            job1._lock_provider(job1)

        with postgres.connect() as conn:
            count_after_first = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Lock"]} WHERE created_by = :creator'),
                                            {'creator': 'node1'}).scalar()
        assert_equal(count_after_first, 2, 'Should have 2 locks after first run')

        def second_lock_provider(job):
            job.register_task_lock('task-3', 'pattern-NEW', 'second run')

        job2 = Job('node1', config, connection_string=connection_string,
                   lock_provider=second_lock_provider, clear_existing_locks=True)
        job2._register_node()
        if job2._lock_provider and job2._locks_enabled:
            if job2._clear_existing_locks:
                job2.clear_locks_by_creator(job2.node_name)
            job2._lock_provider(job2)

        with postgres.connect() as conn:
            result = conn.execute(text(f'SELECT node_pattern FROM {tables["Lock"]} WHERE created_by = :creator'),
                                 {'creator': 'node1'})
            locks = [dict(row._mapping) for row in result]
        patterns = [l['node_pattern'] for l in locks]

        assert_equal(len(locks), 1, 'Should only have 1 lock (old ones cleared)')
        assert_equal(patterns[0], 'pattern-NEW', 'Should have new pattern only')

    def test_clear_existing_locks_false(self, postgres):
        """Verify clear_existing_locks=False preserves existing locks.
        """
        config = get_lock_test_config()
        tables = schema.get_table_names(config)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Lock"]}'))
            conn.commit()

        connection_string = postgres.url.render_as_string(hide_password=False)

        def first_lock_provider(job):
            job.register_task_lock('task-1', 'pattern-OLD', 'first run')

        job1 = Job('node1', config, connection_string=connection_string,
                   lock_provider=first_lock_provider, clear_existing_locks=False)
        job1._register_node()
        if job1._lock_provider and job1._locks_enabled:
            job1._lock_provider(job1)

        def second_lock_provider(job):
            job.register_task_lock('task-2', 'pattern-NEW', 'second run')

        job2 = Job('node1', config, connection_string=connection_string,
                   lock_provider=second_lock_provider, clear_existing_locks=False)
        job2._register_node()
        if job2._lock_provider and job2._locks_enabled:
            if job2._clear_existing_locks:
                job2.clear_locks_by_creator(job2.node_name)
            job2._lock_provider(job2)

        with postgres.connect() as conn:
            result = conn.execute(text(f'SELECT node_pattern FROM {tables["Lock"]} WHERE created_by = :creator ORDER BY node_pattern'),
                                 {'creator': 'node1'})
            locks = [dict(row._mapping) for row in result]
        patterns = [l['node_pattern'] for l in locks]

        assert_equal(len(locks), 2, 'Should have both old and new locks')
        assert_true('pattern-OLD' in patterns, 'Old lock should be preserved')
        assert_true('pattern-NEW' in patterns, 'New lock should be added')


class TestConcurrentLockRegistration:
    """Test concurrent lock registration from multiple nodes."""

    def test_same_lock_from_multiple_nodes_sequential(self, postgres):
        """Verify multiple nodes registering same lock sequentially (idempotent).
        """
        config = get_lock_test_config()
        tables = schema.get_table_names(config)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Lock"]}'))
            conn.commit()

        connection_string = postgres.url.render_as_string(hide_password=False)

        for i in range(1, 11):
            job = Job(f'node{i}', config, connection_string=connection_string)
            job.register_task_lock('task-X1', 'Node1', 'lock X1 to Node1')

        with postgres.connect() as conn:
            result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Lock"]}'))
            lock_count = result.scalar()
        assert_equal(lock_count, 1, 'Only 1 lock should exist (idempotent)')

        with postgres.connect() as conn:
            result = conn.execute(text(f'SELECT node_pattern, created_by FROM {tables["Lock"]}'))
            lock = [dict(row._mapping) for row in result]
        assert_equal(len(lock), 1)
        assert_equal(lock[0]['node_pattern'], 'Node1', 'Pattern should be Node1')
        assert_equal(lock[0]['created_by'], 'node1', 'First node should be recorded as creator')

    def test_same_lock_from_multiple_nodes_simulated_concurrent(self, postgres):
        """Verify multiple nodes can safely register same lock (simulated concurrency).
        """
        config = get_lock_test_config()
        tables = schema.get_table_names(config)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Lock"]}'))
            conn.commit()

        connection_string = postgres.url.render_as_string(hide_password=False)

        def register_lock(node_name):
            job = Job(node_name, config, connection_string=connection_string)
            job.register_task_lock('task-X1', 'Node1', f'lock from {node_name}')

        threads = []
        for i in range(1, 11):
            t = threading.Thread(target=register_lock, args=(f'node{i}',))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        with postgres.connect() as conn:
            result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Lock"]}'))
            lock_count = result.scalar()
        assert_equal(lock_count, 1, 'Only 1 lock should exist despite concurrent registration')

        with postgres.connect() as conn:
            result = conn.execute(text(f'SELECT node_pattern, created_by FROM {tables["Lock"]}'))
            lock = [dict(row._mapping) for row in result]
        assert_equal(len(lock), 1)
        assert_equal(lock[0]['node_pattern'], 'Node1', 'Pattern should be Node1')
        assert_true(lock[0]['created_by'].startswith('node'), 'Should have a node as creator')

    def test_same_lock_provider_across_cluster(self, postgres):
        """Verify all nodes using same lock_provider callback doesn't cause issues.
        """
        config = get_lock_test_config()
        tables = schema.get_table_names(config)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Lock"]}'))
            conn.commit()

        connection_string = postgres.url.render_as_string(hide_password=False)

        def shared_lock_provider(job):
            """All nodes use this same lock provider function."""
            job.register_task_lock('task-X1', 'special-node', 'lock X1')
            job.register_task_lock('task-X2', 'special-node', 'lock X2')
            job.register_task_lock('task-X3', 'special-node', 'lock X3')

        for i in range(1, 6):
            job = Job(f'node{i}', config, connection_string=connection_string,
                     lock_provider=shared_lock_provider)
            job._register_node()
            if job._lock_provider and job._locks_enabled:
                job._lock_provider(job)

        with postgres.connect() as conn:
            result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Lock"]}'))
            lock_count = result.scalar()
        assert_equal(lock_count, 3, 'Should have exactly 3 locks (deduplicated)')

        with postgres.connect() as conn:
            result = conn.execute(text(f'SELECT node_pattern FROM {tables["Lock"]} ORDER BY token_id'))
            patterns = [dict(row._mapping) for row in result]
        assert_equal(len(patterns), 3)
        for pattern_row in patterns:
            assert_equal(pattern_row['node_pattern'], 'special-node',
                        'All locks should have pattern "special-node"')

    def test_bulk_lock_registration_idempotency(self, postgres):
        """Verify bulk lock registration is idempotent across multiple nodes.
        """
        config = get_lock_test_config()
        tables = schema.get_table_names(config)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Lock"]}'))
            conn.commit()

        connection_string = postgres.url.render_as_string(hide_password=False)

        locks_to_register = [
            ('task-1', 'pattern-A', 'reason-1'),
            ('task-2', 'pattern-A', 'reason-2'),
            ('task-3', 'pattern-B', 'reason-3'),
        ]

        for i in range(1, 8):
            job = Job(f'node{i}', config, connection_string=connection_string)
            job.register_task_locks_bulk(locks_to_register)

        with postgres.connect() as conn:
            result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Lock"]}'))
            lock_count = result.scalar()
        assert_equal(lock_count, 3, 'Should have exactly 3 locks despite 7 nodes registering')

    def test_lock_created_by_shows_first_writer(self, postgres):
        """Verify created_by field shows whichever node won the race.
        """
        config = get_lock_test_config()
        tables = schema.get_table_names(config)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Lock"]}'))
            conn.commit()

        connection_string = postgres.url.render_as_string(hide_password=False)

        job1 = Job('first-node', config, connection_string=connection_string)
        job1.register_task_lock('task-99', 'pattern-test', 'registered by first')

        for i in range(2, 11):
            job = Job(f'node{i}', config, connection_string=connection_string)
            job.register_task_lock('task-99', 'pattern-test', f'attempted by node{i}')

        with postgres.connect() as conn:
            result = conn.execute(text(f'SELECT created_by FROM {tables["Lock"]}'))
            lock = [dict(row._mapping) for row in result]
        assert_equal(len(lock), 1)
        assert_equal(lock[0]['created_by'], 'first-node',
                    'created_by should show first node that successfully inserted')


if __name__ == '__main__':
    pytest.main(args=['-sx', __file__])
