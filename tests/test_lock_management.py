"""Unit tests for lock lifecycle management.

Tests lock creation, listing, and cleanup APIs.
"""
import logging

import config as test_config
import database as db
import pytest
from asserts import assert_equal, assert_true

from jobsync import schema
from jobsync.client import CoordinationConfig, Job
from libb import Setting

logger = logging.getLogger(__name__)


def get_lock_test_config():
    """Create a config with coordination enabled for lock tests.
    """
    Setting.unlock()

    config = Setting()
    config.postgres = test_config.postgres
    config.sync.sql.appname = 'sync_'
    config.sync.coordination.enabled = True
    config.sync.coordination.heartbeat_interval_sec = 5
    config.sync.coordination.heartbeat_timeout_sec = 15
    config.sync.coordination.rebalance_check_interval_sec = 30
    config.sync.coordination.dead_node_check_interval_sec = 10
    config.sync.coordination.token_refresh_initial_interval_sec = 5
    config.sync.coordination.token_refresh_steady_interval_sec = 30
    config.sync.coordination.total_tokens = 100
    config.sync.coordination.locks_enabled = True
    config.sync.coordination.lock_orphan_warning_hours = 24
    config.sync.coordination.leader_lock_timeout_sec = 30
    config.sync.coordination.health_check_interval_sec = 30

    Setting.lock()
    return config


class TestLockClearing:
    """Test lock clearing functionality."""

    def test_clear_locks_by_creator(self, psql_docker, postgres):
        """Verify clearing locks by creator removes only that creator's locks.
        """
        config = get_lock_test_config()
        cn = db.connect('postgres', config)
        tables = schema.get_table_names(config)

        db.execute(cn, f'DELETE FROM {tables["Lock"]}')

        job = Job('node1', 'postgres', config, skip_db_init=True)

        job.register_task_lock('task-1', 'pattern-1', 'reason-1')
        job.register_task_lock('task-2', 'pattern-2', 'reason-2')
        job.register_task_lock('task-3', 'pattern-3', 'reason-3')

        count_before = db.select_scalar(cn, f'SELECT COUNT(*) FROM {tables["Lock"]}')
        assert_equal(count_before, 3, 'Should have 3 locks')

        removed = job.clear_locks_by_creator('node1')
        assert_equal(removed, 3, 'Should remove 3 locks')

        count_after = db.select_scalar(cn, f'SELECT COUNT(*) FROM {tables["Lock"]}')
        assert_equal(count_after, 0, 'Should have 0 locks remaining')

    def test_clear_locks_by_creator_selective(self, psql_docker, postgres):
        """Verify clearing only removes specified creator's locks.
        """
        config = get_lock_test_config()
        cn = db.connect('postgres', config)
        tables = schema.get_table_names(config)

        db.execute(cn, f'DELETE FROM {tables["Lock"]}')

        job1 = Job('node1', 'postgres', config, skip_db_init=True)
        job2 = Job('node2', 'postgres', config, skip_db_init=True)

        job1.register_task_lock('task-1', 'pattern-1', 'from node1')
        job1.register_task_lock('task-2', 'pattern-2', 'from node1')
        job2.register_task_lock('task-3', 'pattern-3', 'from node2')

        count_before = db.select_scalar(cn, f'SELECT COUNT(*) FROM {tables["Lock"]}')
        assert_equal(count_before, 3, 'Should have 3 locks total')

        removed = job1.clear_locks_by_creator('node1')
        assert_equal(removed, 2, 'Should remove 2 locks from node1')

        remaining = db.select(cn, f'SELECT created_by FROM {tables["Lock"]}').to_dict('records')
        assert_equal(len(remaining), 1, 'Should have 1 lock remaining')
        assert_equal(remaining[0]['created_by'], 'node2', 'Remaining lock should be from node2')

    def test_clear_all_locks(self, psql_docker, postgres):
        """Verify clear_all_locks removes all locks regardless of creator.
        """
        config = get_lock_test_config()
        cn = db.connect('postgres', config)
        tables = schema.get_table_names(config)

        db.execute(cn, f'DELETE FROM {tables["Lock"]}')

        job1 = Job('node1', 'postgres', config, skip_db_init=True)
        job2 = Job('node2', 'postgres', config, skip_db_init=True)

        job1.register_task_lock('task-1', 'pattern-1', 'from node1')
        job2.register_task_lock('task-2', 'pattern-2', 'from node2')

        count_before = db.select_scalar(cn, f'SELECT COUNT(*) FROM {tables["Lock"]}')
        assert_equal(count_before, 2, 'Should have 2 locks')

        removed = job1.clear_all_locks()
        assert_equal(removed, 2, 'Should remove all locks')

        count_after = db.select_scalar(cn, f'SELECT COUNT(*) FROM {tables["Lock"]}')
        assert_equal(count_after, 0, 'Should have 0 locks')


class TestLockListing:
    """Test lock listing functionality."""

    def test_list_locks_empty(self, psql_docker, postgres):
        """Verify list_locks returns empty list when no locks exist.
        """
        config = get_lock_test_config()
        cn = db.connect('postgres', config)
        tables = schema.get_table_names(config)

        db.execute(cn, f'DELETE FROM {tables["Lock"]}')

        job = Job('node1', 'postgres', config, skip_db_init=True)
        locks = job.list_locks()

        assert_equal(locks, [], 'Should return empty list')

    def test_list_locks_content(self, psql_docker, postgres):
        """Verify list_locks returns correct lock information.
        """
        config = get_lock_test_config()
        cn = db.connect('postgres', config)
        tables = schema.get_table_names(config)

        db.execute(cn, f'DELETE FROM {tables["Lock"]}')

        job = Job('node1', 'postgres', config, skip_db_init=True)

        job.register_task_lock('task-1', 'pattern-1', 'reason-1')
        job.register_task_lock('task-2', 'pattern-2', 'reason-2')

        locks = job.list_locks()
        assert_equal(len(locks), 2, 'Should have 2 locks')

        lock1 = next(l for l in locks if l['node_pattern'] == 'pattern-1')
        assert_equal(lock1['reason'], 'reason-1')
        assert_equal(lock1['created_by'], 'node1')
        assert_equal(lock1['expires_at'], None)

    def test_list_locks_filters_expired(self, psql_docker, postgres):
        """Verify list_locks filters out expired locks.
        """
        config = get_lock_test_config()
        cn = db.connect('postgres', config)
        tables = schema.get_table_names(config)

        db.execute(cn, f'DELETE FROM {tables["Lock"]}')

        job = Job('node1', 'postgres', config, skip_db_init=True)

        from datetime import timedelta

        from date import now

        job.register_task_lock('task-1', 'pattern-1', 'not expired')
        job.register_task_lock('task-2', 'pattern-2', 'expires soon', expires_in_days=1)

        expired_time = now() - timedelta(days=2)
        db.execute(cn, f"""
            INSERT INTO {tables["Lock"]} (token_id, node_pattern, reason, created_at, created_by, expires_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, 999, 'pattern-expired', 'already expired', now(), 'node1', expired_time)

        locks = job.list_locks()

        patterns = [l['node_pattern'] for l in locks]
        assert_true('pattern-1' in patterns, 'Non-expired lock should be listed')
        assert_true('pattern-2' in patterns, 'Future-expiring lock should be listed')
        assert_true('pattern-expired' not in patterns, 'Expired lock should not be listed')


class TestClearExistingLocks:
    """Test clear_existing_locks parameter behavior."""

    def test_clear_existing_locks_true(self, psql_docker, postgres):
        """Verify clear_existing_locks=True clears this node's locks before lock_provider.
        """
        config = get_lock_test_config()
        cn = db.connect('postgres', config)
        tables = schema.get_table_names(config)

        db.execute(cn, f'DELETE FROM {tables["Lock"]}')

        def first_lock_provider(job):
            job.register_task_lock('task-1', 'pattern-OLD', 'first run')
            job.register_task_lock('task-2', 'pattern-OLD', 'first run')

        job1 = Job('node1', 'postgres', config, skip_db_init=True,
                   lock_provider=first_lock_provider, clear_existing_locks=False)
        job1._register_node()
        if job1._lock_provider and job1._locks_enabled:
            job1._lock_provider(job1)

        count_after_first = db.select_scalar(cn, f'SELECT COUNT(*) FROM {tables["Lock"]} WHERE created_by = %s', 'node1')
        assert_equal(count_after_first, 2, 'Should have 2 locks after first run')

        def second_lock_provider(job):
            job.register_task_lock('task-3', 'pattern-NEW', 'second run')

        job2 = Job('node1', 'postgres', config, skip_db_init=True,
                   lock_provider=second_lock_provider, clear_existing_locks=True)
        job2._register_node()
        if job2._lock_provider and job2._locks_enabled:
            if job2._clear_existing_locks:
                job2.clear_locks_by_creator(job2.node_name)
            job2._lock_provider(job2)

        locks = db.select(cn, f'SELECT node_pattern FROM {tables["Lock"]} WHERE created_by = %s', 'node1').to_dict('records')
        patterns = [l['node_pattern'] for l in locks]

        assert_equal(len(locks), 1, 'Should only have 1 lock (old ones cleared)')
        assert_equal(patterns[0], 'pattern-NEW', 'Should have new pattern only')

    def test_clear_existing_locks_false(self, psql_docker, postgres):
        """Verify clear_existing_locks=False preserves existing locks.
        """
        config = get_lock_test_config()
        cn = db.connect('postgres', config)
        tables = schema.get_table_names(config)

        db.execute(cn, f'DELETE FROM {tables["Lock"]}')

        def first_lock_provider(job):
            job.register_task_lock('task-1', 'pattern-OLD', 'first run')

        job1 = Job('node1', 'postgres', config, skip_db_init=True,
                   lock_provider=first_lock_provider, clear_existing_locks=False)
        job1._register_node()
        if job1._lock_provider and job1._locks_enabled:
            job1._lock_provider(job1)

        def second_lock_provider(job):
            job.register_task_lock('task-2', 'pattern-NEW', 'second run')

        job2 = Job('node1', 'postgres', config, skip_db_init=True,
                   lock_provider=second_lock_provider, clear_existing_locks=False)
        job2._register_node()
        if job2._lock_provider and job2._locks_enabled:
            if job2._clear_existing_locks:
                job2.clear_locks_by_creator(job2.node_name)
            job2._lock_provider(job2)

        locks = db.select(cn, f'SELECT node_pattern FROM {tables["Lock"]} WHERE created_by = %s ORDER BY node_pattern', 'node1').to_dict('records')
        patterns = [l['node_pattern'] for l in locks]

        assert_equal(len(locks), 2, 'Should have both old and new locks')
        assert_true('pattern-OLD' in patterns, 'Old lock should be preserved')
        assert_true('pattern-NEW' in patterns, 'New lock should be added')


if __name__ == '__main__':
    pytest.main(args=['-sx', __file__])
