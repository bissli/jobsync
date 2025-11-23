"""Tests for lock registration, management, and coordination.

USE THIS FILE FOR:
- Lock registration and lifecycle tests
- Lock provider callback tests
- Pattern matching and fallback logic
- Concurrent lock acquisition
- Leader lock coordination
"""
import datetime
import logging
import threading
import time
from dataclasses import replace

import pytest
from fixtures import *  # noqa: F401, F403
from sqlalchemy import text

from jobsync import schema
from jobsync.client import CoordinationConfig, JobState, LockNotAcquired

logger = logging.getLogger(__name__)


class TestLockRegistration:
    """Test lock registration API."""

    @clean_tables('Lock')
    def test_register_single_lock(self, postgres):
        """Test registering a single lock."""

        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)

        task_id = 'task-123'
        job.register_lock(task_id, 'special-%', 'test reason')

        with postgres.connect() as conn:
            result = conn.execute(text(f"""
                SELECT node_patterns, reason, created_by
                FROM {tables["Lock"]}
                WHERE task_id = :task_id
            """), {'task_id': str(task_id)})
            lock = [dict(row._mapping) for row in result]

        assert len(lock) == 1, 'Lock should be created'
        patterns = lock[0]['node_patterns']
        assert patterns == ['special-%']
        assert lock[0]['reason'] == 'test reason'
        assert lock[0]['created_by'] == 'node1'

    @clean_tables('Lock')
    def test_register_bulk_locks(self, postgres):
        """Test registering multiple locks in bulk."""
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)

        locks = [
            ('task-1', 'pattern-1', 'reason-1'),
            ('task-2', 'pattern-2', 'reason-2'),
            ('task-3', 'pattern-3', 'reason-3'),
        ]

        job.register_locks_bulk(locks)

        with postgres.connect() as conn:
            result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Lock"]}'))
            lock_count = result.scalar()
        assert lock_count == 3, 'All 3 locks should be created'

    @clean_tables('Lock')
    def test_lock_idempotency(self, postgres):
        """Test that registering same lock twice updates to latest patterns."""

        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)

        task_id = 'task-123'
        job.register_lock(task_id, 'pattern-1', 'reason-1')
        job.register_lock(task_id, 'pattern-2', 'reason-2')  # Different pattern, same token

        with postgres.connect() as conn:
            result = conn.execute(text(f"""
                SELECT node_patterns FROM {tables["Lock"]}
                WHERE task_id = :task_id
            """), {'task_id': str(task_id)})
            locks = [dict(row._mapping) for row in result]

        assert len(locks) == 1, 'Should only have 1 lock (ON CONFLICT DO UPDATE)'
        patterns = locks[0]['node_patterns']
        assert patterns == ['pattern-2'], 'Second pattern should replace first (DO UPDATE)'


class TestConcurrentLockRegistration:
    """Test concurrent lock registration from multiple nodes."""

    @clean_tables('Lock')
    def test_same_lock_from_multiple_nodes_sequential(self, postgres):
        """Verify multiple nodes registering same lock sequentially (idempotent).
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        for i in range(1, 11):
            job = create_job(f'node{i}', postgres, coordination_config=config, wait_on_enter=0)
            job.register_lock('task-X1', 'Node1', 'lock X1 to Node1')

        with postgres.connect() as conn:
            result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Lock"]}'))
            lock_count = result.scalar()
        assert lock_count == 1, 'Only 1 lock should exist (idempotent)'

        with postgres.connect() as conn:
            result = conn.execute(text(f'SELECT node_patterns, created_by FROM {tables["Lock"]}'))
            lock = [dict(row._mapping) for row in result]
        assert len(lock) == 1
        patterns = lock[0]['node_patterns']
        assert patterns == ['Node1'], 'Pattern should be ["Node1"]'
        assert lock[0]['created_by'] == 'node10', 'Last node should be recorded as creator (DO UPDATE)'

    @clean_tables('Lock')
    def test_same_lock_from_multiple_nodes_simulated_concurrent(self, postgres):
        """Verify multiple nodes can safely register same lock (simulated concurrency).
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        def register_lock(node_name):
            job = create_job(node_name, postgres, coordination_config=config, wait_on_enter=0)
            job.register_lock('task-X1', 'Node1', f'lock from {node_name}')

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
        assert lock_count == 1, 'Only 1 lock should exist despite concurrent registration'

    @clean_tables('Lock')
    def test_same_lock_provider_across_cluster(self, postgres):
        """Verify all nodes using same lock_provider callback doesn't cause issues.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        def shared_lock_provider(job):
            job.register_lock('task-X1', 'special-node', 'lock X1')
            job.register_lock('task-X2', 'special-node', 'lock X2')
            job.register_lock('task-X3', 'special-node', 'lock X3')

        for i in range(1, 6):
            job = create_job(f'node{i}', postgres, coordination_config=config, wait_on_enter=0)
            shared_lock_provider(job)

        with postgres.connect() as conn:
            result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Lock"]}'))
            lock_count = result.scalar()
        assert lock_count == 3, 'Should have exactly 3 locks (deduplicated)'

    @clean_tables('Lock')
    def test_bulk_lock_registration_idempotency(self, postgres):
        """Verify bulk lock registration is idempotent across multiple nodes.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        locks_to_register = [
            ('task-1', 'pattern-A', 'reason-1'),
            ('task-2', 'pattern-A', 'reason-2'),
            ('task-3', 'pattern-B', 'reason-3'),
        ]

        for i in range(1, 8):
            job = create_job(f'node{i}', postgres, coordination_config=config, wait_on_enter=0)
            job.register_locks_bulk(locks_to_register)

        with postgres.connect() as conn:
            result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Lock"]}'))
            lock_count = result.scalar()
        assert lock_count == 3, 'Should have exactly 3 locks despite 7 nodes registering'


class TestLockClearing:
    """Test lock clearing functionality."""

    @clean_tables('Lock')
    def test_clear_locks_by_creator(self, postgres):
        """Verify clearing locks by creator removes only that creator's locks.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)

        job.register_lock('task-1', 'pattern-1', 'reason-1')
        job.register_lock('task-2', 'pattern-2', 'reason-2')
        job.register_lock('task-3', 'pattern-3', 'reason-3')

        with postgres.connect() as conn:
            count_before = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Lock"]}')).scalar()
        assert count_before == 3, 'Should have 3 locks'

        removed = job.clear_locks_by_creator('node1')
        assert removed == 3, 'Should remove 3 locks'

        with postgres.connect() as conn:
            count_after = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Lock"]}')).scalar()
        assert count_after == 0, 'Should have 0 locks remaining'

    @clean_tables('Lock')
    def test_clear_locks_by_creator_selective(self, postgres):
        """Verify clearing only removes specified creator's locks.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        job1 = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)
        job2 = create_job('node2', postgres, coordination_config=config, wait_on_enter=0)

        job1.register_lock('task-1', 'pattern-1', 'from node1')
        job1.register_lock('task-2', 'pattern-2', 'from node1')
        job2.register_lock('task-3', 'pattern-3', 'from node2')

        with postgres.connect() as conn:
            count_before = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Lock"]}')).scalar()
        assert count_before == 3, 'Should have 3 locks total'

        removed = job1.clear_locks_by_creator('node1')
        assert removed == 2, 'Should remove 2 locks from node1'

        with postgres.connect() as conn:
            result = conn.execute(text(f'SELECT created_by FROM {tables["Lock"]}'))
            remaining = [dict(row._mapping) for row in result]
        assert len(remaining) == 1, 'Should have 1 lock remaining'
        assert remaining[0]['created_by'] == 'node2', 'Remaining lock should be from node2'

    @clean_tables('Lock')
    def test_clear_all_locks(self, postgres):
        """Verify clear_all_locks removes all locks regardless of creator.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        job1 = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)
        job2 = create_job('node2', postgres, coordination_config=config, wait_on_enter=0)

        job1.register_lock('task-1', 'pattern-1', 'from node1')
        job2.register_lock('task-2', 'pattern-2', 'from node2')

        with postgres.connect() as conn:
            count_before = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Lock"]}')).scalar()
        assert count_before == 2, 'Should have 2 locks'

        removed = job1.clear_all_locks()
        assert removed == 2, 'Should remove all locks'

        with postgres.connect() as conn:
            count_after = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Lock"]}')).scalar()
        assert count_after == 0, 'Should have 0 locks'


class TestLockListing:
    """Test lock listing functionality."""

    @clean_tables('Lock')
    def test_list_locks_empty(self, postgres):
        """Verify list_locks returns empty list when no locks exist.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)
        locks = job.list_locks()

        assert locks == [], 'Should return empty list'

    @clean_tables('Lock')
    def test_list_locks_content(self, postgres):
        """Verify list_locks returns correct lock information.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)

        job.register_lock('task-1', 'pattern-1', 'reason-1')
        job.register_lock('task-2', 'pattern-2', 'reason-2')

        locks = job.list_locks()
        assert len(locks) == 2, 'Should have 2 locks'

        lock1 = next(l for l in locks if l['node_patterns'] == ['pattern-1'])
        assert lock1['reason'] == 'reason-1'
        assert lock1['created_by'] == 'node1'
        assert lock1['expires_at'] is None

    @clean_tables('Lock')
    def test_list_locks_filters_expired(self, postgres):
        """Verify list_locks filters out expired locks.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)

        job.register_lock('task-1', 'pattern-1', 'not expired')
        job.register_lock('task-2', 'pattern-2', 'expires soon', expires_in_days=1)

        expired_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=2)
        insert_lock(postgres, tables, 999, ['pattern-expired'],
                   created_by='node1', expires_at=expired_time, reason='already expired')

        locks = job.list_locks()

        patterns = [l['node_patterns'][0] for l in locks]
        assert 'pattern-1' in patterns, 'Non-expired lock should be listed'
        assert 'pattern-2' in patterns, 'Future-expiring lock should be listed'
        assert 'pattern-expired' not in patterns, 'Expired lock should not be listed'


class TestClearExistingLocks:
    """Test clear_existing_locks parameter behavior."""

    @clean_tables('Lock')
    def test_clear_existing_locks_true(self, postgres):
        """Verify clear_existing_locks=True clears this node's locks before lock_provider.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        def first_lock_provider(job):
            job.register_lock('task-1', 'pattern-OLD', 'first run')
            job.register_lock('task-2', 'pattern-OLD', 'first run')

        with create_job('node1', postgres, coordination_config=config,
                 lock_provider=first_lock_provider, clear_existing_locks=False,
                 wait_on_enter=0, wait_on_exit=0):
            pass

        with postgres.connect() as conn:
            count_after_first = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Lock"]} WHERE created_by = :creator'),
                                            {'creator': 'node1'}).scalar()
        assert count_after_first == 2, 'Should have 2 locks after first run'

        def second_lock_provider(job):
            job.register_lock('task-3', 'pattern-NEW', 'second run')

        with create_job('node1', postgres, coordination_config=config,
                 lock_provider=second_lock_provider, clear_existing_locks=True,
                 wait_on_enter=0, wait_on_exit=0):
            pass

        with postgres.connect() as conn:
            result = conn.execute(text(f'SELECT node_patterns FROM {tables["Lock"]} WHERE created_by = :creator'),
                                 {'creator': 'node1'})
            locks = [dict(row._mapping) for row in result]

        patterns = [l['node_patterns'][0] for l in locks]

        assert len(locks) == 1, 'Should only have 1 lock (old ones cleared)'
        assert patterns[0] == 'pattern-NEW', 'Should have new pattern only'

    @clean_tables('Lock')
    def test_clear_existing_locks_false(self, postgres):
        """Verify clear_existing_locks=False preserves existing locks.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        def first_lock_provider(job):
            job.register_lock('task-1', 'pattern-OLD', 'first run')

        with create_job('node1', postgres, coordination_config=config,
                 lock_provider=first_lock_provider, clear_existing_locks=False,
                 wait_on_enter=0, wait_on_exit=0):
            pass

        def second_lock_provider(job):
            job.register_lock('task-2', 'pattern-NEW', 'second run')

        with create_job('node1', postgres, coordination_config=config,
                 lock_provider=second_lock_provider, clear_existing_locks=False,
                 wait_on_enter=0, wait_on_exit=0):
            pass

        with postgres.connect() as conn:
            result = conn.execute(text(f'SELECT node_patterns FROM {tables["Lock"]} WHERE created_by = :creator ORDER BY task_id'),
                                 {'creator': 'node1'})
            locks = [dict(row._mapping) for row in result]

        patterns = [l['node_patterns'][0] for l in locks]

        assert len(locks) == 2, 'Should have both old and new locks'
        assert 'pattern-OLD' in patterns, 'Old lock should be preserved'
        assert 'pattern-NEW' in patterns, 'New lock should be added'


class TestLockProviderTiming:
    """Test lock_provider callback timing in state machine."""

    @clean_tables('Lock')
    def test_lock_provider_called_during_cluster_forming(self, postgres):
        """Verify lock_provider is invoked during CLUSTER_FORMING state entry.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        callback_invoked = []

        def track_lock_provider(job):
            callback_invoked.append(job.state_machine.state)
            job.register_lock('task-1', 'pattern-1', 'test lock')

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=2,
                         lock_provider=track_lock_provider)

        job.__enter__()

        try:
            assert wait_for_running_state(job, timeout_sec=5)

            assert len(callback_invoked) > 0, 'lock_provider should be invoked'
            assert callback_invoked[0] == JobState.CLUSTER_FORMING, 'lock_provider should be called during CLUSTER_FORMING'

            with postgres.connect() as conn:
                result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Lock"]}'))
                lock_count = result.scalar()

            assert lock_count == 1, 'Lock should be registered'

            logger.info('✓ lock_provider invoked at correct state')

        finally:
            job.__exit__(None, None, None)

    def test_lock_provider_not_called_without_coordination(self, postgres):
        """Verify lock_provider not invoked when coordination disabled.
        """
        callback_invoked = []

        def track_lock_provider(job):
            callback_invoked.append(True)

        job = create_job('node1', postgres, wait_on_enter=0,
                         coordination_config=None, lock_provider=track_lock_provider)

        job.__enter__()

        try:
            assert len(callback_invoked) == 0, 'lock_provider should not be invoked when coordination disabled'

            logger.info('✓ lock_provider skipped when coordination disabled')

        finally:
            job.__exit__(None, None, None)


class TestLockFallbackPatterns:
    """Test lock fallback pattern matching in real cluster."""

    @clean_tables('Node', 'Lock', 'Token')
    def test_fallback_to_second_pattern(self, postgres):
        """Verify lock uses second pattern when first has no match.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        now = datetime.datetime.now(datetime.timezone.utc)
        insert_active_node(postgres, tables, 'node1', created_on=now)
        insert_active_node(postgres, tables, 'node2', created_on=now + datetime.timedelta(seconds=1))
        insert_active_node(postgres, tables, 'special-node', created_on=now + datetime.timedelta(seconds=2))

        def register_fallback_locks(job) -> None:
            job.register_lock('task-1', ['missing-%', 'special-%', 'node%'], 'test fallback')

        coord_config = CoordinationConfig(total_tokens=30)
        job = create_job('node1', postgres, wait_on_enter=5,
                        coordination_config=coord_config, lock_provider=register_fallback_locks)
        job.__enter__()

        try:
            time.sleep(2)

            token_id = job.task_to_token('task-1')

            with postgres.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT node FROM {tables["Token"]} WHERE token_id = :token_id
                """), {'token_id': token_id})
                assigned_node = result.scalar()

            assert assigned_node == 'special-node', \
                'Should use second pattern (special-%) when first (missing-%) has no match'

            logger.info('✓ Lock fallback to second pattern successful')

        finally:
            job.__exit__(None, None, None)

    @clean_tables('Node', 'Lock', 'Token')
    def test_fallback_to_third_pattern(self, postgres):
        """Verify lock falls back to third pattern when first two fail.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        now = datetime.datetime.now(datetime.timezone.utc)
        insert_active_node(postgres, tables, 'node1', created_on=now)
        insert_active_node(postgres, tables, 'node2', created_on=now + datetime.timedelta(seconds=1))

        def register_fallback_locks(job) -> None:
            job.register_lock('task-1', ['missing-%', 'also-missing-%', 'node%'], 'three-level fallback')

        coord_config = CoordinationConfig(total_tokens=30)
        job = create_job('node1', postgres, wait_on_enter=5,
                        coordination_config=coord_config, lock_provider=register_fallback_locks)
        job.__enter__()

        try:
            time.sleep(2)

            token_id = job.task_to_token('task-1')

            with postgres.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT node FROM {tables["Token"]} WHERE token_id = :token_id
                """), {'token_id': token_id})
                assigned_node = result.scalar()

            assert assigned_node in {'node1', 'node2'}, \
                'Should use third pattern (node%) when first two fail'

            logger.info('✓ Lock fallback to third pattern successful')

        finally:
            job.__exit__(None, None, None)

    @clean_tables('Node', 'Lock', 'Token')
    def test_all_fallback_patterns_fail(self, postgres):
        """Verify token not assigned when all fallback patterns fail.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        now = datetime.datetime.now(datetime.timezone.utc)
        insert_active_node(postgres, tables, 'node1', created_on=now)
        insert_active_node(postgres, tables, 'node2', created_on=now + datetime.timedelta(seconds=1))

        def register_failed_fallback_locks(job) -> None:
            job.register_lock('task-1', ['missing-%', 'also-missing-%', 'nope-%'], 'all fail')

        coord_config = CoordinationConfig(total_tokens=30)
        job = create_job('node1', postgres, wait_on_enter=5,
                        coordination_config=coord_config, lock_provider=register_failed_fallback_locks)
        job.__enter__()

        try:
            time.sleep(2)

            token_id = job.task_to_token('task-1')

            with postgres.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM {tables["Token"]} WHERE token_id = :token_id
                """), {'token_id': token_id})
                count = result.scalar()

            assert count == 0, 'Token should not be assigned when all patterns fail'

            logger.info('✓ All fallback patterns failed as expected')

        finally:
            job.__exit__(None, None, None)

    @clean_tables('Node', 'Lock', 'Token')
    def test_first_pattern_used_when_matches(self, postgres):
        """Verify first pattern is used when it matches (no fallback needed).
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        now = datetime.datetime.now(datetime.timezone.utc)
        insert_active_node(postgres, tables, 'primary-node', created_on=now)
        insert_active_node(postgres, tables, 'backup-node', created_on=now + datetime.timedelta(seconds=1))

        def register_primary_locks(job) -> None:
            job.register_lock('task-1', ['primary-%', 'backup-%'], 'primary first')

        coord_config = CoordinationConfig(total_tokens=30)
        job = create_job('primary-node', postgres, wait_on_enter=5,
                        coordination_config=coord_config, lock_provider=register_primary_locks)
        job.__enter__()

        try:
            time.sleep(2)

            token_id = job.task_to_token('task-1')

            with postgres.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT node FROM {tables["Token"]} WHERE token_id = :token_id
                """), {'token_id': token_id})
                assigned_node = result.scalar()

            assert assigned_node == 'primary-node', \
                'Should use first pattern (primary-%) when it matches'

            logger.info('✓ First pattern used when matching')

        finally:
            job.__exit__(None, None, None)

    @clean_tables('Node', 'Lock', 'Token')
    def test_fallback_survives_node_death(self, postgres):
        """Verify fallback patterns work correctly when primary pattern node dies.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        now = datetime.datetime.now(datetime.timezone.utc)
        insert_active_node(postgres, tables, 'leader', created_on=now)
        insert_active_node(postgres, tables, 'primary-node', created_on=now + datetime.timedelta(seconds=1))
        insert_active_node(postgres, tables, 'backup-node', created_on=now + datetime.timedelta(seconds=2))

        def register_failover_locks(job) -> None:
            job.register_lock('task-1', ['primary-%', 'backup-%'], 'failover test')

        coord_config = CoordinationConfig(total_tokens=30, dead_node_check_interval_sec=1)
        leader = create_job('leader', postgres, wait_on_enter=8,
                           coordination_config=coord_config, lock_provider=register_failover_locks)
        leader.__enter__()

        try:
            time.sleep(2)

            token_id = leader.task_to_token('task-1')

            with postgres.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT node FROM {tables["Token"]} WHERE token_id = :token_id
                """), {'token_id': token_id})
                initial_assignment = result.scalar()

            assert initial_assignment == 'primary-node', 'Should initially use primary-node'

            with postgres.connect() as conn:
                conn.execute(text(f"""
                    UPDATE {tables["Node"]}
                    SET last_heartbeat = NOW() - INTERVAL '30 seconds'
                    WHERE name = 'primary-node'
                """))
                conn.commit()

            assert wait_for_dead_node_removal(postgres, tables, 'primary-node', timeout_sec=15)

            assert wait_for_rebalance(postgres, tables, min_count=2, timeout_sec=20)

            with postgres.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT node FROM {tables["Token"]} WHERE token_id = :token_id
                """), {'token_id': token_id})
                final_assignment = result.scalar()

            assert final_assignment == 'backup-node', \
                'Should fallback to backup-node when primary-node dies'

            logger.info('✓ Fallback pattern used after primary node died')

        finally:
            leader.__exit__(None, None, None)

    @clean_tables('Node', 'Lock', 'Token')
    def test_multiple_locks_with_different_fallbacks(self, postgres):
        """Verify multiple locks with different fallback patterns work independently.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        now = datetime.datetime.now(datetime.timezone.utc)
        insert_active_node(postgres, tables, 'node1', created_on=now)
        insert_active_node(postgres, tables, 'alpha-node', created_on=now + datetime.timedelta(seconds=1))
        insert_active_node(postgres, tables, 'beta-node', created_on=now + datetime.timedelta(seconds=2))

        def register_multiple_fallbacks(job) -> None:
            job.register_lock('task-1', ['missing-%', 'alpha-%'], 'fallback to alpha')
            job.register_lock('task-2', ['missing-%', 'beta-%'], 'fallback to beta')
            job.register_lock('task-3', ['node%', 'alpha-%'], 'use node pattern')

        coord_config = CoordinationConfig(total_tokens=50)
        job = create_job('node1', postgres, wait_on_enter=5,
                        coordination_config=coord_config, lock_provider=register_multiple_fallbacks)
        job.__enter__()

        try:
            time.sleep(2)

            assignments = {}
            for task_id in ['task-1', 'task-2', 'task-3']:
                token_id = job.task_to_token(task_id)
                with postgres.connect() as conn:
                    result = conn.execute(text(f"""
                        SELECT node FROM {tables["Token"]} WHERE token_id = :token_id
                    """), {'token_id': token_id})
                    assignments[task_id] = result.scalar()

            assert assignments['task-1'] == 'alpha-node', 'task-1 should use alpha-node'
            assert assignments['task-2'] == 'beta-node', 'task-2 should use beta-node'
            assert assignments['task-3'] == 'node1', 'task-3 should use node1 (first match)'

            logger.info('✓ Multiple independent fallback patterns work correctly')

        finally:
            job.__exit__(None, None, None)


class TestConcurrentLeaderLockAcquisition:
    """Test concurrent leader lock acquisition from multiple nodes."""

    @clean_tables('LeaderLock')
    def test_only_one_node_acquires_leader_lock(self, postgres):
        """Verify only one node can acquire leader lock when multiple try simultaneously.
        """
        lock_config = replace(get_coordination_config(), leader_lock_timeout_sec=0.2)
        tables = schema.get_table_names('sync_')

        acquired_by = []
        lock = threading.Lock()
        barrier = threading.Barrier(5)

        def try_acquire(node_name):
            job = create_job(node_name, postgres, coordination_config=lock_config, wait_on_enter=0)
            job.__enter__()
            try:
                barrier.wait()

                with job.locks.acquire_leader_lock('concurrent-test'):
                    with lock:
                        acquired_by.append(node_name)
                    time.sleep(0.5)
            except LockNotAcquired:
                pass
            finally:
                job.__exit__(None, None, None)

        threads = []
        for i in range(1, 6):
            t = threading.Thread(target=try_acquire, args=(f'node{i}',))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        assert len(acquired_by) == 1, f'Only 1 node should acquire lock, but {len(acquired_by)} did: {acquired_by}'

    @clean_tables('LeaderLock')
    def test_leader_lock_released_after_operation(self, postgres):
        """Verify leader lock is released after operation completes.
        """
        lock_config = get_coordination_config()
        tables = schema.get_table_names('sync_')

        job1 = create_job('node1', postgres, coordination_config=lock_config, wait_on_enter=0)
        job1.__enter__()

        try:
            with job1.locks.acquire_leader_lock('operation-1'):
                pass

            # Verify lock released
            with postgres.connect() as conn:
                result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["LeaderLock"]}'))
                lock_count = result.scalar()

            assert lock_count == 0, 'Lock should be released after context manager exit'

        finally:
            job1.__exit__(None, None, None)

    @clean_tables('LeaderLock')
    def test_second_node_waits_for_lock_release(self, postgres):
        """Verify second node waits when lock is held.
        """
        lock_config = replace(get_coordination_config(), leader_lock_timeout_sec=2)
        tables = schema.get_table_names(lock_config.appname)

        job1 = create_job('node1', postgres, coordination_config=lock_config, wait_on_enter=0)
        job2 = create_job('node2', postgres, coordination_config=lock_config, wait_on_enter=0)
        job1.__enter__()
        job2.__enter__()

        try:
            assert wait_for_running_state(job1, timeout_sec=5)
            assert wait_for_running_state(job2, timeout_sec=5)

            assert wait_for_rebalance(postgres, tables, min_count=1, timeout_sec=10)
            assert wait_for_no_leader_lock(postgres, tables, timeout_sec=5)

            acquired_node2 = False
            lock_acquired = threading.Event()

            def node1_holds_lock():
                with job1.locks.acquire_leader_lock('long-operation'):
                    lock_acquired.set()
                    time.sleep(4)

            t1 = threading.Thread(target=node1_holds_lock)
            t1.start()

            assert lock_acquired.wait(timeout=5), 'node1 should have acquired lock'

            try:
                with job2.locks.acquire_leader_lock('waiting-operation'):
                    acquired_node2 = True
            except LockNotAcquired:
                pass

            t1.join()

            assert not acquired_node2, 'node2 should timeout waiting for lock'

        finally:
            job1.__exit__(None, None, None)
            job2.__exit__(None, None, None)

    @clean_tables('LeaderLock')
    def test_lock_holder_recorded_in_database(self, postgres):
        """Verify lock holder is recorded correctly in database.
        """
        lock_config = get_coordination_config()
        tables = schema.get_table_names(lock_config.appname)

        job = create_job('test-node', postgres, coordination_config=lock_config, wait_on_enter=0)
        job.__enter__()

        try:
            with job.locks.acquire_leader_lock('test-operation'):
                with postgres.connect() as conn:
                    result = conn.execute(text(f"""
                        SELECT node, operation FROM {tables["LeaderLock"]} WHERE singleton = 1
                    """))
                    lock_info = result.first()

                assert lock_info is not None, 'Lock record should exist'
                assert lock_info[0] == 'test-node', 'Node should be recorded as lock holder'
                assert lock_info[1] == 'test-operation', 'Operation should be recorded'

        finally:
            job.__exit__(None, None, None)

    @clean_tables('LeaderLock')
    def test_sequential_acquisitions_after_release(self, postgres):
        """Verify multiple nodes can acquire lock sequentially after release.
        """
        lock_config = get_coordination_config()
        tables = schema.get_table_names(lock_config.appname)

        nodes = [create_job(f'node{i}', postgres, coordination_config=lock_config, wait_on_enter=0) for i in range(1, 4)]

        for node in nodes:
            node.__enter__()

        try:
            for node in nodes:
                acquired = False
                try:
                    with node.locks.acquire_leader_lock(f'operation-{node.node_name}'):
                        acquired = True
                except LockNotAcquired:
                    pass

                assert acquired, f'{node.node_name} should acquire lock after previous release'

        finally:
            for node in nodes:
                node.__exit__(None, None, None)


if __name__ == '__main__':
    pytest.main(args=['-sx', __file__])
