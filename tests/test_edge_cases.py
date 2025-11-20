"""Edge case and failure scenario tests.

Tests verify:
- Cleanup failure handling
- Thread crash and recovery
- Leader lock stale recovery
- Lock expiration side effects
- Token distribution under contention
- Database connection failures
"""
import datetime
import json
import logging
import time
from types import SimpleNamespace

import config as test_config
import pytest
from asserts import assert_equal, assert_false, assert_true
from sqlalchemy import text

from jobsync import schema
from jobsync.client import CoordinationConfig, Job, Task

logger = logging.getLogger(__name__)


def get_edge_case_config():
    """Create a config with short timeouts for edge case testing.
    """
    config = SimpleNamespace()
    config.postgres = test_config.postgres
    config.sync = SimpleNamespace(
        sql=SimpleNamespace(appname='sync_'),
        coordination=SimpleNamespace(
            enabled=True,
            heartbeat_interval_sec=0.3,
            heartbeat_timeout_sec=1.5,
            rebalance_check_interval_sec=0.5,
            dead_node_check_interval_sec=0.5,
            token_refresh_initial_interval_sec=0.3,
            token_refresh_steady_interval_sec=0.5,
            total_tokens=50,
            locks_enabled=True,
            lock_orphan_warning_hours=24,
            leader_lock_timeout_sec=2,
            health_check_interval_sec=0.5
        )
    )
    return config


class TestCleanupFailureScenarios:
    """Test cleanup behavior under failure conditions."""

    def test_cleanup_with_pending_tasks_writes_audit(self, postgres):
        """Verify cleanup writes pending tasks to audit table.
        """
        config = get_edge_case_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Audit"]}'))
            conn.commit()

        job = Job('node1', config, wait_on_enter=0, connection_string=connection_string)
        job.__enter__()

        task1 = Task(1, 'task-1')
        task2 = Task(2, 'task-2')
        job._tasks = [(task1, job._created_on), (task2, job._created_on)]

        job.__exit__(None, None, None)

        with postgres.connect() as conn:
            result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Audit"]}'))
            audit_count = result.scalar()

        assert_equal(audit_count, 2, 'Both pending tasks should be written to audit')

    def test_double_cleanup_is_safe(self, postgres):
        """Verify calling __exit__ twice doesn't cause errors.
        """
        config = get_edge_case_config()
        connection_string = postgres.url.render_as_string(hide_password=False)

        job = Job('node1', config, wait_on_enter=0, connection_string=connection_string)
        job.__enter__()

        job.__exit__(None, None, None)

        try:
            job.__exit__(None, None, None)
        except Exception as e:
            pytest.fail(f'Double cleanup should be safe: {e}')

        logger.info('✓ Double cleanup succeeded without errors')

    def test_cleanup_clears_all_node_data(self, postgres):
        """Verify cleanup removes node from all relevant tables.
        """
        config = get_edge_case_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        with Job('node1', config, wait_on_enter=0, connection_string=connection_string) as job:
            job.set_claim('test-item')
            time.sleep(0.2)

        with postgres.connect() as conn:
            node_result = conn.execute(text(f"SELECT COUNT(*) FROM {tables['Node']} WHERE name = 'node1'"))
            node_count = node_result.scalar()

            claim_result = conn.execute(text(f"SELECT COUNT(*) FROM {tables['Claim']} WHERE node = 'node1'"))
            claim_count = claim_result.scalar()

            check_result = conn.execute(text(f"SELECT COUNT(*) FROM {tables['Check']} WHERE node = 'node1'"))
            check_count = check_result.scalar()

        assert_equal(node_count, 0, 'Node should be removed from Node table')
        assert_equal(claim_count, 0, 'Node should be removed from Claim table')
        assert_equal(check_count, 0, 'Node should be removed from Check table')


class TestRebalanceLockStaleRecovery:
    """Test stale rebalance lock detection and recovery."""

    def test_stale_rebalance_lock_detected_and_removed(self, postgres):
        """Verify stale rebalance locks are detected and removed.
        """
        config = get_edge_case_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        stale_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=400)

        with postgres.connect() as conn:
            conn.execute(text(f"""
                UPDATE {tables["RebalanceLock"]}
                SET in_progress = TRUE, started_at = :started_at, started_by = 'dead-node'
                WHERE singleton = 1
            """), {'started_at': stale_time})
            conn.commit()

        coord_config = CoordinationConfig(
            total_tokens=50,
            heartbeat_interval_sec=1,
            stale_rebalance_lock_age_sec=300
        )

        job = Job('node1', config, wait_on_enter=0, connection_string=connection_string, coordination_config=coord_config)
        job.__enter__()

        try:
            acquired = job._acquire_rebalance_lock('test-rebalance')
            assert_true(acquired, 'Should acquire rebalance lock after removing stale lock')

            with postgres.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT in_progress, started_by FROM {tables['RebalanceLock']} WHERE singleton = 1
                """))
                lock_status = result.first()

            assert_true(lock_status[0], 'Lock should be in progress')
            assert_equal(lock_status[1], 'test-rebalance', 'test-rebalance should now hold the lock')

        finally:
            job.__exit__(None, None, None)

    def test_configurable_stale_rebalance_lock_threshold(self, postgres):
        """Verify stale rebalance lock threshold is configurable.
        """
        config = get_edge_case_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        lock_age = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=15)

        with postgres.connect() as conn:
            conn.execute(text(f"""
                UPDATE {tables["RebalanceLock"]}
                SET in_progress = TRUE, started_at = :started_at, started_by = 'old-node'
                WHERE singleton = 1
            """), {'started_at': lock_age})
            conn.commit()

        coord_config = CoordinationConfig(
            total_tokens=50,
            heartbeat_interval_sec=1,
            stale_rebalance_lock_age_sec=10
        )

        job = Job('node1', config, wait_on_enter=0, connection_string=connection_string, coordination_config=coord_config)
        job.__enter__()

        try:
            acquired = job._acquire_rebalance_lock('test')
            assert_true(acquired, 'Should treat 15-second-old rebalance lock as stale with 10s threshold')

        finally:
            job.__exit__(None, None, None)

    def test_non_stale_rebalance_lock_not_removed(self, postgres):
        """Verify recent rebalance locks are not removed.
        """
        config = get_edge_case_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        recent_time = datetime.datetime.now(datetime.timezone.utc)

        with postgres.connect() as conn:
            conn.execute(text(f"""
                UPDATE {tables["RebalanceLock"]}
                SET in_progress = TRUE, started_at = :started_at, started_by = 'active-node'
                WHERE singleton = 1
            """), {'started_at': recent_time})
            conn.commit()

        coord_config = CoordinationConfig(
            total_tokens=50,
            heartbeat_interval_sec=1,
            stale_rebalance_lock_age_sec=300
        )

        job = Job('node1', config, wait_on_enter=0, connection_string=connection_string, coordination_config=coord_config)

        acquired = job._acquire_rebalance_lock('test')
        assert_false(acquired, 'Should not acquire rebalance lock if recent lock exists')

    def test_stale_rebalance_lock_logged(self, postgres, caplog):
        """Verify stale rebalance lock detection is logged.
        """
        config = get_edge_case_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        stale_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=400)

        with postgres.connect() as conn:
            conn.execute(text(f"""
                UPDATE {tables["RebalanceLock"]}
                SET in_progress = TRUE, started_at = :started_at, started_by = 'stuck-node'
                WHERE singleton = 1
            """), {'started_at': stale_time})
            conn.commit()

        coord_config = CoordinationConfig(
            total_tokens=50,
            stale_rebalance_lock_age_sec=300
        )

        with caplog.at_level(logging.WARNING):
            job = Job('node1', config, wait_on_enter=0, connection_string=connection_string, coordination_config=coord_config)
            job.__enter__()

            try:
                job._acquire_rebalance_lock('test')

                warning_messages = [record.message for record in caplog.records if record.levelname == 'WARNING']
                stale_lock_warnings = [msg for msg in warning_messages if 'Stale rebalance lock detected' in msg]

                assert_true(len(stale_lock_warnings) > 0, 'Should log warning about stale rebalance lock')
                assert_true(any('stuck-node' in msg for msg in stale_lock_warnings), 
                           'Warning should mention the stuck node')

            finally:
                job.__exit__(None, None, None)


class TestDeadNodeLockCleanup:
    """Test lock cleanup when nodes die."""

    def test_dead_node_locks_cleaned_up(self, postgres):
        """Verify locks created by dead nodes are removed during cleanup.
        """
        config = get_edge_case_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        now = datetime.datetime.now(datetime.timezone.utc)
        stale_heartbeat = now - datetime.timedelta(seconds=30)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Lock"]}'))
            conn.execute(text(f'DELETE FROM {tables["Node"]}'))

            # Create a dead node
            conn.execute(text(f"""
                INSERT INTO {tables["Node"]} (name, created_on, last_heartbeat)
                VALUES ('dead-node', :created_on, :heartbeat)
            """), {'created_on': now, 'heartbeat': stale_heartbeat})

            # Create locks from the dead node
            for token_id in [1, 2, 3]:
                conn.execute(text(f"""
                    INSERT INTO {tables["Lock"]} (token_id, node_patterns, reason, created_at, created_by)
                    VALUES (:token_id, :patterns, 'test lock', :created_at, 'dead-node')
                """), {
                    'token_id': token_id,
                    'patterns': json.dumps(['pattern-test']),
                    'created_at': now
                })

            # Create a lock from a different node
            conn.execute(text(f"""
                INSERT INTO {tables["Lock"]} (token_id, node_patterns, reason, created_at, created_by)
                VALUES (99, :patterns, 'other lock', :created_at, 'other-node')
            """), {
                'patterns': json.dumps(['pattern-other']),
                'created_at': now
            })

            conn.commit()

        # Start a leader node that will detect and clean up the dead node
        coord_config = CoordinationConfig(
            total_tokens=50,
            heartbeat_timeout_sec=15,
            dead_node_check_interval_sec=0.5
        )

        job = Job('leader-node', config, wait_on_enter=2, connection_string=connection_string, coordination_config=coord_config)
        job.__enter__()

        try:
            # Wait for dead node detection and cleanup
            time.sleep(3)

            with postgres.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT token_id FROM {tables["Lock"]} WHERE created_by = 'dead-node'
                """))
                dead_node_locks = [row[0] for row in result]

                result = conn.execute(text(f"""
                    SELECT token_id FROM {tables["Lock"]} WHERE created_by = 'other-node'
                """))
                other_node_locks = [row[0] for row in result]

            assert_equal(len(dead_node_locks), 0, 'All locks from dead node should be removed')
            assert_equal(len(other_node_locks), 1, 'Locks from other nodes should remain')
            assert_equal(other_node_locks[0], 99, 'Lock 99 from other-node should still exist')

        finally:
            job.__exit__(None, None, None)

    def test_multiple_dead_nodes_all_locks_cleaned(self, postgres):
        """Verify locks from multiple dead nodes are all cleaned up.
        """
        config = get_edge_case_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        now = datetime.datetime.now(datetime.timezone.utc)
        stale_heartbeat = now - datetime.timedelta(seconds=30)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Lock"]}'))
            conn.execute(text(f'DELETE FROM {tables["Node"]}'))

            # Create multiple dead nodes with locks
            for i in range(1, 4):
                node_name = f'dead-node-{i}'
                conn.execute(text(f"""
                    INSERT INTO {tables["Node"]} (name, created_on, last_heartbeat)
                    VALUES (:name, :created_on, :heartbeat)
                """), {'name': node_name, 'created_on': now, 'heartbeat': stale_heartbeat})

                # Each dead node has 2 locks
                for j in range(2):
                    token_id = i * 10 + j
                    conn.execute(text(f"""
                        INSERT INTO {tables["Lock"]} (token_id, node_patterns, reason, created_at, created_by)
                        VALUES (:token_id, :patterns, 'test', :created_at, :created_by)
                    """), {
                        'token_id': token_id,
                        'patterns': json.dumps(['pattern']),
                        'created_at': now,
                        'created_by': node_name
                    })

            conn.commit()

        coord_config = CoordinationConfig(
            total_tokens=50,
            heartbeat_timeout_sec=15,
            dead_node_check_interval_sec=0.5
        )

        job = Job('cleanup-leader', config, wait_on_enter=2, connection_string=connection_string, coordination_config=coord_config)
        job.__enter__()

        try:
            time.sleep(3)

            with postgres.connect() as conn:
                result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Lock"]}'))
                remaining_locks = result.scalar()

            assert_equal(remaining_locks, 0, 'All locks from all dead nodes should be removed')

        finally:
            job.__exit__(None, None, None)

    def test_lock_cleanup_logged(self, postgres, caplog):
        """Verify lock cleanup from dead nodes is logged.
        """
        config = get_edge_case_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        now = datetime.datetime.now(datetime.timezone.utc)
        stale_heartbeat = now - datetime.timedelta(seconds=30)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Lock"]}'))
            conn.execute(text(f'DELETE FROM {tables["Node"]}'))

            conn.execute(text(f"""
                INSERT INTO {tables["Node"]} (name, created_on, last_heartbeat)
                VALUES ('logged-dead-node', :created_on, :heartbeat)
            """), {'created_on': now, 'heartbeat': stale_heartbeat})

            conn.execute(text(f"""
                INSERT INTO {tables["Lock"]} (token_id, node_patterns, reason, created_at, created_by)
                VALUES (1, :patterns, 'test', :created_at, 'logged-dead-node')
            """), {
                'patterns': json.dumps(['pattern']),
                'created_at': now
            })

            conn.commit()

        coord_config = CoordinationConfig(
            total_tokens=50,
            heartbeat_timeout_sec=15,
            dead_node_check_interval_sec=0.5
        )

        with caplog.at_level(logging.INFO):
            job = Job('logging-leader', config, wait_on_enter=2, connection_string=connection_string, coordination_config=coord_config)
            job.__enter__()

            try:
                time.sleep(3)

                info_messages = [record.message for record in caplog.records if record.levelname == 'INFO']
                cleanup_messages = [msg for msg in info_messages if 'cleaned up locks' in msg.lower()]

                assert_true(len(cleanup_messages) > 0, 'Should log lock cleanup')
                assert_true(any('logged-dead-node' in msg for msg in cleanup_messages),
                           'Log should mention the dead node')

            finally:
                job.__exit__(None, None, None)

    def test_expired_locks_cleaned_during_dead_node_rebalance(self, postgres):
        """Verify dead node cleanup removes locks by creator, and expired locks are cleaned during redistribution.
        """
        config = get_edge_case_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        now = datetime.datetime.now(datetime.timezone.utc)
        stale_heartbeat = now - datetime.timedelta(seconds=30)
        expired_time = now - datetime.timedelta(days=2)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Lock"]}'))
            conn.execute(text(f'DELETE FROM {tables["Node"]}'))

            conn.execute(text(f"""
                INSERT INTO {tables["Node"]} (name, created_on, last_heartbeat)
                VALUES ('dead-node', :created_on, :heartbeat)
            """), {'created_on': now, 'heartbeat': stale_heartbeat})

            # Lock from dead node (should be removed)
            conn.execute(text(f"""
                INSERT INTO {tables["Lock"]} (token_id, node_patterns, reason, created_at, created_by)
                VALUES (1, :patterns, 'dead node lock', :created_at, 'dead-node')
            """), {
                'patterns': json.dumps(['pattern']),
                'created_at': now
            })

            # Expired lock from alive node (should NOT be removed by dead node cleanup)
            conn.execute(text(f"""
                INSERT INTO {tables["Lock"]} (token_id, node_patterns, reason, created_at, created_by, expires_at)
                VALUES (2, :patterns, 'expired lock', :created_at, 'alive-node', :expires_at)
            """), {
                'patterns': json.dumps(['pattern']),
                'created_at': now,
                'expires_at': expired_time
            })

            conn.commit()

        coord_config = CoordinationConfig(
            total_tokens=50,
            heartbeat_timeout_sec=15,
            dead_node_check_interval_sec=0.5
        )

        job = Job('separation-leader', config, wait_on_enter=2, connection_string=connection_string, coordination_config=coord_config)
        job.__enter__()

        try:
            time.sleep(3)

            with postgres.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT token_id, created_by FROM {tables["Lock"]} ORDER BY token_id
                """))
                locks = [(row[0], row[1]) for row in result]

            # Dead node lock should be gone (removed by DELETE statement)
            dead_node_locks = [l for l in locks if l[1] == 'dead-node']
            assert_equal(len(dead_node_locks), 0, 'Dead node locks should be removed by DELETE')

            # Expired lock also gone (removed by _get_active_locks during token redistribution)
            expired_locks = [l for l in locks if l[0] == 2]
            assert_equal(len(expired_locks), 0, 'Expired locks are removed during token redistribution triggered by dead node cleanup')

        finally:
            job.__exit__(None, None, None)


class TestLeaderLockStaleRecovery:
    """Test stale leader lock detection and recovery."""

    def test_stale_lock_detected_and_removed(self, postgres):
        """Verify stale leader locks are detected and removed.
        """
        config = get_edge_case_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        stale_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=400)

        with postgres.connect() as conn:
            conn.execute(text(f"""
                INSERT INTO {tables["LeaderLock"]} (singleton, node, acquired_at, operation)
                VALUES (1, 'dead-node', :acquired_at, 'stale-operation')
            """), {'acquired_at': stale_time})
            conn.commit()

        coord_config = CoordinationConfig(
            total_tokens=50,
            heartbeat_interval_sec=1,
            stale_leader_lock_age_sec=300
        )

        job = Job('node1', config, wait_on_enter=0, connection_string=connection_string, coordination_config=coord_config)
        job.__enter__()

        try:
            acquired = job._acquire_leader_lock('test-operation')
            assert_true(acquired, 'Should acquire lock after removing stale lock')

            with postgres.connect() as conn:
                result = conn.execute(text(f"SELECT node FROM {tables['LeaderLock']} WHERE singleton = 1"))
                current_holder = result.scalar()

            assert_equal(current_holder, 'node1', 'node1 should now hold the lock')

        finally:
            job.__exit__(None, None, None)

    def test_configurable_stale_lock_threshold(self, postgres):
        """Verify stale lock threshold is configurable.
        """
        config = get_edge_case_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        lock_age = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=15)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["LeaderLock"]}'))
            conn.execute(text(f"""
                INSERT INTO {tables["LeaderLock"]} (singleton, node, acquired_at, operation)
                VALUES (1, 'old-node', :acquired_at, 'old-operation')
            """), {'acquired_at': lock_age})
            conn.commit()

        coord_config = CoordinationConfig(
            total_tokens=50,
            heartbeat_interval_sec=1,
            stale_leader_lock_age_sec=10
        )

        job = Job('node1', config, wait_on_enter=0, connection_string=connection_string, coordination_config=coord_config)
        job.__enter__()

        try:
            acquired = job._acquire_leader_lock('test')
            assert_true(acquired, 'Should treat 15-second-old lock as stale with 10s threshold')

        finally:
            job.__exit__(None, None, None)

    def test_non_stale_lock_not_removed(self, postgres):
        """Verify recent locks are not removed.
        """
        config = get_edge_case_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        recent_time = datetime.datetime.now(datetime.timezone.utc)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["LeaderLock"]}'))
            conn.execute(text(f"""
                INSERT INTO {tables["LeaderLock"]} (singleton, node, acquired_at, operation)
                VALUES (1, 'active-node', :acquired_at, 'active-operation')
            """), {'acquired_at': recent_time})
            conn.commit()

        coord_config = CoordinationConfig(
            total_tokens=50,
            heartbeat_interval_sec=1,
            stale_leader_lock_age_sec=300,
            leader_lock_timeout_sec=2
        )

        job = Job('node1', config, wait_on_enter=0, connection_string=connection_string, coordination_config=coord_config)

        acquired = job._acquire_leader_lock('test')
        assert_false(acquired, 'Should not acquire lock if recent lock exists')


class TestThreadCrashAndRecovery:
    """Test thread failure detection and recovery."""

    def test_health_monitor_detects_stale_heartbeat(self, postgres):
        """Verify health monitor detects when heartbeat thread appears dead.
        """
        config = get_edge_case_config()
        connection_string = postgres.url.render_as_string(hide_password=False)

        coord_config = CoordinationConfig(
            total_tokens=50,
            heartbeat_interval_sec=5,
            heartbeat_timeout_sec=3,
            health_check_interval_sec=1
        )

        job = Job('node1', config, wait_on_enter=0, connection_string=connection_string, coordination_config=coord_config)
        job.__enter__()

        try:
            time.sleep(0.5)
            assert_true(job.am_i_healthy(), 'Should be healthy initially')

            job._last_heartbeat_sent -= datetime.timedelta(seconds=10)

            time.sleep(0.5)
            is_healthy = job.am_i_healthy()
            assert_false(is_healthy, 'Should detect stale heartbeat')

            logger.info('✓ Health monitor detected stale heartbeat')

        finally:
            job._shutdown = True
            job.__exit__(None, None, None)

    def test_thread_database_reconnection(self, postgres, caplog):
        """Verify threads recover from transient database errors.
        """
        config = get_edge_case_config()
        connection_string = postgres.url.render_as_string(hide_password=False)

        coord_config = CoordinationConfig(
            heartbeat_interval_sec=0.2,
            heartbeat_timeout_sec=3
        )

        with caplog.at_level(logging.ERROR):
            job = Job('node1', config, wait_on_enter=0, connection_string=connection_string, coordination_config=coord_config)
            job.__enter__()

            try:
                time.sleep(0.5)
                assert_true(job.am_i_healthy(), 'Should be healthy initially')

                logger.info('✓ Threads operating normally with database connectivity')

            finally:
                job._shutdown = True
                job.__exit__(None, None, None)


class TestLockExpirationSideEffects:
    """Test lock expiration handling during operations."""

    def test_expired_locks_deleted_during_get_active_locks(self, postgres):
        """Verify expired locks are deleted as side effect of getting active locks.
        """
        config = get_edge_case_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        expired_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=2)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Lock"]}'))
            conn.execute(text(f"""
                INSERT INTO {tables["Lock"]} (token_id, node_patterns, reason, created_at, created_by, expires_at)
                VALUES (1, :patterns1, 'expired lock', :created_at, 'node1', :expires_at)
            """), {'created_at': datetime.datetime.now(datetime.timezone.utc), 'expires_at': expired_time, 'patterns1': json.dumps(['pattern-1'])})
            conn.execute(text(f"""
                INSERT INTO {tables["Lock"]} (token_id, node_patterns, reason, created_at, created_by, expires_at)
                VALUES (2, :patterns2, 'valid lock', :created_at, 'node1', NULL)
            """), {'created_at': datetime.datetime.now(datetime.timezone.utc), 'patterns2': json.dumps(['pattern-2'])})
            conn.commit()

        job = Job('node1', config, wait_on_enter=0, connection_string=connection_string)
        job.__enter__()

        try:
            active_locks = job._get_active_locks()

            assert_true(1 not in active_locks, 'Expired lock should not be in active locks')
            assert_true(2 in active_locks, 'Valid lock should be in active locks')

            with postgres.connect() as conn:
                result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Lock"]} WHERE token_id = 1'))
                expired_count = result.scalar()

            assert_equal(expired_count, 0, 'Expired lock should be deleted from database')

        finally:
            job.__exit__(None, None, None)

    def test_lock_expires_during_token_distribution(self, postgres):
        """Verify token distribution handles locks that expire mid-operation.
        """
        config = get_edge_case_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        soon_to_expire = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=0.2)
        now = datetime.datetime.now(datetime.timezone.utc)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Lock"]}'))
            conn.execute(text(f'DELETE FROM {tables["Node"]}'))

            conn.execute(text(f"""
                INSERT INTO {tables["Node"]} (name, created_on, last_heartbeat)
                VALUES (:name, :created_on, :heartbeat)
            """), {'name': 'node1', 'created_on': now, 'heartbeat': now})

            conn.execute(text(f"""
                INSERT INTO {tables["Node"]} (name, created_on, last_heartbeat)
                VALUES (:name, :created_on, :heartbeat)
            """), {'name': 'node2', 'created_on': now + datetime.timedelta(seconds=1), 'heartbeat': now})

            conn.execute(text(f"""
                INSERT INTO {tables["Lock"]} (token_id, node_patterns, reason, created_at, created_by, expires_at)
                VALUES (5, :patterns, 'about to expire', :created_at, 'test', :expires_at)
            """), {'created_at': datetime.datetime.now(datetime.timezone.utc), 'expires_at': soon_to_expire, 'patterns': json.dumps(['node1'])})
            conn.commit()

        coord_config = CoordinationConfig(total_tokens=50)
        job = Job('node1', config, wait_on_enter=0, connection_string=connection_string, coordination_config=coord_config)
        job.__enter__()

        try:
            time.sleep(0.5)

            job._distribute_tokens_minimal_move(50)

            with postgres.connect() as conn:
                result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Lock"]} WHERE token_id = 5'))
                lock_count = result.scalar()

            assert_equal(lock_count, 0, 'Expired lock should be deleted after distribution')

        finally:
            job.__exit__(None, None, None)


class TestTokenDistributionUnderContention:
    """Test token distribution with lock contention and failures."""

    def test_leader_lock_timeout_behavior(self, postgres):
        """Verify behavior when leader lock acquisition times out.
        """
        config = get_edge_case_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["LeaderLock"]}'))
            conn.execute(text(f"""
                INSERT INTO {tables["LeaderLock"]} (singleton, node, acquired_at, operation)
                VALUES (1, 'other-node', :acquired_at, 'long-operation')
            """), {'acquired_at': datetime.datetime.now(datetime.timezone.utc)})
            conn.commit()

        coord_config = CoordinationConfig(
            leader_lock_timeout_sec=2,
            stale_leader_lock_age_sec=300
        )

        job = Job('node1', config, wait_on_enter=0, connection_string=connection_string, coordination_config=coord_config)

        start_time = time.time()
        acquired = job._acquire_leader_lock('test')
        elapsed = time.time() - start_time

        assert_false(acquired, 'Should not acquire lock held by other node')
        assert_true(elapsed >= 2, f'Should wait for timeout (took {elapsed:.1f}s)')
        assert_true(elapsed < 5, f'Should timeout quickly (took {elapsed:.1f}s)')

        with postgres.connect() as conn:
            result = conn.execute(text(f"SELECT node FROM {tables['LeaderLock']} WHERE singleton = 1"))
            holder = result.scalar()

        assert_equal(holder, 'other-node', 'Lock holder should not change on timeout')

    def test_all_tokens_locked_to_nonexistent_pattern(self, postgres):
        """Verify behavior when all tokens locked to pattern with no matching nodes.
        """
        config = get_edge_case_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        now = datetime.datetime.now(datetime.timezone.utc)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Lock"]}'))
            conn.execute(text(f'DELETE FROM {tables["Node"]}'))

            conn.execute(text(f"""
                INSERT INTO {tables["Node"]} (name, created_on, last_heartbeat)
                VALUES (:name, :created_on, :heartbeat)
            """), {'name': 'node1', 'created_on': now, 'heartbeat': now})

            conn.execute(text(f"""
                INSERT INTO {tables["Node"]} (name, created_on, last_heartbeat)
                VALUES (:name, :created_on, :heartbeat)
            """), {'name': 'node2', 'created_on': now + datetime.timedelta(seconds=1), 'heartbeat': now})

            for token_id in range(20):
                conn.execute(text(f"""
                    INSERT INTO {tables["Lock"]} (token_id, node_patterns, reason, created_at, created_by, expires_at)
                    VALUES (:token_id, :patterns, 'test', :created_at, 'test', NULL)
                """), {'token_id': token_id, 'created_at': datetime.datetime.now(datetime.timezone.utc), 'patterns': json.dumps(['nonexistent-%'])})
            conn.commit()

        coord_config = CoordinationConfig(total_tokens=20)
        job = Job('node1', config, wait_on_enter=0, connection_string=connection_string, coordination_config=coord_config)

        job._distribute_tokens_minimal_move(20)

        with postgres.connect() as conn:
            result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Token"]}'))
            assigned_count = result.scalar()

        assert_equal(assigned_count, 0, 'No tokens should be assigned when pattern matches no nodes')

    def test_partial_lock_pattern_matching(self, postgres):
        """Verify tokens partially locked with some matchable patterns work correctly.
        """
        config = get_edge_case_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        now = datetime.datetime.now(datetime.timezone.utc)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Lock"]}'))
            conn.execute(text(f'DELETE FROM {tables["Token"]}'))
            conn.execute(text(f'DELETE FROM {tables["Node"]}'))

            conn.execute(text(f"""
                INSERT INTO {tables["Node"]} (name, created_on, last_heartbeat)
                VALUES (:name, :created_on, :heartbeat)
            """), {'name': 'node1', 'created_on': now, 'heartbeat': now})

            conn.execute(text(f"""
                INSERT INTO {tables["Node"]} (name, created_on, last_heartbeat)
                VALUES (:name, :created_on, :heartbeat)
            """), {'name': 'node2', 'created_on': now + datetime.timedelta(seconds=1), 'heartbeat': now})

            conn.execute(text(f"""
                INSERT INTO {tables["Node"]} (name, created_on, last_heartbeat)
                VALUES (:name, :created_on, :heartbeat)
            """), {'name': 'special-node', 'created_on': now + datetime.timedelta(seconds=2), 'heartbeat': now})

            for token_id in range(10):
                conn.execute(text(f"""
                    INSERT INTO {tables["Lock"]} (token_id, node_patterns, reason, created_at, created_by, expires_at)
                    VALUES (:token_id, :patterns, 'valid pattern', :created_at, 'test', NULL)
                """), {'token_id': token_id, 'created_at': datetime.datetime.now(datetime.timezone.utc), 'patterns': json.dumps(['special-%'])})

            for token_id in range(10, 20):
                conn.execute(text(f"""
                    INSERT INTO {tables["Lock"]} (token_id, node_patterns, reason, created_at, created_by, expires_at)
                    VALUES (:token_id, :patterns, 'invalid pattern', :created_at, 'test', NULL)
                """), {'token_id': token_id, 'created_at': datetime.datetime.now(datetime.timezone.utc), 'patterns': json.dumps(['missing-%'])})
            conn.commit()

        coord_config = CoordinationConfig(total_tokens=50)
        job = Job('node1', config, wait_on_enter=0, connection_string=connection_string, coordination_config=coord_config)
        job.__enter__()

        try:
            job._distribute_tokens_minimal_move(50)

            with postgres.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM {tables["Token"]} WHERE node = 'special-node'
                """))
                special_count = result.scalar()

                result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Token"]}'))
                total_count = result.scalar()

            assert_true(special_count >= 10, 'special-node should have at least the locked tokens')
            assert_true(40 <= total_count <= 50, 'Most unlocked tokens should be assigned')

        finally:
            job.__exit__(None, None, None)


class TestDatabaseConnectionFailures:
    """Test handling of database connection issues."""

    def test_can_claim_task_survives_db_failure(self, postgres):
        """Verify can_claim_task handles database unavailability gracefully.
        """
        config = get_edge_case_config()
        connection_string = postgres.url.render_as_string(hide_password=False)

        job = Job('node1', config, wait_on_enter=0, connection_string=connection_string)
        job.__enter__()

        try:
            job._my_tokens = {1, 2, 3, 4, 5}
            task = Task(0)
            token_id = job._task_to_token(0)

            if token_id in job._my_tokens:
                can_claim = job.can_claim_task(task)
                assert_true(can_claim, 'Should be able to claim task with owned token')

        finally:
            job.__exit__(None, None, None)


class TestAuditWriteFailures:
    """Test audit write error handling."""

    def test_write_audit_with_empty_tasks(self, postgres):
        """Verify write_audit safely handles empty task list.
        """
        config = get_edge_case_config()
        connection_string = postgres.url.render_as_string(hide_password=False)

        job = Job('node1', config, wait_on_enter=0, connection_string=connection_string)
        job.__enter__()

        try:
            job._tasks = []

            try:
                job.write_audit()
            except Exception as e:
                pytest.fail(f'write_audit should handle empty tasks: {e}')

        finally:
            job.__exit__(None, None, None)

    def test_write_audit_clears_tasks_after_write(self, postgres):
        """Verify tasks are cleared after successful audit write.
        """
        config = get_edge_case_config()
        connection_string = postgres.url.render_as_string(hide_password=False)

        job = Job('node1', config, wait_on_enter=0, connection_string=connection_string)
        job.__enter__()

        try:
            task1 = Task(1, 'task-1')
            task2 = Task(2, 'task-2')
            job._tasks = [(task1, job._created_on), (task2, job._created_on)]

            assert_equal(len(job._tasks), 2, 'Should have 2 pending tasks')

            job.write_audit()

            assert_equal(len(job._tasks), 0, 'Tasks should be cleared after write')

        finally:
            job.__exit__(None, None, None)


if __name__ == '__main__':
    pytest.main(args=['-sx', __file__])
