"""Timezone handling tests for coordination system.

Tests verify:
- Heartbeat timeout detection across timezones
- Lock expiration with timezone-aware timestamps
- Leader lock stale detection across timezones
- Token distribution timestamp comparisons
- Leader election agreement across timezones
- Rebalance timing with mixed timezone nodes
"""
import datetime
import json
import logging
import time
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import config as test_config
import pytest
from asserts import assert_equal, assert_true
from sqlalchemy import text

from jobsync import schema
from jobsync.client import CoordinationConfig, Job, LockNotAcquired, Task

logger = logging.getLogger(__name__)


def get_timezone_test_config():
    """Create a config with short timeouts for timezone testing.
    """
    config = SimpleNamespace()
    config.postgres = test_config.postgres
    config.sync = SimpleNamespace(
        sql=SimpleNamespace(appname='sync_'),
        coordination=SimpleNamespace(
            enabled=True,
            heartbeat_interval_sec=2,
            heartbeat_timeout_sec=6,
            rebalance_check_interval_sec=5,
            dead_node_check_interval_sec=3,
            token_refresh_initial_interval_sec=2,
            token_refresh_steady_interval_sec=5,
            total_tokens=100,
            locks_enabled=True,
            lock_orphan_warning_hours=24,
            leader_lock_timeout_sec=10,
            health_check_interval_sec=5
        )
    )
    return config


class TestHeartbeatTimezoneHandling:
    """Test heartbeat timeout detection across timezones."""

    def test_heartbeat_timeout_utc_vs_local(self, postgres):
        """Verify heartbeat timeout detection works with UTC and local times.
        """
        config = get_timezone_test_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        # Insert node with UTC timestamp
        utc_now = datetime.datetime.now(datetime.timezone.utc)
        old_heartbeat = utc_now - datetime.timedelta(seconds=10)

        with postgres.connect() as conn:
            conn.execute(text(f"""
                INSERT INTO {tables["Node"]} (name, created_on, last_heartbeat)
                VALUES (:name, :created_on, :heartbeat)
            """), {
                'name': 'node-utc',
                'created_on': utc_now,
                'heartbeat': old_heartbeat
            })
            conn.commit()

        job = Job('test', config, wait_on_enter=0, connection_string=connection_string)

        try:
            dead_nodes = job.get_dead_nodes()
            assert_true('node-utc' in dead_nodes,
                       'Node with old UTC heartbeat should be detected as dead')
        finally:
            with postgres.connect() as conn:
                conn.execute(text(f'DELETE FROM {tables["Node"]} WHERE name = :name'),
                           {'name': 'node-utc'})
                conn.commit()

    def test_heartbeat_timeout_different_timezones(self, postgres):
        """Verify heartbeat timeout works when nodes use different timezones.
        """
        config = get_timezone_test_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        # Simulate nodes in different timezones
        timezones = ['UTC', 'America/New_York', 'Asia/Tokyo', 'Europe/London']
        current_time = datetime.datetime.now(datetime.timezone.utc)

        for tz in timezones:
            tz_time = current_time.astimezone(ZoneInfo(tz))
            with postgres.connect() as conn:
                conn.execute(text(f"""
                    INSERT INTO {tables["Node"]} (name, created_on, last_heartbeat)
                    VALUES (:name, :created_on, :heartbeat)
                """), {
                    'name': f'node-{tz.replace("/", "-")}',
                    'created_on': tz_time,
                    'heartbeat': tz_time
                })
                conn.commit()

        job = Job('test', config, wait_on_enter=0, connection_string=connection_string)

        try:
            time.sleep(1)
            active_nodes = job.get_active_nodes()
            active_names = [n['name'] for n in active_nodes]

            # All nodes should be active (heartbeat within timeout)
            for tz in timezones:
                node_name = f'node-{tz.replace("/", "-")}'
                assert_true(node_name in active_names,
                           f'{node_name} should be active')

        finally:
            with postgres.connect() as conn:
                for tz in timezones:
                    conn.execute(text(f'DELETE FROM {tables["Node"]} WHERE name = :name'),
                               {'name': f'node-{tz.replace("/", "-")}'})
                conn.commit()

    def test_heartbeat_comparison_after_dst_change(self, postgres):
        """Verify heartbeat comparisons work correctly around DST transitions.
        """
        config = get_timezone_test_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        # Simulate a node registered just before DST transition
        # Use a fixed date known to have DST transition
        dst_transition = datetime.datetime(2024, 3, 10, 1, 0, 0, tzinfo=ZoneInfo('America/New_York'))
        before_dst = dst_transition - datetime.timedelta(hours=2)

        with postgres.connect() as conn:
            conn.execute(text(f"""
                INSERT INTO {tables["Node"]} (name, created_on, last_heartbeat)
                VALUES (:name, :created_on, :heartbeat)
            """), {
                'name': 'node-dst-test',
                'created_on': before_dst,
                'heartbeat': before_dst
            })
            conn.commit()

        job = Job('test', config, wait_on_enter=0, connection_string=connection_string)

        try:
            # Node should be detected as dead (old heartbeat)
            dead_nodes = job.get_dead_nodes()
            assert_true('node-dst-test' in dead_nodes,
                       'Node with old heartbeat should be dead regardless of DST')

        finally:
            with postgres.connect() as conn:
                conn.execute(text(f'DELETE FROM {tables["Node"]} WHERE name = :name'),
                           {'name': 'node-dst-test'})
                conn.commit()


class TestLockExpirationTimezones:
    """Test lock expiration handling across timezones."""

    def test_lock_expiration_utc_vs_local(self, postgres):
        """Verify lock expiration works with different timezone timestamps.
        """
        config = get_timezone_test_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        # Create locks with different timezone timestamps
        utc_now = datetime.datetime.now(datetime.timezone.utc)
        ny_now = datetime.datetime.now(ZoneInfo('America/New_York'))

        expired_utc = utc_now - datetime.timedelta(days=2)
        expired_ny = ny_now - datetime.timedelta(days=2)
        valid_utc = utc_now + datetime.timedelta(days=1)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Lock"]}'))

            # Expired lock with UTC timestamp
            conn.execute(text(f"""
                INSERT INTO {tables["Lock"]} (token_id, node_patterns, reason, created_at, created_by, expires_at)
                VALUES (:token_id, :patterns, :reason, :created_at, :created_by, :expires_at)
            """), {
                'token_id': 1,
                'patterns': json.dumps(['pattern-utc']),
                'reason': 'expired UTC',
                'created_at': utc_now,
                'created_by': 'test',
                'expires_at': expired_utc
            })

            # Expired lock with NY timezone
            conn.execute(text(f"""
                INSERT INTO {tables["Lock"]} (token_id, node_patterns, reason, created_at, created_by, expires_at)
                VALUES (:token_id, :patterns, :reason, :created_at, :created_by, :expires_at)
            """), {
                'token_id': 2,
                'patterns': json.dumps(['pattern-ny']),
                'reason': 'expired NY',
                'created_at': ny_now,
                'created_by': 'test',
                'expires_at': expired_ny
            })

            # Valid lock
            conn.execute(text(f"""
                INSERT INTO {tables["Lock"]} (token_id, node_patterns, reason, created_at, created_by, expires_at)
                VALUES (:token_id, :patterns, :reason, :created_at, :created_by, :expires_at)
            """), {
                'token_id': 3,
                'patterns': json.dumps(['pattern-valid']),
                'reason': 'valid lock',
                'created_at': utc_now,
                'created_by': 'test',
                'expires_at': valid_utc
            })
            conn.commit()

        job = Job('test', config, wait_on_enter=0, connection_string=connection_string)

        active_locks = job.locks.get_active_locks()

        # Only valid lock should remain
        assert_true(1 not in active_locks, 'Expired UTC lock should be removed')
        assert_true(2 not in active_locks, 'Expired NY lock should be removed')
        assert_true(3 in active_locks, 'Valid lock should remain')

    def test_lock_expiration_boundary_conditions(self, postgres):
        """Verify lock expiration at exact timezone boundary times.
        """
        config = get_timezone_test_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        # Create lock that expires at midnight in different timezones
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        utc_midnight = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        now_ny = datetime.datetime.now(ZoneInfo('America/New_York'))
        ny_midnight = now_ny.replace(hour=0, minute=0, second=0, microsecond=0)
        now_tokyo = datetime.datetime.now(ZoneInfo('Asia/Tokyo'))
        tokyo_midnight = now_tokyo.replace(hour=0, minute=0, second=0, microsecond=0)

        # Lock expires at UTC midnight (in the past)
        expires_at_utc_midnight = utc_midnight - datetime.timedelta(days=1)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Lock"]}'))
            conn.execute(text(f"""
                INSERT INTO {tables["Lock"]} (token_id, node_patterns, reason, created_at, created_by, expires_at)
                VALUES (:token_id, :patterns, :reason, :created_at, :created_by, :expires_at)
            """), {
                'token_id': 100,
                'patterns': json.dumps(['midnight-test']),
                'reason': 'expires at midnight',
                'created_at': utc_midnight - datetime.timedelta(days=2),
                'created_by': 'test',
                'expires_at': expires_at_utc_midnight
            })
            conn.commit()

        job = Job('test', config, wait_on_enter=0,
                 connection_string=connection_string)

        active_locks = job.locks.get_active_locks()
        assert_true(100 not in active_locks,
                   'Lock expired at midnight should be removed')


class TestLeaderLockTimezones:
    """Test leader lock timing across timezones."""

    def test_stale_leader_lock_detection_different_timezones(self, postgres):
        """Verify stale lock detection works with timezone-aware timestamps.
        """
        config = get_timezone_test_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        # Create stale lock with NY timezone timestamp
        ny_time = datetime.datetime.now(ZoneInfo('America/New_York'))
        stale_time = ny_time - datetime.timedelta(seconds=400)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["LeaderLock"]}'))
            conn.execute(text(f"""
                INSERT INTO {tables["LeaderLock"]} (singleton, node, acquired_at, operation)
                VALUES (1, :node, :acquired_at, :operation)
            """), {
                'node': 'stale-node',
                'acquired_at': stale_time,
                'operation': 'stale-operation'
            })
            conn.commit()

        coord_config = CoordinationConfig(
            total_tokens=100,
            heartbeat_interval_sec=1,
            stale_leader_lock_age_sec=300
        )

        job = Job('test', config, wait_on_enter=0,
                 connection_string=connection_string, coordination_config=coord_config)

        # Should be able to acquire lock (stale lock removed)
        try:
            with job.locks.acquire_leader_lock('test-operation'):
                acquired = True
        except LockNotAcquired:
            acquired = False
        assert_true(acquired, 'Should acquire lock after removing stale NY timezone lock')

    def test_leader_lock_acquired_time_across_timezones(self, postgres):
        """Verify leader lock timing works when nodes are in different timezones.
        """
        config = get_timezone_test_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["LeaderLock"]}'))
            conn.commit()

        # Node1 in Tokyo timezone acquires lock
        tokyo_time = datetime.datetime.now(ZoneInfo('Asia/Tokyo'))
        job1 = Job('node-tokyo', config, wait_on_enter=0, connection_string=connection_string)

        try:
            with job1.locks.acquire_leader_lock('tokyo-operation'):
                acquired_tokyo = True
                
                # Node2 in NY timezone tries to acquire immediately
                job2 = Job('node-ny', config, wait_on_enter=0, connection_string=connection_string)
                
                try:
                    with job2.locks.acquire_leader_lock('ny-operation'):
                        acquired_ny = True
                except LockNotAcquired:
                    acquired_ny = False
                assert_true(not acquired_ny, 'NY node should not acquire lock held by Tokyo node')
        except LockNotAcquired:
            acquired_tokyo = False
            
        assert_true(acquired_tokyo, 'Tokyo node should acquire lock')

        # Now NY node should be able to acquire
        try:
            with job2.locks.acquire_leader_lock('ny-operation-after'):
                acquired_ny_after = True
        except LockNotAcquired:
            acquired_ny_after = False
        assert_true(acquired_ny_after, 'NY node should acquire after Tokyo released')


class TestLeaderElectionTimezones:
    """Test leader election with timezone-aware timestamps."""

    def test_leader_election_with_mixed_timezones(self, postgres):
        """Verify leader election works correctly when nodes registered in different timezones.
        """
        config = get_timezone_test_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        # Register nodes in different timezones at different times
        base_utc = datetime.datetime.now(datetime.timezone.utc)

        nodes = [
            ('node-tokyo', base_utc.astimezone(ZoneInfo('Asia/Tokyo')) - datetime.timedelta(seconds=30)),
            ('node-london', base_utc.astimezone(ZoneInfo('Europe/London')) - datetime.timedelta(seconds=20)),
            ('node-ny', base_utc.astimezone(ZoneInfo('America/New_York')) - datetime.timedelta(seconds=10)),
        ]

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Node"]}'))
            for name, created_on in nodes:
                conn.execute(text(f"""
                    INSERT INTO {tables["Node"]} (name, created_on, last_heartbeat)
                    VALUES (:name, :created_on, :heartbeat)
                """), {
                    'name': name,
                    'created_on': created_on,
                    'heartbeat': base_utc
                })
            conn.commit()

        job = Job('test', config, wait_on_enter=0, connection_string=connection_string)

        leader = job.cluster.elect_leader()
        assert_equal(leader, 'node-tokyo',
                    'Oldest node (Tokyo) should be elected regardless of timezone')

    def test_leader_election_timestamp_comparison_across_dst(self, postgres):
        """Verify leader election timestamp comparison handles DST correctly.
        """
        config = get_timezone_test_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        # Create timestamps spanning DST transition
        before_dst = datetime.datetime(2024, 3, 9, 23, 0, 0, tzinfo=ZoneInfo('America/New_York'))
        after_dst = datetime.datetime(2024, 3, 10, 3, 0, 0, tzinfo=ZoneInfo('America/New_York'))

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Node"]}'))

            conn.execute(text(f"""
                INSERT INTO {tables["Node"]} (name, created_on, last_heartbeat)
                VALUES (:name, :created_on, :heartbeat)
            """), {
                'name': 'node-before-dst',
                'created_on': before_dst,
                'heartbeat': datetime.datetime.now(datetime.timezone.utc)
            })

            conn.execute(text(f"""
                INSERT INTO {tables["Node"]} (name, created_on, last_heartbeat)
                VALUES (:name, :created_on, :heartbeat)
            """), {
                'name': 'node-after-dst',
                'created_on': after_dst,
                'heartbeat': datetime.datetime.now(datetime.timezone.utc)
            })
            conn.commit()

        job = Job('test', config, wait_on_enter=0, connection_string=connection_string)

        leader = job.cluster.elect_leader()
        assert_equal(leader, 'node-before-dst',
                    'Node created before DST should be older despite DST transition')


class TestTokenDistributionTimezones:
    """Test token distribution timestamp handling across timezones."""

    def test_token_assigned_at_different_timezones(self, postgres):
        """Verify token assignment timestamps work with mixed timezones.
        """
        config = get_timezone_test_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        # Register nodes in different timezones
        timezones = ['UTC', 'America/New_York', 'Asia/Tokyo']
        base_time = datetime.datetime.now(datetime.timezone.utc)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Node"]}'))
            conn.execute(text(f'DELETE FROM {tables["Token"]}'))

            for i, tz in enumerate(timezones):
                tz_time = base_time.astimezone(ZoneInfo(tz))
                conn.execute(text(f"""
                    INSERT INTO {tables["Node"]} (name, created_on, last_heartbeat)
                    VALUES (:name, :created_on, :heartbeat)
                """), {
                    'name': f'node-{i+1}',
                    'created_on': tz_time,
                    'heartbeat': tz_time
                })
            conn.commit()

        coord_config = CoordinationConfig(total_tokens=30)
        job = Job('node-1', config, wait_on_enter=0, connection_string=connection_string, coordination_config=coord_config)

        job.tokens.distribute(job.locks, job.cluster)

        # Verify all tokens have assigned_at timestamps
        with postgres.connect() as conn:
            result = conn.execute(text(f"""
                SELECT token_id, node, assigned_at
                FROM {tables["Token"]}
                ORDER BY token_id
            """))
            tokens = [dict(row._mapping) for row in result]

        assert_equal(len(tokens), 30, 'All tokens should be assigned')
        for token in tokens:
            assert_true(token['assigned_at'] is not None,
                       f'Token {token["token_id"]} should have assigned_at timestamp')

    def test_token_version_increment_across_timezones(self, postgres):
        """Verify token version increments work correctly with timezone-aware times.
        """
        config = get_timezone_test_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        base_time = datetime.datetime.now(datetime.timezone.utc)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Node"]}'))
            conn.execute(text(f'DELETE FROM {tables["Token"]}'))

            for i in range(1, 4):
                conn.execute(text(f"""
                    INSERT INTO {tables["Node"]} (name, created_on, last_heartbeat)
                    VALUES (:name, :created_on, :heartbeat)
                """), {
                    'name': f'node{i}',
                    'created_on': base_time,
                    'heartbeat': base_time
                })
            conn.commit()

        coord_config = CoordinationConfig(total_tokens=30)
        job = Job('node1', config, wait_on_enter=0, connection_string=connection_string, coordination_config=coord_config)

        # First distribution
        job.tokens.distribute(job.locks, job.cluster)

        with postgres.connect() as conn:
            result = conn.execute(text(f'SELECT MAX(version) FROM {tables["Token"]}'))
            version1 = result.scalar()

        # Second distribution (simulating rebalance)
        time.sleep(1)
        job.tokens.distribute(job.locks, job.cluster)

        with postgres.connect() as conn:
            result = conn.execute(text(f'SELECT MAX(version) FROM {tables["Token"]}'))
            version2 = result.scalar()

        assert_true(version2 > version1,
                   'Token version should increment on redistribution')


class TestRebalanceTimingTimezones:
    """Test rebalance timing with timezone-aware timestamps."""

    def test_rebalance_log_timestamps_consistent(self, postgres):
        """Verify rebalance log timestamps are consistent across timezone changes.
        """
        config = get_timezone_test_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        base_time = datetime.datetime.now(datetime.timezone.utc)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Node"]}'))
            conn.execute(text(f'DELETE FROM {tables["Rebalance"]}'))

            for i in range(1, 3):
                conn.execute(text(f"""
                    INSERT INTO {tables["Node"]} (name, created_on, last_heartbeat)
                    VALUES (:name, :created_on, :heartbeat)
                """), {
                    'name': f'node{i}',
                    'created_on': base_time,
                    'heartbeat': base_time
                })
            conn.commit()

        coord_config = CoordinationConfig(total_tokens=30)
        job = Job('node1', config, wait_on_enter=0,
                 connection_string=connection_string, coordination_config=coord_config)

        # Trigger rebalance
        job._distribute_tokens_safe()

        with postgres.connect() as conn:
            result = conn.execute(text(f"""
                SELECT triggered_at, leader_node, duration_ms
                FROM {tables["Rebalance"]}
                ORDER BY triggered_at DESC
                LIMIT 1
            """))
            rebalance = [dict(row._mapping) for row in result]

        assert_equal(len(rebalance), 1, 'Rebalance should be logged')
        assert_true(rebalance[0]['triggered_at'] is not None,
                   'Rebalance triggered_at should have timestamp')
        assert_true(rebalance[0]['duration_ms'] > 0,
                   'Rebalance should have measurable duration')

    def test_membership_change_detection_across_timezones(self, postgres):
        """Verify membership change detection works with nodes in different timezones.
        """
        config = get_timezone_test_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        # Initial nodes in different timezones
        utc_time = datetime.datetime.now(datetime.timezone.utc)
        tokyo_time = datetime.datetime.now(ZoneInfo('Asia/Tokyo'))

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Node"]}'))

            conn.execute(text(f"""
                INSERT INTO {tables["Node"]} (name, created_on, last_heartbeat)
                VALUES (:name, :created_on, :heartbeat)
            """), {
                'name': 'node-utc',
                'created_on': utc_time,
                'heartbeat': utc_time
            })

            conn.execute(text(f"""
                INSERT INTO {tables["Node"]} (name, created_on, last_heartbeat)
                VALUES (:name, :created_on, :heartbeat)
            """), {
                'name': 'node-tokyo',
                'created_on': tokyo_time,
                'heartbeat': tokyo_time
            })
            conn.commit()

        job = Job('test', config, wait_on_enter=0, connection_string=connection_string)

        active_nodes = job.get_active_nodes()
        assert_equal(len(active_nodes), 2,
                    'Both nodes should be detected as active')

        # Simulate one node dying (old heartbeat in another timezone)
        old_heartbeat = utc_time - datetime.timedelta(seconds=20)
        with postgres.connect() as conn:
            conn.execute(text(f"""
                UPDATE {tables["Node"]}
                SET last_heartbeat = :heartbeat
                WHERE name = 'node-tokyo'
            """), {'heartbeat': old_heartbeat})
            conn.commit()

        time.sleep(1)
        active_nodes_after = job.get_active_nodes()
        assert_equal(len(active_nodes_after), 1,
                    'Only UTC node should remain active')


class TestDatabaseTimezoneConsistency:
    """Test database timezone configuration doesn't cause issues."""

    def test_database_timezone_vs_client_timezone(self, postgres):
        """Verify operations work correctly when database and client have different timezones.
        """
        config = get_timezone_test_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        # Set different timezone for this test's context
        client_time = datetime.datetime.now(ZoneInfo('America/Chicago'))

        with postgres.connect() as conn:
            # Check database timezone
            result = conn.execute(text('SHOW timezone'))
            db_tz = result.scalar()
            logger.info(f'Database timezone: {db_tz}')

        job = Job('test-node', config, wait_on_enter=5, connection_string=connection_string)
        job.__enter__()

        try:
            time.sleep(2)

            # Verify node registered successfully
            with postgres.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT name, created_on, last_heartbeat
                    FROM {tables["Node"]}
                    WHERE name = 'test-node'
                """))
                node = [dict(row._mapping) for row in result]

            assert_equal(len(node), 1, 'Node should be registered')
            assert_true(node[0]['created_on'] is not None,
                       'Node created_on should have value')
            assert_true(node[0]['last_heartbeat'] is not None,
                       'Node last_heartbeat should have value')

            # Verify node is detected as alive
            assert_true(job.am_i_healthy(),
                       'Node should be healthy regardless of timezone differences')

        finally:
            job.__exit__(None, None, None)

    def test_timestamp_without_timezone_compatibility(self, postgres):
        """Verify system handles timestamp without timezone correctly.
        """
        config = get_timezone_test_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        # PostgreSQL stores "timestamp with time zone" and we use timezone-aware datetime
        # This test verifies the conversion is handled correctly

        job = Job('tz-test', config, wait_on_enter=5, connection_string=connection_string)
        job.__enter__()

        try:
            time.sleep(1)

            # Read back the timestamp
            with postgres.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT created_on, last_heartbeat
                    FROM {tables["Node"]}
                    WHERE name = 'tz-test'
                """))
                node = result.first()

            created_on = node[0]
            last_heartbeat = node[1]

            # Should be able to compare with current time
            now = datetime.datetime.now(datetime.timezone.utc)
            age_created = now - created_on
            age_heartbeat = now - last_heartbeat

            assert_true(age_created.total_seconds() < 10,
                       'Created timestamp should be recent')
            assert_true(age_heartbeat.total_seconds() < 10,
                       'Heartbeat timestamp should be recent')

        finally:
            job.__exit__(None, None, None)

    def test_audit_timestamps_use_database_timezone(self, postgres):
        """Verify audit logging timestamps are stored in database's local timezone.

        Database is configured for America/New_York timezone in conftest.py.
        This test ensures audit records preserve timezone information correctly.
        """
        config = get_timezone_test_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Audit"]}'))
            conn.commit()

            result = conn.execute(text('SHOW timezone'))
            db_timezone = result.scalar()
            logger.info(f'Database timezone: {db_timezone}')

        job = Job('audit-tz-test', config, wait_on_enter=0, connection_string=connection_string)
        job.__enter__()

        try:
            task1 = Task(1, 'task-1')
            task2 = Task(2, 'task-2')

            client_time_utc = datetime.datetime.now(datetime.timezone.utc)
            client_time_tokyo = datetime.datetime.now(ZoneInfo('Asia/Tokyo'))
            client_time_db = datetime.datetime.now(ZoneInfo(db_timezone))

            job.tasks._tasks = [
                (task1, client_time_utc),
                (task2, client_time_tokyo),
            ]

            job.write_audit()

            with postgres.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT item, created_on
                    FROM {tables["Audit"]}
                    WHERE node = 'audit-tz-test'
                    ORDER BY item
                """))
                audit_records = [dict(row._mapping) for row in result]

            assert_equal(len(audit_records), 2, 'Should have 2 audit records')

            for record in audit_records:
                stored_time = record['created_on']
                logger.info(f"Task {record['item']}: stored as {stored_time}")

                age_seconds = abs((datetime.datetime.now(datetime.timezone.utc) - stored_time).total_seconds())
                assert_true(age_seconds < 30,
                           f'Audit timestamp should be recent (age: {age_seconds}s)')

            utc_stored = audit_records[0]['created_on']
            tokyo_stored = audit_records[1]['created_on']

            try:
                utc_to_tokyo_diff = abs((client_time_utc - client_time_tokyo).total_seconds())
            except:
                utc_to_tokyo_diff = abs((datetime.datetime.fromisoformat(str(client_time_utc)) - datetime.datetime.fromisoformat(str(client_time_tokyo))).total_seconds())
            stored_diff = abs((utc_stored - tokyo_stored).total_seconds())

            assert_true(abs(stored_diff - utc_to_tokyo_diff) < 2,
                       'Time differences should be preserved across different input timezones')

            logger.info('✓ Audit timestamps correctly stored with timezone information')

        finally:
            job.__exit__(None, None, None)


class TestDatetimeParameterTimezones:
    """Test Job initialization with datetime objects in various timezone configurations."""

    @pytest.mark.parametrize(('label', 'datetime_factory'), [
        ('naive', lambda: datetime.datetime(2024, 3, 15, 14, 30, 0)),
        ('utc', lambda: datetime.datetime(2024, 3, 15, 14, 30, 0, tzinfo=datetime.timezone.utc)),
        ('tokyo', lambda: datetime.datetime(2024, 3, 15, 14, 0, 0, tzinfo=ZoneInfo('Asia/Tokyo'))),
        ('local', lambda: datetime.datetime(2024, 3, 15, 14, 0, 0, tzinfo=datetime.timezone.utc)),
    ])
    def test_datetime_with_various_timezones(self, postgres, label, datetime_factory):
        """Verify datetime objects with various timezone configurations are handled correctly.
        """
        config = get_timezone_test_config()
        connection_string = postgres.url.render_as_string(hide_password=False)

        dt = datetime_factory()
        job = Job(f'test-{label}', config, date=dt, wait_on_enter=0,
                 connection_string=connection_string)

        assert_true(job.tasks.date is not None, f'{label}: Date should be set')
        assert_equal(job.tasks.date.year, 2024, f'{label}: Year should match')
        assert_equal(job.tasks.date.month, 3, f'{label}: Month should match')
        assert_equal(job.tasks.date.day, 15, f'{label}: Day should match')

        job.__enter__()
        try:
            assert_true(job.am_i_healthy(), f'{label}: Job should be healthy')
        finally:
            job.__exit__(None, None, None)

    def test_datetime_timezone_preserved_in_audit(self, postgres):
        """Verify timezone information is correctly handled in audit writes.
        """
        config = get_timezone_test_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        with postgres.connect() as conn:
            conn.execute(text(f'DELETE FROM {tables["Audit"]}'))
            conn.commit()

        test_cases = [
            ('naive', datetime.datetime(2024, 3, 15, 10, 0, 0)),
            ('utc', datetime.datetime(2024, 3, 15, 10, 0, 0, tzinfo=datetime.timezone.utc)),
            ('tokyo', datetime.datetime(2024, 3, 15, 10, 0, 0, tzinfo=ZoneInfo('Asia/Tokyo'))),
            ('ny', datetime.datetime(2024, 3, 15, 10, 0, 0, tzinfo=ZoneInfo('America/New_York'))),
        ]

        for label, dt in test_cases:
            job = Job(f'audit-{label}', config, date=dt, wait_on_enter=0,
                     connection_string=connection_string)
            job.__enter__()

            try:
                task = Task(1, f'task-{label}')
                job.tasks._tasks.append((task, datetime.datetime.now(datetime.timezone.utc)))
                job.write_audit()

                with postgres.connect() as conn:
                    result = conn.execute(text(f"""
                        SELECT date, node, item, created_on
                        FROM {tables["Audit"]}
                        WHERE node = :node
                    """), {'node': f'audit-{label}'})
                    audit_record = result.first()

                assert_true(audit_record is not None, f'Audit record should exist for {label}')

                stored_date = audit_record[0]
                assert_equal(stored_date.year, 2024, f'Year should match for {label}')
                assert_equal(stored_date.month, 3, f'Month should match for {label}')
                assert_equal(stored_date.day, 15, f'Day should match for {label}')

                logger.info(f'✓ {label}: date stored correctly as {stored_date}')

            finally:
                job.__exit__(None, None, None)

    def test_datetime_comparison_across_timezones(self, postgres):
        """Verify datetime comparisons work correctly across different timezone inputs.
        """
        config = get_timezone_test_config()
        connection_string = postgres.url.render_as_string(hide_password=False)

        same_moment_different_tz = [
            datetime.datetime(2024, 3, 15, 10, 0, 0, tzinfo=datetime.timezone.utc),
            datetime.datetime(2024, 3, 15, 5, 0, 0, tzinfo=ZoneInfo('America/New_York')),
            datetime.datetime(2024, 3, 15, 19, 0, 0, tzinfo=ZoneInfo('Asia/Tokyo')),
        ]

        jobs = []
        for i, dt in enumerate(same_moment_different_tz):
            job = Job(f'compare-{i}', config, date=dt, wait_on_enter=0,
                     connection_string=connection_string)
            jobs.append(job)

        for job in jobs:
            job.__enter__()

        try:
            dates = [job.tasks.date for job in jobs]

            for i, date1 in enumerate(dates):
                for j, date2 in enumerate(dates):
                    if i != j:
                        logger.info(f'Comparing {date1} vs {date2}')
                        assert_equal(date1.year, date2.year, 'Years should match')
                        assert_equal(date1.month, date2.month, 'Months should match')
                        assert_equal(date1.day, date2.day, 'Days should match')

            logger.info('✓ All dates representing same moment compare correctly')

        finally:
            for job in jobs:
                job.__exit__(None, None, None)

    def test_datetime_date_boundary_timezones(self, postgres):
        """Verify date boundaries are handled correctly across timezones.
        """
        config = get_timezone_test_config()
        connection_string = postgres.url.render_as_string(hide_password=False)

        midnight_utc = datetime.datetime(2024, 3, 15, 0, 0, 0, tzinfo=datetime.timezone.utc)
        late_tokyo = datetime.datetime(2024, 3, 15, 23, 59, 59, tzinfo=ZoneInfo('Asia/Tokyo'))
        early_ny = datetime.datetime(2024, 3, 15, 0, 0, 1, tzinfo=ZoneInfo('America/New_York'))

        test_cases = [
            ('midnight-utc', midnight_utc),
            ('late-tokyo', late_tokyo),
            ('early-ny', early_ny),
        ]

        for label, dt in test_cases:
            job = Job(label, config, date=dt, wait_on_enter=0,
                     connection_string=connection_string)

            assert_equal(job.tasks.date.day, 15, f'{label} should preserve day 15')
            logger.info(f'✓ {label}: {dt} -> day={job.tasks.date.day}')

    def test_timezone_aware_datetime_types(self, postgres):
        """Verify timezone-aware datetime objects work correctly.
        """
        config = get_timezone_test_config()
        connection_string = postgres.url.render_as_string(hide_password=False)

        tz_aware_dt = datetime.datetime(2024, 3, 15, 14, 30, 0, tzinfo=datetime.timezone.utc)

        job = Job('test-tzaware', config, date=tz_aware_dt, wait_on_enter=0,
                 connection_string=connection_string)

        assert_true(job.tasks.date is not None, 'Date should be set')
        assert_equal(job.tasks.date.year, 2024, 'Year should match')
        assert_equal(job.tasks.date.month, 3, 'Month should match')
        assert_equal(job.tasks.date.day, 15, 'Day should match')

        job.__enter__()
        try:
            assert_true(job.am_i_healthy(), 'Job should be healthy with timezone-aware datetime')
        finally:
            job.__exit__(None, None, None)


class TestDateConsistency:
    """Test that self._date is always a date-only object (datetime.date), never datetime."""

    def test_date_none_returns_date_not_datetime(self, postgres):
        """Verify date=None results in date-only object (datetime.date), not datetime.
        """
        config = get_timezone_test_config()
        connection_string = postgres.url.render_as_string(hide_password=False)

        job = Job('test-none', config, date=None, wait_on_enter=0,
                 connection_string=connection_string)

        assert_true(isinstance(job.tasks.date, datetime.date),
                   f'date=None should create date object, got {type(job.tasks.date).__name__}')
        assert_true(not isinstance(job.tasks.date, datetime.datetime),
                   f'date=None should NOT create datetime object, got {type(job.tasks.date).__name__}')

    def test_all_date_inputs_return_date_type(self, postgres):
        """Verify all date initialization paths result in date-only objects (datetime.date).
        """
        config = get_timezone_test_config()
        connection_string = postgres.url.render_as_string(hide_password=False)

        test_cases = [
            ('none', None),
            ('date', datetime.datetime(2024, 3, 15).date()),
            ('datetime-naive', datetime.datetime(2024, 3, 15, 10, 0, 0)),
            ('datetime-utc', datetime.datetime(2024, 3, 15, 10, 0, 0, tzinfo=datetime.timezone.utc)),
            ('datetime-tokyo', datetime.datetime(2024, 3, 15, 10, 0, 0, tzinfo=ZoneInfo('Asia/Tokyo'))),
        ]

        for label, date_input in test_cases:
            job = Job(f'test-{label}', config, date=date_input, wait_on_enter=0,
                     connection_string=connection_string)

            assert_true(isinstance(job.tasks.date, datetime.date),
                       f'{label}: should create date object, got {type(job.tasks.date).__name__}')
            assert_true(not isinstance(job.tasks.date, datetime.datetime),
                       f'{label}: should NOT create datetime object, got {type(job.tasks.date).__name__}')

            logger.info(f'✓ {label}: correctly creates date object (type={type(job.tasks.date).__name__})')

    def test_date_type_prevents_timezone_issues(self, postgres):
        """Verify date objects don't have timezone information that could cause issues.
        """
        config = get_timezone_test_config()
        connection_string = postgres.url.render_as_string(hide_password=False)

        tokyo_dt = datetime.datetime(2024, 3, 15, 23, 0, 0, tzinfo=ZoneInfo('Asia/Tokyo'))

        job = Job('test-date-only', config, date=tokyo_dt, wait_on_enter=0,
                 connection_string=connection_string)

        assert_true(isinstance(job.tasks.date, datetime.date), 'Should be date-only object')
        assert_true(not isinstance(job.tasks.date, datetime.datetime), 'Should NOT be datetime object')

        assert_equal(job.tasks.date.year, 2024, 'Year should match')
        assert_equal(job.tasks.date.month, 3, 'Month should match')
        assert_equal(job.tasks.date.day, 15, 'Day should preserve input date regardless of timezone')

        # Note: date objects don't have tzinfo attribute
        assert_true(not hasattr(job.tasks.date, 'tzinfo') or job.tasks.date.tzinfo is None,
                   'Date objects should not have timezone information')


if __name__ == '__main__':
    pytest.main(args=['-sx', __file__])
