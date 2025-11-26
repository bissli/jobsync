"""Tests for timezone handling across all operations.

USE THIS FILE FOR:
- Timezone-related functionality
- Cross-timezone coordination scenarios
- Timestamp handling edge cases
- Database timezone configuration tests
"""
import datetime
import logging
import time
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import text

from jobsync import schema
from jobsync.client import CoordinationConfig, LockNotAcquired

logger = logging.getLogger(__name__)


from fixtures import *  # noqa: F401, F403

logger = logging.getLogger(__name__)


class TestHeartbeatTimezoneHandling:
    """Test heartbeat timeout detection across timezones."""

    @clean_tables('Node')
    def test_heartbeat_timeout_different_timezones(self, postgres):
        """Verify heartbeat timeout works when nodes use different timezones.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        # Simulate nodes in different timezones
        timezones = ['UTC', 'America/New_York', 'Asia/Tokyo', 'Europe/London']
        current_time = datetime.datetime.now(datetime.timezone.utc)

        for tz in timezones:
            tz_time = current_time.astimezone(ZoneInfo(tz))
            insert_active_node(postgres, tables, f'node-{tz.replace("/", "-")}', created_on=tz_time)

        job = create_job('test', postgres, coordination_config=config, wait_on_enter=0)

        try:
            active_nodes = job.get_active_nodes()
            active_names = [n['name'] for n in active_nodes]

            # All nodes should be active (heartbeat within timeout)
            for tz in timezones:
                node_name = f'node-{tz.replace("/", "-")}'
                assert node_name in active_names, f'{node_name} should be active'

        finally:
            with postgres.connect() as conn:
                for tz in timezones:
                    delete_rows(postgres, tables, 'Node', 'name = :name',
                               {'name': f'node-{tz.replace("/", "-")}'})


class TestLockExpirationTimezones:
    """Test lock expiration handling across timezones."""

    @clean_tables('Lock')
    def test_lock_expiration_utc_vs_local(self, postgres):
        """Verify lock expiration works with different timezone timestamps.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        # Create locks with different timezone timestamps
        utc_now = datetime.datetime.now(datetime.timezone.utc)
        ny_now = datetime.datetime.now(ZoneInfo('America/New_York'))

        expired_utc = utc_now - datetime.timedelta(days=2)
        expired_ny = ny_now - datetime.timedelta(days=2)
        valid_utc = utc_now + datetime.timedelta(days=1)

        insert_lock(postgres, tables, 1, ['pattern-utc'], created_by='test', expires_at=expired_utc)
        insert_lock(postgres, tables, 2, ['pattern-ny'], created_by='test', expires_at=expired_ny)
        insert_lock(postgres, tables, 3, ['pattern-valid'], created_by='test', expires_at=valid_utc)

        job = create_job('test', postgres, coordination_config=config, wait_on_enter=0)

        active_locks = job.locks.get_active_locks()

        # Convert task_ids to token_ids for comparison (get_active_locks returns token_ids as keys)
        token_1 = job.task_to_token(1)
        token_2 = job.task_to_token(2)
        token_3 = job.task_to_token(3)

        # Only valid lock should remain
        assert token_1 not in active_locks, 'Expired UTC lock should be removed'
        assert token_2 not in active_locks, 'Expired NY lock should be removed'
        assert token_3 in active_locks, 'Valid lock should remain'


class TestLeaderLockTimezones:
    """Test leader lock timing across timezones."""

    @clean_tables('LeaderLock')
    def test_stale_leader_lock_detection_different_timezones(self, postgres):
        """Verify stale lock detection works with timezone-aware timestamps.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        # Create stale lock with NY timezone timestamp
        ny_time = datetime.datetime.now(ZoneInfo('America/New_York'))
        stale_time = ny_time - datetime.timedelta(seconds=400)

        insert_leader_lock(postgres, tables, 'stale-node', 'stale-operation', acquired_at=stale_time)

        coord_config = CoordinationConfig(
            total_tokens=100,
            heartbeat_interval_sec=1,
            stale_leader_lock_age_sec=300
        )

        job = create_job('test', postgres, wait_on_enter=0, coordination_config=coord_config)

        # Should be able to acquire lock (stale lock removed)
        try:
            with job.locks.acquire_leader_lock('test-operation'):
                acquired = True
        except LockNotAcquired:
            acquired = False
        assert acquired, 'Should acquire lock after removing stale NY timezone lock'

    @clean_tables('LeaderLock')
    def test_leader_lock_acquired_time_across_timezones(self, postgres):
        """Verify leader lock timing works when nodes are in different timezones.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        # Node1 in Tokyo timezone acquires lock
        tokyo_time = datetime.datetime.now(ZoneInfo('Asia/Tokyo'))

        job1 = create_job('node-tokyo', postgres, config)

        try:
            with job1.locks.acquire_leader_lock('tokyo-operation'):
                acquired_tokyo = True

                # Node2 in NY timezone tries to acquire immediately
                job2 = create_job('node-ny', postgres, config)

                try:
                    with job2.locks.acquire_leader_lock('ny-operation'):
                        acquired_ny = True
                except LockNotAcquired:
                    acquired_ny = False
                assert not acquired_ny, 'NY node should not acquire lock held by Tokyo node'
        except LockNotAcquired:
            acquired_tokyo = False

        assert acquired_tokyo, 'Tokyo node should acquire lock'

        # Now NY node should be able to acquire
        try:
            with job2.locks.acquire_leader_lock('ny-operation-after'):
                acquired_ny_after = True
        except LockNotAcquired:
            acquired_ny_after = False
        assert acquired_ny_after, 'NY node should acquire after Tokyo released'


class TestLeaderElectionTimezones:
    """Test leader election with timezone-aware timestamps."""

    @clean_tables('Node')
    def test_leader_election_with_mixed_timezones(self, postgres):
        """Verify leader election works correctly when nodes registered in different timezones.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        base_utc = datetime.datetime.now(datetime.timezone.utc)

        nodes = [
            ('node-tokyo', base_utc.astimezone(ZoneInfo('Asia/Tokyo')) - datetime.timedelta(seconds=30)),
            ('node-london', base_utc.astimezone(ZoneInfo('Europe/London')) - datetime.timedelta(seconds=20)),
            ('node-ny', base_utc.astimezone(ZoneInfo('America/New_York')) - datetime.timedelta(seconds=10)),
        ]

        for name, created_on in nodes:
            insert_active_node(postgres, tables, name, created_on=created_on)

        with postgres.connect() as conn:
            conn.execute(text(f"""
                UPDATE {tables["Node"]} SET last_heartbeat = :heartbeat
            """), {'heartbeat': base_utc})
            conn.commit()

        job = create_job('test', postgres, config)

        leader = job.cluster.elect_leader()
        assert leader == 'node-tokyo', 'Oldest node (Tokyo) should be elected regardless of timezone'


class TestTokenDistributionTimezones:
    """Test token distribution timestamp handling across timezones."""

    @clean_tables('Node', 'Token')
    def test_token_assigned_at_different_timezones(self, postgres):
        """Verify token assignment timestamps work with mixed timezones.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        # Register nodes in different timezones
        timezones = ['UTC', 'America/New_York', 'Asia/Tokyo']
        base_time = datetime.datetime.now(datetime.timezone.utc)

        for i, tz in enumerate(timezones):
            tz_time = base_time.astimezone(ZoneInfo(tz))
            insert_active_node(postgres, tables, f'node-{i+1}', created_on=tz_time)

        coord_config = CoordinationConfig(total_tokens=30)
        job = create_job('node-1', postgres, coordination_config=coord_config, wait_on_enter=0)

        job.tokens.distribute(job.locks, job.cluster)

        # Verify all tokens have assigned_at timestamps
        with postgres.connect() as conn:
            result = conn.execute(text(f"""
                SELECT token_id, node, assigned_at
                FROM {tables["Token"]}
                ORDER BY token_id
            """))
            tokens = [dict(row._mapping) for row in result]

        assert len(tokens) == 30, 'All tokens should be assigned'
        for token in tokens:
            assert token['assigned_at'] is not None, f'Token {token["token_id"]} should have assigned_at timestamp'

    @clean_tables('Node', 'Token')
    def test_token_version_increment_across_timezones(self, postgres):
        """Verify token version increments work correctly with timezone-aware times.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        base_time = datetime.datetime.now(datetime.timezone.utc)

        for i in range(1, 4):
            insert_active_node(postgres, tables, f'node{i}', created_on=base_time)

        coord_config = CoordinationConfig(total_tokens=30)
        job = create_job('node1', postgres, wait_on_enter=0, coordination_config=coord_config)

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

        assert version2 > version1, 'Token version should increment on redistribution'


class TestRebalanceTimingTimezones:
    """Test rebalance timing with timezone-aware timestamps."""

    @clean_tables('Node', 'Rebalance')
    def test_rebalance_log_timestamps_consistent(self, postgres):
        """Verify rebalance log timestamps are consistent across timezone changes.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        base_time = datetime.datetime.now(datetime.timezone.utc)

        for i in range(1, 3):
            insert_active_node(postgres, tables, f'node{i}', created_on=base_time)

        coord_config = CoordinationConfig(total_tokens=30)
        job = create_job('node1', postgres, wait_on_enter=0, coordination_config=coord_config)

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

        assert len(rebalance) == 1, 'Rebalance should be logged'
        assert rebalance[0]['triggered_at'] is not None, 'Rebalance triggered_at should have timestamp'
        assert rebalance[0]['duration_ms'] > 0, 'Rebalance should have measurable duration'

    @clean_tables('Node')
    def test_membership_change_detection_across_timezones(self, postgres):
        """Verify membership change detection works with nodes in different timezones.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        # Initial nodes in different timezones
        utc_time = datetime.datetime.now(datetime.timezone.utc)
        tokyo_time = datetime.datetime.now(ZoneInfo('Asia/Tokyo'))

        insert_active_node(postgres, tables, 'node-utc', created_on=utc_time)
        insert_active_node(postgres, tables, 'node-tokyo', created_on=tokyo_time)

        job = create_job('test', postgres, config)

        active_nodes = job.get_active_nodes()
        assert len(active_nodes) == 2, 'Both nodes should be detected as active'

        # Simulate one node dying (old heartbeat in another timezone)
        old_heartbeat = utc_time - datetime.timedelta(seconds=20)
        with postgres.connect() as conn:
            conn.execute(text(f"""
                UPDATE {tables["Node"]}
                SET last_heartbeat = :heartbeat
                WHERE name = 'node-tokyo'
            """), {'heartbeat': old_heartbeat})
            conn.commit()

        active_nodes_after = job.get_active_nodes()
        assert len(active_nodes_after) == 1, 'Only UTC node should remain active'


class TestDatabaseTimezoneConsistency:
    """Test database timezone configuration doesn't cause issues."""

    @clean_tables('Audit')
    def test_audit_timestamps_use_database_timezone(self, postgres):
        """Verify audit logging timestamps are stored in database's local timezone.

        Database is configured for America/New_York timezone in conftest.py.
        This test ensures audit records preserve timezone information correctly.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        with postgres.connect() as conn:
            result = conn.execute(text('SHOW timezone'))
            db_timezone = result.scalar()
            logger.info(f'Database timezone: {db_timezone}')

        job = create_job('audit-tz-test', postgres, coordination_config=config, wait_on_enter=0)
        job.__enter__()

        try:
            task1 = create_task(1, 'task-1')
            task2 = create_task(2, 'task-2')

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
                    SELECT task_id, created_on
                    FROM {tables["Audit"]}
                    WHERE node = 'audit-tz-test'
                    ORDER BY task_id
                """))
                audit_records = [dict(row._mapping) for row in result]

            assert len(audit_records) == 2, 'Should have 2 audit records'

            for record in audit_records:
                stored_time = record['created_on']
                logger.info(f"Task {record['task_id']}: stored as {stored_time}")

                age_seconds = abs((datetime.datetime.now(datetime.timezone.utc) - stored_time).total_seconds())
                assert age_seconds < 30, f'Audit timestamp should be recent (age: {age_seconds}s)'

            utc_stored = audit_records[0]['created_on']
            tokyo_stored = audit_records[1]['created_on']

            try:
                utc_to_tokyo_diff = abs((client_time_utc - client_time_tokyo).total_seconds())
            except:
                utc_to_tokyo_diff = abs((datetime.datetime.fromisoformat(str(client_time_utc)) - datetime.datetime.fromisoformat(str(client_time_tokyo))).total_seconds())
            stored_diff = abs((utc_stored - tokyo_stored).total_seconds())

            assert abs(stored_diff - utc_to_tokyo_diff) < 2, 'Time differences should be preserved across different input timezones'

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
        config = get_coordination_config()

        dt = datetime_factory()
        job = create_job(f'test-{label}', postgres, config, date=dt)

        assert job.tasks.date is not None, f'{label}: Date should be set'
        assert job.tasks.date.year == 2024, f'{label}: Year should match'
        assert job.tasks.date.month == 3, f'{label}: Month should match'
        assert job.tasks.date.day == 15, f'{label}: Day should match'

        job.__enter__()
        try:
            assert job.am_i_healthy(), f'{label}: Job should be healthy'
        finally:
            job.__exit__(None, None, None)

    @clean_tables('Audit')
    def test_datetime_timezone_preserved_in_audit(self, postgres):
        """Verify timezone information is correctly handled in audit writes.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        test_cases = [
            ('naive', datetime.datetime(2024, 3, 15, 10, 0, 0)),
            ('utc', datetime.datetime(2024, 3, 15, 10, 0, 0, tzinfo=datetime.timezone.utc)),
            ('tokyo', datetime.datetime(2024, 3, 15, 10, 0, 0, tzinfo=ZoneInfo('Asia/Tokyo'))),
            ('ny', datetime.datetime(2024, 3, 15, 10, 0, 0, tzinfo=ZoneInfo('America/New_York'))),
        ]

        for label, dt in test_cases:
            job = create_job(f'audit-{label}', postgres, coordination_config=config, wait_on_enter=0, date=dt)
            job.__enter__()

            try:
                task = create_task(1)
                job.tasks._tasks.append((task, datetime.datetime.now(datetime.timezone.utc)))
                job.write_audit()

                with postgres.connect() as conn:
                    result = conn.execute(text(f"""
                        SELECT date, node, task_id, created_on
                        FROM {tables["Audit"]}
                        WHERE node = :node
                    """), {'node': f'audit-{label}'})
                    audit_record = result.first()

                assert audit_record is not None, f'Audit record should exist for {label}'

                stored_date = audit_record[0]
                assert stored_date.year == 2024, f'Year should match for {label}'
                assert stored_date.month == 3, f'Month should match for {label}'
                assert stored_date.day == 15, f'Day should match for {label}'

                logger.info(f'✓ {label}: date stored correctly as {stored_date}')

            finally:
                job.__exit__(None, None, None)


class TestDateConsistency:
    """Test that self._date is always a date-only object (datetime.date), never datetime."""

    def test_date_none_returns_date_not_datetime(self, postgres):
        """Verify date=None results in date-only object (datetime.date), not datetime.
        """
        config = get_coordination_config()

        job = create_job('test-none', postgres, coordination_config=config, wait_on_enter=0, date=None)

        assert isinstance(job.tasks.date, datetime.date), \
                   f'date=None should create date object, got {type(job.tasks.date).__name__}'
        assert not isinstance(job.tasks.date, datetime.datetime), \
                   f'date=None should NOT create datetime object, got {type(job.tasks.date).__name__}'

    def test_all_date_inputs_return_date_type(self, postgres):
        """Verify all date initialization paths result in date-only objects (datetime.date).
        """
        config = get_coordination_config()

        test_cases = [
            ('none', None),
            ('date', datetime.datetime(2024, 3, 15).date()),
            ('datetime-naive', datetime.datetime(2024, 3, 15, 10, 0, 0)),
            ('datetime-utc', datetime.datetime(2024, 3, 15, 10, 0, 0, tzinfo=datetime.timezone.utc)),
            ('datetime-tokyo', datetime.datetime(2024, 3, 15, 10, 0, 0, tzinfo=ZoneInfo('Asia/Tokyo'))),
        ]

        for label, date_input in test_cases:
            job = create_job(f'test-{label}', postgres, coordination_config=config, wait_on_enter=0, date=date_input)

            assert isinstance(job.tasks.date, datetime.date), \
                       f'{label}: should create date object, got {type(job.tasks.date).__name__}'
            assert not isinstance(job.tasks.date, datetime.datetime), \
                       f'{label}: should NOT create datetime object, got {type(job.tasks.date).__name__}'

            logger.info(f'✓ {label}: correctly creates date object (type={type(job.tasks.date).__name__})')

    def test_date_type_prevents_timezone_issues(self, postgres):
        """Verify date objects don't have timezone information that could cause issues.
        """
        config = get_coordination_config()

        tokyo_dt = datetime.datetime(2024, 3, 15, 23, 0, 0, tzinfo=ZoneInfo('Asia/Tokyo'))

        job = create_job('test-date-only', postgres, coordination_config=config, wait_on_enter=0, date=tokyo_dt)

        assert isinstance(job.tasks.date, datetime.date), 'Should be date-only object'
        assert not isinstance(job.tasks.date, datetime.datetime), 'Should NOT be datetime object'

        assert job.tasks.date.year == 2024, 'Year should match'
        assert job.tasks.date.month == 3, 'Month should match'
        assert job.tasks.date.day == 15, 'Day should preserve input date regardless of timezone'

        assert not hasattr(job.tasks.date, 'tzinfo'), \
                   'Date objects should not have timezone information'


if __name__ == '__main__':
    pytest.main(args=['-sx', __file__])
