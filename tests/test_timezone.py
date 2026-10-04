"""Tests for timezone handling across all operations.

USE THIS FILE FOR:
- Timezone-related functionality
- Cross-timezone coordination scenarios
- Timestamp handling edge cases
- Database timezone configuration tests
"""
import datetime
import logging
from zoneinfo import ZoneInfo

import pytest
from fixtures import *  # noqa: F401, F403
from sqlalchemy import text

from jobsync import schema
from jobsync.client import CoordinationConfig, LockNotAcquired

logger = logging.getLogger(__name__)


class TestHeartbeatTimezoneHandling:
    """Test heartbeat timeout detection across timezones."""

    @clean_tables('Node')
    def test_heartbeat_timeout_different_timezones(self, postgres):
        """Verify heartbeats written in any zone are aged as instants.

        Mutation: active_nodes_sql using heartbeat_interval_sec in place of
            heartbeat_timeout_sec, or the comparison flipped.
        Oracle: 10s timeout; heartbeats 0s and 5s old are active, 20s old
            are dead, each written in a different zone.
        """
        config = get_coordination_config(heartbeat_timeout_sec=10)
        tables = schema.get_table_names(config.appname)

        now = datetime.datetime.now(datetime.timezone.utc)
        heartbeat_age_by_zone = {
            'UTC': 20,
            'America/New_York': 20,
            'Asia/Tokyo': 5,
            'Europe/London': 0,
            }

        with postgres.connect() as conn:
            for zone, age_sec in heartbeat_age_by_zone.items():
                heartbeat = (now - datetime.timedelta(seconds=age_sec)).astimezone(ZoneInfo(zone))
                conn.execute(text(f"""
                    INSERT INTO {tables["Node"]} (name, created_on, last_heartbeat)
                    VALUES (:name, :heartbeat, :heartbeat)
                """), {'name': f'node-{zone}', 'heartbeat': heartbeat})
            conn.commit()

        job = create_job('test', postgres, coordination_config=config)

        try:
            active_names = {node['name'] for node in job.get_active_nodes()}

            assert active_names == {'node-Asia/Tokyo', 'node-Europe/London'}

        finally:
            job.__exit__(None, None, None)


class TestLockExpirationTimezones:
    """Test lock expiration handling across timezones."""

    @clean_tables('Lock')
    def test_lock_expiration_utc_vs_local(self, postgres):
        """Verify lock expiry compares instants, not wall-clock readings.

        Mutation: the expired-lock DELETE in get_active_locks dropped or its
            comparison flipped.
        Oracle: an expiry one hour past written in Tokyo (wall clock reads
            ahead) and one hour ahead written in New York (reads behind).
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        utc_now = datetime.datetime.now(datetime.timezone.utc)
        expired_tokyo = (utc_now - datetime.timedelta(hours=1)).astimezone(ZoneInfo('Asia/Tokyo'))
        expired_utc = utc_now - datetime.timedelta(hours=1)
        valid_ny = (utc_now + datetime.timedelta(hours=1)).astimezone(ZoneInfo('America/New_York'))

        insert_lock(postgres, tables, 1, ['pattern-tokyo'], created_by='test', expires_at=expired_tokyo)
        insert_lock(postgres, tables, 2, ['pattern-utc'], created_by='test', expires_at=expired_utc)
        insert_lock(postgres, tables, 3, ['pattern-valid'], created_by='test', expires_at=valid_ny)

        job = create_job('test', postgres, coordination_config=config)

        try:
            active_locks = job.locks.get_active_locks()

            assert active_locks == {job.task_to_token(3): ['pattern-valid']}, \
                'Only the lock expiring in the future should remain'

        finally:
            job.__exit__(None, None, None)


class TestLeaderLockTimezones:
    """Test leader lock timing across timezones."""

    @clean_tables('LeaderLock')
    def test_stale_leader_lock_detection_different_timezones(self, postgres):
        """Verify a New York leader lock is stolen past 300s and kept before.

        Mutation: _check_and_clear_stale_lock clearing whatever lock it
            finds, or its age comparison flipped.
        Oracle: stale_leader_lock_age_sec=300 with locks 200s and 400s old,
            one on either side of it.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        coord_config = CoordinationConfig(
            total_tokens=100,
            heartbeat_interval_sec=1,
            stale_leader_lock_age_sec=300,
            leader_lock_timeout_sec=2
        )

        job = create_job('test', postgres, coordination_config=coord_config)

        try:
            ny_now = datetime.datetime.now(ZoneInfo('America/New_York'))

            insert_leader_lock(postgres, tables, 'fresh-node', 'fresh-operation',
                               acquired_at=ny_now - datetime.timedelta(seconds=200))
            with pytest.raises(LockNotAcquired), job.locks.acquire_leader_lock('test-operation'):
                pass

            delete_rows(postgres, tables, 'LeaderLock', 'singleton = 1')
            insert_leader_lock(postgres, tables, 'stale-node', 'stale-operation',
                               acquired_at=ny_now - datetime.timedelta(seconds=400))
            with job.locks.acquire_leader_lock('test-operation'):
                with postgres.connect() as conn:
                    holder = conn.execute(text(f'select node from {tables["LeaderLock"]}')).scalar_one()

            assert holder == 'test', 'Should acquire lock after removing stale NY timezone lock'

        finally:
            job.__exit__(None, None, None)

    @clean_tables('LeaderLock')
    def test_leader_lock_acquired_time_across_timezones(self, postgres):
        """Verify a held leader lock is fresh by DB time and blocks others.

        Mutation: acquired_at bound from naive datetime.datetime.utcnow() in
            _try_acquire_leader_lock, which the America/New_York session
            reads as hours off.
        Oracle: NOW() - acquired_at from the database clock must be under 5s
            while the lock is held.
        """
        config = get_coordination_config(leader_lock_timeout_sec=1)
        tables = schema.get_table_names(config.appname)

        job1 = create_job('node-tokyo', postgres, coordination_config=config)
        job2 = create_job('node-ny', postgres, coordination_config=config)

        try:
            with job1.locks.acquire_leader_lock('tokyo-operation'):
                with postgres.connect() as conn:
                    lock_age_sec = conn.execute(text(
                        f'select extract(epoch from now() - acquired_at) from {tables["LeaderLock"]}')).scalar_one()

                assert abs(lock_age_sec) < 5, f'Held lock should be fresh, age {lock_age_sec}s'

                with pytest.raises(LockNotAcquired), job2.locks.acquire_leader_lock('ny-operation'):
                    pass

            with job2.locks.acquire_leader_lock('ny-operation-after'):
                pass

        finally:
            job1.__exit__(None, None, None)
            job2.__exit__(None, None, None)


class TestLeaderElectionTimezones:
    """Test leader election with timezone-aware timestamps."""

    @clean_tables('Node')
    def test_leader_election_with_mixed_timezones(self, postgres):
        """Verify the first-created node wins whatever zone recorded it.

        Mutation: elect_leader ordering by created_on DESC, or by name first.
        Oracle: Tokyo 30s old, London 20s, New York 10s; by wall clock New
            York reads earliest and London sorts first by name.
        """
        config = get_coordination_config(heartbeat_timeout_sec=60)
        tables = schema.get_table_names(config.appname)

        base_utc = datetime.datetime.now(datetime.timezone.utc)

        nodes = [
            ('node-tokyo', base_utc.astimezone(ZoneInfo('Asia/Tokyo')) - datetime.timedelta(seconds=30)),
            ('node-london', base_utc.astimezone(ZoneInfo('Europe/London')) - datetime.timedelta(seconds=20)),
            ('node-ny', base_utc.astimezone(ZoneInfo('America/New_York')) - datetime.timedelta(seconds=10)),
        ]

        for name, created_on in nodes:
            insert_active_node(postgres, tables, name, created_on=created_on)

        job = create_job('test', postgres, coordination_config=config)

        try:
            leader = job.cluster.elect_leader()
            assert leader == 'node-tokyo', 'Oldest node (Tokyo) should be elected regardless of timezone'

        finally:
            job.__exit__(None, None, None)


class TestTokenDistributionTimezones:
    """Test token distribution timestamp handling across timezones."""

    @clean_tables('Node', 'Token')
    def test_token_version_increment_across_timezones(self, postgres):
        """Verify each distribution writes every token at the next version.

        Mutation: new_version left at MAX(version) with no + 1, or a stale
            row kept at the old version.
        Oracle: hand count of versions 1 then 2 from an empty Token table.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        base_time = datetime.datetime.now(datetime.timezone.utc)

        for i in range(1, 4):
            insert_active_node(postgres, tables, f'node{i}', created_on=base_time)

        coord_config = CoordinationConfig(total_tokens=30)
        job = create_job('node1', postgres, coordination_config=coord_config)

        try:
            versions_sql = f'select distinct version from {tables["Token"]}'

            job.tokens.distribute(job.locks, job.cluster)
            with postgres.connect() as conn:
                versions_first = {row[0] for row in conn.execute(text(versions_sql))}

            job.tokens.distribute(job.locks, job.cluster)
            with postgres.connect() as conn:
                versions_second = {row[0] for row in conn.execute(text(versions_sql))}

            assert versions_first == {1}
            assert versions_second == {2}, 'Token version should increment on redistribution'

        finally:
            job.__exit__(None, None, None)


class TestRebalanceTimingTimezones:
    """Test rebalance timing with timezone-aware timestamps."""

    @clean_tables('Node', 'Rebalance')
    def test_rebalance_log_timestamps_consistent(self, postgres):
        """Verify _distribute_tokens_safe logs one row with its reason.

        Mutation: trigger_reason not forwarded to distribute(), or
            duration_ms recorded in whole seconds.
        Oracle: the 'membership_change' reason passed in, leader 'node1',
            two active nodes, and a triggered_at between real-clock
            readings taken around the call.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        base_time = datetime.datetime.now(datetime.timezone.utc)

        for i in range(1, 3):
            insert_active_node(postgres, tables, f'node{i}', created_on=base_time)

        coord_config = CoordinationConfig(total_tokens=30)
        job = create_job('node1', postgres, coordination_config=coord_config)

        try:
            before = datetime.datetime.now(datetime.timezone.utc)
            job._distribute_tokens_safe('membership_change')
            after = datetime.datetime.now(datetime.timezone.utc)

            with postgres.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT triggered_at, trigger_reason, leader_node, nodes_after, duration_ms
                    FROM {tables["Rebalance"]}
                """))
                rebalance = [dict(row._mapping) for row in result]

            assert len(rebalance) == 1, 'Rebalance should be logged once'
            assert rebalance[0]['trigger_reason'] == 'membership_change'
            assert rebalance[0]['leader_node'] == 'node1'
            assert rebalance[0]['nodes_after'] == 2
            assert before <= rebalance[0]['triggered_at'] <= after
            assert rebalance[0]['duration_ms'] > 0, 'Rebalance should have measurable duration'

        finally:
            job.__exit__(None, None, None)


class TestDatabaseTimezoneConsistency:
    """Test database timezone configuration doesn't cause issues."""

    @clean_tables('Audit')
    def test_audit_timestamps_use_database_timezone(self, postgres):
        """Verify audit created_on is the true write instant.

        Mutation: write_audit binding created_on from naive
            datetime.datetime.utcnow(), which the session reads as hours
            off.
        Oracle: real-clock readings taken around write_audit(); tasks
            queued with UTC and Tokyo timestamps.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        job = create_job('audit-tz-test', postgres, coordination_config=config)

        try:
            before = datetime.datetime.now(datetime.timezone.utc)
            job.tasks._tasks = [
                (create_task(1, 'task-1'), datetime.datetime.now(datetime.timezone.utc)),
                (create_task(2, 'task-2'), datetime.datetime.now(ZoneInfo('Asia/Tokyo'))),
            ]

            job.write_audit()
            after = datetime.datetime.now(datetime.timezone.utc)

            with postgres.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT task_id, created_on
                    FROM {tables["Audit"]}
                    WHERE node = 'audit-tz-test'
                    ORDER BY task_id
                """))
                created_on_by_task = {row[0]: row[1] for row in result}

            assert set(created_on_by_task) == {'1', '2'}, 'Should have 2 audit records'
            for task_id, created_on in created_on_by_task.items():
                assert before <= created_on <= after, f'Task {task_id} created_on {created_on} outside the write'

        finally:
            job.__exit__(None, None, None)


class TestDatetimeParameterTimezones:
    """Test Job initialization with datetimes in various zones."""

    @pytest.mark.parametrize(('label', 'datetime_factory'), [
        ('naive', lambda: datetime.datetime(2024, 3, 15, 23, 30, 0)),
        ('utc', lambda: datetime.datetime(2024, 3, 15, 0, 30, 0, tzinfo=datetime.timezone.utc)),
        ('tokyo', lambda: datetime.datetime(2024, 3, 15, 2, 0, 0, tzinfo=ZoneInfo('Asia/Tokyo'))),
        ('new_york', lambda: datetime.datetime(2024, 3, 15, 22, 0, 0, tzinfo=ZoneInfo('America/New_York'))),
    ])
    def test_datetime_with_various_timezones(self, postgres, label, datetime_factory):
        """Verify a datetime date keeps its own wall-clock day.

        Mutation: Job.__init__ converting with astimezone() (local or UTC)
            before taking .date().
        Oracle: inputs within three hours of midnight, so converting to UTC
            or New York lands on the 14th or 16th.
        """
        config = get_coordination_config()

        job = create_job(f'test-{label}', postgres, coordination_config=config, date=datetime_factory())

        try:
            assert job.tasks.date == datetime.date(2024, 3, 15), f'{label}: date should be 2024-03-15'
        finally:
            job.__exit__(None, None, None)

    @clean_tables('Audit')
    def test_datetime_timezone_preserved_in_audit(self, postgres):
        """Verify the audit row stores the job date's own wall-clock day.

        Mutation: Job.__init__ keeping the datetime, so the date column
            casts it in the America/New_York session.
        Oracle: Tokyo 10:00 is 21:00 the day before in New York; every
            case must store 2024-03-15.
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
            job = create_job(f'audit-{label}', postgres, coordination_config=config, date=dt)

            try:
                job.tasks._tasks.append((create_task(1), datetime.datetime.now(datetime.timezone.utc)))
                job.write_audit()

                with postgres.connect() as conn:
                    result = conn.execute(text(f"""
                        SELECT date
                        FROM {tables["Audit"]}
                        WHERE node = :node
                    """), {'node': f'audit-{label}'})
                    stored_date = result.scalar_one()

                assert stored_date == datetime.date(2024, 3, 15), f'{label}: stored {stored_date}'

            finally:
                job.__exit__(None, None, None)


class TestDateConsistency:
    """Test that the job date is always a plain datetime.date."""

    def test_date_none_returns_date_not_datetime(self, postgres):
        """Verify date=None gives today's date as a plain date.

        Mutation: Job.__init__ defaulting to datetime.datetime.now() with
            no .date().
        Oracle: datetime.date.today() in the test process's zone.
        """
        config = get_coordination_config()

        job = create_job('test-none', postgres, coordination_config=config, date=None)

        try:
            assert type(job.tasks.date) is datetime.date, \
                f'date=None should create date object, got {type(job.tasks.date).__name__}'
            assert job.tasks.date == datetime.date.today()
        finally:
            job.__exit__(None, None, None)

    def test_all_date_inputs_return_date_type(self, postgres):
        """Verify every accepted date input is stored as a plain date.

        Mutation: the isinstance(date, datetime.datetime) branch in
            Job.__init__ dropped, so a datetime is stored as given; or the
            type guard narrowed to datetime.datetime, so a plain date raises
            TypeError.
        Oracle: type() is datetime.date, which a datetime instance fails;
            the plain-date case must construct without raising.
        """
        config = get_coordination_config()

        test_cases = [
            ('none', None),
            ('date', datetime.date(2024, 3, 15)),
            ('datetime-naive', datetime.datetime(2024, 3, 15, 10, 0, 0)),
            ('datetime-utc', datetime.datetime(2024, 3, 15, 10, 0, 0, tzinfo=datetime.timezone.utc)),
            ('datetime-tokyo', datetime.datetime(2024, 3, 15, 10, 0, 0, tzinfo=ZoneInfo('Asia/Tokyo'))),
        ]

        for label, date_input in test_cases:
            job = create_job(f'test-{label}', postgres, coordination_config=config, date=date_input)

            try:
                assert type(job.tasks.date) is datetime.date, \
                    f'{label}: should create date object, got {type(job.tasks.date).__name__}'
            finally:
                job.__exit__(None, None, None)


if __name__ == '__main__':
    pytest.main(args=['-sx', __file__])
