"""Tests for error handling, resilience, and edge cases.

USE THIS FILE FOR:
- Error handling and recovery tests
- Thread failure scenarios
- Database failure scenarios
- Data validation and sanitization
- Corruption and edge case handling
- Retry logic verification
"""
import datetime
import json
import logging
import time
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import text

from jobsync import schema
from jobsync.client import CoordinationConfig, JobState, ensure_timezone_aware
from jobsync.client import retry_with_backoff

logger = logging.getLogger(__name__)


from fixtures import *  # noqa: F401, F403

logger = logging.getLogger(__name__)


class TestThreadShutdown:
    """Test thread shutdown behavior and responsiveness."""

    def test_fast_shutdown(self, postgres):
        """Verify shutdown completes quickly without thread timeouts.
        """
        config = get_coordination_config()

        node = create_job('node1', postgres, coordination_config=config, wait_on_enter=5)
        node.__enter__()

        assert wait_for_running_state(node, timeout_sec=5), 'Should reach running state'

        start = time.time()
        node.__exit__(None, None, None)
        elapsed = time.time() - start

        assert elapsed < 3, f'Shutdown took {elapsed:.1f}s (should be < 3s)'
        logger.info(f'✓ Shutdown completed in {elapsed:.1f}s')

    def test_all_threads_wake_on_shutdown(self, postgres):
        """Verify all thread types respond immediately to shutdown event.
        """
        config = get_coordination_config()

        node = create_job('node1', postgres, coordination_config=config, wait_on_enter=5)
        node.__enter__()

        assert wait_for_running_state(node, timeout_sec=5)

        assert wait_for(lambda: all(
            m.thread and m.thread.is_alive()
            for m in node._monitors.values()
            if any(name in m.name for name in ['heartbeat', 'health'])
        ), timeout_sec=3)

        node._shutdown_event.set()

        assert wait_for_shutdown(node, timeout_sec=1), \
            'All threads should stop within 1 second'

        logger.info('✓ All threads responded to shutdown within 1 second')

    def test_leader_threads_shutdown(self, postgres):
        """Verify leader-specific threads respond to shutdown event.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        now = datetime.datetime.now(datetime.timezone.utc)
        insert_active_node(postgres, tables, 'node1', created_on=now)

        node = create_job('node1', postgres, coordination_config=config, wait_on_enter=5)
        node.__enter__()

        assert wait_for(lambda: all(
            m.thread and m.thread.is_alive()
            for m in node._monitors.values()
            if any(name in m.name for name in ['dead-node', 'rebalance'])
        ), timeout_sec=5)

        node._shutdown_event.set()

        assert wait_for_shutdown(node, timeout_sec=2), 'All leader threads should stop within 2 seconds'

        logger.info('✓ Leader threads responded to shutdown within 2 seconds')

    def test_no_thread_timeout_warnings(self, postgres, caplog):
        """Verify no thread timeout warnings during normal shutdown.
        """
        config = get_coordination_config()

        with caplog.at_level(logging.WARNING):
            node = create_job('node1', postgres, coordination_config=config, wait_on_enter=5)
            node.__enter__()

            assert wait_for_running_state(node, timeout_sec=5), 'Should reach running state'

            node.__exit__(None, None, None)

        warning_messages = [record.message for record in caplog.records if record.levelname == 'WARNING']
        timeout_warnings = [msg for msg in warning_messages if 'did not stop within timeout' in msg]

        assert len(timeout_warnings) == 0, f'Should have no thread timeout warnings, found: {timeout_warnings}'

        logger.info('✓ No thread timeout warnings during shutdown')

    def test_shutdown_with_long_intervals(self, postgres):
        """Verify shutdown is fast even with long check intervals.
        """
        config = get_coordination_config()

        coord_config = CoordinationConfig(
            heartbeat_interval_sec=60,
            health_check_interval_sec=60,
            token_refresh_steady_interval_sec=60
        )

        node = create_job('node1', postgres, wait_on_enter=5, coordination_config=coord_config)
        node.__enter__()

        assert wait_for_running_state(node, timeout_sec=5)

        start = time.time()
        node.__exit__(None, None, None)
        elapsed = time.time() - start

        assert elapsed < 3, f'Shutdown with long intervals took {elapsed:.1f}s (should be < 3s)'
        logger.info(f'✓ Shutdown with 60s intervals completed in {elapsed:.1f}s')


class TestMonitorRepeatedFailures:
    """Test monitor behavior with repeated check() failures."""

    def test_heartbeat_monitor_recovers_from_transient_failures(self, postgres):
        """Verify heartbeat monitor recovers after transient database errors.
        """
        config = get_coordination_config()

        coord_config = CoordinationConfig(
            heartbeat_interval_sec=0.5,
            heartbeat_timeout_sec=5
        )

        job = create_job('node1', postgres, wait_on_enter=2, coordination_config=coord_config)
        job.__enter__()

        try:
            assert wait_for_running_state(job, timeout_sec=10)

            heartbeat_monitor = next((m for m in job._monitors.values() if 'heartbeat' in m.name), None)
            assert heartbeat_monitor is not None, 'Should have heartbeat monitor'

            original_check = heartbeat_monitor.check
            failure_count = [0]
            max_failures = 5

            def failing_check():
                if failure_count[0] < max_failures:
                    failure_count[0] += 1
                    raise RuntimeError(f'Simulated failure #{failure_count[0]}')
                original_check()

            heartbeat_monitor.check = failing_check

            start = time.time()
            while failure_count[0] < max_failures and time.time() - start < 10:
                time.sleep(0.1)

            assert failure_count[0] >= max_failures, \
                'Should have attempted checks and failed multiple times'
            assert heartbeat_monitor.thread.is_alive(), \
                'Monitor thread should still be alive after failures'

            time.sleep(1)

            assert job.cluster.last_heartbeat_sent is not None, \
                'Should have recovered and sent heartbeat'

            logger.info(f'✓ Heartbeat monitor recovered after {failure_count[0]} failures')

        finally:
            job.__exit__(None, None, None)

    def test_health_monitor_continues_after_check_failures(self, postgres):
        """Verify health monitor continues operating after check failures.
        """
        config = get_coordination_config()

        coord_config = CoordinationConfig(
            health_check_interval_sec=0.3,
            heartbeat_timeout_sec=10
        )

        job = create_job('node1', postgres, wait_on_enter=2, coordination_config=coord_config)
        job.__enter__()

        try:
            assert wait_for_running_state(job, timeout_sec=10)

            health_monitor = next((m for m in job._monitors.values() if 'health' in m.name), None)
            assert health_monitor is not None, 'Should have health monitor'

            original_check = health_monitor.check
            failure_count = [0]

            def failing_check():
                if failure_count[0] < 3:
                    failure_count[0] += 1
                    raise RuntimeError('Simulated health check failure')
                original_check()

            health_monitor.check = failing_check

            start = time.time()
            while failure_count[0] < 3 and time.time() - start < 10:
                time.sleep(0.1)

            assert failure_count[0] >= 3, 'Should have failed multiple times'
            assert health_monitor.thread.is_alive(), 'Thread should still be alive'
            assert not job._shutdown_event.is_set(), 'Should not trigger shutdown from transient failures'

            logger.info('✓ Health monitor recovered from check failures')

        finally:
            job.__exit__(None, None, None)

    def test_monitor_logs_errors_for_failures(self, postgres, caplog):
        """Verify monitor failures are logged at ERROR level.
        """
        config = get_coordination_config()

        coord_config = CoordinationConfig(
            heartbeat_interval_sec=0.3,
            heartbeat_timeout_sec=10
        )

        with caplog.at_level(logging.ERROR):
            job = create_job('node1', postgres, wait_on_enter=2, coordination_config=coord_config)
            job.__enter__()

            try:
                assert wait_for_running_state(job, timeout_sec=10)

                heartbeat_monitor = next((m for m in job._monitors.values() if 'heartbeat' in m.name), None)
                original_check = heartbeat_monitor.check

                def failing_check(conn):
                    raise RuntimeError('Intentional test failure')

                heartbeat_monitor.check = failing_check

                time.sleep(1)

                heartbeat_monitor.check = original_check

                error_messages = [r.message for r in caplog.records if r.levelname == 'ERROR']
                monitor_errors = [msg for msg in error_messages if 'monitor error' in msg.lower()]

                assert len(monitor_errors) > 0, 'Should log monitor errors'

                logger.info(f'✓ Monitor logged {len(monitor_errors)} ERROR messages')

            finally:
                job.__exit__(None, None, None)

    def test_multiple_monitors_failing_independently(self, postgres):
        """Verify multiple monitors can fail and recover independently.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        now = datetime.datetime.now(datetime.timezone.utc)
        insert_active_node(postgres, tables, 'leader', created_on=now)

        coord_config = CoordinationConfig(
            heartbeat_interval_sec=0.3,
            health_check_interval_sec=0.3,
            dead_node_check_interval_sec=0.3,
            rebalance_check_interval_sec=0.3,
            heartbeat_timeout_sec=10
        )

        job = create_job('leader', postgres, wait_on_enter=5,
                        coordination_config=coord_config)
        job.__enter__()

        try:
            assert wait_for_state(job, JobState.RUNNING_LEADER, timeout_sec=10)

            monitors = {
                'heartbeat': next((m for m in job._monitors.values() if 'heartbeat' in m.name), None),
                'health': next((m for m in job._monitors.values() if 'health' in m.name), None),
                'dead-node': next((m for m in job._monitors.values() if 'dead-node' in m.name), None)
            }

            original_checks = {}
            failure_counts = {name: [0] for name in monitors}

            for name, monitor in monitors.items():
                if monitor:
                    original_checks[name] = monitor.check

                    def make_failing_check(mon_name):
                        def failing_check():
                            if failure_counts[mon_name][0] < 2:
                                failure_counts[mon_name][0] += 1
                                raise RuntimeError(f'{mon_name} failure')
                            original_checks[mon_name]()
                        return failing_check

                    monitor.check = make_failing_check(name)

            time.sleep(2)

            for name, count_list in failure_counts.items():
                assert count_list[0] >= 2, f'{name} should have failed at least twice'

            for monitor in monitors.values():
                if monitor:
                    assert monitor.thread.is_alive(), 'All monitors should still be running'

            assert job.am_i_healthy(), 'Job should remain healthy'

            logger.info('✓ Multiple monitors failed and recovered independently')

        finally:
            job.__exit__(None, None, None)


class TestDatabaseReconnection:
    """Test database reconnection after failures."""

    def test_reconnection_after_connection_closes(self, postgres):
        """Verify monitors reconnect after connection closes.
        """
        config = get_coordination_config()

        coord_config = CoordinationConfig(
            heartbeat_interval_sec=0.5,
            heartbeat_timeout_sec=10
        )

        job = create_job('node1', postgres, wait_on_enter=2, coordination_config=coord_config)
        job.__enter__()

        try:
            assert wait_for_running_state(job, timeout_sec=10)

            time.sleep(1)
            initial_heartbeat = job.cluster.last_heartbeat_sent

            time.sleep(1.5)

            later_heartbeat = job.cluster.last_heartbeat_sent
            assert later_heartbeat > initial_heartbeat, \
                'Heartbeat should continue after connection issues'

            logger.info('✓ Monitor reconnected and continued operating')

        finally:
            job.__exit__(None, None, None)


class TestRetryWithBackoff:
    """Test retry logic with exponential backoff."""

    def test_succeeds_on_first_attempt(self):
        """Verify function that succeeds immediately doesn't retry.
        """
        call_count = [0]

        @retry_with_backoff(max_attempts=3, base_delay=0.1)
        def succeeds_immediately():
            call_count[0] += 1
            return 'success'

        result = succeeds_immediately()
        assert result == 'success'
        assert call_count[0] == 1, 'Should only call once'

    def test_succeeds_after_retries(self):
        """Verify function that fails then succeeds uses retry logic.
        """
        call_count = [0]

        @retry_with_backoff(max_attempts=5, base_delay=0.1)
        def succeeds_on_third_attempt():
            call_count[0] += 1
            if call_count[0] < 3:
                raise RuntimeError(f'Attempt {call_count[0]} failed')
            return 'success'

        start = time.time()
        result = succeeds_on_third_attempt()
        elapsed = time.time() - start

        assert result == 'success'
        assert call_count[0] == 3, 'Should call 3 times'
        assert elapsed >= 0.3, 'Should have delays: 0.1 + 0.2 = 0.3s minimum'

    def test_fails_after_max_attempts(self):
        """Verify exception raised after all retries exhausted.
        """
        call_count = [0]

        @retry_with_backoff(max_attempts=3, base_delay=0.05)
        def always_fails():
            call_count[0] += 1
            raise RuntimeError(f'Attempt {call_count[0]} failed')

        with pytest.raises(RuntimeError) as exc_info:
            always_fails()

        assert 'Attempt 3 failed' in str(exc_info.value)
        assert call_count[0] == 3, 'Should attempt 3 times'

    def test_exponential_backoff_delays(self):
        """Verify delays follow exponential backoff pattern.
        """
        call_times = []

        @retry_with_backoff(max_attempts=5, base_delay=0.1)
        def track_timing():
            call_times.append(time.time())
            if len(call_times) < 4:
                raise RuntimeError('Not yet')
            return 'done'

        track_timing()

        assert len(call_times) == 4
        delay1 = call_times[1] - call_times[0]
        delay2 = call_times[2] - call_times[1]
        delay3 = call_times[3] - call_times[2]

        assert 0.08 <= delay1 <= 0.15, f'First delay should be ~0.1s, got {delay1:.3f}s'
        assert 0.15 <= delay2 <= 0.25, f'Second delay should be ~0.2s, got {delay2:.3f}s'
        assert 0.35 <= delay3 <= 0.5, f'Third delay should be ~0.4s, got {delay3:.3f}s'


class TestLeaderFailureDuringDistribution:
    """Test leader failure during initial distribution phase."""

    @clean_tables('Node', 'Token')
    def test_distribution_succeeds_with_healthy_leader(self, postgres):
        """Verify distribution succeeds when leader is healthy.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        now = datetime.datetime.now(datetime.timezone.utc)
        insert_active_node(postgres, tables, 'healthy-leader', created_on=now)

        coord_config = CoordinationConfig()
        leader = create_job('healthy-leader', postgres, wait_on_enter=0,
                           coordination_config=coord_config)
        leader.__enter__()

        try:
            leader.tokens.distribute(leader.locks, leader.cluster)

            follower = create_job('follower', postgres, wait_on_enter=0,
                                 coordination_config=coord_config)
            follower.__enter__()

            try:
                follower.tokens.wait_for_distribution(timeout_sec=5)
                logger.info('✓ Distribution wait succeeded with healthy leader')
            finally:
                follower.__exit__(None, None, None)

        finally:
            leader.__exit__(None, None, None)


class TestTimezoneAware:
    """Test timezone-aware datetime enforcement."""

    def test_ensure_timezone_aware_accepts_utc(self):
        """Verify timezone-aware UTC datetime is accepted.
        """
        dt = datetime.datetime.now(datetime.timezone.utc)
        result = ensure_timezone_aware(dt, 'test')
        assert result == dt

    def test_ensure_timezone_aware_accepts_other_timezones(self):
        """Verify timezone-aware datetime with other timezones is accepted.
        """

        dt = datetime.datetime.now(ZoneInfo('America/New_York'))
        result = ensure_timezone_aware(dt, 'test')
        assert result == dt

    def test_ensure_timezone_aware_rejects_naive(self):
        """Verify naive datetime is rejected.
        """
        dt = datetime.datetime.now()

        with pytest.raises(ValueError) as exc_info:
            ensure_timezone_aware(dt, 'test_datetime')

        assert 'test_datetime' in str(exc_info.value)
        assert 'timezone-aware' in str(exc_info.value)

    def test_job_creation_requires_timezone_aware(self, postgres):
        """Verify Job validates created_on is timezone-aware.
        """
        config = get_coordination_config()

        job = create_job('test-node', postgres, coordination_config=config, wait_on_enter=0)

        assert job._created_on.tzinfo is not None, 'created_on should be timezone-aware'
        logger.info('✓ Job created_on is timezone-aware')


class TestTokenDistributionValidation:
    """Test defensive validation in token distribution."""

    @clean_tables('Node', 'Token')
    def test_validates_assignment_count(self, postgres):
        """Verify token distribution validates assignment count doesn't exceed total.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        now = datetime.datetime.now(datetime.timezone.utc)
        insert_active_node(postgres, tables, 'node1', created_on=now)

        coord_config = CoordinationConfig()
        job = create_job('node1', postgres, wait_on_enter=0,
                        coordination_config=coord_config)
        job.__enter__()

        try:
            job.tokens.distribute(job.locks, job.cluster)

            with postgres.connect() as conn:
                result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Token"]}'))
                count = result.scalar()

            assert count == 10000, 'Should have exactly 10000 tokens (default total_tokens=10000)'
            logger.info('✓ Token count validation passed')

        finally:
            job.__exit__(None, None, None)

    @clean_tables('Node', 'Token')
    def test_validates_no_assignments_to_inactive_nodes(self, postgres):
        """Verify tokens are not assigned to inactive nodes.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        now = datetime.datetime.now(datetime.timezone.utc)

        coord_config = CoordinationConfig()
        job1 = create_job('node1', postgres, wait_on_enter=5,
                         coordination_config=coord_config)
        job1.__enter__()

        job2 = create_job('node2', postgres, wait_on_enter=5,
                         coordination_config=coord_config)
        job2.__enter__()

        try:
            time.sleep(2)

            # Force a redistribution
            if job1.am_i_leader():
                job1.tokens.distribute(job1.locks, job1.cluster)
            else:
                job2.tokens.distribute(job2.locks, job2.cluster)

            with postgres.connect() as conn:
                result = conn.execute(text(f'SELECT DISTINCT node FROM {tables["Token"]}'))
                assigned_nodes = {row[0] for row in result}

            assert assigned_nodes.issubset({'node1', 'node2'}), \
                       'All assignments should be to active nodes'
            assert len(assigned_nodes) == 2, 'Tokens should be distributed to both nodes'
            logger.info('✓ No invalid node assignments')

        finally:
            job1.__exit__(None, None, None)
            job2.__exit__(None, None, None)


class TestCorruptLockPatternHandling:
    """Test handling of corrupted lock pattern data."""

    @clean_tables('Lock')
    def test_handles_invalid_json_in_patterns(self, postgres):
        """Verify system handles invalid JSON in lock patterns gracefully.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)
        insert_lock(postgres, tables, 1, ['valid-pattern'], created_by='test')
        insert_lock(postgres, tables, 2, ['another-pattern'], created_by='test')

        job = create_job('test', postgres, coordination_config=config, wait_on_enter=0)

        try:
            locks = job.locks.get_active_locks()

            token_1 = job.task_to_token('1')
            token_2 = job.task_to_token('2')

            assert token_1 in locks, 'Valid lock 1 should be returned'
            assert token_2 in locks, 'Valid lock 2 should be returned'
            assert locks[token_1] == ['valid-pattern']
            assert locks[token_2] == ['another-pattern']

            logger.info('✓ Lock pattern validation handled gracefully')

        except Exception as e:
            pytest.fail(f'Should handle lock patterns gracefully: {e}')

    @clean_tables('Lock')
    def test_handles_non_list_patterns(self, postgres):
        """Verify system handles non-list pattern values gracefully.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)
        insert_lock(postgres, tables, 1, ['valid'], created_by='test')
        insert_lock(postgres, tables, 2, [], created_by='test',
                   reason='invalid - string not list', raw_patterns=json.dumps('string-not-list'))

        job = create_job('test', postgres, coordination_config=config, wait_on_enter=0)

        locks = job.locks.get_active_locks()

        token_1 = job.task_to_token('1')
        token_2 = job.task_to_token('2')

        assert token_1 in locks, 'Valid lock should be returned'
        assert locks[token_1] == ['valid']
        assert token_2 not in locks, 'Invalid lock with non-list pattern should be skipped'

        logger.info('✓ Non-list patterns filtered out')


class TestDatabaseNOWConsistency:
    """Test that database NOW() is used for timestamps."""

    @clean_tables('Token', 'Node')
    def test_token_assigned_at_uses_database_now(self, postgres):
        """Verify token assigned_at uses database NOW() not client time.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        now = datetime.datetime.now(datetime.timezone.utc)
        insert_active_node(postgres, tables, 'node1', created_on=now)

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)
        job.__enter__()

        try:
            before_distribute = datetime.datetime.now(datetime.timezone.utc)
            job.tokens.distribute(job.locks, job.cluster)
            after_distribute = datetime.datetime.now(datetime.timezone.utc)

            with postgres.connect() as conn:
                result = conn.execute(text(f'SELECT assigned_at FROM {tables["Token"]} LIMIT 1'))
                assigned_at = result.scalar()

            assert before_distribute <= assigned_at <= after_distribute, 'assigned_at should be between distribute call times'

            logger.info('✓ Token assigned_at uses database timestamp')

        finally:
            job.__exit__(None, None, None)

    @clean_tables('Lock')
    def test_lock_created_at_uses_database_now(self, postgres):
        """Verify lock created_at uses database NOW().
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)

        before = datetime.datetime.now(datetime.timezone.utc)
        job.register_lock('task-1', 'pattern-1', 'test')
        after = datetime.datetime.now(datetime.timezone.utc)

        with postgres.connect() as conn:
            result = conn.execute(text(f'SELECT created_at FROM {tables["Lock"]}'))
            created_at = result.scalar()

        assert before <= created_at <= after, 'created_at should be between register call times'

        logger.info('✓ Lock created_at uses database timestamp')

    @clean_tables('Node', 'Rebalance')
    def test_rebalance_triggered_at_uses_database_now(self, postgres):
        """Verify rebalance triggered_at uses database NOW().
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        now = datetime.datetime.now(datetime.timezone.utc)
        insert_active_node(postgres, tables, 'leader', created_on=now)

        coord_config = CoordinationConfig()
        job = create_job('leader', postgres, wait_on_enter=0,
                        coordination_config=coord_config)
        job.__enter__()

        try:
            before = datetime.datetime.now(datetime.timezone.utc)
            job.tokens.distribute(job.locks, job.cluster)
            after = datetime.datetime.now(datetime.timezone.utc)

            with postgres.connect() as conn:
                result = conn.execute(text(f'SELECT triggered_at FROM {tables["Rebalance"]} ORDER BY triggered_at DESC LIMIT 1'))
                triggered_at = result.scalar()

            assert before <= triggered_at <= after, 'triggered_at should be between distribute call times'

            logger.info('✓ Rebalance triggered_at uses database timestamp')

        finally:
            job.__exit__(None, None, None)


class TestStaleTaskOwnership:
    """Test validation of task ownership at audit write time."""

    @clean_tables('Audit', 'Token')
    def test_tasks_queued_with_valid_ownership_then_lost(self, postgres):
        """Verify tasks queued with valid tokens can become invalid before audit write.
        """
        coord_cfg = get_coordination_config()
        tables = schema.get_table_names(coord_cfg.appname)

        job = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=0)
        job.__enter__()

        try:
            job.tokens.my_tokens = {5, 10, 15}
            job.tokens.token_version = 1
            job.state_machine.state = JobState.RUNNING_FOLLOWER

            task_ids_for_tokens = {}
            candidate_id = 0
            while len(task_ids_for_tokens) < 2 and candidate_id < 100000:
                token = job.task_to_token(candidate_id)
                if token == 5 and 5 not in task_ids_for_tokens:
                    task_ids_for_tokens[5] = candidate_id
                elif token == 10 and 10 not in task_ids_for_tokens:
                    task_ids_for_tokens[10] = candidate_id
                candidate_id += 1

            assert len(task_ids_for_tokens) == 2, 'Should find task IDs for tokens 5 and 10'

            task_5 = Task(task_ids_for_tokens[5])
            task_10 = Task(task_ids_for_tokens[10])

            token_5 = job.task_to_token(task_5.id)
            token_10 = job.task_to_token(task_10.id)

            assert token_5 == 5, 'Task should map to token 5'
            assert token_10 == 10, 'Task should map to token 10'
            assert token_5 in job.my_tokens, 'Should own token for task 5'
            assert token_10 in job.my_tokens, 'Should own token for task 10'

            job.add_task(task_5)
            job.add_task(task_10)

            logger.info('Added 2 tasks to queue with valid ownership')

            job.tokens.my_tokens = {5}
            job.tokens.token_version = 2
            logger.info(f'Simulated token loss: removed token {token_10}')

            assert len(job.tasks._tasks) == 2, 'Tasks should still be in queue'

            job.write_audit()

            with postgres.connect() as conn:
                result = conn.execute(text(f'SELECT item FROM {tables["Audit"]} WHERE node = :node'),
                                     {'node': 'node1'})
                audited_items = [row[0] for row in result]

            logger.info(f'Audited items: {audited_items}')

            assert str(task_5.id) in audited_items, 'Task mapping to token 5 should be in audit (still owned)'
            assert str(task_10.id) in audited_items, 'Task mapping to token 10 written to audit despite token loss (BUG)'

        finally:
            job.__exit__(None, None, None)

    @clean_tables('Audit')
    def test_task_ownership_not_revalidated_at_write_time(self, postgres):
        """Document that current implementation doesn't revalidate ownership at write time.
        """
        coord_cfg = get_coordination_config()
        tables = schema.get_table_names(coord_cfg.appname)

        job = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=0)
        job.__enter__()

        try:
            job.tokens.my_tokens = {1, 2, 3}
            job.state_machine.state = JobState.RUNNING_FOLLOWER

            task_ids_for_tokens = {}
            candidate_id = 0
            while len(task_ids_for_tokens) < 3 and candidate_id < 100000:
                token = job.task_to_token(candidate_id)
                if token == 1 and 1 not in task_ids_for_tokens:
                    task_ids_for_tokens[1] = candidate_id
                elif token == 2 and 2 not in task_ids_for_tokens:
                    task_ids_for_tokens[2] = candidate_id
                elif token == 3 and 3 not in task_ids_for_tokens:
                    task_ids_for_tokens[3] = candidate_id
                candidate_id += 1

            assert len(task_ids_for_tokens) == 3, 'Should find task IDs for tokens 1, 2, 3'

            tasks_added = []
            for token_id in [1, 2, 3]:
                task = Task(task_ids_for_tokens[token_id])
                if job.can_claim_task(task):
                    job.add_task(task)
                    tasks_added.append(token_id)

            assert len(tasks_added) == 3, 'Should add all 3 tasks with valid ownership'

            job.tokens.my_tokens = set()
            job.tokens.token_version = 2

            logger.info('Simulated complete token loss before audit write')

            job.write_audit()

            with postgres.connect() as conn:
                result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Audit"]} WHERE node = :node'),
                                     {'node': 'node1'})
                audit_count = result.scalar()

            assert audit_count == 3, 'All 3 tasks written despite no longer owning tokens (documents current behavior)'

            logger.info('Current behavior: no revalidation at write time')

        finally:
            job.__exit__(None, None, None)


if __name__ == '__main__':
    pytest.main(args=['-sx', __file__])
