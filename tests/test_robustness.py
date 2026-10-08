"""Tests for error handling, resilience, and edge cases.

Scope
-----
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
import threading
import time
import types
from collections.abc import Iterator
from typing import Any
from zoneinfo import ZoneInfo

import pytest
import sqlalchemy.exc
from fixtures import *  # noqa: F401, F403
from sqlalchemy import Engine, text

import jobsync.client
from jobsync import schema
from jobsync.client import CoordinationConfig, Job, JobState, Task
from jobsync.client import ensure_timezone_aware, retry_with_backoff

logger = logging.getLogger(__name__)


def skew_client_clock(
    monkeypatch: pytest.MonkeyPatch,
    skew: datetime.timedelta
) -> None:
    """Shift the wall clock jobsync.client reads, leaving the database clock.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Undoes the shift at test teardown.
    skew : datetime.timedelta
        Amount subtracted from every datetime.datetime.now() call made
        inside jobsync.client.
    """
    real_datetime = datetime.datetime

    class SkewedDatetime(real_datetime):

        @classmethod
        def now(cls, tz: datetime.tzinfo | None = None) -> datetime.datetime:
            return real_datetime.now(tz) - skew

    skewed_module = types.SimpleNamespace(
        datetime=SkewedDatetime,
        date=datetime.date,
        timedelta=datetime.timedelta,
        timezone=datetime.timezone)
    monkeypatch.setattr(jobsync.client, 'datetime', skewed_module)


class TestThreadShutdown:
    """Test thread shutdown behavior and responsiveness.
    """

    def test_fast_shutdown(self, postgres):
        """Verify __exit__ of a running leader returns within 3s.

        Mutation: Monitor._run sleeping time.sleep(interval) in place of
            shutdown_event.wait(interval), so __exit__ joins a sleeping
            token-refresh thread.
        Oracle: 3s bound, under the 5s token-refresh interval and the 10s
            per-thread join timeout.
        """
        config = get_coordination_config()

        node = create_job('node1', postgres, coordination_config=config)
        node.__enter__()

        assert wait_for_running_state(node, timeout_sec=5), 'Should reach running state'

        start = time.time()
        node.__exit__(None, None, None)
        elapsed = time.time() - start

        assert elapsed < 3, f'Shutdown took {elapsed:.1f}s (should be < 3s)'

    def test_all_threads_wake_on_shutdown(self, postgres):
        """Verify every leader monitor thread stops within 1s of shutdown.

        Mutation: Monitor._run sleeping time.sleep(interval) in place of
            shutdown_event.wait(interval), or DeadNodeMonitor built with a
            fresh threading.Event() in place of the job's shutdown event.
        Oracle: 1s bound, under the 2s-5s monitor intervals; all five
            monitors named below must be alive before the event is set.
        """
        config = get_coordination_config()

        node = create_job('node1', postgres, coordination_config=config)
        node.__enter__()

        try:
            assert wait_for_state(node, JobState.RUNNING_LEADER, timeout_sec=5)

            monitor_names = {
                'heartbeat',
                'health',
                'token_refresh',
                'dead_node',
                'rebalance',
                }
            assert monitor_names <= set(node._monitors), \
                f'Leader should run {monitor_names}, has {set(node._monitors)}'
            assert all(node._monitors[name].thread.is_alive() for name in monitor_names)

            node._shutdown_event.set()

            assert wait_for_shutdown(node, timeout_sec=1), \
                'All threads should stop within 1 second'

        finally:
            node.__exit__(None, None, None)

    def test_no_thread_timeout_warnings(self, postgres, caplog):
        """Verify a normal shutdown logs no thread-join timeout warning.

        Mutation: __exit__ never moving to SHUTTING_DOWN, so the shutdown
            event stays clear and every join times out with a warning.
        Oracle: the 'did not stop within timeout' warning text __exit__
            emits per stuck thread.
        """
        config = get_coordination_config()

        with caplog.at_level(logging.WARNING):
            node = create_job('node1', postgres, coordination_config=config)
            node.__enter__()

            assert wait_for_running_state(node, timeout_sec=5), \
                'Should reach running state'

            node.__exit__(None, None, None)

        timeout_warnings = [
            record.getMessage() for record in caplog.records
            if record.levelno == logging.WARNING
            and 'did not stop within timeout' in record.getMessage()
            ]

        assert timeout_warnings == [], \
            f'Should have no thread timeout warnings, found: {timeout_warnings}'

    def test_shutdown_ends_distribution_wait(self, postgres):
        """Verify wait_for_distribution returns False soon after shutdown.

        Mutation: the poll sleeping time.sleep(check_interval) in place of
            shutdown_event.wait, or the loop ignoring shutdown and raising
            TimeoutError.
        Oracle: shutdown set 0.3s into a wait with no Token rows, a 5s poll
            interval and a 10s timeout; a 2s bound sits under both.
        """
        coord_config = get_coordination_config(total_tokens=10)
        job = create_job('node1', postgres, coordination_config=coord_config)

        try:
            threading.Timer(0.3, job._shutdown_event.set).start()
            start = time.time()
            assigned = job.tokens.wait_for_distribution(
                job._shutdown_event,
                timeout_sec=10,
                check_interval=5)
            elapsed = time.time() - start

            assert assigned is False
            assert elapsed < 2, f'Wait took {elapsed:.1f}s after shutdown'
        finally:
            job.__exit__(None, None, None)

    def test_shutdown_leader_lock_wait_logs_own_message(self, postgres, caplog):
        """Verify a leader lock wait ended by shutdown is not called a timeout.

        Mutation: _try_acquire_leader_lock logging 'Leader lock acquisition
            timeout' on shutdown, or acquire_leader_lock raising 'Another
            leader is performing'.
        Oracle: shutdown set before the call, then a LockNotAcquired naming
            shutdown and no timeout warning.
        """
        coord_config = get_coordination_config(total_tokens=10)
        job = create_job('node1', postgres, coordination_config=coord_config)

        try:
            job._shutdown_event.set()
            with caplog.at_level(logging.INFO), pytest.raises(
                jobsync.client.LockNotAcquired,
                match='Shutdown ended leader lock wait for test'):
                with job.locks.acquire_leader_lock('test'):
                    pass

            messages = [record.getMessage() for record in caplog.records]
            assert not any('timeout' in message for message in messages), messages
            assert any('ended by shutdown' in message for message in messages)
        finally:
            job.__exit__(None, None, None)

    def test_shutdown_with_long_intervals(self, postgres):
        """Verify shutdown stays fast when monitor intervals are 60s.

        Mutation: Monitor._run sleeping time.sleep(interval) in place of
            shutdown_event.wait(interval).
        Oracle: 3s bound against 60s heartbeat, health and steady refresh
            intervals.
        """
        coord_config = CoordinationConfig(
            heartbeat_interval_sec=60,
            health_check_interval_sec=60,
            token_refresh_steady_interval_sec=60)

        node = create_job('node1', postgres, coordination_config=coord_config)
        node.__enter__()

        assert wait_for_running_state(node, timeout_sec=5)

        start = time.time()
        node.__exit__(None, None, None)
        elapsed = time.time() - start

        assert elapsed < 3, \
            f'Shutdown with long intervals took {elapsed:.1f}s (should be < 3s)'


class TestMonitorRepeatedFailures:
    """Test monitor behavior with repeated check() failures.
    """

    def test_heartbeat_monitor_recovers_from_transient_failures(self, postgres):
        """Verify the heartbeat resumes after five consecutive check() errors.

        Mutation: Monitor._run letting a check() exception end the loop
            (break, or no try/except), so the heartbeat thread dies.
        Oracle: a stub check that raises five times, then a
            last_heartbeat_sent newer than the value held after the fifth
            failure.
        """
        coord_config = CoordinationConfig(
            heartbeat_interval_sec=0.5,
            heartbeat_timeout_sec=10)

        job = create_job('node1', postgres, coordination_config=coord_config)
        job.__enter__()

        try:
            assert wait_for_running_state(job, timeout_sec=10)

            heartbeat_monitor = job._monitors['heartbeat']
            original_check = heartbeat_monitor.check
            failure_count = [0]

            def failing_check():
                if failure_count[0] < 5:
                    failure_count[0] += 1
                    raise RuntimeError(f'Simulated failure #{failure_count[0]}')
                original_check()

            heartbeat_monitor.check = failing_check

            assert wait_for(lambda: failure_count[0] == 5, timeout_sec=10), \
                f'Monitor should keep calling check(), got {failure_count[0]} failures'
            heartbeat_after_failures = job.cluster.last_heartbeat_sent

            assert wait_for(
                lambda: job.cluster.last_heartbeat_sent > heartbeat_after_failures,
                timeout_sec=5), \
                'Heartbeat should resume after the failures stop'
            assert heartbeat_monitor.thread.is_alive(), \
                'Monitor thread should still be alive after failures'

        finally:
            job.__exit__(None, None, None)

    def test_health_monitor_continues_after_check_failures(self, postgres):
        """Verify the health monitor keeps calling check() past three errors.

        Mutation: Monitor._run giving up after a check() exception, or
            setting the shutdown event on one.
        Oracle: a stub check that raises on calls 1-3, then two more calls
            reached with the job not shutting down.
        """
        coord_config = CoordinationConfig(
            health_check_interval_sec=0.3,
            heartbeat_timeout_sec=10)

        job = create_job('node1', postgres, coordination_config=coord_config)
        job.__enter__()

        try:
            assert wait_for_running_state(job, timeout_sec=10)

            health_monitor = job._monitors['health']
            original_check = health_monitor.check
            call_count = [0]

            def failing_check():
                call_count[0] += 1
                if call_count[0] <= 3:
                    raise RuntimeError('Simulated health check failure')
                original_check()

            health_monitor.check = failing_check

            assert wait_for(lambda: call_count[0] >= 5, timeout_sec=10), \
                f'Monitor should keep calling check() after failures, got {call_count[0]} calls'
            assert health_monitor.thread.is_alive(), 'Thread should still be alive'
            assert not job._shutdown_event.is_set(), \
                'Should not trigger shutdown from transient failures'

        finally:
            job.__exit__(None, None, None)

    def test_monitor_logs_errors_for_failures(self, postgres, caplog):
        """Verify a check() exception is logged at ERROR with the monitor name.

        Mutation: Monitor._run logging the exception at WARNING or DEBUG, or
            not logging it.
        Oracle: the stub's exception text 'Intentional test failure' inside
            an ERROR record naming 'heartbeat-node1'.
        """
        coord_config = CoordinationConfig(
            heartbeat_interval_sec=0.3,
            heartbeat_timeout_sec=10)

        with caplog.at_level(logging.ERROR):
            job = create_job('node1', postgres, coordination_config=coord_config)
            job.__enter__()

            try:
                assert wait_for_running_state(job, timeout_sec=10)

                def failing_check():
                    raise RuntimeError('Intentional test failure')

                job._monitors['heartbeat'].check = failing_check

                def monitor_error_logged():
                    return any(
                        record.levelno == logging.ERROR
                        and 'heartbeat-node1 monitor error' in record.getMessage()
                        and 'Intentional test failure' in record.getMessage()
                        for record in list(caplog.records))

                assert wait_for(monitor_error_logged, timeout_sec=5), \
                    'Should log monitor errors at ERROR'

            finally:
                job.__exit__(None, None, None)

    def test_multiple_monitors_failing_independently(self, postgres):
        """Verify three failing monitors keep running and leadership holds.

        Mutation: Monitor._run setting the shutdown event or ending its
            loop on a check() exception.
        Oracle: stub checks raising twice per monitor, then the job still in
            RUNNING_LEADER with all three threads alive.
        """
        coord_config = CoordinationConfig(
            heartbeat_interval_sec=0.3,
            health_check_interval_sec=0.3,
            dead_node_check_interval_sec=0.3,
            rebalance_check_interval_sec=0.3,
            heartbeat_timeout_sec=10)

        job = create_job('leader', postgres, coordination_config=coord_config)
        job.__enter__()

        try:
            assert wait_for_state(job, JobState.RUNNING_LEADER, timeout_sec=10)

            monitors = {
                name: job._monitors[name]
                for name in ('heartbeat', 'health', 'dead_node')
                }
            failure_counts = dict.fromkeys(monitors, 0)

            def make_failing_check(name, original_check):
                def failing_check():
                    if failure_counts[name] < 2:
                        failure_counts[name] += 1
                        raise RuntimeError(f'{name} failure')
                    original_check()
                return failing_check

            for name, monitor in monitors.items():
                monitor.check = make_failing_check(name, monitor.check)

            assert wait_for(
                lambda: all(count == 2 for count in failure_counts.values()),
                timeout_sec=10), \
                f'Each monitor should fail twice, got {failure_counts}'
            assert all(monitor.thread.is_alive() for monitor in monitors.values()), \
                'All monitors should still be running'
            assert job.state_machine.state == JobState.RUNNING_LEADER, \
                'Job should remain leader'
            assert job.am_i_healthy(), 'Job should remain healthy'

        finally:
            job.__exit__(None, None, None)


class TestDatabaseReconnection:
    """Test database reconnection after failures.
    """

    def test_reconnection_after_connection_closes(self, postgres, caplog):
        """Verify monitors survive dropped pool connections without an error.

        Mutation: pool_pre_ping=False in DatabaseContext, so the first
            monitor to check out a dead connection raises.
        Oracle: pg_terminate_backend on every idle backend, then a heartbeat
            stamped after the termination and no monitor error record; the
            token-refresh leader check logs its failure at WARNING.
        """
        coord_config = CoordinationConfig(
            heartbeat_interval_sec=0.5,
            heartbeat_timeout_sec=10)

        with caplog.at_level(logging.WARNING):
            job = create_job('node1', postgres, coordination_config=coord_config)
            job.__enter__()

            try:
                assert wait_for_running_state(job, timeout_sec=10)
                assert wait_for(lambda: job.db.engine.pool.checkedin() > 0), \
                    'Job pool should hold idle connections'

                terminate_sql = """
select pg_terminate_backend(pid)
from pg_stat_activity
where datname = current_database()
and pid <> pg_backend_pid()
and state = 'idle'
"""
                with postgres.connect() as conn:
                    terminated = conn.execute(text(terminate_sql)).fetchall()
                    conn.commit()
                terminated_at = datetime.datetime.now(datetime.timezone.utc)

                assert terminated, 'Should terminate at least one idle backend'
                assert wait_for(
                    lambda: job.cluster.last_heartbeat_sent > terminated_at,
                    timeout_sec=5), \
                    'Heartbeat should continue after its connections close'

                connection_errors = [
                    record.getMessage() for record in caplog.records
                    if 'monitor error' in record.getMessage()
                    or 'Failed to check leader status' in record.getMessage()
                    ]
                assert connection_errors == [], \
                    f'Reconnect should be silent, got {connection_errors}'

            finally:
                job.__exit__(None, None, None)


class TestRetryWithBackoff:
    """Test retry logic with exponential backoff.
    """

    def test_succeeds_on_first_attempt(self):
        """Verify a succeeding function is called once and its value returned.

        Mutation: wrapper not returning on success, so it calls again.
        Oracle: hand count of one call and the literal 'success'.
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
        """Verify two failures then success returns after 0.1s + 0.2s of delay.

        Mutation: delay = base_delay with no doubling, giving 0.2s total.
        Oracle: hand-computed 0.1 + 0.2 = 0.3s minimum.
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
        """Verify the last error is raised after exactly max_attempts calls.

        Mutation: range(1, max_attempts) in the retry loop, one attempt short.
        Oracle: hand count of 3 attempts and the third attempt's message.
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
        """Verify the delays between attempts double from base_delay.

        Mutation: 2 ** attempt in place of 2 ** (attempt - 1), giving
            0.2/0.4/0.8s, or delay = base_delay with no doubling.
        Oracle: hand-computed 0.1/0.2/0.4s for base_delay=0.1; each delay
            must sit below the next power of two, so any factor-of-2 error
            fails.
        """
        call_times = []

        @retry_with_backoff(max_attempts=5, base_delay=0.1)
        def track_timing():
            call_times.append(time.monotonic())
            if len(call_times) < 4:
                raise RuntimeError('Not yet')
            return 'done'

        track_timing()

        assert len(call_times) == 4
        delay1 = call_times[1] - call_times[0]
        delay2 = call_times[2] - call_times[1]
        delay3 = call_times[3] - call_times[2]

        assert 0.09 <= delay1 < 0.2, f'First delay should be ~0.1s, got {delay1:.3f}s'
        assert 0.18 <= delay2 < 0.4, f'Second delay should be ~0.2s, got {delay2:.3f}s'
        assert 0.36 <= delay3 < 0.8, f'Third delay should be ~0.4s, got {delay3:.3f}s'


class TestFollowerJoinDistribution:
    """Test token distribution to a follower that joins a running leader.
    """

    @clean_tables('Node', 'Token')
    def test_distribution_succeeds_with_healthy_leader(self, postgres):
        """Verify a follower joining a running leader gets a token share.

        Mutation: RebalanceMonitor not publishing membership_changed, so
            the follower's __enter__ times out waiting for tokens.
        Oracle: token ids 0-99 for total_tokens=100, split over exactly
            the two node names.
        """
        config = get_coordination_config(
            total_tokens=100,
            token_distribution_timeout_sec=15)
        tables = schema.get_table_names(config.appname)

        leader = create_job('healthy-leader', postgres, coordination_config=config)
        leader.__enter__()

        try:
            assert wait_for_state(leader, JobState.RUNNING_LEADER, timeout_sec=5)

            follower = create_job('follower', postgres, coordination_config=config)
            follower.__enter__()

            try:
                assert follower.state_machine.state == JobState.RUNNING_FOLLOWER
                assert follower.my_tokens, 'Follower should own tokens after __enter__'

                assignments = get_token_assignments(postgres, tables)
                assert set(assignments) == set(range(100)), \
                    'Every token should have an owner'
                assert set(assignments.values()) == {'healthy-leader', 'follower'}

            finally:
                follower.__exit__(None, None, None)

        finally:
            leader.__exit__(None, None, None)

    @clean_tables('Node', 'Token')
    def test_follower_gets_tokens_after_rebalance_lock_released(
        self, postgres, monkeypatch
    ):
        """Verify a join refused by a held rebalance lock gets tokens later.

        Mutation: the membership_changed handler dropping a distribution
            refused by LockNotAcquired after RebalanceMonitor has recorded
            the mismatch, so no later distribution comes.
        Oracle: docs/OPERATOR_GUIDE.md, tokens are redistributed
            automatically on membership changes; the follower must own a
            token once the outside holder releases the lock.
        """
        config = get_coordination_config(
            total_tokens=100,
            token_distribution_timeout_sec=10)
        tables = schema.get_table_names(config.appname)
        set_lock_sql = f"""
update {tables["RebalanceLock"]}
set in_progress = true, started_at = now(), started_by = 'other-node'
where singleton = 1
"""
        clear_lock_sql = f"""
update {tables["RebalanceLock"]}
set in_progress = false, started_at = null, started_by = null
where singleton = 1
"""

        def follower_owns_tokens():
            return 'late-follower' in get_token_assignments(postgres, tables).values()

        leader = create_job('lock-leader', postgres, coordination_config=config)
        leader.__enter__()
        follower = create_job('late-follower', postgres, coordination_config=config)
        enter_errors = []

        def enter_follower():
            try:
                follower.__enter__()
            except Exception as e:
                enter_errors.append(e)

        follower_thread = threading.Thread(target=enter_follower, daemon=True)

        refused = threading.Event()
        real_try_acquire = leader.locks._try_acquire_rebalance_lock

        def recording_try_acquire(started_by):
            acquired = real_try_acquire(started_by)
            if started_by == 'token_distribution' and not acquired:
                refused.set()
            return acquired

        monkeypatch.setattr(
            leader.locks,
            '_try_acquire_rebalance_lock',
            recording_try_acquire)

        try:
            assert wait_for_state(leader, JobState.RUNNING_LEADER, timeout_sec=5)

            with postgres.connect() as conn:
                conn.execute(text(set_lock_sql))
                conn.commit()

            follower_thread.start()

            assert refused.wait(timeout=10), \
                'Leader should try to distribute for the join and be refused'
            assert not follower_owns_tokens()

            with postgres.connect() as conn:
                conn.execute(text(clear_lock_sql))
                conn.commit()

            assert wait_for(follower_owns_tokens, timeout_sec=8), \
                'Follower should own tokens once the rebalance lock is released'
            follower_thread.join(timeout=5)
            assert not enter_errors, f'Follower __enter__ raised {enter_errors}'

        finally:
            follower_thread.join(timeout=25)
            follower.__exit__(None, None, None)
            leader.__exit__(None, None, None)


class TestDistributionLockContention:
    """Distributions refused or delayed by a lock held elsewhere.
    """

    def test_dead_node_tokens_move_after_rebalance_lock_released(
        self, postgres, monkeypatch
    ):
        """Verify a dead-node rebalance refused by a held lock runs later.

        Mutation: the dead_nodes_detected handler ignoring a False return
            from _distribute_tokens_safe, so the deleted node keeps its
            tokens.
        Oracle: docs/OPERATOR_GUIDE.md, tokens are redistributed
            automatically on membership changes; DeadNodeMonitor deletes the
            ghost's Node row, so the ghost must own no token once the outside
            holder releases the rebalance lock.
        """
        config = get_coordination_config(total_tokens=20)
        tables = schema.get_table_names(config.appname)
        set_lock_sql = f"""
update {tables["RebalanceLock"]}
set in_progress = true, started_at = now(), started_by = 'other-node'
where singleton = 1
"""
        clear_lock_sql = f"""
update {tables["RebalanceLock"]}
set in_progress = false, started_at = null, started_by = null
where singleton = 1
"""
        move_tokens_sql = f"""
update {tables["Token"]}
set node = 'ghost'
where token_id < 5
"""

        def ghost_token_count():
            return list(get_token_assignments(postgres, tables).values()).count('ghost')

        leader = create_job('dead-leader', postgres, coordination_config=config)
        leader.__enter__()

        refused = threading.Event()
        real_try_acquire = leader.locks._try_acquire_rebalance_lock

        def refusing_try_acquire(started_by):
            if started_by == 'token_distribution' and not refused.is_set():
                with postgres.connect() as conn:
                    conn.execute(text(set_lock_sql))
                    conn.commit()
                acquired = real_try_acquire(started_by)
                if not acquired:
                    refused.set()
                return acquired
            return real_try_acquire(started_by)

        monkeypatch.setattr(
            leader.locks,
            '_try_acquire_rebalance_lock',
            refusing_try_acquire)

        try:
            assert wait_for_state(leader, JobState.RUNNING_LEADER, timeout_sec=5)

            with postgres.connect() as conn:
                conn.execute(text(move_tokens_sql))
                conn.commit()
            insert_stale_node(postgres, tables, 'ghost')

            assert refused.wait(timeout=10), \
                'Leader should try to rebalance for the dead node and be refused'
            assert ghost_token_count() == 5

            with postgres.connect() as conn:
                conn.execute(text(clear_lock_sql))
                conn.commit()

            assert wait_for(lambda: ghost_token_count() == 0, timeout_sec=8), \
                f'Deleted ghost should own no token, owns {ghost_token_count()}'

        finally:
            leader.__exit__(None, None, None)

    def test_exit_interrupts_leader_lock_wait(self, postgres, monkeypatch):
        """Verify shutdown ends a distribution waiting on the leader lock.

        Mutation: _try_acquire_leader_lock ignoring the job's shutdown
            event, so the coordination thread waits out
            leader_lock_timeout_sec past __exit__ and then distributes.
        Oracle: __exit__ joins each monitor thread for 10s, under the 30s
            leader_lock_timeout_sec default; once __exit__ has deleted the
            node's own Node row, no distribution may run.
        """
        config = get_coordination_config(total_tokens=20)
        tables = schema.get_table_names(config.appname)

        leader = create_job('wait-leader', postgres, coordination_config=config)
        leader.__enter__()
        exited = False

        waiting = threading.Event()
        real_try_leader = leader.locks._try_acquire_leader_lock

        def recording_try_leader(operation):
            waiting.set()
            return real_try_leader(operation)

        late_distributions = []
        real_distribute = leader.tokens.distribute

        def recording_distribute(*args, **kwargs):
            if leader._shutdown_event.is_set():
                late_distributions.append(args)
            return real_distribute(*args, **kwargs)

        monkeypatch.setattr(
            leader.locks,
            '_try_acquire_leader_lock',
            recording_try_leader)
        monkeypatch.setattr(leader.tokens, 'distribute', recording_distribute)

        try:
            assert wait_for_state(leader, JobState.RUNNING_LEADER, timeout_sec=5)
            coordination = leader._monitors['coordination']

            insert_leader_lock(postgres, tables, 'other-node', 'outside-hold')
            insert_active_node(postgres, tables, 'joiner')

            assert waiting.wait(timeout=10), \
                'Leader should start a distribution for the join'

            start = time.time()
            leader.__exit__(None, None, None)
            elapsed = time.time() - start
            exited = True
            alive_after_exit = coordination.thread.is_alive()

            delete_rows(postgres, tables, 'LeaderLock', 'singleton = 1')
            coordination.thread.join(timeout=35)

            assert not late_distributions, \
                f'Distribution ran after shutdown: {len(late_distributions)} call(s)'
            assert not alive_after_exit, \
                'Coordination thread should stop within the __exit__ join'
            assert elapsed < 10, f'__exit__ took {elapsed:.1f}s'

        finally:
            if not exited:
                leader.__exit__(None, None, None)

    def test_failed_enter_stops_monitors_and_removes_node(self, postgres):
        """Verify a raising __enter__ stops its threads and deletes its Node row.

        Mutation: __enter__ re-raising without the __exit__ shutdown, so the
            heartbeat, health and coordination threads keep running and the
            Node row keeps its heartbeat.
        Oracle: PEP 343, a with statement skips __exit__ when __enter__
            raises; the TimeoutError is the raise CoordinationConfig documents
            for minimum_nodes.
        """
        config = get_coordination_config(minimum_nodes=2)
        tables = schema.get_table_names(config.appname)
        monitor_names = {'heartbeat-lonely', 'health-lonely', 'coordination'}
        node_count_sql = f"""
select count(*)
from {tables["Node"]}
where name = 'lonely'
"""

        job = create_job(
            'lonely',
            postgres,
            coordination_config=config,
            wait_on_enter=1)
        threads_before = set(threading.enumerate())

        try:
            with pytest.raises(TimeoutError, match=r'Minimum nodes \(2\) not reached'):
                with job:
                    pass

            leaked = [
                t.name for t in threading.enumerate()
                if t not in threads_before and t.name in monitor_names and t.is_alive()
                ]
            assert leaked == [], f'Threads still running after the raise: {leaked}'

            with postgres.connect() as conn:
                node_count = conn.execute(text(node_count_sql)).scalar()
            assert node_count == 0, 'Node row should be deleted after the raise'

        finally:
            job.__exit__(None, None, None)

    def test_failed_enter_raises_original_error_when_exit_fails(
        self,
        postgres,
        monkeypatch
    ):
        """Verify a raising __exit__ does not replace the __enter__ error.

        Mutation: the failure path calling self.__exit__ unguarded, so its
            RuntimeError replaces the TimeoutError.
        Oracle: the minimum_nodes TimeoutError CoordinationConfig documents,
            and a stub __exit__ that cleans up and then raises.
        """
        config = get_coordination_config(minimum_nodes=2)
        job = create_job(
            'lonely',
            postgres,
            coordination_config=config,
            wait_on_enter=1)
        real_exit = job.__exit__

        def failing_exit(*args):
            real_exit(*args)
            raise RuntimeError('cleanup failed')

        monkeypatch.setattr(job, '__exit__', failing_exit)

        try:
            with pytest.raises(TimeoutError, match=r'Minimum nodes \(2\) not reached'):
                job.__enter__()
        finally:
            real_exit(None, None, None)


class TestTimezoneAware:
    """Test timezone-aware datetime enforcement.
    """

    def test_ensure_timezone_aware_accepts_utc(self):
        """Verify an aware UTC datetime is returned as the same object.

        Mutation: the naive check inverted (tzinfo is not None), so aware
            values raise.
        Oracle: identity with the input object.
        """
        dt = datetime.datetime.now(datetime.timezone.utc)
        result = ensure_timezone_aware(dt, 'test')
        assert result is dt

    def test_ensure_timezone_aware_accepts_other_timezones(self):
        """Verify an aware non-UTC datetime is returned unconverted.

        Mutation: rejecting any tzinfo other than UTC, or returning
            dt.astimezone(UTC).
        Oracle: identity with the America/New_York input object.
        """
        dt = datetime.datetime.now(ZoneInfo('America/New_York'))
        result = ensure_timezone_aware(dt, 'test')
        assert result is dt

    def test_ensure_timezone_aware_rejects_naive(self):
        """Verify a naive datetime raises ValueError naming the argument.

        Mutation: the tzinfo is None test dropped, so naive values pass.
        Oracle: the name 'test_datetime' passed in, echoed in the message.
        """
        dt = datetime.datetime.now()

        with pytest.raises(ValueError) as exc_info:
            ensure_timezone_aware(dt, 'test_datetime')

        assert 'test_datetime' in str(exc_info.value)
        assert 'timezone-aware' in str(exc_info.value)

    def test_job_creation_requires_timezone_aware(self, postgres):
        """Verify a Job's created_on is timezone-aware.

        Mutation: Job.__init__ taking created_on from naive
            datetime.datetime.now().
        Oracle: tzinfo of the stored value.
        """
        config = get_coordination_config()

        job = create_job('test-node', postgres, coordination_config=config)

        try:
            assert job._created_on.tzinfo is not None, \
                'created_on should be timezone-aware'
        finally:
            job.__exit__(None, None, None)


class TestTokenDistributionValidation:
    """Test defensive validation in token distribution.
    """

    @clean_tables('Node', 'Token')
    def test_validates_assignment_count(self, postgres, monkeypatch):
        """Verify distribute() writes nothing past total_tokens assignments.

        Mutation: the len(new_assignments) > total_tokens guard dropped, so
            the oversized result is written.
        Oracle: stub returning token ids 0-10 for total_tokens=10, one
            past the limit.
        """
        config = get_coordination_config(total_tokens=10, heartbeat_timeout_sec=60)
        tables = schema.get_table_names(config.appname)
        insert_active_node(postgres, tables, 'node1')

        job = create_job('node1', postgres, coordination_config=config)

        try:
            oversized = dict.fromkeys(range(11), 'node1')
            monkeypatch.setattr(
                'jobsync.client.compute_minimal_move_distribution',
                lambda *args: (oversized, len(oversized)))

            with pytest.raises(ValueError, match='11 > 10'):
                job.tokens.distribute(job.locks, job.cluster)

            assert get_token_assignments(postgres, tables) == {}, \
                'Rejected distribution should write nothing'

        finally:
            job.__exit__(None, None, None)

    @clean_tables('Node', 'Token')
    def test_validates_no_assignments_to_inactive_nodes(self, postgres, monkeypatch):
        """Verify distribute() writes nothing when it assigns an inactive node.

        Mutation: the invalid_nodes guard dropped, so the token assigned to
            'ghost' is written.
        Oracle: stub assigning token 1 to 'ghost', a name absent from the
            Node table.
        """
        config = get_coordination_config(total_tokens=10, heartbeat_timeout_sec=60)
        tables = schema.get_table_names(config.appname)
        insert_active_node(postgres, tables, 'node1')

        job = create_job('node1', postgres, coordination_config=config)

        try:
            monkeypatch.setattr(
                'jobsync.client.compute_minimal_move_distribution',
                lambda *args: ({0: 'node1', 1: 'ghost'}, 2))

            with pytest.raises(ValueError, match='ghost'):
                job.tokens.distribute(job.locks, job.cluster)

            assert get_token_assignments(postgres, tables) == {}, \
                'Rejected distribution should write nothing'

        finally:
            job.__exit__(None, None, None)


class TestCorruptLockPatternHandling:
    """Test handling of corrupted lock pattern data.
    """

    @clean_tables('Lock')
    def test_handles_invalid_json_in_patterns(self, postgres):
        """Verify bad JSON pattern text is skipped and string lists decode.

        Mutation: json.JSONDecodeError dropped from get_active_locks' except
            tuple, or the json.loads of a string value removed.
        Oracle: lock 2 stored as the jsonb string '[not json'; lock 3 as the
            jsonb string '["legacy-pattern"]'.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)
        insert_lock(postgres, tables, 1, ['valid-pattern'], created_by='test')
        insert_lock(
            postgres,
            tables,
            2,
            [],
            created_by='test',
            raw_patterns=json.dumps('[not json'))
        insert_lock(
            postgres,
            tables,
            3,
            [],
            created_by='test',
            raw_patterns=json.dumps(json.dumps(['legacy-pattern'])))

        job = create_job('test', postgres, coordination_config=config)

        try:
            locks = job.locks.get_active_locks()

            assert locks == {
                job.task_to_token('1'): ['valid-pattern'],
                job.task_to_token('3'): ['legacy-pattern'],
                }

        finally:
            job.__exit__(None, None, None)

    @clean_tables('Lock')
    def test_handles_non_list_patterns(self, postgres):
        """Verify a lock whose patterns decode to a non-list is skipped.

        Mutation: the isinstance(patterns, list) check dropped from
            get_active_locks, so the dict is returned as patterns.
        Oracle: lock 2 stored as the jsonb object {"node": "node1"}.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)
        insert_lock(postgres, tables, 1, ['valid'], created_by='test')
        insert_lock(
            postgres,
            tables,
            2,
            [],
            created_by='test',
            reason='invalid - object not list',
            raw_patterns=json.dumps({'node': 'node1'}))

        job = create_job('test', postgres, coordination_config=config)

        try:
            locks = job.locks.get_active_locks()

            assert locks == {job.task_to_token('1'): ['valid']}, \
                'Invalid lock with non-list pattern should be skipped'

        finally:
            job.__exit__(None, None, None)


class TestDatabaseNOWConsistency:
    """Test that database NOW() is used for timestamps.
    """

    @clean_tables('Token', 'Node')
    def test_token_assigned_at_uses_database_now(self, postgres, monkeypatch):
        """Verify token assigned_at comes from the database clock.

        Mutation: assigned_at bound from datetime.datetime.now() in
            distribute() in place of SQL NOW().
        Oracle: client clock skewed one hour back; stored values must fall
            between real-clock readings taken around the call.
        """
        config = get_coordination_config(total_tokens=10, heartbeat_timeout_sec=60)
        tables = schema.get_table_names(config.appname)
        insert_active_node(postgres, tables, 'node1')

        job = create_job('node1', postgres, coordination_config=config)

        try:
            skew_client_clock(monkeypatch, datetime.timedelta(hours=1))

            before_distribute = datetime.datetime.now(datetime.timezone.utc)
            job.tokens.distribute(job.locks, job.cluster)
            after_distribute = datetime.datetime.now(datetime.timezone.utc)

            with postgres.connect() as conn:
                result = conn.execute(
                    text(f'select min(assigned_at), max(assigned_at) from {tables["Token"]}'))
                earliest, latest = result.one()

            assert before_distribute <= earliest <= latest <= after_distribute, \
                'assigned_at should be between distribute call times'

        finally:
            job.__exit__(None, None, None)

    @clean_tables('Lock')
    def test_lock_created_at_uses_database_now(self, postgres, monkeypatch):
        """Verify lock created_at comes from the database clock.

        Mutation: created_at bound from datetime.datetime.now() in the lock
            upsert in place of SQL NOW().
        Oracle: client clock skewed one hour back; the stored value must
            fall between real-clock readings taken around the call.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        job = create_job('node1', postgres, coordination_config=config)

        try:
            skew_client_clock(monkeypatch, datetime.timedelta(hours=1))

            before = datetime.datetime.now(datetime.timezone.utc)
            job.register_lock('task-1', 'pattern-1', 'test')
            after = datetime.datetime.now(datetime.timezone.utc)

            with postgres.connect() as conn:
                result = conn.execute(text(f'select created_at from {tables["Lock"]}'))
                created_at = result.scalar()

            assert before <= created_at <= after, \
                'created_at should be between register call times'

        finally:
            job.__exit__(None, None, None)

    @clean_tables('Node', 'Rebalance')
    def test_rebalance_triggered_at_uses_database_now(self, postgres, monkeypatch):
        """Verify rebalance triggered_at comes from the database clock.

        Mutation: triggered_at bound from datetime.datetime.now() in
            _log_rebalance in place of SQL NOW().
        Oracle: client clock skewed one hour back; the stored value must
            fall between real-clock readings taken around the call.
        """
        config = get_coordination_config(total_tokens=10, heartbeat_timeout_sec=60)
        tables = schema.get_table_names(config.appname)
        insert_active_node(postgres, tables, 'leader')

        job = create_job('leader', postgres, coordination_config=config)

        try:
            skew_client_clock(monkeypatch, datetime.timedelta(hours=1))

            before = datetime.datetime.now(datetime.timezone.utc)
            job.tokens.distribute(job.locks, job.cluster)
            after = datetime.datetime.now(datetime.timezone.utc)

            with postgres.connect() as conn:
                result = conn.execute(
                    text(f'select triggered_at from {tables["Rebalance"]}'))
                triggered_at = result.scalar_one()

            assert before <= triggered_at <= after, \
                'triggered_at should be between distribute call times'

        finally:
            job.__exit__(None, None, None)


class TestStaleTaskOwnership:
    """Test validation of task ownership at audit write time.
    """

    @clean_tables('Audit')
    def test_task_ownership_not_revalidated_at_write_time(self, postgres):
        """Verify write_audit() records queued tasks whose tokens moved.

        Mutation: write_audit filtering queued tasks by current token
            ownership.
        Oracle: three tasks queued while owning their tokens, then
            my_tokens emptied; the audit must hold all three task ids.
        """
        coord_cfg = get_coordination_config(total_tokens=10)
        tables = schema.get_table_names(coord_cfg.appname)

        job = create_job('node1', postgres, coordination_config=coord_cfg)

        try:
            task_id_by_token = {}
            for candidate_id in range(1000):
                task_id_by_token.setdefault(
                    job.task_to_token(candidate_id),
                    candidate_id)
            queued_task_ids = [task_id_by_token[token_id] for token_id in (1, 2, 3)]

            job.state_machine.state = JobState.RUNNING_FOLLOWER
            job.tokens.my_tokens = {1, 2, 3}

            for task_id in queued_task_ids:
                job.add_task(Task(task_id))

            assert len(job.tasks._tasks) == 3, \
                'Should queue all 3 tasks with valid ownership'

            job.tokens.my_tokens = set()

            job.write_audit()

            with postgres.connect() as conn:
                result = conn.execute(
                    text(f'select task_id from {tables["Audit"]} where node = :node'),
                    {'node': 'node1'})
                audited_task_ids = sorted(row[0] for row in result)

            assert audited_task_ids == sorted(
                str(task_id) for task_id in queued_task_ids), \
                'All 3 tasks written despite no longer owning tokens'

        finally:
            job.__exit__(None, None, None)


class TestLiveSessionDatabaseFailure:
    """Test that a live Job logs a database failure and returns a default.
    """

    @pytest.fixture
    def live_job(self, postgres: Engine) -> Iterator[Job]:
        """Running follower that owns every token, with no monitors started.
        """
        coord_cfg = get_coordination_config(total_tokens=10)
        job = create_job('node1', postgres, coordination_config=coord_cfg)
        job.state_machine.state = JobState.RUNNING_FOLLOWER
        job.tokens.my_tokens = set(range(10))
        try:
            yield job
        finally:
            job.__exit__(None, None, None)

    @staticmethod
    def break_database(monkeypatch: pytest.MonkeyPatch, job: Job) -> None:
        """Make each new connection on job's engine raise OperationalError.
        """
        def refuse_connect(*args: Any, **kwargs: Any) -> None:
            raise sqlalchemy.exc.OperationalError(
                'select 1', {}, Exception('server closed the connection'))
        monkeypatch.setattr(job.db.engine, 'connect', refuse_connect)

    @staticmethod
    def warnings_logged(caplog: pytest.LogCaptureFixture) -> list[str]:
        """Messages of the WARNING records caplog holds.
        """
        return [
            record.getMessage() for record in caplog.records
            if record.levelno == logging.WARNING
            ]

    def test_add_task_skips_audit_when_claim_fails(
        self,
        live_job,
        monkeypatch,
        caplog
    ):
        """Verify add_task logs a failed claim and queues no audit entry.

        Mutation: add_task queuing the task before set_claim, or letting the
            claim error raise.
        Oracle: an engine whose connect raises, then an empty audit queue
            and a warning naming task 7.
        """
        with monkeypatch.context() as patch, caplog.at_level(logging.WARNING):
            self.break_database(patch, live_job)
            live_job.add_task(Task(7))

        assert live_job.tasks._tasks == [], 'Unclaimed task must not be queued'
        assert any('7' in message for message in self.warnings_logged(caplog))

    def test_set_claim_logs_failure(self, live_job, monkeypatch, caplog):
        """Verify set_claim logs a database error and returns.

        Mutation: Job.set_claim letting the claim error raise.
        Oracle: an engine whose connect raises, then a warning.
        """
        with monkeypatch.context() as patch, caplog.at_level(logging.WARNING):
            self.break_database(patch, live_job)
            live_job.set_claim(Task(7))

        assert self.warnings_logged(caplog)

    @clean_tables('Audit')
    def test_write_audit_keeps_queue_on_failure(
        self,
        live_job,
        postgres,
        monkeypatch,
        caplog
    ):
        """Verify a failed write_audit keeps its queue for the next call.

        Mutation: Job.write_audit letting the error raise, or the queue
            cleared before the insert commits.
        Oracle: a failed write, then a write on a working engine that
            stores task 7, queued before the failure.
        """
        live_job.add_task(Task(7))

        with monkeypatch.context() as patch, caplog.at_level(logging.WARNING):
            self.break_database(patch, live_job)
            live_job.write_audit()

        assert self.warnings_logged(caplog)
        live_job.write_audit()
        assert [row['task_id'] for row in live_job.get_audit()] == ['7']

    @pytest.mark.parametrize('call_name', ['get_audit', 'get_active_nodes'])
    def test_read_returns_empty_list_on_failure(
        self,
        live_job,
        monkeypatch,
        caplog,
        call_name
    ):
        """Verify a read logs a database error and returns [].

        Mutation: the read letting the error raise, or returning None.
        Oracle: an engine whose connect raises, then [] and a warning.
        """
        with monkeypatch.context() as patch, caplog.at_level(logging.WARNING):
            self.break_database(patch, live_job)
            result = getattr(live_job, call_name)()

        assert result == []
        assert self.warnings_logged(caplog)

    @clean_tables('Lock')
    @pytest.mark.parametrize(('call_name', 'args', 'break_db'), [
        ('register_lock', (7, 'node%'), True),
        ('register_lock', (7, [object()]), False),
        ('register_locks_bulk', ([(7, 'node%', 'ok'), (8, 'node%')],), False),
        ('register_locks_bulk', ([(7, 'node%', 'ok')],), True),
        ])
    def test_register_lock_stores_nothing_on_failure(
        self,
        live_job,
        postgres,
        monkeypatch,
        caplog,
        call_name,
        args,
        break_db
    ):
        """Verify lock registration logs a failure and stores no lock.

        Mutation: the registration letting a database error, a JSON
            encoding error or a short bulk tuple raise, or storing the
            valid entries of a bulk call that holds a bad one.
        Oracle: an engine whose connect raises, patterns holding an
            object(), or a bulk list whose second tuple has two items; then
            an empty lock list and a warning.
        """
        with monkeypatch.context() as patch, caplog.at_level(logging.WARNING):
            if break_db:
                self.break_database(patch, live_job)
            getattr(live_job, call_name)(*args)

        assert live_job.list_locks() == []
        assert self.warnings_logged(caplog)

    def test_lock_provider_error_fails_startup(self, postgres):
        """Verify a bad lock_provider entry still makes __enter__ raise.

        Mutation: the register_locks_bulk wrapper logging the error during
            startup too, so the job starts with its locks missing.
        Oracle: a bulk tuple with two items, whose unpacking raises
            ValueError inside lock_provider.
        """
        coord_cfg = get_coordination_config(total_tokens=10)
        job = create_job(
            'node1',
            postgres,
            coordination_config=coord_cfg,
            lock_provider=lambda job: job.register_locks_bulk([(7, 'node%')]))

        try:
            with pytest.raises(ValueError):
                job.__enter__()
        finally:
            job.__exit__(None, None, None)


if __name__ == '__main__':
    pytest.main(args=['-sx', __file__])
