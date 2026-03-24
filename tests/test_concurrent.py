"""Tests for concurrent operations, race conditions, and thread safety.

USE THIS FILE FOR:
- Concurrency and threading tests
- Race condition scenarios
- Thread safety verification
- Monitor interaction tests
- Simultaneous operation tests
"""
import datetime
import threading
import time

import pytest
from fixtures import *  # noqa: F401, F403
from sqlalchemy import text

from jobsync import schema
from jobsync.client import CoordinationConfig, JobState, Task


class TestMonitorRaceConditions:
    """Test race conditions between monitoring threads."""

    @clean_tables('Node')
    def test_dead_node_detected_during_token_refresh(self, postgres):
        """Verify dead node detection works when token refresh is checking.
        """
        base_config = get_coordination_config()
        tables = schema.get_table_names(base_config.appname)

        now = datetime.datetime.now(datetime.timezone.utc)
        insert_active_node(postgres, tables, 'leader', created_on=now)
        insert_active_node(postgres, tables, 'node2', created_on=now + datetime.timedelta(seconds=1))
        insert_stale_node(postgres, tables, 'stale-node', heartbeat_age_seconds=30)

        coord_config = CoordinationConfig(
            total_tokens=30,
            dead_node_check_interval_sec=0.5,
            token_refresh_initial_interval_sec=0.3,
            heartbeat_timeout_sec=10
        )

        job = create_job('leader', postgres, coordination_config=coord_config, wait_on_enter=5)
        job.__enter__()

        try:
            assert wait_for_state(job, JobState.RUNNING_LEADER, timeout_sec=10)

            assert wait_for_dead_node_removal(postgres, tables, 'stale-node', timeout_sec=10)

            assert job.am_i_healthy(), 'Leader should remain healthy'

        finally:
            job.__exit__(None, None, None)


class TestPromotionDemotionRaceConditions:
    """Test race conditions during leader promotion/demotion."""

    @clean_tables('Node')
    def test_promotion_while_monitoring_thread_running(self, postgres):
        """Verify promotion works while monitoring threads are active.
        """
        base_config = get_coordination_config()
        tables = schema.get_table_names(base_config.appname)

        now = datetime.datetime.now(datetime.timezone.utc)
        insert_active_node(postgres, tables, 'node1', created_on=now)
        insert_active_node(postgres, tables, 'node2', created_on=now + datetime.timedelta(seconds=1))

        coord_config = CoordinationConfig(
            total_tokens=50,
            heartbeat_interval_sec=1,
            heartbeat_timeout_sec=3,
            token_refresh_initial_interval_sec=0.5
        )

        node1 = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=5)
        node2 = create_job('node2', postgres, coordination_config=coord_config, wait_on_enter=5)

        node1.__enter__()
        time.sleep(0.5)
        node2.__enter__()

        try:
            assert wait_for_state(node1, JobState.RUNNING_LEADER, timeout_sec=10)
            assert wait_for_state(node2, JobState.RUNNING_FOLLOWER, timeout_sec=10)

            simulate_node_crash(node1)

            assert wait_for_state(node2, JobState.RUNNING_LEADER, timeout_sec=15)

            time.sleep(1)

            leader_monitors = [m for m in node2._monitors.values() if 'dead-node' in m.name or 'rebalance' in m.name]
            assert len(leader_monitors) >= 2, 'Should have leader monitors after promotion'

            for monitor in leader_monitors:
                assert monitor.thread.is_alive(), \
                    f'{monitor.name} should be running after promotion'

            assert node2.am_i_healthy(), 'Promoted node should be healthy'

        finally:
            try:
                node2.__exit__(None, None, None)
            except:
                pass

    @clean_tables('Node')
    def test_demotion_stops_monitors_immediately(self, postgres):
        """Verify demotion stops leader monitors without delay.
        """
        base_config = get_coordination_config()
        tables = schema.get_table_names(base_config.appname)

        now = datetime.datetime.now(datetime.timezone.utc)
        insert_active_node(postgres, tables, 'node1', created_on=now)

        coord_config = CoordinationConfig(total_tokens=50)
        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=5)
        job.__enter__()

        try:
            assert wait_for_state(job, JobState.RUNNING_LEADER, timeout_sec=10)

            leader_monitors_before = [m for m in job._monitors.values() if 'dead-node' in m.name or 'rebalance' in m.name]
            assert len(leader_monitors_before) >= 2, 'Should have leader monitors'

            job.state_machine.transition_to(JobState.RUNNING_FOLLOWER)

            time.sleep(0.2)

            for monitor in leader_monitors_before:
                assert monitor._stop_requested, \
                    f'{monitor.name} should be stopped immediately after demotion'

        finally:
            job.__exit__(None, None, None)


class TestConcurrentDistributionEvents:
    """Test concurrent distribution-related events."""

    @clean_tables('Node')
    def test_membership_and_dead_node_events_near_simultaneous(self, postgres):
        """Verify system handles membership change and dead node events together.
        """
        base_config = get_coordination_config()
        tables = schema.get_table_names(base_config.appname)

        now = datetime.datetime.now(datetime.timezone.utc)
        insert_active_node(postgres, tables, 'leader', created_on=now)
        insert_active_node(postgres, tables, 'node2', created_on=now + datetime.timedelta(seconds=1))
        insert_stale_node(postgres, tables, 'dead-node', heartbeat_age_seconds=30)

        coord_config = CoordinationConfig(total_tokens=50, dead_node_check_interval_sec=0.5, rebalance_check_interval_sec=0.5)

        job = create_job('leader', postgres, coordination_config=coord_config, wait_on_enter=5)
        job.__enter__()

        try:
            assert wait_for_state(job, JobState.RUNNING_LEADER, timeout_sec=10)

            distribution_count = [0]
            original_distribute = job._distribute_tokens_safe

            def track_distribute():
                distribution_count[0] += 1
                original_distribute()

            job._distribute_tokens_safe = track_distribute

            assert wait_for_dead_node_removal(postgres, tables, 'dead-node', timeout_sec=10)

            insert_active_node(postgres, tables, 'node3',
                               created_on=now + datetime.timedelta(seconds=3))

            time.sleep(3)

            assert distribution_count[0] >= 1, \
                'Distribution should be called for concurrent events'
            assert job.am_i_healthy(), 'Leader should remain healthy'

        finally:
            job._distribute_tokens_safe = original_distribute
            job.__exit__(None, None, None)


class TestConcurrentStateTransitions:
    """Test concurrent state machine transitions."""

    def test_concurrent_state_transition_safety(self, postgres):
        """Verify state machine handles concurrent transition attempts safely.

        Critical scenario: Multiple threads attempting state transitions
        simultaneously should not corrupt state or cause race conditions.
        """
        config = get_coordination_config()

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)
        job.__enter__()

        try:
            # Wait for stable running state
            assert wait_for_running_state(job, timeout_sec=10)
            initial_state = job.state_machine.state

            results = []
            errors = []

            def attempt_transition(target_state, thread_id):
                try:
                    # Small random sleep to increase chance of real concurrency
                    time.sleep(0.001 * (thread_id % 3))
                    result = job.state_machine.transition_to(target_state)
                    results.append((thread_id, target_state, result))
                except Exception as e:
                    errors.append((thread_id, e))

            # Create threads that all try to transition simultaneously
            threads = []
            if initial_state == JobState.RUNNING_LEADER:
                # Try concurrent transitions to RUNNING_FOLLOWER and SHUTTING_DOWN
                for i in range(5):
                    t = threading.Thread(
                        target=attempt_transition,
                        args=(JobState.RUNNING_FOLLOWER, i)
                    )
                    threads.append(t)
                for i in range(5, 10):
                    t = threading.Thread(
                        target=attempt_transition,
                        args=(JobState.SHUTTING_DOWN, i)
                    )
                    threads.append(t)
            else:  # RUNNING_FOLLOWER
                # Try concurrent transitions to RUNNING_LEADER and SHUTTING_DOWN
                for i in range(5):
                    t = threading.Thread(
                        target=attempt_transition,
                        args=(JobState.RUNNING_LEADER, i)
                    )
                    threads.append(t)
                for i in range(5, 10):
                    t = threading.Thread(
                        target=attempt_transition,
                        args=(JobState.SHUTTING_DOWN, i)
                    )
                    threads.append(t)

            # Start all threads nearly simultaneously
            for t in threads:
                t.start()

            # Wait for all to complete
            for t in threads:
                t.join(timeout=5)

            # Verify no exceptions occurred
            assert len(errors) == 0, f'Concurrent transitions should not cause exceptions: {errors}'

            # Verify state machine ended in valid state
            final_state = job.state_machine.state
            assert final_state in {JobState.RUNNING_LEADER, JobState.RUNNING_FOLLOWER, JobState.SHUTTING_DOWN}, \
                f'Final state should be valid: {final_state}'

            # Verify at least one transition succeeded
            successful_transitions = [r for r in results if r[2]]
            assert len(successful_transitions) > 0, 'At least one transition should succeed'

        finally:
            job.__exit__(None, None, None)


class TestConcurrentCallbackAndRebalance:
    """Test race conditions between callbacks and rebalancing events."""

    @clean_tables('Node', 'Token')
    def test_rebalance_during_callback_execution(self, postgres):
        """Verify rebalance can occur while on_rebalance is executing.
        """
        coord_cfg = get_coordination_config()
        tables = schema.get_table_names(coord_cfg.appname)

        callback_states = []

        def long_callback():
            callback_states.append(('started', 0))
            time.sleep(3)
            callback_states.append(('finished', 0))

        job1 = create_job('node1', postgres, coordination_config=coord_cfg,
                          wait_on_enter=10, on_rebalance=long_callback)
        job1.__enter__()

        try:
            assert wait_for(lambda: len(job1.my_tokens) >= 1, timeout_sec=15)
            assert wait_for_running_state(job1, timeout_sec=5)

            assert wait_for(lambda: any(s[0] == 'started' for s in callback_states), timeout_sec=5)

            job2 = create_job('node2', postgres, coordination_config=coord_cfg, wait_on_enter=10)
            job2.__enter__()

            try:
                assert wait_for(lambda: len(job2.my_tokens) >= 1, timeout_sec=15)
                assert wait_for_rebalance(postgres, tables, min_count=1, timeout_sec=20)

                assert len(callback_states) >= 2, 'Multiple callback invocations expected'

            finally:
                job2.__exit__(None, None, None)

        finally:
            job1.__exit__(None, None, None)

    @clean_tables('Node', 'Token')
    def test_rapid_membership_changes_during_callback(self, postgres):
        """Verify rapid node joins/leaves during slow callback are all detected.
        """
        coord_cfg = get_coordination_config()
        tables = schema.get_table_names(coord_cfg.appname)

        callback_active = []

        def blocking_callback():
            callback_active.append(True)
            time.sleep(8)
            callback_active.append(False)

        job1 = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10,
                          on_rebalance=blocking_callback)
        job1.__enter__()

        try:
            assert wait_for_running_state(job1, timeout_sec=10)

            temp_nodes = []
            for i in range(4):
                temp = create_job(f'temp-{i}', postgres, coordination_config=coord_cfg, wait_on_enter=5)
                temp.__enter__()
                temp_nodes.append(temp)
                time.sleep(0.3)

                if i == 0:
                    assert wait_for(lambda: len(callback_active) >= 1, timeout_sec=5)

            for temp in temp_nodes:
                temp.__exit__(None, None, None)
                time.sleep(0.3)

            time.sleep(10)

            with postgres.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM {tables["Rebalance"]}
                    WHERE triggered_at > NOW() - INTERVAL '30 seconds'
                """))
                rebalance_count = result.scalar()

            assert rebalance_count >= 4, 'All membership changes should trigger rebalances'

        finally:
            job1.__exit__(None, None, None)

    @clean_tables('Node', 'Token')
    def test_callback_and_leadership_change_concurrent(self, postgres):
        """Verify leadership changes can occur while callbacks are executing.
        """
        coord_cfg = get_coordination_config()
        tables = schema.get_table_names(coord_cfg.appname)

        callback_events = []

        def slow_callback():
            callback_events.append(('callback_start', 0))
            time.sleep(4)
            callback_events.append(('callback_end', 0))

        now = datetime.datetime.now(datetime.timezone.utc)
        insert_active_node(postgres, tables, 'node1', created_on=now)
        insert_active_node(postgres, tables, 'node2', created_on=now + datetime.timedelta(seconds=1))

        job1 = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=5)
        job2 = create_job('node2', postgres, coordination_config=coord_cfg, wait_on_enter=5,
                          on_rebalance=slow_callback)

        job1.__enter__()
        time.sleep(0.5)
        job2.__enter__()

        try:
            assert wait_for_state(job1, JobState.RUNNING_LEADER, timeout_sec=10)
            assert wait_for_state(job2, JobState.RUNNING_FOLLOWER, timeout_sec=10)

            assert wait_for(lambda: any('callback_start' in str(e) for e in callback_events),
                            timeout_sec=5)

            simulate_node_crash(job1, cleanup=True)

            time.sleep(6)

            assert wait_for_state(job2, JobState.RUNNING_LEADER, timeout_sec=15), \
                'node2 should be promoted despite slow callback'

        finally:
            try:
                job2.__exit__(None, None, None)
            except:
                pass

    @clean_tables('Node', 'Token')
    def test_callbacks_queued_during_rapid_membership_changes(self, postgres):
        """Verify callbacks are queued and executed for all membership changes.

        When rapid membership changes occur while a slow callback is executing,
        the system queues all callback invocations and executes them sequentially.
        No callbacks are dropped - they're all eventually processed.
        """
        coord_cfg = get_coordination_config()
        tables = schema.get_table_names(coord_cfg.appname)

        version_history = []

        def version_tracking_callback():
            version_history.append(('callback', time.time()))
            time.sleep(6)

        job1 = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10,
                          on_rebalance=version_tracking_callback)
        job1.__enter__()

        try:
            assert wait_for_running_state(job1, timeout_sec=10)
            initial_tokens = len(job1.my_tokens)
            initial_version = job1.token_version

            temp_nodes = []
            for i in range(3):
                temp = create_job(f'temp{i}', postgres, coordination_config=coord_cfg, wait_on_enter=5)
                temp.__enter__()
                temp_nodes.append(temp)
                time.sleep(0.5)

            for temp in temp_nodes:
                temp.__exit__(None, None, None)
                time.sleep(0.5)

            time.sleep(8)

            events = job1._event_queue.get_history()
            rebalance_events = [e for e in events if 'membership' in e.type or 'dead_nodes' in e.type]

            assert len(rebalance_events) >= 4, 'Multiple rebalance-triggering events should occur (nodes joining and leaving)'

            expected_callbacks = 1 + len(rebalance_events)
            assert len(version_history) == expected_callbacks, \
                f'Expected {expected_callbacks} callbacks (1 initial + {len(rebalance_events)} rebalances), got {len(version_history)}'

            callback_timestamps = [ts for _, ts in version_history]
            for i in range(len(callback_timestamps) - 1):
                time_diff = callback_timestamps[i + 1] - callback_timestamps[i]
                assert time_diff >= 5.9, \
                    f'Callbacks should be queued and executed sequentially (gap: {time_diff:.1f}s between callbacks {i} and {i+1})'

        finally:
            job1.__exit__(None, None, None)
            for temp in temp_nodes:
                try:
                    temp.__exit__(None, None, None)
                except:
                    pass


class TestClaimAndAuditOperations:
    """Test claim and audit table operations with Task objects."""

    @clean_tables('Node', 'Token', 'Claim')
    def test_set_claim_accepts_task_objects(self, postgres):
        """Verify set_claim works with both Task objects and raw task IDs.
        """
        coord_cfg = get_coordination_config(total_tokens=10)
        job = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10)
        job.__enter__()

        try:
            assert wait_for_running_state(job, timeout_sec=10)

            job.set_claim(Task('BBG123'))
            job.set_claim('BBG456')

            tables = schema.get_table_names(coord_cfg.appname)
            with postgres.connect() as conn:
                result = conn.execute(text(
                    f'SELECT node, task_id FROM {tables["Claim"]} ORDER BY task_id'
                ))
                rows = result.fetchall()

            assert len(rows) == 2, f'Expected 2 claim rows, got {len(rows)}'
            assert rows[0][1] == 'BBG123'
            assert rows[1][1] == 'BBG456'
            assert all(row[0] == 'node1' for row in rows)

        finally:
            job.__exit__(None, None, None)

    @clean_tables('Node', 'Token', 'Claim')
    def test_multi_node_add_task_during_coordination(self, postgres):
        """Verify multiple nodes can add_task concurrently without errors.
        """
        coord_cfg = get_coordination_config(total_tokens=20)

        with cluster(postgres, 'node1', 'node2', total_tokens=20) as nodes:
            assert wait_for_cluster_running(nodes, timeout_sec=15)

            for i in range(10):
                task = Task(i * 2)
                if nodes[0].can_claim_task(task):
                    nodes[0].add_task(task)
                elif nodes[1].can_claim_task(task):
                    nodes[1].add_task(task)

            for i in range(10):
                task = Task(i * 2 + 1)
                if nodes[1].can_claim_task(task):
                    nodes[1].add_task(task)
                elif nodes[0].can_claim_task(task):
                    nodes[0].add_task(task)

            tables = schema.get_table_names(coord_cfg.appname)
            with postgres.connect() as conn:
                result = conn.execute(text(
                    f'SELECT COUNT(*) FROM {tables["Claim"]}'
                ))
                claim_count = result.scalar()

            assert claim_count > 0, 'At least some claims should be written'

            assert nodes[0].am_i_healthy()
            assert nodes[1].am_i_healthy()


class TestCallbackTiming:
    """Test callback timing relative to job lifecycle."""

    @clean_tables('Node', 'Token')
    def test_initial_rebalance_callback_fires_after_enter(self, postgres):
        """Verify initial on_rebalance fires after __enter__ within expected interval.
        """
        tracker = CallbackTracker()
        coord_cfg = get_coordination_config(
            total_tokens=10,
            token_refresh_initial_interval_sec=2,
        )

        enter_time = None
        job = create_job(
            'node1', postgres,
            coordination_config=coord_cfg,
            wait_on_enter=10,
            on_rebalance=tracker.on_rebalance,
        )

        enter_time = time.time()
        job.__enter__()

        try:
            assert wait_for_state(job, JobState.RUNNING_LEADER, timeout_sec=10)

            timeout = coord_cfg.token_refresh_initial_interval_sec + 5
            assert wait_for(
                lambda: len(tracker.rebalance_calls) >= 1,
                timeout_sec=timeout,
            ), f'Initial rebalance callback not fired within {timeout}s'

            first_callback_time = tracker.rebalance_calls[0]
            assert first_callback_time >= enter_time, (
                'Callback fired before __enter__ returned '
                f'(enter={enter_time:.3f}, callback={first_callback_time:.3f})'
            )

        finally:
            job.__exit__(None, None, None)


if __name__ == '__main__':
    pytest.main(args=['-sx', __file__])
