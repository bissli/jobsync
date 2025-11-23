"""Tests for concurrent operations, race conditions, and thread safety.

USE THIS FILE FOR:
- Concurrency and threading tests
- Race condition scenarios
- Thread safety verification
- Monitor interaction tests
- Simultaneous operation tests
"""
import datetime
import logging
import threading
import time

import pytest

from jobsync import schema
from jobsync.client import CoordinationConfig, JobState

logger = logging.getLogger(__name__)


from fixtures import *  # noqa: F401, F403

logger = logging.getLogger(__name__)


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

            logger.info('✓ Dead node detection worked concurrently with token refresh')

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

            logger.info('✓ Promotion during monitoring activity successful')

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

            logger.info('✓ Demotion stopped monitors immediately')

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

            logger.info('✓ Concurrent membership and dead node events handled')

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

            logger.info(f'Initial state: {initial_state}')

            # Attempt concurrent transitions from multiple threads
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

            logger.info(f'✓ Concurrent transitions handled safely: {initial_state} -> {final_state}')
            logger.info(f'  Successful transitions: {len(successful_transitions)}/{len(results)}')

        finally:
            job.__exit__(None, None, None)


class TestConcurrentCallbackAndRebalance:
    """Test race conditions between callbacks and rebalancing events."""

    @clean_tables('Node', 'Token')
    def test_rebalance_during_callback_execution(self, postgres):
        """Verify rebalance can occur while on_tokens_added is executing.
        """
        coord_cfg = get_coordination_config()
        tables = schema.get_table_names(coord_cfg.appname)

        callback_states = []

        def long_callback(token_ids: set[int]):
            callback_states.append(('started', len(token_ids)))
            time.sleep(3)
            callback_states.append(('finished', len(token_ids)))

        job1 = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10,
                  on_tokens_added=long_callback)
        job1.__enter__()

        try:
            assert wait_for(lambda: len(job1.my_tokens) >= 1, timeout_sec=15)
            assert wait_for_running_state(job1, timeout_sec=5)

            assert wait_for(lambda: any(s[0] == 'started' for s in callback_states), timeout_sec=5)

            logger.info('Initial callback started, triggering rebalance...')

            job2 = create_job('node2', postgres, coordination_config=coord_cfg, wait_on_enter=10)
            job2.__enter__()

            try:
                assert wait_for(lambda: len(job2.my_tokens) >= 1, timeout_sec=15)
                assert wait_for_rebalance(postgres, tables, min_count=1, timeout_sec=20)

                logger.info(f'Callback states: {callback_states}')

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

        def blocking_callback(token_ids: set[int]):
            callback_active.append(True)
            time.sleep(8)
            callback_active.append(False)

        job1 = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10,
                  on_tokens_removed=blocking_callback)
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
                    logger.info('Callback started, continuing to add nodes...')

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

            logger.info(f'Rebalances despite blocking callback: {rebalance_count}')

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

        def slow_callback(token_ids: set[int]):
            callback_events.append(('callback_start', len(token_ids)))
            time.sleep(4)
            callback_events.append(('callback_end', len(token_ids)))

        now = datetime.datetime.now(datetime.timezone.utc)
        insert_active_node(postgres, tables, 'node1', created_on=now)
        insert_active_node(postgres, tables, 'node2', created_on=now + datetime.timedelta(seconds=1))

        job1 = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=5)
        job2 = create_job('node2', postgres, coordination_config=coord_cfg, wait_on_enter=5,
                  on_tokens_added=slow_callback)

        job1.__enter__()
        time.sleep(0.5)
        job2.__enter__()

        try:
            assert wait_for_state(job1, JobState.RUNNING_LEADER, timeout_sec=10)
            assert wait_for_state(job2, JobState.RUNNING_FOLLOWER, timeout_sec=10)

            assert wait_for(lambda: any('callback_start' in str(e) for e in callback_events),
                          timeout_sec=5)

            logger.info('Callback started on node2, killing node1...')

            simulate_node_crash(job1, cleanup=True)

            time.sleep(6)

            assert wait_for_state(job2, JobState.RUNNING_LEADER, timeout_sec=15), \
                'node2 should be promoted despite slow callback'

            logger.info(f'Callback events: {callback_events}')
            logger.info('Leadership change occurred during callback execution')

        finally:
            try:
                job2.__exit__(None, None, None)
            except:
                pass

    @clean_tables('Node', 'Token')
    def test_token_version_changes_missed_during_callback(self, postgres):
        """Document that token version changes can be missed if callback is slow.
        """
        coord_cfg = get_coordination_config()
        tables = schema.get_table_names(coord_cfg.appname)

        version_history = []

        def version_tracking_callback(token_ids: set[int]):
            version_history.append(('callback', len(token_ids)))
            time.sleep(6)

        job1 = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10,
                  on_tokens_removed=version_tracking_callback)
        job1.__enter__()

        try:
            assert wait_for_running_state(job1, timeout_sec=10)
            initial_tokens = len(job1.my_tokens)
            initial_version = job1.token_version

            logger.info(f'Initial: {initial_tokens} tokens, v{initial_version}')

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

            logger.info(f'Coordination events: {[e.type for e in events]}')
            logger.info(f'Rebalance-triggering events: {len(rebalance_events)}')
            logger.info(f'Callback invocations: {len(version_history)}')
            logger.info(f'Final job1 version: v{job1.token_version}')

            assert len(rebalance_events) >= 4, 'Multiple rebalance-triggering events should occur (nodes joining and leaving)'

            if len(version_history) > 0:
                assert len(version_history) < len(rebalance_events), \
                    'Callback invocations should be fewer than rebalance events (some missed due to blocking)'
            else:
                logger.warning('No callbacks fired - callback may still be blocked on initial invocation')

        finally:
            job1.__exit__(None, None, None)
            for temp in temp_nodes:
                try:
                    temp.__exit__(None, None, None)
                except:
                    pass


if __name__ == '__main__':
    pytest.main(args=['-sx', __file__])
