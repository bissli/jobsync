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
        """Verify the leader removes a stale node and redistributes while token refresh polls.

        Mutation: DeadNodeMonitor skips the DELETE, or the dead_nodes_detected
            handler skips _distribute_tokens_safe.
        Oracle: the stale row the test inserts, and a 'dead_nodes' row in the
            Rebalance table.
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
        try:
            job.__enter__()
            assert wait_for_state(job, JobState.RUNNING_LEADER, timeout_sec=10)

            assert wait_for_dead_node_removal(postgres, tables, 'stale-node', timeout_sec=10)
            assert wait_for(
                lambda: get_rebalance_count(postgres, tables, 'dead_nodes') >= 1,
                timeout_sec=10), 'dead node removal did not trigger a redistribution'

            assert job.am_i_healthy(), 'Leader should remain healthy'

        finally:
            job.__exit__(None, None, None)


class TestPromotionDemotionRaceConditions:
    """Test race conditions during leader promotion/demotion."""

    @clean_tables('Node')
    def test_promotion_while_monitoring_thread_running(self, postgres):
        """Verify a promoted follower starts live leader-only monitors.

        Mutation: _on_enter_running_leader skips starting DeadNodeMonitor or
            RebalanceMonitor.
        Oracle: node1 stops heartbeating, so node2 is the oldest active node
            and must run dead-node-node2 and rebalance-node2.
        """
        coord_config = get_coordination_config(total_tokens=50, token_refresh_initial_interval_sec=0.5)

        node1 = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=5)
        node2 = create_job('node2', postgres, coordination_config=coord_config, wait_on_enter=5)

        try:
            node1.__enter__()
            try:
                node2.__enter__()
                assert wait_for_state(node1, JobState.RUNNING_LEADER, timeout_sec=10)
                assert wait_for_state(node2, JobState.RUNNING_FOLLOWER, timeout_sec=10)

                simulate_node_crash(node1)

                assert wait_for_state(node2, JobState.RUNNING_LEADER, timeout_sec=15)

                leader_monitor_names = {'dead-node-node2', 'rebalance-node2'}
                assert wait_for(lambda: leader_monitor_names <= {
                    m.name for m in node2._monitors.values() if m.thread and m.thread.is_alive()
                    }), f'leader monitors not running after promotion: {list(node2._monitors)}'

                assert node2.am_i_healthy(), 'Promoted node should be healthy'
            finally:
                node2.__exit__(None, None, None)
        finally:
            node1.__exit__(None, None, None)

    @clean_tables('Node')
    def test_demotion_stops_monitors_immediately(self, postgres):
        """Verify demotion flags the leader-only monitors to stop before transition_to returns.

        Mutation: _stop_monitor drops the monitor without calling stop(), or
            the RUNNING_LEADER on_exit callback is not registered.
        Oracle: the two monitor objects captured before the transition.
        """
        base_config = get_coordination_config()
        tables = schema.get_table_names(base_config.appname)

        now = datetime.datetime.now(datetime.timezone.utc)
        insert_active_node(postgres, tables, 'node1', created_on=now)

        coord_config = CoordinationConfig(total_tokens=50)
        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=5)
        try:
            job.__enter__()
            assert wait_for_state(job, JobState.RUNNING_LEADER, timeout_sec=10)

            leader_monitors_before = [m for m in job._monitors.values() if 'dead-node' in m.name or 'rebalance' in m.name]
            assert len(leader_monitors_before) == 2, 'Should have leader monitors'

            assert job.state_machine.transition_to(JobState.RUNNING_FOLLOWER)

            for monitor in leader_monitors_before:
                assert monitor._stop_requested, \
                    f'{monitor.name} should be stopped immediately after demotion'

        finally:
            job.__exit__(None, None, None)


class TestConcurrentDistributionEvents:
    """Test concurrent distribution-related events."""

    @clean_tables('Node')
    def test_membership_and_dead_node_events_near_simultaneous(self, postgres):
        """Verify a join and a dead node arriving together both reach the token table.

        Mutation: the dead_nodes_detected handler skips _distribute_tokens_safe.
        Oracle: node3 is active and dead-node is stale, so after both events
            dead-node's row is gone, a 'dead_nodes' distribution is logged, and
            node3 owns tokens.
        """
        coord_config = CoordinationConfig(total_tokens=50, dead_node_check_interval_sec=0.5, rebalance_check_interval_sec=0.5)
        tables = schema.get_table_names(coord_config.appname)

        job = create_job('leader', postgres, coordination_config=coord_config, wait_on_enter=5)
        try:
            job.__enter__()
            assert wait_for_state(job, JobState.RUNNING_LEADER, timeout_sec=10)

            # node3 goes in first so the dead_nodes distribution always sees
            # it. A membership_change distribution that collides with the
            # DeadNodeMonitor rebalance lock is dropped, so this test does
            # not require one.
            insert_active_node(postgres, tables, 'node3')
            insert_stale_node(postgres, tables, 'dead-node', heartbeat_age_seconds=30)

            assert wait_for_dead_node_removal(postgres, tables, 'dead-node', timeout_sec=10)
            assert wait_for(
                lambda: get_rebalance_count(postgres, tables, 'dead_nodes') >= 1,
                timeout_sec=10), 'dead node removal did not trigger a redistribution'
            assert wait_for(
                lambda: 'node3' in set(get_token_assignments(postgres, tables).values()),
                timeout_sec=10), 'node3 never received tokens'

            assert job.am_i_healthy(), 'Leader should remain healthy'

        finally:
            job.__exit__(None, None, None)


class TestConcurrentStateTransitions:
    """Test concurrent state machine transitions."""

    def test_concurrent_state_transition_safety(self, postgres):
        """Verify racing RUNNING_FOLLOWER and SHUTTING_DOWN requests end in SHUTTING_DOWN.

        Mutation: the lock removed from JobStateMachine.transition_to, so a
            RUNNING_FOLLOWER request validated before SHUTTING_DOWN lands
            overwrites it.
        Oracle: SHUTTING_DOWN has no outgoing transition, so every serial
            order of these requests ends there and leaves RUNNING_LEADER once.
        """
        config = get_coordination_config()

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)
        try:
            job.__enter__()
            assert wait_for_state(job, JobState.RUNNING_LEADER, timeout_sec=10)

            exit_leader_calls = []
            exit_leader = job.state_machine._on_exit_callbacks[JobState.RUNNING_LEADER]

            def slow_exit_leader():
                exit_leader_calls.append(threading.get_ident())
                # Holds the transition open so racing requests interleave.
                time.sleep(0.05)
                exit_leader()

            job.state_machine.on_exit(JobState.RUNNING_LEADER, slow_exit_leader)

            targets = [JobState.RUNNING_FOLLOWER] * 5 + [JobState.SHUTTING_DOWN] * 5
            start_barrier = threading.Barrier(len(targets))
            errors = []

            def attempt_transition(target_state):
                start_barrier.wait()
                try:
                    job.state_machine.transition_to(target_state)
                except Exception as exc:
                    errors.append(exc)

            threads = [threading.Thread(target=attempt_transition, args=(target,)) for target in targets]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=5)

            assert not any(t.is_alive() for t in threads), 'transition threads hung'
            assert errors == [], f'Concurrent transitions raised: {errors}'
            assert len(exit_leader_calls) == 1, f'RUNNING_LEADER exited {len(exit_leader_calls)} times'
            assert job.state_machine.state == JobState.SHUTTING_DOWN
            assert job._shutdown_event.is_set(), 'SHUTTING_DOWN entry action did not run'

        finally:
            job.__exit__(None, None, None)


class TestConcurrentCallbackAndRebalance:
    """Test race conditions between callbacks and rebalancing events."""

    @clean_tables('Node', 'Token')
    def test_rebalance_during_callback_execution(self, postgres):
        """Verify a rebalance reaches the leader's token cache while its on_rebalance is blocked.

        Mutation: invoke_callback runs the callback inline on the
            TokenRefreshMonitor thread, or the executor gets a second worker.
        Oracle: two nodes split total_tokens evenly, so the queued second
            event removes half of node1's tokens and adds none.
        """
        coord_cfg = get_coordination_config()

        release_callback = threading.Event()
        callback_events = []

        def blocking_callback(event):
            callback_events.append(event)
            release_callback.wait(timeout=60)

        job1 = create_job('node1', postgres, coordination_config=coord_cfg,
                          wait_on_enter=10, on_rebalance=blocking_callback)
        try:
            job1.__enter__()
            assert wait_for(lambda: len(callback_events) == 1, timeout_sec=15)

            job2 = create_job('node2', postgres, coordination_config=coord_cfg, wait_on_enter=10)
            try:
                job2.__enter__()
                assert wait_for_token_sync([job1, job2], coord_cfg.total_tokens, check_cache=True, timeout_sec=20), \
                    'node1 token cache did not follow the rebalance while its callback was blocked'
                assert wait_for(lambda: len(job1.tokens._pending_callbacks) == 2, timeout_sec=5), \
                    'second callback was never submitted'
                assert not wait_for(lambda: len(callback_events) > 1, timeout_sec=1), \
                    'second callback started before the first returned'

                release_callback.set()
                assert wait_for(lambda: len(callback_events) == 2, timeout_sec=10)

                second = callback_events[1]
                assert (second.is_initial, second.tokens_added, second.tokens_removed) == \
                    (False, 0, coord_cfg.total_tokens // 2)

            finally:
                release_callback.set()
                job2.__exit__(None, None, None)

        finally:
            release_callback.set()
            job1.__exit__(None, None, None)

    @clean_tables('Node', 'Token')
    def test_rapid_membership_changes_during_callback(self, postgres):
        """Verify joins and leaves during a slow callback all reach the leader.

        Mutation: RebalanceMonitor publishes membership_changed only when the
            node count rises, so departures never redistribute.
        Oracle: once every temp node has left, node1 is the only node and
            must own all total_tokens again.
        """
        coord_cfg = get_coordination_config()

        callback_active = []

        def blocking_callback():
            callback_active.append(True)
            time.sleep(8)
            callback_active.append(False)

        job1 = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10,
                          on_rebalance=blocking_callback)

        temp_nodes = []
        try:
            job1.__enter__()
            assert wait_for_running_state(job1, timeout_sec=10)

            for i in range(4):
                temp = create_job(f'temp-{i}', postgres, coordination_config=coord_cfg, wait_on_enter=5)
                temp_nodes.append(temp)
                temp.__enter__()

                if i == 0:
                    assert wait_for(lambda: len(callback_active) >= 1, timeout_sec=5)

            while temp_nodes:
                temp_nodes.pop().__exit__(None, None, None)

            assert wait_for_token_sync([job1], coord_cfg.total_tokens, check_cache=True, timeout_sec=20), \
                'node1 did not regain every token after the temp nodes left'

        finally:
            while temp_nodes:
                temp_nodes.pop().__exit__(None, None, None)
            job1.__exit__(None, None, None)

    @clean_tables('Node', 'Token')
    def test_callback_and_leadership_change_concurrent(self, postgres):
        """Verify a follower is promoted while its on_rebalance callback is blocked.

        Mutation: invoke_callback runs the callback inline on the
            TokenRefreshMonitor thread, the thread that detects promotion.
        Oracle: node1's Node row is deleted, so node2 is the oldest active
            node and must reach RUNNING_LEADER.
        """
        coord_cfg = get_coordination_config()

        callback_started = threading.Event()
        release_callback = threading.Event()

        def blocking_callback():
            callback_started.set()
            release_callback.wait(timeout=60)

        job1 = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=5)
        job2 = create_job('node2', postgres, coordination_config=coord_cfg, wait_on_enter=5,
                          on_rebalance=blocking_callback)

        try:
            job1.__enter__()
            try:
                job2.__enter__()
                assert wait_for_state(job1, JobState.RUNNING_LEADER, timeout_sec=10)
                assert wait_for_state(job2, JobState.RUNNING_FOLLOWER, timeout_sec=10)
                assert callback_started.wait(timeout=10), 'node2 initial callback never started'

                simulate_node_crash(job1, cleanup=True)

                assert wait_for_state(job2, JobState.RUNNING_LEADER, timeout_sec=15), \
                    'node2 should be promoted while its callback is blocked'

            finally:
                release_callback.set()
                job2.__exit__(None, None, None)
        finally:
            job1.__exit__(None, None, None)

    @clean_tables('Node', 'Token')
    def test_callbacks_queued_during_rapid_membership_changes(self, postgres):
        """Verify every token version reaches on_rebalance, in order, one callback at a time.

        Mutation: invoke_callback skips a callback while one is pending, or
            the callback executor gets a second worker.
        Oracle: each distribution raises the version by one from 1, so six
            membership changes deliver versions 1-7, and serial callbacks
            start at least callback_sec apart.
        """
        callback_sec = 3
        coord_cfg = get_coordination_config(heartbeat_timeout_sec=60, token_refresh_initial_interval_sec=0.3)
        tables = schema.get_table_names(coord_cfg.appname)

        delivered = []

        def slow_callback(event):
            delivered.append((event.token_version, time.time()))
            time.sleep(callback_sec)

        job1 = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=2,
                          on_rebalance=slow_callback)
        try:
            job1.__enter__()
            assert wait_for_state(job1, JobState.RUNNING_LEADER, timeout_sec=10)
            assert wait_for(lambda: job1.token_version == 1, timeout_sec=5)

            fake_nodes = ['fake-0', 'fake-1', 'fake-2']
            changes = [('insert', name) for name in fake_nodes] + [('delete', name) for name in fake_nodes]
            for step, (action, name) in enumerate(changes, start=1):
                if action == 'insert':
                    insert_active_node(postgres, tables, name)
                else:
                    delete_rows(postgres, tables, 'Node', 'name = :name', {'name': name})
                assert wait_for(
                    lambda step=step: get_rebalance_count(postgres, tables, 'membership_change') == step,
                    timeout_sec=10), f'{action} {name} did not trigger a distribution'
                assert wait_for(lambda step=step: job1.token_version == step + 1, timeout_sec=5), \
                    f'node1 did not see version {step + 1}'

            expected_versions = list(range(1, len(changes) + 2))
            assert wait_for(
                lambda: len(delivered) == len(expected_versions),
                timeout_sec=callback_sec * len(expected_versions) + 10)
            assert [version for version, _ in delivered] == expected_versions

            starts = [ts for _, ts in delivered]
            gaps = [later - earlier for earlier, later in zip(starts, starts[1:])]
            assert min(gaps) >= callback_sec - 0.1, f'Callbacks overlapped: gaps {gaps}'

        finally:
            job1.__exit__(None, None, None)


class TestClaimAndAuditOperations:
    """Test claim and audit table operations with Task objects."""

    @clean_tables('Node', 'Token', 'Claim')
    def test_set_claim_accepts_task_objects(self, postgres):
        """Verify set_claim stores the task id for both a Task object and a raw id.

        Mutation: TaskManager.set_claim writes str(task) in place of
            str(extract_task_id(task)).
        Oracle: the literal ids 'BBG123' and 'BBG456'.
        """
        coord_cfg = get_coordination_config(total_tokens=10)
        job = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10)
        try:
            job.__enter__()
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
        """Verify concurrent add_task on two nodes claims each task once, on its token's owner.

        Mutation: TaskManager.can_claim ignores my_tokens, or the
            claimability check in Job.add_task is flipped.
        Oracle: the Token table's owner of each task's token.
        """
        total_tokens = 20
        coord_cfg = get_coordination_config(total_tokens=total_tokens)
        tables = schema.get_table_names(coord_cfg.appname)
        task_ids = list(range(20))

        with cluster(postgres, 'node1', 'node2', total_tokens=total_tokens) as nodes:
            assert wait_for_cluster_running(nodes, timeout_sec=15)
            assert wait_for_token_sync(nodes, total_tokens, check_cache=True)

            owner_by_token = get_token_assignments(postgres, tables)
            assert set(owner_by_token.values()) == {'node1', 'node2'}

            def add_all_tasks(node):
                for task_id in task_ids:
                    node.add_task(Task(task_id))

            threads = [threading.Thread(target=add_all_tasks, args=(node,)) for node in nodes]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=10)
            assert not any(t.is_alive() for t in threads), 'add_task threads hung'

            with postgres.connect() as conn:
                claims = set(conn.execute(text(f'SELECT node, task_id FROM {tables["Claim"]}')))

            expected = {(owner_by_token[nodes[0].task_to_token(task_id)], str(task_id)) for task_id in task_ids}
            assert claims == expected

            assert nodes[0].am_i_healthy()
            assert nodes[1].am_i_healthy()


class TestCallbackTiming:
    """Test callback timing relative to job lifecycle."""

    @clean_tables('Node', 'Token')
    def test_initial_rebalance_callback_reports_full_assignment_once(self, postgres):
        """Verify a lone node gets exactly one initial on_rebalance event covering all its tokens.

        Mutation: TokenRefreshMonitor never sets initial_callback_sent, or
            builds the initial event with is_initial=False or the wrong
            tokens_added.
        Oracle: a single node owns all 10 tokens at version 1.
        """
        refresh_sec = 1
        coord_cfg = get_coordination_config(total_tokens=10, token_refresh_initial_interval_sec=refresh_sec)

        events = []

        def record_event(event):
            events.append(event)

        job = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=2,
                         on_rebalance=record_event)
        try:
            job.__enter__()
            assert wait_for_state(job, JobState.RUNNING_LEADER, timeout_sec=10)
            assert wait_for(lambda: len(events) >= 1, timeout_sec=refresh_sec + 5), \
                'Initial rebalance callback never fired'

            token_refresh = job._monitors['token_refresh']
            checks_at_first_event = token_refresh.check_count
            assert wait_for(lambda: token_refresh.check_count >= checks_at_first_event + 2, timeout_sec=10), \
                'token refresh stopped checking'

            assert len(events) == 1, f'Expected one callback, got {events}'
            first = events[0]
            assert (first.is_initial, first.token_version, first.tokens_added, first.tokens_removed) == \
                (True, 1, 10, 0)

        finally:
            job.__exit__(None, None, None)


if __name__ == '__main__':
    pytest.main(args=['-sx', __file__])
