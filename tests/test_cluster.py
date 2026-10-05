"""Integration tests for cluster-wide operations and coordination.

Scope
-----
- Tests requiring multiple coordinated nodes
- Cluster-wide coordination scenarios
- Leader/follower interaction tests
- Rebalancing and failover tests
- Production-like end-to-end scenarios
"""
import datetime
import logging
import threading
import time
from datetime import timedelta

import pytest
from fixtures import *  # noqa: F401, F403
from sqlalchemy import text

from jobsync import schema
from jobsync.client import CoordinationConfig, DeadNodeMonitor, EventQueue
from jobsync.client import JobState, LockNotAcquired, RebalanceMonitor, Task


def test_3node_cluster_formation(postgres):
    """Verify 3 nodes elect the oldest as leader and split 100 tokens 34/33/33.

    Mutation: elect_leader ordering by created_on DESC, or the remainder
        token handed to every node in compute_minimal_move_distribution.
    Oracle: node1 is created first; 100 tokens over 3 nodes is 33, 33, 34.
    """
    with cluster(postgres, 'node1', 'node2', 'node3', total_tokens=100) as nodes:
        assert wait_for_cluster_running(nodes, leader_name='node1'), \
            'Each node should reach its running state'

        assert len(nodes[0].get_active_nodes()) == 3, 'All 3 nodes should be active'

        for node in nodes:
            wait_for_leader_election(node, expected_leader='node1', timeout_sec=5)

        assert wait_for_cached_tokens_sync(nodes, expected_total=100, timeout_sec=10), \
            'All nodes should sync their token caches after distribution'

        token_counts = sorted(len(node.my_tokens) for node in nodes)
        assert token_counts == [33, 33, 34], \
            f'Expected a 33/33/34 split, got {token_counts}'


def test_fresh_cluster_rebalances_stale_tokens(postgres):
    """Verify a fresh cluster moves every token off a prior run's dead owner.

    Mutation: compute_minimal_move_distribution keeping a token with an
        owner absent from the active nodes, or the new version not
        built on MAX(version).
    Oracle: 100 seeded rows owned by 'old-dead-node' at version 1.
    """
    tables = schema.get_table_names('sync_')

    for token_id in range(100):
        insert_token(postgres, tables, token_id, 'old-dead-node', version=1)

    with cluster(postgres, 'node1', 'node2', 'node3', total_tokens=100) as nodes:
        assert wait_for_cluster_running(nodes, leader_name='node1'), \
            'Each node should reach its running state'

        with postgres.connect() as conn:
            sql = f"""
            select node, count(*) as count, version
            from {tables["Token"]}
            group by node, version
            order by node
            """
            result = conn.execute(text(sql))
            distribution = [dict(row._mapping) for row in result]

        stale_node_tokens = [r for r in distribution if r['node'] == 'old-dead-node']
        assert len(stale_node_tokens) == 0, 'Stale node should not own any tokens'

        total_redistributed = sum(r['count'] for r in distribution)
        assert total_redistributed == 100, 'All 100 tokens should be redistributed'

        assert wait_for_cached_tokens_sync(nodes, expected_total=100, timeout_sec=10), \
            'All nodes should sync their token caches after rebalancing'
        assert_token_distribution_balanced(nodes, total_tokens=100, tolerance=0.2)

        versions = {r['version'] for r in distribution}
        assert min(versions) > 1, f'Version should increment from 1, got {versions}'


def test_token_based_task_claiming(postgres):
    """Verify add_task claims each task only on the node owning its token.

    Mutation: Job.add_task skipping its can_claim_task guard, or
        TaskManager.can_claim ignoring my_tokens.
    Oracle: the Token table owner of each task's token, read straight
        from the database.
    """
    tables = schema.get_table_names('sync_')

    with cluster(postgres, 'node1', 'node2', 'node3', total_tokens=100) as nodes:
        assert wait_for_cached_tokens_sync(nodes, expected_total=100, timeout_sec=10), \
            'All nodes should sync their token caches after cluster formation'

        for node in nodes:
            for task_id in range(30):
                node.add_task(create_task(task_id))

        owner_by_token = get_token_assignments(postgres, tables)
        with postgres.connect() as conn:
            claims = conn.execute(
                text(f'select task_id, node from {tables["Claim"]}')).all()

        claimed_task_ids = sorted(int(task_id) for task_id, _ in claims)
        assert claimed_task_ids == list(range(30)), \
            'Each task should be claimed exactly once'
        for task_id, node_name in claims:
            assert node_name == owner_by_token[nodes[0].task_to_token(task_id)], \
                f'Task {task_id} claimed by {node_name}, which does not own its token'


def test_node_death_and_rebalancing(postgres):
    """Verify a crashed node's tokens move to the two survivors.

    Mutation: compute_minimal_move_distribution keeping a token with any
        previous owner, active or not.
    Oracle: 100 tokens over the 2 survivors, each above its 3-node share.
    """
    tables = schema.get_table_names('sync_')

    with cluster(postgres, 'node1', 'node2', 'node3', total_tokens=100) as nodes:
        assert wait_for_cluster_running(nodes, leader_name='node1')

        assert wait_for_cached_tokens_sync(nodes, expected_total=100, timeout_sec=10), \
            'All nodes should sync their token caches after cluster formation'

        initial_tokens = {node.node_name: len(node.my_tokens) for node in nodes}

        simulate_node_crash(nodes[1])

        assert wait_for_dead_node_removal(postgres, tables, 'node2', timeout_sec=20), \
            'node2 should be detected as dead and removed by leader'

        assert wait_for_all_nodes_token_sync(
            [nodes[0], nodes[2]],
            expected_total=100,
            timeout_sec=10)

        for node in [nodes[0], nodes[2]]:
            new_token_count = get_fresh_token_count(node)
            assert new_token_count > initial_tokens[node.node_name], \
                f'{node.node_name} should have gained tokens after node2 died'

        total_after = sum(get_fresh_token_count(node) for node in [nodes[0], nodes[2]])
        assert total_after == 100, \
            'All 100 tokens should be redistributed to surviving nodes'


def test_lock_registration_and_enforcement(postgres):
    """Verify tokens locked to 'special-%' go only to the matching node.

    Mutation: matches_pattern treating '%' as a literal, or
        compute_minimal_move_distribution ignoring locked_tokens.
    Oracle: 'special-%' matches special-alpha and neither node1 nor node2.
    """
    config = get_coordination_config(total_tokens=100)
    tables = schema.get_table_names(config.appname)

    def register_locks(job):
        locks = [(i, 'special-%', 'test lock') for i in range(10)]
        job.register_locks_bulk(locks)

    node1 = create_job(
        'node1',
        postgres,
        coordination_config=config,
        wait_on_enter=15,
        lock_provider=register_locks)
    node2 = create_job(
        'node2',
        postgres,
        coordination_config=config,
        wait_on_enter=15,
        lock_provider=register_locks)
    special = create_job(
        'special-alpha',
        postgres,
        coordination_config=config,
        wait_on_enter=15,
        lock_provider=register_locks)

    try:
        node1.__enter__()
        node2.__enter__()
        special.__enter__()

        assert wait_for_cached_tokens_sync(
            [node1, node2, special],
            expected_total=100,
            timeout_sec=15)

        locked_token_owners = {}
        for task_id in range(10):
            token_id = node1.task_to_token(task_id)

            with postgres.connect() as conn:
                result = conn.execute(
                    text(f'select node from {tables["Token"]} where token_id = :token_id'),
                    {'token_id': token_id})
                owner = result.scalar()
            locked_token_owners[task_id] = owner

        for task_id, owner in locked_token_owners.items():
            assert owner == 'special-alpha', \
                f'Locked task {task_id} should be assigned to special-alpha'

        for task_id in range(10):
            task = create_task(task_id)
            assert not node1.can_claim_task(task), \
                f'node1 should not claim locked task {task_id}'
            assert not node2.can_claim_task(task), \
                f'node2 should not claim locked task {task_id}'
            assert special.can_claim_task(task), \
                f'special-alpha should claim locked task {task_id}'

    finally:
        node1.__exit__(None, None, None)
        node2.__exit__(None, None, None)
        special.__exit__(None, None, None)


def test_health_monitoring(postgres):
    """Verify am_i_healthy turns False once the heartbeat passes its timeout.

    Mutation: am_i_healthy comparing the heartbeat age against
        heartbeat_interval_sec, or the age comparison flipped.
    Oracle: heartbeat_timeout_sec=10, heartbeat_interval_sec=1; ages of 5s
        and 15s sit either side of the timeout.
    """
    config = get_coordination_config(heartbeat_timeout_sec=10)

    node = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)
    node.__enter__()

    try:
        heartbeat = node._monitors['heartbeat']
        heartbeat.stop()
        heartbeat.thread.join(timeout=5)
        assert not heartbeat.thread.is_alive(), \
            'Heartbeat thread should stop before the test sets its age'

        now = datetime.datetime.now(datetime.timezone.utc)
        node.cluster.last_heartbeat_sent = now - timedelta(seconds=5)
        assert node.am_i_healthy(), 'A 5s-old heartbeat is within the 10s timeout'

        node.cluster.last_heartbeat_sent = now - timedelta(seconds=15)
        assert not node.am_i_healthy(), 'A 15s-old heartbeat is past the 10s timeout'

    finally:
        node.__exit__(None, None, None)


def test_leader_failover(postgres):
    """Verify the next-oldest node promotes itself once the leader leaves.

    Mutation: TokenRefreshMonitor never publishing leadership_promoted, or
        the leadership_promoted event left unhandled.
    Oracle: node2 is the oldest node left once node1's row is deleted;
        node3 stays a follower.
    """
    config = get_coordination_config()

    nodes = []
    try:
        for i in range(1, 4):
            node = create_job(
                f'node{i}',
                postgres,
                coordination_config=config,
                wait_on_enter=15)
            nodes.append(node)
            node.__enter__()

        assert wait_for_state(nodes[0], JobState.RUNNING_LEADER, timeout_sec=10)
        wait_for_leader_election(nodes[0], expected_leader='node1', timeout_sec=5)

        simulate_node_crash(nodes[0], cleanup=True)

        assert wait_for_state(nodes[1], JobState.RUNNING_LEADER, timeout_sec=15), \
            'node2 should promote itself to leader after node1 leaves'
        wait_for_leader_election(nodes[2], expected_leader='node2', timeout_sec=5)
        assert nodes[2].state_machine.state == JobState.RUNNING_FOLLOWER, \
            'node3 should stay a follower'

    finally:
        for node in nodes:
            try:
                node.__exit__(None, None, None)
            except Exception:
                pass


def test_follower_promotion_starts_leader_duties(postgres):
    """Verify a follower promoted after a leader crash reclaims its tokens.

    Mutation: _on_enter_running_leader not starting DeadNodeMonitor, so
        the crashed leader's row and tokens are never reclaimed.
    Oracle: 100 tokens over the 2 survivors is 50 each, logged by node2.
    """
    config = get_coordination_config(total_tokens=100)
    tables = schema.get_table_names(config.appname)

    nodes = []
    try:
        for i in range(1, 4):
            node = create_job(
                f'node{i}',
                postgres,
                coordination_config=config,
                wait_on_enter=15)
            nodes.append(node)
            node.__enter__()

        wait_for_leader_election(nodes[0], expected_leader='node1', timeout_sec=10)

        assert wait_for_cluster_running(nodes, leader_name='node1')

        assert wait_for_cached_tokens_sync(nodes, expected_total=100, timeout_sec=10)

        initial_tokens = {node.node_name: len(node.my_tokens) for node in nodes}
        assert sum(initial_tokens.values()) == 100, \
            'All 100 tokens should be distributed'

        simulate_node_crash(nodes[0])

        assert wait_for_dead_node_removal(postgres, tables, 'node1', timeout_sec=20), \
            'Dead leader should be removed from Node table by new leader'
        assert wait_for_state(nodes[1], JobState.RUNNING_LEADER, timeout_sec=5), \
            'node2 should be running as leader'

        assert wait_for_all_nodes_token_sync(
            [nodes[1], nodes[2]],
            expected_total=100,
            timeout_sec=10)

        final_tokens = {}
        for node in [nodes[1], nodes[2]]:
            token_count = get_fresh_token_count(node)
            final_tokens[node.node_name] = token_count

        assert final_tokens == {'node2': 50, 'node3': 50}, \
            f'Survivors should split 100 tokens evenly, got {final_tokens}'

        def node2_rebalance_count():
            with postgres.connect() as conn:
                sql = f"""
                select count(*) from {tables['Rebalance']}
                where leader_node = 'node2'
                """
                return conn.execute(text(sql)).scalar()

        assert wait_for(lambda: node2_rebalance_count() > 0, timeout_sec=10), \
            'New leader should have logged a rebalance'

    finally:
        for node in nodes:
            try:
                node.__exit__(None, None, None)
            except Exception:
                pass


class TestCleanupFailureScenarios:
    """Test cleanup behavior under failure conditions.
    """

    @clean_tables('Audit')
    def test_cleanup_with_pending_tasks_writes_audit(self, postgres):
        """Verify __exit__ writes each pending task to the Audit table.

        Mutation: the 'audit' step dropped from Job._cleanup, or
            write_audit inserting only the first pending task.
        Oracle: the two hand-queued tasks, ids 1 and 2.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)
        job.__enter__()

        task1 = create_task(1)
        task2 = create_task(2)
        job.tasks._tasks = [(task1, job._created_on), (task2, job._created_on)]

        job.__exit__(None, None, None)

        with postgres.connect() as conn:
            audit_rows = conn.execute(
                text(f'select node, task_id from {tables["Audit"]}')).all()

        assert (sorted(tuple(row) for row in audit_rows)
                == [('node1', '1'), ('node1', '2')]), \
            f'Both pending tasks should be audited once, got {audit_rows}'

    @clean_tables('Audit')
    def test_double_cleanup_is_safe(self, postgres):
        """Verify a second __exit__ raises nothing and audits no task twice.

        Mutation: write_audit leaving self._tasks uncleared after the flush.
        Oracle: one queued task yields exactly one Audit row.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)
        job.__enter__()
        job.tasks._tasks = [(create_task(1), job._created_on)]

        job.__exit__(None, None, None)
        job.__exit__(None, None, None)

        with postgres.connect() as conn:
            audit_count = conn.execute(
                text(f'select count(*) from {tables["Audit"]}')).scalar()

        assert audit_count == 1, \
            f'The queued task should be audited once, got {audit_count} rows'

    def test_cleanup_clears_all_node_data(self, postgres):
        """Verify __exit__ removes the node's Node, Claim and Check rows.

        Mutation: TaskManager.cleanup dropping its Check or Claim DELETE,
            or the 'cluster' step dropped from Job._cleanup.
        Oracle: one row per table for node1, counted before and after exit.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        def node1_row_counts():
            with postgres.connect() as conn:
                return tuple(
                    conn.execute(
                        text(f"select count(*) from {tables[key]} where {column} = 'node1'")).scalar()
                    for key, column in [
                        ('Node', 'name'),
                        ('Claim', 'node'),
                        ('Check', 'node'),
                        ])

        with create_job(
            'node1',
            postgres,
            coordination_config=config,
            wait_on_enter=0) as job:
            job.set_claim('test-item')
            with postgres.connect() as conn:
                conn.execute(
                    text(f"insert into {tables['Check']} (node, created_on) values ('node1', now())"))
                conn.commit()
            assert node1_row_counts() == (1, 1, 1), \
                'Setup should leave one Node, Claim and Check row'

        assert node1_row_counts() == (0, 0, 0), \
            'Node, Claim and Check rows should all be removed'

    def test_cleanup_completes_despite_exception(self, postgres, caplog):
        """Verify a failing cleanup step does not stop the steps after it.

        Mutation: one try/except around the whole cleanup_steps loop in
            Job._cleanup in place of one per step.
        Oracle: the first step, audit, is stubbed to raise; the Node and
            Claim rows that later steps delete must be gone.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)
        job.__enter__()
        job.set_claim('test-item')

        def failing_write_audit():
            raise RuntimeError('simulated audit failure')

        job.tasks.write_audit = failing_write_audit

        with caplog.at_level(logging.ERROR):
            job.__exit__(None, None, None)

        assert any(
            'audit cleanup failed' in record.message for record in caplog.records), \
            'The audit step failure should be logged'

        with postgres.connect() as conn:
            node_count = conn.execute(
                text(f"select count(*) from {tables['Node']} where name = 'node1'")).scalar()
            claim_count = conn.execute(
                text(f"select count(*) from {tables['Claim']} where node = 'node1'")).scalar()

        assert node_count == 0, \
            'The cluster step should still run after the audit step fails'
        assert claim_count == 0, \
            'The tasks step should still run after the audit step fails'


class TestRebalanceLockStaleRecovery:
    """Test stale rebalance lock detection and recovery.
    """

    def test_stale_rebalance_lock_detected_and_removed(self, postgres):
        """Verify a rebalance lock past its stale age is taken over.

        Mutation: the age comparison in _check_and_clear_stale_lock
            flipped, so a stale lock is never cleared.
        Oracle: a lock started 400s ago against a 300s threshold.
        """
        tables = schema.get_table_names('sync_')

        stale_time = (
            datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(seconds=400))

        with postgres.connect() as conn:
            sql = f"""
            update {tables["RebalanceLock"]}
            set in_progress = true, started_at = :started_at,
                started_by = 'dead-node'
            where singleton = 1
            """
            conn.execute(text(sql), {'started_at': stale_time})
            conn.commit()

        coord_config = CoordinationConfig(
            total_tokens=50,
            heartbeat_interval_sec=1,
            stale_rebalance_lock_age_sec=300)

        job = create_job(
            'node1',
            postgres,
            wait_on_enter=0,
            coordination_config=coord_config)

        try:
            with job.locks.acquire_rebalance_lock('test-rebalance'):
                with postgres.connect() as conn:
                    sql = f"""
                    select in_progress, started_by from {tables['RebalanceLock']}
                    where singleton = 1
                    """
                    result = conn.execute(text(sql))
                    lock_status = result.first()

                assert lock_status[0], 'Lock should be in progress'
                assert lock_status[1] == 'test-rebalance', \
                    'test-rebalance should now hold the lock'

        finally:
            job.__exit__(None, None, None)

    @pytest.mark.parametrize(
        ('threshold_sec', 'expect_acquired'),
        [(10, True), (20, False)])
    def test_configurable_stale_rebalance_lock_threshold(
        self,
        postgres,
        threshold_sec,
        expect_acquired):
        """Verify stale_rebalance_lock_age_sec decides if a 15s lock is stale.

        Mutation: LockManager ignoring stale_rebalance_lock_age_sec, or
            reading stale_leader_lock_age_sec in its place (300s default).
        Oracle: thresholds of 10s and 20s either side of the lock's 15s age.
        """
        tables = schema.get_table_names('sync_')

        coord_config = CoordinationConfig(
            total_tokens=50,
            heartbeat_interval_sec=1,
            stale_rebalance_lock_age_sec=threshold_sec)

        job = create_job(
            'node1',
            postgres,
            coordination_config=coord_config,
            wait_on_enter=0)

        lock_age = (
            datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(seconds=15))

        with postgres.connect() as conn:
            sql = f"""
            update {tables["RebalanceLock"]}
            set in_progress = true, started_at = :started_at,
                started_by = 'old-node'
            where singleton = 1
            """
            conn.execute(text(sql), {'started_at': lock_age})
            conn.commit()

        try:
            try:
                with job.locks.acquire_rebalance_lock('test'):
                    pass
                acquired = True
            except LockNotAcquired:
                acquired = False

            assert acquired == expect_acquired, \
                f'15s-old lock with a {threshold_sec}s threshold: acquired={acquired}'

        finally:
            job.__exit__(None, None, None)

    def test_non_stale_rebalance_lock_not_removed(self, postgres):
        """Verify a fresh rebalance lock blocks acquisition, holder kept.

        Mutation: _try_acquire_rebalance_lock dropping the in_progress =
            FALSE guard, or _check_and_clear_stale_lock ignoring the age.
        Oracle: a lock started now against a 300s threshold.
        """
        tables = schema.get_table_names('sync_')

        recent_time = datetime.datetime.now(datetime.timezone.utc)

        with postgres.connect() as conn:
            sql = f"""
            update {tables["RebalanceLock"]}
            set in_progress = true, started_at = :started_at,
                started_by = 'active-node'
            where singleton = 1
            """
            conn.execute(text(sql), {'started_at': recent_time})
            conn.commit()

        coord_config = CoordinationConfig(
            total_tokens=50,
            heartbeat_interval_sec=1,
            stale_rebalance_lock_age_sec=300)

        job = create_job(
            'node1',
            postgres,
            coordination_config=coord_config,
            wait_on_enter=0)

        try:
            with pytest.raises(LockNotAcquired):
                with job.locks.acquire_rebalance_lock('test'):
                    pass

            with postgres.connect() as conn:
                sql = f"""
                select started_by from {tables["RebalanceLock"]}
                where singleton = 1 and in_progress
                """
                holder = conn.execute(text(sql)).scalar()

            assert holder == 'active-node', \
                f'active-node should still hold the lock, got {holder}'

        finally:
            job.__exit__(None, None, None)

    def test_stale_rebalance_lock_logged(self, postgres, caplog):
        """Verify clearing a stale rebalance lock logs the holder's name.

        Mutation: the stale-lock warning dropped, or logged without the
            holder name.
        Oracle: the seeded holder 'stuck-node' and a 400s age against 300s.
        """
        tables = schema.get_table_names('sync_')

        stale_time = (
            datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(seconds=400))

        with postgres.connect() as conn:
            sql = f"""
            update {tables["RebalanceLock"]}
            set in_progress = true, started_at = :started_at,
                started_by = 'stuck-node'
            where singleton = 1
            """
            conn.execute(text(sql), {'started_at': stale_time})
            conn.commit()

        coord_config = CoordinationConfig(
            total_tokens=50,
            stale_rebalance_lock_age_sec=300)

        with caplog.at_level(logging.WARNING):
            job = create_job(
                'node1',
                postgres,
                coordination_config=coord_config,
                wait_on_enter=0)

            try:
                with job.locks.acquire_rebalance_lock('test'):
                    pass

                warning_messages = [
                    record.message for record in caplog.records
                    if record.levelname == 'WARNING'
                    ]
                stale_lock_warnings = [
                    msg for msg in warning_messages
                    if 'Stale rebalance lock detected' in msg
                    ]

                assert len(stale_lock_warnings) > 0, \
                    'Should log warning about stale rebalance lock'
                assert any('stuck-node' in msg for msg in stale_lock_warnings), \
                    'Warning should mention the stuck node'

            finally:
                job.__exit__(None, None, None)


class TestDeadNodeLockCleanup:
    """Test lock cleanup when nodes die.
    """

    @clean_tables('Lock', 'Node')
    def test_dead_node_locks_cleaned_up(self, postgres):
        """Verify dead-node cleanup deletes the dead node's locks only.

        Mutation: the Lock DELETE in DeadNodeMonitor.check dropped, or run
            without its created_by filter.
        Oracle: 3 locks by 'dead-node' and 1 by 'other-node', seeded.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        insert_stale_node(postgres, tables, 'dead-node', heartbeat_age_seconds=30)

        for token_id in [1, 2, 3]:
            insert_lock(
                postgres,
                tables,
                token_id,
                ['pattern-test'],
                created_by='dead-node')

        insert_lock(postgres, tables, 99, ['pattern-other'], created_by='other-node')

        coord_config = CoordinationConfig(
            total_tokens=50,
            heartbeat_timeout_sec=15,
            dead_node_check_interval_sec=0.5)

        job = create_job(
            'leader-node',
            postgres,
            wait_on_enter=2,
            coordination_config=coord_config)
        job.__enter__()

        try:
            assert wait_for_dead_node_removal(
                postgres,
                tables,
                'dead-node',
                timeout_sec=10)

            with postgres.connect() as conn:
                sql = f"""
                select task_id from {tables["Lock"]} where created_by = 'dead-node'
                """
                result = conn.execute(text(sql))
                dead_node_locks = [row[0] for row in result]

                sql = f"""
                select task_id from {tables["Lock"]} where created_by = 'other-node'
                """
                result = conn.execute(text(sql))
                other_node_locks = [row[0] for row in result]

            assert len(dead_node_locks) == 0, \
                'All locks from dead node should be removed'
            assert len(other_node_locks) == 1, 'Locks from other nodes should remain'
            assert other_node_locks[0] == '99', \
                'Lock 99 from other-node should still exist'

        finally:
            job.__exit__(None, None, None)

    @clean_tables('Lock', 'Node')
    def test_multiple_dead_nodes_all_locks_cleaned(self, postgres):
        """Verify dead-node cleanup removes every dead node and its locks.

        Mutation: the Lock DELETE in DeadNodeMonitor.check bound to
            dead_nodes[0] in place of the loop's node.
        Oracle: 3 seeded dead nodes with 2 locks each, 6 locks in all.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        for i in range(1, 4):
            node_name = f'dead-node-{i}'
            insert_stale_node(postgres, tables, node_name, heartbeat_age_seconds=30)

            for j in range(2):
                token_id = i * 10 + j
                insert_lock(
                    postgres,
                    tables,
                    token_id,
                    ['pattern'],
                    created_by=node_name)

        coord_config = CoordinationConfig(
            total_tokens=50,
            heartbeat_timeout_sec=15,
            dead_node_check_interval_sec=0.5)

        job = create_job(
            'cleanup-leader',
            postgres,
            wait_on_enter=2,
            coordination_config=coord_config)
        job.__enter__()

        try:
            for i in range(1, 4):
                assert wait_for_dead_node_removal(
                    postgres,
                    tables,
                    f'dead-node-{i}',
                    timeout_sec=10)

            with postgres.connect() as conn:
                result = conn.execute(text(f'select count(*) from {tables["Lock"]}'))
                remaining_locks = result.scalar()

            assert remaining_locks == 0, \
                'All locks from all dead nodes should be removed'

        finally:
            job.__exit__(None, None, None)

    @clean_tables('Lock', 'Node')
    def test_lock_cleanup_logged(self, postgres, caplog):
        """Verify dead-node cleanup logs an INFO line naming the dead node.

        Mutation: the 'Removed dead node and cleaned up locks' log dropped
            or logged without the node name.
        Oracle: the seeded node name 'logged-dead-node'.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        insert_stale_node(
            postgres,
            tables,
            'logged-dead-node',
            heartbeat_age_seconds=30)
        insert_lock(postgres, tables, 1, ['pattern'], created_by='logged-dead-node')

        coord_config = CoordinationConfig(
            total_tokens=50,
            heartbeat_timeout_sec=15,
            dead_node_check_interval_sec=0.5)

        with caplog.at_level(logging.INFO):
            job = create_job(
                'logging-leader',
                postgres,
                wait_on_enter=2,
                coordination_config=coord_config)
            job.__enter__()

            try:
                assert wait_for_dead_node_removal(
                    postgres,
                    tables,
                    'logged-dead-node',
                    timeout_sec=10)

                info_messages = [
                    record.message for record in caplog.records
                    if record.levelname == 'INFO'
                    ]
                cleanup_messages = [
                    msg for msg in info_messages if 'cleaned up locks' in msg.lower()
                    ]

                assert len(cleanup_messages) > 0, 'Should log lock cleanup'
                assert any('logged-dead-node' in msg for msg in cleanup_messages), \
                    'Log should mention the dead node'

            finally:
                job.__exit__(None, None, None)

    @clean_tables('Lock', 'Node')
    def test_expired_locks_cleaned_during_dead_node_rebalance(self, postgres):
        """Verify a dead-node rebalance drops its locks and expired locks.

        Mutation: the dead_nodes_detected event no longer calling
            _distribute_tokens_safe, so get_active_locks never runs again.
        Oracle: both locks are seeded after the leader's initial
            distribution, so only the dead-node rebalance can delete the
            expired one.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        coord_config = CoordinationConfig(
            total_tokens=50,
            heartbeat_timeout_sec=15,
            dead_node_check_interval_sec=0.5)

        job = create_job(
            'separation-leader',
            postgres,
            wait_on_enter=0,
            coordination_config=coord_config)
        job.__enter__()

        try:
            assert wait_for_state(job, JobState.RUNNING_LEADER, timeout_sec=5)

            expired_time = (
                datetime.datetime.now(datetime.timezone.utc)
                - datetime.timedelta(days=2))
            insert_lock(postgres, tables, 1, ['pattern'], created_by='dead-node')
            insert_lock(
                postgres,
                tables,
                2,
                ['pattern'],
                created_by='alive-node',
                expires_at=expired_time)
            insert_stale_node(postgres, tables, 'dead-node', heartbeat_age_seconds=30)

            def remaining_lock_ids():
                with postgres.connect() as conn:
                    return [
                        row[0] for row in conn.execute(
                            text(f'select task_id from {tables["Lock"]}'))
                        ]

            assert wait_for(lambda: remaining_lock_ids() == [], timeout_sec=10), \
                f'Dead-node and expired locks should both be deleted, left {remaining_lock_ids()}'

            def dead_node_rebalance_count():
                with postgres.connect() as conn:
                    sql = f"""
                    select count(*) from {tables['Rebalance']}
                    where trigger_reason = 'dead_nodes'
                    """
                    return conn.execute(text(sql)).scalar()

            assert wait_for(lambda: dead_node_rebalance_count() > 0, timeout_sec=5), \
                'The leader should log a dead_nodes rebalance'

        finally:
            job.__exit__(None, None, None)


class TestLeaderLockStaleRecovery:
    """Test stale leader lock detection and recovery.
    """

    def test_stale_lock_detected_and_removed(self, postgres):
        """Verify a leader lock past its stale age is taken over.

        Mutation: the age comparison in _check_and_clear_stale_lock
            flipped, so a stale leader lock is never cleared.
        Oracle: a lock acquired 400s ago against a 300s threshold.
        """
        tables = schema.get_table_names('sync_')

        stale_time = (
            datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(seconds=400))

        insert_leader_lock(
            postgres,
            tables,
            'dead-node',
            'stale-operation',
            acquired_at=stale_time)

        coord_config = CoordinationConfig(
            total_tokens=50,
            heartbeat_interval_sec=1,
            stale_leader_lock_age_sec=300,
            leader_lock_timeout_sec=2)

        job = create_job(
            'node1',
            postgres,
            coordination_config=coord_config,
            wait_on_enter=0)

        try:
            with job.locks.acquire_leader_lock('test-operation'):
                with postgres.connect() as conn:
                    result = conn.execute(
                        text(f"select node from {tables['LeaderLock']} where singleton = 1"))
                    current_holder = result.scalar()

                assert current_holder == 'node1', 'node1 should now hold the lock'

        finally:
            job.__exit__(None, None, None)

    @pytest.mark.parametrize(
        ('threshold_sec', 'expect_acquired'),
        [(10, True), (20, False)])
    @clean_tables('LeaderLock')
    def test_configurable_stale_lock_threshold(
        self,
        postgres,
        threshold_sec,
        expect_acquired):
        """Verify stale_leader_lock_age_sec decides if a 15s-old lock is stale.

        Mutation: LockManager ignoring stale_leader_lock_age_sec, or
            reading stale_rebalance_lock_age_sec in its place (300s default).
        Oracle: thresholds of 10s and 20s either side of the lock's 15s age.
        """
        tables = schema.get_table_names('sync_')

        coord_config = CoordinationConfig(
            total_tokens=50,
            heartbeat_interval_sec=1,
            stale_leader_lock_age_sec=threshold_sec,
            leader_lock_timeout_sec=2)

        job = create_job(
            'node1',
            postgres,
            coordination_config=coord_config,
            wait_on_enter=0)

        lock_age = (
            datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(seconds=15))
        insert_leader_lock(
            postgres,
            tables,
            'old-node',
            'old-operation',
            acquired_at=lock_age)

        try:
            try:
                with job.locks.acquire_leader_lock('test'):
                    pass
                acquired = True
            except LockNotAcquired:
                acquired = False

            assert acquired == expect_acquired, \
                f'15s-old lock with a {threshold_sec}s threshold: acquired={acquired}'

        finally:
            job.__exit__(None, None, None)

    @clean_tables('LeaderLock')
    def test_non_stale_lock_not_removed(self, postgres):
        """Verify a fresh leader lock blocks acquisition and keeps its holder.

        Mutation: _check_and_clear_stale_lock ignoring the age, or the
            leader lock INSERT made an upsert that overwrites the holder.
        Oracle: a lock acquired now against a 300s threshold.
        """
        tables = schema.get_table_names('sync_')

        recent_time = datetime.datetime.now(datetime.timezone.utc)
        insert_leader_lock(
            postgres,
            tables,
            'active-node',
            'active-operation',
            acquired_at=recent_time)

        coord_config = CoordinationConfig(
            total_tokens=50,
            heartbeat_interval_sec=1,
            stale_leader_lock_age_sec=300,
            leader_lock_timeout_sec=2)

        job = create_job(
            'node1',
            postgres,
            coordination_config=coord_config,
            wait_on_enter=0)

        try:
            with pytest.raises(LockNotAcquired):
                with job.locks.acquire_leader_lock('test'):
                    pass

            with postgres.connect() as conn:
                holder = conn.execute(
                    text(f"select node from {tables['LeaderLock']} where singleton = 1")).scalar()

            assert holder == 'active-node', \
                f'active-node should still hold the lock, got {holder}'

        finally:
            job.__exit__(None, None, None)


class TestThreadCrashAndRecovery:
    """Test thread failure detection and recovery.
    """

    def test_health_monitor_detects_stale_heartbeat(self, postgres):
        """Verify HealthMonitor shuts the node down on a stale heartbeat.

        Mutation: HealthMonitor.check with its age comparison flipped, or
            comparing against heartbeat_interval_sec (1s here).
        Oracle: heartbeat_timeout_sec=15; a 5s-old heartbeat must not trip
            it and a 30s-old one must.
        """
        coord_config = CoordinationConfig(
            total_tokens=50,
            heartbeat_interval_sec=1,
            heartbeat_timeout_sec=15,
            health_check_interval_sec=1)

        job = create_job(
            'node1',
            postgres,
            coordination_config=coord_config,
            wait_on_enter=0)
        job.__enter__()

        try:
            heartbeat = job._monitors['heartbeat']
            heartbeat.stop()
            heartbeat.thread.join(timeout=5)
            assert not heartbeat.thread.is_alive(), \
                'Heartbeat thread should stop before the test sets its age'

            now = datetime.datetime.now(datetime.timezone.utc)
            job.cluster.last_heartbeat_sent = now - datetime.timedelta(seconds=5)
            assert not wait_for(job._shutdown_event.is_set, timeout_sec=3), \
                'A 5s-old heartbeat is within the 15s timeout and should not trigger shutdown'

            job.cluster.last_heartbeat_sent = now - datetime.timedelta(seconds=30)

            assert wait_for(job._shutdown_event.is_set, timeout_sec=5), \
                'Shutdown event should be set after stale heartbeat detected'
            assert wait_for(
                lambda: job.state_machine.state == JobState.SHUTTING_DOWN,
                timeout_sec=1), \
                'Should transition to SHUTTING_DOWN state'

        finally:
            job.__exit__(None, None, None)


class TestLockExpirationSideEffects:
    """Test lock expiration handling during operations.
    """

    @clean_tables('Lock')
    def test_expired_locks_deleted_during_get_active_locks(self, postgres):
        """Verify get_active_locks deletes only locks past expires_at.

        Mutation: the expiry DELETE missing its expires_at < NOW() term, or
            with the comparison flipped.
        Oracle: three seeded locks, expired 2 days ago, never expiring,
            and expiring in 1 day; only the first may go.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        now = datetime.datetime.now(datetime.timezone.utc)
        insert_lock(
            postgres,
            tables,
            1,
            ['pattern-1'],
            created_by='node1',
            expires_at=now - datetime.timedelta(days=2))
        insert_lock(
            postgres,
            tables,
            2,
            ['pattern-2'],
            created_by='node1',
            expires_at=None)
        insert_lock(
            postgres,
            tables,
            3,
            ['pattern-3'],
            created_by='node1',
            expires_at=now + datetime.timedelta(days=1))

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)

        try:
            active_locks = job.locks.get_active_locks()

            expected = {
                job.task_to_token('2'): ['pattern-2'],
                job.task_to_token('3'): ['pattern-3'],
                }
            assert active_locks == expected, \
                f'Only unexpired locks should be active, got {active_locks}'

            with postgres.connect() as conn:
                remaining = sorted(
                    row[0] for row in conn.execute(
                        text(f'select task_id from {tables["Lock"]}')))

            assert remaining == ['2', '3'], \
                f'Only the expired lock should be deleted, left {remaining}'

        finally:
            job.__exit__(None, None, None)

    @clean_tables('Lock', 'Node')
    def test_expired_lock_releases_token_at_next_distribution(self, postgres):
        """Verify a token locked to an absent node is assigned after expiry.

        Mutation: get_active_locks skipping the expiry DELETE, so
            distribute keeps the token blocked after the lock expires.
        Oracle: pattern 'ghost-node' matches no active node, so the token
            is unowned while the lock holds and owned by node1 after.
        """
        config = get_coordination_config(total_tokens=50)
        tables = schema.get_table_names(config.appname)

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)
        expires_at = (
            datetime.datetime.now(datetime.timezone.utc)
            + datetime.timedelta(days=1))
        insert_lock(
            postgres,
            tables,
            5,
            ['ghost-node'],
            created_by='test',
            expires_at=expires_at,
            reason='about to expire')

        try:
            job.__enter__()
            token_id = job.task_to_token('5')

            assert token_id not in get_token_assignments(postgres, tables), \
                'The locked token should stay unowned while its lock holds'

            with postgres.connect() as conn:
                conn.execute(
                    text(f"update {tables['Lock']} set expires_at = now() - interval '1 second'"))
                conn.commit()
            job.tokens.distribute(job.locks, job.cluster)

            assert get_token_assignments(postgres, tables).get(token_id) == 'node1', \
                'The token should go to node1 once its lock expires'

            with postgres.connect() as conn:
                lock_count = conn.execute(
                    text(f'select count(*) from {tables["Lock"]}')).scalar()
            assert lock_count == 0, 'The expired lock should be deleted'

        finally:
            job.__exit__(None, None, None)


class TestTokenDistributionUnderContention:
    """Test token distribution with lock contention and failures.
    """

    @clean_tables('LeaderLock')
    def test_leader_lock_timeout_behavior(self, postgres):
        """Verify a fresh held leader lock makes acquisition time out.

        Mutation: the retry loop ignoring leader_lock_timeout_sec, or the
            stale check clearing a fresh lock.
        Oracle: leader_lock_timeout_sec=2 bounds the wait; the seeded holder
            'other-node' is younger than stale_leader_lock_age_sec=300.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)
        insert_leader_lock(postgres, tables, 'other-node', 'long-operation')

        coord_config = CoordinationConfig(
            leader_lock_timeout_sec=2,
            stale_leader_lock_age_sec=300)

        job = create_job(
            'node1',
            postgres,
            coordination_config=coord_config,
            wait_on_enter=0)

        try:
            start_time = time.time()
            with pytest.raises(
                LockNotAcquired,
                match='Another leader is performing test'):
                with job.locks.acquire_leader_lock('test'):
                    pass
            elapsed = time.time() - start_time

            assert elapsed >= 2, f'Should wait for timeout (took {elapsed:.1f}s)'
            assert elapsed < 5, f'Should timeout quickly (took {elapsed:.1f}s)'

            with postgres.connect() as conn:
                result = conn.execute(
                    text(f"select node from {tables['LeaderLock']} where singleton = 1"))
                holder = result.scalar()

            assert holder == 'other-node', 'Lock holder should not change on timeout'
        finally:
            job.__exit__(None, None, None)

    @clean_tables('Lock', 'Node')
    def test_all_tokens_locked_to_nonexistent_pattern(self, postgres):
        """Verify a token locked to no active node stays unassigned.

        Mutation: categorize_tokens_by_locks sending blocked tokens to the
            distributable pool, or get_active_locks dropping the locks.
        Oracle: every one of the 20 tokens is locked to 'nonexistent-%' and
            the active nodes are node1 and node2, so zero rows is expected.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        now = datetime.datetime.now(datetime.timezone.utc)

        insert_active_node(postgres, tables, 'node1', created_on=now)
        insert_active_node(
            postgres,
            tables,
            'node2',
            created_on=now + datetime.timedelta(seconds=1))

        coord_config = CoordinationConfig(total_tokens=20)
        task_ids = find_task_ids_covering_all_tokens(coord_config)
        for task_id in task_ids:
            insert_lock(postgres, tables, task_id, ['nonexistent-%'], created_by='test')

        job = create_job(
            'node1',
            postgres,
            coordination_config=coord_config,
            wait_on_enter=0)

        try:
            job.tokens.distribute(job.locks, job.cluster)

            with postgres.connect() as conn:
                result = conn.execute(text(f'select count(*) from {tables["Token"]}'))
                assigned_count = result.scalar()

            assert assigned_count == 0, \
                'No tokens should be assigned when pattern matches no nodes'
        finally:
            job.__exit__(None, None, None)

    @clean_tables('Lock', 'Token', 'Node')
    def test_partial_lock_pattern_matching(self, postgres):
        """Verify locks pin, unmatched locks block, and the rest balance.

        Mutation: a locked token handed to a non-matching node, a blocked
            token assigned anyway, or '%' not treated as a wildcard.
        Oracle: tokens 0-9 locked to 'special-%', 10-19 to 'missing-%'; the
            30 unlocked tokens split 10/10/10 over 3 nodes by hand.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        now = datetime.datetime.now(datetime.timezone.utc)

        insert_active_node(postgres, tables, 'node1', created_on=now)
        insert_active_node(
            postgres,
            tables,
            'node2',
            created_on=now + datetime.timedelta(seconds=1))
        insert_active_node(
            postgres,
            tables,
            'special-node',
            created_on=now + datetime.timedelta(seconds=2))

        coord_config = CoordinationConfig(total_tokens=50)
        task_ids = find_task_ids_covering_all_tokens(coord_config)
        for token_id in range(10):
            insert_lock(
                postgres,
                tables,
                task_ids[token_id],
                ['special-%'],
                created_by='test')
        for token_id in range(10, 20):
            insert_lock(
                postgres,
                tables,
                task_ids[token_id],
                ['missing-%'],
                created_by='test')

        job = create_job(
            'node1',
            postgres,
            coordination_config=coord_config,
            wait_on_enter=0)

        try:
            job.tokens.distribute(job.locks, job.cluster)

            assignments = get_token_assignments(postgres, tables)
            unlocked_count_by_node = {}
            for token_id in range(20, 50):
                node = assignments.get(token_id)
                unlocked_count_by_node[node] = unlocked_count_by_node.get(node, 0) + 1

            assert ({assignments.get(token_id) for token_id in range(10)}
                    == {'special-node'}), \
                'Tokens locked to special-% should all go to special-node'
            assert not set(range(10, 20)) & set(assignments), \
                'Tokens locked to missing-% should stay unowned'
            assert (unlocked_count_by_node
                    == {'node1': 10, 'node2': 10, 'special-node': 10}), \
                f'Unlocked tokens should split evenly, got {unlocked_count_by_node}'
        finally:
            job.__exit__(None, None, None)


class TestDatabaseConnectionFailures:
    """Test handling of database connection issues.
    """

    def test_can_claim_task_survives_db_failure(self, postgres, monkeypatch):
        """Verify can_claim_task answers from cache while the DB is down.

        Mutation: can_claim_task re-reading ownership from the database, or
            skipping the token check.
        Oracle: cached tokens set by hand to {0}; task_ids[0] hashes to
            token 0 and task_ids[1] to token 1; every engine.connect raises.
        """
        config = get_coordination_config(total_tokens=10)
        task_ids = find_task_ids_covering_all_tokens(config)

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)
        job.__enter__()

        def refuse_connection(*args, **kwargs):
            raise ConnectionError('database down')

        try:
            assert wait_for(
                lambda: job._monitors['token_refresh'].initial_callback_sent)
            job.tokens.my_tokens = {0}

            with monkeypatch.context() as patch:
                patch.setattr(job.db.engine, 'connect', refuse_connection)
                owned_claimable = job.can_claim_task(create_task(task_ids[0]))
                unowned_claimable = job.can_claim_task(create_task(task_ids[1]))

            assert owned_claimable, 'Task on a cached token should be claimable'
            assert not unowned_claimable, \
                'Task on an uncached token should not be claimable'
        finally:
            job.__exit__(None, None, None)


class TestAuditWriteFailures:
    """Test audit write error handling.
    """

    def test_write_audit_clears_tasks_after_write(self, postgres):
        """Verify write_audit writes each queued task exactly once.

        Mutation: write_audit not clearing the queue after the insert, or
            writing the wrong node or task_id.
        Oracle: two tasks queued by hand, so exactly the rows
            ('node1', '1') and ('node1', '2') are expected after two calls.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)
        job.__enter__()

        try:
            task1 = Task(1, 'task-1')
            task2 = Task(2, 'task-2')
            job.tasks._tasks = [(task1, job._created_on), (task2, job._created_on)]

            job.write_audit()
            job.write_audit()

            with postgres.connect() as conn:
                result = conn.execute(
                    text(f'select node, task_id from {tables["Audit"]}'))
                audit_rows = sorted(tuple(row) for row in result)

            assert audit_rows == [('node1', '1'), ('node1', '2')], \
                f'Each task should be audited exactly once, got {audit_rows}'
        finally:
            job.__exit__(None, None, None)


class TestDeadNodeTokenRedistribution:
    """Test token redistribution when coordinated node dies.
    """

    @clean_tables('Inst', 'Audit', 'Claim', 'Token', 'Node')
    def test_dead_node_tokens_redistributed_to_survivors(self, postgres):
        """Verify survivors take a crashed node's tokens and audit all tasks.

        Mutation: the leader ignoring dead_nodes_detected (only the
            membership_change path rebalances), or a rebalance that leaves
            tokens on the dead node.
        Oracle: 30 tokens over 3 nodes; after node2 stops heartbeating,
            node1 and node3 must own all 30 and audit task ids '0'-'29'.
        """
        tables = schema.get_table_names('sync_')

        with cluster(
            postgres,
            'node1',
            'node2',
            'node3',
            total_tokens=30,
            dead_node_check_interval_sec=2,
            heartbeat_timeout_sec=5) as nodes:
            for node in nodes:
                assert wait_for(lambda n=node: len(n.my_tokens) >= 5, timeout_sec=15)

            assert wait_for_cached_tokens_sync(
                nodes,
                expected_total=30,
                timeout_sec=10), \
                'Token caches should sync after cluster formation'

            simulate_node_crash(nodes[1], cleanup=False)

            with postgres.connect() as conn:
                sql = f"""
                select count(*) from {tables["Token"]} where node = 'node2'
                """
                result = conn.execute(text(sql))
                node2_token_count = result.scalar()

            assert node2_token_count > 0, 'Dead node should still own tokens initially'

            assert wait_for_dead_node_removal(postgres, tables, 'node2', timeout_sec=20)

            def has_dead_nodes_rebalance() -> bool:
                with postgres.connect() as conn:
                    sql = f"""
                    select count(*) from {tables["Rebalance"]}
                    where trigger_reason = 'dead_nodes'
                    """
                    return conn.execute(text(sql)).scalar() >= 1

            assert wait_for(has_dead_nodes_rebalance, timeout_sec=20), \
                'Rebalance audit should record dead_nodes as trigger reason'

            survivor_nodes = [nodes[0], nodes[2]]
            assert wait_for_cached_tokens_sync(
                survivor_nodes,
                expected_total=30,
                timeout_sec=20), \
                'Survivors should own and cache all 30 tokens after rebalancing'

            unclaimable = []
            for task_id in range(30):
                task = create_task(task_id)
                claimer = next(
                    (node for node in survivor_nodes if node.can_claim_task(task)),
                    None)
                if claimer is None:
                    unclaimable.append(task_id)
                else:
                    claimer.add_task(task)

            assert not unclaimable, f'No survivor can claim tasks {unclaimable}'

            for node in survivor_nodes:
                node.write_audit()

            with postgres.connect() as conn:
                result = conn.execute(
                    text(f'select node, task_id from {tables["Audit"]}'))
                audit_rows = list(result)

            assert ({row[1] for row in audit_rows}
                    == {str(task_id) for task_id in range(30)}), \
                'Survivors should audit every task'
            assert {row[0] for row in audit_rows} <= {'node1', 'node3'}, \
                'Only survivors should audit tasks'

    @clean_tables('Lock', 'Rebalance', 'Token', 'Node')
    def test_dead_node_rebalance_logs_node_counts(self, postgres):
        """Verify the rebalance row records 3 -> 2 after one of 3 nodes dies.

        Mutation: distribute passing the live node count as nodes_before.
        Oracle: the Rebalance table's nodes_before and nodes_after columns
            name the node counts on either side of the rebalance. Tokens
            are seeded across node1, node2 and node3, and only node1 and
            node2 are live.
        """
        tables = schema.get_table_names('sync_')

        insert_active_node(postgres, tables, 'node1')
        insert_active_node(postgres, tables, 'node2')
        for token_id in range(30):
            insert_token(postgres, tables, token_id, f'node{token_id % 3 + 1}')

        job = create_job(
            'node1',
            postgres,
            coordination_config=get_coordination_config(total_tokens=30))
        try:
            job.tokens.distribute(job.locks, job.cluster, 'dead_nodes')
        finally:
            job.__exit__(None, None, None)

        with postgres.connect() as conn:
            sql = f'select nodes_before, nodes_after from {tables["Rebalance"]}'
            rows = [tuple(row) for row in conn.execute(text(sql))]

        assert rows == [(3, 2)]


class TestLeadershipDemotion:
    """Test leader demotion when older node rejoins.
    """

    def test_leader_demoted_when_older_node_rejoins(self, postgres):
        """Verify the leader steps down to follower when an older node rejoins.

        Mutation: TokenRefreshMonitor not publishing leadership_demoted, or
            elect_leader not ordering by created_on.
        Oracle: the rejoined node1 carries the original, older created_on,
            so it must lead and node2 must follow.
        """
        config = get_coordination_config()

        node1 = create_job(
            'node1',
            postgres,
            coordination_config=config,
            wait_on_enter=10)
        node2 = create_job(
            'node2',
            postgres,
            coordination_config=config,
            wait_on_enter=10)
        node1_rejoined = None

        node1.__enter__()
        time.sleep(0.5)
        node2.__enter__()

        try:
            wait_for_leader_election(node1, expected_leader='node1', timeout_sec=10)

            simulate_node_crash(node1)

            wait_for_leader_election(node2, expected_leader='node2', timeout_sec=15)
            assert wait_for_state(node2, JobState.RUNNING_LEADER, timeout_sec=30)

            node1_rejoined = create_job(
                'node1',
                postgres,
                coordination_config=config,
                wait_on_enter=10)
            node1_rejoined._created_on = node1._created_on
            node1_rejoined.cluster.created_on = node1._created_on
            node1_rejoined.__enter__()

            assert wait_for_state(
                node1_rejoined,
                JobState.RUNNING_LEADER,
                timeout_sec=15)
            assert wait_for_state(node2, JobState.RUNNING_FOLLOWER, timeout_sec=15)
        finally:
            for node in (node1_rejoined, node2):
                if node is not None:
                    node.__exit__(None, None, None)
            node1.db.dispose()

    def test_demoted_leader_monitors_stop_gracefully(self, postgres):
        """Verify demotion ends the leader-only monitors and drops them.

        Mutation: _on_exit_running_leader not stopping the monitors, Monitor
            ignoring _stop_requested, or _stop_monitor keeping the entry.
        Oracle: the DeadNodeMonitor and RebalanceMonitor captured while
            node2 led; their threads must end within 5s of demotion.
        """
        config = get_coordination_config()

        node1 = create_job(
            'node1',
            postgres,
            coordination_config=config,
            wait_on_enter=10)
        node2 = create_job(
            'node2',
            postgres,
            coordination_config=config,
            wait_on_enter=10)
        node1_rejoined = None

        node1.__enter__()
        time.sleep(0.5)
        node2.__enter__()

        try:
            wait_for_leader_election(node1, expected_leader='node1', timeout_sec=10)

            simulate_node_crash(node1)

            wait_for_leader_election(node2, expected_leader='node2', timeout_sec=15)
            assert wait_for_state(node2, JobState.RUNNING_LEADER, timeout_sec=30)

            def running_leader_monitors() -> list:
                return [
                    monitor for monitor in list(node2._monitors.values())
                    if isinstance(monitor, (DeadNodeMonitor, RebalanceMonitor))
                    and monitor.thread is not None and monitor.thread.is_alive()
                    ]

            assert wait_for(
                lambda: len(running_leader_monitors()) == 2,
                timeout_sec=10), \
                'node2 should start DeadNodeMonitor and RebalanceMonitor on promotion'
            leader_monitors = running_leader_monitors()

            node1_rejoined = create_job(
                'node1',
                postgres,
                coordination_config=config,
                wait_on_enter=10)
            node1_rejoined._created_on = node1._created_on
            node1_rejoined.cluster.created_on = node1._created_on
            node1_rejoined.__enter__()

            wait_for_leader_election(
                node1_rejoined,
                expected_leader='node1',
                timeout_sec=15)
            assert wait_for_state(node2, JobState.RUNNING_FOLLOWER, timeout_sec=10)

            for monitor in leader_monitors:
                assert wait_for(
                    lambda m=monitor: not m.thread.is_alive(),
                    timeout_sec=5), \
                    f'{monitor.name} thread should end after demotion'
            assert not {'dead_node', 'rebalance'} & set(node2._monitors), \
                'Leader-only monitors should be removed after demotion'

            assert node2.am_i_healthy(), 'node2 should still be healthy after demotion'
        finally:
            for node in (node1_rejoined, node2):
                if node is not None:
                    node.__exit__(None, None, None)
            node1.db.dispose()

    def test_leader_death_with_leader_lock_held(self, postgres):
        """Verify a new leader clears a dead leader's stale lock.

        Mutation: _try_acquire_leader_lock skipping the stale-lock check, so
            the dead_nodes distribution times out on node1's lock.
        Oracle: node1's lock is older than stale_leader_lock_age_sec=5 by
            the time node2 leads, so node2 must end up owning all 30 tokens.
        """
        tables = schema.get_table_names('sync_')

        config = CoordinationConfig(
            total_tokens=30,
            stale_leader_lock_age_sec=5,
            leader_lock_timeout_sec=2)

        node1 = create_job(
            'node1',
            postgres,
            coordination_config=config,
            wait_on_enter=10)
        node2 = create_job(
            'node2',
            postgres,
            coordination_config=config,
            wait_on_enter=10)

        node1.__enter__()
        time.sleep(0.5)
        node2.__enter__()

        try:
            assert wait_for_state(node1, JobState.RUNNING_LEADER, timeout_sec=10)
            assert wait_for_state(node2, JobState.RUNNING_FOLLOWER, timeout_sec=10)

            with postgres.connect() as conn:
                sql = f"""
                insert into {tables["LeaderLock"]} (node, acquired_at, operation)
                values (:node, now(), 'test_operation')
                on conflict (singleton) do update
                set node = :node, acquired_at = now(), operation = 'test_operation'
                """
                conn.execute(text(sql), {'node': 'node1'})
                conn.commit()

            simulate_node_crash(node1)

            assert wait_for_state(node2, JobState.RUNNING_LEADER, timeout_sec=30)
            assert node2.am_i_leader(), 'node2 should be the active leader'

            def node2_owns_all_tokens() -> bool:
                with postgres.connect() as conn:
                    sql = f"""
                    select count(*) from {tables["Token"]} where node = 'node2'
                    """
                    result = conn.execute(text(sql))
                    return result.scalar() == 30

            assert wait_for(node2_owns_all_tokens, timeout_sec=20), \
                'node2 should redistribute node1 tokens past the stale leader lock'

            with postgres.connect() as conn:
                sql = f"""
                select count(*) from {tables["LeaderLock"]} where node = 'node1'
                """
                result = conn.execute(text(sql))
                node1_lock_count = result.scalar()

            assert node1_lock_count == 0, 'Stale node1 leader lock should be cleared'
        finally:
            node2.__exit__(None, None, None)
            node1.db.dispose()


def test_rebalance_detects_nodes_joining_after_distribution(postgres):
    """Verify each late join records a membership_change rebalance.

    Mutation: the membership_changed handler not forwarding
        'membership_change' as the trigger reason, or the remainder token
        going to the wrong node.
    Oracle: two joins give two membership_change rows; 100 tokens over
        node1-node3 split 34/33/33 by hand, the extra token to the first name.
    """
    tables = schema.get_table_names('sync_')

    for token_id in range(100):
        insert_token(postgres, tables, token_id, 'node1', version=1)

    coord_config = CoordinationConfig(
        total_tokens=100,
        rebalance_check_interval_sec=1)

    node1 = create_job(
        'node1',
        postgres,
        coordination_config=coord_config,
        wait_on_enter=0)
    node2 = None
    node3 = None
    node1.__enter__()

    try:
        assert wait_for_state(node1, JobState.RUNNING_LEADER, timeout_sec=10)

        node2 = create_job(
            'node2',
            postgres,
            coordination_config=coord_config,
            wait_on_enter=0)
        node3 = create_job(
            'node3',
            postgres,
            coordination_config=coord_config,
            wait_on_enter=0)

        node2.__enter__()
        node3.__enter__()

        assert wait_for_state(node2, JobState.RUNNING_FOLLOWER, timeout_sec=10)
        assert wait_for_state(node3, JobState.RUNNING_FOLLOWER, timeout_sec=10)

        def membership_change_count() -> int:
            with postgres.connect() as conn:
                sql = f"""
                select count(*) from {tables["Rebalance"]}
                where trigger_reason = 'membership_change'
                """
                return conn.execute(text(sql)).scalar()

        assert wait_for(lambda: membership_change_count() >= 2, timeout_sec=30), \
            'Each join should record a membership_change rebalance'

        assert wait_for_all_nodes_token_sync(
            [node1, node2, node3],
            expected_total=100,
            timeout_sec=20)

        with postgres.connect() as conn:
            sql = f"""
            select node, count(*) from {tables["Token"]} group by node
            """
            result = conn.execute(text(sql))
            final_distribution = {row[0]: row[1] for row in result}

        assert final_distribution == {'node1': 34, 'node2': 33, 'node3': 33}, \
            f'Unexpected final distribution {final_distribution}'
    finally:
        for node in (node1, node2, node3):
            if node is not None:
                node.__exit__(None, None, None)


class TestMinimumNodesRequirement:
    """Test minimum_nodes coordination during cluster formation.
    """

    @clean_tables('Node', 'Token')
    def test_wait_on_enter_waits_full_duration_even_after_minimum_nodes(self, postgres):
        """Verify __enter__ waits out wait_on_enter past minimum_nodes.

        Mutation: the grace loop breaking out as soon as minimum_nodes are
            active.
        Oracle: minimum_nodes=2 is met about 1s in, while wait_on_enter=10
            holds node1 in CLUSTER_FORMING for at least 10s.
        """
        coord_config = get_coordination_config(
            total_tokens=30,
            minimum_nodes=2,
            heartbeat_interval_sec=1)

        node1 = create_job(
            'node1',
            postgres,
            coordination_config=coord_config,
            wait_on_enter=10)
        node2 = create_job(
            'node2',
            postgres,
            coordination_config=coord_config,
            wait_on_enter=10)

        enter_start = time.time()
        node1_thread = threading.Thread(target=node1.__enter__)
        node1_thread.start()

        try:
            assert wait_for_state(node1, JobState.CLUSTER_FORMING, timeout_sec=5)

            node2_thread = threading.Thread(target=node2.__enter__)
            node2_thread.start()

            assert wait_for(
                lambda: len(node1.get_active_nodes()) >= 2,
                timeout_sec=5), \
                'minimum_nodes=2 should be reached'

            time.sleep(2)
            elapsed = time.time() - enter_start

            assert node1.state_machine.state == JobState.CLUSTER_FORMING, \
                f'node1 should STILL be in CLUSTER_FORMING at {elapsed:.1f}s, got {node1.state_machine.state.value}'

            node1_thread.join(timeout=15)
            enter_duration = time.time() - enter_start
            node2_thread.join(timeout=15)

            assert enter_duration >= 10.0, \
                f'__enter__ exited too early ({enter_duration:.1f}s), should wait full wait_on_enter=10s'

            assert enter_duration <= 12.0, \
                f'__enter__ took too long ({enter_duration:.1f}s), should be ~10s'

            assert wait_for_state(node1, JobState.RUNNING_LEADER, timeout_sec=3)
            assert wait_for_state(node2, JobState.RUNNING_FOLLOWER, timeout_sec=3)
        finally:
            node2.__exit__(None, None, None)
            node1.__exit__(None, None, None)

    @clean_tables('Node', 'Token')
    def test_exactly_minimum_nodes_proceeds_to_distribution(self, postgres):
        """Verify exactly minimum_nodes members proceed to distribution.

        Mutation: the minimum_nodes check off by one (requiring more than
            minimum_nodes), so every node raises TimeoutError.
        Oracle: minimum_nodes=3 with 3 nodes; 30 tokens split 10/10/10 by
            hand.
        """
        with cluster(
            postgres,
            'node1',
            'node2',
            'node3',
            total_tokens=30,
            minimum_nodes=3) as nodes:
            assert wait_for_cluster_running(nodes, leader_name='node1')
            assert wait_for_cached_tokens_sync(nodes, expected_total=30)
            assert [len(node.my_tokens) for node in nodes] == [10, 10, 10]

    @clean_tables('Node', 'Token')
    def test_timeout_when_minimum_not_reached(self, postgres):
        """Verify __enter__ raises TimeoutError below minimum_nodes.

        Mutation: _wait_for_enter_time_and_minimum_nodes ignoring
            minimum_nodes, or the message losing the node counts.
        Oracle: 1 node against minimum_nodes=5 with wait_on_enter=0; the
            message names both numbers.
        """
        coord_config = get_coordination_config(
            total_tokens=30,
            minimum_nodes=5,
            token_distribution_timeout_sec=5)

        node1 = create_job(
            'node1',
            postgres,
            coordination_config=coord_config,
            wait_on_enter=0)

        exception_holder = []

        def enter_and_capture():
            try:
                node1.__enter__()
            except Exception as e:
                exception_holder.append(e)

        node1_thread = threading.Thread(target=enter_and_capture)
        node1_thread.start()
        node1_thread.join(timeout=10)

        try:
            assert len(exception_holder) == 1, 'Should have raised exception'
            e = exception_holder[0]
            assert isinstance(e, TimeoutError), \
                f'Should be TimeoutError, got {type(e).__name__}'
            assert 'Minimum nodes (5) not reached after 0s grace period' in str(e), \
                f'Wrong message: {e}'
        finally:
            node1.__exit__(None, None, None)

    @clean_tables('Node', 'Token')
    def test_shutdown_during_cluster_formation_wait(self, postgres):
        """Verify shutdown mid-grace returns from __enter__ undistributed.

        Mutation: the grace loop sleeping instead of waiting on the shutdown
            event, breaking out to distribute, or raising TimeoutError.
        Oracle: wait_on_enter=60 against a 5s join; minimum_nodes=1 is met,
            so only the shutdown keeps the 30 tokens unassigned.
        """
        tables = schema.get_table_names('sync_')

        coord_config = get_coordination_config(
            total_tokens=30,
            minimum_nodes=1)

        node1 = create_job(
            'node1',
            postgres,
            coordination_config=coord_config,
            wait_on_enter=60)
        enter_errors = []

        def enter_and_capture():
            try:
                node1.__enter__()
            except Exception as e:
                enter_errors.append(e)

        node1_thread = threading.Thread(target=enter_and_capture)
        node1_thread.start()

        try:
            assert wait_for_state(node1, JobState.CLUSTER_FORMING, timeout_sec=10)

            time.sleep(2)

            node1._shutdown_event.set()

            node1_thread.join(timeout=5)

            assert not node1_thread.is_alive(), \
                '__enter__ should return promptly after shutdown'
            assert enter_errors == [], \
                f'__enter__ should not raise on shutdown: {enter_errors}'

            with postgres.connect() as conn:
                result = conn.execute(text(f'select count(*) from {tables["Token"]}'))
                token_count = result.scalar()

            assert token_count == 0, 'No tokens should be distributed after shutdown'
        finally:
            node1.__exit__(None, None, None)
            node1_thread.join(timeout=60)

    @clean_tables('Node', 'Token')
    def test_minimum_nodes_one_proceeds_immediately(self, postgres):
        """Verify a lone node with minimum_nodes=1 leads and owns every token.

        Mutation: the minimum_nodes check off by one (requiring more than
            minimum_nodes).
        Oracle: 1 active node equals minimum_nodes=1, so all 30 tokens.
        """
        tables = schema.get_table_names('sync_')

        coord_config = get_coordination_config(
            total_tokens=30,
            minimum_nodes=1,
            token_distribution_timeout_sec=30)

        node1 = create_job(
            'node1',
            postgres,
            coordination_config=coord_config,
            wait_on_enter=0)
        node1.__enter__()

        try:
            assert wait_for_state(node1, JobState.RUNNING_LEADER, timeout_sec=10)

            with postgres.connect() as conn:
                result = conn.execute(text(f'select count(*) from {tables["Token"]}'))
                token_count = result.scalar()

            assert token_count == 30, \
                'Should distribute all tokens with minimum_nodes=1'

        finally:
            node1.__exit__(None, None, None)


class TestLateNodeJoining:
    """Test scenarios where nodes join an already-running cluster.
    """

    @clean_tables('Node', 'Token', 'Rebalance')
    def test_late_joining_node_waits_for_token_assignment(self, postgres):
        """Verify a late node's __enter__ blocks until it receives tokens.

        Mutation: the follower path skipping wait_for_distribution, or the
            membership_changed handler mislabeling its trigger reason.
        Oracle: with wait_on_enter=0 node4 can only own tokens once a
            membership_change rebalance ran; 100 tokens over 4 nodes is 25
            each by hand.
        """
        config = get_coordination_config(total_tokens=100)
        tables = schema.get_table_names(config.appname)

        with cluster(postgres, 'node1', 'node2', 'node3', total_tokens=100) as nodes:
            assert wait_for_cluster_running(nodes, leader_name='node1')
            assert wait_for_cached_tokens_sync(
                nodes,
                expected_total=100,
                timeout_sec=10)

            node4 = create_job(
                'node4',
                postgres,
                coordination_config=config,
                wait_on_enter=0)

            try:
                node4.__enter__()

                assert len(node4.my_tokens) > 0, \
                    'Late-joining node must have tokens immediately after __enter__ completes'

                nodes_with_4 = nodes + [node4]
                assert wait_for_cached_tokens_sync(
                    nodes_with_4,
                    expected_total=100,
                    timeout_sec=15)

                final_distribution = {
                    node.node_name: len(node.my_tokens) for node in nodes_with_4
                    }
                assert (final_distribution
                        == {'node1': 25, 'node2': 25, 'node3': 25, 'node4': 25}), \
                    f'Unexpected final distribution {final_distribution}'

                with postgres.connect() as conn:
                    sql = f"""
                    select count(*) from {tables["Rebalance"]}
                    where trigger_reason = 'membership_change'
                    """
                    result = conn.execute(text(sql))
                    rebalance_count = result.scalar()

                assert rebalance_count >= 1, \
                    'Late node join should record a membership_change rebalance'
            finally:
                node4.__exit__(None, None, None)

    @clean_tables('Node', 'Token', 'Rebalance')
    def test_rebalance_monitor_detects_late_node_with_correct_baseline(self, postgres):
        """Verify a node joining before the monitor starts still rebalances.

        Mutation: RebalanceMonitor started without the distribution-time
            node count, so it baselines on the count that already includes
            node2.
        Oracle: node2 registers in the DISTRIBUTING exit hook, after node1
            distributed to itself alone; 50 tokens over 2 nodes is 25 each.
        """
        config = get_coordination_config(
            total_tokens=50,
            rebalance_check_interval_sec=2)
        tables = schema.get_table_names(config.appname)

        def register_node2_before_monitor_starts():
            with postgres.connect() as conn:
                sql = f"""
                insert into {tables["Node"]} (name, created_on, last_heartbeat)
                values ('node2', now(), now() + interval '1 hour')
                """
                conn.execute(text(sql))
                conn.commit()

        def token_count_by_node() -> dict:
            with postgres.connect() as conn:
                result = conn.execute(
                    text(f'select node, count(*) from {tables["Token"]} group by node'))
                return {row[0]: row[1] for row in result}

        node1 = create_job(
            'node1',
            postgres,
            coordination_config=config,
            wait_on_enter=0)
        node1.state_machine.on_exit(
            JobState.DISTRIBUTING,
            register_node2_before_monitor_starts)
        node1.__enter__()

        try:
            assert wait_for_state(node1, JobState.RUNNING_LEADER, timeout_sec=10)

            assert wait_for(
                lambda: token_count_by_node() == {'node1': 25, 'node2': 25},
                timeout_sec=15), \
                f'node2 should receive half the tokens, got {token_count_by_node()}'

            with postgres.connect() as conn:
                sql = f"""
                select count(*) from {tables["Rebalance"]}
                where trigger_reason = 'membership_change'
                """
                result = conn.execute(text(sql))
                membership_rebalances = result.scalar()

            assert membership_rebalances >= 1, \
                'Rebalance audit should record membership_change trigger (1->2 nodes)'
        finally:
            node1.__exit__(None, None, None)

    @clean_tables('Node')
    def test_rebalance_monitor_detects_leave_and_join_between_checks(self, postgres):
        """Verify one node leaving and another joining between checks publishes.

        Mutation: RebalanceMonitor.check comparing only the live node count,
            so {a, b, c} -> {a, b, d} reads as 3 -> 3 and publishes nothing,
            or never moving its baseline, so the same swap publishes again.
        Oracle: docs/OPERATOR_GUIDE.md, tokens are redistributed
            automatically on membership changes; swapping c for d changes
            the membership while the count stays 3.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)
        watcher = create_job('watcher', postgres, coordination_config=config)

        for name in ('node-a', 'node-b', 'node-c'):
            insert_active_node(postgres, tables, name)

        event_queue = EventQueue()
        monitor = RebalanceMonitor(
            node_name='node-a',
            db=watcher.db,
            cluster=watcher.cluster,
            event_queue=event_queue,
            shutdown_event=threading.Event())

        try:
            monitor.check()
            assert event_queue.consume_all() == [], \
                'Unchanged membership should not publish'

            delete_rows(postgres, tables, 'Node', 'name = :name', {'name': 'node-c'})
            insert_active_node(postgres, tables, 'node-d')
            monitor.check()

            events = event_queue.consume_all()
            assert [event.type for event in events] == ['membership_changed']
            assert events[0].data == {'previous_count': 3, 'current_count': 3}

            monitor.check()
            assert event_queue.consume_all() == [], \
                'A membership change already published should not publish again'
        finally:
            watcher.db.dispose()

    @clean_tables('Node', 'Token', 'Inst')
    def test_late_node_can_claim_tasks_immediately(self, postgres):
        """Verify a late node claims exactly its own tasks after __enter__.

        Mutation: __enter__ not loading the assigned tokens into the cache,
            or can_claim_task skipping the token check.
        Oracle: one task per token; node3's claimable tasks must match the
            tokens the Token table gives node3, and once caches sync each
            task has exactly one claimer.
        """
        config = get_coordination_config(total_tokens=30)
        tables = schema.get_table_names(config.appname)
        task_ids = find_task_ids_covering_all_tokens(config)

        with cluster(postgres, 'node1', 'node2', total_tokens=30) as initial_nodes:
            assert wait_for_cached_tokens_sync(
                initial_nodes,
                expected_total=30,
                timeout_sec=10)

            node3 = create_job(
                'node3',
                postgres,
                coordination_config=config,
                wait_on_enter=0)

            try:
                node3.__enter__()

                claimed_by_node3 = {
                    task_id for task_id in task_ids
                    if node3.can_claim_task(create_task(task_id))
                    }
                node3_db_tokens = {
                    token_id for token_id, node
                    in get_token_assignments(postgres, tables).items()
                    if node == 'node3'
                    }

                assert node3_db_tokens, 'node3 should own tokens after __enter__'
                assert (claimed_by_node3
                        == {task_ids[token_id] for token_id in node3_db_tokens}), \
                    'node3 should claim exactly the tasks on its own tokens'

                all_nodes = initial_nodes + [node3]
                assert wait_for_cached_tokens_sync(
                    all_nodes,
                    expected_total=30,
                    timeout_sec=15)

                claimer_count_by_task = {
                    task_id: sum(
                        node.can_claim_task(create_task(task_id))
                        for node in all_nodes)
                    for task_id in task_ids
                    }
                assert set(claimer_count_by_task.values()) == {1}, \
                    f'Each task should have exactly one claimer, got {claimer_count_by_task}'
            finally:
                node3.__exit__(None, None, None)

    @clean_tables('Node', 'Token')
    def test_multiple_late_nodes_join_sequentially(self, postgres):
        """Verify two sequential joins each rebalance and end balanced.

        Mutation: the membership_changed handler mislabeling its trigger
            reason, or a rebalance that leaves the joiners unbalanced.
        Oracle: two joins give two membership_change rows; 100 tokens over
            4 nodes is 25 each by hand.
        """
        config = get_coordination_config(
            total_tokens=100,
            rebalance_check_interval_sec=2)
        tables = schema.get_table_names(config.appname)

        with cluster(postgres, 'node1', 'node2', total_tokens=100) as initial_nodes:
            assert wait_for_cached_tokens_sync(
                initial_nodes,
                expected_total=100,
                timeout_sec=10)

            node3 = create_job(
                'node3',
                postgres,
                coordination_config=config,
                wait_on_enter=0)
            node4 = create_job(
                'node4',
                postgres,
                coordination_config=config,
                wait_on_enter=0)

            try:
                node3.__enter__()
                assert len(node3.my_tokens) > 0, 'node3 should have tokens'

                node4.__enter__()
                assert len(node4.my_tokens) > 0, 'node4 should have tokens'

                all_nodes = initial_nodes + [node3, node4]
                assert wait_for_cached_tokens_sync(
                    all_nodes,
                    expected_total=100,
                    timeout_sec=20), \
                    'All node token caches should sync after sequential joins'

                final_dist = {node.node_name: len(node.my_tokens) for node in all_nodes}
                assert (final_dist
                        == {'node1': 25, 'node2': 25, 'node3': 25, 'node4': 25}), \
                    f'Unexpected final distribution {final_dist}'

                with postgres.connect() as conn:
                    sql = f"""
                    select count(*) from {tables["Rebalance"]}
                    where trigger_reason = 'membership_change'
                    """
                    result = conn.execute(text(sql))
                    membership_rebalances = result.scalar()

                assert membership_rebalances >= 2, \
                    f'Each join should record a membership_change rebalance, got {membership_rebalances}'
            finally:
                node4.__exit__(None, None, None)
                node3.__exit__(None, None, None)

    @clean_tables('Node', 'Token')
    def test_late_node_timeout_if_leader_unresponsive(self, postgres, caplog):
        """Verify a late node times out when no rebalance reaches it.

        Mutation: wait_for_distribution returning without raising when the
            timeout passes, or the follower path skipping the wait.
        Oracle: node1 runs its first membership check before node2 registers,
            and rebalance_check_interval_sec=60 outlasts node2's 10s wait
            (2 x token_distribution_timeout_sec=5), so node2 gets no tokens.
        """
        config = get_coordination_config(
            total_tokens=30,
            token_distribution_timeout_sec=5,
            rebalance_check_interval_sec=60)

        caplog.set_level(logging.DEBUG, logger='jobsync.client')
        node1 = create_job(
            'node1',
            postgres,
            coordination_config=config,
            wait_on_enter=0)
        node1.__enter__()

        try:
            assert wait_for_state(node1, JobState.RUNNING_LEADER, timeout_sec=10)
            assert wait_for(
                lambda: any(
                    'Rebalance check: last=1, current=1' in record.message
                    for record in caplog.records)), \
                'node1 should run its first membership check before node2 registers'

            node2 = create_job(
                'node2',
                postgres,
                coordination_config=config,
                wait_on_enter=0)

            try:
                with pytest.raises(
                    TimeoutError,
                    match='Token distribution did not complete for node2'):
                    node2.__enter__()
            finally:
                node2.__exit__(None, None, None)
        finally:
            node1.__exit__(None, None, None)


class TestFollowerTimeoutBugFix:
    """Test timeout behavior when minimum_nodes is not reached.
    """

    @clean_tables('Node', 'Token')
    def test_follower_timeout_still_works_when_leader_truly_stuck(self, postgres):
        """Verify both nodes time out on minimum_nodes after the full grace.

        Mutation: minimum_nodes ignored, or the grace loop skipped so the
            check runs at once.
        Oracle: 2 nodes against minimum_nodes=10; node2 starts 0.5s late
            with wait_on_enter=5, so both finish no sooner than 5.5s.
        """
        coord_config = get_coordination_config(
            total_tokens=30,
            minimum_nodes=10,
            token_distribution_timeout_sec=60)

        node1 = create_job(
            'node1',
            postgres,
            coordination_config=coord_config,
            wait_on_enter=5)
        node2 = create_job(
            'node2',
            postgres,
            coordination_config=coord_config,
            wait_on_enter=5)

        node1_exception = []
        node2_exception = []

        def start_node1():
            try:
                node1.__enter__()
            except Exception as e:
                node1_exception.append(e)

        def start_node2():
            try:
                node2.__enter__()
            except Exception as e:
                node2_exception.append(e)

        node1_thread = threading.Thread(target=start_node1)
        node2_thread = threading.Thread(target=start_node2)

        start_time = time.time()
        node1_thread.start()
        time.sleep(0.5)
        node2_thread.start()

        try:
            node1_thread.join(timeout=15)
            node2_thread.join(timeout=15)
            elapsed = time.time() - start_time

            assert elapsed >= 5.5, \
                f'Both nodes should wait the full grace period (took {elapsed:.1f}s)'

            assert len(node1_exception) == 1, 'Node1 should have exception'
            assert isinstance(node1_exception[0], TimeoutError), \
                f'Node1 should get TimeoutError, got {type(node1_exception[0]).__name__}'
            assert ('Minimum nodes (10) not reached after 5s grace period'
                    in str(node1_exception[0])), \
                f'Node1 should timeout after wait_on_enter: {node1_exception[0]}'

            assert len(node2_exception) == 1, 'Node2 should have exception'
            assert isinstance(node2_exception[0], TimeoutError), \
                f'Node2 should get TimeoutError, got {type(node2_exception[0]).__name__}'
            assert ('Minimum nodes (10) not reached after 5s grace period'
                    in str(node2_exception[0])), \
                f'Node2 should timeout after wait_on_enter: {node2_exception[0]}'
        finally:
            node1.__exit__(None, None, None)
            node2.__exit__(None, None, None)


if __name__ == '__main__':
    pytest.main(args=['-sx', __file__])
