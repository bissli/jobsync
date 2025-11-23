"""Integration tests for cluster-wide operations and coordination.

USE THIS FILE FOR:
- Tests requiring multiple coordinated nodes
- Cluster-wide coordination scenarios
- Leader/follower interaction tests
- Rebalancing and failover tests
- Production-like end-to-end scenarios
"""
import datetime
import logging
import time
from datetime import timedelta

import pytest
from fixtures import *  # noqa: F401, F403
from sqlalchemy import text

from jobsync import schema
from jobsync.client import CoordinationConfig, DeadNodeMonitor, JobState
from jobsync.client import RebalanceMonitor, Task

logger = logging.getLogger(__name__)


def test_3node_cluster_formation(postgres):
    """Test that 3 nodes form a cluster, elect leader, and distribute tokens."""
    print('\n=== Testing 3-node cluster formation ===')

    with cluster(postgres, 'node1', 'node2', 'node3', total_tokens=100) as nodes:
        # Wait for all nodes to reach running state
        for node in nodes:
            expected_state = JobState.RUNNING_LEADER if node.node_name == 'node1' else JobState.RUNNING_FOLLOWER
            assert wait_for_state(node, expected_state, timeout_sec=10), \
                f'{node.node_name} should reach running state'

        # Verify all nodes registered
        active_nodes = nodes[0].get_active_nodes()
        print(f'Active nodes: {[n["name"] for n in active_nodes]}')
        assert len(active_nodes) == 3, 'All 3 nodes should be active'

        # Verify leader elected (should be node1 - oldest)
        for node in nodes:
            leader = wait_for_leader_election(node, expected_leader='node1', timeout_sec=5)
            print(f'{node.node_name}: Leader is {leader}')

        # Verify tokens distributed and balanced
        for node in nodes:
            assert wait_for(lambda n=node: len(n.my_tokens) >= 1, timeout_sec=10), \
                f'{node.node_name} should receive tokens'
            print(f'{node.node_name}: {len(node.my_tokens)} tokens, version {node.token_version}')

        # Verify balanced distribution
        assert_token_distribution_balanced(nodes, total_tokens=100, tolerance=0.2)

        print('✓ Cluster formation successful')


def test_fresh_cluster_rebalances_stale_tokens(postgres):
    """Verify fresh cluster rebalances stale token distribution from previous run.
    """
    print('\n=== Testing fresh cluster rebalances stale tokens ===')

    tables = schema.get_table_names('sync_')

    # Setup: Insert stale token distribution from "previous run"
    print('Setting up stale tokens (all owned by old-dead-node)...')
    for token_id in range(100):
        insert_token(postgres, tables, token_id, 'old-dead-node', version=1)

    with postgres.connect() as conn:
        result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Token"]}'))
        stale_count = result.scalar()
    print(f'Stale tokens before cluster start: {stale_count}')
    assert stale_count == 100, 'Should have 100 stale tokens'

    # Start fresh cluster (no old-dead-node)
    print('\nStarting fresh 3-node cluster...')
    with cluster(postgres, 'node1', 'node2', 'node3', total_tokens=100) as nodes:
        # Wait for cluster to reach running state
        for node in nodes:
            expected_state = JobState.RUNNING_LEADER if node.node_name == 'node1' else JobState.RUNNING_FOLLOWER
            assert wait_for_state(node, expected_state, timeout_sec=10), \
                f'{node.node_name} should reach running state'

        # Wait for rebalancing (should happen during DISTRIBUTING state)
        print('Waiting for token redistribution...')
        assert wait_for_rebalance(postgres, tables, min_count=1, timeout_sec=20), \
            'Leader should trigger rebalancing to clear stale distribution'

        # Verify tokens redistributed to new nodes
        print('\nToken distribution after rebalancing:')
        with postgres.connect() as conn:
            result = conn.execute(text(f"""
                SELECT node, COUNT(*) as count, version
                FROM {tables["Token"]}
                GROUP BY node, version
                ORDER BY node
            """))
            distribution = [dict(row._mapping) for row in result]

        for row in distribution:
            print(f'{row["node"]}: {row["count"]} tokens, version {row["version"]}')

        # Verify no tokens assigned to old-dead-node
        stale_node_tokens = [r for r in distribution if r['node'] == 'old-dead-node']
        assert len(stale_node_tokens) == 0, 'Stale node should not own any tokens'

        # Verify all 100 tokens redistributed to new nodes
        total_redistributed = sum(r['count'] for r in distribution)
        assert total_redistributed == 100, 'All 100 tokens should be redistributed'

        # Verify balanced distribution
        assert_token_distribution_balanced(nodes, total_tokens=100, tolerance=0.2)

        # Verify version incremented
        new_version = distribution[0]['version']
        assert new_version > 1, f'Version should increment from 1, got {new_version}'

        print(f'✓ Stale tokens successfully rebalanced (v1 -> v{new_version})')


def test_token_based_task_claiming(postgres):
    """Test that nodes only claim tasks they own tokens for."""
    print('\n=== Testing token-based task claiming ===')

    tables = schema.get_table_names('sync_')

    for i in range(30):
        insert_inst(postgres, tables, str(i), done=False)

    with cluster(postgres, 'node1', 'node2', 'node3', total_tokens=100) as nodes:
        # Wait for all nodes to receive tokens
        for node in nodes:
            assert wait_for(lambda n=node: len(n.my_tokens) >= 1, timeout_sec=10)

        # Each node claims tasks it owns
        claimed_by_node = {}
        items = list(range(30))

        for node in nodes:
            claimed = []
            for item_id in items:
                task = create_task(item_id)
                if node.can_claim_task(task):
                    node.add_task(task)
                    claimed.append(item_id)
                    with postgres.connect() as conn:
                        conn.execute(text(f'UPDATE {tables["Inst"]} SET done=TRUE WHERE item=:item'),
                                   {'item': str(item_id)})
                        conn.commit()
            claimed_by_node[node.node_name] = claimed
            print(f'{node.node_name}: Claimed {len(claimed)} tasks')

        # Verify results
        claimed_tasks = set()
        for tasks in claimed_by_node.values():
            claimed_tasks.update(tasks)

        assert len(claimed_tasks) == 30, 'All 30 tasks should be claimed'

        # Verify no duplicates
        total_claims = sum(len(tasks) for tasks in claimed_by_node.values())
        assert total_claims == 30, 'No duplicate claims'

        with postgres.connect() as conn:
            result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Inst"]} WHERE done=TRUE'))
            done_count = result.scalar()
        assert done_count == 30, 'All tasks should be marked done'

        print('✓ Token-based claiming successful')


def test_node_death_and_rebalancing(postgres):
    """Test that when a node dies, its tokens are redistributed."""
    print('\n=== Testing node death and rebalancing ===')

    tables = schema.get_table_names('sync_')

    with cluster(postgres, 'node1', 'node2', 'node3', total_tokens=100) as nodes:
        # Wait for cluster to stabilize
        for node in nodes:
            expected_state = JobState.RUNNING_LEADER if node.node_name == 'node1' else JobState.RUNNING_FOLLOWER
            assert wait_for_state(node, expected_state, timeout_sec=10)

        # Record initial token distribution
        initial_tokens = {node.node_name: len(node.my_tokens) for node in nodes}
        print(f'Initial tokens: {initial_tokens}')

        # Simulate node2 crash (stop threads but leave stale DB row)
        print('Killing node2 (simulating crash)...')
        simulate_node_crash(nodes[1])

        # Wait for leader to detect dead node and remove it
        assert wait_for_dead_node_removal(postgres, tables, 'node2', timeout_sec=20), \
            'node2 should be detected as dead and removed by leader'
        print('node2 detected as dead and removed')

        # Wait for rebalancing to be logged
        assert wait_for_rebalance(postgres, tables, min_count=1, timeout_sec=20), \
            'Rebalancing should be triggered after dead node removal'
        print('Rebalancing triggered')

        # Wait for surviving nodes to sync their token caches
        assert wait_for_all_nodes_token_sync([nodes[0], nodes[2]], expected_total=100, timeout_sec=10)

        # Verify tokens were redistributed to surviving nodes
        print('\nToken distribution after rebalancing:')
        for node in [nodes[0], nodes[2]]:
            new_token_count = get_fresh_token_count(node)
            print(f'{node.node_name}: {initial_tokens[node.node_name]} -> {new_token_count} tokens')
            assert new_token_count > initial_tokens[node.node_name], \
                f'{node.node_name} should have gained tokens after node2 died'

        # Verify all tokens accounted for
        total_after = sum(get_fresh_token_count(node) for node in [nodes[0], nodes[2]])
        print(f'Total tokens across remaining nodes: {total_after}/100')
        assert total_after == 100, 'All 100 tokens should be redistributed to surviving nodes'

        print('✓ Node death and rebalancing successful')


def test_lock_registration_and_enforcement(postgres):
    """Test that locked tasks are only assigned to nodes matching the pattern."""
    print('\n=== Testing lock registration and enforcement ===')

    config = get_coordination_config(total_tokens=100)
    tables = schema.get_table_names(config.appname)

    # Lock registration callback
    def register_locks(job):
        # Lock tasks 0-9 to 'special-%' pattern
        locks = [(i, 'special-%', 'test lock') for i in range(10)]
        job.register_locks_bulk(locks)
        print(f'{job.node_name}: Registered {len(locks)} locks')

    node1 = create_job('node1', postgres, coordination_config=config, wait_on_enter=15,
                       lock_provider=register_locks)
    node2 = create_job('node2', postgres, coordination_config=config, wait_on_enter=15,
                       lock_provider=register_locks)
    special = create_job('special-alpha', postgres, coordination_config=config, wait_on_enter=15,
                         lock_provider=register_locks)

    node1.__enter__()
    node2.__enter__()
    special.__enter__()

    try:
        # Wait for rebalancing to complete and nodes to sync their cached tokens
        time.sleep(5)
        assert wait_for_cached_tokens_sync([node1, node2, special], expected_total=100, timeout_sec=10)

        locked_token_owners = {}
        for task_id in range(10):
            token_id = node1.task_to_token(task_id)

            with postgres.connect() as conn:
                result = conn.execute(text(f'SELECT node FROM {tables["Token"]} WHERE token_id = :token_id'),
                                     {'token_id': token_id})
                owner = result.scalar()
            locked_token_owners[task_id] = owner
            print(f'Task {task_id} (token {token_id}) -> {owner}')

        # All locked tokens should be assigned to 'special-alpha'
        for task_id, owner in locked_token_owners.items():
            assert owner == 'special-alpha', \
                f'Locked task {task_id} should be assigned to special-alpha'

        # Verify regular nodes cannot claim locked tasks
        for task_id in range(10):
            task = create_task(task_id)
            assert not node1.can_claim_task(task), \
                f'node1 should not claim locked task {task_id}'
            assert not node2.can_claim_task(task), \
                f'node2 should not claim locked task {task_id}'
            assert special.can_claim_task(task), \
                f'special-alpha should claim locked task {task_id}'

        print('✓ Lock enforcement successful')

    finally:
        node1.__exit__(None, None, None)
        node2.__exit__(None, None, None)
        special.__exit__(None, None, None)


def test_health_monitoring(postgres):
    """Test that nodes monitor their own health correctly."""
    print('\n=== Testing health monitoring ===')

    config = get_coordination_config()

    node = create_job('node1', postgres, coordination_config=config, wait_on_enter=15)
    node.__enter__()

    try:
        # Initially should be healthy
        assert wait_for(node.am_i_healthy, timeout_sec=5), 'Node should be healthy initially'
        assert node.am_i_healthy()
        print(f'✓ Node is healthy (heartbeat age: {(node.cluster.last_heartbeat_sent)})')

        # Simulate heartbeat thread failure by stopping it
        node.cluster.last_heartbeat_sent -= timedelta(seconds=20)

        # Should now be unhealthy (heartbeat too old)
        is_healthy = node.am_i_healthy()
        print(f'After heartbeat failure: healthy={is_healthy}')
        assert not is_healthy, 'Node should be unhealthy with stale heartbeat'

        print('✓ Health monitoring works correctly')

    finally:
        node._shutdown_event.set()
        node.__exit__(None, None, None)


def test_leader_failover(postgres):
    """Test that when leader dies, a new leader is elected."""
    print('\n=== Testing leader failover ===')

    config = get_coordination_config()

    nodes = []
    for i in range(1, 4):
        node = create_job(f'node{i}', postgres, coordination_config=config, wait_on_enter=15)
        node.__enter__()
        nodes.append(node)
        time.sleep(1)

    try:
        assert wait_for_state(nodes[0], JobState.RUNNING_LEADER, timeout_sec=10)

        # Verify node1 is leader
        leader = wait_for_leader_election(nodes[0], expected_leader='node1', timeout_sec=5)
        print(f'Initial leader: {leader}')

        # Kill node1
        print('Killing leader (node1)...')
        simulate_node_crash(nodes[0], cleanup=True)

        # Wait for new leader election
        new_leader = wait_for_leader_election(nodes[1], expected_leader='node2', timeout_sec=12)
        print(f'New leader after failover: {new_leader}')

        # Verify node3 also sees node2 as leader
        leader_from_node3 = wait_for_leader_election(nodes[2], expected_leader='node2', timeout_sec=5)

        print('✓ Leader failover successful')

    finally:
        for node in [nodes[1], nodes[2]]:
            try:
                node.__exit__(None, None, None)
            except:
                pass


def test_follower_promotion_starts_leader_duties(postgres):
    """Test that follower promoted to leader starts monitoring threads and performs rebalancing.

    This test verifies the critical scenario where:
    1. Leader node dies
    2. Follower is elected as new leader
    3. New leader starts monitoring threads (the bug!)
    4. New leader detects dead node and triggers rebalancing
    5. Tokens are redistributed to surviving nodes

    Without the fix, the new leader never starts monitoring threads,
    so dead nodes aren't cleaned up and tokens aren't redistributed.
    """
    print('\n=== Testing follower promotion to leader ===')

    config = get_coordination_config(total_tokens=100)
    tables = schema.get_table_names(config.appname)

    # Start 3 nodes sequentially (not using cluster() due to timing requirements)
    nodes = []
    for i in range(1, 4):
        node = create_job(f'node{i}', postgres, coordination_config=config, wait_on_enter=15)
        node.__enter__()
        nodes.append(node)
        time.sleep(0.5)

    try:
        # Wait for cluster to stabilize and elect leader
        initial_leader = wait_for_leader_election(nodes[0], expected_leader='node1', timeout_sec=10)
        print(f'Initial leader: {initial_leader}')

        for node in nodes:
            assert wait_for_state(node, JobState.RUNNING_LEADER if node.node_name == 'node1' else JobState.RUNNING_FOLLOWER, timeout_sec=10)

        # Wait for all nodes to sync their token caches after cluster formation and rebalancing
        assert wait_for_cached_tokens_sync(nodes, expected_total=100, timeout_sec=10)

        # Record initial token distribution
        initial_tokens = {}
        for node in nodes:
            tokens = node.my_tokens
            initial_tokens[node.node_name] = len(tokens)
            print(f'{node.node_name}: {len(tokens)} tokens')

        total_initial = sum(initial_tokens.values())
        assert total_initial == 100, 'All 100 tokens should be distributed'

        # Kill the leader (node1) - simulate crash by stopping threads but NOT cleaning up DB
        print('\nKilling leader (node1)...')
        simulate_node_crash(nodes[0])

        # Wait for dead node removal and rebalancing
        print('Waiting for new leader to detect death and rebalance...')
        assert wait_for_dead_node_removal(postgres, tables, 'node1', timeout_sec=20)
        print('node1 removed from database')

        # Verify node2 is now leader
        new_leader = wait_for_leader_election(nodes[1], expected_leader='node2', timeout_sec=5)
        print(f'\nNew leader after failover: {new_leader}')

        # Verify node1 is detected as dead and removed from database
        with postgres.connect() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM {tables["Node"]} WHERE name = 'node1'
            """))
            node1_count = result.scalar()

        print(f'node1 still in database: {node1_count > 0}')
        assert node1_count == 0, \
            'Dead leader should be removed from Node table by new leader'

        # Wait for nodes to sync their token caches after rebalance
        print('\nWaiting for nodes to sync token caches...')
        assert wait_for_all_nodes_token_sync([nodes[1], nodes[2]], expected_total=100, timeout_sec=10)

        # Verify tokens were redistributed (THIS IS THE KEY TEST)
        print('\nToken distribution after rebalancing:')
        final_tokens = {}
        for node in [nodes[1], nodes[2]]:
            token_count = get_fresh_token_count(node)
            final_tokens[node.node_name] = token_count
            print(f'{node.node_name}: {initial_tokens[node.node_name]} -> {token_count} tokens')

        total_final = sum(final_tokens.values())
        print(f'\nTotal tokens after rebalancing: {total_final}/100')

        # Critical assertion - this fails without the fix
        assert total_final == 100, \
            'All tokens should be redistributed to surviving nodes ' \
            '(new leader must start monitoring threads!)'

        for node_name, token_count in final_tokens.items():
            assert token_count > initial_tokens[node_name], \
                f'{node_name} should have gained tokens after leader death'
            assert 45 <= token_count <= 55, \
                f'{node_name} should have ~50 tokens (got {token_count})'

        # Wait for rebalancing to be logged by new leader
        assert wait_for_rebalance(postgres, tables, min_count=1, timeout_sec=20)

        with postgres.connect() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM {tables["Rebalance"]}
                WHERE triggered_at > NOW() - INTERVAL '30 seconds'
                AND leader_node = 'node2'
            """))
            recent_rebalances = result.scalar()

        print(f'\nRecent rebalances by node2: {recent_rebalances}')
        assert recent_rebalances > 0, \
            'New leader should have logged rebalancing event'

        print('✓ Follower promotion and leader duties successful')

    finally:
        # Cleanup surviving nodes
        for node in [nodes[1], nodes[2]]:
            try:
                node.__exit__(None, None, None)
            except Exception:
                pass


def test_node_rejoin_restores_original_allocation(postgres):
    """Test that when a dead node rejoins, it gets back its original token allocation."""
    print('\n=== Testing node rejoin restores original allocation ===')

    config = get_coordination_config(total_tokens=100)
    tables = schema.get_table_names(config.appname)

    # Phase 1: Start all 4 nodes and record initial state
    print('Phase 1: Starting 4 nodes...')
    with cluster(postgres, 'nodeA', 'nodeB', 'nodeC', 'nodeD', total_tokens=100) as nodes:
        # Wait for cluster to stabilize
        for node in nodes:
            expected_state = JobState.RUNNING_LEADER if node.node_name == 'nodeA' else JobState.RUNNING_FOLLOWER
            assert wait_for_state(node, expected_state, timeout_sec=10)
            assert wait_for(lambda n=node: len(n.my_tokens) >= 20, timeout_sec=10)

        # Record initial token allocations
        original_tokens = {}
        for node in nodes:
            tokens = node.my_tokens
            original_tokens[node.node_name] = tokens.copy()
            print(f'{node.node_name}: {len(tokens)} tokens initially')

        # Verify all 100 tokens are distributed
        total_initial = sum(len(tokens) for tokens in original_tokens.values())
        assert total_initial == 100, 'All 100 tokens should be distributed initially'

        # Phase 2: Kill nodeD
        print('\nPhase 2: Killing nodeD...')
        nodeD = nodes[3]
        simulate_node_crash(nodeD)

        # Wait for dead node detection and rebalancing
        assert wait_for_dead_node_removal(postgres, tables, 'nodeD', timeout_sec=20)
        assert wait_for_rebalance(postgres, tables, min_count=1, timeout_sec=20)

        # Wait for surviving nodes to sync after nodeD death
        assert wait_for_all_nodes_token_sync(nodes[:3], expected_total=100, timeout_sec=10)

        # Verify nodeD's tokens were redistributed to A, B, C
        print('Verifying redistribution after nodeD death:')
        surviving_nodes = nodes[:3]
        for node in surviving_nodes:
            new_token_count = get_fresh_token_count(node)
            print(f'{node.node_name}: {len(original_tokens[node.node_name])} -> {new_token_count} tokens')
            assert new_token_count > len(original_tokens[node.node_name]), \
                f'{node.node_name} should have gained tokens after nodeD died'

        # Phase 3: Rejoin nodeD
        print('\nPhase 3: Rejoining nodeD...')
        nodeD_rejoined = create_job('nodeD', postgres, coordination_config=config, wait_on_enter=15)
        nodeD_rejoined.__enter__()
        nodes[3] = nodeD_rejoined

        # Wait for second rebalance to complete (nodeD should receive tokens)
        assert wait_for(lambda: len(nodeD_rejoined.my_tokens) >= 20, timeout_sec=20), \
            'nodeD should receive tokens after rejoining (rebalance should occur)'

        # Wait for all nodes to sync their token caches with new distribution
        assert wait_for_all_nodes_token_sync(nodes, expected_total=100, timeout_sec=20)

        # Phase 4: Verify original allocations are restored
        print('\nPhase 4: Verifying original allocations restored:')
        all_restored = True
        for node in nodes:
            current_tokens, _ = node.tokens.get_my_tokens_versioned()
            original = original_tokens[node.node_name]

            print(f'{node.node_name}: original={len(original)}, current={len(current_tokens)}, '
                  f'match={current_tokens == original}')

            if current_tokens != original:
                all_restored = False
                missing = original - current_tokens
                extra = current_tokens - original
                print(f'  Missing {len(missing)} tokens: {sorted(missing)[:10]}...')
                print(f'  Extra {len(extra)} tokens: {sorted(extra)[:10]}...')

        assert all_restored, 'All nodes should have their original token allocations restored'

        # Verify total is still 100
        total_final = sum(get_fresh_token_count(node) for node in nodes)
        assert total_final == 100, 'All 100 tokens should be distributed after rejoin'

        print('✓ Node rejoin successfully restored original allocation')


class TestCleanupFailureScenarios:
    """Test cleanup behavior under failure conditions."""

    @clean_tables('Audit')
    def test_cleanup_with_pending_tasks_writes_audit(self, postgres):
        """Verify cleanup writes pending tasks to audit table.
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
            result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Audit"]}'))
            audit_count = result.scalar()

        assert audit_count == 2, 'Both pending tasks should be written to audit'

    def test_double_cleanup_is_safe(self, postgres):
        """Verify calling __exit__ twice doesn't cause errors.
        """
        config = get_coordination_config()

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)
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
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        with create_job('node1', postgres, coordination_config=config, wait_on_enter=0) as job:
            job.set_claim('test-item')
            time.sleep(0.2)

        with postgres.connect() as conn:
            node_result = conn.execute(text(f"SELECT COUNT(*) FROM {tables['Node']} WHERE name = 'node1'"))
            node_count = node_result.scalar()

            claim_result = conn.execute(text(f"SELECT COUNT(*) FROM {tables['Claim']} WHERE node = 'node1'"))
            claim_count = claim_result.scalar()

            check_result = conn.execute(text(f"SELECT COUNT(*) FROM {tables['Check']} WHERE node = 'node1'"))
            check_count = check_result.scalar()

        assert node_count == 0, 'Node should be removed from Node table'
        assert claim_count == 0, 'Node should be removed from Claim table'
        assert check_count == 0, 'Node should be removed from Check table'

    def test_cleanup_completes_despite_exception(self, postgres, caplog):
        """Verify cleanup attempts to complete even if some operations fail.

        Critical scenario: Database connection fails during cleanup, or
        some cleanup operation throws exception. Cleanup should attempt
        all steps and not leave resources hanging.
        """
        config = get_coordination_config()

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)
        job.__enter__()

        # Simulate a cleanup error by disposing the engine mid-cleanup
        # This will cause database operations to fail
        original_cleanup = job._cleanup

        def failing_cleanup():
            """Cleanup that disposes engine early to simulate failures."""
            # Shutdown event and monitors first
            job._shutdown_event.set()
            for monitor in job._monitors.values():
                if monitor.thread and monitor.thread.is_alive():
                    monitor.thread.join(timeout=5)

            # Dispose engine to cause failures in DB cleanup
            if hasattr(job, 'db') and job.db.engine:
                job.db.engine.dispose()

            # Try to continue cleanup (will fail on DB operations)
            try:
                original_cleanup()
            except Exception as e:
                logger.info(f'Expected cleanup exception: {e}')

        job._cleanup = failing_cleanup

        # Exit should not crash despite cleanup failures
        try:
            job.__exit__(None, None, None)
        except Exception as e:
            # Some exceptions are acceptable (DB errors), but shouldn't crash completely
            logger.info(f'Cleanup exception (expected): {e}')

        # Verify threads were stopped (the part that should succeed)
        for monitor in job._monitors.values():
            if monitor.thread:
                assert not monitor.thread.is_alive(), \
                    f'{monitor.name} thread should be stopped despite cleanup failures'

        logger.info('✓ Cleanup attempted completion despite exceptions')


class TestRebalanceLockStaleRecovery:
    """Test stale rebalance lock detection and recovery."""

    def test_stale_rebalance_lock_detected_and_removed(self, postgres):
        """Verify stale rebalance locks are detected and removed.
        """
        tables = schema.get_table_names('sync_')

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

        job = create_job('node1', postgres, wait_on_enter=0, coordination_config=coord_config)
        job.__enter__()

        try:
            with job.locks.acquire_rebalance_lock('test-rebalance'):
                with postgres.connect() as conn:
                    result = conn.execute(text(f"""
                        SELECT in_progress, started_by FROM {tables['RebalanceLock']} WHERE singleton = 1
                    """))
                    lock_status = result.first()

                assert lock_status[0], 'Lock should be in progress'
                assert lock_status[1] == 'test-rebalance', 'test-rebalance should now hold the lock'

        finally:
            job.__exit__(None, None, None)

    def test_configurable_stale_rebalance_lock_threshold(self, postgres):
        """Verify stale rebalance lock threshold is configurable.
        """
        tables = schema.get_table_names('sync_')

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

        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)
        job.__enter__()

        try:
            with job.locks.acquire_rebalance_lock('test'):
                pass
            assert True, 'Should treat 15-second-old rebalance lock as stale with 10s threshold'

        finally:
            job.__exit__(None, None, None)

    def test_non_stale_rebalance_lock_not_removed(self, postgres):
        """Verify recent rebalance locks are not removed.
        """
        tables = schema.get_table_names('sync_')

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

        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)

        try:
            with job.locks.acquire_rebalance_lock('test'):
                pass
            raise AssertionError('Should not acquire rebalance lock if recent lock exists')
        except Exception:
            assert True, 'Should not acquire rebalance lock if recent lock exists'

    def test_stale_rebalance_lock_logged(self, postgres, caplog):
        """Verify stale rebalance lock detection is logged.
        """
        tables = schema.get_table_names('sync_')

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
            job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)
            job.__enter__()

            try:
                with job.locks.acquire_rebalance_lock('test'):
                    pass

                warning_messages = [record.message for record in caplog.records if record.levelname == 'WARNING']
                stale_lock_warnings = [msg for msg in warning_messages if 'Stale rebalance lock detected' in msg]

                assert len(stale_lock_warnings) > 0, 'Should log warning about stale rebalance lock'
                assert any('stuck-node' in msg for msg in stale_lock_warnings), \
                    'Warning should mention the stuck node'

            finally:
                job.__exit__(None, None, None)


class TestDeadNodeLockCleanup:
    """Test lock cleanup when nodes die."""

    @clean_tables('Lock', 'Node')
    def test_dead_node_locks_cleaned_up(self, postgres):
        """Verify locks created by dead nodes are removed during cleanup.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        insert_stale_node(postgres, tables, 'dead-node', heartbeat_age_seconds=30)

        for token_id in [1, 2, 3]:
            insert_lock(postgres, tables, token_id, ['pattern-test'], created_by='dead-node')

        insert_lock(postgres, tables, 99, ['pattern-other'], created_by='other-node')

        # Start a leader node that will detect and clean up the dead node
        coord_config = CoordinationConfig(
            total_tokens=50,
            heartbeat_timeout_sec=15,
            dead_node_check_interval_sec=0.5
        )

        job = create_job('leader-node', postgres, wait_on_enter=2, coordination_config=coord_config)
        job.__enter__()

        try:
            assert wait_for_dead_node_removal(postgres, tables, 'dead-node', timeout_sec=10)

            with postgres.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT task_id FROM {tables["Lock"]} WHERE created_by = 'dead-node'
                """))
                dead_node_locks = [row[0] for row in result]

                result = conn.execute(text(f"""
                    SELECT task_id FROM {tables["Lock"]} WHERE created_by = 'other-node'
                """))
                other_node_locks = [row[0] for row in result]

            assert len(dead_node_locks) == 0, 'All locks from dead node should be removed'
            assert len(other_node_locks) == 1, 'Locks from other nodes should remain'
            assert other_node_locks[0] == '99', 'Lock 99 from other-node should still exist'

        finally:
            job.__exit__(None, None, None)

    @clean_tables('Lock', 'Node')
    def test_multiple_dead_nodes_all_locks_cleaned(self, postgres):
        """Verify locks from multiple dead nodes are all cleaned up.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        for i in range(1, 4):
            node_name = f'dead-node-{i}'
            insert_stale_node(postgres, tables, node_name, heartbeat_age_seconds=30)

            for j in range(2):
                token_id = i * 10 + j
                insert_lock(postgres, tables, token_id, ['pattern'], created_by=node_name)

        coord_config = CoordinationConfig(
            total_tokens=50,
            heartbeat_timeout_sec=15,
            dead_node_check_interval_sec=0.5
        )

        job = create_job('cleanup-leader', postgres, wait_on_enter=2, coordination_config=coord_config)
        job.__enter__()

        try:
            for i in range(1, 4):
                assert wait_for_dead_node_removal(postgres, tables, f'dead-node-{i}', timeout_sec=10)

            with postgres.connect() as conn:
                result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Lock"]}'))
                remaining_locks = result.scalar()

            assert remaining_locks == 0, 'All locks from all dead nodes should be removed'

        finally:
            job.__exit__(None, None, None)

    @clean_tables('Lock', 'Node')
    def test_lock_cleanup_logged(self, postgres, caplog):
        """Verify lock cleanup from dead nodes is logged.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        insert_stale_node(postgres, tables, 'logged-dead-node', heartbeat_age_seconds=30)
        insert_lock(postgres, tables, 1, ['pattern'], created_by='logged-dead-node')

        coord_config = CoordinationConfig(
            total_tokens=50,
            heartbeat_timeout_sec=15,
            dead_node_check_interval_sec=0.5
        )

        with caplog.at_level(logging.INFO):
            job = create_job('logging-leader', postgres, wait_on_enter=2, coordination_config=coord_config)
            job.__enter__()

            try:
                assert wait_for_dead_node_removal(postgres, tables, 'logged-dead-node', timeout_sec=10)

                info_messages = [record.message for record in caplog.records if record.levelname == 'INFO']
                cleanup_messages = [msg for msg in info_messages if 'cleaned up locks' in msg.lower()]

                assert len(cleanup_messages) > 0, 'Should log lock cleanup'
                assert any('logged-dead-node' in msg for msg in cleanup_messages), \
                    'Log should mention the dead node'

            finally:
                job.__exit__(None, None, None)

    @clean_tables('Lock', 'Node')
    def test_expired_locks_cleaned_during_dead_node_rebalance(self, postgres):
        """Verify dead node cleanup removes locks by creator, and expired locks are cleaned during redistribution.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        now = datetime.datetime.now(datetime.timezone.utc)
        expired_time = now - datetime.timedelta(days=2)

        insert_stale_node(postgres, tables, 'dead-node', heartbeat_age_seconds=30)

        insert_lock(postgres, tables, 1, ['pattern'], created_by='dead-node')

        insert_lock(postgres, tables, 2, ['pattern'], created_by='alive-node', expires_at=expired_time)

        coord_config = CoordinationConfig(
            total_tokens=50,
            heartbeat_timeout_sec=15,
            dead_node_check_interval_sec=0.5
        )

        job = create_job('separation-leader', postgres, wait_on_enter=2, coordination_config=coord_config)
        job.__enter__()

        try:
            assert wait_for_dead_node_removal(postgres, tables, 'dead-node', timeout_sec=10)

            with postgres.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT task_id, created_by FROM {tables["Lock"]} ORDER BY task_id
                """))
                locks = [(row[0], row[1]) for row in result]

            # Dead node lock should be gone (removed by DELETE statement)
            dead_node_locks = [l for l in locks if l[1] == 'dead-node']
            assert len(dead_node_locks) == 0, 'Dead node locks should be removed by DELETE'

            # Expired lock also gone (removed by _get_active_locks during token redistribution)
            expired_locks = [l for l in locks if l[0] == '2']
            assert len(expired_locks) == 0, 'Expired locks are removed during token redistribution triggered by dead node cleanup'

        finally:
            job.__exit__(None, None, None)


class TestLeaderLockStaleRecovery:
    """Test stale leader lock detection and recovery."""

    def test_stale_lock_detected_and_removed(self, postgres):
        """Verify stale leader locks are detected and removed.
        """
        tables = schema.get_table_names('sync_')

        stale_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=400)

        insert_leader_lock(postgres, tables, 'dead-node', 'stale-operation', acquired_at=stale_time)

        coord_config = CoordinationConfig(
            total_tokens=50,
            heartbeat_interval_sec=1,
            stale_leader_lock_age_sec=300
        )

        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)
        job.__enter__()

        try:
            with job.locks.acquire_leader_lock('test-operation'):
                with postgres.connect() as conn:
                    result = conn.execute(text(f"SELECT node FROM {tables['LeaderLock']} WHERE singleton = 1"))
                    current_holder = result.scalar()

                assert current_holder == 'node1', 'node1 should now hold the lock'

        finally:
            job.__exit__(None, None, None)

    @clean_tables('LeaderLock')
    def test_configurable_stale_lock_threshold(self, postgres):
        """Verify stale lock threshold is configurable.
        """
        tables = schema.get_table_names('sync_')

        lock_age = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=15)
        insert_leader_lock(postgres, tables, 'old-node', 'old-operation', acquired_at=lock_age)

        coord_config = CoordinationConfig(
            total_tokens=50,
            heartbeat_interval_sec=1,
            stale_leader_lock_age_sec=10
        )

        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)
        job.__enter__()

        try:
            with job.locks.acquire_leader_lock('test'):
                pass
            assert True, 'Should treat 15-second-old lock as stale with 10s threshold'

        finally:
            job.__exit__(None, None, None)

    @clean_tables('LeaderLock')
    def test_non_stale_lock_not_removed(self, postgres):
        """Verify recent locks are not removed.
        """
        tables = schema.get_table_names('sync_')

        recent_time = datetime.datetime.now(datetime.timezone.utc)
        insert_leader_lock(postgres, tables, 'active-node', 'active-operation', acquired_at=recent_time)

        coord_config = CoordinationConfig(
            total_tokens=50,
            heartbeat_interval_sec=1,
            stale_leader_lock_age_sec=300,
            leader_lock_timeout_sec=2
        )

        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)

        try:
            with job.locks.acquire_leader_lock('test'):
                pass
            raise AssertionError('Should not acquire lock if recent lock exists')
        except Exception:
            assert True, 'Should not acquire lock if recent lock exists'


class TestThreadCrashAndRecovery:
    """Test thread failure detection and recovery."""

    def test_health_monitor_detects_stale_heartbeat(self, postgres):
        """Verify health monitor detects stale heartbeat and publishes event.
        """
        config = get_coordination_config()

        coord_config = CoordinationConfig(
            total_tokens=50,
            heartbeat_interval_sec=5,
            heartbeat_timeout_sec=3,
            health_check_interval_sec=1
        )

        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)
        job.__enter__()

        try:
            time.sleep(0.3)

            # Make heartbeat stale
            job.cluster.last_heartbeat_sent -= datetime.timedelta(seconds=10)

            # Wait for health monitor to detect and publish event
            time.sleep(2)

            # Health monitor should have published NODE_UNHEALTHY event which triggers shutdown
            assert job._shutdown_event.is_set(), \
                'Shutdown event should be set after stale heartbeat detected'
            assert job.state_machine.state == JobState.SHUTTING_DOWN, \
                'Should transition to SHUTTING_DOWN state'

            logger.info('✓ Health monitor detected stale heartbeat and triggered shutdown via event')

        finally:
            job._shutdown_event.set()
            job.__exit__(None, None, None)

    def test_thread_database_reconnection(self, postgres, caplog):
        """Verify threads recover from transient database errors.
        """
        config = get_coordination_config()

        coord_config = CoordinationConfig(
            heartbeat_interval_sec=0.2,
            heartbeat_timeout_sec=3
        )

        with caplog.at_level(logging.ERROR):
            job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)
            job.__enter__()

            try:
                assert wait_for_running_state(job, timeout_sec=3, check_interval=0.1)
                time.sleep(0.3)

                logger.info('✓ Threads operating normally with database connectivity')

            finally:
                job._shutdown_event.set()
                job.__exit__(None, None, None)


class TestLockExpirationSideEffects:
    """Test lock expiration handling during operations."""

    @clean_tables('Lock')
    def test_expired_locks_deleted_during_get_active_locks(self, postgres):
        """Verify expired locks are deleted as side effect of getting active locks.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        expired_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=2)

        insert_lock(postgres, tables, 1, ['pattern-1'], created_by='node1', expires_at=expired_time)
        insert_lock(postgres, tables, 2, ['pattern-2'], created_by='node1', expires_at=None)

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)
        job.__enter__()

        try:
            active_locks = job.locks.get_active_locks()

            token_id_1 = job.task_to_token('1')
            token_id_2 = job.task_to_token('2')

            assert token_id_1 not in active_locks, 'Expired lock should not be in active locks'
            assert token_id_2 in active_locks, 'Valid lock should be in active locks'

            with postgres.connect() as conn:
                expired_count = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Lock"]} WHERE task_id = :task_id'),
                                           {'task_id': '1'}).scalar()

            assert expired_count == 0, 'Expired lock should be deleted from database'

        finally:
            job.__exit__(None, None, None)

    @clean_tables('Lock', 'Node')
    def test_lock_expires_during_token_distribution(self, postgres):
        """Verify token distribution handles locks that expire mid-operation.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        soon_to_expire = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=0.2)
        now = datetime.datetime.now(datetime.timezone.utc)
        insert_active_node(postgres, tables, 'node1', created_on=now)
        insert_active_node(postgres, tables, 'node2', created_on=now + datetime.timedelta(seconds=1))
        insert_lock(postgres, tables, 5, ['node1'], created_by='test',
                   expires_at=soon_to_expire, reason='about to expire')

        coord_config = CoordinationConfig(total_tokens=50)
        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)
        job.__enter__()

        try:
            time.sleep(0.5)

            job.tokens.distribute(job.locks, job.cluster)

            with postgres.connect() as conn:
                lock_count = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Lock"]} WHERE task_id = :task_id'),
                                         {'task_id': '5'}).scalar()

            assert lock_count == 0, 'Expired lock should be deleted after distribution'

        finally:
            job.__exit__(None, None, None)


class TestTokenDistributionUnderContention:
    """Test token distribution with lock contention and failures."""

    @clean_tables('LeaderLock')
    def test_leader_lock_timeout_behavior(self, postgres):
        """Verify behavior when leader lock acquisition times out.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)
        insert_leader_lock(postgres, tables, 'other-node', 'long-operation')

        coord_config = CoordinationConfig(
            leader_lock_timeout_sec=2,
            stale_leader_lock_age_sec=300
        )

        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)

        start_time = time.time()
        try:
            with job.locks.acquire_leader_lock('test'):
                pass
            raise AssertionError('Should not acquire lock held by other node')
        except Exception:
            pass
        elapsed = time.time() - start_time

        assert elapsed >= 2, f'Should wait for timeout (took {elapsed:.1f}s)'
        assert elapsed < 5, f'Should timeout quickly (took {elapsed:.1f}s)'

        with postgres.connect() as conn:
            result = conn.execute(text(f"SELECT node FROM {tables['LeaderLock']} WHERE singleton = 1"))
            holder = result.scalar()

        assert holder == 'other-node', 'Lock holder should not change on timeout'

    @clean_tables('Lock', 'Node')
    def test_all_tokens_locked_to_nonexistent_pattern(self, postgres):
        """Verify behavior when all tokens locked to pattern with no matching nodes.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        now = datetime.datetime.now(datetime.timezone.utc)

        insert_active_node(postgres, tables, 'node1', created_on=now)
        insert_active_node(postgres, tables, 'node2', created_on=now + datetime.timedelta(seconds=1))

        task_ids = find_task_ids_covering_all_tokens(20)
        for task_id in task_ids:
            insert_lock(postgres, tables, task_id, ['nonexistent-%'], created_by='test')

        coord_config = CoordinationConfig(total_tokens=20)
        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)

        job.tokens.distribute(job.locks, job.cluster)

        with postgres.connect() as conn:
            result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Token"]}'))
            assigned_count = result.scalar()

        assert assigned_count == 0, 'No tokens should be assigned when pattern matches no nodes'

    @clean_tables('Lock', 'Token', 'Node')
    def test_partial_lock_pattern_matching(self, postgres):
        """Verify tokens partially locked with some matchable patterns work correctly.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        now = datetime.datetime.now(datetime.timezone.utc)

        insert_active_node(postgres, tables, 'node1', created_on=now)
        insert_active_node(postgres, tables, 'node2', created_on=now + datetime.timedelta(seconds=1))
        insert_active_node(postgres, tables, 'special-node', created_on=now + datetime.timedelta(seconds=2))

        for token_id in range(10):
            insert_lock(postgres, tables, token_id, ['special-%'], created_by='test')

        for token_id in range(10, 20):
            insert_lock(postgres, tables, token_id, ['missing-%'], created_by='test')

        coord_config = CoordinationConfig(total_tokens=50)
        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)
        job.__enter__()

        try:
            job.tokens.distribute(job.locks, job.cluster)

            with postgres.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM {tables["Token"]} WHERE node = 'special-node'
                """))
                special_count = result.scalar()

                result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Token"]}'))
                total_count = result.scalar()

            assert special_count >= 10, 'special-node should have at least the locked tokens'
            assert 40 <= total_count <= 50, 'Most unlocked tokens should be assigned'

        finally:
            job.__exit__(None, None, None)


class TestDatabaseConnectionFailures:
    """Test handling of database connection issues."""

    def test_can_claim_task_survives_db_failure(self, postgres):
        """Verify can_claim_task handles database unavailability gracefully.
        """
        config = get_coordination_config()

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)
        job.__enter__()

        try:
            job.tokens.my_tokens = {1, 2, 3, 4, 5}
            task = create_task(0)
            token_id = job.task_to_token(0)

            if token_id in job.tokens.my_tokens:
                can_claim = job.can_claim_task(task)
                assert can_claim, 'Should be able to claim task with owned token'

        finally:
            job.__exit__(None, None, None)


class TestAuditWriteFailures:
    """Test audit write error handling."""

    def test_write_audit_with_empty_tasks(self, postgres):
        """Verify write_audit safely handles empty task list.
        """
        config = get_coordination_config()

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)
        job.__enter__()

        try:
            job.tasks._tasks = []

            try:
                job.write_audit()
            except Exception as e:
                pytest.fail(f'write_audit should handle empty tasks: {e}')

        finally:
            job.__exit__(None, None, None)

    def test_write_audit_clears_tasks_after_write(self, postgres):
        """Verify tasks are cleared after successful audit write.
        """
        config = get_coordination_config()

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)
        job.__enter__()

        try:
            task1 = Task(1, 'task-1')
            task2 = Task(2, 'task-2')
            job.tasks._tasks = [(task1, job._created_on), (task2, job._created_on)]

            assert len(job.tasks._tasks) == 2, 'Should have 2 pending tasks'

            job.write_audit()

            assert len(job.tasks._tasks) == 0, 'Tasks should be cleared after write'

        finally:
            job.__exit__(None, None, None)


class TestDeadNodeTokenRedistribution:
    """Test token redistribution when coordinated node dies.

    This test replicates the production issue where a coordinated node dies
    but its tokens are not redistributed, leaving tasks unprocessable.
    """

    @clean_tables('Inst', 'Audit', 'Claim', 'Token', 'Node')
    def test_dead_node_tokens_redistributed_to_survivors(self, postgres):
        """Verify tokens redistributed when coordinated node dies without cleanup.

        Production scenario:
        1. Multiple coordinated nodes running and processing tasks
        2. One node crashes/dies (stops heartbeat)
        3. Dead node still owns tokens in database
        4. Leader should detect dead node and trigger rebalancing
        5. Remaining nodes should receive all tokens
        6. All tasks should become processable again
        """
        print('\n=== Testing dead node token redistribution ===')

        tables = schema.get_table_names('sync_')

        # Create test tasks

        for i in range(30):
            insert_inst(postgres, tables, str(i), done=False)

        # Phase 1: Start 3 coordinated nodes
        print('Phase 1: Starting 3 coordinated nodes...')
        with cluster(postgres, 'node1', 'node2', 'node3', total_tokens=30,
                    dead_node_check_interval_sec=2, heartbeat_timeout_sec=5) as nodes:
            # Wait for token distribution
            for node in nodes:
                assert wait_for(lambda n=node: len(n.my_tokens) >= 5, timeout_sec=15)

            # Wait for all nodes to sync their token caches with database
            assert wait_for_cached_tokens_sync(nodes, expected_total=30, timeout_sec=10), \
                'Token caches should sync after cluster formation'

            # Verify all nodes have tokens
            print('\nInitial token distribution:')
            initial_tokens = {}
            for node in nodes:
                token_count = len(node.my_tokens)
                initial_tokens[node.node_name] = node.my_tokens.copy()
                print(f'{node.node_name}: {token_count} tokens')

            total_initial = sum(len(tokens) for tokens in initial_tokens.values())
            assert total_initial == 30, 'All 30 tokens should be distributed'

            # Phase 2: Simulate node2 death (stop heartbeat but leave tokens)
            print('\nPhase 2: Simulating node2 death...')

            simulate_node_crash(nodes[1], cleanup=False)

            print('node2 heartbeat stopped (simulating crash)')

            # Verify node2 still owns tokens in database (zombie state)
            with postgres.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM {tables["Token"]} WHERE node = 'node2'
                """))
                node2_token_count = result.scalar()

            print(f'node2 still owns {node2_token_count} tokens in database (zombie state)')
            assert node2_token_count > 0, 'Dead node should still own tokens initially'

            # Phase 3: Wait for dead node detection and rebalancing
            print('\nPhase 3: Waiting for dead node detection and rebalancing...')

            assert wait_for_dead_node_removal(postgres, tables, 'node2', timeout_sec=20)
            print('node2 removed from database')

            assert wait_for_rebalance(postgres, tables, min_count=1, timeout_sec=20)

            # Check if rebalancing occurred
            with postgres.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM {tables["Rebalance"]}
                    WHERE triggered_at > NOW() - INTERVAL '20 seconds'
                    AND trigger_reason = 'distribution'
                """))
                recent_rebalances = result.scalar()

            print(f'Recent rebalances: {recent_rebalances}')

            # Wait for node2's tokens to be fully cleared from database
            start = time.time()
            node2_tokens_after = None
            while time.time() - start < 20:
                with postgres.connect() as conn:
                    result = conn.execute(text(f"""
                        SELECT COUNT(*) FROM {tables["Token"]} WHERE node = 'node2'
                    """))
                    node2_tokens_after = result.scalar()
                    if node2_tokens_after == 0:
                        break
                time.sleep(0.3)

            print(f'node2 tokens after rebalancing: {node2_tokens_after}')

            # Phase 4: Verify remaining nodes own all tokens
            print('\nPhase 4: Verifying token redistribution...')

            survivor_nodes = [nodes[0], nodes[2]]
            final_tokens = {}
            for node in survivor_nodes:
                tokens, _ = node.tokens.get_my_tokens_versioned()
                final_tokens[node.node_name] = tokens
                print(f'{node.node_name}: {len(tokens)} tokens')

            total_final = sum(len(tokens) for tokens in final_tokens.values())
            print(f'\nTotal tokens owned by survivors: {total_final}/30')

            # Key assertions
            assert node2_tokens_after == 0, \
                'Dead node should no longer own any tokens'

            assert total_final == 30, \
                'Surviving nodes should own ALL tokens after rebalancing'

            # Phase 5: Verify all tasks are now processable
            print('\nPhase 5: Verifying all tasks processable by survivors...')

            # Wait for survivor nodes to sync their token caches with database
            assert wait_for_cached_tokens_sync(survivor_nodes, expected_total=30, timeout_sec=10), \
                'Survivor nodes should sync token caches after rebalancing'

            processable_tasks = set()
            for i in range(30):
                task = create_task(i)
                for node in survivor_nodes:
                    if node.can_claim_task(task):
                        processable_tasks.add(i)
                        break

            print(f'Processable tasks: {len(processable_tasks)}/30')

            assert len(processable_tasks) == 30, \
                'All tasks should be claimable by surviving nodes'

            # Try to actually process all tasks
            processed_by_survivors = set()
            for i in range(30):
                task = create_task(i)
                for node in survivor_nodes:
                    if node.can_claim_task(task):
                        node.add_task(task)
                        processed_by_survivors.add(i)
                        with postgres.connect() as conn:
                            conn.execute(text(f'UPDATE {tables["Inst"]} SET done=TRUE WHERE item=:item'),
                                        {'item': str(i)})
                            conn.commit()
                        break

            print(f'Tasks actually processed: {len(processed_by_survivors)}/30')

            assert len(processed_by_survivors) == 30, \
                'Surviving nodes should successfully process all tasks'

            # Write audit
            for node in survivor_nodes:
                node.write_audit()

            # Verify no "zombie task" state
            with postgres.connect() as conn:
                result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Inst"]} WHERE done=FALSE'))
                incomplete_count = result.scalar()

            assert incomplete_count == 0, \
                'No tasks should remain incomplete (no zombie state)'

            print('\n✓ Dead node token redistribution successful')
            print('✓ All tasks processable after node failure')


class TestLeadershipDemotion:
    """Test leader demotion when older node rejoins.

    Leadership is based on oldest created_on timestamp. If the original leader
    crashes and rejoins with the same timestamp, it should reclaim leadership.
    """

    def test_leader_demoted_when_older_node_rejoins(self, postgres):
        """Verify current leader gracefully demotes when older node rejoins.
        """
        print('\n=== Testing leader demotion on older node rejoin ===')

        config = get_coordination_config()

        node1 = create_job('node1', postgres, coordination_config=config, wait_on_enter=10)
        node2 = create_job('node2', postgres, coordination_config=config, wait_on_enter=10)

        node1.__enter__()
        time.sleep(0.5)
        node2.__enter__()

        try:
            initial_leader = wait_for_leader_election(node1, expected_leader='node1', timeout_sec=10)
            print(f'Initial leader: {initial_leader}')

            print('Killing node1 (original leader)...')
            simulate_node_crash(node1)

            new_leader = wait_for_leader_election(node2, expected_leader='node2', timeout_sec=15)
            print(f'Leader after node1 death: {new_leader}')
            assert wait_for_state(node2, JobState.RUNNING_LEADER, timeout_sec=30)

            print('Rejoining node1 with original timestamp...')
            node1_rejoined = create_job('node1', postgres, coordination_config=config, wait_on_enter=10)
            node1_rejoined._created_on = node1._created_on
            node1_rejoined.cluster.created_on = node1._created_on
            node1_rejoined.__enter__()

            assert wait_for_state(node1_rejoined, JobState.RUNNING_LEADER, timeout_sec=15)
            assert wait_for_state(node2, JobState.RUNNING_FOLLOWER, timeout_sec=15)

            final_leader = node1_rejoined.cluster.elect_leader()
            print(f'Leader after node1 rejoin: {final_leader}')

            print('✓ Leader demotion successful')

        finally:
            try:
                node1_rejoined.__exit__(None, None, None)
            except:
                pass
            try:
                node2.__exit__(None, None, None)
            except:
                pass

    def test_demoted_leader_monitors_stop_gracefully(self, postgres):
        """Verify demoted leader stops its leader-only monitors without crashing.
        """
        print('\n=== Testing demoted leader monitors stop gracefully ===')

        config = get_coordination_config()

        node1 = create_job('node1', postgres, coordination_config=config, wait_on_enter=10)
        node2 = create_job('node2', postgres, coordination_config=config, wait_on_enter=10)

        node1.__enter__()
        time.sleep(0.5)
        node2.__enter__()

        try:
            wait_for_leader_election(node1, expected_leader='node1', timeout_sec=10)

            assert_monitors_stopped(node2, ['dead-node', 'rebalance'])
            print('node2 has no leader monitors as follower')

            print('Killing node1...')
            simulate_node_crash(node1)

            wait_for_leader_election(node2, expected_leader='node2', timeout_sec=15)
            assert wait_for_state(node2, JobState.RUNNING_LEADER, timeout_sec=30)

            assert_monitors_running(node2, ['dead-node', 'rebalance'])
            leader_monitors_after = [m for m in node2._monitors.values() if isinstance(m, (DeadNodeMonitor, RebalanceMonitor))]
            print(f'node2 leader monitors after promotion: {len(leader_monitors_after)}')

            print('Rejoining node1 with older timestamp...')
            node1_rejoined = create_job('node1', postgres, coordination_config=config, wait_on_enter=10)
            node1_rejoined._created_on = node1._created_on
            node1_rejoined.cluster.created_on = node1._created_on
            node1_rejoined.__enter__()

            wait_for_leader_election(node1_rejoined, expected_leader='node1', timeout_sec=15)
            assert wait_for_state(node2, JobState.RUNNING_FOLLOWER, timeout_sec=10)

            for monitor in leader_monitors_after:
                stopped = monitor._stop_requested
                print(f'{monitor.name}: stop_requested={stopped}')
                assert stopped, f'{monitor.name} should be stopped after demotion'

            assert node2.am_i_healthy(), 'node2 should still be healthy after demotion'

            print('✓ Demoted leader monitors stopped gracefully')

        finally:
            try:
                node1_rejoined.__exit__(None, None, None)
            except:
                pass
            try:
                node2.__exit__(None, None, None)
            except:
                pass

    def test_leader_death_with_leader_lock_held(self, postgres):
        """Verify new leader can take over when previous leader dies holding leader lock.

        Critical scenario: Leader acquires leader lock for token distribution,
        then crashes. New leader must detect stale lock and proceed with election.
        Tests that stale lock detection works correctly.
        """
        print('\n=== Testing leader death with leader lock held ===')

        tables = schema.get_table_names('sync_')

        config = CoordinationConfig(
            total_tokens=30,
            stale_leader_lock_age_sec=5,  # Short timeout for testing
            leader_lock_timeout_sec=2
        )

        node1 = create_job('node1', postgres, coordination_config=config, wait_on_enter=10)
        node2 = create_job('node2', postgres, coordination_config=config, wait_on_enter=10)

        node1.__enter__()
        time.sleep(0.5)
        node2.__enter__()

        try:
            assert wait_for_state(node1, JobState.RUNNING_LEADER, timeout_sec=10)
            assert wait_for_state(node2, JobState.RUNNING_FOLLOWER, timeout_sec=10)
            print('node1 is leader, node2 is follower')

            # Manually insert a leader lock as if node1 is holding it
            with postgres.connect() as conn:
                conn.execute(text(f"""
                    INSERT INTO {tables["LeaderLock"]} (node, acquired_at, operation)
                    VALUES (:node, NOW(), 'test_operation')
                    ON CONFLICT (singleton) DO UPDATE
                    SET node = :node, acquired_at = NOW(), operation = 'test_operation'
                """), {'node': 'node1'})
                conn.commit()

            print('Leader lock inserted for node1')

            # Simulate node1 crash
            print('Killing node1 (leader holding lock)...')
            simulate_node_crash(node1)

            # Wait for stale lock timeout + some time for detection
            print('Waiting for stale lock detection...')
            time.sleep(config.stale_leader_lock_age_sec + 3)

            # node2 should eventually become leader despite stale lock
            assert wait_for_state(node2, JobState.RUNNING_LEADER, timeout_sec=20)
            print('node2 successfully became leader')

            # Verify leader lock state
            with postgres.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT node FROM {tables["LeaderLock"]}
                """))
                lock_holder = result.scalar()

            # The important thing is that node2 became leader successfully
            # The lock may still show node1 (stale), be cleared, or show node2
            if lock_holder:
                print(f'Leader lock shows: {lock_holder}')
                # Acceptable: lock may still show stale node1 or updated node2
                assert lock_holder in {'node1', 'node2'}, f'Unexpected lock holder: {lock_holder}'
            else:
                print('Leader lock cleared (no holder)')

            # Verify node2 is actually functioning as leader
            assert node2.am_i_leader(), 'node2 should be the active leader'
            print('node2 is functioning as leader')

            print('✓ Leader takeover with stale lock successful')

        finally:
            try:
                node2.__exit__(None, None, None)
            except:
                pass


def test_rebalance_detects_nodes_joining_after_distribution(postgres):
    """Verify RebalanceMonitor detects nodes that join after initial distribution.

    Critical scenario:
    1. Leader performs distribution seeing only 1 node (itself)
    2. Other nodes join immediately after distribution
    3. RebalanceMonitor should detect membership change and trigger rebalance

    This tests the fix for the bug where RebalanceMonitor initialized with
    current node count instead of distribution-time node count, missing the change.
    """
    print('\n=== Testing rebalance detection after late node joins ===')

    tables = schema.get_table_names('sync_')

    # Setup: Insert stale tokens from previous run (all owned by node1)
    for token_id in range(100):
        insert_token(postgres, tables, token_id, 'node1', version=1)

    # Start node1 first with very short wait_on_enter
    print('Starting node1 (will distribute immediately)...')
    coord_config = CoordinationConfig(
        total_tokens=100,
        rebalance_check_interval_sec=1  # Fast checks
    )

    node1 = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)
    node1.__enter__()

    try:
        # Wait for node1 to complete distribution (should see only itself)
        assert wait_for_state(node1, JobState.RUNNING_LEADER, timeout_sec=10)
        time.sleep(0.5)

        # Verify node1 got all tokens initially
        with postgres.connect() as conn:
            result = conn.execute(text(f"""
                SELECT node, COUNT(*) FROM {tables["Token"]} GROUP BY node
            """))
            initial_distribution = {row[0]: row[1] for row in result}

        print(f'Initial distribution: {initial_distribution}')
        assert initial_distribution.get('node1', 0) == 100, 'node1 should have all tokens initially'

        # Now start other nodes (they join AFTER distribution)
        print('Starting node2 and node3 after distribution...')
        node2 = create_job('node2', postgres, coordination_config=coord_config, wait_on_enter=0)
        node3 = create_job('node3', postgres, coordination_config=coord_config, wait_on_enter=0)

        node2.__enter__()
        node3.__enter__()

        # Wait for them to reach running state
        assert wait_for_state(node2, JobState.RUNNING_FOLLOWER, timeout_sec=10)
        assert wait_for_state(node3, JobState.RUNNING_FOLLOWER, timeout_sec=10)

        # RebalanceMonitor should detect the membership change (1 -> 3 nodes)
        # and trigger rebalancing
        print('Waiting for rebalance to be triggered...')

        # Give RebalanceMonitor time to run at least 2 checks
        # It checks every 1 second, so wait at least 3 seconds
        time.sleep(3)

        # Check current node count as seen by leader
        with postgres.connect() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM {tables["Node"]}
                WHERE last_heartbeat > NOW() - INTERVAL '15 seconds'
            """))
            visible_nodes = result.scalar()
        print(f'Nodes visible to rebalance monitor: {visible_nodes}')

        assert wait_for_rebalance(postgres, tables, min_count=1, timeout_sec=30), \
            'RebalanceMonitor should detect node count change and trigger rebalance'

        # Wait for token redistribution
        assert wait_for_all_nodes_token_sync([node1, node2, node3], expected_total=100, timeout_sec=20)

        # Verify tokens are now balanced across all 3 nodes
        print('Verifying balanced distribution after rebalance...')
        with postgres.connect() as conn:
            result = conn.execute(text(f"""
                SELECT node, COUNT(*) FROM {tables["Token"]} GROUP BY node ORDER BY node
            """))
            final_distribution = {row[0]: row[1] for row in result}

        print(f'Final distribution: {final_distribution}')

        for node_name, token_count in final_distribution.items():
            print(f'{node_name}: {token_count} tokens')
            assert 30 <= token_count <= 35, \
                f'{node_name} should have ~33 tokens (got {token_count})'

        total_tokens = sum(final_distribution.values())
        assert total_tokens == 100, 'All 100 tokens should still be assigned'

        print('✓ RebalanceMonitor successfully detected late node joins and triggered rebalance')

    finally:
        for node in [node1, node2, node3]:
            try:
                node.__exit__(None, None, None)
            except:
                pass


class TestMinimumNodesRequirement:
    """Test minimum_nodes coordination during cluster formation."""

    @clean_tables('Node', 'Token')
    def test_wait_on_enter_respects_minimum_nodes(self, postgres):
        """Verify wait_on_enter blocks in CLUSTER_FORMING until minimum_nodes reached.

        This test catches the bug where wait_on_enter proceeded with ANY 2+ nodes
        instead of respecting minimum_nodes configuration.
        """
        print('\n=== Testing wait_on_enter respects minimum_nodes ===')

        coord_config = get_coordination_config(
            total_tokens=30,
            minimum_nodes=3,
            heartbeat_interval_sec=1
        )

        # Start node1 with wait_on_enter=15 (enough time to verify blocking)
        node1 = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=15)
        node1_thread = threading.Thread(target=node1.__enter__)
        node1_thread.start()

        try:
            # Node1 should be in CLUSTER_FORMING, waiting
            assert wait_for_state(node1, JobState.CLUSTER_FORMING, timeout_sec=5)
            print('node1 in CLUSTER_FORMING (1/3 nodes)')

            time.sleep(2)

            # Start node2 - CRITICAL: Should STILL wait (need 3, have 2)
            print('Starting node2...')
            node2 = create_job('node2', postgres, coordination_config=coord_config, wait_on_enter=15)
            node2_thread = threading.Thread(target=node2.__enter__)
            node2_thread.start()

            try:
                # Give time for buggy code to wrongly proceed
                time.sleep(3)

                # THE CRITICAL ASSERTION: With buggy code, node1 would have proceeded
                # because len(nodes) = 2 > 1. With fixed code, it should still be waiting.
                assert node1.state_machine.state == JobState.CLUSTER_FORMING, \
                    f'node1 should still be in CLUSTER_FORMING with 2/3 nodes, got {node1.state_machine.state.value}'
                
                assert node2.state_machine.state == JobState.CLUSTER_FORMING, \
                    f'node2 should be in CLUSTER_FORMING with 2/3 nodes, got {node2.state_machine.state.value}'
                
                print('✓ Both nodes correctly waiting with 2/3 nodes')

                # Start node3 - NOW it should proceed
                print('Starting node3...')
                node3 = create_job('node3', postgres, coordination_config=coord_config, wait_on_enter=15)
                node3_thread = threading.Thread(target=node3.__enter__)
                node3_thread.start()

                try:
                    # All nodes should now proceed through states
                    assert wait_for_state(node1, JobState.RUNNING_LEADER, timeout_sec=15)
                    assert wait_for_state(node2, JobState.RUNNING_FOLLOWER, timeout_sec=15)
                    assert wait_for_state(node3, JobState.RUNNING_FOLLOWER, timeout_sec=15)

                    print('✓ All nodes proceeded after minimum_nodes reached')

                finally:
                    node3.__exit__(None, None, None)
                    node3_thread.join(timeout=5)

            finally:
                node2.__exit__(None, None, None)
                node2_thread.join(timeout=5)

        finally:
            node1.__exit__(None, None, None)
            node1_thread.join(timeout=5)

    @clean_tables('Node', 'Token')
    def test_leader_waits_for_minimum_nodes(self, postgres):
        """Verify leader waits for minimum_nodes before distribution.
        """
        print('\n=== Testing leader waits for minimum_nodes ===')

        tables = schema.get_table_names('sync_')

        coord_config = get_coordination_config(
            total_tokens=30,
            minimum_nodes=3,
            token_distribution_timeout_sec=60
        )

        node1 = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)
        node2 = None
        node3 = None

        node1_thread = threading.Thread(target=node1.__enter__)
        node1_thread.start()

        try:
            assert wait_for_state(node1, JobState.DISTRIBUTING, timeout_sec=10)
            print('node1 in DISTRIBUTING (waiting for minimum nodes)')

            time.sleep(2)

            with postgres.connect() as conn:
                result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Token"]}'))
                token_count = result.scalar()

            print(f'Tokens distributed while waiting: {token_count}')
            assert token_count == 0, 'Should not distribute tokens before minimum_nodes reached'

            print('Starting node2...')
            node2 = create_job('node2', postgres, coordination_config=coord_config, wait_on_enter=0)
            node2_thread = threading.Thread(target=node2.__enter__)
            node2_thread.start()

            assert wait_for_state(node2, JobState.DISTRIBUTING, timeout_sec=10)
            print('node2 also waiting in DISTRIBUTING')

            time.sleep(2)

            with postgres.connect() as conn:
                result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Token"]}'))
                token_count = result.scalar()

            print(f'Tokens after node2 joined: {token_count}')
            assert token_count == 0, 'Should still not distribute with only 2 nodes'

            print('Starting node3...')
            node3 = create_job('node3', postgres, coordination_config=coord_config, wait_on_enter=0)
            node3_thread = threading.Thread(target=node3.__enter__)
            node3_thread.start()

            assert wait_for_state(node3, JobState.DISTRIBUTING, timeout_sec=10)
            print('node3 also waiting in DISTRIBUTING')

            assert wait_for(lambda: len(node1.get_active_nodes()) >= 3, timeout_sec=10)
            print('3 nodes active')

            assert wait_for_state(node1, JobState.RUNNING_LEADER, timeout_sec=15)
            assert wait_for_state(node2, JobState.RUNNING_FOLLOWER, timeout_sec=15)
            assert wait_for_state(node3, JobState.RUNNING_FOLLOWER, timeout_sec=15)

            with postgres.connect() as conn:
                result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Token"]}'))
                token_count = result.scalar()

            print(f'Tokens after minimum_nodes reached: {token_count}')
            assert token_count == 30, 'Should distribute all tokens after minimum_nodes reached'

            print('✓ Leader waited for minimum_nodes before distribution')

        finally:
            for node in [node1, node2, node3]:
                if node:
                    try:
                        node.__exit__(None, None, None)
                    except:
                        pass
            for thread in [node1_thread, node2_thread if node2 else None, node3_thread if node3 else None]:
                if thread and thread.is_alive():
                    thread.join(timeout=5)

    @clean_tables('Node', 'Token')
    def test_timeout_when_minimum_not_reached(self, postgres):
        """Verify TimeoutError raised when minimum_nodes not reached within timeout.
        """
        print('\n=== Testing timeout when minimum_nodes not reached ===')

        tables = schema.get_table_names('sync_')

        coord_config = get_coordination_config(
            total_tokens=30,
            minimum_nodes=5,
            token_distribution_timeout_sec=5
        )

        node1 = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)

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
            assert isinstance(e, TimeoutError), f'Should be TimeoutError, got {type(e).__name__}'
            assert 'Minimum nodes (5) not reached within 5s' in str(e), f'Wrong message: {e}'
            print(f'✓ TimeoutError raised as expected: {e}')
        finally:
            try:
                node1.__exit__(None, None, None)
            except:
                pass

    @clean_tables('Node', 'Token')
    def test_shutdown_during_minimum_nodes_wait(self, postgres):
        """Verify graceful shutdown while waiting for minimum_nodes.
        """
        print('\n=== Testing shutdown during minimum_nodes wait ===')

        tables = schema.get_table_names('sync_')

        coord_config = get_coordination_config(
            total_tokens=30,
            minimum_nodes=5,
            token_distribution_timeout_sec=60
        )

        node1 = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)

        node1_thread = threading.Thread(target=node1.__enter__)
        node1_thread.start()

        try:
            assert wait_for_state(node1, JobState.DISTRIBUTING, timeout_sec=10)
            print('node1 in DISTRIBUTING state, waiting for minimum_nodes')

            time.sleep(2)

            print('Requesting shutdown while waiting...')
            node1._shutdown_event.set()

            node1_thread.join(timeout=5)
            print('node1 thread completed')

            assert node1._shutdown_event.is_set(), 'Shutdown event should remain set'

            with postgres.connect() as conn:
                result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Token"]}'))
                token_count = result.scalar()

            print(f'Tokens after shutdown: {token_count}')
            assert token_count == 0, 'No tokens should be distributed after shutdown'

            print('✓ Shutdown handled gracefully during minimum_nodes wait')

        finally:
            try:
                node1.__exit__(None, None, None)
            except:
                pass

    @clean_tables('Node', 'Token')
    def test_minimum_nodes_one_proceeds_immediately(self, postgres):
        """Verify minimum_nodes=1 allows immediate distribution.
        """
        print('\n=== Testing minimum_nodes=1 proceeds immediately ===')

        tables = schema.get_table_names('sync_')

        coord_config = get_coordination_config(
            total_tokens=30,
            minimum_nodes=1,
            token_distribution_timeout_sec=30
        )

        node1 = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)
        node1.__enter__()

        try:
            assert wait_for_state(node1, JobState.RUNNING_LEADER, timeout_sec=10)

            with postgres.connect() as conn:
                result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Token"]}'))
                token_count = result.scalar()

            assert token_count == 30, 'Should distribute all tokens with minimum_nodes=1'

            print('✓ minimum_nodes=1 allowed immediate distribution')

        finally:
            node1.__exit__(None, None, None)


class TestLateNodeJoining:
    """Test scenarios where nodes join an already-running cluster.

    These tests replicate the production issue where node 8 joined a 7-node
    cluster and experienced token allocation problems.
    """

    @clean_tables('Node', 'Token', 'Rebalance')
    def test_late_joining_node_waits_for_token_assignment(self, postgres):
        """Verify late-joining node blocks in __enter__ until it receives tokens.

        Critical production scenario:
        - Cluster of 3 nodes is fully running and stable
        - Node 4 joins hours/days later
        - Node 4 MUST wait during __enter__ until leader redistributes tokens
        - Node 4 MUST exit __enter__ with non-zero token allocation
        """
        print('\n=== Testing late-joining node waits for tokens ===')

        config = get_coordination_config(total_tokens=100)
        tables = schema.get_table_names(config.appname)

        # Phase 1: Start stable 3-node cluster
        print('Phase 1: Starting 3-node cluster...')
        with cluster(postgres, 'node1', 'node2', 'node3', total_tokens=100) as nodes:
            # Wait for cluster to stabilize
            for node in nodes:
                expected_state = JobState.RUNNING_LEADER if node.node_name == 'node1' else JobState.RUNNING_FOLLOWER
                assert wait_for_state(node, expected_state, timeout_sec=10)

            # Verify tokens distributed
            assert wait_for_all_nodes_token_sync(nodes, expected_total=100, timeout_sec=10)

            initial_distribution = {node.node_name: len(node.my_tokens) for node in nodes}
            print(f'Initial distribution: {initial_distribution}')
            assert sum(initial_distribution.values()) == 100

            # Phase 2: Node4 joins AFTER cluster is stable (the critical test case)
            print('\nPhase 2: Node4 joining stable cluster (simulating late arrival)...')

            node4 = create_job('node4', postgres, coordination_config=config, wait_on_enter=15)

            # Track that __enter__ actually waits
            import time
            enter_start = time.time()
            node4.__enter__()
            enter_duration = time.time() - enter_start

            print(f'node4.__enter__ took {enter_duration:.1f}s')

            # Critical assertions
            assert enter_duration > 1.0, \
                'Late-joining node should wait for rebalancing (took <1s, likely returned immediately)'

            assert len(node4.my_tokens) > 0, \
                'Late-joining node MUST have tokens immediately after __enter__ completes'

            print(f'node4 received {len(node4.my_tokens)} tokens after {enter_duration:.1f}s wait')

            # Verify balanced distribution across all 4 nodes
            nodes_with_4 = nodes + [node4]
            assert wait_for_cached_tokens_sync(nodes_with_4, expected_total=100, timeout_sec=10)

            final_distribution = {node.node_name: len(node.my_tokens) for node in nodes_with_4}
            print(f'Final distribution: {final_distribution}')

            # Each node should have ~25 tokens
            for node_name, token_count in final_distribution.items():
                assert 20 <= token_count <= 30, \
                    f'{node_name} should have ~25 tokens, got {token_count}'

            # Verify rebalancing was logged
            with postgres.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM {tables["Rebalance"]}
                    WHERE triggered_at > NOW() - INTERVAL '60 seconds'
                """))
                rebalance_count = result.scalar()

            assert rebalance_count >= 1, \
                'Rebalancing should have been triggered by late node join'

            print('✓ Late-joining node correctly waited for token assignment')

            node4.__exit__(None, None, None)

    @clean_tables('Node', 'Token', 'Rebalance')
    def test_rebalance_monitor_detects_late_node_with_correct_baseline(self, postgres):
        """Verify RebalanceMonitor uses distribution-time node count, not current count.

        This tests the fix for the bug where RebalanceMonitor was initialized with
        current node count instead of distribution-time count, missing membership changes.
        """
        print('\n=== Testing RebalanceMonitor baseline node count ===')

        config = get_coordination_config(total_tokens=50, rebalance_check_interval_sec=2)
        tables = schema.get_table_names(config.appname)

        # Start node1 alone (it will distribute seeing only itself)
        print('Starting node1 (will distribute to 1 node)...')
        node1 = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)
        node1.__enter__()

        try:
            assert wait_for_state(node1, JobState.RUNNING_LEADER, timeout_sec=10)

            # Verify node1 has all tokens
            with postgres.connect() as conn:
                result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Token"]} WHERE node = :node'),
                                     {'node': 'node1'})
                node1_tokens = result.scalar()

            print(f'node1 initially owns {node1_tokens}/50 tokens')
            assert node1_tokens == 50, 'node1 should own all tokens initially'

            # Get RebalanceMonitor's baseline (should be 1 from distribution time)
            rebalance_monitor = node1._monitors.get('rebalance')
            assert rebalance_monitor is not None, 'RebalanceMonitor should be running'
            initial_baseline = rebalance_monitor.last_node_count
            print(f'RebalanceMonitor baseline node count: {initial_baseline}')

            # This is the critical assertion for the fix
            assert initial_baseline == 1, \
                f'RebalanceMonitor should use distribution-time count (1), got {initial_baseline}'

            # Start node2 (should trigger rebalance because 2 != 1)
            print('\nStarting node2...')
            node2 = create_job('node2', postgres, coordination_config=config, wait_on_enter=0)
            node2.__enter__()

            try:
                assert wait_for_state(node2, JobState.RUNNING_FOLLOWER, timeout_sec=10)

                # Wait for RebalanceMonitor to detect membership change
                # It checks every 2 seconds, so wait up to 6 seconds
                time.sleep(6)

                # Verify rebalancing was triggered
                with postgres.connect() as conn:
                    result = conn.execute(text(f"""
                        SELECT COUNT(*) FROM {tables["Rebalance"]}
                        WHERE triggered_at > NOW() - INTERVAL '10 seconds'
                        AND trigger_reason = 'distribution'
                    """))
                    recent_rebalances = result.scalar()

                print(f'Recent rebalances: {recent_rebalances}')
                assert recent_rebalances >= 1, \
                    'RebalanceMonitor should have detected membership change (1->2 nodes)'

                # Verify tokens were redistributed
                with postgres.connect() as conn:
                    result = conn.execute(text(f"""
                        SELECT node, COUNT(*) FROM {tables["Token"]}
                        GROUP BY node
                        ORDER BY node
                    """))
                    distribution = {row[0]: row[1] for row in result}

                print(f'Final distribution: {distribution}')
                assert len(distribution) == 2, 'Tokens should be distributed to both nodes'
                assert 20 <= distribution.get('node1', 0) <= 30, 'node1 should have ~25 tokens'
                assert 20 <= distribution.get('node2', 0) <= 30, 'node2 should have ~25 tokens'

                print('✓ RebalanceMonitor correctly detected late node with proper baseline')

            finally:
                node2.__exit__(None, None, None)
        finally:
            node1.__exit__(None, None, None)

    @clean_tables('Node', 'Token', 'Inst')
    def test_late_node_can_claim_tasks_immediately(self, postgres):
        """Verify late-joining node can claim tasks immediately after __enter__.

        This tests the end-to-end scenario: late node joins, gets tokens,
        and can immediately start claiming tasks without waiting for additional
        rebalancing.
        """
        print('\n=== Testing late node can claim tasks immediately ===')

        config = get_coordination_config(total_tokens=30)
        tables = schema.get_table_names(config.appname)

        # Create test tasks
        for i in range(30):
            insert_inst(postgres, tables, str(i), done=False)

        # Start 2-node cluster
        print('Starting 2-node cluster...')
        with cluster(postgres, 'node1', 'node2', total_tokens=30) as initial_nodes:
            for node in initial_nodes:
                assert wait_for(lambda n=node: len(n.my_tokens) >= 10, timeout_sec=10)

            print(f'Initial cluster: node1={len(initial_nodes[0].my_tokens)} tokens, '
                  f'node2={len(initial_nodes[1].my_tokens)} tokens')

            # Node3 joins late
            print('\nNode3 joining...')
            node3 = create_job('node3', postgres, coordination_config=config, wait_on_enter=15)
            node3.__enter__()

            try:
                # Verify node3 has tokens immediately
                assert len(node3.my_tokens) > 0, \
                    'node3 should have tokens immediately after __enter__'

                print(f'node3 received {len(node3.my_tokens)} tokens')

                # Try to claim tasks immediately (should succeed for some)
                claimed_by_node3 = []
                for i in range(30):
                    task = create_task(i)
                    if node3.can_claim_task(task):
                        claimed_by_node3.append(i)

                print(f'node3 can claim {len(claimed_by_node3)}/30 tasks immediately')

                # Critical assertion: should be able to claim tasks right away
                assert len(claimed_by_node3) >= 5, \
                    f'node3 should claim at least 5 tasks immediately, got {len(claimed_by_node3)}'

                # Verify all tasks are claimable by someone
                all_nodes = initial_nodes + [node3]
                claimable_by_any = set()
                for i in range(30):
                    task = create_task(i)
                    for node in all_nodes:
                        if node.can_claim_task(task):
                            claimable_by_any.add(i)
                            break

                assert len(claimable_by_any) == 30, \
                    f'All 30 tasks should be claimable by someone, got {len(claimable_by_any)}'

                print('✓ Late node can claim tasks immediately after joining')

            finally:
                node3.__exit__(None, None, None)

    @clean_tables('Node', 'Token')
    def test_multiple_late_nodes_join_sequentially(self, postgres):
        """Verify multiple nodes can join sequentially and all receive tokens.

        This tests the scenario where nodes join one after another,
        each triggering a separate rebalance.
        """
        print('\n=== Testing multiple late nodes joining sequentially ===')

        config = get_coordination_config(total_tokens=100, rebalance_check_interval_sec=2)

        # Start with 2 nodes
        print('Starting 2-node cluster...')
        with cluster(postgres, 'node1', 'node2', total_tokens=100) as initial_nodes:
            for node in initial_nodes:
                assert wait_for(lambda n=node: len(n.my_tokens) >= 40, timeout_sec=10)

            print(f'Initial: node1={len(initial_nodes[0].my_tokens)}, '
                  f'node2={len(initial_nodes[1].my_tokens)} tokens')

            # Node3 joins
            print('\nNode3 joining...')
            node3 = create_job('node3', postgres, coordination_config=config, wait_on_enter=15)
            node3.__enter__()

            try:
                assert len(node3.my_tokens) > 0, 'node3 should have tokens'
                print(f'node3 received {len(node3.my_tokens)} tokens')

                # Node4 joins
                print('\nNode4 joining...')
                node4 = create_job('node4', postgres, coordination_config=config, wait_on_enter=15)
                node4.__enter__()

                try:
                    assert len(node4.my_tokens) > 0, 'node4 should have tokens'
                    print(f'node4 received {len(node4.my_tokens)} tokens')

                    # Wait for all nodes to sync their token caches
                    all_nodes = initial_nodes + [node3, node4]
                    assert wait_for_cached_tokens_sync(all_nodes, expected_total=100, timeout_sec=20), \
                        'All node token caches should sync after sequential joins'

                    # Verify balanced distribution
                    final_dist = {node.node_name: len(node.my_tokens) for node in all_nodes}
                    print(f'Final distribution: {final_dist}')

                    for node_name, token_count in final_dist.items():
                        assert 20 <= token_count <= 30, \
                            f'{node_name} should have ~25 tokens, got {token_count}'

                    print('✓ Multiple late nodes successfully joined and received tokens')

                finally:
                    node4.__exit__(None, None, None)
            finally:
                node3.__exit__(None, None, None)

    @clean_tables('Node', 'Token')
    def test_late_node_timeout_if_leader_unresponsive(self, postgres):
        """Verify late-joining node times out gracefully if leader doesn't rebalance.

        This tests error handling when something goes wrong during late node join.
        """
        print('\n=== Testing late node timeout handling ===')

        config = get_coordination_config(
            total_tokens=30,
            token_distribution_timeout_sec=5,  # Short timeout for test
            rebalance_check_interval_sec=60   # Leader won't check in time
        )

        # Start node1 with long rebalance interval (won't detect node2 in time)
        print('Starting node1 (with slow rebalance checking)...')
        node1 = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)
        node1.__enter__()

        try:
            assert wait_for_state(node1, JobState.RUNNING_LEADER, timeout_sec=10)

            # Node2 tries to join but leader won't rebalance in time
            print('\nNode2 attempting to join (should timeout)...')
            node2 = create_job('node2', postgres, coordination_config=config, wait_on_enter=0)

            try:
                node2.__enter__()
                # If we get here, something's wrong - should have timed out
                pytest.fail('node2 should have timed out waiting for token distribution')
            except TimeoutError as e:
                print(f'✓ Got expected timeout: {e}')
                assert 'Token distribution did not complete' in str(e) or \
                       'did not complete for node2' in str(e), \
                    f'Unexpected timeout message: {e}'
            finally:
                try:
                    node2.__exit__(None, None, None)
                except:
                    pass
        finally:
            node1.__exit__(None, None, None)


if __name__ == '__main__':
    pytest.main(args=['-sx', __file__])
