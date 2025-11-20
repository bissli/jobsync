"""Integration tests for hybrid coordination system.

These tests verify:
- 3-node cluster formation
- Leader election
- Token distribution
- Task claiming with token ownership
- Node death and failover
- Token rebalancing
- Lock registration and enforcement
"""
import logging
import multiprocessing
import threading
import time
from datetime import timedelta
from types import SimpleNamespace

import config as test_config
import pytest
from asserts import assert_equal, assert_true
from sqlalchemy import create_engine, text

from jobsync import schema
from jobsync.client import Job, Task

logger = logging.getLogger(__name__)


# Create coordination-enabled config for these tests
def get_coordination_config():
    """Create a config with coordination enabled for testing."""
    coord_config = SimpleNamespace()
    coord_config.postgres = test_config.postgres
    coord_config.sync = SimpleNamespace(
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
    return coord_config


def node_worker(node_name: str, connection_string: str, items: list, barrier: multiprocessing.Barrier, results: dict):
    """Worker function for multiprocessing node simulation.

    Args:
        node_name: Name of this node
        connection_string: SQLAlchemy connection string
        items: List of task IDs to attempt
        barrier: Synchronization barrier
        results: Shared dict for results
    """
    try:
        config = get_coordination_config()
        engine = create_engine(connection_string, pool_pre_ping=True, pool_size=10, max_overflow=5)
        tables = schema.get_table_names(config)

        with Job(node_name, config, wait_on_enter=10, connection_string=connection_string) as job:
            # Wait for all nodes to be ready
            barrier.wait()

            logger.info(f'{node_name}: Started with {len(job._my_tokens)} tokens')

            # Try to claim tasks
            claimed = []
            for item_id in items:
                task = Task(item_id)
                if job.can_claim_task(task):
                    job.add_task(task)
                    claimed.append(item_id)
                    with engine.connect() as conn:
                        conn.execute(text(f'UPDATE {tables["Inst"]} SET done=TRUE WHERE item=:item'), {'item': str(item_id)})
                        conn.commit()

            logger.info(f'{node_name}: Claimed {len(claimed)} tasks')
            results[node_name] = claimed

    except Exception as e:
        logger.error(f'{node_name}: Error - {e}')
        results[node_name] = {'error': str(e)}


def test_3node_cluster_formation(postgres):
    """Test that 3 nodes form a cluster, elect leader, and distribute tokens."""
    print('\n=== Testing 3-node cluster formation ===')

    config = get_coordination_config()
    tables = schema.get_table_names(config)
    connection_string = postgres.url.render_as_string(hide_password=False)

    nodes = []
    for i in range(1, 4):
        node = Job(f'node{i}', config, wait_on_enter=15, connection_string=connection_string)
        nodes.append(node)

    # Start all nodes in parallel so they discover each other during wait_on_enter
    def enter_node(node):
        node.__enter__()

    threads = []
    for node in nodes:
        t = threading.Thread(target=enter_node, args=(node,))
        t.start()
        threads.append(t)
        time.sleep(0.1)  # Stagger slightly to ensure predictable registration order

    # Wait for all nodes to complete their __enter__() blocking period
    for t in threads:
        t.join()

    try:
        # Wait for final coordination to complete
        time.sleep(5)

        # Verify all nodes registered
        active_nodes = nodes[0].get_active_nodes()
        print(f'Active nodes: {[n["name"] for n in active_nodes]}')
        assert_equal(len(active_nodes), 3, 'All 3 nodes should be active')

        # Verify leader elected (should be node1 - oldest)
        for node in nodes:
            leader = node._elect_leader()
            print(f'{node.node_name}: Leader is {leader}')
            assert_equal(leader, 'node1', 'node1 should be elected leader')

        # Verify tokens distributed
        total_tokens = 0
        for node in nodes:
            token_count = len(node._my_tokens)
            total_tokens += token_count
            print(f'{node.node_name}: {token_count} tokens, version {node._token_version}')
            assert_true(token_count > 0, f'{node.node_name} should have tokens')

        # Total should equal config total_tokens (100)
        assert_equal(total_tokens, 100, 'All tokens should be distributed')

        # Verify balanced distribution (each node should have ~33 tokens)
        for node in nodes:
            token_count = len(node._my_tokens)
            assert_true(25 <= token_count <= 40,
                       f'{node.node_name} should have balanced token count (got {token_count})')

        print('✓ Cluster formation successful')

    finally:
        # Cleanup
        for node in nodes:
            node.__exit__(None, None, None)


def test_token_based_task_claiming(postgres):
    """Test that nodes only claim tasks they own tokens for."""
    print('\n=== Testing token-based task claiming ===')

    config = get_coordination_config()
    tables = schema.get_table_names(config)
    connection_string = postgres.url.render_as_string(hide_password=False)

    with postgres.connect() as conn:
        for i in range(30):
            conn.execute(text(f'INSERT INTO {tables["Inst"]} (item, done) VALUES (:item, :done)'),
                        {'item': str(i), 'done': False})
        conn.commit()

    # Start 3 nodes with multiprocessing
    barrier = multiprocessing.Barrier(3)
    manager = multiprocessing.Manager()
    results = manager.dict()

    items = list(range(30))
    processes = [
        multiprocessing.Process(
            target=node_worker,
            args=(f'node{i}', connection_string, items, barrier, results)
        )
        for i in range(1, 4)
    ]

    for p in processes:
        p.start()

    for p in processes:
        p.join(timeout=30)
        assert_equal(p.exitcode, 0, 'Process should exit cleanly')

    # Verify results
    print(f'Results: {dict(results)}')

    # Check that all tasks were claimed
    claimed_tasks = set()
    for node_name, tasks in results.items():
        if isinstance(tasks, dict) and 'error' in tasks:
            pytest.fail(f'{node_name} had error: {tasks["error"]}')
        claimed_tasks.update(tasks)
        print(f'{node_name}: {len(tasks)} tasks')

    assert_equal(len(claimed_tasks), 30, 'All 30 tasks should be claimed')

    # Verify no duplicates (no two nodes claimed same task)
    total_claims = sum(len(tasks) for tasks in results.values() if isinstance(tasks, list))
    assert_equal(total_claims, 30, 'No duplicate claims')

    with postgres.connect() as conn:
        result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Inst"]} WHERE done=TRUE'))
        done_count = result.scalar()
    assert_equal(done_count, 30, 'All tasks should be marked done')

    print('✓ Token-based claiming successful')


def test_node_death_and_rebalancing(postgres):
    """Test that when a node dies, its tokens are redistributed."""
    print('\n=== Testing node death and rebalancing ===')

    config = get_coordination_config()
    connection_string = postgres.url.render_as_string(hide_password=False)

    nodes = []
    for i in range(1, 4):
        node = Job(f'node{i}', config, wait_on_enter=15, connection_string=connection_string)
        node.__enter__()
        nodes.append(node)

    try:
        time.sleep(5)

        # Record initial token distribution
        initial_tokens = {node.node_name: len(node._my_tokens) for node in nodes}
        print(f'Initial tokens: {initial_tokens}')

        # Kill node2 (simulate death by stopping heartbeat and exiting)
        print('Killing node2...')
        nodes[1]._shutdown = True
        nodes[1].__exit__(None, None, None)

        # Wait for dead node detection (timeout + check interval)
        time.sleep(12)

        # Remaining nodes should detect node2 as dead
        dead_nodes = nodes[0].get_dead_nodes()
        print(f'Dead nodes detected: {dead_nodes}')
        # Note: node2 might still be in the table if leader hasn't cleaned up yet

        # Wait a bit more for rebalancing
        time.sleep(10)

        # Check that node1 and node3 have more tokens now
        for node in [nodes[0], nodes[2]]:
            new_token_count = len(node._get_my_tokens())
            print(f'{node.node_name}: {initial_tokens[node.node_name]} -> {new_token_count} tokens')
            # Should have gained tokens (approximately 50 each instead of 33)
            assert_true(new_token_count > initial_tokens[node.node_name],
                       f'{node.node_name} should have more tokens after rebalance')

        # Verify total tokens still 100
        total = sum(len(node._get_my_tokens()) for node in [nodes[0], nodes[2]])
        print(f'Total tokens across remaining nodes: {total}')
        assert_true(total >= 90, 'Most tokens should be redistributed')

        print('✓ Node death and rebalancing successful')

    finally:
        # Cleanup remaining nodes
        for node in [nodes[0], nodes[2]]:
            try:
                node.__exit__(None, None, None)
            except:
                pass


def test_lock_registration_and_enforcement(postgres):
    """Test that locked tasks are only assigned to nodes matching the pattern."""
    print('\n=== Testing lock registration and enforcement ===')

    config = get_coordination_config()
    tables = schema.get_table_names(config)
    connection_string = postgres.url.render_as_string(hide_password=False)

    # Lock registration callback
    def register_locks(job):
        # Lock tasks 0-9 to 'special-%' pattern
        locks = [(i, 'special-%', 'test lock') for i in range(10)]
        job.register_locks_bulk(locks)
        print(f'{job.node_name}: Registered {len(locks)} locks')

    node1 = Job('node1', config, wait_on_enter=15, connection_string=connection_string,
                lock_provider=register_locks)
    node2 = Job('node2', config, wait_on_enter=15, connection_string=connection_string,
                lock_provider=register_locks)
    special = Job('special-alpha', config, wait_on_enter=15, connection_string=connection_string,
                  lock_provider=register_locks)

    node1.__enter__()
    node2.__enter__()
    special.__enter__()

    try:
        time.sleep(5)

        locked_token_owners = {}
        for task_id in range(10):
            token_id = node1._task_to_token(task_id)

            with postgres.connect() as conn:
                result = conn.execute(text(f'SELECT node FROM {tables["Token"]} WHERE token_id = :token_id'),
                                     {'token_id': token_id})
                owner = result.scalar()
            locked_token_owners[task_id] = owner
            print(f'Task {task_id} (token {token_id}) -> {owner}')

        # All locked tokens should be assigned to 'special-alpha'
        for task_id, owner in locked_token_owners.items():
            assert_equal(owner, 'special-alpha',
                        f'Locked task {task_id} should be assigned to special-alpha')

        # Verify regular nodes cannot claim locked tasks
        for task_id in range(10):
            task = Task(task_id)
            assert_equal(node1.can_claim_task(task), False,
                        f'node1 should not claim locked task {task_id}')
            assert_equal(node2.can_claim_task(task), False,
                        f'node2 should not claim locked task {task_id}')
            assert_equal(special.can_claim_task(task), True,
                        f'special-alpha should claim locked task {task_id}')

        print('✓ Lock enforcement successful')

    finally:
        node1.__exit__(None, None, None)
        node2.__exit__(None, None, None)
        special.__exit__(None, None, None)


def test_health_monitoring(postgres):
    """Test that nodes monitor their own health correctly."""
    print('\n=== Testing health monitoring ===')

    config = get_coordination_config()
    connection_string = postgres.url.render_as_string(hide_password=False)

    node = Job('node1', config, wait_on_enter=15, connection_string=connection_string)
    node.__enter__()

    try:
        # Initially should be healthy
        time.sleep(3)
        assert_true(node.am_i_healthy(), 'Node should be healthy initially')
        print(f'✓ Node is healthy (heartbeat age: {(node._last_heartbeat_sent)})')

        # Simulate heartbeat thread failure by stopping it
        node._heartbeat_thread = None
        node._last_heartbeat_sent = node._last_heartbeat_sent - timedelta(seconds=20)

        # Should now be unhealthy (heartbeat too old)
        is_healthy = node.am_i_healthy()
        print(f'After heartbeat failure: healthy={is_healthy}')
        assert_equal(is_healthy, False, 'Node should be unhealthy with stale heartbeat')

        print('✓ Health monitoring works correctly')

    finally:
        node._shutdown = True
        node.__exit__(None, None, None)


def test_leader_failover(postgres):
    """Test that when leader dies, a new leader is elected."""
    print('\n=== Testing leader failover ===')

    config = get_coordination_config()
    connection_string = postgres.url.render_as_string(hide_password=False)

    nodes = []
    for i in range(1, 4):
        node = Job(f'node{i}', config, wait_on_enter=15, connection_string=connection_string)
        node.__enter__()
        nodes.append(node)
        time.sleep(1)

    try:
        time.sleep(5)

        # Verify node1 is leader
        leader = nodes[0]._elect_leader()
        print(f'Initial leader: {leader}')
        assert_equal(leader, 'node1', 'node1 should be initial leader')

        # Kill node1
        print('Killing leader (node1)...')
        nodes[0]._shutdown = True
        nodes[0].__exit__(None, None, None)

        # Wait for timeout + detection
        time.sleep(12)

        # New leader should be elected (node2 - now oldest)
        new_leader = nodes[1]._elect_leader()
        print(f'New leader after failover: {new_leader}')
        assert_equal(new_leader, 'node2', 'node2 should become new leader')

        # Verify node3 also sees node2 as leader
        leader_from_node3 = nodes[2]._elect_leader()
        assert_equal(leader_from_node3, 'node2', 'All nodes should agree on leader')

        print('✓ Leader failover successful')

    finally:
        for node in [nodes[1], nodes[2]]:
            try:
                node.__exit__(None, None, None)
            except:
                pass


def test_node_rejoin_restores_original_allocation(postgres):
    """Test that when a dead node rejoins, it gets back its original token allocation."""
    print('\n=== Testing node rejoin restores original allocation ===')

    config = get_coordination_config()
    connection_string = postgres.url.render_as_string(hide_password=False)

    # Phase 1: Start all 4 nodes and record initial state
    print('Phase 1: Starting 4 nodes...')
    nodes = []
    for name in ['nodeA', 'nodeB', 'nodeC', 'nodeD']:
        node = Job(name, config, wait_on_enter=15, connection_string=connection_string)
        nodes.append(node)

    def enter_node(node):
        node.__enter__()

    threads = []
    for node in nodes:
        t = threading.Thread(target=enter_node, args=(node,))
        t.start()
        threads.append(t)
        time.sleep(0.1)

    for t in threads:
        t.join()

    try:
        time.sleep(5)

        # Record initial token allocations
        original_tokens = {}
        for node in nodes:
            tokens = node._get_my_tokens()
            original_tokens[node.node_name] = tokens.copy()
            print(f'{node.node_name}: {len(tokens)} tokens initially')

        # Verify all 100 tokens are distributed
        total_initial = sum(len(tokens) for tokens in original_tokens.values())
        assert_equal(total_initial, 100, 'All 100 tokens should be distributed initially')

        # Phase 2: Kill nodeD
        print('\nPhase 2: Killing nodeD...')
        nodeD = nodes[3]
        nodeD._shutdown = True
        nodeD.__exit__(None, None, None)

        # Wait for dead node detection and rebalancing
        time.sleep(15)

        # Verify nodeD's tokens were redistributed to A, B, C
        print('Verifying redistribution after nodeD death:')
        surviving_nodes = nodes[:3]
        for node in surviving_nodes:
            new_tokens = node._get_my_tokens()
            print(f'{node.node_name}: {len(original_tokens[node.node_name])} -> {len(new_tokens)} tokens')
            assert_true(len(new_tokens) > len(original_tokens[node.node_name]),
                       f'{node.node_name} should have gained tokens after nodeD died')

        # Phase 3: Rejoin nodeD
        print('\nPhase 3: Rejoining nodeD...')
        nodeD_rejoined = Job('nodeD', config, wait_on_enter=15, connection_string=connection_string)
        nodeD_rejoined.__enter__()
        nodes[3] = nodeD_rejoined

        # Wait for rebalancing to complete
        time.sleep(15)

        # Phase 4: Verify original allocations are restored
        print('\nPhase 4: Verifying original allocations restored:')
        all_restored = True
        for node in nodes:
            current_tokens = node._get_my_tokens()
            original = original_tokens[node.node_name]

            print(f'{node.node_name}: original={len(original)}, current={len(current_tokens)}, '
                  f'match={current_tokens == original}')

            if current_tokens != original:
                all_restored = False
                missing = original - current_tokens
                extra = current_tokens - original
                print(f'  Missing {len(missing)} tokens: {sorted(missing)[:10]}...')
                print(f'  Extra {len(extra)} tokens: {sorted(extra)[:10]}...')

        assert_true(all_restored, 'All nodes should have their original token allocations restored')

        # Verify total is still 100
        total_final = sum(len(node._get_my_tokens()) for node in nodes)
        assert_equal(total_final, 100, 'All 100 tokens should be distributed after rejoin')

        print('✓ Node rejoin successfully restored original allocation')

    finally:
        # Cleanup all nodes
        for node in nodes:
            try:
                node.__exit__(None, None, None)
            except:
                pass


if __name__ == '__main__':
    pytest.main(args=['-sx', __file__])
