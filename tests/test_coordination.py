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

import config as test_config
import database as db
import pytest
from asserts import assert_equal, assert_true

from jobsync import schema
from jobsync.client import Job, Task
from libb import Setting, delay

logger = logging.getLogger(__name__)


# Create coordination-enabled config for these tests
def get_coordination_config():
    """Create a config with coordination enabled for testing."""
    Setting.unlock()

    coord_config = Setting()
    coord_config.postgres = test_config.postgres
    coord_config.sqlite = test_config.sqlite

    coord_config.sync.sql.appname = 'sync_'
    coord_config.sync.coordination.enabled = True
    coord_config.sync.coordination.heartbeat_interval_sec = 2  # Faster for tests
    coord_config.sync.coordination.heartbeat_timeout_sec = 6
    coord_config.sync.coordination.rebalance_check_interval_sec = 5
    coord_config.sync.coordination.dead_node_check_interval_sec = 3
    coord_config.sync.coordination.token_refresh_initial_interval_sec = 2
    coord_config.sync.coordination.token_refresh_steady_interval_sec = 5
    coord_config.sync.coordination.total_tokens = 100  # Smaller for faster tests
    coord_config.sync.coordination.locks_enabled = True
    coord_config.sync.coordination.lock_orphan_warning_hours = 24
    coord_config.sync.coordination.leader_lock_timeout_sec = 10
    coord_config.sync.coordination.health_check_interval_sec = 5

    Setting.lock()
    return coord_config


def node_worker(node_name: str, site: str, items: list, barrier: multiprocessing.Barrier, results: dict):
    """Worker function for multiprocessing node simulation.

    Args:
        node_name: Name of this node
        site: Database site (postgres/sqlite)
        items: List of task IDs to attempt
        barrier: Synchronization barrier
        results: Shared dict for results
    """
    try:
        config = get_coordination_config()
        cn = db.connect(site, config)
        tables = schema.get_table_names(config)

        with Job(node_name, site, config, wait_on_enter=10, skip_db_init=True) as job:
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
                    # Simulate work
                    db.execute(cn, f'UPDATE {tables["Inst"]} SET done=TRUE WHERE item=%s', str(item_id))

            logger.info(f'{node_name}: Claimed {len(claimed)} tasks')
            results[node_name] = claimed

    except Exception as e:
        logger.error(f'{node_name}: Error - {e}')
        results[node_name] = {'error': str(e)}


def test_3node_cluster_formation(psql_docker, postgres):
    """Test that 3 nodes form a cluster, elect leader, and distribute tokens."""
    print('\n=== Testing 3-node cluster formation ===')

    config = get_coordination_config()
    cn = db.connect('postgres', config)
    tables = schema.get_table_names(config)

    # Create 3 node objects
    nodes = []
    for i in range(1, 4):
        node = Job(f'node{i}', 'postgres', config, wait_on_enter=15, skip_db_init=True)
        nodes.append(node)

    # Start all nodes in parallel so they discover each other during wait_on_enter
    def enter_node(node):
        node.__enter__()

    threads = []
    for node in nodes:
        t = threading.Thread(target=enter_node, args=(node,))
        t.start()
        threads.append(t)
        delay(0.1)  # Stagger slightly to ensure predictable registration order

    # Wait for all nodes to complete their __enter__() blocking period
    for t in threads:
        t.join()

    try:
        # Wait for final coordination to complete
        delay(5)

        # Verify all nodes registered
        active_nodes = nodes[0].get_active_nodes()
        print(f'Active nodes: {[n.name for n in active_nodes]}')
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


def test_token_based_task_claiming(psql_docker, postgres):
    """Test that nodes only claim tasks they own tokens for."""
    print('\n=== Testing token-based task claiming ===')

    config = get_coordination_config()
    cn = db.connect('postgres', config)
    tables = schema.get_table_names(config)

    # Create test data
    db.insert_rows(cn, tables['Inst'], [{'item': str(i), 'done': False} for i in range(30)])

    # Start 3 nodes with multiprocessing
    barrier = multiprocessing.Barrier(3)
    manager = multiprocessing.Manager()
    results = manager.dict()

    items = list(range(30))
    processes = [
        multiprocessing.Process(
            target=node_worker,
            args=(f'node{i}', 'postgres', items, barrier, results)
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

    # Verify all tasks marked done
    done_count = db.select_scalar(cn, f'SELECT COUNT(*) FROM {tables["Inst"]} WHERE done=TRUE')
    assert_equal(done_count, 30, 'All tasks should be marked done')

    print('✓ Token-based claiming successful')


def test_node_death_and_rebalancing(psql_docker, postgres):
    """Test that when a node dies, its tokens are redistributed."""
    print('\n=== Testing node death and rebalancing ===')

    config = get_coordination_config()
    cn = db.connect('postgres', config)

    # Start 3 nodes
    nodes = []
    for i in range(1, 4):
        node = Job(f'node{i}', 'postgres', config, wait_on_enter=15, skip_db_init=True)
        node.__enter__()
        nodes.append(node)

    try:
        delay(5)

        # Record initial token distribution
        initial_tokens = {node.node_name: len(node._my_tokens) for node in nodes}
        print(f'Initial tokens: {initial_tokens}')

        # Kill node2 (simulate death by stopping heartbeat and exiting)
        print('Killing node2...')
        nodes[1]._shutdown = True
        nodes[1].__exit__(None, None, None)

        # Wait for dead node detection (timeout + check interval)
        delay(12)

        # Remaining nodes should detect node2 as dead
        dead_nodes = nodes[0].get_dead_nodes()
        print(f'Dead nodes detected: {dead_nodes}')
        # Note: node2 might still be in the table if leader hasn't cleaned up yet

        # Wait a bit more for rebalancing
        delay(10)

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


def test_lock_registration_and_enforcement(psql_docker, postgres):
    """Test that locked tasks are only assigned to nodes matching the pattern."""
    print('\n=== Testing lock registration and enforcement ===')

    config = get_coordination_config()
    cn = db.connect('postgres', config)
    tables = schema.get_table_names(config)

    # Lock registration callback
    def register_locks(job):
        # Lock tasks 0-9 to 'special-%' pattern
        locks = [(i, 'special-%', 'test lock') for i in range(10)]
        job.register_task_locks_bulk(locks)
        print(f'{job.node_name}: Registered {len(locks)} locks')

    # Start 2 regular nodes and 1 special node
    node1 = Job('node1', 'postgres', config, wait_on_enter=15, skip_db_init=True,
                lock_provider=register_locks)
    node2 = Job('node2', 'postgres', config, wait_on_enter=15, skip_db_init=True,
                lock_provider=register_locks)
    special = Job('special-alpha', 'postgres', config, wait_on_enter=15, skip_db_init=True,
                  lock_provider=register_locks)

    node1.__enter__()
    node2.__enter__()
    special.__enter__()

    try:
        delay(5)

        # Check which node owns locked tokens
        locked_token_owners = {}
        for task_id in range(10):
            token_id = node1._task_to_token(task_id)

            # Query who owns this token
            sql = f'SELECT node FROM {tables["Token"]} WHERE token_id = %s'
            owner = db.select_scalar(cn, sql, token_id)
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


def test_health_monitoring(psql_docker, postgres):
    """Test that nodes monitor their own health correctly."""
    print('\n=== Testing health monitoring ===')

    config = get_coordination_config()

    node = Job('node1', 'postgres', config, wait_on_enter=15, skip_db_init=True)
    node.__enter__()

    try:
        # Initially should be healthy
        delay(3)
        assert_true(node.am_i_healthy(), 'Node should be healthy initially')
        print(f'✓ Node is healthy (heartbeat age: {(node._last_heartbeat_sent)})')

        # Simulate heartbeat thread failure by stopping it
        from datetime import timedelta
        node._heartbeat_thread = None
        node._last_heartbeat_sent -= timedelta(seconds=20)

        # Should now be unhealthy (heartbeat too old)
        is_healthy = node.am_i_healthy()
        print(f'After heartbeat failure: healthy={is_healthy}')
        assert_equal(is_healthy, False, 'Node should be unhealthy with stale heartbeat')

        print('✓ Health monitoring works correctly')

    finally:
        node._shutdown = True
        node.__exit__(None, None, None)


def test_leader_failover(psql_docker, postgres):
    """Test that when leader dies, a new leader is elected."""
    print('\n=== Testing leader failover ===')

    config = get_coordination_config()

    # Start 3 nodes (node1 will be leader - oldest)
    nodes = []
    for i in range(1, 4):
        node = Job(f'node{i}', 'postgres', config, wait_on_enter=15, skip_db_init=True)
        node.__enter__()
        nodes.append(node)
        delay(1)  # Stagger to ensure node1 is oldest

    try:
        delay(5)

        # Verify node1 is leader
        leader = nodes[0]._elect_leader()
        print(f'Initial leader: {leader}')
        assert_equal(leader, 'node1', 'node1 should be initial leader')

        # Kill node1
        print('Killing leader (node1)...')
        nodes[0]._shutdown = True
        nodes[0].__exit__(None, None, None)

        # Wait for timeout + detection
        delay(12)

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


if __name__ == '__main__':
    pytest.main(args=['-sx', __file__])
