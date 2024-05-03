import logging
import multiprocessing
import random

import config
import pytest
from asserts import assert_almost_equal, assert_equal
from jobsync import schema
from jobsync.client import Job, Task

import db
from libb import delay

logger = logging.getLogger(__name__)


def trigger(node_name, items, site, skip_sync=False, assert_node_count=3):
    total = 0
    kw = {'wait_on_enter': 5, 'skip_sync': skip_sync, 'skip_db_init': True}
    with Job(node_name, site, config, **kw) as self:
        # we want all nodes to be online
        assert_equal(len(self.get_idle()), assert_node_count)
        while True:
            # gather and process tasks
            inst = db.select(self.cn, f'select item, done from {schema.Inst} where done is False').to_dict('records')
            if not inst:
                break
            x_ref = [x['item'] for x in inst]
            random.shuffle(x_ref)
            x = x_ref[:5]
            total += len(x)
            logger.info(f'Node {self.node_name} found {len(inst)} items, working {len(x)}')
            [self.add_task(Task(i)) for i in x]
            # write that task if completed
            for item in x:
                db.update(self.cn, f'update {schema.Inst} set done=True where item = %s', item)
            # check-in and wait until other nodes are finished
            self.set_done()
            delay(0.3)
            for _ in range(10):
                if self.others_done():
                    break
                delay(3)


def run_only_one_node(site, skip_sync):
    cn = db.connect(site, config)
    items = 30
    nodes = 1
    db.insert_rows(cn, schema.Inst, [{'item': i, 'done': False} for i in range(items)])
    trigger('host1', items, site, skip_sync=skip_sync, assert_node_count=1)
    # nodes
    host1 = db.select_scalar_or_none(cn, f'select count(1) from {schema.Node} where name=%s', 'host1')
    print('No nodes should remain online (cleanup)')
    assert_equal(host1, 0)
    # checkpoints
    host1 = db.select_scalar_or_none(cn, f'select count(1) from {schema.Check} where node=%s', 'host1')
    print('No checkpoints should remain after run (cleanup)')
    assert_equal(host1, 0)
    # audit
    host1 = db.select_scalar_or_none(cn, f'select count(1) from {schema.Audit} where node=%s', 'host1')
    print(f'Host1 handled all tasks: {host1}')
    expect, delta = int(items / nodes), int(items / nodes)
    assert_almost_equal(host1, expect, delta=delta)


def run_more_than_one_node(site, threshold):
    cn = db.connect(site, config)
    items = 91
    nodes = 3
    db.insert_rows(cn, schema.Inst, [{'item': i, 'done': False} for i in range(items)])
    # simulate
    tasks = []
    for i in range(1, 4):
        tasks.append(
            multiprocessing.Process(target=trigger, args=(f'host{i}', items, site)))
    for task in tasks:
        task.start()
    for task in tasks:
        task.join()
    assert task.exitcode != 1, 'Multiprocessing terminated irregularly'
    # result
    # nodes
    host1 = db.select_scalar_or_none(cn, f'select count(1) from {schema.Node} where name=%s', 'host1')
    host2 = db.select_scalar_or_none(cn, f'select count(1) from {schema.Node} where name=%s', 'host2')
    host3 = db.select_scalar_or_none(cn, f'select count(1) from {schema.Node} where name=%s', 'host3')
    print('No nodes should remain online (cleanup)')
    assert_equal(host1, 0)
    assert_equal(host1, host2)
    assert_equal(host2, host3)
    # checkpoints
    host1 = db.select_scalar_or_none(cn, f'select count(1) from {schema.Check} where node=%s', 'host1')
    host2 = db.select_scalar_or_none(cn, f'select count(1) from {schema.Check} where node=%s', 'host2')
    host3 = db.select_scalar_or_none(cn, f'select count(1) from {schema.Check} where node=%s', 'host3')
    print('No checkpoints should remain after run (cleanup)')
    assert_equal(host1, 0)
    assert_equal(host1, host2)
    assert_equal(host2, host3)
    # audit
    host1 = db.select_scalar_or_none(cn, f'select count(1) from {schema.Audit} where node=%s', 'host1')
    host2 = db.select_scalar_or_none(cn, f'select count(1) from {schema.Audit} where node=%s', 'host2')
    host3 = db.select_scalar_or_none(cn, f'select count(1) from {schema.Audit} where node=%s', 'host3')
    print(f'Each node should process roughly the same number of tasks: {host1}, {host2}, {host3}')
    expect, delta = int(items / nodes), int((items / nodes) * threshold)  # multiprocessing somewhat unpredictable
    assert_almost_equal(host1, expect, delta=delta)
    assert_almost_equal(host2, expect, delta=delta)
    assert_almost_equal(host3, expect, delta=delta)


def test_simulate_postgres_multiple_nodes(psql_docker, postgres):
    print('Running simulation with Postgres with multiple nodes')
    run_more_than_one_node('postgres', 0.3)


def test_simulate_postgres_one_node_no_sync(psql_docker, postgres):
    print('Running simulation with Postgres one node no sync')
    run_only_one_node('postgres', skip_sync=False)


def test_simulate_postgres_one_node_sync(psql_docker, postgres):
    print('Running simulation with Postgres one node sync')
    run_only_one_node('postgres', skip_sync=True)


def test_simulate_sqlite_multiple_nodes(sqlite):
    print('Running simulation with Sqlite multilple nodes')
    run_more_than_one_node('sqlite', 0.4)


def test_simulate_sqlite_one_node_no_sync(sqlite):
    print('Running simulation with Sqlite one node no sync')
    run_only_one_node('sqlite', skip_sync=False)


def test_simulate_sqlite_one_node_sync(sqlite):
    print('Running simulation with Sqlite one node sync')
    run_only_one_node('sqlite', skip_sync=True)


if __name__ == '__main__':
    pytest.main(args=['-sx', __file__])
