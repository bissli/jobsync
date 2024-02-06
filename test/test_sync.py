import logging
import multiprocessing
import random
from pathlib import Path

import pytest
from asserts import assert_almost_equal, assert_equal
from conftest import conn
from jobsync import config, schema
from jobsync.sync import BaseStep, Job
from libb import delay

logger = logging.getLogger(__name__)

Inst = f'{config.sync.sql.appname}inst'


def trigger(node_name, items, db_url, skip_sync=False, assert_node_count=3):
    total = 0
    kw = {'wait_on_enter': 5, 'db_url': db_url, 'db_init': True,
          'skip_sync': skip_sync}
    with Job(node_name, **kw) as job:
        # we want all nodes to be online
        assert_equal(len(job.get_idle()), assert_node_count)
        while True:
            # gather and process tasks
            inst = list(job.db[Inst].find(done=False))  # iterdict
            if not inst:
                break
            x_ref = [x['item'] for x in inst]
            random.shuffle(x_ref)
            x = x_ref[:5]
            total += len(x)
            logger.info(f'Node {job.node_name} found {len(inst)} items, working {len(x)}')
            [job.add_step(BaseStep(i)) for i in x]
            # write that task if completed
            job.db[Inst].update({'done': True, 'item': x}, ['item'])
            # check-in and wait until other nodes are finished
            job.set_done()
            delay(0.3)
            for _ in range(10):
                if job.others_done():
                    break
                delay(3)


def run_only_one_node(db_url, skip_sync):
    with conn(db_url) as db:
        items = 30
        nodes = 1
        db[Inst].insert_many([{'item': i, 'done': False} for i in range(items)])
        trigger('host1', items, db_url, skip_sync=skip_sync, assert_node_count=1)
        # nodes
        host1 = db[schema.Node].count(name='host1')
        print('No nodes should remain online (cleanup)')
        assert_equal(host1, 0)
        # checkpoints
        host1 = db[schema.Check].count(node='host1')
        print('No checkpoints should remain after run (cleanup)')
        assert_equal(host1, 0)
        # audit
        host1 = db[schema.Audit].count(node='host1')
        print(f'Host1 handled all tasks: {host1}')
        expect, delta = int(items / nodes), int((items / nodes))
        assert_almost_equal(host1, expect, delta=delta)


def run_more_than_one_node(db_url, threshold):
    with conn(db_url) as db:
        items = 91
        nodes = 3
        db[Inst].insert_many([{'item': i, 'done': False} for i in range(items)])
        # simulate
        steps = []
        for i in range(1, 4):
            steps.append(
                multiprocessing.Process(target=trigger,
                                        args=(f'host{i}', items, db_url)))
        for step in steps:
            step.start()
        for step in steps:
            step.join()
        # result
        # nodes
        host1 = db[schema.Node].count(name='host1')
        host2 = db[schema.Node].count(name='host2')
        host3 = db[schema.Node].count(name='host3')
        print('No nodes should remain online (cleanup)')
        assert_equal(host1, 0)
        assert_equal(host1, host2)
        assert_equal(host2, host3)
        # checkpoints
        host1 = db[schema.Check].count(node='host1')
        host2 = db[schema.Check].count(node='host2')
        host3 = db[schema.Check].count(node='host3')
        print('No checkpoints should remain after run (cleanup)')
        assert_equal(host1, 0)
        assert_equal(host1, host2)
        assert_equal(host2, host3)
        # audit
        host1 = db[schema.Audit].count(node='host1')
        host2 = db[schema.Audit].count(node='host2')
        host3 = db[schema.Audit].count(node='host3')
        print(f'Each node should process roughly the same number of tasks: {host1}, {host2}, {host3}')
        expect, delta = int(items / nodes), int((items / nodes) * threshold)  # multiprocessing somewhat unpredictable
        assert_almost_equal(host1, expect, delta=delta)
        assert_almost_equal(host2, expect, delta=delta)
        assert_almost_equal(host3, expect, delta=delta)


class TestSimulatePostgres:
    """Postgres runners
    """
    db_url = f"""
postgresql+psycopg2
://{config.sync.sql.user}
:{config.sync.sql.passwd}
@{config.sync.sql.host}
:{config.sync.sql.port}
/{config.sync.sql.dbname}
        """.replace('\r', '').replace('\n', '').strip()

    @pytest.mark.parametrize('params', [['jobsync', 'postgres', 'postgres', 5432]])
    def test_simulate_postgres(self, psql_docker):
        print('Running simulation with Postgres with multiple nodes')
        run_more_than_one_node(self.db_url, 0.3)

    @pytest.mark.parametrize('params', [['jobsync', 'postgres', 'postgres', 5432]])
    def test_simulate_postgres(self, psql_docker):
        print('Running simulation with Postgres one node no sync')
        run_only_one_node(self.db_url, skip_sync=False)

    @pytest.mark.parametrize('params', [['jobsync', 'postgres', 'postgres', 5432]])
    def test_simulate_postgres(self, psql_docker):
        print('Running simulation with Postgres one node sync')
        run_only_one_node(self.db_url, skip_sync=True)


def test_simulate_sqlite():
    db_url = 'sqlite:///database.db'
    try:
        print('Running simulation with Sqlite multilple nodes')
        run_more_than_one_node(db_url, 0.4)
    finally:
        Path('database.db').unlink(missing_ok=True)
    try:
        print('Running simulation with Sqlite one node no sync')
        run_only_one_node(db_url, skip_sync=False)
    finally:
        Path('database.db').unlink(missing_ok=True)
    try:
        print('Running simulation with Sqlite one node sync')
        run_only_one_node(db_url, skip_sync=True)
    finally:
        Path('database.db').unlink(missing_ok=True)


if __name__ == '__main__':
    pytest.main(args=['-sx', __file__])
