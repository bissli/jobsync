import logging
import multiprocessing
import random
from pathlib import Path

import pytest
from asserts import assert_almost_equal, assert_equal
from conftest import conn
from syncman import config, schema
from syncman.delay import delay
from syncman.sync import Job

logger = logging.getLogger(__name__)

Inst = f'{config.sql.appname}inst'


def _simulation(db_url, threshold):
    """Full simulation
    """
    def action(node_name, items):
        total = 0
        with Job(node_name, wait_on_enter=5, db_url=db_url, create_tables=False) as job:
            # we want all nodes to be online
            assert_equal(len(job.poll_ready()), 3)
            while 1:
                # gather and process tasks
                inst = list(job.db[Inst].find(done=False))  # iterdict
                if not inst:
                    break
                x_ref = [x['item'] for x in inst]
                random.shuffle(x_ref)
                x = x_ref[:5]
                total += len(x)
                logger.info(f'Node {job.node_name} found {len(inst)} items, working {len(x)}')
                [job.add_step(i) for i in x]
                # write that task if completed
                job.db[Inst].update({'done': True, 'item': x}, ['item'])
                # check-in and wait until other nodes are finished
                job.tell_done()
                delay(0.3)
                for _ in range(10):
                    if job.all_done():
                        break
                    delay(3)

    def run(items):
        steps = []
        for i in range(1, 4):
            steps.append(
                multiprocessing.Process(target=action, args=('host' + str(i), items)))
        for step in steps:
            step.start()
        for step in steps:
            step.join()

    with conn(db_url) as db:
        items = 91
        nodes = 3
        db[Inst].insert_many([{'item': i, 'done': False} for i in range(items)])
        run(items)
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
    @pytest.mark.parametrize('params', [['syncman', 'postgres', 'postgres', 5432]])
    def test_simulate_postgres(self, psql_docker):
        print('Running simulation with Postgres')
        db_url = f'postgresql+psycopg2://{config.sql.user}:{config.sql.passwd}@{config.sql.host}:{config.sql.port}/{config.sql.dbname}'
        _simulation(db_url, 0.3)


def test_simulate_sqlite():
    print('Running simulation with Sqlite')
    db_url = 'sqlite:///database.db'
    try:
        _simulation(db_url, 0.4)
    finally:
        Path('database.db').unlink(missing_ok=True)


if __name__ == '__main__':
    pytest.main(args=['-sx', __file__])
