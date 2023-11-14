import logging
import multiprocessing
import random
from pathlib import Path

import pytest
import syncman
from asserts import assert_almost_equal, assert_equal
from conftest import conn
from syncman import db
from syncman.delay import delay
from syncman.sync import SyncManager

logger = logging.getLogger(__name__)

Inst = f'{syncman.config.sql.appname}inst'


def _simulation(profile, threshold):
    """Full simulation
    """
    def action(name, items):
        total = 0
        with SyncManager(name=name, wait_on_enter=5, cn=db.connect(profile), create_tables=False) as sync:
            # we want all nodes to be online
            assert_equal(len(sync.get_online()), 3)
            while 1:
                # gather and process tasks
                df = db.select(sync.cn, f'select item from {Inst} where done is false')
                if df.empty:
                    break
                x_ref = list(df['item'])
                random.shuffle(x_ref)
                x = x_ref[:5]
                total += len(x)
                logger.info(f'Node {sync.name} found {len(df.index)} items, working {len(x)}')
                [sync.add_local_task(i) for i in x]
                # write that task if completed
                db.execute(sync.cn, f"update {Inst} set done=true where item in ({','.join(['%s'] * len(x))})", *x)
                # check-in and wait until other nodes are finished
                sync.publish_checkpoint()
                delay(0.3)
                for _ in range(10):
                    if sync.all_nodes_published_checkpoints():
                        break
                    delay(3)

    def run(items):
        tasks = []
        for i in range(1, 4):
            tasks.append(multiprocessing.Process(target=action, args=('host' + str(i), items)))
        for task in tasks:
            task.start()
        for task in tasks:
            task.join()

    with conn(profile) as cn:
        items = 91
        nodes = 3
        for i in range(items):
            db.execute(cn, f'insert into {Inst} (item, done) values (%s, %s)', i, False)
        run(items)
        # nodes
        host1 = db.select_scalar(cn, f'select count(name) from {syncman.schema.Node} where name = %s', 'host1')
        host2 = db.select_scalar(cn, f'select count(name) from {syncman.schema.Node} where name = %s', 'host2')
        host3 = db.select_scalar(cn, f'select count(name) from {syncman.schema.Node} where name = %s', 'host3')
        print('No nodes should remain online (cleanup)')
        assert_equal(host1, 0)
        assert_equal(host1, host2)
        assert_equal(host2, host3)
        # checkpoints
        host1 = db.select_scalar(cn, f'select count(node) from {syncman.schema.Check} where node = %s', 'host1')
        host2 = db.select_scalar(cn, f'select count(node) from {syncman.schema.Check} where node = %s', 'host2')
        host3 = db.select_scalar(cn, f'select count(node) from {syncman.schema.Check} where node = %s', 'host3')
        print('No checkpoints should remain after run (cleanup)')
        assert_equal(host1, 0)
        assert_equal(host1, host2)
        assert_equal(host2, host3)
        # audit
        host1 = db.select_scalar(cn, f'select count(node) from {syncman.schema.Audit} where node = %s', 'host1')
        host2 = db.select_scalar(cn, f'select count(node) from {syncman.schema.Audit} where node = %s', 'host2')
        host3 = db.select_scalar(cn, f'select count(node) from {syncman.schema.Audit} where node = %s', 'host3')
        print(f'Each node should process roughly the same number of tasks: {host1}, {host2}, {host3}')
        expect, delta = int(items / nodes), int((items / nodes) * threshold)  # multiprocessing somewhat unpredictable
        assert_almost_equal(host1, expect, delta=delta)
        assert_almost_equal(host2, expect, delta=delta)
        assert_almost_equal(host3, expect, delta=delta)


class TestSimulatePostgres:
    @pytest.mark.parametrize('params', [['syncman', 'postgres', 'postgres', 5432]])
    def test_simulate_postgres(self, psql_docker):
        print('Running simulation with Postgres')
        _simulation('postgres', 0.3)


def test_simulate_sqlite():
    print('Running simulation with Sqlite')
    _simulation('sqlite', 0.4)
    Path('database.db').unlink(missing_ok=True)


if __name__ == '__main__':
    pytest.main(args=['-sx', __file__])
