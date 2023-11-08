import logging
import multiprocessing
import os

import pytest
from asserts import assert_almost_equal, assert_equal
from conftest import conn
from syncman import config, db, schema
from syncman.delay import NonBlockingDelay, delay
from syncman.sync import SyncTaskManager

logger = logging.getLogger(__name__)

current_path = os.path.dirname(os.path.realpath(__file__))

Inst = f'{config.sql.appname}inst'


def test_tables_count(psql_docker):
    """Create basic tables"""
    with conn('postgres') as cn:
        tables = db.select(cn, "SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name")
        assert_equal(len(tables), 4)


def test_insert_instruments(psql_docker):
    """Create instruments"""
    with conn('postgres') as cn:
        for i in range(200):
            db.execute(cn, f'insert into {Inst} (item, done) values (?, ?)', i, False)
        count = db.select_scalar(cn, f'select count(1) as count from {Inst}')
        assert_equal(count, 200)


def test_multiprocess(psql_docker):

    nodes = 3
    items = 30

    with conn('postgres') as cn:
        for i in range(1, items + 1):
            db.execute(cn, f'insert into {Inst} (item, done) values (%s, %s)', i, False)

        def worker(num):
            cn_loc = db.connect('postgres')
            delay = NonBlockingDelay()
            while 1:
                df = db.select(cn_loc, f'select item, done from {Inst} where done is false')
                if df.empty:
                    break
                df = df.sample(n=1)
                item = int(df.item.iloc[0])
                sql = f'insert into {schema.Sync} (node, item) values (%s, %s) on conflict do nothing'
                i = db.execute(cn_loc, sql, num, item)
                if not i:
                    continue
                sql = f'update {Inst} set done=true where item = %s'
                db.execute(cn_loc, sql, item)
                delay.delay(0.1)
                while not delay.timeout():
                    continue

        threads = []
        for i in range(1, nodes + 1):
            threads.append(multiprocessing.Process(target=worker, args=(i,)))
        for task in threads:
            task.start()
        for task in threads:
            task.join()

        df = db.select(cn, f'select * from {schema.Sync}')
        df = df.groupby('node').count().reset_index()

        expect, delta = int(items / nodes), int((items / nodes) * 0.1)
        assert_almost_equal(int(df[df['node'] == '1'].iloc[0]['item']), expect, delta=delta)
        assert_almost_equal(int(df[df['node'] == '2'].iloc[0]['item']), expect, delta=delta)
        assert_almost_equal(int(df[df['node'] == '3'].iloc[0]['item']), expect, delta=delta)


def test_run_and_sync(psql_docker):

    def action(name, items):
        with SyncTaskManager(
            name=name,
            wait_on_enter=5,
            sync_nodes=True,
            cn=db.connect('postgres'),
            ) as manager:
            while 1:
                with manager.synchronize(wait=6):
                    assert_equal(manager.nodecount, 3)
                    df = db.select(manager.cn, f'select item from {Inst} where done is false')
                    if df.empty:
                        break
                    x = list(df.sample(n=min(5, len(df.index)))['item'])
                    logger.info(f'Node {manager.name} found {len(df.index)} items, working {len(x)}')
                    manager.add_sync(x)
                    db.execute(manager.cn, f"update {Inst} set done=true where item in ({','.join(['%s'] * len(x))})", *x)
                    delay(0.1)

    def run(items):
        tasks = []
        for i in range(1, 4):
            tasks.append(multiprocessing.Process(target=action, args=('host' + str(i), items)))
        for task in tasks:
            task.start()
        for task in tasks:
            task.join()

    with conn('postgres') as cn:
        items = 91
        for i in range(items):
            db.execute(cn, f'insert into {Inst} (item, done) values (%s, %s)', i, False)
        run(items)
        # nodes
        host1 = db.select_scalar(cn, f'select count(name) from {schema.Node} where name = %s', 'host1')
        host2 = db.select_scalar(cn, f'select count(name) from {schema.Node} where name = %s', 'host2')
        host3 = db.select_scalar(cn, f'select count(name) from {schema.Node} where name = %s', 'host3')
        print('Each node should have one entry')
        assert_equal(host1, 1)
        assert_equal(host1, host2)
        assert_equal(host2, host3)
        # sync items
        host1 = db.select_scalar(cn, f'select count(node) from {schema.Sync} where node = %s', 'host1')
        host2 = db.select_scalar(cn, f'select count(node) from {schema.Sync} where node = %s', 'host2')
        host3 = db.select_scalar(cn, f'select count(node) from {schema.Sync} where node = %s', 'host3')
        print('No items should remain after run')
        assert_equal(host1, 0)
        assert_equal(host1, host2)
        assert_equal(host2, host3)
        # audit
        host1 = db.select_scalar(cn, f'select count(node) from {schema.Audit} where node = %s', 'host1')
        host2 = db.select_scalar(cn, f'select count(node) from {schema.Audit} where node = %s', 'host2')
        host3 = db.select_scalar(cn, f'select count(node) from {schema.Audit} where node = %s', 'host3')
        print(f'Each node should process roughly the same number of tasks: {host1}, {host2}, {host3}')
        expect, delta = int(items / 3), int((items / 3) * 0.15)
        assert_almost_equal(host1, expect, delta=delta)
        assert_almost_equal(host2, expect, delta=delta)
        assert_almost_equal(host3, expect, delta=delta)


if __name__ == '__main__':
    pytest.main(args=['-sx', __file__])
