import asyncio
import glob
import logging
import os

import pytest
import sqlalchemy as sa
from asserts import assert_equal
from syncman.model import create
from syncman.sync import SyncTaskManager

logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
logger = logging.getLogger(__name__)

current_path = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture
def cleanup():
    yield
    [os.remove(x) for x in glob.glob(os.path.join(current_path, '*.db'))]


async def action(db_connection, name, pre_wait, pool):
    with SyncTaskManager(
        name=name,
        pre_wait=pre_wait,
        db_appname='syncman_',
        db_connection=db_connection,
    ) as manager, manager.register_task(wait=pre_wait):
        while 1:
            try:
                manager.add_sync(pool.pop())
                await asyncio.sleep(2)
            except:
                break


async def run(db_connection, pool, loop):
    t1 = loop.create_task(action(db_connection, 'host1', 5, pool))
    t2 = loop.create_task(action(db_connection, 'host2', 5, pool))
    t3 = loop.create_task(action(db_connection, 'host3', 5, pool))
    await asyncio.wait([t1, t2, t3])


def validate(db_connection):
    _ = create(db_connection=db_connection, db_appname='syncman_')
    engine, Node, Sync, Audit = _.values()
    with engine.connect() as conn:
        # names
        host1 = conn.execute(sa.select(sa.func.count(Node.c.name)).where(Node.c.name == 'host1')).scalar()
        host2 = conn.execute(sa.select(sa.func.count(Node.c.name)).where(Node.c.name == 'host2')).scalar()
        host3 = conn.execute(sa.select(sa.func.count(Node.c.name)).where(Node.c.name == 'host3')).scalar()
        print('Each node should have one entry')
        assert_equal(host1, 1)
        assert_equal(host1, host2)
        assert_equal(host2, host3)
        # sync items
        host1 = conn.execute(sa.select(sa.func.count(Sync.c.node)).where(Sync.c.node == 'host1')).scalar()
        host2 = conn.execute(sa.select(sa.func.count(Sync.c.node)).where(Sync.c.node == 'host2')).scalar()
        host3 = conn.execute(sa.select(sa.func.count(Sync.c.node)).where(Sync.c.node == 'host3')).scalar()
        print('No items should remain after run')
        assert_equal(host1, 0)
        assert_equal(host1, host2)
        assert_equal(host2, host3)
        # audit
        host1 = conn.execute(sa.select(sa.func.count(Audit.c.node)).where(Audit.c.node == 'host1')).scalar()
        host2 = conn.execute(sa.select(sa.func.count(Audit.c.node)).where(Audit.c.node == 'host2')).scalar()
        host3 = conn.execute(sa.select(sa.func.count(Audit.c.node)).where(Audit.c.node == 'host3')).scalar()
        print('Each node should process 5 tasks')
        assert_equal(host1, 5)
        assert_equal(host1, host2)
        assert_equal(host2, host3)


def execute(db_connection):
    pool = set(range(0, 15))
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(db_connection, pool, loop))
    validate(db_connection)


def test_run_and_sync(cleanup):
    execute(db_connection='sqlite:///syncman.db')


if __name__ == '__main__':
    pytest.main(args=['-sx', __file__])
