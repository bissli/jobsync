import asyncio
import logging

import pytest
import sqlalchemy as sa
from syncman.db import create
from syncman.sync import SyncTaskManager

logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
logging.getLogger('syncman').setLevel(logging.WARNING)
logger = logging.getLogger('syncman')


async def action(config, name, pre_wait, pool):
    with SyncTaskManager(
        config=config,
        name=name,
        pre_wait=pre_wait,
    ) as manager, manager.register_task(wait=pre_wait):
        while 1:
            try:
                manager.add_claim(pool.pop())
                await asyncio.sleep(2)
            except:
                break


async def run(config, pool, loop):
    t1 = loop.create_task(action(config, 'host1', 5, pool))
    t2 = loop.create_task(action(config, 'host2', 5, pool))
    t3 = loop.create_task(action(config, 'host3', 5, pool))
    await asyncio.wait([t1, t2, t3])


def validate(config):
    c = create(config=config)
    engine = c['engine']
    _, Agent = c['agent']
    _, Claim = c['claim']
    _, Audit = c['audit']
    with engine.connect() as conn:
        # agents
        host1 = conn.execute(sa.select(Agent).where(Agent.c.agent == 'host1')).fetchall()
        host2 = conn.execute(sa.select(Agent).where(Agent.c.agent == 'host2')).fetchall()
        host3 = conn.execute(sa.select(Agent).where(Agent.c.agent == 'host3')).fetchall()
        assert len(host1) == len(host2) == len(host3) == 1, 'Each agent should have one entry'
        # claims
        host1 = conn.execute(sa.select(Claim).where(Claim.c.agent == 'host1')).fetchall()
        host2 = conn.execute(sa.select(Claim).where(Claim.c.agent == 'host2')).fetchall()
        host3 = conn.execute(sa.select(Claim).where(Claim.c.agent == 'host3')).fetchall()
        assert len(host1) == len(host2) == len(host3) == 0, 'No claims should remain after run'
        # audit
        host1 = conn.execute(sa.select(Audit).where(Audit.c.agent == 'host1')).fetchall()
        host2 = conn.execute(sa.select(Audit).where(Audit.c.agent == 'host2')).fetchall()
        host3 = conn.execute(sa.select(Audit).where(Audit.c.agent == 'host3')).fetchall()
        assert len(host1) == len(host2) == len(host3) == 5, 'Each agent should process 5 tasks'


def execute(config):
    pool = set(range(0, 15))
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(config, pool, loop))
    validate(config)


def test_run_and_sync_from_file():
    execute(config='config-sa.yml')


def test_run_and_sync_from_dict():
    config = {
        'database': {
            'type': 'sqlite',
            'name': 'test_d',
            'host': None,
            'username': None,
            'password': None,
            'timezone': 'US/Eastern',
        },
        'tables': {
            'agent': {
                'name': 'sync_agent',
                'agent': 'agent',
                'created': 'created_on',
            },
            'claim': {
                'name': 'sync_claim',
                'agent': 'agent',
                'claim': 'claim',
            },
            'audit': {
                'name': 'sync_audit',
                'created': 'created_on',
                'agent': 'agent',
                'claim': 'claim',
                'date': 'date',
            },
        },
    }
    execute(config=config)


if __name__ == '__main__':
    pytest.main(args=['-sx', __file__])
