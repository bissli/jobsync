import logging
import os
import time

import config
import docker
import pytest
from jobsync import schema

import db

logger = logging.getLogger(__name__)

current_path = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture(scope='module')
def psql_docker():
    client = docker.from_env()
    container = client.containers.run(
        image='postgres:12',
        auto_remove=True,
        environment={
            'POSTGRES_DB': 'jobsync',
            'POSTGRES_USER': 'postgres',
            'POSTGRES_PASSWORD': 'postgres',
            'TZ': 'US/Eastern',
            'PGTZ': 'US/Eastern'},
        name='test_postgres',
        ports={'5432/tcp': ('127.0.0.1', 5432)},
        detach=True,
        remove=True,
    )
    time.sleep(5)
    yield
    container.stop()


def drop_tables(cn):
    for table in [schema.Node, schema.Check, schema.Audit, schema.Inst]:
        db.execute(cn, f'drop table {table}')


def terminate_postgres_connections(cn):
    sql = """
select
    pg_terminate_backend(pg_stat_activity.pid)
from
    pg_stat_activity
where
    pg_stat_activity.datname = current_database()
    and pid <> pg_backend_pid()
    """
    db.select(cn, sql)


@pytest.fixture()
def postgres():
    cn = db.connect('postgres', config)
    terminate_postgres_connections(cn)
    schema.init_database(cn)
    try:
        yield cn
    finally:
        terminate_postgres_connections(cn)
        drop_tables(cn)
        cn.close()


@pytest.fixture()
def sqlite():
    cn = db.connect('sqlite', config)
    schema.init_database(cn)
    try:
        yield cn
    finally:
        drop_tables(cn)
        cn.close()
