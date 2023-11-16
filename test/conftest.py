import logging
import os
import time
from contextlib import contextmanager

import dataset
import docker
import psycopg2
import pytest
import wrapt
from syncman import config, schema

logger = logging.getLogger(__name__)

current_path = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture
def psql_docker(params):
    client = docker.from_env()
    container = client.containers.run(
        image='postgres:12',
        auto_remove=True,
        environment={
            'POSTGRES_DB': params[0],
            'POSTGRES_USER': params[1],
            'POSTGRES_PASSWORD': params[2],
            'TZ': 'US/Eastern',
            'PGTZ': 'US/Eastern',
        },
        name='test_postgres',
        ports={'5432/tcp': ('127.0.0.1', params[3])},
        detach=True,
        remove=True,
    )
    time.sleep(5)
    yield
    container.stop()


Inst = f'{config.sql.appname}inst'


def drop_tables(db):
    for table in [schema.Node, schema.Check, schema.Audit, Inst]:
        db[table].drop()


def create_inst_table(db):
    t = db[Inst]
    t.create_column('item', db.types.integer, nullable=False)
    t.create_column('done', db.types.boolean, nullable=False)


def terminate_postgres_connections(db_url):
    sql = """
select
    pg_terminate_backend(pg_stat_activity.pid)
from
    pg_stat_activity
where
    pg_stat_activity.datname = current_database()
    and pid <> pg_backend_pid()
    """
    db = dataset.connect(db_url)
    db.query(sql)


@wrapt.patch_function_wrapper(psycopg2, 'connect')
def patch_connect(wrapped, instance, args, kwargs):
    kwargs['database'] = 'syncman'
    kwargs['host'] = 'localhost'
    kwargs['user'] = 'postgres'
    kwargs['port'] = 5432
    kwargs['password'] = 'postgres'
    return wrapped(*args, **kwargs)


@contextmanager
def conn(db_url):
    if 'postgres' in db_url:
        terminate_postgres_connections(db_url)
    db = dataset.connect(db_url)
    schema.create_tables(db)
    create_inst_table(db)
    try:
        yield db
    finally:
        drop_tables(db)
        db.close()
