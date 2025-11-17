import logging
import os
import time

import config
import database as db
import docker
import pytest

from jobsync import schema
import pathlib

logger = logging.getLogger(__name__)

current_path = pathlib.Path(os.path.realpath(__file__)).parent


@pytest.fixture(scope='module')
def psql_docker():
    client = docker.from_env()
    try:
        existing = client.containers.get('test_postgres')
        existing.stop()
        existing.remove()
    except docker.errors.NotFound:
        pass
    container = client.containers.run(
        image='postgres:17',
        auto_remove=True,
        environment={
            'POSTGRES_DB': 'jobsync',
            'POSTGRES_USER': 'postgres',
            'POSTGRES_PASSWORD': 'postgres',
            'TZ': 'America/New_York',
            'PGTZ': 'America/New_York'},
        name='test_postgres',
        ports={'5432/tcp': ('127.0.0.1', 5432)},
        detach=True,
        remove=True,
    )
    time.sleep(5)
    yield
    container.stop()


def drop_tables(cn, config):
    tables = schema.get_table_names(config)
    for table in [tables['Node'], tables['Check'], tables['Audit'], tables['Inst'], tables['Claim']]:
        db.execute(cn, f'drop table {table}')


def create_extensions(cn):
    sql = 'CREATE EXTENSION IF NOT EXISTS hstore'
    db.execute(cn, sql)


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


@pytest.fixture
def postgres():
    cn = db.connect('postgres', config)
    create_extensions(cn)
    terminate_postgres_connections(cn)
    schema.init_database(cn, config, is_test=True)
    try:
        yield cn
    finally:
        terminate_postgres_connections(cn)
        drop_tables(cn, config)
        cn.close()


@pytest.fixture
def sqlite():
    cn = db.connect('sqlite', config)
    schema.init_database(cn, config, is_test=True)
    db_path = config.sqlite.database
    try:
        yield cn
    finally:
        drop_tables(cn, config)
        cn.close()
        if pathlib.Path(db_path).exists():
            pathlib.Path(db_path).unlink()
            logger.debug(f'Removed SQLite database file: {db_path}')
