import logging
import os
import time
from contextlib import contextmanager

import docker
import pytest
from syncman import config, db, schema

logger = logging.getLogger(__name__)

current_path = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture(scope='session')
def psql_docker():
    client = docker.from_env()
    container = client.containers.run(
        image='postgres:12',
        auto_remove=True,
        environment={
            'POSTGRES_PASSWORD': config.sql.passwd,
            'POSTGRES_USER': config.sql.user,
            'POSTGRES_DB': config.sql.database,
            'TZ': 'US/Eastern',
            'PGTZ': 'US/Eastern',
        },
        name='test_postgres',
        ports={'5432/tcp': ('127.0.0.1', config.sql.port)},
        detach=True,
        remove=True,
    )
    time.sleep(5)
    yield
    container.stop()


Inst = f'{config.sql.appname}inst'


def drop_tables(cn):
    for table in [schema.Node, schema.Check, schema.Audit, Inst]:
        db.execute(cn, f'drop table if exists {table}')


def create_inst_table(cn):
    sql = f"""
create table if not exists {Inst} (
    item integer not null,
    done boolean default False not null,
    primary key (item, done)
)
    """
    db.execute(cn, sql)


def terminate_postgres_connections():
    sql = """
select
    pg_terminate_backend(pg_stat_activity.pid)
from
    pg_stat_activity
where
    pg_stat_activity.datname = current_database()
    and pid <> pg_backend_pid()
    """
    db.execute(db.connect('postgres'), sql)


@contextmanager
def conn(profile):
    if profile == 'postgres':
        terminate_postgres_connections()
    cn = db.connect(profile)
    schema.create_tables(cn)
    create_inst_table(cn)
    try:
        yield cn
    finally:
        drop_tables(cn)
        cn.close()
