"""PostgreSQL 17 container and engine fixtures, in the America/New_York zone.
"""
import logging
import os
import pathlib
import socket
import time
from collections.abc import Iterator

import docker
import psycopg
import pytest
from sqlalchemy import Engine, create_engine, text

from jobsync import schema

logger = logging.getLogger(__name__)

current_path = pathlib.Path(os.path.realpath(__file__)).parent


def find_free_port() -> int:
    """A TCP port on 127.0.0.1 that no process holds at the time of the call.

    Returns
    -------
    int
        Port number the OS picked. Another process may bind it before the
        caller does.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


@pytest.fixture(scope='module')
def psql_docker() -> Iterator[int]:
    """Start a PostgreSQL Docker container on a free host port.

    Yields
    ------
    int
        Host port on 127.0.0.1 mapped to the container's 5432, once the
        server answers a query.

    Raises
    ------
    psycopg.OperationalError
        The server did not answer within 60 seconds. The container is
        stopped first.
    """
    port = find_free_port()
    client = docker.from_env()
    container = client.containers.run(
        image='postgres:17',
        auto_remove=True,
        environment={
            'POSTGRES_DB': 'jobsync',
            'POSTGRES_USER': 'postgres',
            'POSTGRES_PASSWORD': 'postgres',
            'TZ': 'America/New_York',
            'PGTZ': 'America/New_York',
            },
        name=f'test_postgres_{port}',
        ports={'5432/tcp': ('127.0.0.1', port)},
        detach=True,
        remove=True)
    conninfo = f'host=127.0.0.1 port={port} dbname=jobsync user=postgres password=postgres connect_timeout=2'
    try:
        deadline = time.time() + 60
        while True:
            try:
                with psycopg.connect(conninfo) as conn:
                    conn.execute('select 1')
                break
            except psycopg.OperationalError:
                if time.time() > deadline:
                    raise
                time.sleep(0.5)
        yield port
    finally:
        container.stop()


def terminate_postgres_connections(engine: Engine) -> None:
    """Terminate all other connections to the test database.
    """
    sql = """
select pg_terminate_backend(pg_stat_activity.pid)
from pg_stat_activity
where pg_stat_activity.datname = current_database()
and pid <> pg_backend_pid()
"""
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()


@pytest.fixture
def postgres(psql_docker: int) -> Iterator[Engine]:
    """Engine on the container's jobsync database, with the sync_ schema built.

    Yields
    ------
    Engine
        Teardown terminates every other connection and drops every sync_
        table, so each test starts from empty tables.
    """
    connection_string = f'postgresql+psycopg://postgres:postgres@127.0.0.1:{psql_docker}/jobsync'
    engine = create_engine(
        connection_string,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=5)

    with engine.connect() as conn:
        conn.execute(text('create extension if not exists hstore'))
        conn.commit()
    terminate_postgres_connections(engine)
    engine.dispose()

    engine = create_engine(
        connection_string,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=5)
    schema.ensure_database_ready(engine, 'sync_')

    tables = schema.get_table_names('sync_')
    sql = f"""
create table if not exists {tables['Inst']} (
    item varchar not null,
    done boolean not null
);
"""
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()

    try:
        yield engine
    finally:
        terminate_postgres_connections(engine)
        with engine.connect() as conn:
            for table in [
                tables['Rebalance'],
                tables['RebalanceLock'],
                tables['LeaderLock'],
                tables['Lock'],
                tables['Token'],
                tables['Claim'],
                tables['Inst'],
                tables['Audit'],
                tables['Check'],
                tables['Node'],
                ]:
                conn.execute(text(f'drop table if exists {table}'))
            conn.commit()
        engine.dispose()
