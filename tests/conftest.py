import logging
import os
import pathlib
import time

import config
import docker
import pytest
from sqlalchemy import create_engine, text

from jobsync import schema

logger = logging.getLogger(__name__)

current_path = pathlib.Path(os.path.realpath(__file__)).parent


@pytest.fixture(scope='module')
def psql_docker():
    """Start PostgreSQL Docker container for testing.
    """
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


def drop_tables(engine, config):
    """Drop all test tables.
    """
    tables = schema.get_table_names(config)
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
            tables['Node']
        ]:
            conn.execute(text(f'DROP TABLE IF EXISTS {table}'))
        conn.commit()


def create_extensions(engine):
    """Create required PostgreSQL extensions.
    """
    with engine.connect() as conn:
        conn.execute(text('CREATE EXTENSION IF NOT EXISTS hstore'))
        conn.commit()


def terminate_postgres_connections(engine):
    """Terminate all other connections to the test database.
    """
    sql = """
    SELECT pg_terminate_backend(pg_stat_activity.pid)
    FROM pg_stat_activity
    WHERE pg_stat_activity.datname = current_database()
    AND pid <> pg_backend_pid()
    """
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()


@pytest.fixture
def postgres(psql_docker):
    """Provide SQLAlchemy engine for PostgreSQL tests.
    """
    connection_string = (
        f'postgresql+psycopg://{config.postgres.username}:{config.postgres.password}'
        f'@{config.postgres.hostname}:{config.postgres.port}/{config.postgres.database}'
    )
    engine = create_engine(connection_string, pool_pre_ping=True, pool_size=10, max_overflow=5)
    
    create_extensions(engine)
    terminate_postgres_connections(engine)
    schema.init_database(engine, config, is_test=True)
    
    try:
        yield engine
    finally:
        terminate_postgres_connections(engine)
        drop_tables(engine, config)
        engine.dispose()


@pytest.fixture
def sqlite():
    """Provide SQLAlchemy engine for SQLite tests.
    """
    connection_string = f'sqlite:///{config.sqlite.database}'
    engine = create_engine(connection_string, pool_pre_ping=True)
    
    schema.init_database(engine, config, is_test=True)
    db_path = config.sqlite.database
    
    try:
        yield engine
    finally:
        drop_tables(engine, config)
        engine.dispose()
        if pathlib.Path(db_path).exists():
            pathlib.Path(db_path).unlink()
            logger.debug(f'Removed SQLite database file: {db_path}')
