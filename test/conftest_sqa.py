import logging
import os
import time

import docker
import pytest
from pytest_postgresql.janitor import DatabaseJanitor
from sqlalchemy.engine import URL
from syncman import config, schema
from syncman.model import ManagedSession

logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

current_path = os.path.dirname(os.path.realpath(__file__))

pg_url = URL.create(
    'postgresql+psycopg',
    username=config.sql.user,
    password=config.sql.passwd,
    host=config.sql.host,
    port=config.sql.port,
    database=config.sql.database,
    )


@pytest.fixture(scope='session')
def psql_docker():
    client = docker.from_env()
    container = client.containers.run(
        image='postgres:12',
        auto_remove=True,
        environment={'POSTGRES_PASSWORD': config.sql.passwd},
        name='test_postgres',
        ports={'5432/tcp': ('127.0.0.1', config.sql.port)},
        detach=True,
        remove=True,
        )
    time.sleep(5)
    yield
    container.stop()


@pytest.fixture(scope='function')
def db_session(psql_docker):
    """Returns SqlAlchemy session object"""
    with DatabaseJanitor(
        version=12,
        host=config.sql.host,
        user=config.sql.user,
        password=config.sql.passwd,
        port=config.sql.port,
        dbname=config.sql.database,
        ), ManagedSession(pg_url) as session:
        yield session
    print('CLOSING DATABASE CONNECTION')


@pytest.fixture(scope='function')
def stage_tables(db_session):
    engine = db_session.get_bind()
    schema.Test.metadata.create_all(engine, checkfirst=True)
    engine.dispose()
