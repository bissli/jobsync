import logging
import os
import time
from contextlib import contextmanager

import docker
import pytest
import sqlalchemy as sa
from sqlalchemy.engine import URL
from sqlalchemy.orm import Session, declarative_base
from syncman import config, schema

logging.basicConfig()
logger = logging.getLogger(__name__)

current_path = os.path.dirname(os.path.realpath(__file__))

Test = declarative_base()


class Inst(Test):
    __tablename__ = f'{config.sql.appname}inst'

    Item = sa.Column('item', sa.INTEGER, primary_key=True)
    Done = sa.Column('done', sa.BOOLEAN, default=False, nullable=False)

    def __repr__(self):
        return "<Inst(Id='%s', Done='%s')>" % (self.Id, self.Done)


pg_url = URL.create(
    'postgresql+psycopg',
    username=config.sql.user,
    password=config.sql.passwd,
    host=config.sql.host,
    port=config.sql.port,
    database=config.sql.database,
    )

sql_url = 'sqlite:///database.db'


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


def create_tables(session):
    engine = session.bind
    Test.metadata.drop_all(engine)
    Test.metadata.create_all(engine)


def terminate_postgres_connections(url):
    sql = sa.text(
        """
select
    pg_terminate_backend(pg_stat_activity.pid)
from
    pg_stat_activity
where
    pg_stat_activity.datname = current_database()
    and pid <> pg_backend_pid()
    """
        )
    engine = sa.create_engine(url)
    with Session(bind=engine) as cn:
        cn.execute(sql)


@contextmanager
def non_build_session(url):
    engine = sa.create_engine(url, execution_options={'isolation_level': 'AUTOCOMMIT'})
    with Session(bind=engine, autoflush=True, expire_on_commit=False) as session:
        try:
            yield session
        except:
            raise
        finally:
            session.close()


@contextmanager
def db_session(url):
    """Creates a context with an open SQLAlchemy session."""
    if 'postgres' in url:
        terminate_postgres_connections(url)
    engine = sa.create_engine(url, execution_options={'isolation_level': 'AUTOCOMMIT'})
    with Session(bind=engine, autoflush=True, expire_on_commit=False) as session:
        schema.create_tables(session, drop=True)
        create_tables(session)
        try:
            yield session
            #  session.commit()
            #  session.flush()
        except:
            #  session.rollback()
            raise
        finally:
            session.close()
