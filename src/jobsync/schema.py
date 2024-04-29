import logging

from jobsync import config

import db

logger = logging.getLogger(__name__)

Node = f'{config.sync.sql.appname}node'
Check = f'{config.sync.sql.appname}checkpoint'
Audit = f'{config.sync.sql.appname}audit'
Inst = f'{config.sync.sql.appname}inst'


def init_database(cn):
    """Init datbase and return engine
    """
    logger.debug('Checking for primary sync tables')

    db.execute(cn, f"""
CREATE TABLE IF NOT EXISTS {Node} (
    name varchar not null,
    created timestamp without time zone not null,
    primary key (name, created)
);
    """)

    db.execute(cn, f"""
CREATE TABLE IF NOT EXISTS {Check} (
    node varchar not null,
    created timestamp without time zone not null,
    primary key (node, created)
);
    """)

    db.execute(cn, f"""
CREATE TABLE IF NOT EXISTS {Audit} (
    created timestamp without time zone not null,
    node varchar not null,
    item varchar not null,
    date date not null
);
    """)

    db.execute(cn, f"""
CREATE TABLE IF NOT EXISTS {Inst} (
    item integer not null,
    done boolean not null
);
    """)
