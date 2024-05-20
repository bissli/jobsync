import logging

import database as db

from jobsync import config

logger = logging.getLogger(__name__)

Node = f'{config.sync.sql.appname}node'
Check = f'{config.sync.sql.appname}checkpoint'
Audit = f'{config.sync.sql.appname}audit'
Inst = f'{config.sync.sql.appname}inst'
Claim = f'{config.sync.sql.appname}claim'


def init_database(cn, is_test=False):
    """Init datbase and return engine
    """
    logger.debug(f'Initializing tables {Node},{Check},{Audit},{Claim}')

    db.execute(cn, f"""
CREATE TABLE IF NOT EXISTS {Node} (
    name varchar not null,
    created_on timestamp without time zone not null,
    primary key (name, created_on)
);
    """)

    db.execute(cn, f"""
CREATE TABLE IF NOT EXISTS {Check} (
    node varchar not null,
    created_on timestamp without time zone not null,
    primary key (node, created_on)
);
    """)

    db.execute(cn, f"""
CREATE TABLE IF NOT EXISTS {Audit} (
    created_on timestamp without time zone not null,
    node varchar not null,
    item varchar not null,
    date date not null
);
    """)

    logger.debug(f'Initializing table {Claim}')

    db.execute(cn, f"""
CREATE TABLE IF NOT EXISTS {Claim} (
    node varchar not null,
    item varchar not null,
    created_on timestamp without time zone not null,
    primary key (node, item)
);
    """)

    if not is_test:
        return

    logger.debug(f'Initializing table {Inst}')

    db.execute(cn, f"""
CREATE TABLE IF NOT EXISTS {Inst} (
    item varchar not null,
    done boolean not null
);
    """)
