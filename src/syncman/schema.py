import logging

from syncman import config, db

logger = logging.getLogger(__name__)

Node = f'{config.sql.appname}node'
Check = f'{config.sql.appname}checkpoint'
Audit = f'{config.sql.appname}audit'


def create_tables(cn):
    logger.debug('Creating primary sync tables')
    sqls = [
        f"""
create table if not exists {Node} (
    name varchar(255),
    created timestamp without time zone,
    primary key(name, created)
)
    """, f"""
create table if not exists {Check} (
    node varchar(255) not null,
    created timestamp without time zone,
    primary key (node, created)
)
    """, f"""
create table if not exists {Audit} (
    created timestamp without time zone not null,
    node varchar(255) not null,
    item varchar(255) not null,
    date date not null
)
    """
    ]
    for sql in sqls:
        db.execute(cn, sql)
