import logging

from jobsync import config

logger = logging.getLogger(__name__)

Node = f'{config.sync.sql.appname}node'
Check = f'{config.sync.sql.appname}checkpoint'
Audit = f'{config.sync.sql.appname}audit'


def create_tables(db):
    logger.debug('Creating primary sync tables')
    t = db[Node]
    t.create_column('name', db.types.string, nullable=False)
    t.create_column('created', db.types.datetime, nullable=False)
    t.create_index(['name', 'created'], unique=True)
    t = db[Check]
    t.create_column('node', db.types.string, nullable=False)
    t.create_column('created', db.types.datetime, nullable=False)
    t.create_index(['node', 'created'], unique=False)
    t = db[Audit]
    t.create_column('created', db.types.datetime, nullable=False)
    t.create_column('node', db.types.string, nullable=False)
    t.create_column('item', db.types.string, nullable=False)
    t.create_column('date', db.types.date, nullable=False)
