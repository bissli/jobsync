import logging
from types import ModuleType

import database as db

from jobsync import config as default_config

logger = logging.getLogger(__name__)


def get_table_names(config: ModuleType = None) -> dict[str, str]:
    """Get table names based on config.

    Parameters
        config: Configuration module to use. If None, uses default config.

    Returns
        Dictionary containing table names
    """
    _config = config or default_config

    if hasattr(_config, 'sync') and hasattr(_config.sync, 'sql') and hasattr(_config.sync.sql, 'appname'):
        appname = _config.sync.sql.appname
    else:
        appname = 'sync_'

    return {
        'Node': f'{appname}node',
        'Check': f'{appname}checkpoint',
        'Audit': f'{appname}audit',
        'Inst': f'{appname}inst',
        'Claim': f'{appname}claim'
    }


def init_database(cn, config: ModuleType = None, is_test: bool = False):
    """Init database and return engine

    Parameters
        cn: Database connection
        config: Configuration module to use. If None, uses default config.
        is_test: Whether this is a test environment
    """
    tables = get_table_names(config)
    Node = tables['Node']
    Check = tables['Check']
    Audit = tables['Audit']
    Claim = tables['Claim']
    Inst = tables['Inst']

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
