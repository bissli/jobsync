import logging
from types import ModuleType

from sqlalchemy import Engine, text

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
        'Claim': f'{appname}claim',
        'Token': f'{appname}token',
        'Lock': f'{appname}lock',
        'LeaderLock': f'{appname}leader_lock',
        'RebalanceLock': f'{appname}rebalance_lock',
        'Rebalance': f'{appname}rebalance'
    }


def init_database(engine: Engine, config: ModuleType = None, is_test: bool = False):
    """Init database and return engine

    Parameters
        engine: SQLAlchemy engine
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

    try:
        with engine.connect() as conn:
            conn.execute(text(f"""
CREATE TABLE IF NOT EXISTS {Node} (
    name varchar not null,
    created_on timestamp without time zone not null,
    last_heartbeat timestamp without time zone,
    primary key (name)
);
            """))

            conn.execute(text(f'CREATE INDEX IF NOT EXISTS idx_{Node}_heartbeat ON {Node}(last_heartbeat)'))

            conn.execute(text(f"""
CREATE TABLE IF NOT EXISTS {Check} (
    node varchar not null,
    created_on timestamp without time zone not null,
    primary key (node, created_on)
);
            """))

            conn.execute(text(f"""
CREATE TABLE IF NOT EXISTS {Audit} (
    created_on timestamp without time zone not null,
    node varchar not null,
    item varchar not null,
    date date not null
);
            """))

            conn.execute(text(f'CREATE INDEX IF NOT EXISTS idx_{Audit}_date_item ON {Audit}(date, item)'))

            conn.execute(text(f"""
CREATE TABLE IF NOT EXISTS {Claim} (
    node varchar not null,
    item varchar not null,
    created_on timestamp without time zone not null,
    primary key (node, item)
);
            """))

            conn.commit()
        logger.info(f'Successfully created tables: {Node}, {Check}, {Audit}, {Claim}')
    except Exception as e:
        logger.error(f'Failed to create coordination tables: {e}')
        raise

    Token = tables['Token']
    Lock = tables['Lock']
    LeaderLock = tables['LeaderLock']
    RebalanceLock = tables['RebalanceLock']
    Rebalance = tables['Rebalance']

    logger.debug(f'Initializing coordination tables {Token}, {Lock}, {LeaderLock}, {RebalanceLock}, {Rebalance}')

    try:
        with engine.connect() as conn:
            conn.execute(text(f"""
CREATE TABLE IF NOT EXISTS {Token} (
    token_id integer not null,
    node varchar not null,
    assigned_at timestamp without time zone not null,
    version integer not null default 1,
    primary key (token_id)
);
            """))

            conn.execute(text(f'CREATE INDEX IF NOT EXISTS idx_{Token}_node ON {Token}(node)'))
            conn.execute(text(f'CREATE INDEX IF NOT EXISTS idx_{Token}_assigned ON {Token}(assigned_at)'))
            conn.execute(text(f'CREATE INDEX IF NOT EXISTS idx_{Token}_version ON {Token}(version)'))

            conn.execute(text(f"""
CREATE TABLE IF NOT EXISTS {Lock} (
    token_id integer not null,
    node_pattern varchar not null,
    reason varchar,
    created_at timestamp without time zone not null,
    created_by varchar not null,
    expires_at timestamp without time zone,
    primary key (token_id)
);
            """))

            conn.execute(text(f'CREATE INDEX IF NOT EXISTS idx_{Lock}_pattern ON {Lock}(node_pattern)'))
            conn.execute(text(f'CREATE INDEX IF NOT EXISTS idx_{Lock}_created_by ON {Lock}(created_by)'))
            conn.execute(text(f'CREATE INDEX IF NOT EXISTS idx_{Lock}_expires ON {Lock}(expires_at) WHERE expires_at IS NOT NULL'))

            conn.execute(text(f"""
CREATE TABLE IF NOT EXISTS {LeaderLock} (
    singleton integer primary key default 1,
    node varchar not null,
    acquired_at timestamp without time zone not null,
    operation varchar not null,
    check (singleton = 1)
);
            """))

            conn.execute(text(f"""
CREATE TABLE IF NOT EXISTS {RebalanceLock} (
    singleton integer primary key default 1,
    in_progress boolean not null default false,
    started_at timestamp without time zone,
    started_by varchar,
    check (singleton = 1)
);
            """))

            conn.execute(text(f"""
INSERT INTO {RebalanceLock} (singleton, in_progress)
VALUES (1, false)
ON CONFLICT (singleton) DO NOTHING
            """))

            conn.execute(text(f"""
CREATE TABLE IF NOT EXISTS {Rebalance} (
    id serial primary key,
    triggered_at timestamp without time zone not null,
    trigger_reason varchar not null,
    leader_node varchar not null,
    nodes_before integer not null,
    nodes_after integer not null,
    tokens_moved integer not null,
    duration_ms integer
);
            """))

            conn.execute(text(f'CREATE INDEX IF NOT EXISTS idx_{Rebalance}_triggered ON {Rebalance}(triggered_at DESC)'))

            if is_test:
                conn.execute(text(f"""
CREATE TABLE IF NOT EXISTS {Inst} (
    item varchar not null,
    done boolean not null
);
                """))

            conn.commit()
        logger.info(f'Successfully created coordination tables: {Token}, {Lock}, {LeaderLock}, {RebalanceLock}, {Rebalance}')
    except Exception as e:
        logger.error(f'Failed to create coordination tables: {e}')
        raise
