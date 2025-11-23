import logging

from sqlalchemy import Engine, text

logger = logging.getLogger(__name__)


def get_table_names(appname: str = 'sync_') -> dict[str, str]:
    """Get table names based on appname prefix.

    Args
        appname: Application name prefix for tables

    Returns
        Dictionary containing table names
    """
    return get_table_names_for_appname(appname)


def get_table_names_for_appname(appname: str) -> dict[str, str]:
    """Get table names for a specific appname prefix.
    """
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


def verify_tables_exist(engine: Engine, appname: str = 'sync_') -> dict[str, bool]:
    """Verify which required tables exist in the database.

    Args:
        engine: SQLAlchemy engine
        appname: Application name prefix for tables

    Returns
        Dictionary mapping table keys to existence status (True if exists, False otherwise)
    """
    tables = get_table_names(appname)
    status = {}

    table_keys = ['Node', 'Check', 'Audit', 'Claim', 'Token', 'Lock', 'LeaderLock', 'RebalanceLock', 'Rebalance']

    with engine.connect() as conn:
        for table_key in table_keys:
            table_name = tables[table_key]
            result = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = :table_name
                )
            """), {'table_name': table_name})
            status[table_key] = result.scalar()

    return status


def _create_core_tables(engine: Engine, tables: dict[str, str]) -> None:
    """Create core tables (Node, Check, Audit, Claim).
    """
    Node = tables['Node']
    Check = tables['Check']
    Audit = tables['Audit']
    Claim = tables['Claim']

    with engine.connect() as conn:
        conn.execute(text(f"""
CREATE TABLE IF NOT EXISTS {Node} (
    name varchar not null,
    created_on timestamp with time zone not null,
    last_heartbeat timestamp with time zone,
    primary key (name)
);
        """))

        conn.execute(text(f'CREATE INDEX IF NOT EXISTS idx_{Node}_heartbeat ON {Node}(last_heartbeat)'))

        conn.execute(text(f"""
CREATE TABLE IF NOT EXISTS {Check} (
    node varchar not null,
    created_on timestamp with time zone not null,
    primary key (node, created_on)
);
        """))

        conn.execute(text(f"""
CREATE TABLE IF NOT EXISTS {Audit} (
    created_on timestamp with time zone not null,
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
    created_on timestamp with time zone not null,
    primary key (node, item)
);
        """))

        conn.commit()

    logger.debug(f'Core tables verified: {Node}, {Check}, {Audit}, {Claim}')


def _create_coordination_tables(engine: Engine, tables: dict[str, str]) -> None:
    """Create coordination tables (Token, Lock, LeaderLock, RebalanceLock, Rebalance).
    """
    Token = tables['Token']
    Lock = tables['Lock']
    LeaderLock = tables['LeaderLock']
    RebalanceLock = tables['RebalanceLock']
    Rebalance = tables['Rebalance']

    with engine.connect() as conn:
        conn.execute(text(f"""
CREATE TABLE IF NOT EXISTS {Token} (
    token_id integer not null,
    node varchar not null,
    assigned_at timestamp with time zone not null,
    version integer not null default 1,
    primary key (token_id)
);
        """))

        conn.execute(text(f'CREATE INDEX IF NOT EXISTS idx_{Token}_node ON {Token}(node)'))
        conn.execute(text(f'CREATE INDEX IF NOT EXISTS idx_{Token}_assigned ON {Token}(assigned_at)'))
        conn.execute(text(f'CREATE INDEX IF NOT EXISTS idx_{Token}_version ON {Token}(version)'))

        conn.execute(text(f"""
CREATE TABLE IF NOT EXISTS {Lock} (
    task_id varchar not null,
    node_patterns jsonb not null,
    reason varchar,
    created_at timestamp with time zone not null,
    created_by varchar not null,
    expires_at timestamp with time zone,
    primary key (task_id)
);
        """))

        conn.execute(text(f'CREATE INDEX IF NOT EXISTS idx_{Lock}_created_by ON {Lock}(created_by)'))
        conn.execute(text(f'CREATE INDEX IF NOT EXISTS idx_{Lock}_expires ON {Lock}(expires_at) WHERE expires_at IS NOT NULL'))

        conn.execute(text(f"""
CREATE TABLE IF NOT EXISTS {LeaderLock} (
    singleton integer primary key default 1,
    node varchar not null,
    acquired_at timestamp with time zone not null,
    operation varchar not null,
    check (singleton = 1)
);
        """))

        conn.execute(text(f"""
CREATE TABLE IF NOT EXISTS {RebalanceLock} (
    singleton integer primary key default 1,
    in_progress boolean not null default false,
    started_at timestamp with time zone,
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
    triggered_at timestamp with time zone not null,
    trigger_reason varchar not null,
    leader_node varchar not null,
    nodes_before integer not null,
    nodes_after integer not null,
    tokens_moved integer not null,
    duration_ms integer
);
        """))

        conn.execute(text(f'CREATE INDEX IF NOT EXISTS idx_{Rebalance}_triggered ON {Rebalance}(triggered_at DESC)'))

        conn.commit()

    logger.debug(f'Coordination tables verified: {Token}, {Lock}, {LeaderLock}, {RebalanceLock}, {Rebalance}')


def ensure_database_ready(engine: Engine, appname: str = 'sync_') -> None:
    """Ensure database has all required tables with correct structure.

    This function checks which tables exist and creates any missing tables.
    Safe to call repeatedly - uses CREATE TABLE IF NOT EXISTS.

    Args:
        engine: SQLAlchemy engine
        appname: Application name prefix for tables
    """
    tables = get_table_names(appname)

    logger.debug('Verifying database structure')

    table_status = verify_tables_exist(engine, appname)

    missing_tables = [k for k in ['Node', 'Check', 'Audit', 'Claim', 'Token', 'Lock', 'LeaderLock', 'RebalanceLock', 'Rebalance']
                     if not table_status.get(k, False)]
    if missing_tables:
        logger.info(f'Creating missing tables: {missing_tables}')

    try:
        _create_core_tables(engine, tables)
        _create_coordination_tables(engine, tables)
        logger.info('Database tables ready')
    except Exception as e:
        logger.error(f'Failed to create tables: {e}')
        raise

    logger.info('Database structure verified and ready')
