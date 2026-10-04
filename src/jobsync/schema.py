import logging

from sqlalchemy import Engine, text

logger = logging.getLogger(__name__)


def get_table_names(appname: str = 'sync_') -> dict[str, str]:
    """Table name per table key, each prefixed with appname.

    Parameters
    ----------
    appname : str, default 'sync_'
        Prefix of every table name.

    Returns
    -------
    dict[str, str]
        Name per key, as get_table_names_for_appname returns.
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
        'Rebalance': f'{appname}rebalance',
        }


def verify_tables_exist(engine: Engine, appname: str = 'sync_') -> dict[str, bool]:
    """Whether each required table exists in the public schema.

    Parameters
    ----------
    engine : Engine
        Engine for the target database.
    appname : str, default 'sync_'
        Prefix of every table name.

    Returns
    -------
    dict[str, bool]
        Existence per table key, for every get_table_names key except
        'Inst'.
    """
    tables = get_table_names(appname)
    status = {}

    table_keys = [
        'Node',
        'Check',
        'Audit',
        'Claim',
        'Token',
        'Lock',
        'LeaderLock',
        'RebalanceLock',
        'Rebalance',
        ]

    with engine.connect() as conn:
        for table_key in table_keys:
            table_name = tables[table_key]
            sql = """
select exists (
    select from information_schema.tables
    where table_schema = 'public'
    and table_name = :table_name
)
"""
            result = conn.execute(text(sql), {'table_name': table_name})
            status[table_key] = result.scalar()

    return status


def _create_core_tables(engine: Engine, tables: dict[str, str]) -> None:
    """Create core tables (Node, Check, Audit, Claim).
    """
    node_table = tables['Node']
    check_table = tables['Check']
    audit_table = tables['Audit']
    claim_table = tables['Claim']

    with engine.connect() as conn:
        sql = f"""
create table if not exists {node_table} (
    name varchar not null,
    created_on timestamp with time zone not null,
    last_heartbeat timestamp with time zone,
    primary key (name)
);
"""
        conn.execute(text(sql))

        conn.execute(text(f'create index if not exists idx_{node_table}_heartbeat on {node_table}(last_heartbeat)'))

        sql = f"""
create table if not exists {check_table} (
    node varchar not null,
    created_on timestamp with time zone not null,
    primary key (node, created_on)
);
"""
        conn.execute(text(sql))

        sql = f"""
create table if not exists {audit_table} (
    created_on timestamp with time zone not null,
    node varchar not null,
    task_id varchar not null,
    date date not null
);
"""
        conn.execute(text(sql))

        sql = f"""
create index if not exists idx_{audit_table}_date_task_id
on {audit_table}(date, task_id)
"""
        conn.execute(text(sql))

        sql = f"""
create table if not exists {claim_table} (
    node varchar not null,
    task_id varchar not null,
    created_on timestamp with time zone not null,
    primary key (node, task_id)
);
"""
        conn.execute(text(sql))

        conn.commit()

    logger.debug(f'Core tables verified: {node_table}, {check_table}, {audit_table}, {claim_table}')


def _create_coordination_tables(engine: Engine, tables: dict[str, str]) -> None:
    """Create the Token, Lock, LeaderLock, RebalanceLock and Rebalance tables.
    """
    token_table = tables['Token']
    lock_table = tables['Lock']
    leader_lock_table = tables['LeaderLock']
    rebalance_lock_table = tables['RebalanceLock']
    rebalance_table = tables['Rebalance']

    with engine.connect() as conn:
        sql = f"""
create table if not exists {token_table} (
    token_id integer not null,
    node varchar not null,
    assigned_at timestamp with time zone not null,
    version integer not null default 1,
    primary key (token_id)
);
"""
        conn.execute(text(sql))

        conn.execute(text(f'create index if not exists idx_{token_table}_node on {token_table}(node)'))
        conn.execute(text(f'create index if not exists idx_{token_table}_assigned on {token_table}(assigned_at)'))
        conn.execute(text(f'create index if not exists idx_{token_table}_version on {token_table}(version)'))

        sql = f"""
create table if not exists {lock_table} (
    task_id varchar not null,
    node_patterns jsonb not null,
    reason varchar,
    created_at timestamp with time zone not null,
    created_by varchar not null,
    expires_at timestamp with time zone,
    primary key (task_id)
);
"""
        conn.execute(text(sql))

        conn.execute(text(f'create index if not exists idx_{lock_table}_created_by on {lock_table}(created_by)'))
        sql = f"""
create index if not exists idx_{lock_table}_expires on {lock_table}(expires_at)
where expires_at is not null
"""
        conn.execute(text(sql))

        sql = f"""
create table if not exists {leader_lock_table} (
    singleton integer primary key default 1,
    node varchar not null,
    acquired_at timestamp with time zone not null,
    operation varchar not null,
    check (singleton = 1)
);
"""
        conn.execute(text(sql))

        sql = f"""
create table if not exists {rebalance_lock_table} (
    singleton integer primary key default 1,
    in_progress boolean not null default false,
    started_at timestamp with time zone,
    started_by varchar,
    check (singleton = 1)
);
"""
        conn.execute(text(sql))

        sql = f"""
insert into {rebalance_lock_table} (singleton, in_progress)
values (1, false)
on conflict (singleton) do nothing
"""
        conn.execute(text(sql))

        sql = f"""
create table if not exists {rebalance_table} (
    id serial primary key,
    triggered_at timestamp with time zone not null,
    trigger_reason varchar not null,
    leader_node varchar not null,
    nodes_before integer not null,
    nodes_after integer not null,
    tokens_moved integer not null,
    duration_ms integer
);
"""
        conn.execute(text(sql))

        sql = f"""
create index if not exists idx_{rebalance_table}_triggered
on {rebalance_table}(triggered_at desc)
"""
        conn.execute(text(sql))

        conn.commit()

    logger.debug(
        f'Coordination tables verified: {token_table}, {lock_table}, '
        f'{leader_lock_table}, {rebalance_lock_table}, {rebalance_table}')


def ensure_database_ready(engine: Engine, appname: str = 'sync_') -> None:
    """Create any missing jobsync table and index.

    Safe to call repeatedly. An existing table keeps its structure.

    Parameters
    ----------
    engine : Engine
        Engine for the target database.
    appname : str, default 'sync_'
        Prefix of every table name.

    Raises
    ------
    sqlalchemy.exc.SQLAlchemyError
        A statement fails. A create failure is logged, then re-raised.
    """
    tables = get_table_names(appname)

    logger.debug('Verifying database structure')

    table_status = verify_tables_exist(engine, appname)

    required_keys = [
        'Node',
        'Check',
        'Audit',
        'Claim',
        'Token',
        'Lock',
        'LeaderLock',
        'RebalanceLock',
        'Rebalance',
        ]
    missing_tables = [k for k in required_keys if not table_status.get(k, False)]
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
