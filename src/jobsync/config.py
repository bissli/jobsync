import os
import pathlib
from dataclasses import dataclass
from types import SimpleNamespace

HERE = pathlib.Path(pathlib.Path(__file__).parent).resolve()


@dataclass
class CoordinationConfig:
    """Configuration for hybrid coordination system.

    All timing parameters are in seconds.
    """
    enabled: bool = True
    total_tokens: int = 10000
    heartbeat_interval_sec: int = 5
    heartbeat_timeout_sec: int = 15
    rebalance_check_interval_sec: int = 30
    dead_node_check_interval_sec: int = 10
    token_refresh_initial_interval_sec: int = 5
    token_refresh_steady_interval_sec: int = 30
    locks_enabled: bool = True
    leader_lock_timeout_sec: int = 30
    health_check_interval_sec: int = 30
    stale_leader_lock_age_sec: int = 300


sync = SimpleNamespace(
    sql=SimpleNamespace(
        appname=os.getenv('SYNC_SQL_APPNAME', 'sync_'),
        profile=os.getenv('SYNC_SQL_PROFILE', 'postgres'),
        timezone=os.getenv('SYNC_SQL_TIMEZONE', 'US/Eastern'),
        host=os.getenv('SYNC_SQL_HOST', 'localhost'),
        dbname=os.getenv('SYNC_SQL_DATABASE', 'jobsync'),
        user=os.getenv('SYNC_SQL_USERNAME', 'postgres'),
        passwd=os.getenv('SYNC_SQL_PASSWORD', 'postgres'),
        port=os.getenv('SYNC_SQL_PORT', 5432)
    ),
    coordination=SimpleNamespace(
        enabled=os.getenv('SYNC_COORDINATION_ENABLED', 'true').lower() == 'true',
        heartbeat_interval_sec=int(os.getenv('SYNC_HEARTBEAT_INTERVAL', '5')),
        heartbeat_timeout_sec=int(os.getenv('SYNC_HEARTBEAT_TIMEOUT', '15')),
        rebalance_check_interval_sec=int(os.getenv('SYNC_REBALANCE_INTERVAL', '30')),
        dead_node_check_interval_sec=int(os.getenv('SYNC_DEAD_NODE_INTERVAL', '10')),
        token_refresh_initial_interval_sec=int(os.getenv('SYNC_TOKEN_REFRESH_INITIAL', '5')),
        token_refresh_steady_interval_sec=int(os.getenv('SYNC_TOKEN_REFRESH_STEADY', '30')),
        total_tokens=int(os.getenv('SYNC_TOTAL_TOKENS', '10000')),
        locks_enabled=os.getenv('SYNC_LOCKS_ENABLED', 'true').lower() == 'true',
        lock_orphan_warning_hours=int(os.getenv('SYNC_LOCK_ORPHAN_WARNING', '24')),
        leader_lock_timeout_sec=int(os.getenv('SYNC_LEADER_LOCK_TIMEOUT', '30')),
        health_check_interval_sec=int(os.getenv('SYNC_HEALTH_CHECK_INTERVAL', '30'))
    )
)

if __name__ == '__main__':
    __import__('doctest').testmod()
