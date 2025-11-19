import os
import pathlib

from libb import Setting

HERE = pathlib.Path(pathlib.Path(__file__).parent).resolve()

Setting.unlock()

sync = Setting()
# sql
sync.sql.appname = os.getenv('SYNC_SQL_APPNAME', 'sync_')
sync.sql.profile = os.getenv('SYNC_SQL_PROFILE', 'postgres')
sync.sql.timezone = os.getenv('SYNC_SQL_TIMEZONE', 'US/Eastern')
sync.sql.host = os.getenv('SYNC_SQL_HOST', 'localhost')
sync.sql.dbname = os.getenv('SYNC_SQL_DATABASE', 'jobsync')
sync.sql.user = os.getenv('SYNC_SQL_USERNAME', 'postgres')
sync.sql.passwd = os.getenv('SYNC_SQL_PASSWORD', 'postgres')
sync.sql.port = os.getenv('SYNC_SQL_PORT', 5432)
# coordination
sync.coordination.enabled = os.getenv('SYNC_COORDINATION_ENABLED', 'true').lower() == 'true'
sync.coordination.heartbeat_interval_sec = int(os.getenv('SYNC_HEARTBEAT_INTERVAL', '5'))
sync.coordination.heartbeat_timeout_sec = int(os.getenv('SYNC_HEARTBEAT_TIMEOUT', '15'))
sync.coordination.rebalance_check_interval_sec = int(os.getenv('SYNC_REBALANCE_INTERVAL', '30'))
sync.coordination.dead_node_check_interval_sec = int(os.getenv('SYNC_DEAD_NODE_INTERVAL', '10'))
sync.coordination.token_refresh_initial_interval_sec = int(os.getenv('SYNC_TOKEN_REFRESH_INITIAL', '5'))
sync.coordination.token_refresh_steady_interval_sec = int(os.getenv('SYNC_TOKEN_REFRESH_STEADY', '30'))
sync.coordination.total_tokens = int(os.getenv('SYNC_TOTAL_TOKENS', '10000'))
sync.coordination.locks_enabled = os.getenv('SYNC_LOCKS_ENABLED', 'true').lower() == 'true'
sync.coordination.lock_orphan_warning_hours = int(os.getenv('SYNC_LOCK_ORPHAN_WARNING', '24'))
sync.coordination.leader_lock_timeout_sec = int(os.getenv('SYNC_LEADER_LOCK_TIMEOUT', '30'))
sync.coordination.health_check_interval_sec = int(os.getenv('SYNC_HEALTH_CHECK_INTERVAL', '30'))

Setting.lock()

if __name__ == '__main__':
    __import__('doctest').testmod()
