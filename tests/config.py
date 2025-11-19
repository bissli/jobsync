from libb import Setting

Setting.unlock()

postgres = Setting()
postgres.drivername = 'postgresql'
postgres.database = 'jobsync'
postgres.hostname = 'localhost'
postgres.username = 'postgres'
postgres.password = 'postgres'
postgres.port = 5432
postgres.timeout = 30
postgres.check_connection = True
postgres.cleanup = True

# Coordination settings (disabled for legacy tests)
sync = Setting()
sync.sql.appname = 'sync_'
sync.coordination.enabled = False  # Disable for backward compatibility with existing tests
sync.coordination.heartbeat_interval_sec = 5
sync.coordination.heartbeat_timeout_sec = 15
sync.coordination.rebalance_check_interval_sec = 30
sync.coordination.dead_node_check_interval_sec = 10
sync.coordination.token_refresh_initial_interval_sec = 5
sync.coordination.token_refresh_steady_interval_sec = 30
sync.coordination.total_tokens = 10000
sync.coordination.locks_enabled = True
sync.coordination.lock_orphan_warning_hours = 24
sync.coordination.leader_lock_timeout_sec = 30
sync.coordination.health_check_interval_sec = 30

Setting.lock()
