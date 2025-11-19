from types import SimpleNamespace

postgres = SimpleNamespace(
    drivername='postgresql',
    database='jobsync',
    hostname='localhost',
    username='postgres',
    password='postgres',
    port=5432,
    timeout=30,
    check_connection=True,
    cleanup=True
)

sync = SimpleNamespace(
    sql=SimpleNamespace(appname='sync_'),
    coordination=SimpleNamespace(
        enabled=False,  # Disable for backward compatibility with existing tests
        heartbeat_interval_sec=5,
        heartbeat_timeout_sec=15,
        rebalance_check_interval_sec=30,
        dead_node_check_interval_sec=10,
        token_refresh_initial_interval_sec=5,
        token_refresh_steady_interval_sec=30,
        total_tokens=10000,
        locks_enabled=True,
        lock_orphan_warning_hours=24,
        leader_lock_timeout_sec=30,
        health_check_interval_sec=30
    )
)
