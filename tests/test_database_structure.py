"""Database structure verification tests.

Tests verify:
- Table existence checking works correctly
- Core tables are created when missing
- Coordination tables are created only when coordination enabled
- Database initialization is idempotent
- Missing tables are created automatically
"""
import logging
from types import SimpleNamespace

import config as test_config
import pytest
from asserts import assert_equal, assert_false, assert_true
from sqlalchemy import text

from jobsync import schema
from jobsync.client import CoordinationConfig, Job

logger = logging.getLogger(__name__)


def get_structure_test_config():
    """Create a config for structure testing.
    """
    config = SimpleNamespace()
    config.postgres = test_config.postgres
    config.sync = SimpleNamespace(
        sql=SimpleNamespace(appname='sync_'),
        coordination=SimpleNamespace(
            enabled=True,
            heartbeat_interval_sec=5,
            heartbeat_timeout_sec=15,
            total_tokens=100
        )
    )
    return config


class TestTableVerification:
    """Test table existence verification."""

    def test_verify_tables_exist_all_present(self, postgres):
        """Verify verification returns True when all tables exist.
        """
        config = get_structure_test_config()
        tables = schema.get_table_names(config)

        schema.ensure_database_ready(postgres, config, coordination_enabled=True)

        status = schema.verify_tables_exist(postgres, config, coordination_enabled=True)

        expected_tables = ['Node', 'Check', 'Audit', 'Claim', 'Token', 'Lock',
                          'LeaderLock', 'RebalanceLock', 'Rebalance']

        for table_key in expected_tables:
            assert_true(status[table_key], f'{table_key} should exist')

    def test_verify_tables_exist_missing_coordination_tables(self, postgres):
        """Verify verification detects missing coordination tables.
        """
        config = get_structure_test_config()
        tables = schema.get_table_names(config)

        schema.ensure_database_ready(postgres, config, coordination_enabled=False)

        with postgres.connect() as conn:
            for table in ['Token', 'Lock', 'LeaderLock', 'RebalanceLock', 'Rebalance']:
                conn.execute(text(f'DROP TABLE IF EXISTS {tables[table]}'))
            conn.commit()

        status = schema.verify_tables_exist(postgres, config, coordination_enabled=True)

        for table_key in ['Node', 'Check', 'Audit', 'Claim']:
            assert_true(status[table_key], f'Core table {table_key} should exist')

        for table_key in ['Token', 'Lock', 'LeaderLock', 'RebalanceLock', 'Rebalance']:
            assert_false(status.get(table_key, False), f'Coordination table {table_key} should not exist')

    def test_verify_tables_only_checks_requested_tables(self, postgres):
        """Verify verification only checks coordination tables when requested.
        """
        config = get_structure_test_config()

        schema.ensure_database_ready(postgres, config, coordination_enabled=False)

        status = schema.verify_tables_exist(postgres, config, coordination_enabled=False)

        for table_key in ['Node', 'Check', 'Audit', 'Claim']:
            assert_true(table_key in status, f'{table_key} should be in status')

        for table_key in ['Token', 'Lock', 'LeaderLock', 'RebalanceLock', 'Rebalance']:
            assert_false(table_key in status, f'{table_key} should not be in status when coordination disabled')


class TestCoreTableCreation:
    """Test core table creation."""

    def test_core_tables_created_when_missing(self, postgres):
        """Verify core tables are created when missing.
        """
        config = get_structure_test_config()
        tables = schema.get_table_names(config)

        with postgres.connect() as conn:
            for table in ['Node', 'Check', 'Audit', 'Claim']:
                conn.execute(text(f'DROP TABLE IF EXISTS {tables[table]}'))
            conn.commit()

        schema.ensure_database_ready(postgres, config, coordination_enabled=False)

        with postgres.connect() as conn:
            for table in ['Node', 'Check', 'Audit', 'Claim']:
                result = conn.execute(text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'public'
                        AND table_name = :table_name
                    )
                """), {'table_name': tables[table]})
                exists = result.scalar()
                assert_true(exists, f'{table} should be created')

    def test_core_tables_have_required_indexes(self, postgres):
        """Verify core tables have required indexes.
        """
        config = get_structure_test_config()
        tables = schema.get_table_names(config)

        schema.ensure_database_ready(postgres, config, coordination_enabled=False)

        with postgres.connect() as conn:
            result = conn.execute(text("""
                SELECT indexname FROM pg_indexes
                WHERE tablename = :table_name
                AND schemaname = 'public'
            """), {'table_name': tables['Node']})
            indexes = [row[0] for row in result]

        expected_index = f'idx_{tables["Node"]}_heartbeat'
        assert_true(expected_index in indexes, 'Node table should have heartbeat index')


class TestCoordinationTableCreation:
    """Test coordination table creation."""

    def test_coordination_tables_created_when_enabled(self, postgres):
        """Verify coordination tables are created when coordination enabled.
        """
        config = get_structure_test_config()
        tables = schema.get_table_names(config)

        with postgres.connect() as conn:
            for table in ['Token', 'Lock', 'LeaderLock', 'RebalanceLock', 'Rebalance']:
                conn.execute(text(f'DROP TABLE IF EXISTS {tables[table]}'))
            conn.commit()

        schema.ensure_database_ready(postgres, config, coordination_enabled=True)

        with postgres.connect() as conn:
            for table in ['Token', 'Lock', 'LeaderLock', 'RebalanceLock', 'Rebalance']:
                result = conn.execute(text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'public'
                        AND table_name = :table_name
                    )
                """), {'table_name': tables[table]})
                exists = result.scalar()
                assert_true(exists, f'{table} should be created when coordination enabled')

    def test_coordination_tables_not_created_when_disabled(self, postgres):
        """Verify coordination tables are not created when coordination disabled.
        """
        config = get_structure_test_config()
        tables = schema.get_table_names(config)

        with postgres.connect() as conn:
            for table in ['Token', 'Lock', 'LeaderLock', 'RebalanceLock', 'Rebalance']:
                conn.execute(text(f'DROP TABLE IF EXISTS {tables[table]}'))
            conn.commit()

        schema.ensure_database_ready(postgres, config, coordination_enabled=False)

        with postgres.connect() as conn:
            for table in ['Token', 'Lock', 'LeaderLock', 'RebalanceLock', 'Rebalance']:
                result = conn.execute(text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'public'
                        AND table_name = :table_name
                    )
                """), {'table_name': tables[table]})
                exists = result.scalar()
                assert_false(exists, f'{table} should not be created when coordination disabled')

    def test_coordination_tables_have_required_indexes(self, postgres):
        """Verify coordination tables have required indexes.
        """
        config = get_structure_test_config()
        tables = schema.get_table_names(config)

        schema.ensure_database_ready(postgres, config, coordination_enabled=True)

        expected_indexes = {
            tables['Token']: [f'idx_{tables["Token"]}_node', f'idx_{tables["Token"]}_assigned', f'idx_{tables["Token"]}_version'],
            tables['Lock']: [f'idx_{tables["Lock"]}_created_by', f'idx_{tables["Lock"]}_expires'],
        }

        with postgres.connect() as conn:
            for table_name, expected in expected_indexes.items():
                result = conn.execute(text("""
                    SELECT indexname FROM pg_indexes
                    WHERE tablename = :table_name
                    AND schemaname = 'public'
                """), {'table_name': table_name})
                indexes = [row[0] for row in result]

                for expected_index in expected:
                    assert_true(expected_index in indexes, f'{table_name} should have {expected_index} index')


class TestIdempotentInitialization:
    """Test that database initialization is idempotent."""

    def test_ensure_database_ready_is_idempotent(self, postgres):
        """Verify ensure_database_ready can be called multiple times safely.
        """
        config = get_structure_test_config()

        schema.ensure_database_ready(postgres, config, coordination_enabled=True)

        try:
            schema.ensure_database_ready(postgres, config, coordination_enabled=True)
            schema.ensure_database_ready(postgres, config, coordination_enabled=True)
        except Exception as e:
            pytest.fail(f'ensure_database_ready should be idempotent: {e}')

    def test_rebalance_lock_initialized_only_once(self, postgres):
        """Verify RebalanceLock singleton row is only created once.
        """
        config = get_structure_test_config()
        tables = schema.get_table_names(config)

        schema.ensure_database_ready(postgres, config, coordination_enabled=True)
        schema.ensure_database_ready(postgres, config, coordination_enabled=True)

        with postgres.connect() as conn:
            result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["RebalanceLock"]}'))
            count = result.scalar()

        assert_equal(count, 1, 'RebalanceLock should have exactly one row')


class TestJobInitialization:
    """Test Job automatically initializes database."""

    def test_job_creates_core_tables_automatically(self, postgres):
        """Verify Job creation triggers core table creation.
        """
        config = get_structure_test_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        with postgres.connect() as conn:
            for table in ['Node', 'Check', 'Audit', 'Claim']:
                conn.execute(text(f'DROP TABLE IF EXISTS {tables[table]}'))
            conn.commit()

        coord_config = CoordinationConfig(enabled=False)
        job = Job('test-node', config, wait_on_enter=0, connection_string=connection_string, coordination_config=coord_config)

        with postgres.connect() as conn:
            for table in ['Node', 'Check', 'Audit', 'Claim']:
                result = conn.execute(text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'public'
                        AND table_name = :table_name
                    )
                """), {'table_name': tables[table]})
                exists = result.scalar()
                assert_true(exists, f'{table} should be created by Job initialization')

    def test_job_creates_coordination_tables_when_enabled(self, postgres):
        """Verify Job creates coordination tables when coordination enabled.
        """
        config = get_structure_test_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        with postgres.connect() as conn:
            for table in ['Token', 'Lock', 'LeaderLock', 'RebalanceLock', 'Rebalance']:
                conn.execute(text(f'DROP TABLE IF EXISTS {tables[table]}'))
            conn.commit()

        coord_config = CoordinationConfig(enabled=True, total_tokens=100)
        job = Job('test-node', config, wait_on_enter=0, connection_string=connection_string, coordination_config=coord_config)

        with postgres.connect() as conn:
            for table in ['Token', 'Lock', 'LeaderLock', 'RebalanceLock', 'Rebalance']:
                result = conn.execute(text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'public'
                        AND table_name = :table_name
                    )
                """), {'table_name': tables[table]})
                exists = result.scalar()
                assert_true(exists, f'{table} should be created when coordination enabled')

    def test_job_without_coordination_skips_coordination_tables(self, postgres):
        """Verify Job without coordination doesn't create coordination tables.
        """
        config = get_structure_test_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        with postgres.connect() as conn:
            for table in ['Token', 'Lock', 'LeaderLock', 'RebalanceLock', 'Rebalance']:
                conn.execute(text(f'DROP TABLE IF EXISTS {tables[table]}'))
            conn.commit()

        coord_config = CoordinationConfig(enabled=False)
        job = Job('test-node', config, wait_on_enter=0, connection_string=connection_string, coordination_config=coord_config)

        with postgres.connect() as conn:
            for table in ['Token', 'Lock', 'LeaderLock', 'RebalanceLock', 'Rebalance']:
                result = conn.execute(text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'public'
                        AND table_name = :table_name
                    )
                """), {'table_name': tables[table]})
                exists = result.scalar()
                assert_false(exists, f'{table} should not exist when coordination disabled')


class TestMixedModeOperations:
    """Test switching between coordination modes."""

    def test_switching_from_disabled_to_enabled_creates_tables(self, postgres):
        """Verify switching to coordination mode creates missing tables.
        """
        config = get_structure_test_config()
        tables = schema.get_table_names(config)
        connection_string = postgres.url.render_as_string(hide_password=False)

        with postgres.connect() as conn:
            for table in ['Token', 'Lock', 'LeaderLock', 'RebalanceLock', 'Rebalance']:
                conn.execute(text(f'DROP TABLE IF EXISTS {tables[table]}'))
            conn.commit()

        coord_config = CoordinationConfig(enabled=False)
        job1 = Job('test-node-1', config, wait_on_enter=0, connection_string=connection_string, coordination_config=coord_config)

        with postgres.connect() as conn:
            result = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = :table_name
                )
            """), {'table_name': tables['Token']})
            exists_before = result.scalar()

        assert_false(exists_before, 'Token table should not exist with coordination disabled')

        coord_config = CoordinationConfig(enabled=True, total_tokens=100)
        job2 = Job('test-node-2', config, wait_on_enter=0, connection_string=connection_string, coordination_config=coord_config)

        with postgres.connect() as conn:
            result = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = :table_name
                )
            """), {'table_name': tables['Token']})
            exists_after = result.scalar()

        assert_true(exists_after, 'Token table should be created when switching to coordination mode')


if __name__ == '__main__':
    pytest.main(args=['-sx', __file__])
