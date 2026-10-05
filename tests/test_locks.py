"""Tests for lock registration, management, and coordination.

Scope
-----
- Lock registration and lifecycle tests
- Lock provider callback tests
- Pattern matching and fallback logic
- Concurrent lock acquisition
- Leader lock coordination
"""
import datetime
import logging
import threading
from dataclasses import replace

import pytest
from fixtures import *  # noqa: F401, F403
from sqlalchemy import Engine, text

from jobsync import schema
from jobsync.client import CoordinationConfig, JobState, LockNotAcquired


def count_locks(postgres: Engine, tables: dict) -> int:
    """Rows in tables['Lock'], expired locks included.
    """
    with postgres.connect() as conn:
        return conn.execute(text(f'select count(*) from {tables["Lock"]}')).scalar()


@pytest.fixture
def jobs_to_exit():
    """Jobs a test builds without entering, exited at teardown.

    Yields
    ------
    list[Job]
        The test appends each job it never enters. Teardown calls __exit__ on
        each, which disposes its engine.
    """
    jobs = []
    yield jobs
    for job in jobs:
        job.__exit__(None, None, None)


class TestLockRegistration:
    """Test lock registration API.
    """

    @clean_tables('Lock')
    def test_register_single_lock(self, postgres, jobs_to_exit):
        """Verify register_lock stores a str pattern as a one-item list.

        Mutation: _build_lock_row stops wrapping a str pattern in a list, or
            records a creator other than the job's node name.
        Oracle: the literal arguments passed to register_lock.
        """

        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)
        jobs_to_exit.append(job)

        task_id = 'task-123'
        job.register_lock(task_id, 'special-%', 'test reason')

        lock_sql = f"""
select node_patterns, reason, created_by
from {tables["Lock"]}
where task_id = :task_id
"""
        with postgres.connect() as conn:
            result = conn.execute(text(lock_sql), {'task_id': str(task_id)})
            lock = [dict(row._mapping) for row in result]

        assert len(lock) == 1, 'Lock should be created'
        patterns = lock[0]['node_patterns']
        assert patterns == ['special-%']
        assert lock[0]['reason'] == 'test reason'
        assert lock[0]['created_by'] == 'node1'

    @clean_tables('Lock')
    def test_register_bulk_locks(self, postgres, jobs_to_exit):
        """Verify register_locks_bulk stores each tuple's pattern and reason.

        Mutation: register_locks_bulk unpacks the tuple in the wrong order
            (pattern and reason swapped), or writes only the first row.
        Oracle: the literal (task_id, pattern, reason) tuples passed in.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)
        jobs_to_exit.append(job)

        locks = [
            ('task-1', 'pattern-1', 'reason-1'),
            ('task-2', 'pattern-2', 'reason-2'),
            ('task-3', 'pattern-3', 'reason-3'),
            ]

        job.register_locks_bulk(locks)

        locks_sql = f"""
select task_id, node_patterns, reason, created_by
from {tables["Lock"]}
order by task_id
"""
        with postgres.connect() as conn:
            result = conn.execute(text(locks_sql))
            rows = [tuple(row) for row in result]
        assert rows == [
            ('task-1', ['pattern-1'], 'reason-1', 'node1'),
            ('task-2', ['pattern-2'], 'reason-2', 'node1'),
            ('task-3', ['pattern-3'], 'reason-3', 'node1'),
            ]

    @clean_tables('Lock')
    def test_lock_idempotency(self, postgres, jobs_to_exit):
        """Verify re-registering a task keeps one row with the latest pattern.

        Mutation: the Lock upsert uses ON CONFLICT DO NOTHING in place of
            DO UPDATE.
        Oracle: the pattern passed on the second call.
        """

        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)
        jobs_to_exit.append(job)

        task_id = 'task-123'
        job.register_lock(task_id, 'pattern-1', 'reason-1')
        job.register_lock(task_id, 'pattern-2', 'reason-2')

        patterns_sql = f"""
select node_patterns
from {tables["Lock"]}
where task_id = :task_id
"""
        with postgres.connect() as conn:
            result = conn.execute(text(patterns_sql), {'task_id': str(task_id)})
            locks = [dict(row._mapping) for row in result]

        assert len(locks) == 1, 'Should only have 1 lock (ON CONFLICT DO UPDATE)'
        patterns = locks[0]['node_patterns']
        assert patterns == ['pattern-2'], \
            'Second pattern should replace first (DO UPDATE)'


class TestConcurrentLockRegistration:
    """Test concurrent lock registration from multiple nodes.
    """

    @clean_tables('Lock')
    def test_same_lock_from_multiple_nodes_sequential(self, postgres, jobs_to_exit):
        """Verify the last of ten registering nodes is the recorded creator.

        Mutation: the Lock upsert's DO UPDATE SET omits created_by, or the
            insert loses its ON CONFLICT clause so node2's call raises.
        Oracle: node10 is the last caller.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        for i in range(1, 11):
            job = create_job(
                f'node{i}',
                postgres,
                coordination_config=config,
                wait_on_enter=0)
            jobs_to_exit.append(job)
            job.register_lock('task-X1', 'Node1', 'lock X1 to Node1')

        assert count_locks(postgres, tables) == 1, \
            'Only 1 lock should exist (idempotent)'

        with postgres.connect() as conn:
            result = conn.execute(
                text(f'select node_patterns, created_by from {tables["Lock"]}'))
            lock = [dict(row._mapping) for row in result]
        assert len(lock) == 1
        patterns = lock[0]['node_patterns']
        assert patterns == ['Node1'], 'Pattern should be ["Node1"]'
        assert lock[0]['created_by'] == 'node10', \
            'Last node should be recorded as creator (DO UPDATE)'

    @clean_tables('Lock')
    def test_same_lock_from_multiple_nodes_simulated_concurrent(
        self,
        postgres,
        jobs_to_exit
    ):
        """Verify ten simultaneous registrations of one task all succeed.

        Mutation: register_lock replaced by a check-then-insert (SELECT, then
            INSERT or UPDATE) that races under concurrency.
        Oracle: ten threads released by one barrier, every call expected to
            return without raising.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        jobs = [
            create_job(
                f'node{i}',
                postgres,
                coordination_config=config,
                wait_on_enter=0)
            for i in range(1, 11)
            ]
        jobs_to_exit.extend(jobs)
        barrier = threading.Barrier(len(jobs))
        errors = []

        def register_lock(job):
            barrier.wait()
            try:
                job.register_lock('task-X1', 'Node1', f'lock from {job.node_name}')
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=register_lock, args=(job,)) for job in jobs]
        for t in threads:
            t.start()

        for t in threads:
            t.join()

        assert errors == [], f'Concurrent registration raised: {errors}'
        assert count_locks(postgres, tables) == 1, \
            'Only 1 lock should exist despite concurrent registration'

    @clean_tables('Lock')
    def test_bulk_lock_registration_idempotency(self, postgres, jobs_to_exit):
        """Verify repeated bulk registration keeps one row per task.

        Mutation: register_locks_bulk uses a plain INSERT without the upsert
            clause, so the second node's batch raises.
        Oracle: three distinct task ids registered.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        locks_to_register = [
            ('task-1', 'pattern-A', 'reason-1'),
            ('task-2', 'pattern-A', 'reason-2'),
            ('task-3', 'pattern-B', 'reason-3'),
            ]

        for i in range(1, 8):
            job = create_job(
                f'node{i}',
                postgres,
                coordination_config=config,
                wait_on_enter=0)
            jobs_to_exit.append(job)
            job.register_locks_bulk(locks_to_register)

        assert count_locks(postgres, tables) == 3, \
            'Should have exactly 3 locks despite 7 nodes registering'


class TestLockClearing:
    """Test lock clearing functionality.
    """

    @clean_tables('Lock')
    def test_clear_locks_by_creator(self, postgres, jobs_to_exit):
        """Verify clear_locks_by_creator deletes and counts a creator's locks.

        Mutation: clear_locks_by_creator returns a constant in place of
            result.rowcount.
        Oracle: three locks registered by node1.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)
        jobs_to_exit.append(job)

        job.register_lock('task-1', 'pattern-1', 'reason-1')
        job.register_lock('task-2', 'pattern-2', 'reason-2')
        job.register_lock('task-3', 'pattern-3', 'reason-3')

        assert count_locks(postgres, tables) == 3, 'Should have 3 locks'

        removed = job.clear_locks_by_creator('node1')
        assert removed == 3, 'Should remove 3 locks'

        assert count_locks(postgres, tables) == 0, 'Should have 0 locks remaining'

    @clean_tables('Lock')
    def test_clear_locks_by_creator_selective(self, postgres, jobs_to_exit):
        """Verify clear_locks_by_creator keeps another creator's locks.

        Mutation: the DELETE drops its WHERE created_by filter.
        Oracle: two locks by node1 and one by node2.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        job1 = create_job(
            'node1',
            postgres,
            coordination_config=config,
            wait_on_enter=0)
        job2 = create_job(
            'node2',
            postgres,
            coordination_config=config,
            wait_on_enter=0)
        jobs_to_exit.extend([job1, job2])

        job1.register_lock('task-1', 'pattern-1', 'from node1')
        job1.register_lock('task-2', 'pattern-2', 'from node1')
        job2.register_lock('task-3', 'pattern-3', 'from node2')

        assert count_locks(postgres, tables) == 3, 'Should have 3 locks total'

        removed = job1.clear_locks_by_creator('node1')
        assert removed == 2, 'Should remove 2 locks from node1'

        with postgres.connect() as conn:
            result = conn.execute(text(f'select created_by from {tables["Lock"]}'))
            remaining = [dict(row._mapping) for row in result]
        assert len(remaining) == 1, 'Should have 1 lock remaining'
        assert remaining[0]['created_by'] == 'node2', \
            'Remaining lock should be from node2'

    @clean_tables('Lock')
    def test_clear_all_locks(self, postgres, jobs_to_exit):
        """Verify clear_all_locks removes all locks regardless of creator.

        Mutation: clear_all_locks filters by the calling node's name.
        Oracle: one lock each from node1 and node2.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        job1 = create_job(
            'node1',
            postgres,
            coordination_config=config,
            wait_on_enter=0)
        job2 = create_job(
            'node2',
            postgres,
            coordination_config=config,
            wait_on_enter=0)
        jobs_to_exit.extend([job1, job2])

        job1.register_lock('task-1', 'pattern-1', 'from node1')
        job2.register_lock('task-2', 'pattern-2', 'from node2')

        assert count_locks(postgres, tables) == 2, 'Should have 2 locks'

        removed = job1.clear_all_locks()
        assert removed == 2, 'Should remove all locks'

        assert count_locks(postgres, tables) == 0, 'Should have 0 locks'


class TestLockListing:
    """Test lock listing functionality.
    """

    @clean_tables('Lock')
    def test_list_locks_content(self, postgres, jobs_to_exit):
        """Verify list_locks returns patterns, reason, creator and expiry.

        Mutation: list_locks drops reason or created_by from its SELECT.
        Oracle: the literal arguments passed to register_lock.
        """
        config = get_coordination_config()

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)
        jobs_to_exit.append(job)

        job.register_lock('task-1', 'pattern-1', 'reason-1')
        job.register_lock('task-2', 'pattern-2', 'reason-2')

        locks = job.list_locks()
        assert len(locks) == 2, 'Should have 2 locks'

        lock1 = next(l for l in locks if l['node_patterns'] == ['pattern-1'])
        assert lock1['reason'] == 'reason-1'
        assert lock1['created_by'] == 'node1'
        assert lock1['expires_at'] is None

    @clean_tables('Lock')
    def test_list_locks_filters_expired(self, postgres, jobs_to_exit):
        """Verify list_locks omits an expired lock and keeps unexpired ones.

        Mutation: list_locks drops the expires_at > NOW() filter, or flips
            the comparison.
        Oracle: expiries one day ahead, two days past, and NULL.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)
        jobs_to_exit.append(job)

        job.register_lock('task-1', 'pattern-1', 'not expired')
        job.register_lock('task-2', 'pattern-2', 'expires soon', expires_in_days=1)

        expired_time = (
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=2))
        insert_lock(
            postgres,
            tables,
            999,
            ['pattern-expired'],
            created_by='node1',
            expires_at=expired_time,
            reason='already expired')

        locks = job.list_locks()

        patterns = [l['node_patterns'][0] for l in locks]
        assert 'pattern-1' in patterns, 'Non-expired lock should be listed'
        assert 'pattern-2' in patterns, 'Future-expiring lock should be listed'
        assert 'pattern-expired' not in patterns, 'Expired lock should not be listed'

    @clean_tables('Lock')
    @pytest.mark.parametrize(
        ('second_patterns', 'expect_warning'),
        [
            (['other-%'], True),
            (['special-%'], False),
            ])
    def test_get_active_locks_warns_on_conflicting_token_collision(
            self, postgres, jobs_to_exit, caplog, second_patterns, expect_warning):
        """Verify get_active_locks warns when two locks on one token disagree.

        Mutation: get_active_locks overwriting the earlier token entry with
            no warning, or warning even when both locks name the same
            patterns.
        Oracle: get_active_locks returns one pattern list per token, so of
            two tasks hashing to one token only one lock can apply.
        """
        config = get_coordination_config(total_tokens=4)
        job = create_job('node1', postgres, coordination_config=config, wait_on_enter=0)
        jobs_to_exit.append(job)

        first_task = 'task-0'
        second_task = next(
            f'task-{i}' for i in range(1, 100)
            if job.task_to_token(f'task-{i}') == job.task_to_token(first_task))
        job.register_lock(first_task, ['special-%'], 'first')
        job.register_lock(second_task, second_patterns, 'second')

        with caplog.at_level(logging.WARNING, logger='jobsync.client'):
            locked_tokens = job.locks.get_active_locks()

        assert list(locked_tokens) == [job.task_to_token(first_task)]
        warnings = [r for r in caplog.records if 'share token' in r.getMessage()]
        assert bool(warnings) is expect_warning


class TestClearExistingLocks:
    """Test clear_existing_locks parameter behavior.
    """

    @clean_tables('Lock')
    def test_clear_existing_locks_true(self, postgres):
        """Verify clear_existing_locks=True keeps only the provider's new lock.

        Mutation: _on_enter_cluster_forming ignores the flag, or clears
            after calling lock_provider.
        Oracle: two locks from the first run, one from the second.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        def first_lock_provider(job):
            job.register_lock('task-1', 'pattern-OLD', 'first run')
            job.register_lock('task-2', 'pattern-OLD', 'first run')

        with create_job(
            'node1',
            postgres,
            coordination_config=config,
            lock_provider=first_lock_provider,
            clear_existing_locks=False,
            wait_on_enter=0,
            wait_on_exit=0):
            pass

        with postgres.connect() as conn:
            count_after_first = conn.execute(
                text(f'select count(*) from {tables["Lock"]} where created_by = :creator'),
                {'creator': 'node1'}).scalar()
        assert count_after_first == 2, 'Should have 2 locks after first run'

        def second_lock_provider(job):
            job.register_lock('task-3', 'pattern-NEW', 'second run')

        with create_job(
            'node1',
            postgres,
            coordination_config=config,
            lock_provider=second_lock_provider,
            clear_existing_locks=True,
            wait_on_enter=0,
            wait_on_exit=0):
            pass

        with postgres.connect() as conn:
            result = conn.execute(
                text(f'select node_patterns from {tables["Lock"]} where created_by = :creator'),
                {'creator': 'node1'})
            locks = [dict(row._mapping) for row in result]

        patterns = [l['node_patterns'][0] for l in locks]

        assert len(locks) == 1, 'Should only have 1 lock (old ones cleared)'
        assert patterns[0] == 'pattern-NEW', 'Should have new pattern only'

    @clean_tables('Lock')
    def test_clear_existing_locks_false(self, postgres):
        """Verify clear_existing_locks=False preserves existing locks.

        Mutation: _on_enter_cluster_forming clears the node's locks whatever
            the flag says.
        Oracle: one lock from each of two runs.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        def first_lock_provider(job):
            job.register_lock('task-1', 'pattern-OLD', 'first run')

        with create_job(
            'node1',
            postgres,
            coordination_config=config,
            lock_provider=first_lock_provider,
            clear_existing_locks=False,
            wait_on_enter=0,
            wait_on_exit=0):
            pass

        def second_lock_provider(job):
            job.register_lock('task-2', 'pattern-NEW', 'second run')

        with create_job(
            'node1',
            postgres,
            coordination_config=config,
            lock_provider=second_lock_provider,
            clear_existing_locks=False,
            wait_on_enter=0,
            wait_on_exit=0):
            pass

        with postgres.connect() as conn:
            result = conn.execute(
                text(f'select node_patterns from {tables["Lock"]} where created_by = :creator order by task_id'),
                {'creator': 'node1'})
            locks = [dict(row._mapping) for row in result]

        patterns = [l['node_patterns'][0] for l in locks]

        assert len(locks) == 2, 'Should have both old and new locks'
        assert 'pattern-OLD' in patterns, 'Old lock should be preserved'
        assert 'pattern-NEW' in patterns, 'New lock should be added'


class TestLockProviderTiming:
    """Test lock_provider callback timing in state machine.
    """

    @clean_tables('Lock')
    def test_lock_provider_called_during_cluster_forming(self, postgres):
        """Verify lock_provider runs in CLUSTER_FORMING, before distribution.

        Mutation: lock_provider invoked from _on_enter_distributing or a
            running-state entry action.
        Oracle: JobState.CLUSTER_FORMING recorded by the callback.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        callback_invoked = []

        def track_lock_provider(job):
            callback_invoked.append(job.state_machine.state)
            job.register_lock('task-1', 'pattern-1', 'test lock')

        job = create_job(
            'node1',
            postgres,
            coordination_config=config,
            wait_on_enter=2,
            lock_provider=track_lock_provider)

        job.__enter__()

        try:
            assert wait_for_running_state(job, timeout_sec=5)

            assert len(callback_invoked) > 0, 'lock_provider should be invoked'
            assert callback_invoked[0] == JobState.CLUSTER_FORMING, \
                'lock_provider should be called during CLUSTER_FORMING'

            assert count_locks(postgres, tables) == 1, 'Lock should be registered'

        finally:
            job.__exit__(None, None, None)

    def test_lock_provider_not_called_without_coordination(self, postgres):
        """Verify lock_provider not invoked when coordination disabled.

        Mutation: __enter__ invokes lock_provider before its standalone-mode
            early return.
        Oracle: zero calls recorded by the callback.
        """
        callback_invoked = []

        def track_lock_provider(job):
            callback_invoked.append(True)

        job = create_job(
            'node1',
            postgres,
            wait_on_enter=0,
            coordination_config=None,
            lock_provider=track_lock_provider)

        job.__enter__()

        try:
            assert len(callback_invoked) == 0, \
                'lock_provider should not be invoked when coordination disabled'

        finally:
            job.__exit__(None, None, None)


class TestLockFallbackPatterns:
    """Test lock fallback pattern matching in real cluster.
    """

    @clean_tables('Node', 'Lock', 'Token')
    def test_fallback_to_second_pattern(self, postgres):
        """Verify lock uses second pattern when first has no match.

        Mutation: find_nodes_matching_patterns returns the union of every
            pattern's matches, or uses only the first pattern.
        Oracle: special-node is the one node matching special-%; the union
            would tie-break to node1.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        now = datetime.datetime.now(datetime.timezone.utc)
        insert_active_node(postgres, tables, 'node1', created_on=now)
        insert_active_node(
            postgres,
            tables,
            'node2',
            created_on=now + datetime.timedelta(seconds=1))
        insert_active_node(
            postgres,
            tables,
            'special-node',
            created_on=now + datetime.timedelta(seconds=2))

        def register_fallback_locks(job) -> None:
            job.register_lock(
                'task-1',
                ['missing-%', 'special-%', 'node%'],
                'test fallback')

        coord_config = CoordinationConfig(total_tokens=30)
        job = create_job(
            'node1',
            postgres,
            wait_on_enter=5,
            coordination_config=coord_config,
            lock_provider=register_fallback_locks)
        job.__enter__()

        try:
            assert wait_for_running_state(job, timeout_sec=5)

            token_id = job.task_to_token('task-1')

            assigned_node = get_token_assignments(postgres, tables).get(token_id)

            assert assigned_node == 'special-node', \
                'Should use second pattern (special-%) when first (missing-%) has no match'

        finally:
            job.__exit__(None, None, None)

    @clean_tables('Node', 'Lock', 'Token')
    def test_fallback_to_third_pattern(self, postgres):
        """Verify lock falls back to third pattern when first two fail.

        Mutation: the fallback loop stops after the second pattern.
        Oracle: only node% matches, and node1 and node2 are its matches.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        now = datetime.datetime.now(datetime.timezone.utc)
        insert_active_node(postgres, tables, 'node1', created_on=now)
        insert_active_node(
            postgres,
            tables,
            'node2',
            created_on=now + datetime.timedelta(seconds=1))

        def register_fallback_locks(job) -> None:
            job.register_lock(
                'task-1',
                ['missing-%', 'also-missing-%', 'node%'],
                'three-level fallback')

        coord_config = CoordinationConfig(total_tokens=30)
        job = create_job(
            'node1',
            postgres,
            wait_on_enter=5,
            coordination_config=coord_config,
            lock_provider=register_fallback_locks)
        job.__enter__()

        try:
            assert wait_for_running_state(job, timeout_sec=5)

            token_id = job.task_to_token('task-1')

            assigned_node = get_token_assignments(postgres, tables).get(token_id)

            assert assigned_node in {'node1', 'node2'}, \
                'Should use third pattern (node%) when first two fail'

        finally:
            job.__exit__(None, None, None)

    @clean_tables('Node', 'Lock', 'Token')
    def test_all_fallback_patterns_fail(self, postgres):
        """Verify token not assigned when all fallback patterns fail.

        Mutation: categorize_tokens_by_locks puts a token whose patterns
            match no node into the distributable list.
        Oracle: no active node matches any of the three patterns.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        now = datetime.datetime.now(datetime.timezone.utc)
        insert_active_node(postgres, tables, 'node1', created_on=now)
        insert_active_node(
            postgres,
            tables,
            'node2',
            created_on=now + datetime.timedelta(seconds=1))

        def register_failed_fallback_locks(job) -> None:
            job.register_lock(
                'task-1',
                ['missing-%', 'also-missing-%', 'nope-%'],
                'all fail')

        coord_config = CoordinationConfig(total_tokens=30)
        job = create_job(
            'node1',
            postgres,
            wait_on_enter=5,
            coordination_config=coord_config,
            lock_provider=register_failed_fallback_locks)
        job.__enter__()

        try:
            assert wait_for_running_state(job, timeout_sec=5)

            token_id = job.task_to_token('task-1')

            assert token_id not in get_token_assignments(postgres, tables), \
                'Token should not be assigned when all patterns fail'

        finally:
            job.__exit__(None, None, None)

    @clean_tables('Node', 'Lock', 'Token')
    def test_first_pattern_used_when_matches(self, postgres):
        """Verify first pattern is used when it matches (no fallback needed).

        Mutation: find_nodes_matching_patterns skips the first pattern, or
            tries the patterns in reverse order.
        Oracle: primary-node is the one node matching primary-%.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        now = datetime.datetime.now(datetime.timezone.utc)
        insert_active_node(postgres, tables, 'primary-node', created_on=now)
        insert_active_node(
            postgres,
            tables,
            'backup-node',
            created_on=now + datetime.timedelta(seconds=1))

        def register_primary_locks(job) -> None:
            job.register_lock('task-1', ['primary-%', 'backup-%'], 'primary first')

        coord_config = CoordinationConfig(total_tokens=30)
        job = create_job(
            'primary-node',
            postgres,
            wait_on_enter=5,
            coordination_config=coord_config,
            lock_provider=register_primary_locks)
        job.__enter__()

        try:
            assert wait_for_running_state(job, timeout_sec=5)

            token_id = job.task_to_token('task-1')

            assigned_node = get_token_assignments(postgres, tables).get(token_id)

            assert assigned_node == 'primary-node', \
                'Should use first pattern (primary-%) when it matches'

        finally:
            job.__exit__(None, None, None)

    @clean_tables('Node', 'Lock', 'Token')
    def test_fallback_survives_node_death(self, postgres):
        """Verify a locked token moves to the fallback node after primary dies.

        Mutation: the dead-node redistribution keeps the token on its current
            owner, or ignores the fallback pattern and leaves it unowned.
        Oracle: backup-node is the one live node matching backup-%.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        now = datetime.datetime.now(datetime.timezone.utc)
        insert_active_node(postgres, tables, 'leader', created_on=now)
        insert_active_node(
            postgres,
            tables,
            'primary-node',
            created_on=now + datetime.timedelta(seconds=1))
        insert_active_node(
            postgres,
            tables,
            'backup-node',
            created_on=now + datetime.timedelta(seconds=2))

        def register_failover_locks(job) -> None:
            job.register_lock('task-1', ['primary-%', 'backup-%'], 'failover test')

        keep_alive = threading.Event()
        heartbeat_sql = f"""
update {tables["Node"]}
set last_heartbeat = now()
where name = 'backup-node'
"""

        def maintain_backup_heartbeat():
            """Keep backup-node alive with periodic heartbeat updates.
            """
            while not keep_alive.is_set():
                with postgres.connect() as conn:
                    conn.execute(text(heartbeat_sql))
                    conn.commit()
                keep_alive.wait(timeout=2)

        heartbeat_thread = threading.Thread(
            target=maintain_backup_heartbeat,
            daemon=True)
        heartbeat_thread.start()

        coord_config = CoordinationConfig(
            total_tokens=30,
            dead_node_check_interval_sec=1)
        leader = create_job(
            'leader',
            postgres,
            wait_on_enter=2,
            coordination_config=coord_config,
            lock_provider=register_failover_locks)
        leader.__enter__()

        try:
            assert wait_for_running_state(leader, timeout_sec=5)

            token_id = leader.task_to_token('task-1')

            initial_assignment = get_token_assignments(postgres, tables).get(token_id)

            assert initial_assignment == 'primary-node', \
                'Should initially use primary-node'

            stale_primary_sql = f"""
update {tables["Node"]}
set last_heartbeat = now() - interval '30 seconds'
where name = 'primary-node'
"""
            with postgres.connect() as conn:
                conn.execute(text(stale_primary_sql))
                conn.commit()

            assert wait_for_dead_node_removal(
                postgres,
                tables,
                'primary-node',
                timeout_sec=15)

            def token_owner():
                return get_token_assignments(postgres, tables).get(token_id)

            assert wait_for(lambda: token_owner() == 'backup-node', timeout_sec=20), \
                f'Should fallback to backup-node when primary-node dies, owner is {token_owner()}'

        finally:
            keep_alive.set()
            heartbeat_thread.join(timeout=1)
            leader.__exit__(None, None, None)

    @clean_tables('Node', 'Lock', 'Token')
    def test_multiple_locks_with_different_fallbacks(self, postgres):
        """Verify locks with different fallback lists resolve independently.

        Mutation: find_nodes_matching_patterns skips the first pattern, which
            sends task-3 to alpha-node.
        Oracle: each task's first matching pattern has exactly one node.
        """
        config = get_coordination_config()
        tables = schema.get_table_names(config.appname)

        now = datetime.datetime.now(datetime.timezone.utc)
        insert_active_node(postgres, tables, 'node1', created_on=now)
        insert_active_node(
            postgres,
            tables,
            'alpha-node',
            created_on=now + datetime.timedelta(seconds=1))
        insert_active_node(
            postgres,
            tables,
            'beta-node',
            created_on=now + datetime.timedelta(seconds=2))

        def register_multiple_fallbacks(job) -> None:
            job.register_lock('task-1', ['missing-%', 'alpha-%'], 'fallback to alpha')
            job.register_lock('task-2', ['missing-%', 'beta-%'], 'fallback to beta')
            job.register_lock('task-3', ['node%', 'alpha-%'], 'use node pattern')

        coord_config = CoordinationConfig(total_tokens=50)
        job = create_job(
            'node1',
            postgres,
            wait_on_enter=5,
            coordination_config=coord_config,
            lock_provider=register_multiple_fallbacks)
        job.__enter__()

        try:
            assert wait_for_running_state(job, timeout_sec=5)

            token_owners = get_token_assignments(postgres, tables)
            assignments = {}
            for task_id in ['task-1', 'task-2', 'task-3']:
                assignments[task_id] = token_owners.get(job.task_to_token(task_id))

            assert assignments['task-1'] == 'alpha-node', 'task-1 should use alpha-node'
            assert assignments['task-2'] == 'beta-node', 'task-2 should use beta-node'
            assert assignments['task-3'] == 'node1', \
                'task-3 should use node1 (first match)'

        finally:
            job.__exit__(None, None, None)


class TestConcurrentLeaderLockAcquisition:
    """Test concurrent leader lock acquisition from multiple nodes.
    """

    @clean_tables('LeaderLock')
    def test_only_one_node_acquires_leader_lock(self, postgres, jobs_to_exit):
        """Verify one of five simultaneous contenders gets the leader lock.

        Mutation: the LeaderLock insert uses ON CONFLICT DO UPDATE, or
            acquisition ignores result.rowcount.
        Oracle: five contenders released by one barrier. The winner holds
            the lock until the other four are refused.
        """
        lock_config = replace(get_coordination_config(), leader_lock_timeout_sec=0.2)

        jobs = [
            create_job(
                f'node{i}',
                postgres,
                coordination_config=lock_config,
                wait_on_enter=0)
            for i in range(1, 6)
            ]
        jobs_to_exit.extend(jobs)
        acquired_by = []
        refused_by = []
        outcome_lock = threading.Lock()
        all_refused = threading.Event()
        barrier = threading.Barrier(len(jobs))

        def try_acquire(job):
            barrier.wait()
            try:
                with job.locks.acquire_leader_lock('concurrent-test'):
                    with outcome_lock:
                        acquired_by.append(job.node_name)
                    all_refused.wait(timeout=30)
            except LockNotAcquired:
                with outcome_lock:
                    refused_by.append(job.node_name)
                    if len(refused_by) == len(jobs) - 1:
                        all_refused.set()

        threads = [threading.Thread(target=try_acquire, args=(job,)) for job in jobs]
        for t in threads:
            t.start()

        for t in threads:
            t.join()

        assert len(acquired_by) == 1, \
            f'Only 1 node should acquire lock, but {len(acquired_by)} did: {acquired_by}'
        assert len(refused_by) == 4, \
            f'The other 4 nodes should be refused, got {refused_by}'

    @clean_tables('LeaderLock')
    def test_leader_lock_released_after_operation(self, postgres):
        """Verify the leader lock row is deleted when the block ends or raises.

        Mutation: _release_leader_lock no longer runs in the finally clause,
            or deletes by a value other than this node's name.
        Oracle: an empty LeaderLock table after each block.
        """
        lock_config = get_coordination_config()
        tables = schema.get_table_names('sync_')

        job1 = create_job(
            'node1',
            postgres,
            coordination_config=lock_config,
            wait_on_enter=0)
        job1.__enter__()

        try:
            with job1.locks.acquire_leader_lock('operation-1'):
                pass

            with postgres.connect() as conn:
                result = conn.execute(
                    text(f'select count(*) from {tables["LeaderLock"]}'))
                lock_count = result.scalar()

            assert lock_count == 0, 'Lock should be released after context manager exit'

            with (
                pytest.raises(RuntimeError),
                job1.locks.acquire_leader_lock('operation-2')):
                raise RuntimeError('operation failed')

            with postgres.connect() as conn:
                result = conn.execute(
                    text(f'select count(*) from {tables["LeaderLock"]}'))
                lock_count = result.scalar()

            assert lock_count == 0, 'Lock should be released when the operation raises'

        finally:
            job1.__exit__(None, None, None)

    @clean_tables('LeaderLock')
    def test_second_node_waits_for_lock_release(self, postgres, jobs_to_exit):
        """Verify a blocked node gets the leader lock once the holder releases.

        Mutation: _try_acquire_leader_lock makes one INSERT attempt and gives
            up without retrying, or overwrites the holder's row.
        Oracle: release_holder, set one second after node2 starts waiting,
            inside node2's 15 s timeout.
        """
        lock_config = replace(get_coordination_config(), leader_lock_timeout_sec=15)

        job1 = create_job(
            'node1',
            postgres,
            coordination_config=lock_config,
            wait_on_enter=0)
        job2 = create_job(
            'node2',
            postgres,
            coordination_config=lock_config,
            wait_on_enter=0)
        jobs_to_exit.extend([job1, job2])
        holder_has_lock = threading.Event()
        release_holder = threading.Event()

        def node1_holds_lock():
            with job1.locks.acquire_leader_lock('long-operation'):
                holder_has_lock.set()
                release_holder.wait(timeout=10)

        holder = threading.Thread(target=node1_holds_lock)
        releaser = threading.Timer(1.0, release_holder.set)
        holder.start()

        try:
            assert holder_has_lock.wait(timeout=15), 'node1 should have acquired lock'
            releaser.start()

            with job2.locks.acquire_leader_lock('waiting-operation'):
                assert release_holder.is_set(), \
                    'node2 acquired the lock while node1 still held it'

        finally:
            release_holder.set()
            releaser.cancel()
            holder.join(timeout=10)

    @clean_tables('LeaderLock')
    def test_lock_holder_recorded_in_database(self, postgres):
        """Verify lock holder is recorded correctly in database.

        Mutation: the LeaderLock insert binds the operation or a constant in
            place of the node name.
        Oracle: the job's node name and the operation string passed in.
        """
        lock_config = get_coordination_config()
        tables = schema.get_table_names(lock_config.appname)

        job = create_job(
            'test-node',
            postgres,
            coordination_config=lock_config,
            wait_on_enter=0)
        job.__enter__()

        try:
            with job.locks.acquire_leader_lock('test-operation'):
                with postgres.connect() as conn:
                    result = conn.execute(
                        text(f'select node, operation from {tables["LeaderLock"]} where singleton = 1'))
                    lock_info = result.first()

                assert lock_info is not None, 'Lock record should exist'
                assert lock_info[0] == 'test-node', \
                    'Node should be recorded as lock holder'
                assert lock_info[1] == 'test-operation', 'Operation should be recorded'

        finally:
            job.__exit__(None, None, None)

    @clean_tables('LeaderLock')
    def test_sequential_acquisitions_after_release(self, postgres, jobs_to_exit):
        """Verify multiple nodes can acquire lock sequentially after release.

        Mutation: _release_leader_lock deletes nothing, so the next node
            times out.
        Oracle: three nodes, each expected to acquire in turn.
        """
        lock_config = replace(get_coordination_config(), leader_lock_timeout_sec=2)

        nodes = [
            create_job(
                f'node{i}',
                postgres,
                coordination_config=lock_config,
                wait_on_enter=0)
            for i in range(1, 4)
            ]
        jobs_to_exit.extend(nodes)

        for node in nodes:
            acquired = False
            try:
                with node.locks.acquire_leader_lock(f'operation-{node.node_name}'):
                    acquired = True
            except LockNotAcquired:
                pass

            assert acquired, \
                f'{node.node_name} should acquire lock after previous release'


if __name__ == '__main__':
    pytest.main(args=['-sx', __file__])
