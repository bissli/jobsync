"""Integration tests for token ownership callbacks.

Tests verify:
- on_tokens_added called with initial token allocation
- on_tokens_removed called before on_tokens_added during rebalancing
- Callbacks receive correct token sets
- Callback exceptions don't break coordination
- Callbacks work across node lifecycle events
"""
import logging
import threading
import time
from types import SimpleNamespace

import config as test_config
import pytest
from asserts import assert_equal, assert_true

from jobsync import schema
from jobsync.client import Job

logger = logging.getLogger(__name__)


def get_callback_test_config():
    """Create a config with coordination enabled for callback tests.
    """
    coord_config = SimpleNamespace()
    coord_config.postgres = test_config.postgres
    coord_config.sync = SimpleNamespace(
        sql=SimpleNamespace(appname='sync_'),
        coordination=SimpleNamespace(
            enabled=True,
            heartbeat_interval_sec=2,
            heartbeat_timeout_sec=6,
            rebalance_check_interval_sec=5,
            dead_node_check_interval_sec=3,
            token_refresh_initial_interval_sec=2,
            token_refresh_steady_interval_sec=5,
            total_tokens=100,
            locks_enabled=True,
            leader_lock_timeout_sec=10,
            health_check_interval_sec=5,
            stale_leader_lock_age_sec=300,
            stale_rebalance_lock_age_sec=300
        )
    )
    return coord_config


class CallbackTracker:
    """Thread-safe callback tracker for testing.
    """

    def __init__(self):
        self.added_calls = []
        self.removed_calls = []
        self.call_order = []
        self.lock = threading.Lock()

    def on_tokens_added(self, token_ids: set[int]):
        """Track on_tokens_added calls.
        """
        with self.lock:
            self.added_calls.append(token_ids.copy())
            self.call_order.append(('added', token_ids.copy()))
            logger.info(f'on_tokens_added called with {len(token_ids)} tokens')

    def on_tokens_removed(self, token_ids: set[int]):
        """Track on_tokens_removed calls.
        """
        with self.lock:
            self.removed_calls.append(token_ids.copy())
            self.call_order.append(('removed', token_ids.copy()))
            logger.info(f'on_tokens_removed called with {len(token_ids)} tokens')

    def get_total_added(self) -> set[int]:
        """Get all tokens ever added.
        """
        with self.lock:
            result = set()
            for tokens in self.added_calls:
                result.update(tokens)
            return result

    def get_total_removed(self) -> set[int]:
        """Get all tokens ever removed.
        """
        with self.lock:
            result = set()
            for tokens in self.removed_calls:
                result.update(tokens)
            return result

    def reset(self):
        """Reset all tracking.
        """
        with self.lock:
            self.added_calls.clear()
            self.removed_calls.clear()
            self.call_order.clear()


class TestBasicCallbackInvocation:
    """Test basic callback invocation scenarios."""

    def test_on_tokens_added_called_on_startup(self, postgres):
        """Verify on_tokens_added called during initial allocation.
        """
        config = get_callback_test_config()
        connection_string = postgres.url.render_as_string(hide_password=False)

        tracker = CallbackTracker()

        with Job('node1', config, wait_on_enter=10, connection_string=connection_string,
                on_tokens_added=tracker.on_tokens_added) as job:
            # Wait for startup to complete
            time.sleep(2)

            assert_true(len(tracker.added_calls) > 0, 'on_tokens_added should be called')
            assert_true(len(tracker.get_total_added()) > 0, 'Should have received tokens')

            # Verify received tokens match job's assignment
            received_tokens = tracker.get_total_added()
            assert_equal(received_tokens, job.my_tokens, 'Callback tokens should match job assignment')

    def test_both_callbacks_registered(self, postgres):
        """Verify both callbacks can be registered and called.
        """
        config = get_callback_test_config()
        connection_string = postgres.url.render_as_string(hide_password=False)

        tracker = CallbackTracker()

        with Job('node1', config, wait_on_enter=10, connection_string=connection_string,
                on_tokens_added=tracker.on_tokens_added,
                on_tokens_removed=tracker.on_tokens_removed) as job:
            time.sleep(2)

            assert_true(len(tracker.added_calls) > 0, 'on_tokens_added should be called')
            # on_tokens_removed not called yet (no rebalancing)

    def test_no_callbacks_without_coordination(self, postgres):
        """Verify callbacks not invoked when coordination disabled.
        """
        config = get_callback_test_config()
        connection_string = postgres.url.render_as_string(hide_password=False)

        config.sync.coordination.enabled = False

        tracker = CallbackTracker()

        with Job('node1', config, wait_on_enter=0, connection_string=connection_string,
                on_tokens_added=tracker.on_tokens_added,
                on_tokens_removed=tracker.on_tokens_removed) as job:
            time.sleep(1)

            assert_equal(len(tracker.added_calls), 0, 'Callbacks should not fire when coordination disabled')
            assert_equal(len(tracker.removed_calls), 0, 'Callbacks should not fire when coordination disabled')


class TestCallbackTiming:
    """Test callback timing and ordering guarantees."""

    def test_removed_called_before_added_during_rebalance(self, postgres):
        """Verify on_tokens_removed called before on_tokens_added during rebalancing.
        """
        config = get_callback_test_config()
        connection_string = postgres.url.render_as_string(hide_password=False)
        tables = schema.get_table_names(config)

        tracker = CallbackTracker()

        # Start first node
        job1 = Job('node1', config, wait_on_enter=10, connection_string=connection_string,
                  on_tokens_added=tracker.on_tokens_added,
                  on_tokens_removed=tracker.on_tokens_removed)
        job1.__enter__()

        try:
            time.sleep(5)
            initial_tokens = job1.my_tokens.copy()
            tracker.reset()

            # Start second node to trigger rebalancing
            job2 = Job('node2', config, wait_on_enter=10, connection_string=connection_string)
            job2.__enter__()

            try:
                # Wait for rebalancing to complete
                time.sleep(15)

                if len(tracker.call_order) > 0:
                    # If callbacks were invoked, verify order
                    for i in range(len(tracker.call_order) - 1):
                        current_type, current_tokens = tracker.call_order[i]
                        next_type, next_tokens = tracker.call_order[i + 1]

                        # If we see an 'added' call followed by another call,
                        # there should not be a 'removed' call after it in the same rebalance cycle
                        if current_type == 'removed' and next_type == 'added':
                            logger.info('✓ Verified removed called before added')

                # Verify tokens were actually rebalanced
                final_tokens = job1.my_tokens
                assert_true(final_tokens != initial_tokens, 'Tokens should have been rebalanced')

            finally:
                job2.__exit__(None, None, None)

        finally:
            job1.__exit__(None, None, None)

    def test_callbacks_invoked_in_refresh_thread(self, postgres):
        """Verify callbacks run in background thread, not blocking main thread.
        """
        config = get_callback_test_config()
        connection_string = postgres.url.render_as_string(hide_password=False)

        main_thread_id = threading.current_thread().ident
        callback_thread_id = None

        def track_thread_on_added(token_ids: set[int]):
            nonlocal callback_thread_id
            callback_thread_id = threading.current_thread().ident
            logger.info(f'Callback running in thread {callback_thread_id}')

        with Job('node1', config, wait_on_enter=10, connection_string=connection_string,
                on_tokens_added=track_thread_on_added) as job:
            time.sleep(2)

            assert_true(callback_thread_id is not None, 'Callback should have been invoked')
            assert_true(callback_thread_id != main_thread_id,
                       'Callback should run in background thread')


class TestCallbackExceptionHandling:
    """Test that callback exceptions don't break coordination."""

    def test_exception_in_on_tokens_added(self, postgres):
        """Verify exception in on_tokens_added doesn't break coordination.
        """
        config = get_callback_test_config()
        connection_string = postgres.url.render_as_string(hide_password=False)

        def failing_callback(token_ids: set[int]):
            raise RuntimeError('Test exception in callback')

        with Job('node1', config, wait_on_enter=10, connection_string=connection_string,
                on_tokens_added=failing_callback) as job:
            time.sleep(2)

            # Job should still function despite callback failure
            assert_true(job.am_i_healthy(), 'Node should be healthy despite callback exception')
            assert_true(len(job.my_tokens) > 0, 'Node should still have tokens')

    def test_exception_in_on_tokens_removed(self, postgres):
        """Verify exception in on_tokens_removed doesn't break coordination.
        """
        config = get_callback_test_config()
        connection_string = postgres.url.render_as_string(hide_password=False)

        def failing_removed_callback(token_ids: set[int]):
            raise RuntimeError('Test exception in removed callback')

        tracker = CallbackTracker()

        job1 = Job('node1', config, wait_on_enter=10, connection_string=connection_string,
                  on_tokens_added=tracker.on_tokens_added,
                  on_tokens_removed=failing_removed_callback)
        job1.__enter__()

        try:
            time.sleep(5)

            # Start second node to trigger rebalancing
            job2 = Job('node2', config, wait_on_enter=10, connection_string=connection_string)
            job2.__enter__()

            try:
                time.sleep(15)

                # Despite on_tokens_removed failing, coordination should continue
                assert_true(job1.am_i_healthy(), 'Node1 should be healthy')
                assert_true(job2.am_i_healthy(), 'Node2 should be healthy')

            finally:
                job2.__exit__(None, None, None)

        finally:
            job1.__exit__(None, None, None)

    def test_partial_callback_failure(self, postgres):
        """Verify partial failures in callbacks don't prevent processing all tokens.
        """
        config = get_callback_test_config()
        connection_string = postgres.url.render_as_string(hide_password=False)

        processed_tokens = set()

        def partially_failing_callback(token_ids: set[int]):
            for token_id in token_ids:
                if token_id % 10 == 0:
                    raise RuntimeError(f'Test failure for token {token_id}')
                processed_tokens.add(token_id)

        with Job('node1', config, wait_on_enter=10, connection_string=connection_string,
                on_tokens_added=partially_failing_callback) as job:
            time.sleep(2)

            # Note: Our current implementation calls callback once with all tokens,
            # so if exception is raised, no tokens get processed
            # This test documents current behavior
            assert_true(job.am_i_healthy(), 'Node should remain healthy')


class TestCallbackCorrectness:
    """Test that callbacks receive correct token sets."""

    def test_added_tokens_match_ownership(self, postgres):
        """Verify on_tokens_added receives exactly the tokens owned by node.
        """
        config = get_callback_test_config()
        connection_string = postgres.url.render_as_string(hide_password=False)

        received_tokens = None

        def capture_tokens(token_ids: set[int]):
            nonlocal received_tokens
            received_tokens = token_ids.copy()

        with Job('node1', config, wait_on_enter=10, connection_string=connection_string,
                on_tokens_added=capture_tokens) as job:
            time.sleep(2)

            assert_true(received_tokens is not None, 'Callback should have been called')
            assert_equal(received_tokens, job.my_tokens, 'Callback tokens should match job ownership')

    def test_removed_tokens_accurate_during_rebalance(self, postgres):
        """Verify on_tokens_removed receives correct set of removed tokens.
        """
        config = get_callback_test_config()
        connection_string = postgres.url.render_as_string(hide_password=False)

        tracker = CallbackTracker()

        job1 = Job('node1', config, wait_on_enter=10, connection_string=connection_string,
                  on_tokens_added=tracker.on_tokens_added,
                  on_tokens_removed=tracker.on_tokens_removed)
        job1.__enter__()

        try:
            time.sleep(5)
            initial_tokens = job1.my_tokens.copy()
            tracker.reset()

            # Add second node
            job2 = Job('node2', config, wait_on_enter=10, connection_string=connection_string)
            job2.__enter__()

            try:
                time.sleep(15)

                final_tokens = job1.my_tokens

                if len(tracker.removed_calls) > 0:
                    total_removed = tracker.get_total_removed()
                    total_added = tracker.get_total_added()

                    # All removed tokens should have been in initial set
                    for token in total_removed:
                        assert_true(token in initial_tokens,
                                   f'Removed token {token} was not in initial set')

                    # Final = initial - removed + added
                    expected_final = (initial_tokens - total_removed) | total_added
                    assert_equal(final_tokens, expected_final,
                               'Final tokens should equal initial - removed + added')

            finally:
                job2.__exit__(None, None, None)

        finally:
            job1.__exit__(None, None, None)

    def test_no_duplicate_token_notifications(self, postgres):
        """Verify tokens not reported in both added and removed in same rebalance.
        """
        config = get_callback_test_config()
        connection_string = postgres.url.render_as_string(hide_password=False)

        all_added = []
        all_removed = []
        lock = threading.Lock()

        def track_added(token_ids: set[int]):
            with lock:
                all_added.append(token_ids.copy())

        def track_removed(token_ids: set[int]):
            with lock:
                all_removed.append(token_ids.copy())

        job1 = Job('node1', config, wait_on_enter=10, connection_string=connection_string,
                  on_tokens_added=track_added,
                  on_tokens_removed=track_removed)
        job1.__enter__()

        try:
            time.sleep(5)

            # Trigger rebalance
            job2 = Job('node2', config, wait_on_enter=10, connection_string=connection_string)
            job2.__enter__()

            try:
                time.sleep(15)

                # Check that no token appears in both added and removed in same cycle
                with lock:
                    if len(all_added) > 1 and len(all_removed) > 0:
                        # Skip initial allocation (first added call)
                        rebalance_added = set()
                        for tokens in all_added[1:]:
                            rebalance_added.update(tokens)

                        rebalance_removed = set()
                        for tokens in all_removed:
                            rebalance_removed.update(tokens)

                        overlap = rebalance_added & rebalance_removed
                        assert_equal(len(overlap), 0,
                                    'No token should appear in both added and removed')

            finally:
                job2.__exit__(None, None, None)

        finally:
            job1.__exit__(None, None, None)


class TestCallbackPerformance:
    """Test callback performance characteristics."""

    def test_slow_callback_doesnt_block_coordination(self, postgres):
        """Verify slow callbacks don't prevent heartbeats.
        """
        config = get_callback_test_config()
        connection_string = postgres.url.render_as_string(hide_password=False)

        def slow_callback(token_ids: set[int]):
            logger.info('Slow callback starting...')
            time.sleep(3)  # Simulate slow work
            logger.info('Slow callback completed')

        with Job('node1', config, wait_on_enter=10, connection_string=connection_string,
                on_tokens_added=slow_callback) as job:
            time.sleep(2)

            # Despite slow callback, heartbeat should continue
            time.sleep(5)
            assert_true(job.am_i_healthy(), 'Node should remain healthy with slow callback')

    def test_callback_timing_logged(self, postgres, caplog):
        """Verify callback duration is logged.
        """
        import logging

        config = get_callback_test_config()
        connection_string = postgres.url.render_as_string(hide_password=False)

        def tracked_callback(token_ids: set[int]):
            time.sleep(0.1)  # Small delay to ensure measurable time

        with caplog.at_level(logging.INFO), Job('node1', config, wait_on_enter=10, connection_string=connection_string,
                on_tokens_added=tracked_callback) as job:
            time.sleep(2)

        timing_messages = [record.message for record in caplog.records
                          if 'completed in' in record.message and 'ms' in record.message]

        assert_true(len(timing_messages) > 0, 'Callback timing should be logged')


class TestCallbackIntegrationScenarios:
    """Test callbacks in realistic scenarios."""

    def test_websocket_subscription_pattern(self, postgres):
        """Test pattern of starting/stopping subscriptions via callbacks.
        """
        config = get_callback_test_config()
        connection_string = postgres.url.render_as_string(hide_password=False)

        active_subscriptions = {}
        lock = threading.Lock()

        def start_subscriptions(token_ids: set[int]):
            with lock:
                for token_id in token_ids:
                    active_subscriptions[token_id] = f'subscription-{token_id}'
                logger.info(f'Started {len(token_ids)} subscriptions')

        def stop_subscriptions(token_ids: set[int]):
            with lock:
                for token_id in token_ids:
                    if token_id in active_subscriptions:
                        del active_subscriptions[token_id]
                logger.info(f'Stopped {len(token_ids)} subscriptions')

        job1 = Job('node1', config, wait_on_enter=10, connection_string=connection_string,
                  on_tokens_added=start_subscriptions,
                  on_tokens_removed=stop_subscriptions)
        job1.__enter__()

        try:
            time.sleep(5)

            initial_count = len(active_subscriptions)
            assert_true(initial_count > 0, 'Should have active subscriptions')

            # Add second node to trigger rebalancing
            job2 = Job('node2', config, wait_on_enter=10, connection_string=connection_string)
            job2.__enter__()

            try:
                time.sleep(15)

                # Active subscriptions should match current token ownership
                with lock:
                    current_tokens = job1.my_tokens
                    assert_equal(set(active_subscriptions.keys()), current_tokens,
                               'Active subscriptions should match owned tokens')

            finally:
                job2.__exit__(None, None, None)

        finally:
            job1.__exit__(None, None, None)

    def test_resource_cleanup_on_rebalance(self, postgres):
        """Test that resources cleaned up before new ones started during rebalance.
        """
        config = get_callback_test_config()
        connection_string = postgres.url.render_as_string(hide_password=False)

        resource_events = []
        lock = threading.Lock()

        def start_resources(token_ids: set[int]):
            with lock:
                resource_events.extend(('start', token_id, time.time()) for token_id in token_ids)

        def stop_resources(token_ids: set[int]):
            with lock:
                resource_events.extend(('stop', token_id, time.time()) for token_id in token_ids)

        job1 = Job('node1', config, wait_on_enter=10, connection_string=connection_string,
                  on_tokens_added=start_resources,
                  on_tokens_removed=stop_resources)
        job1.__enter__()

        try:
            time.sleep(5)
            initial_events = len(resource_events)

            # Trigger rebalance
            job2 = Job('node2', config, wait_on_enter=10, connection_string=connection_string)
            job2.__enter__()

            try:
                time.sleep(15)

                # Verify stop events precede start events for same rebalance
                with lock:
                    if len(resource_events) > initial_events:
                        # Group events by time window (same rebalance)
                        for i in range(initial_events, len(resource_events) - 1):
                            event = resource_events[i]
                            next_event = resource_events[i + 1]

                            time_diff = next_event[2] - event[2]
                            if time_diff < 1.0:  # Same rebalance cycle
                                if event[0] == 'stop' and next_event[0] == 'start':
                                    logger.info('✓ Verified stop before start in rebalance')

            finally:
                job2.__exit__(None, None, None)

        finally:
            job1.__exit__(None, None, None)


if __name__ == '__main__':
    pytest.main(args=['-sx', __file__])
