"""Tests for RebalanceEvent and the on_rebalance arity-dispatch shim.

USE THIS FILE FOR:
- Unit tests of invoke_callback's backward-compatible arity detection
- Integration tests asserting the RebalanceEvent payload at the call sites
"""
import logging
import threading

import jobsync.client as jc
from jobsync import RebalanceEvent
from jobsync.client import CoordinationConfig, TokenDistributor

logger = logging.getLogger(__name__)


from fixtures import *  # noqa: F401, F403


SAMPLE = RebalanceEvent(is_initial=True, token_version=178, tokens_added=82, tokens_removed=0)


def run_callback(callback, event):
    """Dispatch a callback through invoke_callback and wait for the pool to drain.
    """
    distributor = TokenDistributor('node1', None, CoordinationConfig())
    distributor.invoke_callback(callback, event)
    distributor.shutdown_callbacks(wait=True, timeout=5)


class TestArityDispatch:
    """invoke_callback passes the event only to callbacks that accept one."""

    def test_one_arg_callback_receives_event(self):
        """A one-argument callable receives the RebalanceEvent.
        """
        received = []
        run_callback(lambda e: received.append(e), SAMPLE)
        assert received == [SAMPLE]

    def test_zero_arg_callback_invoked_without_event(self):
        """A legacy zero-argument callable is still invoked.
        """
        calls = []
        run_callback(lambda: calls.append(True), SAMPLE)
        assert calls == [True]

    def test_zero_arg_bound_method_invoked_without_event(self):
        """A bound method taking only self is treated as zero-argument.
        """
        class Tracker:
            def __init__(self):
                self.calls = []

            def on_rebalance(self):
                self.calls.append('hit')

        tracker = Tracker()
        run_callback(tracker.on_rebalance, SAMPLE)
        assert tracker.calls == ['hit']

    def test_one_arg_bound_method_receives_event(self):
        """A bound method taking one arg besides self receives the event.
        """
        class Consumer:
            def __init__(self):
                self.events = []

            def on_rebalance(self, event=None):
                self.events.append(event)

        consumer = Consumer()
        run_callback(consumer.on_rebalance, SAMPLE)
        assert consumer.events == [SAMPLE]

    def test_var_positional_callback_receives_event(self):
        """A *args callable is treated as accepting the event.
        """
        received = []
        run_callback(lambda *a: received.append(a), SAMPLE)
        assert received == [(SAMPLE,)]

    def test_signature_failure_falls_back_to_zero_arg(self, monkeypatch):
        """When the signature cannot be inspected, the callback is called with no args.
        """
        def boom(_):
            raise ValueError('no signature for builtin')

        monkeypatch.setattr(jc.inspect, 'signature', boom)
        calls = []
        run_callback(lambda: calls.append(True), SAMPLE)
        assert calls == [True]

    def test_callback_skipped_after_executor_shutdown(self):
        """A callback is not dispatched once the executor is shut down.
        """
        distributor = TokenDistributor('node1', None, CoordinationConfig())
        distributor.shutdown_callbacks(wait=True, timeout=5)
        calls = []
        distributor.invoke_callback(lambda e: calls.append(e), SAMPLE)
        assert calls == []


class EventTracker:
    """Capture RebalanceEvent objects delivered to on_rebalance."""

    def __init__(self):
        self.events = []
        self.lock = threading.Lock()

    def on_rebalance(self, event=None):
        """Record the event passed by jobsync.
        """
        with self.lock:
            self.events.append(event)


class TestRebalanceEventPayload:
    """The call sites build a correct RebalanceEvent."""

    def test_initial_assignment_event_is_initial(self, postgres):
        """Initial token assignment delivers is_initial=True with tokens added, none removed.
        """
        coord_cfg = get_coordination_config()
        tracker = EventTracker()

        with create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10,
                on_rebalance=tracker.on_rebalance) as job:
            assert wait_for(lambda: len(job.my_tokens) >= 1, timeout_sec=15)
            assert wait_for(lambda: len(tracker.events) >= 1, timeout_sec=5)

            event = tracker.events[0]
            assert isinstance(event, RebalanceEvent)
            assert event.is_initial is True
            assert event.tokens_added > 0
            assert event.tokens_removed == 0
            assert event.token_version >= 1

    def test_membership_change_event_not_initial(self, postgres):
        """A node losing tokens to a joiner gets a non-initial event with tokens removed.
        """
        coord_cfg = get_coordination_config()
        tracker = EventTracker()

        job1 = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10,
                  on_rebalance=tracker.on_rebalance)
        job1.__enter__()

        try:
            assert wait_for(lambda: len(job1.my_tokens) >= 30, timeout_sec=15)
            assert wait_for(lambda: len(tracker.events) >= 1, timeout_sec=5)
            assert tracker.events[0].is_initial is True

            with tracker.lock:
                tracker.events.clear()

            job2 = create_job('node2', postgres, coordination_config=coord_cfg, wait_on_enter=10)
            job2.__enter__()

            try:
                assert wait_for(lambda: len(job2.my_tokens) >= 1, timeout_sec=15)
                assert wait_for(
                    lambda: any(
                        e is not None and not e.is_initial and e.tokens_removed > 0
                        for e in tracker.events),
                    timeout_sec=15), 'node1 should receive a non-initial event with tokens removed'
            finally:
                job2.__exit__(None, None, None)
        finally:
            job1.__exit__(None, None, None)
