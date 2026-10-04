"""Tests for RebalanceEvent and the on_rebalance arity-dispatch shim.

USE THIS FILE FOR:
- Unit tests of invoke_callback's backward-compatible arity detection
- Integration tests asserting the RebalanceEvent payload at the call sites
"""
import threading

from fixtures import *  # noqa: F401, F403

import jobsync.client as jc
from jobsync import RebalanceEvent
from jobsync.client import CoordinationConfig, TokenDistributor

SAMPLE = RebalanceEvent(is_initial=True, token_version=178, tokens_added=82, tokens_removed=0)


def run_callback(callback, event):
    """Dispatch callback via invoke_callback and wait for the pool to drain.

    Parameters
    ----------
    callback : callable
        Callback under test. An exception it raises is logged by the
        distributor, never re-raised here.
    event : RebalanceEvent
        Event offered to the callback.
    """
    distributor = TokenDistributor('node1', None, CoordinationConfig())
    distributor.invoke_callback(callback, event)
    distributor.shutdown_callbacks(wait=True, timeout=5)


class TestArityDispatch:
    """invoke_callback passes the event only to callbacks that accept one."""

    def test_one_arg_callback_receives_event(self):
        """Verify a one-argument callable receives the RebalanceEvent.

        Mutation: pass_event forced False, so the callback gets no argument.
        Oracle: the SAMPLE event object passed in.
        """
        received = []
        run_callback(received.append, SAMPLE)
        assert received == [SAMPLE]

    def test_zero_arg_callback_invoked_without_event(self):
        """Verify a legacy zero-argument callable is still invoked.

        Mutation: invoke_callback always passes the event.
        Oracle: one recorded call.
        """
        calls = []
        run_callback(lambda: calls.append(True), SAMPLE)
        assert calls == [True]

    def test_zero_arg_bound_method_invoked_without_event(self):
        """Verify a bound method taking only self is treated as zero-argument.

        Mutation: the signature is read from the unbound function, so self
            counts as a positional parameter.
        Oracle: one recorded call.
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
        """Verify a bound method with a defaulted event parameter receives it.

        Mutation: parameters with a default are excluded from the arity check.
        Oracle: the SAMPLE event object passed in.
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
        """Verify a *args callable is treated as accepting the event.

        Mutation: VAR_POSITIONAL dropped from the accepted parameter kinds.
        Oracle: the one-tuple (SAMPLE,).
        """
        received = []
        run_callback(lambda *a: received.append(a), SAMPLE)
        assert received == [(SAMPLE,)]

    def test_signature_failure_falls_back_to_zero_arg(self, monkeypatch):
        """Verify an uninspectable callback is called with no arguments.

        Mutation: the except clause sets pass_event True, or is removed so
            the ValueError escapes invoke_callback.
        Oracle: one recorded call from a zero-argument callback.
        """
        def boom(_):
            raise ValueError('no signature for builtin')

        monkeypatch.setattr(jc.inspect, 'signature', boom)
        calls = []
        run_callback(lambda: calls.append(True), SAMPLE)
        assert calls == [True]

    def test_callback_skipped_after_executor_shutdown(self):
        """Verify a callback is not dispatched once the executor is shut down.

        Mutation: the _executor_shutdown guard removed, so submit raises
            RuntimeError on the closed pool.
        Oracle: zero recorded calls and no exception.
        """
        distributor = TokenDistributor('node1', None, CoordinationConfig())
        distributor.shutdown_callbacks(wait=True, timeout=5)
        calls = []
        distributor.invoke_callback(calls.append, SAMPLE)
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
        """Verify a lone node's first assignment delivers the exact event.

        Mutation: the initial RebalanceEvent counts tokens_added as the delta
            against my_tokens, which __enter__ has already filled.
        Oracle: a lone node owns all total_tokens at version 1, the first
            distribution on an empty Token table.
        """
        coord_cfg = get_coordination_config()
        tracker = EventTracker()

        with create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10,
                        on_rebalance=tracker.on_rebalance) as job:
            assert wait_for(lambda: len(job.my_tokens) >= 1, timeout_sec=15)
            assert wait_for(lambda: len(tracker.events) >= 1, timeout_sec=15)

            assert tracker.events[0] == RebalanceEvent(
                is_initial=True,
                token_version=1,
                tokens_added=coord_cfg.total_tokens,
                tokens_removed=0)

    def test_membership_change_event_not_initial(self, postgres):
        """Verify a node losing half its tokens to a joiner gets the event.

        Mutation: the non-initial event reports len(new_tokens) as
            tokens_added in place of len(added).
        Oracle: two nodes split total_tokens evenly, so node1 loses half and
            gains none, at version 2.
        """
        coord_cfg = get_coordination_config()
        tracker = EventTracker()

        job1 = create_job('node1', postgres, coordination_config=coord_cfg, wait_on_enter=10,
                          on_rebalance=tracker.on_rebalance)
        job1.__enter__()

        try:
            assert wait_for(lambda: len(job1.my_tokens) >= 30, timeout_sec=15)
            assert wait_for(lambda: len(tracker.events) >= 1, timeout_sec=15)
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
                assert tracker.events == [RebalanceEvent(
                    is_initial=False,
                    token_version=2,
                    tokens_added=0,
                    tokens_removed=coord_cfg.total_tokens // 2)]
            finally:
                job2.__exit__(None, None, None)
        finally:
            job1.__exit__(None, None, None)
