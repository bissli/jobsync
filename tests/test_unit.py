"""Unit tests for individual components and algorithms.

Scope
-----
- Pure unit tests with no database required (or minimal DB usage)
- Algorithm verification (hashing, distribution, pattern matching)
- Data structure validation
- State machine logic
- Configuration validation
- Edge case handling for individual methods
"""
import logging
import threading
import time

import pytest
from fixtures import *  # noqa: F401, F403
from sqlalchemy import text

from jobsync import schema
from jobsync.client import CoordinationConfig, CoordinationEvent, EventQueue
from jobsync.client import Job, JobState, JobStateMachine, Task
from jobsync.client import compute_minimal_move_distribution, matches_pattern
from jobsync.client import task_to_token

logger = logging.getLogger(__name__)


class TestStateTransitions:
    """Test valid and invalid state transitions.
    """

    def test_initial_state(self):
        """Verify a new state machine starts in INITIALIZING.

        Mutation: __init__ sets the initial state to CLUSTER_FORMING.
        Oracle: the JobState lifecycle, whose first state is INITIALIZING.
        """
        sm = JobStateMachine()
        assert sm.state == JobState.INITIALIZING, 'Should start in INITIALIZING state'

    def test_valid_transition_sequence(self):
        """Verify the follower path from INITIALIZING to SHUTTING_DOWN.

        Mutation: dropping a follower-path edge, e.g. ELECTING -> DISTRIBUTING.
        Oracle: the hand-written lifecycle order of JobState.
        """
        sm = JobStateMachine()

        assert sm.transition_to(JobState.CLUSTER_FORMING), \
            'Should transition to CLUSTER_FORMING'
        assert sm.state == JobState.CLUSTER_FORMING

        assert sm.transition_to(JobState.ELECTING), 'Should transition to ELECTING'
        assert sm.state == JobState.ELECTING

        assert sm.transition_to(JobState.DISTRIBUTING), \
            'Should transition to DISTRIBUTING'
        assert sm.state == JobState.DISTRIBUTING

        assert sm.transition_to(JobState.RUNNING_FOLLOWER), \
            'Should transition to RUNNING_FOLLOWER'
        assert sm.state == JobState.RUNNING_FOLLOWER

        assert sm.transition_to(JobState.SHUTTING_DOWN), \
            'Should transition to SHUTTING_DOWN'
        assert sm.state == JobState.SHUTTING_DOWN

    def test_leader_lifecycle_transitions(self):
        """Verify DISTRIBUTING -> RUNNING_LEADER -> SHUTTING_DOWN succeeds.

        Mutation: dropping the DISTRIBUTING -> RUNNING_LEADER or
            RUNNING_LEADER -> SHUTTING_DOWN edge.
        Oracle: the hand-written leader lifecycle order.
        """
        sm = JobStateMachine()

        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ELECTING)
        sm.transition_to(JobState.DISTRIBUTING)

        assert sm.transition_to(JobState.RUNNING_LEADER), \
            'Should transition to RUNNING_LEADER'
        assert sm.state == JobState.RUNNING_LEADER

        assert sm.transition_to(JobState.SHUTTING_DOWN), \
            'Should transition to SHUTTING_DOWN'
        assert sm.state == JobState.SHUTTING_DOWN

    def test_leader_follower_transitions(self):
        """Verify a follower promotes to leader and a leader demotes again.

        Mutation: dropping the RUNNING_FOLLOWER -> RUNNING_LEADER or
            RUNNING_LEADER -> RUNNING_FOLLOWER edge.
        Oracle: failover requires both directions between the running states.
        """
        sm = JobStateMachine()

        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ELECTING)
        sm.transition_to(JobState.DISTRIBUTING)
        sm.transition_to(JobState.RUNNING_FOLLOWER)

        assert sm.transition_to(JobState.RUNNING_LEADER), \
            'Follower should promote to leader'
        assert sm.state == JobState.RUNNING_LEADER

        assert sm.transition_to(JobState.RUNNING_FOLLOWER), \
            'Leader should demote to follower'
        assert sm.state == JobState.RUNNING_FOLLOWER

    def test_invalid_transition_rejected(self):
        """Verify INITIALIZING -> RUNNING_LEADER fails and keeps the state.

        Mutation: transition_to skips the valid_next_states check, or sets
            the state before returning False.
        Oracle: RUNNING_LEADER is reachable only from DISTRIBUTING or
            RUNNING_FOLLOWER.
        """
        sm = JobStateMachine()

        result = sm.transition_to(JobState.RUNNING_LEADER)
        assert not result, 'Should reject invalid transition'
        assert sm.state == JobState.INITIALIZING, \
            'State should not change on invalid transition'

    def test_skip_invalid_state_rejected(self):
        """Verify CLUSTER_FORMING -> DISTRIBUTING, skipping ELECTING, fails.

        Mutation: adding a CLUSTER_FORMING -> DISTRIBUTING edge.
        Oracle: the lifecycle order requires ELECTING before DISTRIBUTING.
        """
        sm = JobStateMachine()
        sm.transition_to(JobState.CLUSTER_FORMING)

        result = sm.transition_to(JobState.DISTRIBUTING)
        assert not result, 'Should reject skipping ELECTING state'
        assert sm.state == JobState.CLUSTER_FORMING, 'State should remain unchanged'

    def test_backward_transition_rejected(self):
        """Verify ELECTING -> CLUSTER_FORMING is rejected and keeps the state.

        Mutation: adding an ELECTING -> CLUSTER_FORMING (re-form) edge.
        Oracle: the lifecycle graph has no backward edges.
        """
        sm = JobStateMachine()
        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ELECTING)

        result = sm.transition_to(JobState.CLUSTER_FORMING)
        assert not result, 'Should reject backward transition'
        assert sm.state == JobState.ELECTING, 'State should remain unchanged'

    def test_transition_to_same_state_succeeds(self):
        """Verify a transition to the current state returns True.

        Mutation: removing the same-state short circuit, so the missing
            INITIALIZING -> INITIALIZING edge returns False.
        Oracle: transition_to's contract that a same-state call is a no-op.
        """
        sm = JobStateMachine()

        result = sm.transition_to(JobState.INITIALIZING)
        assert result, 'Transition to same state should succeed'
        assert sm.state == JobState.INITIALIZING

    @pytest.mark.parametrize(
        'from_state',
        [state for state in JobState if state != JobState.SHUTTING_DOWN])
    def test_shutting_down_from_any_state(self, from_state):
        """Verify SHUTTING_DOWN is reachable from every other state.

        Mutation: dropping any X -> SHUTTING_DOWN edge, e.g. from ELECTING.
        Oracle: every JobState member, enumerated apart from the graph.
        """
        sm = JobStateMachine()
        sm.state = from_state
        assert sm.transition_to(JobState.SHUTTING_DOWN), \
            f'Cannot shut down from {from_state.value}'
        assert sm.state == JobState.SHUTTING_DOWN


class TestCallbacks:
    """Test callback registration and invocation.
    """

    def test_on_enter_callback_invoked(self):
        """Verify the on_enter callback runs once on entering its state.

        Mutation: transition_to omits the on_enter call.
        Oracle: a call-recording callback.
        """
        sm = JobStateMachine()
        callback_invoked = []

        def callback():
            callback_invoked.append(True)

        sm.on_enter(JobState.CLUSTER_FORMING, callback)
        sm.transition_to(JobState.CLUSTER_FORMING)

        assert len(callback_invoked) == 1, 'Callback should be invoked once'

    def test_on_exit_callback_invoked(self):
        """Verify on_exit runs on leaving its state, and not on entering it.

        Mutation: on_exit callbacks looked up by new_state in place of the
            source state.
        Oracle: a call-recording callback, checked after each transition.
        """
        sm = JobStateMachine()
        callback_invoked = []

        def callback():
            callback_invoked.append(True)

        sm.on_exit(JobState.CLUSTER_FORMING, callback)
        sm.transition_to(JobState.CLUSTER_FORMING)

        assert len(callback_invoked) == 0, 'Exit callback not invoked yet'

        sm.transition_to(JobState.ELECTING)
        assert len(callback_invoked) == 1, 'Exit callback should be invoked'

    def test_multiple_transitions_invoke_callbacks(self):
        """Verify enter and exit callbacks of one state each run once.

        Mutation: on_enter and on_exit storing into the same callback dict.
        Oracle: hand-counted callback runs after each transition.
        """
        sm = JobStateMachine()
        enter_count = [0]
        exit_count = [0]

        def enter_callback():
            enter_count[0] += 1

        def exit_callback():
            exit_count[0] += 1

        sm.on_enter(JobState.CLUSTER_FORMING, enter_callback)
        sm.on_exit(JobState.CLUSTER_FORMING, exit_callback)

        sm.transition_to(JobState.CLUSTER_FORMING)
        assert enter_count[0] == 1, 'Enter callback invoked on entry'
        assert exit_count[0] == 0, 'Exit callback not invoked yet'

        sm.transition_to(JobState.ELECTING)
        assert enter_count[0] == 1, 'Enter callback not invoked again'
        assert exit_count[0] == 1, 'Exit callback invoked on exit'

    def test_callback_not_invoked_on_invalid_transition(self):
        """Verify a rejected transition runs no on_enter callback.

        Mutation: the on_enter call moved above the valid_next_states check.
        Oracle: INITIALIZING -> RUNNING_LEADER is not an edge.
        """
        sm = JobStateMachine()
        callback_invoked = []

        def callback():
            callback_invoked.append(True)

        sm.on_enter(JobState.RUNNING_LEADER, callback)
        sm.transition_to(JobState.RUNNING_LEADER)

        assert len(callback_invoked) == 0, \
            'Callback should not be invoked for invalid transition'

    def test_callback_not_invoked_on_same_state_transition(self):
        """Verify a same-state transition runs no enter or exit callback.

        Mutation: a same-state call treated as a valid transition, so it runs
            on_exit and on_enter.
        Oracle: transition_to's contract that a same-state call is a no-op.
        """
        sm = JobStateMachine()
        enter_count = [0]
        exit_count = [0]

        def enter_callback():
            enter_count[0] += 1

        def exit_callback():
            exit_count[0] += 1

        sm.on_enter(JobState.INITIALIZING, enter_callback)
        sm.on_exit(JobState.INITIALIZING, exit_callback)

        sm.transition_to(JobState.INITIALIZING)

        assert enter_count[0] == 0, \
            'Enter callback should not fire for same-state transition'
        assert exit_count[0] == 0, \
            'Exit callback should not fire for same-state transition'

    def test_callback_exception_handling(self):
        """Verify an on_enter exception propagates after the state changes.

        Mutation: transition_to swallows callback exceptions, or runs on_enter
            before assigning the new state.
        Oracle: a callback that always raises RuntimeError.
        """
        sm = JobStateMachine()

        def failing_callback():
            raise RuntimeError('Test exception')

        sm.on_enter(JobState.CLUSTER_FORMING, failing_callback)

        with pytest.raises(RuntimeError):
            sm.transition_to(JobState.CLUSTER_FORMING)

        assert sm.state == JobState.CLUSTER_FORMING, \
            'State should change despite callback exception'

    def test_exit_invoked_before_enter(self):
        """Verify on_exit of the source runs before on_enter of the target.

        Mutation: transition_to runs on_enter before on_exit.
        Oracle: call order recorded by the two callbacks.
        """
        sm = JobStateMachine()
        call_order = []

        def exit_callback():
            call_order.append('exit')

        def enter_callback():
            call_order.append('enter')

        sm.on_exit(JobState.CLUSTER_FORMING, exit_callback)
        sm.on_enter(JobState.ELECTING, enter_callback)

        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ELECTING)

        assert call_order == ['exit', 'enter'], 'Exit should be called before enter'


class TestCanClaimTask:
    """Test state-dependent task claiming behavior.
    """

    @pytest.mark.parametrize(
        ('state_path', 'expected'),
        [
            ([], False),
            ([JobState.CLUSTER_FORMING], False),
            ([JobState.CLUSTER_FORMING, JobState.ELECTING], False),
            ([
                JobState.CLUSTER_FORMING,
                JobState.ELECTING,
                JobState.DISTRIBUTING,
                ], False),
            ([
                JobState.CLUSTER_FORMING,
                JobState.ELECTING,
                JobState.DISTRIBUTING,
                JobState.RUNNING_FOLLOWER,
                ], True),
            ([
                JobState.CLUSTER_FORMING,
                JobState.ELECTING,
                JobState.DISTRIBUTING,
                JobState.RUNNING_LEADER,
                ], True),
            ([
                JobState.CLUSTER_FORMING,
                JobState.ELECTING,
                JobState.DISTRIBUTING,
                JobState.RUNNING_FOLLOWER,
                JobState.SHUTTING_DOWN,
                ], False),
            ])
    def test_can_claim_depends_on_state(self, state_path, expected):
        """Verify can_claim_task is True only in the two running states.

        Mutation: can_claim_task returns is_leader() alone, or
            `not is_initializing()` (True in SHUTTING_DOWN).
        Oracle: hand-labeled expected value per lifecycle path.
        """
        sm = JobStateMachine()
        for state in state_path:
            sm.transition_to(state)
        assert sm.can_claim_task() == expected, \
            f'can_claim_task should be {expected} in {sm.state}'

    def test_can_claim_follows_state_changes(self):
        """Verify can_claim_task tracks promotion, demotion and shutdown.

        Mutation: can_claim_task returns is_follower() alone, so the
            promoted leader cannot claim.
        Oracle: hand-labeled expected value after each transition.
        """
        sm = JobStateMachine()

        assert not sm.can_claim_task(), 'Initial state: cannot claim'

        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ELECTING)
        sm.transition_to(JobState.DISTRIBUTING)
        sm.transition_to(JobState.RUNNING_FOLLOWER)

        assert sm.can_claim_task(), 'Running state: can claim'

        sm.transition_to(JobState.RUNNING_LEADER)
        assert sm.can_claim_task(), 'Leader state: can claim'

        sm.transition_to(JobState.RUNNING_FOLLOWER)
        assert sm.can_claim_task(), 'Back to follower: can claim'

        sm.transition_to(JobState.SHUTTING_DOWN)
        assert not sm.can_claim_task(), 'Shutting down: cannot claim'


class TestErrorStateTransitions:
    """Test ERROR state transitions and behavior.
    """

    @pytest.mark.parametrize(
        'state_path',
        [
            [],
            [JobState.CLUSTER_FORMING],
            [JobState.CLUSTER_FORMING, JobState.ELECTING],
            [JobState.CLUSTER_FORMING, JobState.ELECTING, JobState.DISTRIBUTING],
            ])
    def test_error_state_reachable(self, state_path):
        """Verify ERROR is reachable from each pre-running state.

        Mutation: dropping any pre-running X -> ERROR edge, e.g. from ELECTING.
        Oracle: the four pre-running states, listed by hand.
        """
        sm = JobStateMachine()
        for state in state_path:
            sm.transition_to(state)
        result = sm.transition_to(JobState.ERROR)
        assert result, f'Should transition to ERROR from {sm.state}'
        assert sm.state == JobState.ERROR

    def test_error_to_shutting_down_transition(self):
        """Verify ERROR -> SHUTTING_DOWN succeeds.

        Mutation: dropping the ERROR -> SHUTTING_DOWN edge.
        Oracle: SHUTTING_DOWN is the only exit from ERROR.
        """
        sm = JobStateMachine()
        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ERROR)

        result = sm.transition_to(JobState.SHUTTING_DOWN)
        assert result, 'Should transition to SHUTTING_DOWN from ERROR'
        assert sm.state == JobState.SHUTTING_DOWN

    @pytest.mark.parametrize(
        'running_state',
        [
            JobState.RUNNING_LEADER,
            JobState.RUNNING_FOLLOWER,
            ])
    def test_cannot_transition_to_error_from_running(self, running_state):
        """Verify a running state rejects a transition to ERROR.

        Mutation: adding a RUNNING_LEADER or RUNNING_FOLLOWER -> ERROR edge.
        Oracle: ERROR is reachable only before the node runs.
        """
        sm = JobStateMachine()
        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ELECTING)
        sm.transition_to(JobState.DISTRIBUTING)
        sm.transition_to(running_state)

        result = sm.transition_to(JobState.ERROR)
        assert not result, f'Should not transition to ERROR from {running_state}'
        assert sm.state == running_state

    def test_is_error_returns_true_in_error_state(self):
        """Verify is_error() is False in INITIALIZING and True in ERROR.

        Mutation: is_error() compares against a state other than ERROR.
        Oracle: the state reached by an explicit transition to ERROR.
        """
        sm = JobStateMachine()
        assert not sm.is_error(), 'Should not be in error state initially'

        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ERROR)

        assert sm.is_error(), 'is_error() should return True in ERROR state'

    @pytest.mark.parametrize(
        'state',
        [state for state in JobState if state != JobState.ERROR])
    def test_is_error_returns_false_in_other_states(self, state):
        """Verify is_error() is False in every state other than ERROR.

        Mutation: is_error() returns `not is_initializing()`, True in the
            running states and SHUTTING_DOWN.
        Oracle: every JobState member except ERROR.
        """
        sm = JobStateMachine()
        sm.state = state
        assert not sm.is_error(), f'is_error() is True in {state.value}'

    def test_cannot_claim_tasks_in_error_state(self):
        """Verify can_claim_task is False in ERROR.

        Mutation: can_claim_task excludes only the initializing states and
            SHUTTING_DOWN, so ERROR can claim.
        Oracle: claiming is limited to the two running states.
        """
        sm = JobStateMachine()
        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ERROR)

        assert not sm.can_claim_task(), 'Cannot claim tasks in ERROR state'

    @pytest.mark.parametrize(
        'to_state',
        [
            state for state in JobState
            if state not in {JobState.ERROR, JobState.SHUTTING_DOWN}
            ])
    def test_error_state_exits_only_to_shutting_down(self, to_state):
        """Verify ERROR rejects a transition to every state but SHUTTING_DOWN.

        Mutation: adding a recovery edge out of ERROR, e.g. to CLUSTER_FORMING.
        Oracle: every JobState member except ERROR and SHUTTING_DOWN.
        """
        sm = JobStateMachine()
        sm.transition_to(JobState.ERROR)

        assert not sm.transition_to(to_state), \
            f'ERROR -> {to_state.value} was accepted'
        assert sm.state == JobState.ERROR

    def test_error_state_entry_callback_invoked(self):
        """Verify the on_enter callback for ERROR runs once on entering ERROR.

        Mutation: transition_to omits the on_enter call.
        Oracle: a call-recording callback.
        """
        sm = JobStateMachine()
        callback_invoked = []

        def error_callback():
            callback_invoked.append(True)

        sm.on_enter(JobState.ERROR, error_callback)
        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ERROR)

        assert len(callback_invoked) == 1, 'ERROR entry callback should be invoked'

    def test_error_state_exit_callback_invoked(self):
        """Verify the on_exit callback for ERROR runs once on leaving ERROR.

        Mutation: transition_to omits the on_exit call.
        Oracle: a call-recording callback.
        """
        sm = JobStateMachine()
        callback_invoked = []

        def error_exit_callback():
            callback_invoked.append(True)

        sm.on_exit(JobState.ERROR, error_exit_callback)
        sm.transition_to(JobState.ERROR)
        sm.transition_to(JobState.SHUTTING_DOWN)

        assert len(callback_invoked) == 1, 'ERROR exit callback should be invoked'

    @pytest.mark.parametrize(
        ('start_state', 'expect_error_visit'),
        [
            (JobState.CLUSTER_FORMING, True),
            (JobState.ELECTING, True),
            (JobState.DISTRIBUTING, True),
            (JobState.RUNNING_LEADER, False),
            (JobState.RUNNING_FOLLOWER, False),
            ])
    def test_node_unhealthy_shuts_down_without_invalid_transition(
            self, caplog, start_state, expect_error_visit):
        """Verify node_unhealthy passes through ERROR only where the graph allows.

        Mutation: the handler calling transition_to(ERROR) from every state,
            or dropping the ERROR step for the pre-running states.
        Oracle: HealthMonitor starts in CLUSTER_FORMING and runs through
            the running states; TestTransitionValidation's edge set has
            X -> ERROR only for the pre-running states.
        """
        job = Job('node1')
        job.state_machine.state = start_state
        error_visits = []
        job.state_machine.on_enter(JobState.ERROR, lambda: error_visits.append(True))

        job._event_queue.publish('node_unhealthy', {'reason': 'heartbeat_timeout'})
        with caplog.at_level(logging.ERROR, logger='jobsync.client'):
            job._coordinate_state_transitions()

        invalid_logs = [r for r in caplog.records if 'Invalid transition' in r.getMessage()]
        assert not invalid_logs, [r.getMessage() for r in invalid_logs]
        assert bool(error_visits) is expect_error_visit
        assert job.state_machine.state == JobState.SHUTTING_DOWN

    def test_refused_distributions_republish_one_retry_per_type(self, monkeypatch):
        """Verify a batch of refused distributions leaves one retry per type.

        Mutation: each refused membership_changed republished on its own, a
            refused dead_nodes_detected dropped, or a distribution attempted
            again in the batch after a refusal.
        Oracle: the user's spec for this change; one retry per event type
            per batch, and no further attempt in a batch after a refusal.
            A stub records each attempt and refuses it.
        """
        job = Job('node1')
        job.state_machine.state = JobState.RUNNING_LEADER
        attempts = []

        def refusing_distribute(trigger_reason='distribution'):
            attempts.append(trigger_reason)
            return False

        monkeypatch.setattr(job, '_distribute_tokens_safe', refusing_distribute)

        for current_count in (2, 3, 4):
            job._event_queue.publish(
                'membership_changed',
                {'previous_count': 1, 'current_count': current_count})
        job._event_queue.publish('dead_nodes_detected', {'nodes': ['ghost-1']})
        job._event_queue.publish('dead_nodes_detected', {'nodes': ['ghost-2']})

        job._coordinate_state_transitions()

        assert attempts == ['membership_change']
        retry_types = [event.type for event in job._event_queue.consume_all()]
        assert retry_types == ['membership_changed', 'dead_nodes_detected']


class TestTransitionValidation:
    """Test transition validation logic.
    """

    def test_transition_graph_matches_expected_edges(self):
        """Verify transition_to accepts exactly the expected edges.

        Mutation: adding an edge, e.g. ERROR -> INITIALIZING, or dropping
            one, e.g. ELECTING -> SHUTTING_DOWN.
        Oracle: the hand-written edge set below. Every other ordered pair
            of distinct states must be rejected.
        """
        sm = JobStateMachine()

        expected_transitions = {
            (JobState.INITIALIZING, JobState.CLUSTER_FORMING),
            (JobState.INITIALIZING, JobState.ERROR),
            (JobState.CLUSTER_FORMING, JobState.ELECTING),
            (JobState.CLUSTER_FORMING, JobState.ERROR),
            (JobState.ELECTING, JobState.DISTRIBUTING),
            (JobState.ELECTING, JobState.ERROR),
            (JobState.DISTRIBUTING, JobState.RUNNING_LEADER),
            (JobState.DISTRIBUTING, JobState.RUNNING_FOLLOWER),
            (JobState.DISTRIBUTING, JobState.ERROR),
            (JobState.RUNNING_FOLLOWER, JobState.RUNNING_LEADER),
            (JobState.RUNNING_LEADER, JobState.RUNNING_FOLLOWER),
            (JobState.RUNNING_LEADER, JobState.SHUTTING_DOWN),
            (JobState.RUNNING_FOLLOWER, JobState.SHUTTING_DOWN),
            (JobState.ERROR, JobState.SHUTTING_DOWN),
            (JobState.INITIALIZING, JobState.SHUTTING_DOWN),
            (JobState.CLUSTER_FORMING, JobState.SHUTTING_DOWN),
            (JobState.ELECTING, JobState.SHUTTING_DOWN),
            (JobState.DISTRIBUTING, JobState.SHUTTING_DOWN),
            }

        for from_state in JobState:
            for to_state in JobState:
                if from_state == to_state:
                    continue
                sm.state = from_state
                expected = (from_state, to_state) in expected_transitions
                assert sm.transition_to(to_state) == expected, \
                    f'{from_state.value} -> {to_state.value}: expected {expected}'


class TestTransitionThreadSafety:
    """Verify transition_to is atomic across threads.
    """

    def test_concurrent_identical_transitions_fire_exit_callback_once(self):
        """Verify two threads racing one transition run on_exit only once.

        Mutation: removing `with self._lock:` from transition_to, so both
            threads pass the check before either assigns the new state.
        Oracle: a barrier inside on_exit that holds the first thread until
            the second arrives or the barrier times out.
        """
        sm = JobStateMachine()
        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ELECTING)
        sm.transition_to(JobState.DISTRIBUTING)

        exit_fires = []
        exit_lock = threading.Lock()
        interleave_barrier = threading.Barrier(2, timeout=2.0)

        def on_exit_distributing():
            with exit_lock:
                exit_fires.append(threading.get_ident())
            try:
                interleave_barrier.wait()
            except threading.BrokenBarrierError:
                pass

        sm.on_exit(JobState.DISTRIBUTING, on_exit_distributing)

        def attempt():
            sm.transition_to(JobState.RUNNING_LEADER)

        t1 = threading.Thread(target=attempt)
        t2 = threading.Thread(target=attempt)
        t1.start()
        t2.start()
        t1.join(timeout=5)
        t2.join(timeout=5)
        assert not t1.is_alive() and not t2.is_alive(), 'transition_to hung'

        assert len(exit_fires) == 1, (
            f'on_exit(DISTRIBUTING) fired {len(exit_fires)} times under '
            f'concurrent transition_to(RUNNING_LEADER); expected exactly 1. '
            f'Indicates check-then-act race in transition_to.'
            )
        assert sm.state == JobState.RUNNING_LEADER


class TestEventQueue:
    """Test EventQueue thread-safe event handling.
    """

    def test_publish_single_event(self):
        """Verify a published event keeps its type and data.

        Mutation: publish storing {} in place of the data it was given.
        Oracle: the type and data this test passes to publish.
        """
        queue = EventQueue()
        queue.publish('test_event', {'key': 'value'})

        history = queue.get_history()
        assert len(history) == 1
        assert history[0].type == 'test_event'
        assert history[0].data == {'key': 'value'}

    def test_publish_multiple_events(self):
        """Verify multiple events maintain insertion order.

        Mutation: publish inserting at the front of the queue.
        Oracle: the publish order of event1, event2, event3.
        """
        queue = EventQueue()
        queue.publish('event1', {'id': 1})
        queue.publish('event2', {'id': 2})
        queue.publish('event3', {'id': 3})

        history = queue.get_history()
        assert len(history) == 3
        assert history[0].type == 'event1'
        assert history[1].type == 'event2'
        assert history[2].type == 'event3'

    def test_publish_without_data(self):
        """Verify publish defaults to empty dict when data omitted.

        Mutation: publish storing None when data is omitted.
        Oracle: the documented default of an empty dict.
        """
        queue = EventQueue()
        queue.publish('event_no_data')

        history = queue.get_history()
        assert len(history) == 1
        assert history[0].data == {}

    def test_consume_all_empty_queue(self):
        """Verify consume_all returns an empty list for an empty queue.

        Mutation: consume_all returning None when no event is pending.
        Oracle: a fresh queue holds no events.
        """
        queue = EventQueue()
        events = queue.consume_all()
        assert events == []

    def test_consume_all_multiple_events(self):
        """Verify consume_all returns all pending events in order, then clears.

        Mutation: consume_all keeping its events after returning them, or
            returning the live list that its own clear() then empties.
        Oracle: the publish order of event1, event2, event3.
        """
        queue = EventQueue()
        queue.publish('event1')
        queue.publish('event2')
        queue.publish('event3')

        events = queue.consume_all()
        assert [event.type for event in events] == ['event1', 'event2', 'event3']

        second_consume = queue.consume_all()
        assert second_consume == [], 'Second consume should return empty list'

    def test_get_history_does_not_clear_queue(self):
        """Verify get_history leaves pending events for consume_all.

        Mutation: get_history moving pending events into history, as
            consume_all does.
        Oracle: the two events this test publishes and never consumes.
        """
        queue = EventQueue()
        queue.publish('event1')
        queue.publish('event2')

        assert len(queue.get_history()) == 2
        assert [event.type for event in queue.consume_all()] == ['event1', 'event2']

    def test_get_history_with_limit_less_than_size(self):
        """Verify get_history with limit returns most recent events.

        Mutation: slicing the oldest events, all_events[:limit].
        Oracle: the last 3 of 5 events published, in publish order.
        """
        queue = EventQueue()
        queue.publish('event1')
        queue.publish('event2')
        queue.publish('event3')
        queue.publish('event4')
        queue.publish('event5')

        history = queue.get_history(limit=3)
        assert len(history) == 3
        assert history[0].type == 'event3', 'Should return most recent 3 events'
        assert history[1].type == 'event4'
        assert history[2].type == 'event5'

    def test_get_history_with_limit_equal_to_size(self):
        """Verify get_history returns every event when limit equals the count.

        Mutation: an off-by-one guard that returns [] unless limit is below
            the stored count.
        Oracle: limit at the threshold, the 2 events published.
        """
        queue = EventQueue()
        queue.publish('event1')
        queue.publish('event2')

        history = queue.get_history(limit=2)
        assert len(history) == 2
        assert history[0].type == 'event1'
        assert history[1].type == 'event2'

    def test_get_history_with_limit_greater_than_size(self):
        """Verify get_history returns every event when limit exceeds the count.

        Mutation: a guard that returns [] or raises when limit exceeds the
            stored count.
        Oracle: the 2 events published, below the limit of 10.
        """
        queue = EventQueue()
        queue.publish('event1')
        queue.publish('event2')

        history = queue.get_history(limit=10)
        assert len(history) == 2, 'Should return all available events'

    def test_get_history_with_zero_limit(self):
        """Verify get_history with zero limit returns empty list.

        Mutation: dropping the limit > 0 guard, so all_events[-0:] returns
            every event.
        Oracle: limit 0 asks for no events.
        """
        queue = EventQueue()
        queue.publish('event1')
        queue.publish('event2')

        history = queue.get_history(limit=0)
        assert history == [], 'Zero limit should return empty list'

    def test_event_has_timestamp(self):
        """Verify a published event carries its publish time.

        Mutation: CoordinationEvent.__post_init__ no longer stamping time.
        Oracle: time.time() read on either side of publish.
        """
        queue = EventQueue()
        before = time.time()
        queue.publish('test_event')
        after = time.time()

        history = queue.get_history()
        assert before <= history[0].timestamp <= after

    def test_get_history_returns_copy(self):
        """Verify appending to the get_history result leaves the queue as is.

        Mutation: get_history returning the live pending-event list.
        Oracle: the 2 events published before the caller appends a third.
        """
        queue = EventQueue()
        queue.publish('event1')
        queue.publish('event2')

        history = queue.get_history()
        history.append(CoordinationEvent('fake', {}))

        new_history = queue.get_history()
        assert len(new_history) == 2, 'Modified copy should not affect queue'


class TestEventQueueWithHistory:
    """Test EventQueue ring buffer and history persistence.
    """

    def test_history_persists_after_consume(self):
        """Verify consumed events are retained in history.

        Mutation: consume_all dropping its history.extend, so consumed
            events vanish from get_history.
        Oracle: the 2 events published and then consumed.
        """
        queue = EventQueue(history_size=10)
        queue.publish('event1', {'id': 1})
        queue.publish('event2', {'id': 2})

        consumed = queue.consume_all()
        assert len(consumed) == 2

        history = queue.get_history()
        assert len(history) == 2
        assert history[0].type == 'event1'
        assert history[1].type == 'event2'

    def test_get_history_includes_unprocessed_events(self):
        """Verify get_history returns both processed and unprocessed events.

        Mutation: get_history returning only the consumed history, or only
            the pending events.
        Oracle: event1 consumed, then event2 and event3 left pending.
        """
        queue = EventQueue(history_size=10)

        queue.publish('event1')
        queue.consume_all()

        queue.publish('event2')
        queue.publish('event3')

        history = queue.get_history()
        assert len(history) == 3
        assert history[0].type == 'event1'
        assert history[1].type == 'event2'
        assert history[2].type == 'event3'

    def test_get_history_with_limit_after_consume(self):
        """Verify limit applies to consumed history when nothing is pending.

        Mutation: limit applied to the pending events only.
        Oracle: the last 3 of 10 consumed events, in publish order.
        """
        queue = EventQueue(history_size=20)

        for i in range(10):
            queue.publish(f'event{i}')
        queue.consume_all()

        history = queue.get_history(limit=3)
        assert len(history) == 3
        assert history[0].type == 'event7'
        assert history[1].type == 'event8'
        assert history[2].type == 'event9'

    def test_multiple_consume_cycles(self):
        """Verify history accumulates across multiple consume cycles.

        Mutation: consume_all replacing history with its latest batch.
        Oracle: one event published and consumed per cycle, three cycles.
        """
        queue = EventQueue(history_size=20)

        queue.publish('event1')
        queue.consume_all()

        queue.publish('event2')
        queue.consume_all()

        queue.publish('event3')
        queue.consume_all()

        history = queue.get_history()
        assert len(history) == 3
        assert [e.type for e in history] == ['event1', 'event2', 'event3']

    def test_history_overflow_drops_oldest(self):
        """Verify history keeps the newest history_size events once full.

        Mutation: history_size ignored, or the buffer sized one off from it.
        Oracle: history_size=3 with 4 events consumed, so event1 drops.
        """
        queue = EventQueue(history_size=3)

        queue.publish('event1')
        queue.publish('event2')
        queue.publish('event3')
        queue.consume_all()

        queue.publish('event4')
        queue.consume_all()

        history = queue.get_history()
        assert len(history) == 3
        assert history[0].type == 'event2'
        assert history[1].type == 'event3'
        assert history[2].type == 'event4'

    def test_get_history_limit_with_mixed_events(self):
        """Verify limit spans the consumed history and the pending events.

        Mutation: limit applied to history and pending events separately.
        Oracle: the last 4 of 5 consumed plus 3 pending events.
        """
        queue = EventQueue(history_size=20)

        for i in range(5):
            queue.publish(f'consumed{i}')
        queue.consume_all()

        for i in range(3):
            queue.publish(f'unconsumed{i}')

        history = queue.get_history(limit=4)
        assert len(history) == 4
        assert history[0].type == 'consumed4'
        assert history[1].type == 'unconsumed0'
        assert history[2].type == 'unconsumed1'
        assert history[3].type == 'unconsumed2'

    def test_history_thread_safe(self):
        """Verify concurrent publish and consume_all lose or repeat no event.

        Mutation: consume_all without the lock, so an event published
            between its copy and its clear is dropped.
        Oracle: the event names the publishers send, each seen once in the
            consumed batches and once in history.
        """
        publisher_cnt, per_publisher = 8, 2500
        expected = sorted(
            f'p{k}-{i}' for k in range(publisher_cnt) for i in range(per_publisher))

        # One round lets the unlocked race slip through now and then, so
        # three rounds make the mutant fail reliably.
        for _ in range(3):
            queue = EventQueue(history_size=len(expected))
            consumed, errors = [], []
            publishers_done = threading.Event()

            def publisher(k):
                for i in range(per_publisher):
                    queue.publish(f'p{k}-{i}')

            def consumer():
                try:
                    while not publishers_done.is_set():
                        consumed.extend(queue.consume_all())
                except Exception as e:
                    errors.append(e)

            def reader():
                try:
                    for _ in range(50):
                        queue.get_history(limit=10)
                        time.sleep(0.001)
                except Exception as e:
                    errors.append(e)

            publishers = [
                threading.Thread(target=publisher, args=(k,))
                for k in range(publisher_cnt)
                ]
            others = [
                threading.Thread(target=consumer),
                threading.Thread(target=reader),
                ]
            for t in others + publishers:
                t.start()
            for t in publishers:
                t.join()
            publishers_done.set()
            for t in others:
                t.join()
            consumed.extend(queue.consume_all())

            assert errors == [], f'Thread safety errors: {errors}'
            assert sorted(e.type for e in consumed) == expected
            assert sorted(e.type for e in queue.get_history()) == expected


class TestJobCoordinationStatus:
    """Test Job.get_coordination_status() debugging API.
    """

    def test_coordination_disabled_returns_minimal_info(self):
        """Verify status when coordination is disabled.

        Mutation: the disabled branch removed, so status reads the None
            cluster and token objects.
        Oracle: the single-key dict a standalone job reports.
        """
        job = Job('test-node', coordination_config=None)
        status = job.get_coordination_status()

        assert status == {'coordination_enabled': False}

    def test_coordination_enabled_returns_full_status(self, postgres):
        """Verify a leader with one peer reports its own share of the cluster.

        Mutation: my_tokens reporting total_tokens, active_nodes counting
            only this node, token_version left at 0, or state reported as
            the enum in place of its value.
        Oracle: node1's token rows and version in the Token table, and the
            two node rows this test registers.
        """
        coord_config = get_coordination_config(
            total_tokens=100,
            heartbeat_timeout_sec=60)
        job = create_job('node1', postgres, coordination_config=coord_config)
        tables = schema.get_table_names('sync_')
        insert_active_node(postgres, tables, 'node2')

        with job:
            status = job.get_coordination_status()

            version_sql = f'select distinct version from {tables["Token"]} where node = :node'
            with postgres.connect() as conn:
                rows = conn.execute(text(version_sql), {'node': 'node1'})
                versions = rows.scalars().all()
            assignments = get_token_assignments(postgres, tables)
            node1_tokens = [
                token_id for token_id, node in assignments.items() if node == 'node1'
                ]
            assert 0 < len(node1_tokens) < 100, 'node2 should own part of the tokens'
            assert len(versions) == 1

            assert status['coordination_enabled'] is True
            assert status['node_name'] == 'node1'
            assert status['state'] == 'running_leader'
            assert status['is_leader'] is True
            assert status['my_tokens'] == len(node1_tokens)
            assert status['token_version'] == versions[0]
            assert status['total_tokens'] == 100
            assert status['active_nodes'] == 2
            assert status['last_heartbeat'] is not None

    def test_follower_reports_not_leader(self, postgres):
        """Verify a follower reports is_leader False and its follower state.

        Mutation: is_leader computed as is_running(), which a leader alone
            cannot tell apart from is_leader().
        Oracle: node0 registered before node1 exists, so node1 follows, and
            the 3 tokens this test assigns to node1.
        """
        coord_config = get_coordination_config(
            total_tokens=10,
            heartbeat_timeout_sec=60)
        tables = schema.get_table_names('sync_')
        insert_active_node(postgres, tables, 'node0')
        for token_id in range(3):
            insert_token(postgres, tables, token_id, 'node1')
        job = create_job('node1', postgres, coordination_config=coord_config)

        with job:
            status = job.get_coordination_status()

            assert status['state'] == 'running_follower'
            assert status['is_leader'] is False
            assert status['my_tokens'] == 3

    def test_recent_events_limited_to_20(self, postgres):
        """Verify recent_events holds the 20 newest events, oldest first.

        Mutation: a limit other than 20, or the oldest events kept in place
            of the newest.
        Oracle: event10 to event29, the last 20 of 30 events published.
        """
        coord_config = get_coordination_config()
        job = create_job(
            'node1',
            postgres,
            coordination_config=coord_config,
            wait_on_enter=0)

        with job:
            for i in range(30):
                job._event_queue.publish(f'event{i}')

            status = job.get_coordination_status()

            recent_types = [event['type'] for event in status['recent_events']]
            assert recent_types == [f'event{i}' for i in range(10, 30)]

    def test_recent_events_includes_metadata(self, postgres):
        """Verify event entries include type, timestamp, and data.

        Mutation: an entry dropping a key, or filling data or timestamp from
            the wrong event field.
        Oracle: the type and data published, and time.time() read on
            either side of publish.
        """
        coord_config = get_coordination_config()
        job = create_job(
            'node1',
            postgres,
            coordination_config=coord_config,
            wait_on_enter=0)

        with job:
            before = time.time()
            job._event_queue.publish('test_event', {'key': 'value'})
            after = time.time()

            status = job.get_coordination_status()

            event = status['recent_events'][-1]
            assert set(event) == {'type', 'timestamp', 'data'}
            assert event['type'] == 'test_event'
            assert event['data'] == {'key': 'value'}
            assert before <= event['timestamp'] <= after

    def test_monitors_list_present(self, postgres):
        """Verify monitors lists the names of the running monitors.

        Mutation: monitors listing the monitor objects in place of their
            names, or returning the dict view in place of a list.
        Oracle: every coordinated job starts a monitor named coordination.
        """
        coord_config = get_coordination_config()
        job = create_job(
            'node1',
            postgres,
            coordination_config=coord_config,
            wait_on_enter=0)

        with job:
            status = job.get_coordination_status()

            assert isinstance(status['monitors'], list)
            assert 'coordination' in status['monitors']

    def test_coordination_thread_starts_on_enter(self, postgres):
        """Verify an unentered coordinated job runs no coordination thread.

        Mutation: the CoordinationMonitor started in Job.__init__, so a job
            built and never entered polls until process exit.
        Oracle: the Job lifecycle docstring, whose INITIALIZING phase only
            configures instance variables, checked against the thread list
            from threading.enumerate() before and after construction.
        """
        before = set(threading.enumerate())
        job = create_job(
            'node1',
            postgres,
            coordination_config=get_coordination_config(),
            wait_on_enter=0)
        try:
            started = {t.name for t in set(threading.enumerate()) - before}
            assert 'coordination' not in started

            with job:
                coordination_thread = job._monitors['coordination'].thread
                assert coordination_thread.is_alive()
            assert not coordination_thread.is_alive()
        finally:
            job.__exit__(None, None, None)


class TestTaskTokenMapping:
    """Test task-to-token mapping and related methods.
    """

    @pytest.mark.parametrize(
        ('hash_function', 'int_token', 'str_token'),
        [
            ('md5', 5808, 8672),
            ('sha256', 6717, 3581),
            ('double_sha256', 107, 9302),
            ])
    def test_consistent_hashing(self, hash_function, int_token, str_token):
        """Verify every process maps a task ID to the same pinned token.

        Mutation: Python's per-process hash() in place of the digest, a
            changed digest slice or byte order, or ints hashed apart from
            their str() form.
        Oracle: tokens worked out with hashlib by hand for 10000 tokens.
        """
        assert task_to_token(123, 10000, hash_function) == int_token
        assert task_to_token('123', 10000, hash_function) == int_token
        assert task_to_token('test-task-123', 10000, hash_function) == str_token

    @pytest.mark.parametrize('hash_function', ['md5', 'sha256', 'double_sha256'])
    def test_task_to_token_matches_module_function(self, postgres, hash_function):
        """Verify Job.task_to_token() uses the configured tokens and hash.

        Mutation: Job.task_to_token dropping the configured hash_function
            or total_tokens for the module defaults.
        Oracle: the module-level task_to_token with the configured values.
        """
        coord_config = CoordinationConfig(total_tokens=100, hash_function=hash_function)
        job = create_job(
            'node1',
            postgres,
            coordination_config=coord_config,
            wait_on_enter=0)

        with job:
            for task_id in [0, 1, 99, 'string-task', 'another-task']:
                job_result = job.task_to_token(task_id)
                module_result = task_to_token(task_id, 100, hash_function)
                assert job_result == module_result, \
                    f'Results should match for task {task_id} ({hash_function})'

    @pytest.mark.parametrize('hash_function', ['md5', 'sha256', 'double_sha256'])
    def test_distribution_quality_with_clustered_ids(self, hash_function):
        """Verify sequential task IDs spread over distinct tokens.

        Mutation: the token taken from too few digest bits, such as one
            byte, so tokens collide.
        Oracle: 1000 IDs into 10000 tokens leave about 952 distinct by the
            birthday bound.
        """
        task_ids = range(20001, 21001)
        tokens = [task_to_token(tid, 10000, hash_function) for tid in task_ids]

        unique_tokens = len(set(tokens))
        assert unique_tokens >= 950, \
            f'1000 clustered tasks should use >=950 unique tokens, got {unique_tokens} ({hash_function})'

    @pytest.mark.parametrize('hash_function', ['md5', 'sha256', 'double_sha256'])
    def test_distribution_quality_across_token_space(self, hash_function):
        """Verify tokens spread evenly across the full token range.

        Mutation: the token taken from too few digest bits, or from the
            task ID itself, so tokens crowd the low buckets.
        Oracle: 1000 tasks over 10 equal buckets, 100 expected in each.
        """
        task_ids = range(0, 5000, 5)
        tokens = [task_to_token(tid, 10000, hash_function) for tid in task_ids]

        buckets = [0] * 10
        for token in tokens:
            buckets[token // 1000] += 1

        imbalance = max(buckets) - min(buckets)
        expected_per_bucket = len(tokens) / 10

        logger.debug(f'Token bucket distribution ({hash_function}): {buckets}')

        assert imbalance <= expected_per_bucket * 0.40, \
            f'Bucket imbalance {imbalance} exceeds 40% of expected {expected_per_bucket:.1f} ({hash_function})'

    @pytest.mark.parametrize('hash_function', ['md5', 'sha256', 'double_sha256'])
    def test_distribution_with_string_ids(self, hash_function):
        """Verify string task IDs spread over distinct, evenly filled tokens.

        Mutation: the token taken from too few digest bits, so string IDs
            collide and crowd the low buckets.
        Oracle: 500 IDs into 10000 tokens leave about 488 distinct by the
            birthday bound, and 100 expected in each of 5 buckets.
        """
        task_ids = [f'task-{i:05d}' for i in range(500)]
        tokens = [task_to_token(tid, 10000, hash_function) for tid in task_ids]

        unique_tokens = len(set(tokens))
        assert unique_tokens >= 450, \
            f'500 string tasks should use >=450 unique tokens, got {unique_tokens} ({hash_function})'

        buckets = [0] * 5
        for token in tokens:
            buckets[token // 2000] += 1

        imbalance = max(buckets) - min(buckets)
        expected_per_bucket = len(tokens) / 5

        assert imbalance <= expected_per_bucket * 0.3, \
            f'String ID bucket imbalance {imbalance} exceeds 30% threshold ({hash_function})'

    @pytest.mark.parametrize('hash_function', ['md5', 'sha256', 'double_sha256'])
    @pytest.mark.parametrize(
        'task_id',
        [
            0,
            -1,
            -999999,
            999999999,
            '',
            'unicode-\u03c4\u03b5\u03c3\u03c4-\u65e5\u672c',
            None,
            (1, 2, 3),
            ])
    def test_edge_case_task_ids(self, task_id, hash_function):
        """Verify unusual hashable task IDs map to a token in range.

        Mutation: the ID encoded as ASCII, or hashed without str(), so a
            non-ASCII, None, or tuple ID raises.
        Oracle: the valid token range 0 to 9999.
        """
        token = task_to_token(task_id, 10000, hash_function)
        assert 0 <= token < 10000


class TestCoordinationConfig:
    """Test CoordinationConfig validation and consistency.
    """

    def test_task_mapping_defaults_pinned(self):
        """Verify the defaults that fix the task-to-token mapping stay put.

        Mutation: total_tokens or hash_function default changed, so nodes
            on two releases map one task to different tokens.
        Oracle: 10000 tokens and double_sha256, the defaults README.md and
            docs/USAGE_GUIDE.md document.
        """
        config = CoordinationConfig()
        assert config.total_tokens == 10000
        assert config.hash_function == 'double_sha256'

    def test_heartbeat_timeout_greater_than_interval(self):
        """Verify the default timeout outlasts one missed heartbeat.

        Mutation: heartbeat_timeout_sec default cut to 2 intervals or less.
        Oracle: heartbeat_interval_sec, the default send interval.
        """
        config = CoordinationConfig()
        assert config.heartbeat_timeout_sec > 2 * config.heartbeat_interval_sec, \
            'Timeout should outlast one missed heartbeat'

    def test_token_refresh_steady_exceeds_initial(self):
        """Verify the steady refresh interval is no shorter than the initial.

        Mutation: the two refresh interval defaults swapped.
        Oracle: token_refresh_initial_interval_sec, the default used while
            a node starts.
        """
        config = CoordinationConfig()
        steady_sec = config.token_refresh_steady_interval_sec
        initial_sec = config.token_refresh_initial_interval_sec
        assert steady_sec >= initial_sec, \
            'Steady interval should be >= initial interval'

    def test_stale_lock_ages_are_reasonable(self):
        """Verify a leader lock counts as stale only long after its timeout.

        Mutation: stale_leader_lock_age_sec default cut below ten lock
            timeouts.
        Oracle: leader_lock_timeout_sec, the default acquisition window.
        """
        config = CoordinationConfig()
        lock_timeout_sec = config.leader_lock_timeout_sec
        assert config.stale_leader_lock_age_sec >= 10 * lock_timeout_sec, \
            'Stale lock age should be much greater than lock timeout'


class TestTaskComparison:
    """Test Task comparison and sorting operations.
    """

    def test_task_equality(self):
        """Verify tasks with same ID are equal.

        Mutation: __eq__ comparing names, or object identity.
        Oracle: two tasks sharing id 1 under different names.
        """
        task1 = create_task(1, 'task_a')
        task2 = create_task(1, 'task_b')
        assert task1 == task2

    def test_task_inequality(self):
        """Verify tasks with different IDs are unequal even with one name.

        Mutation: __eq__ comparing names in place of IDs.
        Oracle: ids 1 and 2 under the same name.
        """
        task1 = create_task(1, 'same_name')
        task2 = create_task(2, 'same_name')
        assert task1 != task2

    @pytest.mark.parametrize(
        ('id1', 'id2', 'expected_lt', 'expected_gt'),
        [
            (1, 2, True, False),
            (2, 1, False, True),
            (1, 1, False, False),
            ])
    def test_task_comparisons(self, id1, id2, expected_lt, expected_gt):
        """Verify < and > order tasks by ID and are both false on a tie.

        Mutation: __gt__ written as not __lt__, or either operator flipped.
        Oracle: integer order of the ids, with a tie at 1, 1.
        """
        task1 = create_task(id1, f'task_{id1}')
        task2 = create_task(id2, f'task_{id2}')

        assert (task1 < task2) == expected_lt
        assert (task1 > task2) == expected_gt

    def test_task_sorting(self):
        """Verify sorted() orders tasks by ascending ID.

        Mutation: __lt__ comparing in reverse or by name.
        Oracle: ids 1, 2, 3 given out of order with names in reverse order.
        """
        task3 = create_task(3, 'task_a')
        task1 = create_task(1, 'task_c')
        task2 = create_task(2, 'task_b')
        tasks = [task3, task1, task2]
        sorted_tasks = sorted(tasks)
        assert [task.id for task in sorted_tasks] == [1, 2, 3]

    def test_task_string_ids(self):
        """Verify tasks with string IDs compare in string order.

        Mutation: __lt__ or __gt__ casting IDs to int, which raises on 'a'.
        Oracle: 'a' sorts before 'b'.
        """
        task_a = create_task('a', 'task_a')
        task_b = create_task('b', 'task_b')
        assert task_a < task_b
        assert task_b > task_a
        assert not task_a > task_b

    def test_task_mixed_comparison(self):
        """Verify <= and >= agree with < and == on unequal and equal IDs.

        Mutation: @total_ordering removed, so <= and >= raise TypeError.
        Oracle: ids 1 and 2, and two tasks sharing id 1.
        """
        task1 = create_task(1, 'task_1')
        task2 = create_task(2, 'task_2')

        assert task1 < task2
        assert task1 <= task2
        assert task2 > task1
        assert task2 >= task1
        assert task1 != task2

        task1_dup = create_task(1, 'task_1_dup')
        assert task1 == task1_dup
        assert task1 <= task1_dup
        assert task1 >= task1_dup
        assert not task1 < task1_dup
        assert not task1 > task1_dup


class TestBasicDistribution:
    """Test basic token distribution scenarios.
    """

    @pytest.mark.parametrize(
        ('total_tokens', 'nodes', 'expected_counts'),
        [
            (100, [], {}),
            (100, ['node1'], {'node1': 100}),
            (100, ['node1', 'node2'], {'node1': 50, 'node2': 50}),
            (100, ['node1', 'node2', 'node3'], {'node1': 34, 'node2': 33, 'node3': 33}),
            ])
    def test_basic_token_distribution(self, total_tokens, nodes, expected_counts):
        """Verify a fresh distribution splits tokens evenly, remainder first.

        Mutation: the remainder token given to the last sorted node, or the
            empty-nodes guard dropped (division by zero).
        Oracle: hand-computed 100 // n per node with 100 % n extra to the
            first nodes; from an empty start every assigned token is a move.
        """
        assignments, moved = compute_minimal_move_distribution(
            total_tokens=total_tokens,
            active_nodes=nodes,
            current_assignments={},
            locked_tokens={},
            pattern_matcher=exact_match)

        counts = {}
        for node in assignments.values():
            counts[node] = counts.get(node, 0) + 1
        assert counts == expected_counts
        assert moved == sum(expected_counts.values())

    def test_remainder_tokens_distributed_alphabetically(self):
        """Verify remainder tokens go to the first nodes by sorted name.

        Mutation: sorted() dropped from the receivers loop, so the remainder
            follows the caller's node order (node-e, node-a, node-d).
        Oracle: hand-computed 103 = 5 * 20 + 3, extra token to node-a,
            node-b and node-c.
        """
        nodes = ['node-e', 'node-a', 'node-d', 'node-b', 'node-c']

        assignments, _ = compute_minimal_move_distribution(
            total_tokens=103,
            active_nodes=nodes,
            current_assignments={},
            locked_tokens={},
            pattern_matcher=exact_match)

        counts = dict.fromkeys(nodes, 0)
        for node in assignments.values():
            counts[node] += 1
        assert counts == {
            'node-a': 21,
            'node-b': 21,
            'node-c': 21,
            'node-d': 20,
            'node-e': 20,
            }

    def test_remainder_distribution_minimizes_moves_from_correct_nodes(self):
        """Verify a rebalance takes only the token over a node's own target.

        Mutation: is_over_target compares with >= instead of >, so node-c,
            already at its target, gives up its highest token.
        Oracle: hand-computed targets 56/55/55 for 166 tokens over three
            nodes; only node-b holds one token too many.
        """
        current = {
            **dict.fromkeys(range(55), 'node-a'),
            **dict.fromkeys(range(55, 111), 'node-b'),
            **dict.fromkeys(range(111, 166), 'node-c'),
            }

        nodes = ['node-a', 'node-b', 'node-c']

        assignments, moved = compute_minimal_move_distribution(
            total_tokens=166,
            active_nodes=nodes,
            current_assignments=current,
            locked_tokens={},
            pattern_matcher=exact_match)

        counts = dict.fromkeys(nodes, 0)
        for node in assignments.values():
            counts[node] += 1

        assert counts == {'node-a': 56, 'node-b': 55, 'node-c': 55}
        assert moved == 1


class TestMinimalMovement:
    """Test that algorithm minimizes token movement.
    """

    def test_no_movement_when_balanced(self):
        """Verify no tokens move when distribution is already balanced.

        Mutation: count_assignment_changes compares with == instead of !=.
        Oracle: an interleaved 50/50 split already meets both targets.
        """
        current = {i: f'node{i % 2 + 1}' for i in range(100)}

        assignments, moved = compute_minimal_move_distribution(
            total_tokens=100,
            active_nodes=['node1', 'node2'],
            current_assignments=current,
            locked_tokens={},
            pattern_matcher=exact_match)

        assert moved == 0
        assert assignments == current

    def test_minimal_movement_on_rebalance(self):
        """Verify a 60/40 split rebalances by moving exactly 10 tokens.

        Mutation: the over-target check dropped from the keep condition, so
            node2's own tokens use up node2's deficit and nothing moves.
        Oracle: hand-computed 50/50 targets; node1 holds 10 over.
        """
        current = dict.fromkeys(range(60), 'node1')
        current.update(dict.fromkeys(range(60, 100), 'node2'))

        assignments, moved = compute_minimal_move_distribution(
            total_tokens=100,
            active_nodes=['node1', 'node2'],
            current_assignments=current,
            locked_tokens={},
            pattern_matcher=exact_match)

        assert len(assignments) == 100
        assert moved == 10

        node1_count = sum(1 for node in assignments.values() if node == 'node1')
        node2_count = sum(1 for node in assignments.values() if node == 'node2')
        assert node1_count == 50
        assert node2_count == 50


class TestLockedTokenBehavior:
    """Test locked token constraint handling and strict enforcement.
    """

    def test_locked_token_assigned_to_pattern(self):
        """Verify locked tokens go to their matching node.

        Mutation: locks ignored in categorize_tokens_by_locks, so token 0
            goes to the first receiver, node1.
        Oracle: the lock map; it pins token 0 to node2, against the default
            ascending fill that starts with node1.
        """
        assignments, _ = compute_minimal_move_distribution(
            total_tokens=10,
            active_nodes=['node1', 'node2'],
            current_assignments={},
            locked_tokens={0: 'node2', 1: 'node1'},
            pattern_matcher=exact_match)

        assert assignments[0] == 'node2'
        assert assignments[1] == 'node1'

    def test_locked_tokens_exclude_from_balancing(self):
        """Verify unlocked tokens balance among themselves, apart from locks.

        Mutation: total_distributable counts locked tokens too, so node1
            gets 5 unlocked tokens and node2 gets 2.
        Oracle: hand-computed 7 unlocked tokens over two nodes: 4 and 3,
            extra to node1.
        """
        assignments, _ = compute_minimal_move_distribution(
            total_tokens=10,
            active_nodes=['node1', 'node2'],
            current_assignments={},
            locked_tokens={0: 'node1', 1: 'node1', 2: 'node1'},
            pattern_matcher=exact_match)

        assert assignments[0] == 'node1'
        assert assignments[1] == 'node1'
        assert assignments[2] == 'node1'

        unlocked_node1 = sum(
            1 for tid, node in assignments.items() if node == 'node1' and tid >= 3)
        unlocked_node2 = sum(
            1 for tid, node in assignments.items() if node == 'node2' and tid >= 3)

        assert unlocked_node1 == 4
        assert unlocked_node2 == 3

    def test_wildcard_pattern_matching(self):
        """Verify wildcard patterns match multiple nodes.

        Mutation: locks ignored, so token 0 goes to manager1, the first
            receiver by sorted name.
        Oracle: only worker1 and worker2 start with 'worker'.
        """
        assignments, _ = compute_minimal_move_distribution(
            total_tokens=10,
            active_nodes=['worker1', 'worker2', 'manager1'],
            current_assignments={},
            locked_tokens={0: 'worker%', 5: 'manager%'},
            pattern_matcher=wildcard_match)

        assert assignments[0] in {'worker1', 'worker2'}
        assert assignments[5] == 'manager1'

    def test_no_matching_node_for_locked_token(self):
        """Verify a token locked to an inactive node is never assigned.

        Mutation: blocked tokens added to the distributable list.
        Oracle: node3 is not active, so token 0 has no eligible node.
        """
        assignments, _ = compute_minimal_move_distribution(
            total_tokens=10,
            active_nodes=['node1', 'node2'],
            current_assignments={},
            locked_tokens={0: 'node3'},
            pattern_matcher=exact_match)

        assert 0 not in assignments, \
            'Token 0 locked to node3 should NOT be assigned when node3 inactive'

        for token_id in range(1, 10):
            assert token_id in assignments, \
                f'Unlocked token {token_id} should be assigned'
            assert assignments[token_id] in {'node1', 'node2'}, \
                f'Unlocked token {token_id} assigned to valid node'

    def test_locked_tokens_never_assigned_to_non_matching_nodes(self):
        """Verify locked tokens are NEVER assigned outside their patterns.

        Mutation: find_nodes_matching_patterns returns every active node
            when no pattern matches.
        Oracle: node names against the prefixes; no node starts with
            'admin'.
        """
        assignments, _ = compute_minimal_move_distribution(
            total_tokens=20,
            active_nodes=['worker1', 'worker2', 'manager1'],
            current_assignments={},
            locked_tokens={5: 'worker%', 10: 'manager%', 15: 'admin%'},
            pattern_matcher=wildcard_match)

        assert assignments[5] in {'worker1', 'worker2'}, \
            'Token 5 locked to worker% must only go to worker nodes'
        assert assignments[10] == 'manager1', \
            'Token 10 locked to manager% must only go to manager nodes'
        assert 15 not in assignments, \
            'Token 15 locked to admin% should not be assigned (no admin nodes active)'

    def test_locked_token_stays_with_matching_current_owner(self):
        """Verify a locked token keeps a matching current owner.

        Mutation: assign_locked_token skips the current-owner check, so
            token 5 goes to worker1, the least-loaded worker by name.
        Oracle: current owners from the input; 17 unlocked tokens start
            unowned, so they are the only moves.
        """
        current = {
            5: 'worker2',
            10: 'worker2',
            15: 'manager1',
            }

        assignments, moved = compute_minimal_move_distribution(
            total_tokens=20,
            active_nodes=['worker1', 'worker2', 'manager1'],
            current_assignments=current,
            locked_tokens={5: 'worker%', 10: 'worker%', 15: 'manager%'},
            pattern_matcher=wildcard_match)

        assert assignments[5] == 'worker2'
        assert assignments[10] == 'worker2'
        assert assignments[15] == 'manager1'
        assert moved == 17

    def test_locked_token_moves_when_current_owner_not_matching(self):
        """Verify locked token moves when its current owner fails the pattern.

        Mutation: assign_locked_token keeps any current owner without the
            eligibility check, so token 5 stays on manager1.
        Oracle: 18 unlocked tokens start unowned and both locked tokens
            change owner, so all 20 tokens move.
        """
        current = {
            5: 'manager1',
            10: 'worker1',
            }

        assignments, moved = compute_minimal_move_distribution(
            total_tokens=20,
            active_nodes=['worker1', 'worker2', 'manager1'],
            current_assignments=current,
            locked_tokens={5: 'worker%', 10: 'manager%'},
            pattern_matcher=wildcard_match)

        assert assignments[5] in {'worker1', 'worker2'}, \
            'Token 5 must move from manager1 to worker node (pattern mismatch)'
        assert assignments[10] == 'manager1', \
            'Token 10 must move from worker1 to manager1 (pattern mismatch)'
        assert moved == 20

    def test_multiple_fallback_patterns_first_succeeds(self):
        """Verify first matching fallback pattern is used.

        Mutation: matches from every pattern pooled, so the least-loaded
            tie-break by name picks backup-beta.
        Oracle: only primary-alpha matches the first pattern.
        """
        assignments, _ = compute_minimal_move_distribution(
            total_tokens=20,
            active_nodes=['primary-alpha', 'backup-beta', 'tertiary-gamma'],
            current_assignments={},
            locked_tokens={5: ['primary-%', 'backup-%', 'tertiary-%']},
            pattern_matcher=wildcard_match)

        assert assignments[5] == 'primary-alpha', \
            'Should use first matching pattern (primary-%) over later fallbacks'

    def test_multiple_fallback_patterns_skip_to_second(self):
        """Verify fallback to second pattern when first fails.

        Mutation: fallback patterns tried in reverse order, so token 5 goes
            to tertiary-gamma.
        Oracle: no node starts with 'primary-'; two start with 'backup-'.
        """
        assignments, _ = compute_minimal_move_distribution(
            total_tokens=20,
            active_nodes=['backup-alpha', 'backup-beta', 'tertiary-gamma'],
            current_assignments={},
            locked_tokens={5: ['primary-%', 'backup-%', 'tertiary-%']},
            pattern_matcher=wildcard_match)

        assert assignments[5] in {'backup-alpha', 'backup-beta'}, \
            'Should use second pattern (backup-%) when first pattern (primary-%) has no matches'

    def test_multiple_fallback_patterns_skip_to_third(self):
        """Verify fallback to third pattern when first and second fail.

        Mutation: only the first pattern of a list tried, so token 5 is
            blocked and missing from the result.
        Oracle: only tertiary-alpha matches any of the patterns.
        """
        assignments, _ = compute_minimal_move_distribution(
            total_tokens=20,
            active_nodes=['tertiary-alpha', 'other-node'],
            current_assignments={},
            locked_tokens={5: ['primary-%', 'backup-%', 'tertiary-%']},
            pattern_matcher=wildcard_match)

        assert assignments.get(5) == 'tertiary-alpha', \
            'Should use third pattern (tertiary-%) when first two patterns fail'

    def test_all_fallback_patterns_fail_no_assignment(self):
        """Verify no assignment when all fallback patterns fail.

        Mutation: blocked tokens added to the distributable list.
        Oracle: no node name starts with any of the three prefixes.
        """
        assignments, _ = compute_minimal_move_distribution(
            total_tokens=20,
            active_nodes=['other-node1', 'other-node2'],
            current_assignments={},
            locked_tokens={5: ['primary-%', 'backup-%', 'tertiary-%']},
            pattern_matcher=wildcard_match)

        assert 5 not in assignments, \
            'Token should not be assigned when all fallback patterns fail'

    def test_locked_tokens_dont_affect_unlocked_balance(self):
        """Verify locked tokens don't affect unlocked token distribution.

        Mutation: total_distributable counts locked tokens too, so the
            unlocked split becomes 34/33/30.
        Oracle: hand-computed 97 unlocked tokens over three nodes:
            33/32/32, extra to node1.
        """
        assignments, _ = compute_minimal_move_distribution(
            total_tokens=100,
            active_nodes=['node1', 'node2', 'node3'],
            current_assignments={},
            locked_tokens={0: 'node1', 1: 'node1', 2: 'node1'},
            pattern_matcher=exact_match)

        assert assignments[0] == 'node1'
        assert assignments[1] == 'node1'
        assert assignments[2] == 'node1'

        unlocked_counts = {'node1': 0, 'node2': 0, 'node3': 0}
        for token_id in range(3, 100):
            unlocked_counts[assignments[token_id]] += 1

        assert unlocked_counts == {'node1': 33, 'node2': 32, 'node3': 32}

    def test_mixed_locked_and_unlocked_distribution(self):
        """Verify correct distribution with mix of locked and unlocked tokens.

        Mutation: locks ignored, so tokens 0-9 fill manager1 first and
            token 5 lands there.
        Oracle: node names against the prefixes; no node starts with
            'admin'.
        """
        assignments, _ = compute_minimal_move_distribution(
            total_tokens=30,
            active_nodes=['worker1', 'worker2', 'manager1'],
            current_assignments={},
            locked_tokens={
                0: 'manager%',
                5: 'worker%',
                10: 'worker%',
                15: 'admin%',
                },
            pattern_matcher=wildcard_match)

        assert assignments[0] == 'manager1', 'Token 0 locked to manager'
        assert assignments[5] in {'worker1', 'worker2'}, 'Token 5 locked to workers'
        assert assignments[10] in {'worker1', 'worker2'}, 'Token 10 locked to workers'
        assert 15 not in assignments, 'Token 15 locked to non-existent admin nodes'

        unlocked_tokens = [tid for tid in range(30) if tid not in {0, 5, 10, 15}]
        assigned_unlocked = [tid for tid in unlocked_tokens if tid in assignments]
        assert len(assigned_unlocked) == len(unlocked_tokens), \
            'All unlocked tokens should be assigned'

    def test_locked_token_reassignment_minimizes_moves(self):
        """Verify a displaced locked token goes to the least-loaded match.

        Mutation: assign_locked_token picks the first eligible node by name,
            ignoring load, so token 10 joins token 5 on worker1.
        Oracle: worker1 keeps token 5; worker2 and worker3 hold none, and
            worker2 sorts first.
        """
        current = {
            5: 'worker1',
            10: 'manager1',
            }

        assignments, _ = compute_minimal_move_distribution(
            total_tokens=20,
            active_nodes=['worker1', 'worker2', 'worker3'],
            current_assignments=current,
            locked_tokens={5: 'worker%', 10: 'worker%'},
            pattern_matcher=wildcard_match)

        assert assignments[5] == 'worker1', \
            'Token 5 should stay with worker1 (current owner matches pattern)'
        assert assignments[10] == 'worker2'

    def test_multiple_tokens_locked_to_same_pattern_balanced(self):
        """Verify tokens locked to one pattern spread across its nodes.

        Mutation: assign_locked_token picks the first eligible node by name,
            ignoring load, so all ten land on worker1.
        Oracle: hand-computed round robin of 10 tokens over three workers:
            4/3/3, extra to worker1.
        """
        assignments, _ = compute_minimal_move_distribution(
            total_tokens=50,
            active_nodes=['worker1', 'worker2', 'worker3'],
            current_assignments={},
            locked_tokens=dict.fromkeys(range(10, 20), 'worker%'),
            pattern_matcher=wildcard_match)

        locked_counts = {'worker1': 0, 'worker2': 0, 'worker3': 0}
        for tid in range(10, 20):
            locked_counts[assignments[tid]] += 1

        assert locked_counts == {'worker1': 4, 'worker2': 3, 'worker3': 3}


class TestNodeFailure:
    """Test handling of node failures and recovery.
    """

    def test_dead_node_tokens_redistributed(self):
        """Verify tokens from dead nodes get redistributed.

        Mutation: receiver deficit computed from the target alone, ignoring
            current load, so node1 takes the dead node's tokens.
        Oracle: node1 already holds its 50-token target, so all 50 dead-node
            tokens go to node2.
        """
        current = dict.fromkeys(range(50), 'dead_node')
        current.update(dict.fromkeys(range(50, 100), 'node1'))

        assignments, moved = compute_minimal_move_distribution(
            total_tokens=100,
            active_nodes=['node1', 'node2'],
            current_assignments=current,
            locked_tokens={},
            pattern_matcher=exact_match)

        assert len(assignments) == 100
        assert 'dead_node' not in assignments.values()
        assert moved == 50

        node1_count = sum(1 for node in assignments.values() if node == 'node1')
        node2_count = sum(1 for node in assignments.values() if node == 'node2')
        assert node1_count == 50
        assert node2_count == 50

    def test_new_node_gets_fair_share(self):
        """Verify a new node takes its share and each giver stops at target.

        Mutation: the giver's load decrement dropped, so node2 gives all 33
            tokens and node1 keeps 50; or the remainder token given to the
            last sorted node, so node3 ends with 34.
        Oracle: hand-computed targets 34/33/33; reverse order takes node2's
            top 17 ids (83-99), then node1's top 16 ids (34-49).
        """
        current = {
            **dict.fromkeys(range(50), 'node1'),
            **dict.fromkeys(range(50, 100), 'node2'),
            }

        assignments, moved = compute_minimal_move_distribution(
            total_tokens=100,
            active_nodes=['node1', 'node2', 'node3'],
            current_assignments=current,
            locked_tokens={},
            pattern_matcher=exact_match)

        counts = {}
        for node in assignments.values():
            counts[node] = counts.get(node, 0) + 1

        assert counts == {'node1': 34, 'node2': 33, 'node3': 33}
        assert moved == 33
        node3_tokens = {tid for tid, node in assignments.items() if node == 'node3'}
        assert node3_tokens == set(range(34, 50)) | set(range(83, 100))


class TestDistributionEdgeCases:
    """Test edge cases and boundary conditions for token distribution.
    """

    def test_more_nodes_than_tokens(self):
        """Verify handling when nodes outnumber tokens.

        Mutation: the remainder tokens given to the last sorted nodes, so
            node6 gets one and node1 none.
        Oracle: hand-computed 5 // 6 = 0 per node, with the 5 remainder
            tokens to node1..node5.
        """
        assignments, _ = compute_minimal_move_distribution(
            total_tokens=5,
            active_nodes=['node1', 'node2', 'node3', 'node4', 'node5', 'node6'],
            current_assignments={},
            locked_tokens={},
            pattern_matcher=exact_match)

        assert sorted(assignments.values()) == [
            'node1',
            'node2',
            'node3',
            'node4',
            'node5',
            ]

    def test_all_tokens_locked(self):
        """Verify behavior when all tokens are locked.

        Mutation: locks ignored, so tokens 0-4 all go to node1.
        Oracle: the lock map; odd tokens are locked to node2.
        """
        locked = {i: f'node{i % 2 + 1}' for i in range(10)}

        assignments, _ = compute_minimal_move_distribution(
            total_tokens=10,
            active_nodes=['node1', 'node2'],
            current_assignments={},
            locked_tokens=locked,
            pattern_matcher=exact_match)

        assert assignments == locked

    def test_all_tokens_locked_to_nonexistent_pattern(self):
        """Verify no token is assigned when every lock matches no node.

        Mutation: blocked tokens added to the distributable list.
        Oracle: no node name starts with 'missing-'.
        """
        locked = {i: ['missing-%'] for i in range(10)}

        assignments, _ = compute_minimal_move_distribution(
            total_tokens=10,
            active_nodes=['node1', 'node2'],
            current_assignments={},
            locked_tokens=locked,
            pattern_matcher=wildcard_match)

        assert len(assignments) == 0, 'No assignments when locks match no nodes'

    def test_single_token(self):
        """Verify a single token goes to the first node by sorted name.

        Mutation: the remainder token given to the last sorted node.
        Oracle: hand-computed 1 // 2 = 0 per node, remainder to node1.
        """
        assignments, _ = compute_minimal_move_distribution(
            total_tokens=1,
            active_nodes=['node2', 'node1'],
            current_assignments={},
            locked_tokens={},
            pattern_matcher=exact_match)

        assert assignments == {0: 'node1'}

    def test_deterministic_sorting(self):
        """Verify the result does not depend on the order of active_nodes.

        Mutation: sorted() dropped from the receivers loop, so the remainder
            token follows the caller's node order.
        Oracle: the same call with the node list in sorted order.
        """
        sorted_result = compute_minimal_move_distribution(
            total_tokens=100,
            active_nodes=['node1', 'node2', 'node3'],
            current_assignments={},
            locked_tokens={},
            pattern_matcher=exact_match)

        shuffled_result = compute_minimal_move_distribution(
            total_tokens=100,
            active_nodes=['node3', 'node1', 'node2'],
            current_assignments={},
            locked_tokens={},
            pattern_matcher=exact_match)

        assert shuffled_result == sorted_result

    def test_fallback_patterns_used(self):
        """Verify fallback patterns are tried when primary pattern fails.

        Mutation: matches from every pattern pooled, so the least-loaded
            tie-break by name picks node1.
        Oracle: no node starts with 'missing-'; only special-alpha matches
            the second pattern.
        """
        locked = {
            5: ['missing-%', 'special-%', 'node%'],
            }

        assignments, _ = compute_minimal_move_distribution(
            total_tokens=10,
            active_nodes=['node1', 'special-alpha'],
            current_assignments={},
            locked_tokens=locked,
            pattern_matcher=wildcard_match)

        assert assignments[5] == 'special-alpha', \
            'Should use second fallback pattern when first fails'

    def test_reverse_iteration_moves_only_excess(self):
        """Verify a rebalance moves only node1's excess, highest ids first.

        Mutation: forward iteration on rebalance, so node1's lowest ids
            (0-35) move instead.
        Oracle: hand-computed targets 34/33/33; node1 gives 36 tokens,
            69-67 to node2 (deficit 3) and 66-34 to node3.
        """
        current = {i: 'node1' if i < 70 else 'node2' for i in range(100)}

        assignments, moved = compute_minimal_move_distribution(
            total_tokens=100,
            active_nodes=['node1', 'node2', 'node3'],
            current_assignments=current,
            locked_tokens={},
            pattern_matcher=exact_match)

        expected = {
            **dict.fromkeys(range(34), 'node1'),
            **dict.fromkeys(range(34, 67), 'node3'),
            **dict.fromkeys(range(67, 100), 'node2'),
            }
        assert assignments == expected
        assert moved == 36


class TestTokenIterationOrder:
    """Test token iteration order logic during rebalancing.
    """

    def test_reverse_iteration_when_rebalancing_imbalance(self):
        """Verify an over-quota node gives up its highest token ids.

        Mutation: forward iteration on rebalance, so tokens 0-29 move.
        Oracle: node1 holds 80 against a 50 target, so its top 30 ids
            (50-79) move.
        """
        current = dict.fromkeys(range(80), 'node1')
        current.update(dict.fromkeys(range(80, 100), 'node2'))

        assignments, _ = compute_minimal_move_distribution(
            total_tokens=100,
            active_nodes=['node1', 'node2'],
            current_assignments=current,
            locked_tokens={},
            pattern_matcher=exact_match)

        moved_tokens = {
            tid for tid, node in assignments.items() if current.get(tid) != node
            }

        assert moved_tokens == set(range(50, 80))

    def test_normal_iteration_when_adding_nodes(self):
        """Verify ascending fill order when no node is over quota.

        Mutation: reverse iteration always, so tokens 99-50 fill node1.
        Oracle: hand-computed 50/50 targets filled in ascending token order,
            node1 first.
        """
        assignments, _ = compute_minimal_move_distribution(
            total_tokens=100,
            active_nodes=['node1', 'node2'],
            current_assignments={},
            locked_tokens={},
            pattern_matcher=exact_match)

        expected = {
            **dict.fromkeys(range(50), 'node1'),
            **dict.fromkeys(range(50, 100), 'node2'),
            }
        assert assignments == expected


class TestLargeScale:
    """Test algorithm performance and correctness at scale.
    """

    def test_large_token_count(self):
        """Verify 10,000 tokens split exactly evenly over ten nodes.

        Mutation: receiver deficit one short (target - load - 1), so the
            leftover ten tokens fall back to node0.
        Oracle: hand-computed 10,000 / 10 = 1,000 per node.
        """
        nodes = [f'node{i}' for i in range(10)]

        assignments, _ = compute_minimal_move_distribution(
            total_tokens=10000,
            active_nodes=nodes,
            current_assignments={},
            locked_tokens={},
            pattern_matcher=exact_match)

        counts = dict.fromkeys(nodes, 0)
        for node in assignments.values():
            counts[node] += 1
        assert counts == dict.fromkeys(nodes, 1000)

    def test_large_scale_remainder_distribution(self):
        """Verify remainder tokens distributed alphabetically at scale.

        Mutation: the remainder token given to the last sorted node.
        Oracle: hand-computed 10,000 = 9 * 1,111 + 1, extra to node0.
        """
        nodes = [f'node{i}' for i in range(9)]

        assignments, moved = compute_minimal_move_distribution(
            total_tokens=10000,
            active_nodes=nodes,
            current_assignments={},
            locked_tokens={},
            pattern_matcher=exact_match)

        counts = dict.fromkeys(nodes, 0)
        for node in assignments.values():
            counts[node] += 1

        assert counts == {'node0': 1112, **dict.fromkeys(nodes[1:], 1111)}
        assert moved == 10000, 'All tokens assigned from empty initial state'

    def test_many_nodes(self):
        """Verify 1,000 tokens split exactly evenly over 100 nodes.

        Mutation: receiver deficit one short (target - load - 1), so the
            leftover 100 tokens fall back to node0.
        Oracle: hand-computed 1,000 / 100 = 10 per node.
        """
        nodes = [f'node{i}' for i in range(100)]

        assignments, _ = compute_minimal_move_distribution(
            total_tokens=1000,
            active_nodes=nodes,
            current_assignments={},
            locked_tokens={},
            pattern_matcher=exact_match)

        counts = dict.fromkeys(nodes, 0)
        for node in assignments.values():
            counts[node] += 1
        assert counts == dict.fromkeys(nodes, 10)

    def test_locked_tokens_at_scale(self):
        """Verify locked token handling at scale.

        Mutation: locks ignored, so token 10 (locked to node1) falls in
            node0's ascending block.
        Oracle: the lock map, token i to node{i % 3}.
        """
        locked = {i: f'node{i % 3}' for i in range(0, 1000, 10)}

        assignments, _ = compute_minimal_move_distribution(
            total_tokens=1000,
            active_nodes=['node0', 'node1', 'node2'],
            current_assignments={},
            locked_tokens=locked,
            pattern_matcher=exact_match)

        assert len(assignments) == 1000
        for tid, pattern in locked.items():
            assert assignments[tid] == pattern


class TestPatternMatching:
    """Test SQL LIKE pattern matching.
    """

    @pytest.mark.parametrize(
        ('node_name', 'pattern', 'should_match'),
        [
            ('node1', 'node1', True),
            ('node1', 'node2', False),
            ('prod-alpha', 'prod-%', True),
            ('prod-beta', 'prod-%', True),
            ('test-alpha', 'prod-%', False),
            ('alpha-gpu', '%-gpu', True),
            ('beta-gpu', '%-gpu', True),
            ('alpha-cpu', '%-gpu', False),
            ('prod-special-001', '%special%', True),
            ('special', '%special%', True),
            ('prod-regular-001', '%special%', False),
            ('node1', 'node_', True),
            ('node2', 'node_', True),
            ('node10', 'node_', False),
            ('nodeX1', 'node.1', False),
            ])
    def test_pattern_matching(self, node_name, pattern, should_match):
        """Verify matches_pattern follows SQL LIKE semantics.

        Mutation: '_' translated to '.*', the '$' anchor dropped, or '.' left
            unescaped so it matches any character.
        Oracle: hand-applied LIKE rules: '%' any run, '_' one character,
            every other character literal.
        """
        assert matches_pattern(node_name, pattern) is should_match


class TestTaskHashableValidation:
    """Test Task validation of hashable IDs.
    """

    def test_hashable_ids_accepted(self):
        """Verify hashable types are accepted as task IDs.

        Mutation: the hashable check narrowed to int and str, rejecting
            tuple and frozenset ids.
        Oracle: each value is hashable by Python's own rules.
        """
        valid_ids = [
            42,
            'string-id',
            ('tuple', 'id'),
            frozenset([1, 2, 3]),
            ]

        for task_id in valid_ids:
            task = Task(task_id, 'test-task')
            assert task.id == task_id, \
                f'Should accept hashable type {type(task_id).__name__}'

    @pytest.mark.parametrize(
        'task_id',
        [
            [1, 2, 3],
            {'key': 'value'},
            {1, 2, 3},
            ])
    def test_non_hashable_rejected(self, task_id):
        """Verify non-hashable IDs are rejected.

        Mutation: the hashable assert dropped from Task.__init__.
        Oracle: list, dict and set are unhashable by Python's own rules.
        """
        with pytest.raises(AssertionError, match='must be hashable'):
            Task(task_id, 'test-task')

    def test_none_is_hashable(self):
        """Verify None is accepted as a hashable ID.

        Mutation: the check written as a truthiness test on id, which
            rejects None.
        Oracle: hash(None) is defined in Python.
        """
        task = Task(None, 'none-task')
        assert task.id is None, 'None should be accepted (it is hashable)'


class TestSetClaimEdgeCases:
    """Test TaskManager.set_claim edge cases.
    """

    @pytest.fixture
    def claim_job(self, postgres):
        """Create and enter a job, yield it, then exit.
        """
        coord_config = get_coordination_config()
        job = create_job(
            'node1',
            postgres,
            coordination_config=coord_config,
            wait_on_enter=0)
        job.__enter__()
        try:
            yield job
        finally:
            job.__exit__(None, None, None)

    @clean_tables('Claim')
    @pytest.mark.parametrize('empty_input', [[], ()])
    def test_empty_iterable_handled(self, postgres, claim_job, empty_input):
        """Verify empty iterables are handled without errors.

        Mutation: an empty iterable routed to the single-item branch, which
            claims the string '[]' or '()'.
        Oracle: zero items in, zero rows out.
        """
        tables = schema.get_table_names()
        claim_job.set_claim(empty_input)

        with postgres.connect() as conn:
            result = conn.execute(text(f'select count(*) from {tables["Claim"]}'))
            count = result.scalar()

        assert count == 0, 'No claims should be created for empty iterable'

    @clean_tables('Claim')
    def test_large_iterable(self, postgres, claim_job):
        """Verify large iterables are processed correctly.

        Mutation: conn.commit() dropped from set_claim, so no row persists.
        Oracle: 1,000 distinct ids in.
        """
        tables = schema.get_table_names()
        claim_job.set_claim(list(range(1000)))

        with postgres.connect() as conn:
            result = conn.execute(
                text(f'select count(*) from {tables["Claim"]} where node = :node'),
                {'node': 'node1'})
            count = result.scalar()

        assert count == 1000, 'All 1000 items should be claimed'

    @clean_tables('Claim')
    def test_mixed_type_iterable(self, postgres, claim_job):
        """Verify iterables with mixed types are handled correctly.

        Mutation: repr() in place of str() for the stored task_id, so
            strings gain quotes.
        Oracle: hand-written str() of each input.
        """
        tables = schema.get_table_names()
        claim_job.set_claim([1, 'string-item', (2, 3), 42, 'another-string'])

        claims_sql = f"""
select task_id
from {tables["Claim"]}
where node = :node
order by task_id
"""
        with postgres.connect() as conn:
            result = conn.execute(text(claims_sql), {'node': 'node1'})
            items = [row[0] for row in result]

        assert len(items) == 5, 'All 5 mixed-type items should be claimed'
        expected_items = {'1', 'string-item', '(2, 3)', '42', 'another-string'}
        assert set(items) == expected_items, 'Items should be converted to strings'

    @clean_tables('Claim')
    def test_single_string_not_treated_as_iterable(self, postgres, claim_job):
        """Verify a single string is one claim, not one per character.

        Mutation: the str exclusion dropped from the iterable check, so
            each character becomes a claim.
        Oracle: one string in, one row holding that string out.
        """
        tables = schema.get_table_names()
        claim_job.set_claim('single-string-item')

        claims_sql = f"""
select task_id
from {tables["Claim"]}
where node = :node
"""
        with postgres.connect() as conn:
            result = conn.execute(text(claims_sql), {'node': 'node1'})
            items = [row[0] for row in result]

        assert items == ['single-string-item']

    @clean_tables('Claim')
    def test_single_integer_handled(self, postgres, claim_job):
        """Verify single integer is claimed correctly.

        Mutation: the single-item branch removed, so set_claim iterates the
            int and raises TypeError.
        Oracle: str(42) == '42'.
        """
        tables = schema.get_table_names()
        claim_job.set_claim(42)

        claims_sql = f"""
select task_id
from {tables["Claim"]}
where node = :node
"""
        with postgres.connect() as conn:
            result = conn.execute(text(claims_sql), {'node': 'node1'})
            items = [row[0] for row in result]

        assert items == ['42']

    @clean_tables('Claim')
    def test_generator_expression_handled(self, postgres, claim_job):
        """Verify generator expressions are handled correctly.

        Mutation: the iterable check narrowed to list, tuple and set, so the
            generator is claimed as one str(generator) row.
        Oracle: the generator yields 10 distinct values.
        """
        tables = schema.get_table_names()
        claim_job.set_claim(i * 2 for i in range(10))

        with postgres.connect() as conn:
            result = conn.execute(
                text(f'select count(*) from {tables["Claim"]} where node = :node'),
                {'node': 'node1'})
            count = result.scalar()

        assert count == 10, 'All 10 generated items should be claimed'

    @clean_tables('Claim')
    def test_range_object_handled(self, postgres, claim_job):
        """Verify range objects are handled correctly.

        Mutation: the iterable check narrowed to list, tuple and set, so the
            range is claimed as one 'range(0, 20)' row.
        Oracle: range(20) holds 20 distinct values.
        """
        tables = schema.get_table_names()
        claim_job.set_claim(range(20))

        with postgres.connect() as conn:
            result = conn.execute(
                text(f'select count(*) from {tables["Claim"]} where node = :node'),
                {'node': 'node1'})
            count = result.scalar()

        assert count == 20, 'All 20 range items should be claimed'

    @clean_tables('Claim')
    def test_duplicate_items_in_iterable(self, postgres, claim_job):
        """Verify duplicate items in iterable are handled by ON CONFLICT.

        Mutation: ON CONFLICT DO NOTHING dropped from the insert, so the
            second '2' raises an IntegrityError.
        Oracle: four distinct ids among the seven inputs.
        """
        tables = schema.get_table_names()
        claim_job.set_claim([1, 2, 3, 2, 1, 4, 3])

        claims_sql = f"""
select task_id
from {tables["Claim"]}
where node = :node
"""
        with postgres.connect() as conn:
            result = conn.execute(text(claims_sql), {'node': 'node1'})
            items = [row[0] for row in result]

        assert sorted(items) == ['1', '2', '3', '4']

    @clean_tables('Claim')
    def test_none_in_iterable(self, postgres, claim_job):
        """Verify None values in iterable are claimed as the string 'None'.

        Mutation: None filtered out of the iterable before insert.
        Oracle: str(None) == 'None'; four distinct ids among the inputs.
        """
        tables = schema.get_table_names()
        claim_job.set_claim([1, None, 2, None, 3])

        claims_sql = f"""
select task_id
from {tables["Claim"]}
where node = :node
"""
        with postgres.connect() as conn:
            result = conn.execute(text(claims_sql), {'node': 'node1'})
            items = [row[0] for row in result]

        assert sorted(items) == ['1', '2', '3', 'None']

    @clean_tables('Claim', 'Audit')
    def test_add_task_tuple_id_claims_one_row(self, postgres, claim_job):
        """Verify add_task claims a tuple task id as one task, as it audits it.

        Mutation: Job.add_task handing the bare tuple to set_claim, which
            claims each element as its own task.
        Oracle: the Task docstring allows any Hashable id, and the audit row
            written by write_audit holds str((1, 2)).
        """
        tables = schema.get_table_names()
        claim_job.add_task((1, 2))
        claim_job.write_audit()

        with postgres.connect() as conn:
            claim_sql = f'select task_id from {tables["Claim"]} where node = :node'
            claims = [row[0] for row in conn.execute(text(claim_sql), {'node': 'node1'})]
            audit_sql = f'select task_id from {tables["Audit"]} where node = :node'
            audits = [row[0] for row in conn.execute(text(audit_sql), {'node': 'node1'})]

        assert audits == ['(1, 2)']
        assert claims == ['(1, 2)']


if __name__ == '__main__':
    pytest.main(args=['-sx', __file__])
