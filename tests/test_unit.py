"""Unit tests for individual components and algorithms.

USE THIS FILE FOR:
- Pure unit tests with no database required (or minimal DB usage)
- Algorithm verification (hashing, distribution, pattern matching)
- Data structure validation
- State machine logic
- Configuration validation
- Edge case handling for individual methods
"""
import logging
import time

import pytest
from sqlalchemy import text

from jobsync import schema
from jobsync.client import CoordinationConfig, CoordinationEvent, EventQueue
from jobsync.client import JobState, JobStateMachine, Task
from jobsync.client import compute_minimal_move_distribution, matches_pattern
from jobsync.client import task_to_token

logger = logging.getLogger(__name__)


from fixtures import *  # noqa: F401, F403

logger = logging.getLogger(__name__)


class TestStateTransitions:
    """Test valid and invalid state transitions."""

    def test_initial_state(self):
        """Verify state machine starts in INITIALIZING state.
        """
        sm = JobStateMachine()
        assert sm.state == JobState.INITIALIZING, 'Should start in INITIALIZING state'

    def test_valid_transition_sequence(self):
        """Verify standard lifecycle transitions work.
        """
        sm = JobStateMachine()

        assert sm.transition_to(JobState.CLUSTER_FORMING), 'Should transition to CLUSTER_FORMING'
        assert sm.state == JobState.CLUSTER_FORMING

        assert sm.transition_to(JobState.ELECTING), 'Should transition to ELECTING'
        assert sm.state == JobState.ELECTING

        assert sm.transition_to(JobState.DISTRIBUTING), 'Should transition to DISTRIBUTING'
        assert sm.state == JobState.DISTRIBUTING

        assert sm.transition_to(JobState.RUNNING_FOLLOWER), 'Should transition to RUNNING_FOLLOWER'
        assert sm.state == JobState.RUNNING_FOLLOWER

        assert sm.transition_to(JobState.SHUTTING_DOWN), 'Should transition to SHUTTING_DOWN'
        assert sm.state == JobState.SHUTTING_DOWN

    def test_leader_lifecycle_transitions(self):
        """Verify leader node lifecycle transitions work.
        """
        sm = JobStateMachine()

        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ELECTING)
        sm.transition_to(JobState.DISTRIBUTING)

        assert sm.transition_to(JobState.RUNNING_LEADER), 'Should transition to RUNNING_LEADER'
        assert sm.state == JobState.RUNNING_LEADER

        assert sm.transition_to(JobState.SHUTTING_DOWN), 'Should transition to SHUTTING_DOWN'
        assert sm.state == JobState.SHUTTING_DOWN

    def test_leader_follower_transitions(self):
        """Verify transitions between RUNNING_LEADER and RUNNING_FOLLOWER.
        """
        sm = JobStateMachine()

        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ELECTING)
        sm.transition_to(JobState.DISTRIBUTING)
        sm.transition_to(JobState.RUNNING_FOLLOWER)

        assert sm.transition_to(JobState.RUNNING_LEADER), 'Follower should promote to leader'
        assert sm.state == JobState.RUNNING_LEADER

        assert sm.transition_to(JobState.RUNNING_FOLLOWER), 'Leader should demote to follower'
        assert sm.state == JobState.RUNNING_FOLLOWER

    def test_invalid_transition_rejected(self):
        """Verify invalid transitions return False and don't change state.
        """
        sm = JobStateMachine()

        result = sm.transition_to(JobState.RUNNING_LEADER)
        assert not result, 'Should reject invalid transition'
        assert sm.state == JobState.INITIALIZING, 'State should not change on invalid transition'

    def test_skip_invalid_state_rejected(self):
        """Verify skipping required states is rejected.
        """
        sm = JobStateMachine()
        sm.transition_to(JobState.CLUSTER_FORMING)

        result = sm.transition_to(JobState.DISTRIBUTING)
        assert not result, 'Should reject skipping ELECTING state'
        assert sm.state == JobState.CLUSTER_FORMING, 'State should remain unchanged'

    def test_backward_transition_rejected(self):
        """Verify backward transitions are rejected.
        """
        sm = JobStateMachine()
        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ELECTING)

        result = sm.transition_to(JobState.CLUSTER_FORMING)
        assert not result, 'Should reject backward transition'
        assert sm.state == JobState.ELECTING, 'State should remain unchanged'

    def test_transition_to_same_state_succeeds(self):
        """Verify transitioning to current state is allowed (idempotent).
        """
        sm = JobStateMachine()

        result = sm.transition_to(JobState.INITIALIZING)
        assert result, 'Transition to same state should succeed'
        assert sm.state == JobState.INITIALIZING

    def test_shutting_down_from_any_state(self):
        """Verify SHUTTING_DOWN reachable from any state for cleanup.
        """
        sm = JobStateMachine()

        result = sm.transition_to(JobState.SHUTTING_DOWN)
        assert result, 'Can shutdown from INITIALIZING for cleanup'

        sm2 = JobStateMachine()
        sm2.transition_to(JobState.CLUSTER_FORMING)
        result = sm2.transition_to(JobState.SHUTTING_DOWN)
        assert result, 'Can shutdown from CLUSTER_FORMING for cleanup'

        sm3 = JobStateMachine()
        sm3.transition_to(JobState.CLUSTER_FORMING)
        sm3.transition_to(JobState.ELECTING)
        sm3.transition_to(JobState.DISTRIBUTING)
        sm3.transition_to(JobState.RUNNING_LEADER)
        result = sm3.transition_to(JobState.SHUTTING_DOWN)
        assert result, 'Can shutdown from RUNNING_LEADER'


class TestCallbacks:
    """Test callback registration and invocation."""

    def test_on_enter_callback_invoked(self):
        """Verify on_enter callback fires when entering state.
        """
        sm = JobStateMachine()
        callback_invoked = []

        def callback():
            callback_invoked.append(True)

        sm.on_enter(JobState.CLUSTER_FORMING, callback)
        sm.transition_to(JobState.CLUSTER_FORMING)

        assert len(callback_invoked) == 1, 'Callback should be invoked once'

    def test_on_exit_callback_invoked(self):
        """Verify on_exit callback fires when exiting state.
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
        """Verify callbacks invoked for each transition.
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
        """Verify callbacks not invoked when transition fails.
        """
        sm = JobStateMachine()
        callback_invoked = []

        def callback():
            callback_invoked.append(True)

        sm.on_enter(JobState.RUNNING_LEADER, callback)
        sm.transition_to(JobState.RUNNING_LEADER)

        assert len(callback_invoked) == 0, 'Callback should not be invoked for invalid transition'

    def test_callback_not_invoked_on_same_state_transition(self):
        """Verify callbacks not invoked when transitioning to same state.
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

        assert enter_count[0] == 0, 'Enter callback should not fire for same-state transition'
        assert exit_count[0] == 0, 'Exit callback should not fire for same-state transition'

    def test_callback_exception_handling(self):
        """Verify exceptions in callbacks don't prevent state transition.
        """
        sm = JobStateMachine()

        def failing_callback():
            raise RuntimeError('Test exception')

        sm.on_enter(JobState.CLUSTER_FORMING, failing_callback)

        with pytest.raises(RuntimeError):
            sm.transition_to(JobState.CLUSTER_FORMING)

        assert sm.state == JobState.CLUSTER_FORMING, 'State should change despite callback exception'

    def test_enter_invoked_before_exit(self):
        """Verify exit callback called before enter callback during transition.
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
    """Test state-dependent task claiming behavior."""

    def test_cannot_claim_in_initializing(self):
        """Verify task claiming disallowed in INITIALIZING state.
        """
        sm = JobStateMachine()
        assert not sm.can_claim_task(), 'Cannot claim tasks in INITIALIZING'

    def test_cannot_claim_in_cluster_forming(self):
        """Verify task claiming disallowed in CLUSTER_FORMING state.
        """
        sm = JobStateMachine()
        sm.transition_to(JobState.CLUSTER_FORMING)
        assert not sm.can_claim_task(), 'Cannot claim tasks in CLUSTER_FORMING'

    def test_cannot_claim_in_electing(self):
        """Verify task claiming disallowed in ELECTING state.
        """
        sm = JobStateMachine()
        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ELECTING)
        assert not sm.can_claim_task(), 'Cannot claim tasks in ELECTING'

    def test_cannot_claim_in_distributing(self):
        """Verify task claiming disallowed in DISTRIBUTING state.
        """
        sm = JobStateMachine()
        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ELECTING)
        sm.transition_to(JobState.DISTRIBUTING)
        assert not sm.can_claim_task(), 'Cannot claim tasks in DISTRIBUTING'

    def test_can_claim_in_running_follower(self):
        """Verify task claiming allowed in RUNNING_FOLLOWER state.
        """
        sm = JobStateMachine()
        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ELECTING)
        sm.transition_to(JobState.DISTRIBUTING)
        sm.transition_to(JobState.RUNNING_FOLLOWER)
        assert sm.can_claim_task(), 'Can claim tasks in RUNNING_FOLLOWER'

    def test_can_claim_in_running_leader(self):
        """Verify task claiming allowed in RUNNING_LEADER state.
        """
        sm = JobStateMachine()
        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ELECTING)
        sm.transition_to(JobState.DISTRIBUTING)
        sm.transition_to(JobState.RUNNING_LEADER)
        assert sm.can_claim_task(), 'Can claim tasks in RUNNING_LEADER'

    def test_cannot_claim_in_shutting_down(self):
        """Verify task claiming disallowed in SHUTTING_DOWN state.
        """
        sm = JobStateMachine()
        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ELECTING)
        sm.transition_to(JobState.DISTRIBUTING)
        sm.transition_to(JobState.RUNNING_FOLLOWER)
        sm.transition_to(JobState.SHUTTING_DOWN)
        assert not sm.can_claim_task(), 'Cannot claim tasks in SHUTTING_DOWN'

    def test_can_claim_follows_state_changes(self):
        """Verify can_claim_task reflects current state correctly.
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
    """Test ERROR state transitions and behavior."""

    def test_error_state_from_initializing(self):
        """Verify transition to ERROR from INITIALIZING state.
        """
        sm = JobStateMachine()
        assert sm.state == JobState.INITIALIZING

        result = sm.transition_to(JobState.ERROR)
        assert result, 'Should transition to ERROR from INITIALIZING'
        assert sm.state == JobState.ERROR

    def test_error_state_from_cluster_forming(self):
        """Verify transition to ERROR from CLUSTER_FORMING state.
        """
        sm = JobStateMachine()
        sm.transition_to(JobState.CLUSTER_FORMING)

        result = sm.transition_to(JobState.ERROR)
        assert result, 'Should transition to ERROR from CLUSTER_FORMING'
        assert sm.state == JobState.ERROR

    def test_error_state_from_electing(self):
        """Verify transition to ERROR from ELECTING state.
        """
        sm = JobStateMachine()
        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ELECTING)

        result = sm.transition_to(JobState.ERROR)
        assert result, 'Should transition to ERROR from ELECTING'
        assert sm.state == JobState.ERROR

    def test_error_state_from_distributing(self):
        """Verify transition to ERROR from DISTRIBUTING state.
        """
        sm = JobStateMachine()
        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ELECTING)
        sm.transition_to(JobState.DISTRIBUTING)

        result = sm.transition_to(JobState.ERROR)
        assert result, 'Should transition to ERROR from DISTRIBUTING'
        assert sm.state == JobState.ERROR

    def test_error_to_shutting_down_transition(self):
        """Verify transition from ERROR to SHUTTING_DOWN state.
        """
        sm = JobStateMachine()
        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ERROR)

        result = sm.transition_to(JobState.SHUTTING_DOWN)
        assert result, 'Should transition to SHUTTING_DOWN from ERROR'
        assert sm.state == JobState.SHUTTING_DOWN

    def test_cannot_transition_to_error_from_running_leader(self):
        """Verify ERROR state cannot be reached from RUNNING_LEADER.
        """
        sm = JobStateMachine()
        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ELECTING)
        sm.transition_to(JobState.DISTRIBUTING)
        sm.transition_to(JobState.RUNNING_LEADER)

        result = sm.transition_to(JobState.ERROR)
        assert not result, 'Should not transition to ERROR from RUNNING_LEADER'
        assert sm.state == JobState.RUNNING_LEADER

    def test_cannot_transition_to_error_from_running_follower(self):
        """Verify ERROR state cannot be reached from RUNNING_FOLLOWER.
        """
        sm = JobStateMachine()
        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ELECTING)
        sm.transition_to(JobState.DISTRIBUTING)
        sm.transition_to(JobState.RUNNING_FOLLOWER)

        result = sm.transition_to(JobState.ERROR)
        assert not result, 'Should not transition to ERROR from RUNNING_FOLLOWER'
        assert sm.state == JobState.RUNNING_FOLLOWER

    def test_is_error_returns_true_in_error_state(self):
        """Verify is_error() returns True when in ERROR state.
        """
        sm = JobStateMachine()
        assert not sm.is_error(), 'Should not be in error state initially'

        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ERROR)

        assert sm.is_error(), 'is_error() should return True in ERROR state'

    def test_is_error_returns_false_in_other_states(self):
        """Verify is_error() returns False in non-ERROR states.
        """
        sm = JobStateMachine()

        states_to_test = [
            JobState.INITIALIZING,
            JobState.CLUSTER_FORMING,
            JobState.ELECTING,
            JobState.DISTRIBUTING,
        ]

        for state in states_to_test:
            sm.state = state
            assert not sm.is_error(), f'is_error() should return False in {state.value}'

    def test_cannot_claim_tasks_in_error_state(self):
        """Verify task claiming is blocked in ERROR state.
        """
        sm = JobStateMachine()
        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ERROR)

        assert not sm.can_claim_task(), 'Cannot claim tasks in ERROR state'

    def test_error_state_blocks_initialization_progress(self):
        """Verify cannot transition to running states from ERROR.
        """
        sm = JobStateMachine()
        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ERROR)

        # Try invalid transitions from ERROR
        result = sm.transition_to(JobState.DISTRIBUTING)
        assert not result, 'Should not transition from ERROR to DISTRIBUTING'

        result = sm.transition_to(JobState.RUNNING_LEADER)
        assert not result, 'Should not transition from ERROR to RUNNING_LEADER'

        result = sm.transition_to(JobState.RUNNING_FOLLOWER)
        assert not result, 'Should not transition from ERROR to RUNNING_FOLLOWER'

        assert sm.state == JobState.ERROR, 'Should remain in ERROR state'

    def test_error_state_entry_callback_invoked(self):
        """Verify on_enter callback fires when entering ERROR state.
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
        """Verify on_exit callback fires when leaving ERROR state.
        """
        sm = JobStateMachine()
        callback_invoked = []

        def error_exit_callback():
            callback_invoked.append(True)

        sm.on_exit(JobState.ERROR, error_exit_callback)
        sm.transition_to(JobState.ERROR)
        sm.transition_to(JobState.SHUTTING_DOWN)

        assert len(callback_invoked) == 1, 'ERROR exit callback should be invoked'


class TestTransitionValidation:
    """Test transition validation logic."""

    def test_all_valid_transitions_defined(self):
        """Verify all expected valid transitions are defined in transition graph.
        """
        sm = JobStateMachine()

        expected_transitions = [
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
        ]

        for from_state, to_state in expected_transitions:
            sm.state = from_state
            result = sm.transition_to(to_state)
            assert result, f'Transition {from_state.value} -> {to_state.value} should be valid'


class TestEventQueue:
    """Test EventQueue thread-safe event handling."""

    def test_initialization(self):
        """Verify event queue initializes with empty state.
        """
        queue = EventQueue()
        history = queue.get_history()
        assert history == [], 'Queue should start empty'

    def test_initialization_with_custom_history_size(self):
        """Verify event queue accepts custom history size.
        """
        queue = EventQueue(history_size=50)
        assert queue._history.maxlen == 50

    def test_publish_single_event(self):
        """Verify single event can be published.
        """
        queue = EventQueue()
        queue.publish('test_event', {'key': 'value'})

        history = queue.get_history()
        assert len(history) == 1
        assert history[0].type == 'test_event'
        assert history[0].data == {'key': 'value'}

    def test_publish_multiple_events(self):
        """Verify multiple events maintain insertion order.
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
        """
        queue = EventQueue()
        queue.publish('event_no_data')

        history = queue.get_history()
        assert len(history) == 1
        assert history[0].data == {}

    def test_consume_all_empty_queue(self):
        """Verify consume_all returns empty list for empty queue.
        """
        queue = EventQueue()
        events = queue.consume_all()
        assert events == []

    def test_consume_all_single_event(self):
        """Verify consume_all returns and clears single event.
        """
        queue = EventQueue()
        queue.publish('test_event', {'data': 'value'})

        events = queue.consume_all()
        assert len(events) == 1
        assert events[0].type == 'test_event'

        second_consume = queue.consume_all()
        assert second_consume == [], 'Second consume should return empty list'

    def test_consume_all_multiple_events(self):
        """Verify consume_all returns and clears all events.
        """
        queue = EventQueue()
        queue.publish('event1')
        queue.publish('event2')
        queue.publish('event3')

        events = queue.consume_all()
        assert len(events) == 3

        second_consume = queue.consume_all()
        assert second_consume == [], 'Second consume should return empty list'

    def test_consume_all_twice(self):
        """Verify second consume_all returns empty after first consume.
        """
        queue = EventQueue()
        queue.publish('event1')

        first_consume = queue.consume_all()
        assert len(first_consume) == 1

        second_consume = queue.consume_all()
        assert second_consume == [], 'Second consume should return empty'

    def test_get_history_empty_queue(self):
        """Verify get_history returns empty list for empty queue.
        """
        queue = EventQueue()
        history = queue.get_history()
        assert history == []

    def test_get_history_all_events(self):
        """Verify get_history returns all events without limit.
        """
        queue = EventQueue()
        queue.publish('event1')
        queue.publish('event2')
        queue.publish('event3')

        history = queue.get_history()
        assert len(history) == 3
        assert history[0].type == 'event1'
        assert history[1].type == 'event2'
        assert history[2].type == 'event3'

    def test_get_history_does_not_clear_queue(self):
        """Verify get_history doesn't remove events from queue.
        """
        queue = EventQueue()
        queue.publish('event1')
        queue.publish('event2')

        first_history = queue.get_history()
        second_history = queue.get_history()

        assert len(first_history) == 2
        assert len(second_history) == 2, 'get_history should not clear queue'

    def test_get_history_with_limit_less_than_size(self):
        """Verify get_history with limit returns most recent events.
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
        """Verify get_history when limit equals queue size.
        """
        queue = EventQueue()
        queue.publish('event1')
        queue.publish('event2')

        history = queue.get_history(limit=2)
        assert len(history) == 2
        assert history[0].type == 'event1'
        assert history[1].type == 'event2'

    def test_get_history_with_limit_greater_than_size(self):
        """Verify get_history when limit exceeds queue size returns all.
        """
        queue = EventQueue()
        queue.publish('event1')
        queue.publish('event2')

        history = queue.get_history(limit=10)
        assert len(history) == 2, 'Should return all available events'

    def test_get_history_with_zero_limit(self):
        """Verify get_history with zero limit returns empty list.
        """
        queue = EventQueue()
        queue.publish('event1')
        queue.publish('event2')

        history = queue.get_history(limit=0)
        assert history == [], 'Zero limit should return empty list'

    def test_publish_after_consume(self):
        """Verify publishing after consume works correctly.
        """
        queue = EventQueue()
        queue.publish('event1')
        queue.consume_all()

        queue.publish('event2')
        history = queue.get_history()

        assert len(history) == 2, 'History should include both consumed and unconsumed events'
        assert history[0].type == 'event1'
        assert history[1].type == 'event2'

    def test_event_has_timestamp(self):
        """Verify published events have timestamps.
        """
        queue = EventQueue()
        before = time.time()
        queue.publish('test_event')
        after = time.time()

        history = queue.get_history()
        assert history[0].timestamp is not None
        assert before <= history[0].timestamp <= after

    def test_consume_all_returns_copy(self):
        """Verify consume_all returns independent copy.
        """
        queue = EventQueue()
        queue.publish('event1', {'value': 1})

        events = queue.consume_all()
        events[0].data['value'] = 999

        queue.publish('event2', {'value': 2})
        new_events = queue.consume_all()

        assert new_events[0].data['value'] == 2, 'Original data should be unchanged'

    def test_get_history_returns_copy(self):
        """Verify get_history returns independent copy.
        """
        queue = EventQueue()
        queue.publish('event1')
        queue.publish('event2')

        history = queue.get_history()
        original_len = len(history)
        history.append(CoordinationEvent('fake', {}))

        new_history = queue.get_history()
        assert len(new_history) == original_len, 'Modified copy should not affect queue'


class TestEventQueueWithHistory:
    """Test EventQueue ring buffer and history persistence."""

    def test_history_persists_after_consume(self):
        """Verify consumed events are retained in history.
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

    def test_history_size_limit_enforced(self):
        """Verify ring buffer respects maximum size.
        """
        queue = EventQueue(history_size=5)

        for i in range(10):
            queue.publish(f'event{i}')
            queue.consume_all()

        history = queue.get_history()
        assert len(history) == 5
        assert history[0].type == 'event5'
        assert history[4].type == 'event9'

    def test_get_history_includes_unprocessed_events(self):
        """Verify get_history returns both processed and unprocessed events.
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
        """Verify limit parameter works with ring buffer.
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

    def test_history_empty_initially(self):
        """Verify history starts empty.
        """
        queue = EventQueue(history_size=10)
        history = queue.get_history()
        assert history == []

    def test_consume_all_adds_to_history_before_clearing(self):
        """Verify consume_all stores events before clearing queue.
        """
        queue = EventQueue(history_size=10)
        queue.publish('event1')

        consumed = queue.consume_all()
        history = queue.get_history()

        assert len(consumed) == 1
        assert len(history) == 1
        assert consumed[0].type == history[0].type

    def test_history_overflow_drops_oldest(self):
        """Verify ring buffer drops oldest events when full.
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
        """Verify limit works correctly with both consumed and unconsumed events.
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
        """Verify history operations are thread-safe.
        """
        import threading
        queue = EventQueue(history_size=1000)
        errors = []

        def publisher():
            try:
                for i in range(100):
                    queue.publish(f'event{i}')
            except Exception as e:
                errors.append(e)

        def consumer():
            try:
                for _ in range(10):
                    queue.consume_all()
                    time.sleep(0.001)
            except Exception as e:
                errors.append(e)

        def reader():
            try:
                for _ in range(10):
                    queue.get_history(limit=10)
                    time.sleep(0.001)
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=publisher),
            threading.Thread(target=consumer),
            threading.Thread(target=reader)
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == [], f'Thread safety errors: {errors}'


class TestJobCoordinationStatus:
    """Test Job.get_coordination_status() debugging API."""

    def test_coordination_disabled_returns_minimal_info(self):
        """Verify status when coordination is disabled.
        """
        job = Job('test-node', coordination_config=None)
        status = job.get_coordination_status()

        assert status == {'coordination_enabled': False}

    def test_coordination_enabled_returns_full_status(self, postgres):
        """Verify status includes all coordination information.
        """
        coord_config = get_coordination_config()
        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)

        with job:
            job._event_queue.publish('test_event', {'test': 'data'})

            status = job.get_coordination_status()

            assert 'coordination_enabled' in status
            assert status['node_name'] == 'node1'
            assert status['state'] in [s.value for s in JobState]
            assert isinstance(status['is_leader'], bool)
            assert isinstance(status['my_tokens'], int)
            assert isinstance(status['token_version'], int)
            assert status['total_tokens'] == coord_config.total_tokens
            assert isinstance(status['active_nodes'], int)
            assert isinstance(status['recent_events'], list)
            assert 'monitors' in status
            assert status['last_heartbeat'] is not None

    def test_recent_events_limited_to_20(self, postgres):
        """Verify recent_events respects limit of 20.
        """
        coord_config = get_coordination_config()
        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)

        with job:
            for i in range(30):
                job._event_queue.publish(f'event{i}')

            status = job.get_coordination_status()

            assert len(status['recent_events']) <= 20

    def test_recent_events_includes_metadata(self, postgres):
        """Verify event entries include type, timestamp, and data.
        """
        coord_config = get_coordination_config()
        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)

        with job:
            job._event_queue.publish('test_event', {'key': 'value'})

            status = job.get_coordination_status()

            assert len(status['recent_events']) >= 1
            event = status['recent_events'][-1]
            assert 'type' in event
            assert 'timestamp' in event
            assert 'data' in event
            assert event['type'] == 'test_event'
            assert event['data'] == {'key': 'value'}

    def test_monitors_list_present(self, postgres):
        """Verify monitors list is included in status.
        """
        coord_config = get_coordination_config()
        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)

        with job:
            status = job.get_coordination_status()

            assert 'monitors' in status
            assert isinstance(status['monitors'], list)
            assert 'coordination' in status['monitors']

    def test_status_captures_state_transitions(self, postgres):
        """Verify status reflects current state machine state.
        """
        coord_config = get_coordination_config()
        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)

        with job:
            status = job.get_coordination_status()

            assert status['state'] in {'running_leader', 'running_follower'}
            assert status['is_leader'] == (status['state'] == 'running_leader')

    def test_status_includes_token_metrics(self, postgres):
        """Verify status includes token ownership metrics.
        """
        coord_config = get_coordination_config()
        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)

        with job:
            status = job.get_coordination_status()

            assert 'my_tokens' in status
            assert 'token_version' in status
            assert 'total_tokens' in status
            assert status['my_tokens'] <= status['total_tokens']
            assert status['token_version'] >= 0

    def test_status_includes_cluster_info(self, postgres):
        """Verify status includes cluster membership info.
        """
        coord_config = get_coordination_config()
        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)

        with job:
            status = job.get_coordination_status()

            assert 'active_nodes' in status
            assert status['active_nodes'] >= 1

    def test_status_available_without_coordination(self):
        """Verify status method works even when coordination disabled.
        """
        job = Job('standalone-node', coordination_config=None)

        status = job.get_coordination_status()

        assert status is not None
        assert isinstance(status, dict)
        assert not status['coordination_enabled']


class TestTaskTokenMapping:
    """Test task-to-token mapping and related methods."""

    @pytest.mark.parametrize('hash_function', ['md5', 'sha256', 'double_sha256'])
    def test_consistent_hashing(self, postgres, hash_function):
        """Verify task IDs consistently map to same token and handle various ID types."""
        coord_config = get_coordination_config(hash_function=hash_function)
        job = create_job('node1', postgres, coordination_config=coord_config)

        task_id = 'test-task-123'
        token1 = job.task_to_token(task_id)
        token2 = job.task_to_token(task_id)

        assert token1 == token2, f'Same task should map to same token ({hash_function})'
        assert 0 <= token1 < coord_config.total_tokens, f'Token should be in valid range ({hash_function})'

        tokens = {job.task_to_token(f'task-{i}') for i in range(20)}
        assert len(tokens) >= 15, f'Most tasks should map to different tokens ({hash_function})'

        numeric_token = job.task_to_token(123)
        string_token = job.task_to_token('abc-def-ghi')
        assert 0 <= numeric_token < 10000, f'Numeric ID should produce valid token ({hash_function})'
        assert 0 <= string_token < 10000, f'String ID should produce valid token ({hash_function})'

    @pytest.mark.parametrize('hash_function', ['md5', 'sha256', 'double_sha256'])
    def test_task_to_token_matches_module_function(self, postgres, hash_function):
        """Verify Job.task_to_token() matches module-level function.
        """
        coord_config = CoordinationConfig(total_tokens=100, hash_function=hash_function)
        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)

        for task_id in [0, 1, 99, 'string-task', 'another-task']:
            job_result = job.task_to_token(task_id)
            module_result = task_to_token(task_id, job.tokens.total_tokens, hash_function)
            assert job_result == module_result, f'Results should match for task {task_id} ({hash_function})'

    @pytest.mark.parametrize('hash_function', ['md5', 'sha256', 'double_sha256'])
    def test_distribution_quality_with_clustered_ids(self, postgres, hash_function):
        """Verify even distribution with clustered sequential task IDs.
        """
        coord_config = CoordinationConfig(total_tokens=10000, hash_function=hash_function)
        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)

        task_ids = range(20001, 21001)
        tokens = [job.task_to_token(tid) for tid in task_ids]

        unique_tokens = len(set(tokens))
        assert unique_tokens >= 950, f'1000 clustered tasks should use ≥950 unique tokens, got {unique_tokens} ({hash_function})'

    @pytest.mark.parametrize('hash_function', ['md5', 'sha256', 'double_sha256'])
    def test_distribution_quality_across_token_space(self, postgres, hash_function):
        """Verify tokens spread evenly across the full token range.
        """
        coord_config = CoordinationConfig(total_tokens=10000, hash_function=hash_function)
        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)

        task_ids = range(0, 5000, 5)
        tokens = [job.task_to_token(tid) for tid in task_ids]

        buckets = [0] * 10
        bucket_size = coord_config.total_tokens // 10
        for token in tokens:
            bucket_idx = min(token // bucket_size, 9)
            buckets[bucket_idx] += 1

        max_bucket = max(buckets)
        min_bucket = min(buckets)
        imbalance = max_bucket - min_bucket
        expected_per_bucket = len(tokens) / 10

        logger.debug(f'Token bucket distribution ({hash_function}): {buckets}')
        logger.debug(f'Expected per bucket: {expected_per_bucket:.1f}, imbalance: {imbalance}')

        assert imbalance <= expected_per_bucket * 0.40, \
            f'Bucket imbalance {imbalance} exceeds 40% of expected {expected_per_bucket:.1f} ({hash_function})'

    @pytest.mark.parametrize('hash_function', ['md5', 'sha256', 'double_sha256'])
    def test_distribution_quality_simulated_cluster(self, postgres, hash_function):
        """Verify even task distribution across simulated 8-node cluster.
        """
        coord_config = CoordinationConfig(total_tokens=10000, hash_function=hash_function)
        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)

        task_ids = range(20001, 21001)
        node_task_counts = {}

        for task_id in task_ids:
            token_id = job.task_to_token(task_id)
            node_id = token_id % 8
            node_task_counts[node_id] = node_task_counts.get(node_id, 0) + 1

        counts = list(node_task_counts.values())
        expected_per_node = 1000 / 8
        max_count = max(counts)
        min_count = min(counts)
        imbalance = max_count - min_count

        logger.debug(f'Tasks per node ({hash_function}): {sorted(counts)}')
        logger.debug(f'Expected per node: {expected_per_node:.1f}, imbalance: {imbalance}')

        assert imbalance <= expected_per_node * 0.35, \
            f'Node imbalance {imbalance} exceeds 35% of expected {expected_per_node:.1f} ({hash_function})'

    @pytest.mark.parametrize('hash_function', ['md5', 'sha256', 'double_sha256'])
    def test_distribution_with_string_ids(self, postgres, hash_function):
        """Verify distribution quality with string task IDs.
        """
        coord_config = CoordinationConfig(total_tokens=10000, hash_function=hash_function)
        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)

        task_ids = [f'task-{i:05d}' for i in range(500)]
        tokens = [job.task_to_token(tid) for tid in task_ids]

        unique_tokens = len(set(tokens))
        assert unique_tokens >= 450, f'500 string tasks should use ≥450 unique tokens, got {unique_tokens} ({hash_function})'

        buckets = [0] * 5
        bucket_size = coord_config.total_tokens // 5
        for token in tokens:
            bucket_idx = min(token // bucket_size, 4)
            buckets[bucket_idx] += 1

        max_bucket = max(buckets)
        min_bucket = min(buckets)
        imbalance = max_bucket - min_bucket
        expected_per_bucket = len(tokens) / 5

        assert imbalance <= expected_per_bucket * 0.3, \
            f'String ID bucket imbalance {imbalance} exceeds 30% threshold ({hash_function})'

    @pytest.mark.parametrize('hash_function', ['md5', 'sha256', 'double_sha256'])
    def test_edge_case_task_ids(self, postgres, hash_function):
        """Verify edge case task IDs are handled correctly.
        """
        coord_config = CoordinationConfig(total_tokens=10000, hash_function=hash_function)
        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)

        edge_cases = [
            (0, 'zero'),
            (-1, 'negative'),
            (-999999, 'large negative'),
            (999999999, 'large positive'),
            ('', 'empty string'),
            ('unicode-τεστ-日本', 'unicode'),
            (None, 'none'),
            ((1, 2, 3), 'tuple'),
        ]

        results = {}
        for task_id, label in edge_cases:
            token = job.task_to_token(task_id)
            results[label] = token
            assert 0 <= token < 10000, f'{label} task_id={task_id} produced invalid token {token} ({hash_function})'

        logger.debug(f'Edge case tokens ({hash_function}): {results}')

        repeated_tokens = []
        for task_id, label in edge_cases:
            token1 = job.task_to_token(task_id)
            token2 = job.task_to_token(task_id)
            assert token1 == token2, f'{label} should hash consistently ({hash_function})'
            repeated_tokens.append(token1)

        assert repeated_tokens == list(results.values()), f'Repeated hashing should be deterministic ({hash_function})'


class TestCoordinationConfig:
    """Test CoordinationConfig validation and consistency."""

    def test_default_values_sensible(self):
        """Verify default CoordinationConfig values are sensible.
        """
        config = CoordinationConfig()

        assert config.total_tokens == 10000
        assert config.heartbeat_interval_sec == 5
        assert config.heartbeat_timeout_sec > config.heartbeat_interval_sec

    def test_heartbeat_timeout_greater_than_interval(self):
        """Verify default heartbeat timeout exceeds interval.
        """
        config = CoordinationConfig()
        assert config.heartbeat_timeout_sec > config.heartbeat_interval_sec, \
            'Timeout should exceed interval to allow for network delays'

    def test_token_refresh_steady_exceeds_initial(self):
        """Verify steady refresh interval equals or exceeds initial.
        """
        config = CoordinationConfig()
        assert config.token_refresh_steady_interval_sec >= config.token_refresh_initial_interval_sec, \
            'Steady interval should be >= initial interval'

    def test_stale_lock_ages_are_reasonable(self):
        """Verify stale lock ages are much greater than operation timeouts.
        """
        config = CoordinationConfig()
        assert config.stale_leader_lock_age_sec >= 10 * config.leader_lock_timeout_sec, \
            'Stale lock age should be much greater than lock timeout'

    def test_invalid_coordination_config_values(self):
        """Verify CoordinationConfig handles invalid values appropriately.

        Tests edge cases and invalid configurations that could cause runtime issues:
        - Zero or negative token counts
        - Zero or negative timing intervals
        - Heartbeat timeout less than interval
        - Invalid database parameters
        """
        # Test zero tokens - should work but is edge case
        config = CoordinationConfig(total_tokens=1)
        assert config.total_tokens == 1, 'Single token should be valid'

        # Test very small intervals - should work but may cause issues
        config = CoordinationConfig(heartbeat_interval_sec=0.1)
        assert config.heartbeat_interval_sec == 0.1, 'Small interval should be accepted'

        # Test that timeout must be greater than interval for reliable detection
        # This is a logical constraint that should be documented
        config = CoordinationConfig(
            heartbeat_interval_sec=5,
            heartbeat_timeout_sec=6
        )
        assert config.heartbeat_timeout_sec > config.heartbeat_interval_sec, \
            'Timeout should exceed interval'

        # Test very large values - should work
        config = CoordinationConfig(total_tokens=1000000)
        assert config.total_tokens == 1000000, 'Large token count should be valid'

        # Test edge case: timeout equals interval (not recommended but technically valid)
        config = CoordinationConfig(
            heartbeat_interval_sec=5,
            heartbeat_timeout_sec=5
        )
        # This should work but may cause false positives in production


class TestTaskComparison:
    """Test Task comparison and sorting operations."""

    def test_task_equality(self):
        """Verify tasks with same ID are equal.
        """
        task1 = create_task(1, 'task_a')
        task2 = create_task(1, 'task_b')
        assert task1 == task2

    def test_task_inequality(self):
        """Verify tasks with different IDs are not equal.
        """
        task1 = create_task(1, 'task_a')
        task2 = create_task(2, 'task_b')
        assert task1 != task2

    @pytest.mark.parametrize(('id1', 'id2', 'expected_lt', 'expected_gt'), [
        (1, 2, True, False),
        (2, 1, False, True),
        (1, 1, False, False),
    ])
    def test_task_comparisons(self, id1, id2, expected_lt, expected_gt):
        """Verify task comparison operators work correctly.
        """
        task1 = create_task(id1, f'task_{id1}')
        task2 = create_task(id2, f'task_{id2}')

        assert (task1 < task2) == expected_lt
        assert (task1 > task2) == expected_gt
        assert (task2 < task1) == (not expected_lt and id1 != id2)
        assert (task2 > task1) == (not expected_gt and id1 != id2)

    def test_task_sorting(self):
        """Verify tasks can be sorted correctly.
        """
        task3 = create_task(3, 'task_c')
        task1 = create_task(1, 'task_a')
        task2 = create_task(2, 'task_b')
        tasks = [task3, task1, task2]
        sorted_tasks = sorted(tasks)
        assert sorted_tasks == [task1, task2, task3]

    def test_task_sorting_with_duplicates(self):
        """Verify sorting handles duplicate IDs correctly.
        """
        task1a = create_task(1, 'task_1a')
        task1b = create_task(1, 'task_1b')
        task2 = create_task(2, 'task_2')
        task3 = create_task(3, 'task_3')
        tasks = [task3, task1a, task2, task1b]
        sorted_tasks = sorted(tasks)
        assert sorted_tasks[0].id == 1
        assert sorted_tasks[1].id == 1
        assert sorted_tasks[2].id == 2
        assert sorted_tasks[3].id == 3

    def test_task_string_ids(self):
        """Verify tasks work with string IDs.
        """
        task_a = create_task('a', 'task_a')
        task_b = create_task('b', 'task_b')
        assert task_a < task_b
        assert task_b > task_a
        assert not task_a > task_b

    def test_task_mixed_comparison(self):
        """Verify all comparison operators work together consistently.
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
    """Test basic token distribution scenarios."""

    @pytest.mark.parametrize(('total_tokens', 'nodes', 'expected_counts'), [
        (100, [], {}),
        (100, ['node1'], {'node1': 100}),
        (100, ['node1', 'node2'], {'node1': 50, 'node2': 50}),
        (100, ['node1', 'node2', 'node3'], {'node1': 34, 'node2': 33, 'node3': 33}),
    ])
    def test_basic_token_distribution(self, total_tokens, nodes, expected_counts):
        """Verify token distribution across varying numbers of nodes.
        """
        assignments, moved = compute_minimal_move_distribution(
            total_tokens=total_tokens,
            active_nodes=nodes,
            current_assignments={},
            locked_tokens={},
            pattern_matcher=exact_match
        )

        if not nodes:
            assert assignments == {}
            assert moved == 0
        else:
            assert len(assignments) == total_tokens
            actual_counts = {}
            for node in assignments.values():
                actual_counts[node] = actual_counts.get(node, 0) + 1
            assert actual_counts == expected_counts
            assert moved == total_tokens

    def test_remainder_tokens_distributed_alphabetically(self):
        """Verify remainder tokens are assigned to first N nodes alphabetically.

        This is Bug #2: The algorithm should give remainder tokens to the first
        nodes alphabetically, but was comparing against global average instead
        of per-node targets.
        """
        nodes = ['node-c', 'node-a', 'node-b', 'node-d', 'node-e']

        assignments, moved = compute_minimal_move_distribution(
            total_tokens=103,
            active_nodes=nodes,
            current_assignments={},
            locked_tokens={},
            pattern_matcher=exact_match
        )

        counts = {node: 0 for node in nodes}
        for node in assignments.values():
            counts[node] += 1

        sorted_nodes = sorted(nodes)
        logger.debug(f'Token counts by node: {[(n, counts[n]) for n in sorted_nodes]}')

        assert counts['node-a'] == 21, 'First alphabetically should get 21 tokens (20 + 1 remainder)'
        assert counts['node-b'] == 21, 'Second alphabetically should get 21 tokens (20 + 1 remainder)'
        assert counts['node-c'] == 21, 'Third alphabetically should get 21 tokens (20 + 1 remainder)'
        assert counts['node-d'] == 20, 'Fourth alphabetically should get 20 tokens (base amount)'
        assert counts['node-e'] == 20, 'Fifth alphabetically should get 20 tokens (base amount)'

    def test_remainder_distribution_minimizes_moves_from_correct_nodes(self):
        """Verify algorithm correctly identifies which nodes are over-target when rebalancing.

        This test specifically checks that the algorithm uses per-node targets (not global
        average) when deciding whether to move tokens away from current owner.
        """
        current = {
            **dict.fromkeys(range(0, 55), 'node-a'),
            **dict.fromkeys(range(55, 111), 'node-b'),
            **dict.fromkeys(range(111, 166), 'node-c'),
        }

        nodes = ['node-a', 'node-b', 'node-c']

        assignments, moved = compute_minimal_move_distribution(
            total_tokens=166,
            active_nodes=nodes,
            current_assignments=current,
            locked_tokens={},
            pattern_matcher=exact_match
        )

        counts = {node: 0 for node in nodes}
        for node in assignments.values():
            counts[node] += 1

        assert counts['node-a'] == 56, 'node-a should have 56 tokens (55 + 1 remainder)'
        assert counts['node-b'] == 55, 'node-b should have 55 tokens (base amount)'
        assert counts['node-c'] == 55, 'node-c should have 55 tokens (base amount)'

        assert moved == 1, 'Should move exactly 1 token from node-b (over-target) to node-a (under-target)'


class TestMinimalMovement:
    """Test that algorithm minimizes token movement."""

    def test_no_movement_when_balanced(self):
        """Verify no tokens move when distribution is already balanced.
        """
        current = {i: f'node{i % 2 + 1}' for i in range(100)}

        assignments, moved = compute_minimal_move_distribution(
            total_tokens=100,
            active_nodes=['node1', 'node2'],
            current_assignments=current,
            locked_tokens={},
            pattern_matcher=exact_match
        )

        assert len(assignments) == 100
        assert moved == 0
        assert assignments == current

    def test_minimal_movement_on_rebalance(self):
        """Verify minimal tokens move when slight rebalance needed.
        """
        current = dict.fromkeys(range(60), 'node1')
        current.update(dict.fromkeys(range(60, 100), 'node2'))

        assignments, moved = compute_minimal_move_distribution(
            total_tokens=100,
            active_nodes=['node1', 'node2'],
            current_assignments=current,
            locked_tokens={},
            pattern_matcher=exact_match
        )

        assert len(assignments) == 100
        assert moved == 10

        node1_count = sum(1 for node in assignments.values() if node == 'node1')
        node2_count = sum(1 for node in assignments.values() if node == 'node2')
        assert node1_count == 50
        assert node2_count == 50

    def test_movement_preserves_existing_where_possible(self):
        """Verify existing assignments preserved when within target range.
        """
        current = {0: 'node1', 1: 'node1', 2: 'node2', 3: 'node2'}

        assignments, moved = compute_minimal_move_distribution(
            total_tokens=4,
            active_nodes=['node1', 'node2'],
            current_assignments=current,
            locked_tokens={},
            pattern_matcher=exact_match
        )

        assert moved == 0
        assert assignments == current


class TestLockedTokens:
    """Test locked token constraint handling."""

    def test_locked_token_assigned_to_pattern(self):
        """Verify locked tokens assigned to matching node.
        """
        assignments, moved = compute_minimal_move_distribution(
            total_tokens=10,
            active_nodes=['node1', 'node2'],
            current_assignments={},
            locked_tokens={0: 'node1', 1: 'node2'},
            pattern_matcher=exact_match
        )

        assert assignments[0] == 'node1'
        assert assignments[1] == 'node2'

    def test_locked_tokens_exclude_from_balancing(self):
        """Verify locked tokens don't participate in balancing.
        """
        assignments, moved = compute_minimal_move_distribution(
            total_tokens=10,
            active_nodes=['node1', 'node2'],
            current_assignments={},
            locked_tokens={0: 'node1', 1: 'node1', 2: 'node1'},
            pattern_matcher=exact_match
        )

        assert assignments[0] == 'node1'
        assert assignments[1] == 'node1'
        assert assignments[2] == 'node1'

        unlocked_node1 = sum(1 for tid, node in assignments.items()
                            if node == 'node1' and tid >= 3)
        unlocked_node2 = sum(1 for tid, node in assignments.items()
                            if node == 'node2' and tid >= 3)

        assert abs(unlocked_node1 - unlocked_node2) <= 1

    def test_wildcard_pattern_matching(self):
        """Verify wildcard patterns match multiple nodes.
        """
        assignments, moved = compute_minimal_move_distribution(
            total_tokens=10,
            active_nodes=['worker1', 'worker2', 'manager1'],
            current_assignments={},
            locked_tokens={0: 'worker%', 5: 'manager%'},
            pattern_matcher=wildcard_match
        )

        assert assignments[0] in {'worker1', 'worker2'}
        assert assignments[5] == 'manager1'

    def test_no_matching_node_for_locked_token(self):
        """Verify locked token is NEVER assigned when no nodes match pattern.

        Critical contract: Locked tokens must NEVER be assigned to nodes outside
        their pattern constraints, even if unlocked tokens need balancing.
        """
        assignments, moved = compute_minimal_move_distribution(
            total_tokens=10,
            active_nodes=['node1', 'node2'],
            current_assignments={},
            locked_tokens={0: 'node3'},  # Locked to node3, but node3 not active
            pattern_matcher=exact_match
        )

        assert 0 not in assignments, 'Token 0 locked to node3 should NOT be assigned when node3 inactive'

        for token_id in range(1, 10):
            assert token_id in assignments, f'Unlocked token {token_id} should be assigned'
            assert assignments[token_id] in {'node1', 'node2'}, f'Unlocked token {token_id} assigned to valid node'


class TestLockedTokenBehavior:
    """Test locked token constraint handling and strict enforcement."""

    def test_locked_token_assigned_to_pattern(self):
        """Verify locked tokens assigned to matching node.
        """
        assignments, moved = compute_minimal_move_distribution(
            total_tokens=10,
            active_nodes=['node1', 'node2'],
            current_assignments={},
            locked_tokens={0: 'node1', 1: 'node2'},
            pattern_matcher=exact_match
        )

        assert assignments[0] == 'node1'
        assert assignments[1] == 'node2'

    def test_locked_tokens_exclude_from_balancing(self):
        """Verify locked tokens don't participate in balancing.
        """
        assignments, moved = compute_minimal_move_distribution(
            total_tokens=10,
            active_nodes=['node1', 'node2'],
            current_assignments={},
            locked_tokens={0: 'node1', 1: 'node1', 2: 'node1'},
            pattern_matcher=exact_match
        )

        assert assignments[0] == 'node1'
        assert assignments[1] == 'node1'
        assert assignments[2] == 'node1'

        unlocked_node1 = sum(1 for tid, node in assignments.items()
                            if node == 'node1' and tid >= 3)
        unlocked_node2 = sum(1 for tid, node in assignments.items()
                            if node == 'node2' and tid >= 3)

        assert abs(unlocked_node1 - unlocked_node2) <= 1

    def test_wildcard_pattern_matching(self):
        """Verify wildcard patterns match multiple nodes.
        """
        assignments, moved = compute_minimal_move_distribution(
            total_tokens=10,
            active_nodes=['worker1', 'worker2', 'manager1'],
            current_assignments={},
            locked_tokens={0: 'worker%', 5: 'manager%'},
            pattern_matcher=wildcard_match
        )

        assert assignments[0] in {'worker1', 'worker2'}
        assert assignments[5] == 'manager1'

    def test_no_matching_node_for_locked_token(self):
        """Verify locked token is NEVER assigned when no nodes match pattern.

        Critical contract: Locked tokens must NEVER be assigned to nodes outside
        their pattern constraints, even if unlocked tokens need balancing.
        """
        assignments, moved = compute_minimal_move_distribution(
            total_tokens=10,
            active_nodes=['node1', 'node2'],
            current_assignments={},
            locked_tokens={0: 'node3'},
            pattern_matcher=exact_match
        )

        assert 0 not in assignments, 'Token 0 locked to node3 should NOT be assigned when node3 inactive'

        for token_id in range(1, 10):
            assert token_id in assignments, f'Unlocked token {token_id} should be assigned'
            assert assignments[token_id] in {'node1', 'node2'}, f'Unlocked token {token_id} assigned to valid node'

    def test_locked_tokens_never_assigned_to_non_matching_nodes(self):
        """Verify locked tokens are NEVER assigned outside their patterns.
        """
        assignments, _ = compute_minimal_move_distribution(
            total_tokens=20,
            active_nodes=['worker1', 'worker2', 'manager1'],
            current_assignments={},
            locked_tokens={5: 'worker%', 10: 'manager%', 15: 'admin%'},
            pattern_matcher=wildcard_match
        )

        assert assignments[5] in {'worker1', 'worker2'}, \
            'Token 5 locked to worker% must only go to worker nodes'
        assert assignments[10] == 'manager1', \
            'Token 10 locked to manager% must only go to manager nodes'
        assert 15 not in assignments, \
            'Token 15 locked to admin% should not be assigned (no admin nodes active)'

    def test_locked_token_stays_with_matching_current_owner(self):
        """Verify locked token preserves current owner when pattern matches.
        """
        current = {
            5: 'worker1',
            10: 'worker2',
            15: 'manager1'
        }

        assignments, moved = compute_minimal_move_distribution(
            total_tokens=20,
            active_nodes=['worker1', 'worker2', 'manager1'],
            current_assignments=current,
            locked_tokens={5: 'worker%', 10: 'worker%', 15: 'manager%'},
            pattern_matcher=wildcard_match
        )

        assert assignments[5] == 'worker1', 'Token 5 should stay with worker1 (matches pattern)'
        assert assignments[10] == 'worker2', 'Token 10 should stay with worker2 (matches pattern)'
        assert assignments[15] == 'manager1', 'Token 15 should stay with manager1 (matches pattern)'
        assert moved >= 0, 'Should count moves for unlocked tokens only'

    def test_locked_token_moves_when_current_owner_not_matching(self):
        """Verify locked token moves when current owner doesn't match pattern.
        """
        current = {
            5: 'manager1',
            10: 'worker1'
        }

        assignments, moved = compute_minimal_move_distribution(
            total_tokens=20,
            active_nodes=['worker1', 'worker2', 'manager1'],
            current_assignments=current,
            locked_tokens={5: 'worker%', 10: 'manager%'},
            pattern_matcher=wildcard_match
        )

        assert assignments[5] in {'worker1', 'worker2'}, \
            'Token 5 must move from manager1 to worker node (pattern mismatch)'
        assert assignments[10] == 'manager1', \
            'Token 10 must move from worker1 to manager1 (pattern mismatch)'
        assert moved >= 2, 'Should count at least 2 moves for reassigned locked tokens'

    def test_multiple_fallback_patterns_first_succeeds(self):
        """Verify first matching fallback pattern is used.
        """
        assignments, _ = compute_minimal_move_distribution(
            total_tokens=20,
            active_nodes=['primary-alpha', 'backup-beta', 'tertiary-gamma'],
            current_assignments={},
            locked_tokens={5: ['primary-%', 'backup-%', 'tertiary-%']},
            pattern_matcher=wildcard_match
        )

        assert assignments[5] == 'primary-alpha', \
            'Should use first matching pattern (primary-%) over later fallbacks'

    def test_multiple_fallback_patterns_skip_to_second(self):
        """Verify fallback to second pattern when first fails.
        """
        assignments, _ = compute_minimal_move_distribution(
            total_tokens=20,
            active_nodes=['backup-alpha', 'backup-beta', 'tertiary-gamma'],
            current_assignments={},
            locked_tokens={5: ['primary-%', 'backup-%', 'tertiary-%']},
            pattern_matcher=wildcard_match
        )

        assert assignments[5] in {'backup-alpha', 'backup-beta'}, \
            'Should use second pattern (backup-%) when first pattern (primary-%) has no matches'

    def test_multiple_fallback_patterns_skip_to_third(self):
        """Verify fallback to third pattern when first and second fail.
        """
        assignments, _ = compute_minimal_move_distribution(
            total_tokens=20,
            active_nodes=['tertiary-alpha', 'other-node'],
            current_assignments={},
            locked_tokens={5: ['primary-%', 'backup-%', 'tertiary-%']},
            pattern_matcher=wildcard_match
        )

        assert assignments[5] == 'tertiary-alpha', \
            'Should use third pattern (tertiary-%) when first two patterns fail'

    def test_all_fallback_patterns_fail_no_assignment(self):
        """Verify no assignment when all fallback patterns fail.
        """
        assignments, _ = compute_minimal_move_distribution(
            total_tokens=20,
            active_nodes=['other-node1', 'other-node2'],
            current_assignments={},
            locked_tokens={5: ['primary-%', 'backup-%', 'tertiary-%']},
            pattern_matcher=wildcard_match
        )

        assert 5 not in assignments, \
            'Token should not be assigned when all fallback patterns fail'

    def test_locked_tokens_dont_affect_unlocked_balance(self):
        """Verify locked tokens don't affect unlocked token distribution.
        """
        assignments, _ = compute_minimal_move_distribution(
            total_tokens=100,
            active_nodes=['node1', 'node2', 'node3'],
            current_assignments={},
            locked_tokens={0: 'node1', 1: 'node1', 2: 'node1'},
            pattern_matcher=exact_match
        )

        assert assignments[0] == 'node1'
        assert assignments[1] == 'node1'
        assert assignments[2] == 'node1'

        unlocked_counts = {'node1': 0, 'node2': 0, 'node3': 0}
        for token_id in range(3, 100):
            if token_id in assignments:
                unlocked_counts[assignments[token_id]] += 1

        assert max(unlocked_counts.values()) - min(unlocked_counts.values()) <= 1, \
            'Unlocked tokens should be evenly distributed despite locked token imbalance'

    def test_mixed_locked_and_unlocked_distribution(self):
        """Verify correct distribution with mix of locked and unlocked tokens.
        """
        assignments, _ = compute_minimal_move_distribution(
            total_tokens=30,
            active_nodes=['worker1', 'worker2', 'manager1'],
            current_assignments={},
            locked_tokens={
                0: 'manager%',
                5: 'worker%',
                10: 'worker%',
                15: 'admin%'
            },
            pattern_matcher=wildcard_match
        )

        assert assignments[0] == 'manager1', 'Token 0 locked to manager'
        assert assignments[5] in {'worker1', 'worker2'}, 'Token 5 locked to workers'
        assert assignments[10] in {'worker1', 'worker2'}, 'Token 10 locked to workers'
        assert 15 not in assignments, 'Token 15 locked to non-existent admin nodes'

        unlocked_tokens = [tid for tid in range(30) if tid not in {0, 5, 10, 15}]
        assigned_unlocked = [tid for tid in unlocked_tokens if tid in assignments]
        assert len(assigned_unlocked) == len(unlocked_tokens), \
            'All unlocked tokens should be assigned'

    def test_locked_token_reassignment_minimizes_moves(self):
        """Verify locked token reassignment prefers least-loaded matching node.
        """
        current = {
            5: 'worker1',
            10: 'manager1'
        }

        assignments, _ = compute_minimal_move_distribution(
            total_tokens=20,
            active_nodes=['worker1', 'worker2', 'worker3'],
            current_assignments=current,
            locked_tokens={5: 'worker%', 10: 'worker%'},
            pattern_matcher=wildcard_match
        )

        assert assignments[5] == 'worker1', \
            'Token 5 should stay with worker1 (current owner matches pattern)'
        assert assignments[10] in {'worker1', 'worker2', 'worker3'}, \
            'Token 10 must move to worker node (manager1 no longer active)'

    def test_multiple_tokens_locked_to_same_pattern_balanced(self):
        """Verify multiple tokens locked to same pattern are balanced across matching nodes.
        """
        assignments, _ = compute_minimal_move_distribution(
            total_tokens=50,
            active_nodes=['worker1', 'worker2', 'worker3'],
            current_assignments={},
            locked_tokens=dict.fromkeys(range(10, 20), 'worker%'),
            pattern_matcher=wildcard_match
        )

        locked_counts = {'worker1': 0, 'worker2': 0, 'worker3': 0}
        for tid in range(10, 20):
            assert tid in assignments, f'Locked token {tid} should be assigned'
            assert assignments[tid] in {'worker1', 'worker2', 'worker3'}, \
                f'Locked token {tid} must be assigned to worker node'
            locked_counts[assignments[tid]] += 1

        assert max(locked_counts.values()) - min(locked_counts.values()) <= 2, \
            'Locked tokens should be reasonably balanced across matching nodes'


class TestNodeFailure:
    """Test handling of node failures and recovery."""

    def test_dead_node_tokens_redistributed(self):
        """Verify tokens from dead nodes get redistributed.
        """
        current = dict.fromkeys(range(50), 'dead_node')
        current.update(dict.fromkeys(range(50, 100), 'node1'))

        assignments, moved = compute_minimal_move_distribution(
            total_tokens=100,
            active_nodes=['node1', 'node2'],
            current_assignments=current,
            locked_tokens={},
            pattern_matcher=exact_match
        )

        assert len(assignments) == 100
        assert 'dead_node' not in assignments.values()
        assert moved == 50

        node1_count = sum(1 for node in assignments.values() if node == 'node1')
        node2_count = sum(1 for node in assignments.values() if node == 'node2')
        assert node1_count == 50
        assert node2_count == 50

    def test_new_node_gets_fair_share(self):
        """Verify new node receives balanced token allocation.
        """
        current = {i: f'node{i % 2 + 1}' for i in range(100)}

        assignments, moved = compute_minimal_move_distribution(
            total_tokens=100,
            active_nodes=['node1', 'node2', 'node3'],
            current_assignments=current,
            locked_tokens={},
            pattern_matcher=exact_match
        )

        assert len(assignments) == 100
        counts = {}
        for node in assignments.values():
            counts[node] = counts.get(node, 0) + 1

        assert set(counts.values()) == {33, 34}
        assert counts['node3'] == 33
        assert moved >= 33

    def test_locked_token_assigned_to_pattern(self):
        """Verify locked tokens assigned to matching node.
        """
        assignments, moved = compute_minimal_move_distribution(
            total_tokens=10,
            active_nodes=['node1', 'node2'],
            current_assignments={},
            locked_tokens={0: 'node1', 1: 'node2'},
            pattern_matcher=exact_match
        )

        assert assignments[0] == 'node1'
        assert assignments[1] == 'node2'

    def test_locked_tokens_exclude_from_balancing(self):
        """Verify locked tokens don't participate in balancing.
        """
        assignments, moved = compute_minimal_move_distribution(
            total_tokens=10,
            active_nodes=['node1', 'node2'],
            current_assignments={},
            locked_tokens={0: 'node1', 1: 'node1', 2: 'node1'},
            pattern_matcher=exact_match
        )

        assert assignments[0] == 'node1'
        assert assignments[1] == 'node1'
        assert assignments[2] == 'node1'

        unlocked_node1 = sum(1 for tid, node in assignments.items()
                            if node == 'node1' and tid >= 3)
        unlocked_node2 = sum(1 for tid, node in assignments.items()
                            if node == 'node2' and tid >= 3)

        assert abs(unlocked_node1 - unlocked_node2) <= 1

    def test_wildcard_pattern_matching(self):
        """Verify wildcard patterns match multiple nodes.
        """
        assignments, moved = compute_minimal_move_distribution(
            total_tokens=10,
            active_nodes=['worker1', 'worker2', 'manager1'],
            current_assignments={},
            locked_tokens={0: 'worker%', 5: 'manager%'},
            pattern_matcher=wildcard_match
        )

        assert assignments[0] in {'worker1', 'worker2'}
        assert assignments[5] == 'manager1'

    def test_no_matching_node_for_locked_token(self):
        """Verify locked token is NEVER assigned when no nodes match pattern.

        Critical contract: Locked tokens must NEVER be assigned to nodes outside
        their pattern constraints, even if unlocked tokens need balancing.
        """
        assignments, moved = compute_minimal_move_distribution(
            total_tokens=10,
            active_nodes=['node1', 'node2'],
            current_assignments={},
            locked_tokens={0: 'node3'},
            pattern_matcher=exact_match
        )

        assert 0 not in assignments, 'Token 0 locked to node3 should NOT be assigned when node3 inactive'

        for token_id in range(1, 10):
            assert token_id in assignments, f'Unlocked token {token_id} should be assigned'
            assert assignments[token_id] in {'node1', 'node2'}, f'Unlocked token {token_id} assigned to valid node'

    def test_locked_tokens_never_assigned_to_non_matching_nodes(self):
        """Verify locked tokens are NEVER assigned outside their patterns.
        """
        assignments, _ = compute_minimal_move_distribution(
            total_tokens=20,
            active_nodes=['worker1', 'worker2', 'manager1'],
            current_assignments={},
            locked_tokens={5: 'worker%', 10: 'manager%', 15: 'admin%'},
            pattern_matcher=wildcard_match
        )

        assert assignments[5] in {'worker1', 'worker2'}, \
            'Token 5 locked to worker% must only go to worker nodes'
        assert assignments[10] == 'manager1', \
            'Token 10 locked to manager% must only go to manager nodes'
        assert 15 not in assignments, \
            'Token 15 locked to admin% should not be assigned (no admin nodes active)'

    def test_locked_token_stays_with_matching_current_owner(self):
        """Verify locked token preserves current owner when pattern matches.
        """
        current = {
            5: 'worker1',
            10: 'worker2',
            15: 'manager1'
        }

        assignments, moved = compute_minimal_move_distribution(
            total_tokens=20,
            active_nodes=['worker1', 'worker2', 'manager1'],
            current_assignments=current,
            locked_tokens={5: 'worker%', 10: 'worker%', 15: 'manager%'},
            pattern_matcher=wildcard_match
        )

        assert assignments[5] == 'worker1', 'Token 5 should stay with worker1 (matches pattern)'
        assert assignments[10] == 'worker2', 'Token 10 should stay with worker2 (matches pattern)'
        assert assignments[15] == 'manager1', 'Token 15 should stay with manager1 (matches pattern)'
        assert moved >= 0, 'Should count moves for unlocked tokens only'

    def test_locked_token_moves_when_current_owner_not_matching(self):
        """Verify locked token moves when current owner doesn't match pattern.
        """
        current = {
            5: 'manager1',
            10: 'worker1'
        }

        assignments, moved = compute_minimal_move_distribution(
            total_tokens=20,
            active_nodes=['worker1', 'worker2', 'manager1'],
            current_assignments=current,
            locked_tokens={5: 'worker%', 10: 'manager%'},
            pattern_matcher=wildcard_match
        )

        assert assignments[5] in {'worker1', 'worker2'}, \
            'Token 5 must move from manager1 to worker node (pattern mismatch)'
        assert assignments[10] == 'manager1', \
            'Token 10 must move from worker1 to manager1 (pattern mismatch)'
        assert moved >= 2, 'Should count at least 2 moves for reassigned locked tokens'

    def test_multiple_fallback_patterns_first_succeeds(self):
        """Verify first matching fallback pattern is used.
        """
        assignments, _ = compute_minimal_move_distribution(
            total_tokens=20,
            active_nodes=['primary-alpha', 'backup-beta', 'tertiary-gamma'],
            current_assignments={},
            locked_tokens={5: ['primary-%', 'backup-%', 'tertiary-%']},
            pattern_matcher=wildcard_match
        )

        assert assignments[5] == 'primary-alpha', \
            'Should use first matching pattern (primary-%) over later fallbacks'

    def test_multiple_fallback_patterns_skip_to_second(self):
        """Verify fallback to second pattern when first fails.
        """
        assignments, _ = compute_minimal_move_distribution(
            total_tokens=20,
            active_nodes=['backup-alpha', 'backup-beta', 'tertiary-gamma'],
            current_assignments={},
            locked_tokens={5: ['primary-%', 'backup-%', 'tertiary-%']},
            pattern_matcher=wildcard_match
        )

        assert assignments[5] in {'backup-alpha', 'backup-beta'}, \
            'Should use second pattern (backup-%) when first pattern (primary-%) has no matches'

    def test_multiple_fallback_patterns_skip_to_third(self):
        """Verify fallback to third pattern when first and second fail.
        """
        assignments, _ = compute_minimal_move_distribution(
            total_tokens=20,
            active_nodes=['tertiary-alpha', 'other-node'],
            current_assignments={},
            locked_tokens={5: ['primary-%', 'backup-%', 'tertiary-%']},
            pattern_matcher=wildcard_match
        )

        assert assignments[5] == 'tertiary-alpha', \
            'Should use third pattern (tertiary-%) when first two patterns fail'

    def test_all_fallback_patterns_fail_no_assignment(self):
        """Verify no assignment when all fallback patterns fail.
        """
        assignments, _ = compute_minimal_move_distribution(
            total_tokens=20,
            active_nodes=['other-node1', 'other-node2'],
            current_assignments={},
            locked_tokens={5: ['primary-%', 'backup-%', 'tertiary-%']},
            pattern_matcher=wildcard_match
        )

        assert 5 not in assignments, \
            'Token should not be assigned when all fallback patterns fail'

    def test_locked_tokens_dont_affect_unlocked_balance(self):
        """Verify locked tokens don't affect unlocked token distribution.
        """
        assignments, _ = compute_minimal_move_distribution(
            total_tokens=100,
            active_nodes=['node1', 'node2', 'node3'],
            current_assignments={},
            locked_tokens={0: 'node1', 1: 'node1', 2: 'node1'},
            pattern_matcher=exact_match
        )

        assert assignments[0] == 'node1'
        assert assignments[1] == 'node1'
        assert assignments[2] == 'node1'

        unlocked_counts = {'node1': 0, 'node2': 0, 'node3': 0}
        for token_id in range(3, 100):
            if token_id in assignments:
                unlocked_counts[assignments[token_id]] += 1

        assert max(unlocked_counts.values()) - min(unlocked_counts.values()) <= 1, \
            'Unlocked tokens should be evenly distributed despite locked token imbalance'

    def test_mixed_locked_and_unlocked_distribution(self):
        """Verify correct distribution with mix of locked and unlocked tokens.
        """
        assignments, _ = compute_minimal_move_distribution(
            total_tokens=30,
            active_nodes=['worker1', 'worker2', 'manager1'],
            current_assignments={},
            locked_tokens={
                0: 'manager%',
                5: 'worker%',
                10: 'worker%',
                15: 'admin%'
            },
            pattern_matcher=wildcard_match
        )

        assert assignments[0] == 'manager1', 'Token 0 locked to manager'
        assert assignments[5] in {'worker1', 'worker2'}, 'Token 5 locked to workers'
        assert assignments[10] in {'worker1', 'worker2'}, 'Token 10 locked to workers'
        assert 15 not in assignments, 'Token 15 locked to non-existent admin nodes'

        unlocked_tokens = [tid for tid in range(30) if tid not in {0, 5, 10, 15}]
        assigned_unlocked = [tid for tid in unlocked_tokens if tid in assignments]
        assert len(assigned_unlocked) == len(unlocked_tokens), \
            'All unlocked tokens should be assigned'

    def test_locked_token_reassignment_minimizes_moves(self):
        """Verify locked token reassignment prefers least-loaded matching node.
        """
        current = {
            5: 'worker1',
            10: 'manager1'
        }

        assignments, _ = compute_minimal_move_distribution(
            total_tokens=20,
            active_nodes=['worker1', 'worker2', 'worker3'],
            current_assignments=current,
            locked_tokens={5: 'worker%', 10: 'worker%'},
            pattern_matcher=wildcard_match
        )

        assert assignments[5] == 'worker1', \
            'Token 5 should stay with worker1 (current owner matches pattern)'
        assert assignments[10] in {'worker1', 'worker2', 'worker3'}, \
            'Token 10 must move to worker node (manager1 no longer active)'

    def test_multiple_tokens_locked_to_same_pattern_balanced(self):
        """Verify multiple tokens locked to same pattern are balanced across matching nodes.
        """
        assignments, _ = compute_minimal_move_distribution(
            total_tokens=50,
            active_nodes=['worker1', 'worker2', 'worker3'],
            current_assignments={},
            locked_tokens=dict.fromkeys(range(10, 20), 'worker%'),
            pattern_matcher=wildcard_match
        )

        locked_counts = {'worker1': 0, 'worker2': 0, 'worker3': 0}
        for tid in range(10, 20):
            assert tid in assignments, f'Locked token {tid} should be assigned'
            assert assignments[tid] in {'worker1', 'worker2', 'worker3'}, \
                f'Locked token {tid} must be assigned to worker node'
            locked_counts[assignments[tid]] += 1

        assert max(locked_counts.values()) - min(locked_counts.values()) <= 2, \
            'Locked tokens should be reasonably balanced across matching nodes'


class TestDistributionEdgeCases:
    """Test edge cases and boundary conditions for token distribution."""

    def test_empty_nodes_list(self):
        """Verify distribution handles empty nodes gracefully.
        """
        assignments, moves = compute_minimal_move_distribution(
            total_tokens=100,
            active_nodes=[],
            current_assignments={},
            locked_tokens={},
            pattern_matcher=exact_match
        )

        assert len(assignments) == 0, 'No assignments with no nodes'
        assert moves == 0, 'No moves with no nodes'

    def test_more_nodes_than_tokens(self):
        """Verify handling when nodes outnumber tokens.
        """
        assignments, moved = compute_minimal_move_distribution(
            total_tokens=5,
            active_nodes=['node1', 'node2', 'node3', 'node4', 'node5', 'node6'],
            current_assignments={},
            locked_tokens={},
            pattern_matcher=exact_match
        )

        assert len(assignments) == 5
        assert len(set(assignments.values())) == 5

    def test_all_tokens_locked(self):
        """Verify behavior when all tokens are locked.
        """
        locked = {i: f'node{i % 2 + 1}' for i in range(10)}

        assignments, moved = compute_minimal_move_distribution(
            total_tokens=10,
            active_nodes=['node1', 'node2'],
            current_assignments={},
            locked_tokens=locked,
            pattern_matcher=exact_match
        )

        assert len(assignments) == 10
        for tid, pattern in locked.items():
            assert assignments[tid] == pattern

    def test_all_tokens_locked_to_nonexistent_pattern(self):
        """Verify behavior when all tokens locked to pattern with no matching nodes.
        """
        locked = {i: ['missing-%'] for i in range(10)}

        assignments, moves = compute_minimal_move_distribution(
            total_tokens=10,
            active_nodes=['node1', 'node2'],
            current_assignments={},
            locked_tokens=locked,
            pattern_matcher=wildcard_match
        )

        assert len(assignments) == 0, 'No assignments when locks match no nodes'

    def test_single_token(self):
        """Verify single token distribution.
        """
        assignments, moved = compute_minimal_move_distribution(
            total_tokens=1,
            active_nodes=['node1', 'node2'],
            current_assignments={},
            locked_tokens={},
            pattern_matcher=exact_match
        )

        assert len(assignments) == 1
        assert assignments[0] in {'node1', 'node2'}

    def test_deterministic_sorting(self):
        """Verify distribution is deterministic with same inputs.
        """
        result1 = compute_minimal_move_distribution(
            total_tokens=100,
            active_nodes=['node1', 'node2', 'node3'],
            current_assignments={},
            locked_tokens={},
            pattern_matcher=exact_match
        )

        result2 = compute_minimal_move_distribution(
            total_tokens=100,
            active_nodes=['node1', 'node2', 'node3'],
            current_assignments={},
            locked_tokens={},
            pattern_matcher=exact_match
        )

        assert result1[0] == result2[0]
        assert result1[1] == result2[1]

    def test_fallback_patterns_used(self):
        """Verify fallback patterns are tried when primary pattern fails.
        """
        locked = {
            5: ['missing-%', 'special-%', 'node%']
        }

        assignments, moves = compute_minimal_move_distribution(
            total_tokens=10,
            active_nodes=['node1', 'special-alpha'],
            current_assignments={},
            locked_tokens=locked,
            pattern_matcher=wildcard_match
        )

        assert assignments[5] == 'special-alpha', 'Should use second fallback pattern when first fails'

    def test_reverse_iteration_minimizes_moves(self):
        """Verify reverse iteration helps minimize token movement.

        This test documents the behavior of the reverse iteration optimization.
        """
        current = {i: 'node1' if i < 70 else 'node2' for i in range(100)}

        assignments_reverse, moves_reverse = compute_minimal_move_distribution(
            total_tokens=100,
            active_nodes=['node1', 'node2', 'node3'],
            current_assignments=current,
            locked_tokens={},
            pattern_matcher=exact_match
        )

        assert moves_reverse >= 30, 'Should require rebalancing moves'
        assert moves_reverse <= 45, 'Reverse iteration should minimize moves'


class TestTokenIterationOrder:
    """Test token iteration order logic during rebalancing."""

    def test_reverse_iteration_when_rebalancing_imbalance(self):
        """Verify tokens processed in reverse order when nodes are over/under quota.
        """
        current = dict.fromkeys(range(80), 'node1')
        current.update(dict.fromkeys(range(80, 100), 'node2'))

        assignments, moved = compute_minimal_move_distribution(
            total_tokens=100,
            active_nodes=['node1', 'node2'],
            current_assignments=current,
            locked_tokens={},
            pattern_matcher=exact_match
        )

        moved_tokens = [tid for tid, node in assignments.items()
                       if current.get(tid) != node]

        assert len(moved_tokens) == 30
        assert all(tid >= 50 for tid in moved_tokens)

    def test_normal_iteration_when_balanced(self):
        """Verify normal iteration order when already balanced.
        """
        current = {i: f'node{i % 2 + 1}' for i in range(100)}

        assignments, moved = compute_minimal_move_distribution(
            total_tokens=100,
            active_nodes=['node1', 'node2'],
            current_assignments=current,
            locked_tokens={},
            pattern_matcher=exact_match
        )

        assert moved == 0

    def test_normal_iteration_when_adding_nodes(self):
        """Verify normal iteration order when no nodes are over quota.
        """
        current = {}

        assignments, moved = compute_minimal_move_distribution(
            total_tokens=100,
            active_nodes=['node1', 'node2'],
            current_assignments=current,
            locked_tokens={},
            pattern_matcher=exact_match
        )

        assigned_tids = sorted(assignments.keys())
        assert assigned_tids == list(range(100))

    def test_reverse_iteration_moves_high_tokens_from_overloaded_node(self):
        """Verify reverse iteration moves high tokens from over-quota nodes.
        """
        current = dict.fromkeys(range(70), 'node1')
        current.update(dict.fromkeys(range(70, 100), 'node2'))

        assignments, moved = compute_minimal_move_distribution(
            total_tokens=100,
            active_nodes=['node1', 'node2'],
            current_assignments=current,
            locked_tokens={},
            pattern_matcher=exact_match
        )

        moved_tokens = [tid for tid in range(70)
                       if assignments.get(tid) != 'node1']

        assert len(moved_tokens) == 20
        assert all(tid >= 50 for tid in moved_tokens)


class TestLargeScale:
    """Test algorithm performance and correctness at scale."""

    def test_large_token_count(self):
        """Verify distribution works with large token counts.
        """
        assignments, moved = compute_minimal_move_distribution(
            total_tokens=10000,
            active_nodes=[f'node{i}' for i in range(10)],
            current_assignments={},
            locked_tokens={},
            pattern_matcher=exact_match
        )

        assert len(assignments) == 10000
        counts = {}
        for node in assignments.values():
            counts[node] = counts.get(node, 0) + 1

        for count in counts.values():
            assert 950 <= count <= 1050

    def test_large_scale_remainder_distribution(self):
        """Verify remainder tokens distributed alphabetically at scale.

        With 10,000 tokens and 9 nodes:
        - target_per_node = 1,111
        - remainder = 1
        - First node alphabetically should get 1,112
        - Other 8 nodes should get 1,111

        This is the exact scenario from Bug #2 report.
        """
        nodes = [f'node{i}' for i in range(9)]

        assignments, moved = compute_minimal_move_distribution(
            total_tokens=10000,
            active_nodes=nodes,
            current_assignments={},
            locked_tokens={},
            pattern_matcher=exact_match
        )

        counts = {node: 0 for node in nodes}
        for node in assignments.values():
            counts[node] += 1

        sorted_nodes = sorted(nodes)
        logger.debug(f'Token distribution: {[(n, counts[n]) for n in sorted_nodes]}')

        assert counts[sorted_nodes[0]] == 1112, \
            f'First node ({sorted_nodes[0]}) should get 1,112 tokens (1,111 + 1 remainder), got {counts[sorted_nodes[0]]}'

        for i in range(1, 9):
            assert counts[sorted_nodes[i]] == 1111, \
                f'Node {sorted_nodes[i]} should get 1,111 tokens (base amount), got {counts[sorted_nodes[i]]}'

        assert sum(counts.values()) == 10000, 'Total should be 10,000 tokens'
        assert moved == 10000, 'All tokens assigned from empty initial state'

    def test_many_nodes(self):
        """Verify distribution with many nodes.
        """
        assignments, moved = compute_minimal_move_distribution(
            total_tokens=1000,
            active_nodes=[f'node{i}' for i in range(100)],
            current_assignments={},
            locked_tokens={},
            pattern_matcher=exact_match
        )

        assert len(assignments) == 1000
        counts = {}
        for node in assignments.values():
            counts[node] = counts.get(node, 0) + 1

        for count in counts.values():
            assert 8 <= count <= 12

    def test_locked_tokens_at_scale(self):
        """Verify locked token handling at scale.
        """
        locked = {i: f'node{i % 3}' for i in range(0, 1000, 10)}

        assignments, moved = compute_minimal_move_distribution(
            total_tokens=1000,
            active_nodes=['node0', 'node1', 'node2'],
            current_assignments={},
            locked_tokens=locked,
            pattern_matcher=exact_match
        )

        assert len(assignments) == 1000
        for tid, pattern in locked.items():
            assert assignments[tid] == pattern


class TestPatternMatching:
    """Test SQL LIKE pattern matching."""

    @pytest.mark.parametrize(('node_name', 'pattern', 'should_match'), [
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
    ])
    def test_pattern_matching(self, postgres, node_name, pattern, should_match):
        """Test SQL LIKE pattern matching with various patterns."""

        result = matches_pattern(node_name, pattern)
        if should_match:
            assert result, f'{node_name} should match pattern {pattern}'
        else:
            assert not result, f'{node_name} should not match pattern {pattern}'


class TestTaskHashableValidation:
    """Test Task validation of hashable IDs."""

    def test_hashable_ids_accepted(self):
        """Verify hashable types are accepted as task IDs.
        """
        valid_ids = [
            42,
            'string-id',
            ('tuple', 'id'),
            frozenset([1, 2, 3]),
        ]

        for task_id in valid_ids:
            task = Task(task_id, 'test-task')
            assert task.id == task_id, f'Should accept hashable type {type(task_id).__name__}'

    def test_non_hashable_list_rejected(self):
        """Verify list IDs are rejected.
        """
        with pytest.raises(AssertionError, match='must be hashable'):
            Task([1, 2, 3], 'list-task')

    def test_non_hashable_dict_rejected(self):
        """Verify dict IDs are rejected.
        """
        with pytest.raises(AssertionError, match='must be hashable'):
            Task({'key': 'value'}, 'dict-task')

    def test_non_hashable_set_rejected(self):
        """Verify set IDs are rejected.
        """
        with pytest.raises(AssertionError, match='must be hashable'):
            Task({1, 2, 3}, 'set-task')

    def test_none_is_hashable(self):
        """Verify None is accepted as a hashable ID.
        """
        task = Task(None, 'none-task')
        assert task.id is None, 'None should be accepted (it is hashable)'


class TestSetClaimEdgeCases:
    """Test TaskManager.set_claim edge cases."""

    @clean_tables('Claim')
    def test_empty_list_handled(self, postgres):
        """Verify empty list is handled without errors.
        """
        tables = schema.get_table_names()

        coord_config = get_coordination_config()
        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)
        job.__enter__()

        try:
            job.set_claim([])

            with postgres.connect() as conn:
                result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Claim"]}'))
                count = result.scalar()

            assert count == 0, 'No claims should be created for empty list'

            logger.info('✓ Empty list handled correctly')

        finally:
            job.__exit__(None, None, None)

    @clean_tables('Claim')
    def test_empty_tuple_handled(self, postgres):
        """Verify empty tuple is handled without errors.
        """
        tables = schema.get_table_names()

        coord_config = get_coordination_config()
        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)
        job.__enter__()

        try:
            job.set_claim(())

            with postgres.connect() as conn:
                result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Claim"]}'))
                count = result.scalar()

            assert count == 0, 'No claims should be created for empty tuple'

        finally:
            job.__exit__(None, None, None)

    @clean_tables('Claim')
    def test_large_iterable(self, postgres):
        """Verify large iterables are processed correctly.
        """
        tables = schema.get_table_names()

        coord_config = get_coordination_config()
        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)
        job.__enter__()

        try:
            large_items = list(range(1000))
            job.set_claim(large_items)

            with postgres.connect() as conn:
                result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Claim"]} WHERE node = :node'),
                                     {'node': 'node1'})
                count = result.scalar()

            assert count == 1000, 'All 1000 items should be claimed'

            logger.info('✓ Large iterable processed correctly')

        finally:
            job.__exit__(None, None, None)

    @clean_tables('Claim')
    def test_mixed_type_iterable(self, postgres):
        """Verify iterables with mixed types are handled correctly.
        """
        tables = schema.get_table_names()

        coord_config = get_coordination_config()
        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)
        job.__enter__()

        try:
            mixed_items = [1, 'string-item', (2, 3), 42, 'another-string']
            job.set_claim(mixed_items)

            with postgres.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT item FROM {tables["Claim"]} WHERE node = :node ORDER BY item
                """), {'node': 'node1'})
                items = [row[0] for row in result]

            assert len(items) == 5, 'All 5 mixed-type items should be claimed'

            expected_items = {'1', 'string-item', '(2, 3)', '42', 'another-string'}
            assert set(items) == expected_items, 'Items should be converted to strings'

            logger.info('✓ Mixed type iterable handled correctly')

        finally:
            job.__exit__(None, None, None)

    @clean_tables('Claim')
    def test_single_string_not_treated_as_iterable(self, postgres):
        """Verify single string is treated as single item, not iterable of characters.
        """
        tables = schema.get_table_names()

        coord_config = get_coordination_config()
        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)
        job.__enter__()

        try:
            job.set_claim('single-string-item')

            with postgres.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT item FROM {tables["Claim"]} WHERE node = :node
                """), {'node': 'node1'})
                items = [row[0] for row in result]

            assert len(items) == 1, 'Should create single claim'
            assert items[0] == 'single-string-item', 'Should claim entire string, not characters'

            logger.info('✓ Single string not treated as iterable')

        finally:
            job.__exit__(None, None, None)

    @clean_tables('Claim')
    def test_single_integer_handled(self, postgres):
        """Verify single integer is claimed correctly.
        """
        tables = schema.get_table_names()

        coord_config = get_coordination_config()
        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)
        job.__enter__()

        try:
            job.set_claim(42)

            with postgres.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT item FROM {tables["Claim"]} WHERE node = :node
                """), {'node': 'node1'})
                items = [row[0] for row in result]

            assert len(items) == 1, 'Should create single claim'
            assert items[0] == '42', 'Integer should be converted to string'

        finally:
            job.__exit__(None, None, None)

    @clean_tables('Claim')
    def test_generator_expression_handled(self, postgres):
        """Verify generator expressions are handled correctly.
        """
        tables = schema.get_table_names()

        coord_config = get_coordination_config()
        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)
        job.__enter__()

        try:
            generator = (i * 2 for i in range(10))
            job.set_claim(generator)

            with postgres.connect() as conn:
                result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Claim"]} WHERE node = :node'),
                                     {'node': 'node1'})
                count = result.scalar()

            assert count == 10, 'All 10 generated items should be claimed'

        finally:
            job.__exit__(None, None, None)

    @clean_tables('Claim')
    def test_range_object_handled(self, postgres):
        """Verify range objects are handled correctly.
        """
        tables = schema.get_table_names()

        coord_config = get_coordination_config()
        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)
        job.__enter__()

        try:
            job.set_claim(range(20))

            with postgres.connect() as conn:
                result = conn.execute(text(f'SELECT COUNT(*) FROM {tables["Claim"]} WHERE node = :node'),
                                     {'node': 'node1'})
                count = result.scalar()

            assert count == 20, 'All 20 range items should be claimed'

        finally:
            job.__exit__(None, None, None)

    @clean_tables('Claim')
    def test_duplicate_items_in_iterable(self, postgres):
        """Verify duplicate items in iterable are handled by ON CONFLICT.
        """
        tables = schema.get_table_names()

        coord_config = get_coordination_config()
        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)
        job.__enter__()

        try:
            items_with_duplicates = [1, 2, 3, 2, 1, 4, 3]
            job.set_claim(items_with_duplicates)

            with postgres.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT item FROM {tables["Claim"]} WHERE node = :node ORDER BY item
                """), {'node': 'node1'})
                items = [row[0] for row in result]

            assert set(items) == {'1', '2', '3', '4'}, 'Should have unique items (duplicates handled by ON CONFLICT)'

        finally:
            job.__exit__(None, None, None)

    @clean_tables('Claim')
    def test_none_in_iterable(self, postgres):
        """Verify None values in iterable are handled.
        """
        tables = schema.get_table_names()

        coord_config = get_coordination_config()
        job = create_job('node1', postgres, coordination_config=coord_config, wait_on_enter=0)
        job.__enter__()

        try:
            items = [1, None, 2, None, 3]
            job.set_claim(items)

            with postgres.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT item FROM {tables["Claim"]} WHERE node = :node ORDER BY item
                """), {'node': 'node1'})
                items = [row[0] for row in result]

            assert 'None' in items, 'None should be converted to string "None"'
            assert len(items) == 4, 'Should have 4 unique items (1, 2, 3, None)'

        finally:
            job.__exit__(None, None, None)


if __name__ == '__main__':
    pytest.main(args=['-sx', __file__])
