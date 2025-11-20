"""Unit tests for JobStateMachine state transitions and callbacks.

Tests verify:
- Valid state transitions work correctly
- Invalid transitions are rejected
- Callbacks fire on state entry/exit
- State-dependent behavior (can_claim_task)
"""
import logging

import pytest
from asserts import assert_equal, assert_false, assert_true

from jobsync.client import JobState, JobStateMachine

logger = logging.getLogger(__name__)


class TestStateTransitions:
    """Test valid and invalid state transitions."""

    def test_initial_state(self):
        """Verify state machine starts in INITIALIZING state.
        """
        sm = JobStateMachine()
        assert_equal(sm.state, JobState.INITIALIZING, 'Should start in INITIALIZING state')

    def test_valid_transition_sequence(self):
        """Verify standard lifecycle transitions work.
        """
        sm = JobStateMachine()
        
        assert_true(sm.transition_to(JobState.CLUSTER_FORMING), 'Should transition to CLUSTER_FORMING')
        assert_equal(sm.state, JobState.CLUSTER_FORMING)
        
        assert_true(sm.transition_to(JobState.ELECTING), 'Should transition to ELECTING')
        assert_equal(sm.state, JobState.ELECTING)
        
        assert_true(sm.transition_to(JobState.DISTRIBUTING), 'Should transition to DISTRIBUTING')
        assert_equal(sm.state, JobState.DISTRIBUTING)
        
        assert_true(sm.transition_to(JobState.RUNNING_FOLLOWER), 'Should transition to RUNNING_FOLLOWER')
        assert_equal(sm.state, JobState.RUNNING_FOLLOWER)
        
        assert_true(sm.transition_to(JobState.SHUTTING_DOWN), 'Should transition to SHUTTING_DOWN')
        assert_equal(sm.state, JobState.SHUTTING_DOWN)

    def test_leader_lifecycle_transitions(self):
        """Verify leader node lifecycle transitions work.
        """
        sm = JobStateMachine()
        
        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ELECTING)
        sm.transition_to(JobState.DISTRIBUTING)
        
        assert_true(sm.transition_to(JobState.RUNNING_LEADER), 'Should transition to RUNNING_LEADER')
        assert_equal(sm.state, JobState.RUNNING_LEADER)
        
        assert_true(sm.transition_to(JobState.SHUTTING_DOWN), 'Should transition to SHUTTING_DOWN')
        assert_equal(sm.state, JobState.SHUTTING_DOWN)

    def test_leader_follower_transitions(self):
        """Verify transitions between RUNNING_LEADER and RUNNING_FOLLOWER.
        """
        sm = JobStateMachine()
        
        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ELECTING)
        sm.transition_to(JobState.DISTRIBUTING)
        sm.transition_to(JobState.RUNNING_FOLLOWER)
        
        assert_true(sm.transition_to(JobState.RUNNING_LEADER), 'Follower should promote to leader')
        assert_equal(sm.state, JobState.RUNNING_LEADER)
        
        assert_true(sm.transition_to(JobState.RUNNING_FOLLOWER), 'Leader should demote to follower')
        assert_equal(sm.state, JobState.RUNNING_FOLLOWER)

    def test_invalid_transition_rejected(self):
        """Verify invalid transitions return False and don't change state.
        """
        sm = JobStateMachine()
        
        result = sm.transition_to(JobState.RUNNING_LEADER)
        assert_false(result, 'Should reject invalid transition')
        assert_equal(sm.state, JobState.INITIALIZING, 'State should not change on invalid transition')

    def test_skip_invalid_state_rejected(self):
        """Verify skipping required states is rejected.
        """
        sm = JobStateMachine()
        sm.transition_to(JobState.CLUSTER_FORMING)
        
        result = sm.transition_to(JobState.DISTRIBUTING)
        assert_false(result, 'Should reject skipping ELECTING state')
        assert_equal(sm.state, JobState.CLUSTER_FORMING, 'State should remain unchanged')

    def test_backward_transition_rejected(self):
        """Verify backward transitions are rejected.
        """
        sm = JobStateMachine()
        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ELECTING)
        
        result = sm.transition_to(JobState.CLUSTER_FORMING)
        assert_false(result, 'Should reject backward transition')
        assert_equal(sm.state, JobState.ELECTING, 'State should remain unchanged')

    def test_transition_to_same_state_succeeds(self):
        """Verify transitioning to current state is allowed (idempotent).
        """
        sm = JobStateMachine()
        
        result = sm.transition_to(JobState.INITIALIZING)
        assert_true(result, 'Transition to same state should succeed')
        assert_equal(sm.state, JobState.INITIALIZING)

    def test_shutting_down_only_from_running_states(self):
        """Verify SHUTTING_DOWN only reachable from RUNNING states.
        """
        sm = JobStateMachine()
        
        result = sm.transition_to(JobState.SHUTTING_DOWN)
        assert_false(result, 'Cannot shutdown from INITIALIZING')
        
        sm.transition_to(JobState.CLUSTER_FORMING)
        result = sm.transition_to(JobState.SHUTTING_DOWN)
        assert_false(result, 'Cannot shutdown from CLUSTER_FORMING')


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
        
        assert_equal(len(callback_invoked), 1, 'Callback should be invoked once')

    def test_on_exit_callback_invoked(self):
        """Verify on_exit callback fires when exiting state.
        """
        sm = JobStateMachine()
        callback_invoked = []
        
        def callback():
            callback_invoked.append(True)
        
        sm.on_exit(JobState.CLUSTER_FORMING, callback)
        sm.transition_to(JobState.CLUSTER_FORMING)
        
        assert_equal(len(callback_invoked), 0, 'Exit callback not invoked yet')
        
        sm.transition_to(JobState.ELECTING)
        assert_equal(len(callback_invoked), 1, 'Exit callback should be invoked')

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
        assert_equal(enter_count[0], 1, 'Enter callback invoked on entry')
        assert_equal(exit_count[0], 0, 'Exit callback not invoked yet')
        
        sm.transition_to(JobState.ELECTING)
        assert_equal(enter_count[0], 1, 'Enter callback not invoked again')
        assert_equal(exit_count[0], 1, 'Exit callback invoked on exit')

    def test_callback_not_invoked_on_invalid_transition(self):
        """Verify callbacks not invoked when transition fails.
        """
        sm = JobStateMachine()
        callback_invoked = []
        
        def callback():
            callback_invoked.append(True)
        
        sm.on_enter(JobState.RUNNING_LEADER, callback)
        sm.transition_to(JobState.RUNNING_LEADER)
        
        assert_equal(len(callback_invoked), 0, 'Callback should not be invoked for invalid transition')

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
        
        assert_equal(enter_count[0], 0, 'Enter callback should not fire for same-state transition')
        assert_equal(exit_count[0], 0, 'Exit callback should not fire for same-state transition')

    def test_callback_exception_handling(self):
        """Verify exceptions in callbacks don't prevent state transition.
        """
        sm = JobStateMachine()
        
        def failing_callback():
            raise RuntimeError('Test exception')
        
        sm.on_enter(JobState.CLUSTER_FORMING, failing_callback)
        
        with pytest.raises(RuntimeError):
            sm.transition_to(JobState.CLUSTER_FORMING)
        
        assert_equal(sm.state, JobState.CLUSTER_FORMING, 'State should change despite callback exception')

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
        
        assert_equal(call_order, ['exit', 'enter'], 'Exit should be called before enter')


class TestCanClaimTask:
    """Test state-dependent task claiming behavior."""

    def test_cannot_claim_in_initializing(self):
        """Verify task claiming disallowed in INITIALIZING state.
        """
        sm = JobStateMachine()
        assert_false(sm.can_claim_task(), 'Cannot claim tasks in INITIALIZING')

    def test_cannot_claim_in_cluster_forming(self):
        """Verify task claiming disallowed in CLUSTER_FORMING state.
        """
        sm = JobStateMachine()
        sm.transition_to(JobState.CLUSTER_FORMING)
        assert_false(sm.can_claim_task(), 'Cannot claim tasks in CLUSTER_FORMING')

    def test_cannot_claim_in_electing(self):
        """Verify task claiming disallowed in ELECTING state.
        """
        sm = JobStateMachine()
        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ELECTING)
        assert_false(sm.can_claim_task(), 'Cannot claim tasks in ELECTING')

    def test_cannot_claim_in_distributing(self):
        """Verify task claiming disallowed in DISTRIBUTING state.
        """
        sm = JobStateMachine()
        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ELECTING)
        sm.transition_to(JobState.DISTRIBUTING)
        assert_false(sm.can_claim_task(), 'Cannot claim tasks in DISTRIBUTING')

    def test_can_claim_in_running_follower(self):
        """Verify task claiming allowed in RUNNING_FOLLOWER state.
        """
        sm = JobStateMachine()
        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ELECTING)
        sm.transition_to(JobState.DISTRIBUTING)
        sm.transition_to(JobState.RUNNING_FOLLOWER)
        assert_true(sm.can_claim_task(), 'Can claim tasks in RUNNING_FOLLOWER')

    def test_can_claim_in_running_leader(self):
        """Verify task claiming allowed in RUNNING_LEADER state.
        """
        sm = JobStateMachine()
        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ELECTING)
        sm.transition_to(JobState.DISTRIBUTING)
        sm.transition_to(JobState.RUNNING_LEADER)
        assert_true(sm.can_claim_task(), 'Can claim tasks in RUNNING_LEADER')

    def test_cannot_claim_in_shutting_down(self):
        """Verify task claiming disallowed in SHUTTING_DOWN state.
        """
        sm = JobStateMachine()
        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ELECTING)
        sm.transition_to(JobState.DISTRIBUTING)
        sm.transition_to(JobState.RUNNING_FOLLOWER)
        sm.transition_to(JobState.SHUTTING_DOWN)
        assert_false(sm.can_claim_task(), 'Cannot claim tasks in SHUTTING_DOWN')

    def test_can_claim_follows_state_changes(self):
        """Verify can_claim_task reflects current state correctly.
        """
        sm = JobStateMachine()
        
        assert_false(sm.can_claim_task(), 'Initial state: cannot claim')
        
        sm.transition_to(JobState.CLUSTER_FORMING)
        sm.transition_to(JobState.ELECTING)
        sm.transition_to(JobState.DISTRIBUTING)
        sm.transition_to(JobState.RUNNING_FOLLOWER)
        
        assert_true(sm.can_claim_task(), 'Running state: can claim')
        
        sm.transition_to(JobState.RUNNING_LEADER)
        assert_true(sm.can_claim_task(), 'Leader state: can claim')
        
        sm.transition_to(JobState.RUNNING_FOLLOWER)
        assert_true(sm.can_claim_task(), 'Back to follower: can claim')
        
        sm.transition_to(JobState.SHUTTING_DOWN)
        assert_false(sm.can_claim_task(), 'Shutting down: cannot claim')


class TestTransitionValidation:
    """Test transition validation logic."""

    def test_all_valid_transitions_defined(self):
        """Verify all expected valid transitions are defined in transition graph.
        """
        sm = JobStateMachine()
        
        expected_transitions = [
            (JobState.INITIALIZING, JobState.CLUSTER_FORMING),
            (JobState.CLUSTER_FORMING, JobState.ELECTING),
            (JobState.ELECTING, JobState.DISTRIBUTING),
            (JobState.DISTRIBUTING, JobState.RUNNING_LEADER),
            (JobState.DISTRIBUTING, JobState.RUNNING_FOLLOWER),
            (JobState.RUNNING_FOLLOWER, JobState.RUNNING_LEADER),
            (JobState.RUNNING_LEADER, JobState.RUNNING_FOLLOWER),
            (JobState.RUNNING_LEADER, JobState.SHUTTING_DOWN),
            (JobState.RUNNING_FOLLOWER, JobState.SHUTTING_DOWN),
        ]
        
        for from_state, to_state in expected_transitions:
            sm.state = from_state
            result = sm.transition_to(to_state)
            assert_true(result, f'Transition {from_state.value} -> {to_state.value} should be valid')


if __name__ == '__main__':
    pytest.main(args=['-sx', __file__])
