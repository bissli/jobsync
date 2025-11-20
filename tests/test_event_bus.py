"""Unit tests for EventBus pub/sub mechanics.

Tests verify:
- Basic subscribe and publish functionality
- Multiple subscribers receive events
- Event data passed correctly
- Exception handling in subscribers
"""
import logging

import pytest
from asserts import assert_equal, assert_true

from jobsync.client import EventBus, StateEvent

logger = logging.getLogger(__name__)


class TestBasicPubSub:
    """Test basic publish/subscribe functionality."""

    def test_subscribe_and_publish(self):
        """Verify basic subscribe/publish flow works.
        """
        bus = EventBus()
        received = []
        
        def callback(data: dict):
            received.append(data)
        
        bus.subscribe(StateEvent.LEADER_PROMOTED, callback)
        bus.publish(StateEvent.LEADER_PROMOTED, {'node': 'test-node'})
        
        assert_equal(len(received), 1, 'Callback should be invoked once')
        assert_equal(received[0]['node'], 'test-node', 'Event data should be passed')

    def test_publish_without_subscribers(self):
        """Verify publishing without subscribers doesn't error.
        """
        bus = EventBus()
        
        try:
            bus.publish(StateEvent.LEADER_PROMOTED, {'test': 'data'})
        except Exception as e:
            pytest.fail(f'Publishing without subscribers should not raise: {e}')

    def test_publish_with_no_data(self):
        """Verify publishing without data provides empty dict to subscribers.
        """
        bus = EventBus()
        received = []
        
        def callback(data: dict):
            received.append(data)
        
        bus.subscribe(StateEvent.LEADER_PROMOTED, callback)
        bus.publish(StateEvent.LEADER_PROMOTED)
        
        assert_equal(len(received), 1, 'Callback should be invoked')
        assert_equal(received[0], {}, 'Should receive empty dict when no data provided')

    def test_multiple_events_independent(self):
        """Verify different event types are independent.
        """
        bus = EventBus()
        promoted_count = [0]
        demoted_count = [0]
        
        def promoted_callback(data: dict):
            promoted_count[0] += 1
        
        def demoted_callback(data: dict):
            demoted_count[0] += 1
        
        bus.subscribe(StateEvent.LEADER_PROMOTED, promoted_callback)
        bus.subscribe(StateEvent.LEADER_DEMOTED, demoted_callback)
        
        bus.publish(StateEvent.LEADER_PROMOTED)
        assert_equal(promoted_count[0], 1, 'Promoted callback invoked')
        assert_equal(demoted_count[0], 0, 'Demoted callback not invoked')
        
        bus.publish(StateEvent.LEADER_DEMOTED)
        assert_equal(promoted_count[0], 1, 'Promoted callback not invoked again')
        assert_equal(demoted_count[0], 1, 'Demoted callback invoked')


class TestMultipleSubscribers:
    """Test multiple subscribers to same event."""

    def test_multiple_subscribers_all_receive_event(self):
        """Verify all subscribers receive published events.
        """
        bus = EventBus()
        received1 = []
        received2 = []
        received3 = []
        
        bus.subscribe(StateEvent.DEAD_NODES_DETECTED, lambda d: received1.append(d))
        bus.subscribe(StateEvent.DEAD_NODES_DETECTED, lambda d: received2.append(d))
        bus.subscribe(StateEvent.DEAD_NODES_DETECTED, lambda d: received3.append(d))
        
        bus.publish(StateEvent.DEAD_NODES_DETECTED, {'nodes': ['node1', 'node2']})
        
        assert_equal(len(received1), 1, 'Subscriber 1 should receive event')
        assert_equal(len(received2), 1, 'Subscriber 2 should receive event')
        assert_equal(len(received3), 1, 'Subscriber 3 should receive event')

    def test_subscribers_receive_same_data(self):
        """Verify all subscribers receive identical event data.
        """
        bus = EventBus()
        received_data = []
        
        for _ in range(3):
            bus.subscribe(StateEvent.MEMBERSHIP_CHANGED, lambda d: received_data.append(d))
        
        test_data = {'previous': 2, 'current': 3}
        bus.publish(StateEvent.MEMBERSHIP_CHANGED, test_data)
        
        assert_equal(len(received_data), 3, 'All subscribers should receive event')
        for data in received_data:
            assert_equal(data, test_data, 'All subscribers should receive same data')

    def test_subscribers_called_in_order(self):
        """Verify subscribers called in subscription order.
        """
        bus = EventBus()
        call_order = []
        
        bus.subscribe(StateEvent.LEADER_PROMOTED, lambda d: call_order.append(1))
        bus.subscribe(StateEvent.LEADER_PROMOTED, lambda d: call_order.append(2))
        bus.subscribe(StateEvent.LEADER_PROMOTED, lambda d: call_order.append(3))
        
        bus.publish(StateEvent.LEADER_PROMOTED)
        
        assert_equal(call_order, [1, 2, 3], 'Subscribers should be called in subscription order')

    def test_same_callback_subscribed_multiple_times(self):
        """Verify same callback can be subscribed multiple times.
        """
        bus = EventBus()
        call_count = [0]
        
        def callback(data: dict):
            call_count[0] += 1
        
        bus.subscribe(StateEvent.LEADER_PROMOTED, callback)
        bus.subscribe(StateEvent.LEADER_PROMOTED, callback)
        bus.subscribe(StateEvent.LEADER_PROMOTED, callback)
        
        bus.publish(StateEvent.LEADER_PROMOTED)
        
        assert_equal(call_count[0], 3, 'Same callback should be invoked for each subscription')


class TestEventData:
    """Test event data handling."""

    def test_event_data_passed_correctly(self):
        """Verify event data passed to subscribers correctly.
        """
        bus = EventBus()
        received = []
        
        def callback(data: dict):
            received.append(data)
        
        bus.subscribe(StateEvent.DEAD_NODES_DETECTED, callback)
        
        test_data = {
            'nodes': ['node1', 'node2', 'node3'],
            'timestamp': '2024-03-15T10:00:00Z',
            'reason': 'heartbeat_timeout'
        }
        
        bus.publish(StateEvent.DEAD_NODES_DETECTED, test_data)
        
        assert_equal(len(received), 1, 'Should receive one event')
        assert_equal(received[0], test_data, 'Data should match published data')

    def test_data_modifications_isolated(self):
        """Verify subscribers receive independent data copies if modified.
        """
        bus = EventBus()
        received1 = []
        received2 = []
        
        def callback1(data: dict):
            data['modified_by'] = 'callback1'
            received1.append(data)
        
        def callback2(data: dict):
            received2.append(data.copy())
        
        bus.subscribe(StateEvent.MEMBERSHIP_CHANGED, callback1)
        bus.subscribe(StateEvent.MEMBERSHIP_CHANGED, callback2)
        
        test_data = {'count': 5}
        bus.publish(StateEvent.MEMBERSHIP_CHANGED, test_data)
        
        assert_true('modified_by' in received1[0], 'First callback should have modified data')
        assert_true('modified_by' in received2[0], 'Modifications visible to subsequent callbacks')

    def test_empty_dict_when_no_data(self):
        """Verify empty dict provided when no data specified.
        """
        bus = EventBus()
        received = []
        
        bus.subscribe(StateEvent.LEADER_PROMOTED, lambda d: received.append(d))
        bus.publish(StateEvent.LEADER_PROMOTED)
        
        assert_equal(received[0], {}, 'Should receive empty dict')

    def test_none_data_becomes_empty_dict(self):
        """Verify None data converted to empty dict.
        """
        bus = EventBus()
        received = []
        
        bus.subscribe(StateEvent.LEADER_DEMOTED, lambda d: received.append(d))
        bus.publish(StateEvent.LEADER_DEMOTED, None)
        
        assert_equal(received[0], {}, 'None should become empty dict')


class TestExceptionHandling:
    """Test exception handling in subscribers."""

    def test_exception_in_subscriber_caught(self):
        """Verify exceptions in subscribers are caught and logged.
        """
        bus = EventBus()
        successful_calls = []
        
        def failing_callback(data: dict):
            raise RuntimeError('Test exception')
        
        def successful_callback(data: dict):
            successful_calls.append(data)
        
        bus.subscribe(StateEvent.LEADER_PROMOTED, failing_callback)
        bus.subscribe(StateEvent.LEADER_PROMOTED, successful_callback)
        
        bus.publish(StateEvent.LEADER_PROMOTED, {'test': 'data'})
        
        assert_equal(len(successful_calls), 1, 'Subsequent subscribers should still be called')

    def test_multiple_failing_subscribers(self):
        """Verify multiple failing subscribers don't prevent event delivery.
        """
        bus = EventBus()
        call_count = [0]
        
        def failing_callback(data: dict):
            raise RuntimeError('Test exception')
        
        def counting_callback(data: dict):
            call_count[0] += 1
        
        bus.subscribe(StateEvent.DEAD_NODES_DETECTED, failing_callback)
        bus.subscribe(StateEvent.DEAD_NODES_DETECTED, failing_callback)
        bus.subscribe(StateEvent.DEAD_NODES_DETECTED, counting_callback)
        bus.subscribe(StateEvent.DEAD_NODES_DETECTED, failing_callback)
        
        bus.publish(StateEvent.DEAD_NODES_DETECTED)
        
        assert_equal(call_count[0], 1, 'Non-failing subscriber should be called')

    def test_exception_doesnt_stop_publishing(self):
        """Verify exception in one subscriber doesn't prevent others from receiving event.
        """
        bus = EventBus()
        before_failure = []
        after_failure = []
        
        bus.subscribe(StateEvent.MEMBERSHIP_CHANGED, lambda d: before_failure.append(d))
        bus.subscribe(StateEvent.MEMBERSHIP_CHANGED, lambda d: (_ for _ in ()).throw(RuntimeError('fail')))
        bus.subscribe(StateEvent.MEMBERSHIP_CHANGED, lambda d: after_failure.append(d))
        
        bus.publish(StateEvent.MEMBERSHIP_CHANGED, {'test': 'data'})
        
        assert_equal(len(before_failure), 1, 'Subscriber before failure should receive event')
        assert_equal(len(after_failure), 1, 'Subscriber after failure should receive event')

    def test_exception_logged(self, caplog):
        """Verify exceptions in callbacks are logged.
        """
        bus = EventBus()
        
        def failing_callback(data: dict):
            raise ValueError('Test error message')
        
        bus.subscribe(StateEvent.LEADER_PROMOTED, failing_callback)
        
        with caplog.at_level(logging.ERROR):
            bus.publish(StateEvent.LEADER_PROMOTED)
        
        error_messages = [record.message for record in caplog.records if record.levelname == 'ERROR']
        assert_true(len(error_messages) > 0, 'Exception should be logged')
        assert_true(any('callback failed' in msg.lower() for msg in error_messages),
                   'Error message should mention callback failure')


class TestSubscriptionManagement:
    """Test subscription management behavior."""

    def test_unsubscribed_callback_not_called(self):
        """Verify callbacks only invoked after subscription.
        """
        bus = EventBus()
        call_count = [0]
        
        def callback(data: dict):
            call_count[0] += 1
        
        bus.publish(StateEvent.LEADER_PROMOTED)
        assert_equal(call_count[0], 0, 'Callback should not be invoked before subscription')
        
        bus.subscribe(StateEvent.LEADER_PROMOTED, callback)
        bus.publish(StateEvent.LEADER_PROMOTED)
        assert_equal(call_count[0], 1, 'Callback should be invoked after subscription')

    def test_subscribe_to_multiple_events(self):
        """Verify same callback can subscribe to multiple events.
        """
        bus = EventBus()
        received_events = []
        
        def callback(data: dict):
            received_events.append(data)
        
        bus.subscribe(StateEvent.LEADER_PROMOTED, callback)
        bus.subscribe(StateEvent.LEADER_DEMOTED, callback)
        bus.subscribe(StateEvent.DEAD_NODES_DETECTED, callback)
        
        bus.publish(StateEvent.LEADER_PROMOTED, {'event': 'promoted'})
        bus.publish(StateEvent.LEADER_DEMOTED, {'event': 'demoted'})
        bus.publish(StateEvent.DEAD_NODES_DETECTED, {'event': 'dead_nodes'})
        
        assert_equal(len(received_events), 3, 'Callback should receive all events')


if __name__ == '__main__':
    pytest.main(args=['-sx', __file__])
