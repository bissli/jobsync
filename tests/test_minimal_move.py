"""Unit tests for minimal-move token distribution algorithm."""
import pytest

from jobsync.client import compute_minimal_move_distribution


def exact_match(node: str, pattern: str) -> bool:
    """Simple pattern matcher for testing (exact match only).
    """
    return node == pattern


def wildcard_match(node: str, pattern: str) -> bool:
    """Pattern matcher that supports % wildcard (SQL LIKE style).
    """
    if pattern == node:
        return True
    if '%' not in pattern:
        return False
    if pattern.endswith('%'):
        return node.startswith(pattern[:-1])
    if pattern.startswith('%'):
        return node.endswith(pattern[1:])
    return False


class TestBasicDistribution:
    """Test basic token distribution scenarios."""

    def test_empty_nodes(self):
        """Verify distribution with no nodes returns empty assignment.
        """
        assignments, moved = compute_minimal_move_distribution(
            total_tokens=100,
            active_nodes=[],
            current_assignments={},
            locked_tokens={},
            pattern_matcher=exact_match
        )
        
        assert assignments == {}
        assert moved == 0

    def test_single_node(self):
        """Verify all tokens assigned to single node.
        """
        assignments, moved = compute_minimal_move_distribution(
            total_tokens=100,
            active_nodes=['node1'],
            current_assignments={},
            locked_tokens={},
            pattern_matcher=exact_match
        )
        
        assert len(assignments) == 100
        assert all(node == 'node1' for node in assignments.values())
        assert moved == 100

    def test_two_nodes_even_split(self):
        """Verify even distribution between two nodes.
        """
        assignments, moved = compute_minimal_move_distribution(
            total_tokens=100,
            active_nodes=['node1', 'node2'],
            current_assignments={},
            locked_tokens={},
            pattern_matcher=exact_match
        )
        
        assert len(assignments) == 100
        node1_count = sum(1 for node in assignments.values() if node == 'node1')
        node2_count = sum(1 for node in assignments.values() if node == 'node2')
        
        assert node1_count == 50
        assert node2_count == 50
        assert moved == 100

    def test_three_nodes_uneven_split(self):
        """Verify distribution with remainder tokens.
        """
        assignments, moved = compute_minimal_move_distribution(
            total_tokens=100,
            active_nodes=['node1', 'node2', 'node3'],
            current_assignments={},
            locked_tokens={},
            pattern_matcher=exact_match
        )
        
        assert len(assignments) == 100
        counts = {}
        for node in assignments.values():
            counts[node] = counts.get(node, 0) + 1
        
        assert counts['node1'] == 34
        assert counts['node2'] == 33
        assert counts['node3'] == 33
        assert moved == 100


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
        current = {i: 'node1' for i in range(60)}
        current.update({i: 'node2' for i in range(60, 100)})
        
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
        
        assert assignments[0] in ['worker1', 'worker2']
        assert assignments[5] == 'manager1'

    def test_no_matching_node_for_locked_token(self):
        """Verify locked token without matching node gets no assignment in unlocked pool.
        """
        assignments, moved = compute_minimal_move_distribution(
            total_tokens=10,
            active_nodes=['node1', 'node2'],
            current_assignments={},
            locked_tokens={0: 'node3'},
            pattern_matcher=exact_match
        )
        
        assert 0 not in assignments


class TestNodeFailure:
    """Test handling of node failures and recovery."""

    def test_dead_node_tokens_redistributed(self):
        """Verify tokens from dead nodes get redistributed.
        """
        current = {i: 'dead_node' for i in range(50)}
        current.update({i: 'node1' for i in range(50, 100)})
        
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


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

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
        assert assignments[0] in ['node1', 'node2']

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
