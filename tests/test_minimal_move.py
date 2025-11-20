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


class TestMinimalMoveDistributionEdgeCases:
    """Test minimal-move distribution algorithm edge cases from production scenarios.
    """

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

        # Each node should get ~33 tokens, so expect ~34 moves minimum
        assert moves_reverse >= 30, 'Should require rebalancing moves'
        assert moves_reverse <= 45, 'Reverse iteration should minimize moves'


if __name__ == '__main__':
    __import__('pytest').main([__file__])
