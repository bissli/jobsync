import pytest

from jobsync.client import Task


def test_task_equality():
    """Verify tasks with same ID are equal.
    """
    task1 = Task(1, "task_a")
    task2 = Task(1, "task_b")
    assert task1 == task2


def test_task_inequality():
    """Verify tasks with different IDs are not equal.
    """
    task1 = Task(1, "task_a")
    task2 = Task(2, "task_b")
    assert task1 != task2


def test_task_less_than():
    """Verify less than comparison works correctly.
    """
    task1 = Task(1, "task_a")
    task2 = Task(2, "task_b")
    assert task1 < task2
    assert not task2 < task1


def test_task_greater_than():
    """Verify greater than comparison works correctly.
    """
    task1 = Task(1, "task_a")
    task2 = Task(2, "task_b")
    assert task2 > task1
    assert not task1 > task2


def test_task_equal_not_greater_than():
    """Verify equal tasks are not greater than each other (exposes bug).
    """
    task1 = Task(1, "task_a")
    task2 = Task(1, "task_b")
    assert task1 == task2
    assert not task1 > task2
    assert not task2 > task1


def test_task_equal_not_less_than():
    """Verify equal tasks are not less than each other.
    """
    task1 = Task(1, "task_a")
    task2 = Task(1, "task_b")
    assert task1 == task2
    assert not task1 < task2
    assert not task2 < task1


def test_task_sorting():
    """Verify tasks can be sorted correctly.
    """
    task3 = Task(3, "task_c")
    task1 = Task(1, "task_a")
    task2 = Task(2, "task_b")
    tasks = [task3, task1, task2]
    sorted_tasks = sorted(tasks)
    assert sorted_tasks == [task1, task2, task3]


def test_task_sorting_with_duplicates():
    """Verify sorting handles duplicate IDs correctly.
    """
    task1a = Task(1, "task_1a")
    task1b = Task(1, "task_1b")
    task2 = Task(2, "task_2")
    task3 = Task(3, "task_3")
    tasks = [task3, task1a, task2, task1b]
    sorted_tasks = sorted(tasks)
    assert sorted_tasks[0].id == 1
    assert sorted_tasks[1].id == 1
    assert sorted_tasks[2].id == 2
    assert sorted_tasks[3].id == 3


def test_task_string_ids():
    """Verify tasks work with string IDs.
    """
    task_a = Task("a", "task_a")
    task_b = Task("b", "task_b")
    assert task_a < task_b
    assert task_b > task_a
    assert not task_a > task_b


def test_task_mixed_comparison():
    """Verify all comparison operators work together consistently.
    """
    task1 = Task(1, "task_1")
    task2 = Task(2, "task_2")
    
    assert task1 < task2
    assert task1 <= task2
    assert task2 > task1
    assert task2 >= task1
    assert task1 != task2
    
    task1_dup = Task(1, "task_1_dup")
    assert task1 == task1_dup
    assert task1 <= task1_dup
    assert task1 >= task1_dup
    assert not task1 < task1_dup
    assert not task1 > task1_dup


if __name__ == '__main__':
    pytest.main(args=['-sx', __file__])
