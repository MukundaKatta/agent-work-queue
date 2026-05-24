"""Tests for agent-work-queue."""

from __future__ import annotations

import pytest

from agent_work_queue import ItemStatus, WorkItem, WorkQueue
from agent_work_queue.core import InvalidTransitionError, ItemNotFoundError

# ---------------------------------------------------------------------------
# ItemStatus
# ---------------------------------------------------------------------------


def test_status_values():
    assert ItemStatus.PENDING.value == "pending"
    assert ItemStatus.IN_PROGRESS.value == "in_progress"
    assert ItemStatus.DONE.value == "done"
    assert ItemStatus.FAILED.value == "failed"
    assert ItemStatus.DROPPED.value == "dropped"


# ---------------------------------------------------------------------------
# WorkItem — construction and serialisation
# ---------------------------------------------------------------------------


def test_work_item_minimal():
    item = WorkItem(id=1, task="fetch emails")
    assert item.id == 1
    assert item.task == "fetch emails"
    assert item.priority == 0
    assert item.status is ItemStatus.PENDING
    assert item.attempts == 0
    assert item.payload == {}


def test_work_item_to_dict():
    item = WorkItem(
        id=2,
        task="send",
        payload={"to": "alice"},
        priority=5,
        status=ItemStatus.DONE,
        attempts=1,
        created_at=1.0,
        updated_at=2.0,
    )
    d = item.to_dict()
    assert d["id"] == 2
    assert d["task"] == "send"
    assert d["payload"] == {"to": "alice"}
    assert d["priority"] == 5
    assert d["status"] == "done"
    assert d["attempts"] == 1


def test_work_item_from_dict_round_trip():
    original = WorkItem(
        id=3,
        task="t",
        payload={"k": "v"},
        priority=10,
        status=ItemStatus.FAILED,
        attempts=2,
        created_at=5.0,
        updated_at=6.0,
    )
    restored = WorkItem.from_dict(original.to_dict())
    assert restored.id == original.id
    assert restored.task == original.task
    assert restored.payload == original.payload
    assert restored.priority == original.priority
    assert restored.status is original.status
    assert restored.attempts == original.attempts


def test_work_item_repr_short():
    item = WorkItem(id=1, task="do thing")
    r = repr(item)
    assert "1" in r
    assert "do thing" in r


def test_work_item_repr_long_truncated():
    item = WorkItem(id=1, task="x" * 40)
    assert "..." in repr(item)


# ---------------------------------------------------------------------------
# WorkQueue — enqueue
# ---------------------------------------------------------------------------


def test_enqueue_assigns_id():
    q = WorkQueue(clock=lambda: 0.0)
    item = q.enqueue("task")
    assert item.id == 1


def test_enqueue_increments_id():
    q = WorkQueue(clock=lambda: 0.0)
    i1 = q.enqueue("t1")
    i2 = q.enqueue("t2")
    assert i1.id == 1
    assert i2.id == 2


def test_enqueue_with_priority():
    q = WorkQueue(clock=lambda: 0.0)
    item = q.enqueue("t", priority=7)
    assert item.priority == 7


def test_enqueue_with_payload():
    q = WorkQueue(clock=lambda: 0.0)
    item = q.enqueue("t", payload={"url": "http://x"})
    assert item.payload == {"url": "http://x"}


def test_enqueue_timestamps():
    q = WorkQueue(clock=lambda: 5.0)
    item = q.enqueue("t")
    assert item.created_at == pytest.approx(5.0)
    assert item.updated_at == pytest.approx(5.0)


# ---------------------------------------------------------------------------
# WorkQueue — dequeue
# ---------------------------------------------------------------------------


def test_dequeue_empty_returns_none():
    q = WorkQueue()
    assert q.dequeue() is None


def test_dequeue_returns_pending():
    q = WorkQueue(clock=lambda: 0.0)
    q.enqueue("task")
    item = q.dequeue()
    assert item is not None
    assert item.task == "task"


def test_dequeue_sets_in_progress():
    q = WorkQueue(clock=lambda: 0.0)
    q.enqueue("t")
    item = q.dequeue()
    assert item.status is ItemStatus.IN_PROGRESS


def test_dequeue_increments_attempts():
    q = WorkQueue(clock=lambda: 0.0)
    q.enqueue("t")
    item = q.dequeue()
    assert item.attempts == 1


def test_dequeue_priority_order():
    q = WorkQueue(clock=lambda: 0.0)
    q.enqueue("low", priority=1)
    q.enqueue("high", priority=10)
    item = q.dequeue()
    assert item.task == "high"


def test_dequeue_fifo_on_tie():
    q = WorkQueue(clock=lambda: 0.0)
    q.enqueue("first", priority=5)
    q.enqueue("second", priority=5)
    assert q.dequeue().task == "first"
    assert q.dequeue().task == "second"


def test_dequeue_skips_non_pending():
    q = WorkQueue(clock=lambda: 0.0)
    i1 = q.enqueue("t1")
    q.enqueue("t2")
    q.dequeue()  # t1 goes IN_PROGRESS
    q.complete(i1.id)  # t1 done
    item = q.dequeue()
    assert item.task == "t2"


# ---------------------------------------------------------------------------
# WorkQueue — peek
# ---------------------------------------------------------------------------


def test_peek_returns_next_without_consuming():
    q = WorkQueue(clock=lambda: 0.0)
    q.enqueue("t", priority=5)
    peeked = q.peek()
    assert peeked is not None
    assert peeked.status is ItemStatus.PENDING  # still pending
    assert q.count(ItemStatus.PENDING) == 1


def test_peek_empty_returns_none():
    q = WorkQueue()
    assert q.peek() is None


# ---------------------------------------------------------------------------
# WorkQueue — transitions
# ---------------------------------------------------------------------------


def test_complete():
    q = WorkQueue(clock=lambda: 0.0)
    q.enqueue("t")
    item = q.dequeue()
    q.complete(item.id)
    assert q.get(item.id).status is ItemStatus.DONE


def test_fail():
    q = WorkQueue(clock=lambda: 0.0)
    q.enqueue("t")
    item = q.dequeue()
    q.fail(item.id)
    assert q.get(item.id).status is ItemStatus.FAILED


def test_drop_pending():
    q = WorkQueue(clock=lambda: 0.0)
    item = q.enqueue("t")
    q.drop(item.id)
    assert q.get(item.id).status is ItemStatus.DROPPED


def test_drop_in_progress():
    q = WorkQueue(clock=lambda: 0.0)
    q.enqueue("t")
    item = q.dequeue()
    q.drop(item.id)
    assert q.get(item.id).status is ItemStatus.DROPPED


def test_retry_failed():
    q = WorkQueue(clock=lambda: 0.0)
    q.enqueue("t")
    item = q.dequeue()
    q.fail(item.id)
    q.retry(item.id)
    assert q.get(item.id).status is ItemStatus.PENDING


def test_invalid_transition_raises():
    q = WorkQueue(clock=lambda: 0.0)
    item = q.enqueue("t")
    with pytest.raises(InvalidTransitionError) as exc_info:
        q.complete(item.id)  # PENDING → DONE not allowed
    assert exc_info.value.item_id == item.id


def test_retry_not_failed_raises():
    q = WorkQueue(clock=lambda: 0.0)
    item = q.enqueue("t")
    with pytest.raises(InvalidTransitionError):
        q.retry(item.id)  # PENDING → PENDING not allowed via retry


def test_transition_unknown_raises():
    q = WorkQueue()
    with pytest.raises(ItemNotFoundError):
        q.complete(999)


# ---------------------------------------------------------------------------
# WorkQueue — retrieval
# ---------------------------------------------------------------------------


def test_get():
    q = WorkQueue(clock=lambda: 0.0)
    item = q.enqueue("t")
    assert q.get(item.id) is item


def test_get_missing_raises():
    q = WorkQueue()
    with pytest.raises(ItemNotFoundError):
        q.get(42)


def test_contains():
    q = WorkQueue(clock=lambda: 0.0)
    item = q.enqueue("t")
    assert item.id in q
    assert 999 not in q


def test_len():
    q = WorkQueue(clock=lambda: 0.0)
    q.enqueue("a")
    q.enqueue("b")
    assert len(q) == 2


def test_all():
    q = WorkQueue(clock=lambda: 0.0)
    q.enqueue("a")
    q.enqueue("b")
    assert len(q.all()) == 2


def test_by_status():
    q = WorkQueue(clock=lambda: 0.0)
    q.enqueue("a")
    q.enqueue("b")
    q.dequeue()
    assert len(q.pending()) == 1
    assert len(q.in_progress()) == 1


def test_done_and_failed():
    q = WorkQueue(clock=lambda: 0.0)
    q.enqueue("a")
    q.enqueue("b")
    i1 = q.dequeue()
    i2 = q.dequeue()
    q.complete(i1.id)
    q.fail(i2.id)
    assert len(q.done()) == 1
    assert len(q.failed()) == 1


def test_count_by_status():
    q = WorkQueue(clock=lambda: 0.0)
    q.enqueue("a")
    q.enqueue("b")
    assert q.count(ItemStatus.PENDING) == 2
    assert q.count(ItemStatus.DONE) == 0


# ---------------------------------------------------------------------------
# WorkQueue — clear and serialisation
# ---------------------------------------------------------------------------


def test_clear():
    q = WorkQueue(clock=lambda: 0.0)
    q.enqueue("t")
    q.clear()
    assert len(q) == 0
    next_item = q.enqueue("fresh")
    assert next_item.id == 1


def test_to_dict_round_trip():
    q = WorkQueue(clock=lambda: 0.0)
    q.enqueue("a", priority=5)
    q.enqueue("b", priority=1)
    item = q.dequeue()
    q.complete(item.id)

    restored = WorkQueue.from_dict(q.to_dict())
    assert len(restored) == 2
    assert restored.count(ItemStatus.DONE) == 1
    assert restored.count(ItemStatus.PENDING) == 1


def test_repr():
    q = WorkQueue(clock=lambda: 0.0)
    q.enqueue("t")
    assert "WorkQueue" in repr(q)
    assert "1" in repr(q)
