"""Priority work queue for agent tasks with lifecycle tracking.

:class:`WorkQueue` holds :class:`WorkItem` objects in an ordered queue.
Items are dequeued by priority (higher value = higher priority) with ties
broken by insertion order (FIFO).  Each item moves through a simple
lifecycle: PENDING → IN_PROGRESS → DONE (or FAILED, or DROPPED).

Items can be retried: :meth:`~WorkQueue.retry` moves a FAILED item back
to PENDING and increments its attempt counter.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable


class ItemStatus(str, Enum):
    """Lifecycle status of a work item."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    DONE = "done"
    FAILED = "failed"
    DROPPED = "dropped"


class ItemNotFoundError(KeyError):
    """Raised when a work-item ID is not found."""

    def __init__(self, item_id: int) -> None:
        self.item_id = item_id
        super().__init__(f"Work item {item_id!r} not found.")


class InvalidTransitionError(RuntimeError):
    """Raised when a status transition is not allowed."""

    def __init__(
        self, item_id: int, from_status: ItemStatus, to_status: ItemStatus
    ) -> None:
        self.item_id = item_id
        self.from_status = from_status
        self.to_status = to_status
        super().__init__(
            f"Cannot transition item {item_id!r}"
            f" from {from_status.value!r} to {to_status.value!r}."
        )


@dataclass
class WorkItem:
    """A single work item in the queue.

    Attributes:
        id: Auto-assigned integer ID.
        task: Short description of the work to do.
        payload: Arbitrary data the worker needs.
        priority: Higher values are dequeued first.
        status: Current lifecycle status.
        attempts: How many times this item has been attempted.
        created_at: Unix timestamp of enqueue.
        updated_at: Unix timestamp of last status change.
    """

    id: int
    task: str
    payload: dict[str, Any] = field(default_factory=dict)
    priority: int = 0
    status: ItemStatus = ItemStatus.PENDING
    attempts: int = 0
    created_at: float = 0.0
    updated_at: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serialisable dict."""
        return {
            "id": self.id,
            "task": self.task,
            "payload": dict(self.payload),
            "priority": self.priority,
            "status": self.status.value,
            "attempts": self.attempts,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WorkItem:
        """Reconstruct a :class:`WorkItem` from a plain dict."""
        return cls(
            id=int(data["id"]),
            task=data["task"],
            payload=dict(data.get("payload") or {}),
            priority=int(data.get("priority", 0)),
            status=ItemStatus(data.get("status", ItemStatus.PENDING.value)),
            attempts=int(data.get("attempts", 0)),
            created_at=float(data.get("created_at", 0.0)),
            updated_at=float(data.get("updated_at", 0.0)),
        )

    def __repr__(self) -> str:
        preview = self.task[:35] + "..." if len(self.task) > 35 else self.task
        return (
            f"WorkItem(id={self.id},"
            f" priority={self.priority},"
            f" status={self.status.value!r},"
            f" task={preview!r})"
        )


class WorkQueue:
    """An in-memory priority work queue.

    Items are dequeued in descending priority order; ties resolved FIFO.
    IDs are auto-assigned integers starting at 1.

    Args:
        clock: Callable returning current Unix time.

    Example::

        queue = WorkQueue()
        queue.enqueue("fetch emails", priority=5)
        queue.enqueue("send reply",   priority=10)

        item = queue.dequeue()   # "send reply" (priority 10)
        queue.complete(item.id)
    """

    def __init__(self, *, clock: Callable[[], float] | None = None) -> None:
        self._items: dict[int, WorkItem] = {}
        self._order: list[int] = []  # insertion order for FIFO tiebreak
        self._next_id: int = 1
        self._clock: Callable[[], float] = clock if clock is not None else time.time

    # ------------------------------------------------------------------
    # Enqueue
    # ------------------------------------------------------------------

    def enqueue(
        self,
        task: str,
        *,
        payload: dict[str, Any] | None = None,
        priority: int = 0,
    ) -> WorkItem:
        """Add a new item to the queue.

        Args:
            task: Short task description.
            payload: Data passed to the worker.
            priority: Higher = dequeued sooner.

        Returns:
            The new :class:`WorkItem`.
        """
        now = self._clock()
        item = WorkItem(
            id=self._next_id,
            task=task,
            payload=dict(payload or {}),
            priority=priority,
            status=ItemStatus.PENDING,
            attempts=0,
            created_at=now,
            updated_at=now,
        )
        self._items[item.id] = item
        self._order.append(item.id)
        self._next_id += 1
        return item

    # ------------------------------------------------------------------
    # Dequeue
    # ------------------------------------------------------------------

    def dequeue(self) -> WorkItem | None:
        """Remove and return the highest-priority PENDING item.

        Returns:
            The :class:`WorkItem` with the highest priority (FIFO on ties),
            or ``None`` if no PENDING items exist.
        """
        pending_ids = [
            i for i in self._order if self._items[i].status is ItemStatus.PENDING
        ]
        if not pending_ids:
            return None
        # Sort by (priority desc, insertion-order asc)
        best_id = max(
            pending_ids,
            key=lambda i: (self._items[i].priority, -self._order.index(i)),
        )
        item = self._items[best_id]
        item.status = ItemStatus.IN_PROGRESS
        item.attempts += 1
        item.updated_at = self._clock()
        return item

    def peek(self) -> WorkItem | None:
        """Return the next item that would be dequeued, without removing it."""
        pending_ids = [
            i for i in self._order if self._items[i].status is ItemStatus.PENDING
        ]
        if not pending_ids:
            return None
        return self._items[
            max(
                pending_ids,
                key=lambda i: (self._items[i].priority, -self._order.index(i)),
            )
        ]

    # ------------------------------------------------------------------
    # Status transitions
    # ------------------------------------------------------------------

    def _transition(
        self,
        item_id: int,
        to_status: ItemStatus,
        allowed_from: set[ItemStatus],
    ) -> None:
        item = self.get(item_id)
        if item.status not in allowed_from:
            raise InvalidTransitionError(item_id, item.status, to_status)
        item.status = to_status
        item.updated_at = self._clock()

    def complete(self, item_id: int) -> None:
        """Mark *item_id* as DONE.  Must be IN_PROGRESS."""
        self._transition(item_id, ItemStatus.DONE, {ItemStatus.IN_PROGRESS})

    def fail(self, item_id: int) -> None:
        """Mark *item_id* as FAILED.  Must be IN_PROGRESS."""
        self._transition(item_id, ItemStatus.FAILED, {ItemStatus.IN_PROGRESS})

    def drop(self, item_id: int) -> None:
        """Mark *item_id* as DROPPED.  Can be PENDING or IN_PROGRESS."""
        self._transition(
            item_id,
            ItemStatus.DROPPED,
            {ItemStatus.PENDING, ItemStatus.IN_PROGRESS},
        )

    def retry(self, item_id: int) -> None:
        """Reset a FAILED item back to PENDING."""
        self._transition(item_id, ItemStatus.PENDING, {ItemStatus.FAILED})

    # ------------------------------------------------------------------
    # Retrieval
    # ------------------------------------------------------------------

    def get(self, item_id: int) -> WorkItem:
        """Return the item with *item_id*.

        Raises:
            ItemNotFoundError: If not found.
        """
        if item_id not in self._items:
            raise ItemNotFoundError(item_id)
        return self._items[item_id]

    def by_status(self, status: ItemStatus) -> list[WorkItem]:
        """Return items with *status* in insertion order."""
        return [self._items[i] for i in self._order if self._items[i].status is status]

    def pending(self) -> list[WorkItem]:
        """All PENDING items in insertion order."""
        return self.by_status(ItemStatus.PENDING)

    def in_progress(self) -> list[WorkItem]:
        """All IN_PROGRESS items in insertion order."""
        return self.by_status(ItemStatus.IN_PROGRESS)

    def done(self) -> list[WorkItem]:
        """All DONE items in insertion order."""
        return self.by_status(ItemStatus.DONE)

    def failed(self) -> list[WorkItem]:
        """All FAILED items in insertion order."""
        return self.by_status(ItemStatus.FAILED)

    def all(self) -> list[WorkItem]:
        """All items in insertion order."""
        return [self._items[i] for i in self._order]

    def count(self, status: ItemStatus | None = None) -> int:
        """Total items, optionally filtered by *status*."""
        if status is None:
            return len(self._items)
        return sum(1 for item in self._items.values() if item.status is status)

    def __len__(self) -> int:
        return len(self._items)

    def __contains__(self, item_id: int) -> bool:
        return item_id in self._items

    # ------------------------------------------------------------------
    # Serialisation / reset
    # ------------------------------------------------------------------

    def clear(self) -> None:
        """Remove all items and reset the ID counter."""
        self._items.clear()
        self._order.clear()
        self._next_id = 1

    def to_dict(self) -> dict[str, Any]:
        """Serialise the queue to a plain dict."""
        return {
            "next_id": self._next_id,
            "order": list(self._order),
            "items": [item.to_dict() for item in self._items.values()],
        }

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any],
        *,
        clock: Callable[[], float] | None = None,
    ) -> WorkQueue:
        """Reconstruct a :class:`WorkQueue` from a plain dict."""
        queue = cls(clock=clock)
        for d in data.get("items", []):
            item = WorkItem.from_dict(d)
            queue._items[item.id] = item
        default_order = [item.id for item in queue._items.values()]
        queue._order = list(data.get("order", default_order))
        queue._next_id = int(data.get("next_id", len(queue._items) + 1))
        return queue

    def __repr__(self) -> str:
        return (
            f"WorkQueue(total={len(self._items)},"
            f" pending={self.count(ItemStatus.PENDING)})"
        )
