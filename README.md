# agent-work-queue

Priority work queue for agent tasks with lifecycle tracking.

Items move through a simple lifecycle: `PENDING → IN_PROGRESS → DONE` (or `FAILED`, or `DROPPED`). Dequeue order is priority-descending with FIFO tiebreak. Failed items can be retried.

## Install

```bash
pip install agent-work-queue
```

## Quick start

```python
from agent_work_queue import WorkQueue, ItemStatus

queue = WorkQueue()
queue.enqueue("fetch emails", priority=5)
queue.enqueue("send reply",   priority=10)

item = queue.dequeue()   # "send reply" (priority 10)
queue.complete(item.id)

item2 = queue.dequeue()  # "fetch emails"
queue.fail(item2.id)
queue.retry(item2.id)    # back to PENDING
```

## API

### `WorkQueue`

| Method | Description |
|---|---|
| `enqueue(task, *, payload, priority)` | Add a new item; returns `WorkItem` |
| `dequeue()` | Pop highest-priority PENDING item (or `None`) |
| `peek()` | Inspect next item without consuming it |
| `complete(item_id)` | Mark IN_PROGRESS item as DONE |
| `fail(item_id)` | Mark IN_PROGRESS item as FAILED |
| `drop(item_id)` | Drop PENDING or IN_PROGRESS item |
| `retry(item_id)` | Reset FAILED item to PENDING |
| `get(item_id)` | Return item by id (raises `ItemNotFoundError`) |
| `pending()` / `in_progress()` / `done()` / `failed()` | Filter by status |
| `count(status)` | Count items, optionally by status |
| `clear()` | Remove all items and reset ID counter |
| `to_dict()` / `from_dict(data)` | Serialise/restore |

### `WorkItem`

```python
@dataclass
class WorkItem:
    id: int
    task: str
    payload: dict
    priority: int        # higher = dequeued sooner
    status: ItemStatus
    attempts: int        # incremented on each dequeue
    created_at: float
    updated_at: float
```

### `ItemStatus`

`PENDING` · `IN_PROGRESS` · `DONE` · `FAILED` · `DROPPED`

### Errors

- `ItemNotFoundError` — item id not found
- `InvalidTransitionError` — illegal status transition (e.g. `complete` a PENDING item)

## License

MIT
