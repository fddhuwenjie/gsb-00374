"""Idempotent side-effect execution.

Side-effect nodes (HTTP requests, file writes, ...) must never run twice for the
same logical attempt across process restarts. We derive a stable idempotency key
from ``executionId + nodeId + attempt`` and consult a durable ledger (the event
log's ``effect`` events, folded into ``RecoveredState.effects``) before running.

Flow:
  1. Compute ``key = sha256(executionId | nodeId | attempt)``.
  2. If ``key`` already has a committed result in the ledger -> return it, do NOT
     re-run the effect. This is what makes recovery not repeat a succeeded HTTP
     call or file write.
  3. Otherwise run the effect, then append an ``effect`` event committing the
     result under ``key``.

Because the ledger entry is written *after* the effect succeeds, a crash between
running the effect and committing the ledger entry could in principle re-run it.
To make file writes robust to that window we write to a temp file named by the
key and atomically rename -- so a re-run is a harmless overwrite with identical
content and the observable side effect (the final file) is produced exactly
once. HTTP effects record a monotonic call counter so tests can assert the real
number of outbound calls.
"""

from __future__ import annotations

import hashlib
import os
import time
from typing import Any, Callable, Dict, Optional


def idempotency_key(execution_id: str, node_id: str, attempt: int) -> str:
    raw = f"{execution_id}|{node_id}|{attempt}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


class EffectRunner:
    """Runs side effects at-most-once per idempotency key.

    ``ledger`` is the folded ``effects`` dict from the recovered state; ``commit``
    is a callback that appends an ``effect`` event to the durable log.
    """

    def __init__(
        self,
        execution_id: str,
        ledger: Dict[str, Any],
        commit: Callable[[str, str, int, Any], None],
    ):
        self.execution_id = execution_id
        self.ledger = ledger
        self._commit = commit

    def already_committed(self, node_id: str, attempt: int) -> bool:
        return idempotency_key(self.execution_id, node_id, attempt) in self.ledger

    async def run(
        self,
        node_id: str,
        attempt: int,
        effect: Callable[[], Any],
        is_async: bool = False,
    ) -> Any:
        """Return the committed result, running ``effect`` only if not yet done.

        ``effect`` is a zero-arg callable (sync or async) producing a
        JSON-serialisable result that is durably recorded.
        """
        key = idempotency_key(self.execution_id, node_id, attempt)
        if key in self.ledger:
            # Already succeeded in a prior life -> replay result, no re-execution.
            return self.ledger[key]

        result = await effect() if is_async else effect()
        self.ledger[key] = result
        self._commit(key, node_id, attempt, result)
        return result


def atomic_file_write(path: str, content: str, key: str) -> Dict[str, Any]:
    """Write ``content`` to ``path`` atomically, keyed by idempotency ``key``.

    Uses a temp file + ``os.replace`` so the final file appears exactly once with
    the intended content even if a crashed attempt is retried.
    """
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    tmp = os.path.join(directory, f".{key}.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    return {"path": path, "bytes": len(content.encode("utf-8")), "ts": time.time()}
