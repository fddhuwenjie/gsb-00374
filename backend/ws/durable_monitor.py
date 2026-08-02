"""WebSocket monitor for durable executions.

Protocol
--------
Client connects to ``/ws/durable/{executionId}`` and may send an initial JSON
message ``{"lastSeq": N}`` (0 or omitted for a fresh join). The server then:

  1. Subscribes to the live event stream *before* reading the log, so no event
     committed after the snapshot is lost.
  2. Sends a ``snapshot`` frame: the current folded state plus every event with
     ``seq > lastSeq`` (the *backfill* for a late or reconnecting client).
  3. Streams subsequent events as ``event`` frames.

The client tracks the highest ``seq`` it has applied and ignores any event whose
``seq`` is ``<=`` that high-water mark, so duplicate or out-of-order delivery can
never regress the UI. Because every frame carries the state machine's
``allowedCommands``, the client's buttons stay server-driven.
"""

import asyncio
import json
from typing import Any, Dict

from fastapi import WebSocket, WebSocketDisconnect

from engine.durable.service import get_engine
from engine.durable.state_machine import allowed_commands


async def durable_monitor_endpoint(websocket: WebSocket):
    await websocket.accept()
    execution_id = websocket.path_params.get("execution_id")
    engine = get_engine()

    # Subscribe first so events emitted during snapshot assembly are queued.
    queue = engine.subscribe(execution_id)

    last_seq = 0
    try:
        # Optional initial message carrying the client's resume point.
        try:
            first = await asyncio.wait_for(websocket.receive_text(), timeout=0.5)
            data = json.loads(first)
            last_seq = int(data.get("lastSeq", 0))
        except (asyncio.TimeoutError, json.JSONDecodeError, ValueError):
            last_seq = 0

        snap = engine.snapshot(execution_id, after_seq=last_seq)
        await websocket.send_json({"type": "snapshot", **snap})
        # Advance our high-water mark past the backfilled events.
        for ev in snap["events"]:
            last_seq = max(last_seq, ev["seq"])

        while True:
            try:
                ev = await asyncio.wait_for(queue.get(), timeout=30.0)
            except asyncio.TimeoutError:
                await websocket.send_json({"type": "ping"})
                continue

            # Drop duplicates / already-seen seqs so the UI never goes backwards.
            if ev["seq"] <= last_seq:
                continue
            last_seq = ev["seq"]

            frame: Dict[str, Any] = {"type": "event", "event": ev}
            if ev.get("kind") == "state":
                frame["allowedCommands"] = sorted(allowed_commands(ev.get("state")))
            await websocket.send_json(frame)

    except WebSocketDisconnect:
        pass
    finally:
        engine.unsubscribe(execution_id, queue)
