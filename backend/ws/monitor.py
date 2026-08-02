import asyncio
from typing import Optional

from fastapi import WebSocket, WebSocketDisconnect, Query

from runtime import get_manager


async def monitor_websocket_endpoint(websocket: WebSocket, execution_id: str,
                                     since: Optional[int] = Query(default=None),
                                     manager=None):
    """Monitoring channel with sequence-based replay.

    A client (late joiner or reconnecting) passes the last seq it applied via
    the `since` query param. The server first sends a full snapshot, then
    every persisted event with seq > since in order, then streams live events.
    Every message carries the journal seq so the client can dedupe and must
    never apply an event older than what it has already seen.
    """
    await websocket.accept()
    if manager is None:
        manager = get_manager()

    try:
        snapshot = manager.snapshot(execution_id)
    except KeyError:
        await websocket.send_json({'type': 'error', 'message': 'Execution not found'})
        await websocket.close()
        return

    # Subscribe before reading the backlog so no event is lost in between.
    queue = manager.subscribe(execution_id)
    last_sent = since or 0
    try:
        await websocket.send_json({'type': 'snapshot', **snapshot})

        backlog = manager.events_since(execution_id, last_sent)
        for event in backlog:
            if event['seq'] > last_sent:
                await websocket.send_json({'type': 'event', 'event': event})
                last_sent = event['seq']

        # Drain events that arrived while we were replaying the backlog.
        while not queue.empty():
            event = queue.get_nowait()
            if event['seq'] > last_sent:
                await websocket.send_json({'type': 'event', 'event': event})
                last_sent = event['seq']

        while True:
            event = await queue.get()
            if event['seq'] > last_sent:
                await websocket.send_json({'type': 'event', 'event': event})
                last_sent = event['seq']
    except WebSocketDisconnect:
        pass
    except asyncio.CancelledError:
        pass
    finally:
        manager.unsubscribe(execution_id, queue)
