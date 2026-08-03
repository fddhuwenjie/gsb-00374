import asyncio
import json
import os
from typing import Any, Dict, Optional

from fastapi import WebSocket, WebSocketDisconnect

from engine.runtime_manager import RuntimeManager
from models.flow import FlowDefinition


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def get_runtime_manager(websocket: WebSocket) -> RuntimeManager:
    return websocket.app.state.runtime_manager


class ExecutionConnection:
    def __init__(self, websocket: WebSocket):
        self.websocket = websocket
        self.execution_id: Optional[str] = None
        self.last_seq: int = 0
        self._bus_callback = None
        self._running_task: Optional[asyncio.Task] = None

    async def send(self, message: Dict[str, Any]) -> None:
        await self.websocket.send_json(message)

    async def handle_message(self, data: Dict[str, Any]) -> None:
        msg_type = data.get("type")

        if msg_type == "subscribe":
            await self.handle_subscribe(data)
        elif msg_type == "execute":
            await self.handle_execute(data)
        elif msg_type == "command":
            await self.handle_command(data)
        elif msg_type == "ping":
            await self.send({"type": "pong"})
        else:
            await self.send({"type": "error", "message": f"Unknown message type: {msg_type}"})

    async def handle_subscribe(self, data: Dict[str, Any]) -> None:
        execution_id = data.get("executionId")
        since_seq = int(data.get("sinceSeq", 0))
        if not execution_id:
            await self.send({"type": "error", "message": "executionId required"})
            return

        rm = get_runtime_manager(self.websocket)

        if self.execution_id and self._bus_callback:
            rm.bus.unsubscribe(self.execution_id, self._bus_callback)

        self.execution_id = execution_id
        self.last_seq = since_seq

        try:
            snapshot = rm.store.snapshot(execution_id)
        except KeyError:
            self._bus_callback = None
            self.execution_id = None
            await self.send({"type": "error", "message": "Execution not found"})
            return

        self.last_seq = max(snapshot["latestSeq"], since_seq)

        def on_event(event: Dict[str, Any]):
            seq = event.get("seq", 0)
            if seq <= self.last_seq:
                return
            self.last_seq = max(self.last_seq, seq)
            asyncio.ensure_future(self._safe_send(event))

        rm.bus.subscribe(execution_id, on_event)
        self._bus_callback = on_event

        await self.send({"type": "snapshot", "snapshot": snapshot})

        missed = rm.store.get_events_since(execution_id, snapshot["latestSeq"])
        for event in missed:
            self.last_seq = max(self.last_seq, event["seq"])
            await self.send({"type": "event", "event": event})

    async def _safe_send(self, message: Dict[str, Any]) -> None:
        try:
            await self.send(message)
        except Exception:
            pass

    async def handle_execute(self, data: Dict[str, Any]) -> None:
        rm = get_runtime_manager(self.websocket)
        flow_data = data.get("flow")
        variables = data.get("variables") or {}
        command_id = data.get("commandId")
        if not flow_data:
            await self.send({"type": "error", "message": "No flow definition provided"})
            return

        flow = FlowDefinition(**flow_data)
        flow_store = rm.flow_store
        try:
            existing = flow_store.get_flow(flow.id)
            if not existing:
                flow_store.create_flow(flow)
        except Exception:
            pass

        if command_id:
            claim = rm.store.claim_command("execute", "execute", command_id)
            if claim != "new":
                await self.send({
                    "type": "commandResult",
                    "command": "execute",
                    "commandId": command_id,
                    "accepted": claim == "accepted",
                    "reason": "duplicate",
                })
                return

        execution_id = await rm.start_execution(
            flow.id, variables=variables, flow=flow
        )
        if command_id:
            rm.store.complete_command(command_id, True)

        await self.handle_subscribe({"executionId": execution_id})

    async def handle_command(self, data: Dict[str, Any]) -> None:
        rm = get_runtime_manager(self.websocket)
        execution_id = data.get("executionId") or self.execution_id
        command = data.get("command")
        command_id = data.get("commandId")

        if not execution_id or not command:
            await self.send({"type": "error", "message": "executionId and command required"})
            return

        if command_id:
            claim = rm.store.claim_command(execution_id, command, command_id)
            if claim != "new":
                await self.send({
                    "type": "commandResult",
                    "command": command,
                    "commandId": command_id,
                    "accepted": claim == "accepted",
                    "reason": "duplicate",
                })
                return

        try:
            extra = {}
            if command in ("approve", "reject"):
                extra["token"] = data.get("token")
                extra["comment"] = data.get("comment")
                extra["approver"] = data.get("approver")
            accepted = await rm.send_command(execution_id, command, command_id, **extra)
        except KeyError:
            await self.send({"type": "error", "message": "Execution not found"})
            return
        except Exception as e:
            await self.send({"type": "error", "message": str(e)})
            return

        await self.send({
            "type": "commandResult",
            "command": command,
            "commandId": command_id,
            "accepted": accepted,
        })

    def cleanup(self) -> None:
        if self.execution_id and self._bus_callback:
            rm = None
            try:
                rm = get_runtime_manager(self.websocket)
                rm.bus.unsubscribe(self.execution_id, self._bus_callback)
            except Exception:
                pass
        if self._running_task and not self._running_task.done():
            self._running_task.cancel()


async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connection = ExecutionConnection(websocket)

    try:
        while True:
            try:
                data = await websocket.receive_json()
                await connection.handle_message(data)
            except json.JSONDecodeError:
                await connection.send({"type": "error", "message": "Invalid JSON"})
            except Exception as e:
                await connection.send({"type": "error", "message": str(e)})
    except WebSocketDisconnect:
        connection.cleanup()
