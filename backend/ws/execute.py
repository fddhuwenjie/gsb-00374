import asyncio
import json
import uuid
from typing import Any, Dict, Optional

from fastapi import WebSocket, WebSocketDisconnect

from engine import state_machine as sm
from engine.execution_manager import CommandRejected
from models.flow import ApprovalResponse, ControlCommand, FlowDefinition, StateEvent
from runtime import get_manager
from engine.ast_eval import evaluate_expression, ASTEvaluationError


class ExecutionConnection:
    """A single WebSocket connection.

    Protocol:
      Client -> server: execute | subscribe | pause | resume | cancel | step
      Server -> client: subscribed | snapshot | event | commandResult | error

    On subscribe the server always sends a fresh snapshot first, then all
    events with seq greater than snapshot.seq. Every event carries a
    monotonic seq; the client is expected to ignore duplicates/older
    events so late joiners and reconnects never regress the UI.
    """

    def __init__(self, websocket: WebSocket):
        self.ws = websocket
        self.manager = get_manager()
        self.execution_id: Optional[str] = None
        self.last_seq: int = 0
        self._queue: asyncio.Queue = asyncio.Queue()
        self._sender_task: Optional[asyncio.Task] = None
        self._unsubscribe = None
        self._breakpoints = set()

    # ----- loop -----

    async def run(self) -> None:
        self._sender_task = asyncio.create_task(self._send_loop())
        try:
            while True:
                raw = await self.ws.receive_text()
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    await self._send({'type': 'error', 'message': 'Invalid JSON'})
                    continue
                await self._dispatch(data)
        except WebSocketDisconnect:
            pass
        finally:
            if self._unsubscribe:
                self._unsubscribe()
            if self._sender_task:
                self._sender_task.cancel()

    async def _dispatch(self, data: Dict[str, Any]) -> None:
        msg_type = data.get('type')
        handlers = {
            'execute': self._handle_execute,
            'subscribe': self._handle_subscribe,
            'pause': self._handle_command,
            'resume': self._handle_command,
            'cancel': self._handle_command,
            'step': self._handle_command,
            'setBreakpoint': self._handle_set_breakpoint,
            'evaluate': self._handle_evaluate,
            'setVariable': self._handle_set_variable,
            'approve': self._handle_approval_response,
            'reject': self._handle_approval_response,
        }
        handler = handlers.get(msg_type)
        if handler is None:
            await self._send({'type': 'error', 'message': f'Unknown message type: {msg_type}'})
            return
        try:
            await handler(data)
        except CommandRejected as e:
            await self._send({
                'type': 'commandResult',
                'command': msg_type,
                'accepted': False,
                'reason': e.reason,
                'status': e.status,
                'requestId': data.get('requestId'),
            })
        except Exception as e:
            await self._send({'type': 'error', 'message': str(e)})

    # ----- sender / subscription -----

    def _on_event(self, event: StateEvent) -> None:
        if event.executionId != self.execution_id:
            return
        if event.seq <= self.last_seq:
            return
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            pass

    async def _send_loop(self) -> None:
        while True:
            try:
                event = await self._queue.get()
                if event is None:
                    return
                if event.executionId != self.execution_id or event.seq <= self.last_seq:
                    continue
                await self._send({'type': 'event', 'event': event.model_dump()})
                self.last_seq = event.seq
            except asyncio.CancelledError:
                return
            except Exception:
                continue

    async def _subscribe(self, execution_id: str, after_seq: int = 0) -> None:
        if self._unsubscribe:
            self._unsubscribe()
            self._unsubscribe = None

        self.execution_id = execution_id
        self.last_seq = after_seq
        # Drain stale events from a previous subscription.
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break

        self._unsubscribe = self.manager.subscribe(self._on_event)

        snapshot = self.manager.get_snapshot(execution_id)
        # Fresh snapshot is authoritative; never roll back.
        if snapshot.seq > self.last_seq:
            self.last_seq = snapshot.seq
        await self._send({
            'type': 'subscribed',
            'executionId': execution_id,
            'snapshot': snapshot.model_dump(),
        })

        # Replay any events that arrived after the snapshot.
        for event in self.manager.get_events(execution_id, after_seq=self.last_seq):
            if event.seq > self.last_seq:
                await self._send({'type': 'event', 'event': event.model_dump()})
                self.last_seq = event.seq

    async def _send(self, message: Dict[str, Any]) -> None:
        await self.ws.send_json(message)

    # ----- handlers -----

    async def _handle_execute(self, data: Dict[str, Any]) -> None:
        flow_data = data.get('flow')
        if not flow_data:
            await self._send({'type': 'error', 'message': 'No flow provided'})
            return
        flow = FlowDefinition(**flow_data)
        for node in flow.nodes:
            if node.id in self._breakpoints:
                node.data.breakpoint = True
        variables = data.get('variables') or {}
        request_id = data.get('requestId') or str(uuid.uuid4())
        execution_id = await self.manager.start_execution(
            flow, variables=variables, request_id=request_id,
        )
        await self._subscribe(execution_id, after_seq=0)
        await self._send({
            'type': 'commandResult',
            'command': 'execute',
            'accepted': True,
            'executionId': execution_id,
            'requestId': request_id,
        })

    async def _handle_subscribe(self, data: Dict[str, Any]) -> None:
        execution_id = data.get('executionId')
        if not execution_id:
            await self._send({'type': 'error', 'message': 'executionId required'})
            return
        after_seq = int(data.get('afterSeq') or 0)
        await self._subscribe(execution_id, after_seq=after_seq)

    async def _handle_command(self, data: Dict[str, Any]) -> None:
        command = data['type']
        execution_id = data.get('executionId') or self.execution_id
        if not execution_id:
            await self._send({'type': 'error', 'message': 'No active execution'})
            return
        request_id = data.get('requestId') or str(uuid.uuid4())
        cmd = ControlCommand(
            command=command, requestId=request_id,
            executionId=execution_id,
        )
        snap = await self.manager.send_command(cmd)
        await self._send({
            'type': 'commandResult',
            'command': command,
            'accepted': True,
            'executionId': execution_id,
            'requestId': request_id,
            'snapshot': snap.model_dump(),
        })

    async def _handle_set_breakpoint(self, data: Dict[str, Any]) -> None:
        node_id = data.get('nodeId')
        enabled = data.get('enabled', True)
        if not node_id:
            return
        if enabled:
            self._breakpoints.add(node_id)
        else:
            self._breakpoints.discard(node_id)
        await self._send({
            'type': 'breakpointUpdated',
            'nodeId': node_id,
            'enabled': enabled,
            'breakpoints': list(self._breakpoints),
        })

    async def _handle_evaluate(self, data: Dict[str, Any]) -> None:
        expression = data.get('expression')
        if not expression:
            return
        if not self.execution_id:
            await self._send({'type': 'evaluateResult', 'expression': expression,
                              'error': 'No active execution', 'success': False})
            return
        try:
            snap = self.manager.get_snapshot(self.execution_id)
            ctx = dict(snap.variables)
            result = evaluate_expression(expression, ctx)
            await self._send({'type': 'evaluateResult', 'expression': expression,
                              'result': result, 'success': True})
        except ASTEvaluationError as e:
            await self._send({'type': 'evaluateResult', 'expression': expression,
                              'error': str(e), 'success': False})

    async def _handle_set_variable(self, data: Dict[str, Any]) -> None:
        if not self.execution_id:
            return
        name = data.get('name')
        value = data.get('value')
        if not name:
            return
        record = self.manager.event_store.load_record(self.execution_id)
        if record is None:
            return
        if record.snapshot.status not in (sm.PAUSED, sm.QUEUED):
            await self._send({'type': 'error',
                              'message': 'Variables can only be edited while paused'})
            return
        record.snapshot.variables[name] = value
        record.snapshot.allowedActions = sm.allowed_actions(record.snapshot.status)
        self.manager.event_store.save_record(record)
        await self._send({'type': 'snapshot',
                          'snapshot': record.snapshot.model_dump()})

    async def _handle_approval_response(self, data: Dict[str, Any]) -> None:
        execution_id = data.get('executionId') or self.execution_id
        if not execution_id:
            await self._send({'type': 'error', 'message': 'No active execution'})
            return
        token = data.get('token')
        if not token:
            await self._send({'type': 'error', 'message': 'token required'})
            return
        decision = 'approved' if data['type'] == 'approve' else 'rejected'
        request_id = data.get('requestId') or str(uuid.uuid4())
        response = ApprovalResponse(
            token=token,
            executionId=execution_id,
            decision=decision,
            responder=data.get('responder'),
            comment=data.get('comment'),
            requestId=request_id,
        )
        try:
            snap = await self.manager.respond_to_approval(response)
        except CommandRejected as e:
            await self._send({
                'type': 'commandResult',
                'command': data['type'],
                'accepted': False,
                'reason': e.reason,
                'status': e.status,
                'requestId': request_id,
            })
            return
        await self._send({
            'type': 'commandResult',
            'command': data['type'],
            'accepted': True,
            'executionId': execution_id,
            'requestId': request_id,
            'snapshot': snap.model_dump(),
        })


async def websocket_endpoint(websocket: WebSocket) -> None:
    await websocket.accept()
    connection = ExecutionConnection(websocket)
    await connection.run()
