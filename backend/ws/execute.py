import json
import asyncio
import os
import copy
from typing import Dict, Any, Optional, Set
from fastapi import WebSocket, WebSocketDisconnect
from models.flow import FlowDefinition
from engine.executor import FlowExecutor, ExecutionError, InfiniteLoopError
from engine.ast_eval import evaluate_expression, ASTEvaluationError
from storage.trace_store import TraceStore
from storage.flow_store import FlowStore


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
trace_store = TraceStore(os.path.join(BASE_DIR, "flows", "traces"))
flow_store = FlowStore(os.path.join(BASE_DIR, "flows"))


class ExecutionConnection:
    def __init__(self, websocket: WebSocket):
        self.websocket = websocket
        self.executor: Optional[FlowExecutor] = None
        self.execution_task: Optional[asyncio.Task] = None
        self._breakpoints: Set[str] = set()
        self._call_depth: int = 0
        self._step_target_depth: Optional[int] = None

    async def send(self, message: Dict[str, Any]) -> None:
        await self.websocket.send_json(message)

    async def handle_message(self, data: Dict[str, Any]) -> None:
        msg_type = data.get('type')

        handlers = {
            'execute': self.handle_execute,
            'pause': self.handle_pause,
            'resume': self.handle_resume,
            'step': self.handle_step,
            'stop': self.handle_stop,
            'setVariable': self.handle_set_variable,
            'setBreakpoint': self.handle_set_breakpoint,
            'stepInto': self.handle_step_into,
            'stepOut': self.handle_step_out,
            'evaluate': self.handle_evaluate,
        }

        handler = handlers.get(msg_type)
        if handler:
            await handler(data)
        else:
            await self.send({
                'type': 'error',
                'message': f"Unknown message type: {msg_type}"
            })

    async def handle_set_breakpoint(self, data: Dict[str, Any]) -> None:
        node_id = data.get('nodeId')
        enabled = data.get('enabled', True)

        if not node_id:
            await self.send({
                'type': 'error',
                'message': 'nodeId is required for setBreakpoint'
            })
            return

        if enabled:
            self._breakpoints.add(node_id)
        else:
            self._breakpoints.discard(node_id)

        if self.executor:
            for node in self.executor.flow.nodes:
                if node.id == node_id:
                    node.data.breakpoint = enabled
                    break

        await self.send({
            'type': 'breakpointUpdated',
            'nodeId': node_id,
            'enabled': enabled,
            'breakpoints': list(self._breakpoints)
        })

    async def handle_step_into(self, data: Dict[str, Any]) -> None:
        if not self.executor or self.executor.state.status not in ['paused', 'running']:
            await self.send({
                'type': 'error',
                'message': 'No paused execution to step into'
            })
            return

        self._step_target_depth = None
        await self.executor.step()

    async def handle_step_out(self, data: Dict[str, Any]) -> None:
        if not self.executor or self.executor.state.status not in ['paused', 'running']:
            await self.send({
                'type': 'error',
                'message': 'No paused execution to step out'
            })
            return

        self._step_target_depth = max(0, self._call_depth - 1)
        await self.executor.resume()

    async def handle_evaluate(self, data: Dict[str, Any]) -> None:
        expression = data.get('expression')
        if not expression:
            await self.send({
                'type': 'error',
                'message': 'expression is required for evaluate'
            })
            return

        if not self.executor:
            await self.send({
                'type': 'evaluateResult',
                'expression': expression,
                'error': 'No active execution'
            })
            return

        try:
            ctx = dict(self.executor.state.variables)
            ctx['ctx'] = self.executor.state.variables
            result = evaluate_expression(expression, ctx)
            await self.send({
                'type': 'evaluateResult',
                'expression': expression,
                'result': result,
                'success': True
            })
        except ASTEvaluationError as e:
            await self.send({
                'type': 'evaluateResult',
                'expression': expression,
                'error': str(e),
                'success': False
            })
        except Exception as e:
            await self.send({
                'type': 'evaluateResult',
                'expression': expression,
                'error': f"Unexpected error: {str(e)}",
                'success': False
            })

    async def handle_execute(self, data: Dict[str, Any]) -> None:
        if self.execution_task and not self.execution_task.done():
            await self.send({
                'type': 'error',
                'message': "An execution is already in progress. Stop it first."
            })
            return

        try:
            flow_data = data.get('flow')
            if not flow_data:
                await self.send({
                    'type': 'error',
                    'message': "No flow definition provided"
                })
                return

            flow = FlowDefinition(**flow_data)

            for node in flow.nodes:
                if node.id in self._breakpoints:
                    node.data.breakpoint = True

            self.executor = FlowExecutor(flow, flow_store=flow_store)
            self._call_depth = 0
            self._step_target_depth = None

            original_on_enter = self._on_node_enter
            original_on_exit = self._on_node_exit

            async def wrapped_on_enter(node_id: str, variables: Dict[str, Any]) -> None:
                await original_on_enter(node_id, variables)
                if self.executor and self.executor.state.currentNodeId:
                    current_node = self.executor.nodes.get(self.executor.state.currentNodeId)
                    if current_node and current_node.type == 'subflow':
                        self._call_depth += 1
                    await self._check_debug_pause()

            async def wrapped_on_exit(node_id: str, variables: Dict[str, Any]) -> None:
                if self.executor and self.executor.state.currentNodeId:
                    current_node = self.executor.nodes.get(self.executor.state.currentNodeId)
                    if current_node and current_node.type == 'subflow':
                        self._call_depth -= 1
                await original_on_exit(node_id, variables)

            self.executor.set_callbacks(
                on_node_enter=wrapped_on_enter,
                on_node_exit=wrapped_on_exit,
                on_node_error=self._on_node_error,
                on_status_change=self._on_status_change,
                on_trace=self._on_trace,
                on_completed=self._on_completed
            )

            self.execution_task = asyncio.create_task(self._run_execution())

        except Exception as e:
            await self.send({
                'type': 'error',
                'message': f"Failed to start execution: {str(e)}"
            })

    async def _check_debug_pause(self) -> None:
        if not self.executor:
            return

        current_node_id = self.executor.state.currentNodeId
        if not current_node_id:
            return

        if self._step_target_depth is not None and self._call_depth <= self._step_target_depth:
            self._step_target_depth = None
            await self.executor.pause()
            await self.send({
                'type': 'debugPaused',
                'reason': 'stepOut',
                'nodeId': current_node_id,
                'callDepth': self._call_depth,
                'variables': dict(self.executor.state.variables)
            })

    async def _run_execution(self) -> None:
        try:
            await self.executor.execute()
        except InfiniteLoopError as e:
            await self.send({
                'type': 'error',
                'message': f"Infinite loop detected: {str(e)}"
            })
            await self.send({
                'type': 'status',
                'status': 'error',
                'variables': dict(self.executor.state.variables)
            })
        except ExecutionError as e:
            await self.send({
                'type': 'error',
                'message': f"Execution error: {str(e)}"
            })
            await self.send({
                'type': 'status',
                'status': 'error',
                'variables': dict(self.executor.state.variables)
            })
        except Exception as e:
            await self.send({
                'type': 'error',
                'message': f"Unexpected error: {str(e)}"
            })
            await self.send({
                'type': 'status',
                'status': 'error',
                'variables': dict(self.executor.state.variables) if self.executor else {}
            })

    async def handle_pause(self) -> None:
        if not self.executor:
            await self.send({
                'type': 'error',
                'message': "No execution in progress"
            })
            return
        await self.executor.pause()

    async def handle_resume(self) -> None:
        if not self.executor:
            await self.send({
                'type': 'error',
                'message': "No execution in progress"
            })
            return
        self._step_target_depth = None
        await self.executor.resume()

    async def handle_step(self) -> None:
        if not self.executor:
            await self.send({
                'type': 'error',
                'message': "No execution in progress"
            })
            return
        self._step_target_depth = None
        await self.executor.step()

    async def handle_stop(self) -> None:
        if not self.executor:
            return
        await self.executor.stop()
        if self.execution_task and not self.execution_task.done():
            try:
                await asyncio.wait_for(self.execution_task, timeout=2.0)
            except asyncio.TimeoutError:
                self.execution_task.cancel()

    async def handle_set_variable(self, data: Dict[str, Any]) -> None:
        if not self.executor:
            await self.send({
                'type': 'error',
                'message': "No execution in progress"
            })
            return
        name = data.get('name')
        value = data.get('value')
        if not name:
            await self.send({
                'type': 'error',
                'message': "Variable name is required"
            })
            return
        self.executor.set_variable(name, value)
        await self.send({
            'type': 'status',
            'status': self.executor.state.status,
            'variables': dict(self.executor.state.variables)
        })

    async def _on_node_enter(self, node_id: str, variables: Dict[str, Any]) -> None:
        await self.send({
            'type': 'nodeEnter',
            'nodeId': node_id,
            'variables': dict(variables),
            'callDepth': self._call_depth
        })

        if self.executor and self.executor.state.status == 'paused':
            current_node = self.executor.nodes.get(node_id)
            if current_node and current_node.data.breakpoint:
                await self.send({
                    'type': 'breakpointHit',
                    'nodeId': node_id,
                    'variables': dict(variables),
                    'callDepth': self._call_depth
                })

    async def _on_node_exit(self, node_id: str, variables: Dict[str, Any]) -> None:
        await self.send({
            'type': 'nodeExit',
            'nodeId': node_id,
            'variables': dict(variables),
            'callDepth': self._call_depth
        })

    async def _on_node_error(self, node_id: str, error: str, variables: Dict[str, Any]) -> None:
        await self.send({
            'type': 'nodeError',
            'nodeId': node_id,
            'error': error,
            'variables': dict(variables),
            'callDepth': self._call_depth
        })

    async def _on_status_change(self, status: str, variables: Dict[str, Any]) -> None:
        await self.send({
            'type': 'status',
            'status': status,
            'variables': dict(variables)
        })

    async def _on_trace(self, log: Any) -> None:
        await self.send({
            'type': 'trace',
            'log': log.model_dump()
        })

    async def _on_completed(self, variables: Dict[str, Any], trace: list) -> None:
        if self.executor and self.executor.flow:
            try:
                trace_store.save_trace(self.executor.flow.id, trace)
            except Exception:
                pass

            try:
                from routers.executions import save_execution_from_state
                save_execution_from_state(self.executor.flow.id, self.executor.state)
            except Exception:
                pass

        await self.send({
            'type': 'completed',
            'variables': dict(variables),
            'trace': [log.model_dump() for log in trace]
        })

        try:
            from main import scheduler
            if scheduler:
                flow_id = self.executor.flow.id if self.executor and self.executor.flow else ''
                await scheduler.trigger_flow_completed(flow_id, {
                    'status': 'completed',
                    'variables': dict(variables)
                })
        except Exception:
            pass


async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connection = ExecutionConnection(websocket)

    try:
        while True:
            try:
                data = await websocket.receive_json()
                await connection.handle_message(data)
            except json.JSONDecodeError:
                await connection.send({
                    'type': 'error',
                    'message': "Invalid JSON message"
                })
            except Exception as e:
                await connection.send({
                    'type': 'error',
                    'message': f"Message handling error: {str(e)}"
                })
    except WebSocketDisconnect:
        if connection.executor:
            await connection.executor.stop()
        if connection.execution_task and not connection.execution_task.done():
            connection.execution_task.cancel()
