import asyncio
import sqlite3
import time
from typing import Dict, Any, List, Optional, Callable, Coroutine
import httpx
from models.flow import (
    FlowDefinition, FlowNode, FlowEdge,
    TraceLog, ExecutionState, ExecutionStatus
)
from engine.ast_eval import evaluate_expression, ASTEvaluationError
from engine.sandbox import execute_python_sandbox, SandboxError
from engine.validator import FlowValidator, ValidationError
from storage.flow_store import FlowStore


MAX_LOOP_COUNT = 10000


class ExecutionError(Exception):
    pass


class InfiniteLoopError(ExecutionError):
    pass


class CircularSubflowError(ExecutionError):
    pass


class FlowExecutor:
    def __init__(self, flow: FlowDefinition, flow_store: Optional[FlowStore] = None, call_stack: Optional[List[str]] = None):
        self.flow = flow
        self.flow_store = flow_store
        self.nodes: Dict[str, FlowNode] = {n.id: n for n in flow.nodes}
        self.edges: List[FlowEdge] = flow.edges

        self.outgoing: Dict[str, List[FlowEdge]] = {}
        self.incoming: Dict[str, List[FlowEdge]] = {}

        for edge in self.edges:
            if edge.source not in self.outgoing:
                self.outgoing[edge.source] = []
            self.outgoing[edge.source].append(edge)

            if edge.target not in self.incoming:
                self.incoming[edge.target] = []
            self.incoming[edge.target].append(edge)

        self.state = ExecutionState(
            flowId=flow.id,
            status='idle'
        )

        self._should_stop = False
        self._should_pause = False
        self._step_mode = False
        self._step_event = asyncio.Event()
        self._pause_event = asyncio.Event()
        self._pause_event.set()

        self._call_stack: List[str] = call_stack.copy() if call_stack else []
        self._triggered_breakpoints: set = set()

        self._on_node_enter: Optional[Callable[[str, Dict[str, Any]], Coroutine]] = None
        self._on_node_exit: Optional[Callable[[str, Dict[str, Any]], Coroutine]] = None
        self._on_node_error: Optional[Callable[[str, str, Dict[str, Any]], Coroutine]] = None
        self._on_status_change: Optional[Callable[[ExecutionStatus, Dict[str, Any]], Coroutine]] = None
        self._on_trace: Optional[Callable[[TraceLog], Coroutine]] = None
        self._on_completed: Optional[Callable[[Dict[str, Any], List[TraceLog]], Coroutine]] = None

    def set_callbacks(self,
                     on_node_enter=None,
                     on_node_exit=None,
                     on_node_error=None,
                     on_status_change=None,
                     on_trace=None,
                     on_completed=None):
        self._on_node_enter = on_node_enter
        self._on_node_error = on_node_error
        self._on_status_change = on_status_change
        self._on_trace = on_trace
        self._on_completed = on_completed

    def validate(self) -> None:
        validator = FlowValidator(self.flow)
        validator.validate()

    async def pause(self) -> None:
        if self.state.status == 'running':
            self._should_pause = True
            self._pause_event.clear()
            await self._set_status('paused')

    async def resume(self) -> None:
        if self.state.status == 'paused':
            self._should_pause = False
            self._pause_event.set()
            await self._set_status('running')

    async def step(self) -> None:
        if self.state.status in ['paused', 'idle']:
            self._step_mode = True
            self._step_event.set()
            if self.state.status == 'paused':
                self._should_pause = False
                self._pause_event.set()
            await self._set_status('running')

    async def stop(self) -> None:
        self._should_stop = True
        self._should_pause = False
        self._step_mode = False
        self._step_event.set()
        self._pause_event.set()
        await self._set_status('stopped')

    def set_variable(self, name: str, value: Any) -> None:
        self.state.variables[name] = value

    async def _set_status(self, status: ExecutionStatus) -> None:
        self.state.status = status
        if self._on_status_change:
            await self._on_status_change(status, dict(self.state.variables))

    async def _add_trace(self, node_id: str, node_type: str, action: str, variables: Dict[str, Any], message: Optional[str] = None) -> None:
        log = TraceLog(
            timestamp=time.time(),
            nodeId=node_id,
            nodeType=node_type,
            action=action,
            variables=dict(variables),
            message=message
        )
        self.state.trace.append(log)
        if self._on_trace:
            await self._on_trace(log)

    def _get_start_node(self) -> FlowNode:
        for node in self.flow.nodes:
            if node.type == 'start':
                return node
        raise ExecutionError("No Start node found")

    def _get_next_node(self, current_node: FlowNode, result: Optional[bool] = None, handle: Optional[str] = None) -> Optional[FlowNode]:
        outgoing = self.outgoing.get(current_node.id, [])

        if current_node.type == 'condition':
            handle_str = 'true' if result else 'false'
            for edge in outgoing:
                if edge.sourceHandle == handle_str:
                    return self.nodes[edge.target]
            raise ExecutionError(f"No {handle_str} branch found for condition node {current_node.id}")

        elif current_node.type == 'loop':
            if handle == 'loop':
                for edge in outgoing:
                    if edge.sourceHandle == 'loop':
                        return self.nodes[edge.target]
            else:
                for edge in outgoing:
                    if edge.sourceHandle is None or edge.sourceHandle != 'loop':
                        return self.nodes[edge.target]
            raise ExecutionError(f"No appropriate edge found for loop node {current_node.id}")

        else:
            if len(outgoing) == 1:
                return self.nodes[outgoing[0].target]
            raise ExecutionError(f"Expected exactly 1 outgoing edge for {current_node.id}")

    async def _execute_with_retry(self, node: FlowNode, execute_func: Callable[[], Coroutine]) -> Any:
        retry_config = node.data.retry
        if not retry_config or retry_config.maxAttempts <= 1:
            return await execute_func()

        max_attempts = retry_config.maxAttempts
        delay = retry_config.delaySeconds
        backoff = retry_config.backoff
        max_delay = retry_config.maxDelaySeconds

        last_exception = None
        for attempt in range(max_attempts):
            try:
                return await execute_func()
            except Exception as e:
                last_exception = e
                if attempt < max_attempts - 1:
                    if backoff == 'exponential':
                        delay = min(delay * 2, max_delay)
                    await asyncio.sleep(delay)

        raise last_exception

    async def _execute_start_node(self, node: FlowNode) -> None:
        pass

    async def _execute_end_node(self, node: FlowNode) -> None:
        pass

    async def _execute_task_node(self, node: FlowNode) -> None:
        async def _do_execute():
            code = node.data.code
            if not code:
                return

            loop = asyncio.get_event_loop()
            try:
                await loop.run_in_executor(
                    None,
                    execute_python_sandbox,
                    code,
                    self.state.variables,
                    5
                )
            except SandboxError as e:
                raise ExecutionError(f"Task execution failed: {e}")

        await self._execute_with_retry(node, _do_execute)

    async def _execute_condition_node(self, node: FlowNode) -> bool:
        expression = node.data.expression
        if not expression:
            raise ExecutionError("Condition expression is empty")

        try:
            ctx = dict(self.state.variables)
            ctx['ctx'] = self.state.variables
            result = evaluate_expression(expression, ctx)
            return bool(result)
        except ASTEvaluationError as e:
            raise ExecutionError(f"Condition evaluation failed: {e}")

    async def _execute_loop_node(self, node: FlowNode) -> bool:
        loop_count = self.state.loopCounts.get(node.id, 0)

        if loop_count >= MAX_LOOP_COUNT:
            raise InfiniteLoopError(
                f"Infinite loop detected: Loop node '{node.id}' "
                f"exceeded maximum {MAX_LOOP_COUNT} iterations"
            )

        expression = node.data.expression
        if not expression:
            raise ExecutionError("Loop expression is empty")

        try:
            ctx = dict(self.state.variables)
            ctx['ctx'] = self.state.variables
            result = evaluate_expression(expression, ctx)
            return bool(result)
        except ASTEvaluationError as e:
            raise ExecutionError(f"Loop condition evaluation failed: {e}")

    async def _execute_wait_node(self, node: FlowNode) -> None:
        seconds = node.data.seconds or 0
        if seconds > 0:
            await asyncio.sleep(seconds)

    async def _execute_http_node(self, node: FlowNode) -> None:
        async def _do_execute():
            config = node.data.httpConfig
            if not config:
                raise ExecutionError("HTTP config is empty")

            method = config.method
            url = config.url
            headers = config.headers or {}
            body = config.body
            timeout = config.timeout or 30.0

            try:
                async with httpx.AsyncClient(timeout=timeout) as client:
                    response = await client.request(
                        method=method,
                        url=url,
                        headers=headers,
                        content=body
                    )
                    result = {
                        'status_code': response.status_code,
                        'headers': dict(response.headers),
                        'text': response.text,
                        'json': None
                    }
                    try:
                        result['json'] = response.json()
                    except Exception:
                        pass
                    self.state.variables[node.id + '_result'] = result
            except Exception as e:
                raise ExecutionError(f"HTTP request failed: {e}")

        await self._execute_with_retry(node, _do_execute)

    async def _execute_sql_node(self, node: FlowNode) -> None:
        async def _do_execute():
            config = node.data.sqlConfig
            if not config:
                raise ExecutionError("SQL config is empty")

            conn_str = config.connectionString
            query = config.query
            params = config.params or []

            loop = asyncio.get_event_loop()
            try:
                def _run_sql():
                    conn = sqlite3.connect(conn_str)
                    conn.row_factory = sqlite3.Row
                    cursor = conn.cursor()
                    cursor.execute(query, params)
                    if query.strip().upper().startswith('SELECT'):
                        rows = cursor.fetchall()
                        result = [dict(row) for row in rows]
                    else:
                        conn.commit()
                        result = {
                            'rowcount': cursor.rowcount,
                            'lastrowid': cursor.lastrowid
                        }
                    cursor.close()
                    conn.close()
                    return result

                result = await loop.run_in_executor(None, _run_sql)
                self.state.variables[node.id + '_result'] = result
            except Exception as e:
                raise ExecutionError(f"SQL execution failed: {e}")

        await self._execute_with_retry(node, _do_execute)

    async def _execute_branch_until_end(self, start_node_id: str, ctx_snapshot: Dict[str, Any]) -> Dict[str, Any]:
        current_node = self.nodes.get(start_node_id)
        if not current_node:
            raise ExecutionError(f"Branch start node {start_node_id} not found")

        while current_node and current_node.type != 'end' and not self._should_stop:
            await self._check_pause()
            if self._should_stop:
                break

            try:
                if current_node.type == 'start':
                    await self._execute_start_node(current_node)
                elif current_node.type == 'task':
                    await self._execute_task_node(current_node)
                elif current_node.type == 'http':
                    await self._execute_http_node(current_node)
                elif current_node.type == 'sql':
                    await self._execute_sql_node(current_node)
                elif current_node.type == 'wait':
                    await self._execute_wait_node(current_node)
            except ExecutionError:
                raise

            if current_node.data.anchorId:
                break

            if current_node.type == 'condition':
                result = await self._execute_condition_node(current_node)
                current_node = self._get_next_node(current_node, result=result)
            elif current_node.type == 'loop':
                result = await self._execute_loop_node(current_node)
                if result:
                    self.state.loopCounts[current_node.id] = self.state.loopCounts.get(current_node.id, 0) + 1
                    current_node = self._get_next_node(current_node, handle='loop')
                else:
                    current_node = self._get_next_node(current_node, handle='exit')
            else:
                current_node = self._get_next_node(current_node)

        return self.state.variables

    async def _execute_parallel_node(self, node: FlowNode) -> None:
        config = node.data.parallelConfig
        if not config:
            raise ExecutionError("Parallel config is empty")

        branch_ids = config.branchNodeIds
        if not branch_ids:
            return

        ctx_snapshot = dict(self.state.variables)

        async def _run_branch(branch_id: str) -> Dict[str, Any]:
            branch_ctx = dict(ctx_snapshot)
            temp_executor = FlowExecutor(self.flow, self.flow_store, self._call_stack)
            temp_executor.state.variables = branch_ctx
            temp_executor.nodes = self.nodes
            temp_executor.edges = self.edges
            temp_executor.outgoing = self.outgoing
            temp_executor.incoming = self.incoming
            try:
                await temp_executor._execute_branch_until_end(branch_id, branch_ctx)
                return temp_executor.state.variables
            except Exception as e:
                return {'error': str(e), **branch_ctx}

        tasks = [_run_branch(bid) for bid in branch_ids]
        results = await asyncio.gather(*tasks)

        merged = {}
        for i, branch_result in enumerate(results):
            merged[f'branch_{i}'] = branch_result
            for key, value in branch_result.items():
                if key.endswith('_result'):
                    merged[key] = value

        self.state.variables[node.id + '_result'] = merged
        self.state.variables.update(merged)

    async def _execute_subflow_node(self, node: FlowNode) -> None:
        async def _do_execute():
            config = node.data.subflowConfig
            if not config:
                raise ExecutionError("Subflow config is empty")

            if not self.flow_store:
                raise ExecutionError("Flow store is not available for subflow execution")

            subflow_id = config.subflowId
            current_call_chain = self._call_stack + [self.flow.id]
            if subflow_id in current_call_chain:
                raise CircularSubflowError(
                    f"Circular subflow reference detected: {' -> '.join(current_call_chain + [subflow_id])}"
                )

            subflow = self.flow_store.get_flow(subflow_id)
            if not subflow:
                raise ExecutionError(f"Subflow {subflow_id} not found")

            sub_executor = FlowExecutor(subflow, self.flow_store, current_call_chain)
            sub_executor.state.variables = dict(self.state.variables)
            sub_executor._should_stop = self._should_stop

            try:
                await sub_executor.execute()
                self.state.variables.update(sub_executor.state.variables)
                self.state.variables[node.id + '_result'] = {
                    'subflowId': subflow_id,
                    'status': sub_executor.state.status,
                    'variables': dict(sub_executor.state.variables)
                }
            except Exception as e:
                raise ExecutionError(f"Subflow execution failed: {e}")

        await self._execute_with_retry(node, _do_execute)

    async def _execute_trycatch_node(self, node: FlowNode) -> None:
        config = node.data.tryCatchConfig
        if not config:
            raise ExecutionError("TryCatch config is empty")

        try_node_ids = config.tryNodeIds or []
        catch_node_ids = config.catchNodeIds or []

        ctx_snapshot = dict(self.state.variables)
        caught_error = None

        try:
            for node_id in try_node_ids:
                if self._should_stop:
                    break
                await self._execute_branch_until_end(node_id, self.state.variables)
        except Exception as e:
            caught_error = str(e)
            self.state.variables.clear()
            self.state.variables.update(ctx_snapshot)
            self.state.variables[node.id + '_error'] = caught_error

            try:
                for node_id in catch_node_ids:
                    if self._should_stop:
                        break
                    await self._execute_branch_until_end(node_id, self.state.variables)
            except Exception as catch_e:
                caught_error = f"{caught_error}; Catch handler also failed: {catch_e}"

        self.state.variables[node.id + '_result'] = {
            'caught_error': caught_error,
            'success': caught_error is None
        }

    async def _check_pause(self, current_node: Optional[FlowNode] = None) -> None:
        if current_node and current_node.data.breakpoint and current_node.id not in self._triggered_breakpoints:
            self._triggered_breakpoints.add(current_node.id)
            self._should_pause = True
            self._pause_event.clear()

        if self._should_pause:
            await self._set_status('paused')
            await self._pause_event.wait()
            if not self._should_stop:
                await self._set_status('running')
            self._should_pause = False

        if self._step_mode:
            self._step_event.clear()
            await self._set_status('paused')
            await self._step_event.wait()
            if not self._should_stop and self._step_mode:
                await self._set_status('running')
                self._step_mode = False

    async def execute(self) -> ExecutionState:
        try:
            self.validate()
        except ValidationError as e:
            await self._set_status('error')
            raise ExecutionError(f"Flow validation failed: {e}")

        await self._set_status('running')
        current_node = self._get_start_node()

        try:
            while current_node.type != 'end' and not self._should_stop:

                self.state.snapshots[current_node.id] = dict(self.state.variables)

                await self._check_pause(current_node)
                if self._should_stop:
                    break

                self.state.currentNodeId = current_node.id

                await self._add_trace(
                    current_node.id,
                    current_node.type,
                    'enter',
                    self.state.variables
                )

                if self._on_node_enter:
                    await self._on_node_enter(current_node.id, dict(self.state.variables))

                await self._check_pause(current_node)
                if self._should_stop:
                    break

                try:
                    if current_node.type == 'start':
                        await self._execute_start_node(current_node)
                    elif current_node.type == 'task':
                        await self._execute_task_node(current_node)
                    elif current_node.type == 'http':
                        await self._execute_http_node(current_node)
                    elif current_node.type == 'sql':
                        await self._execute_sql_node(current_node)
                    elif current_node.type == 'parallel':
                        await self._execute_parallel_node(current_node)
                    elif current_node.type == 'subflow':
                        await self._execute_subflow_node(current_node)
                    elif current_node.type == 'trycatch':
                        await self._execute_trycatch_node(current_node)
                    elif current_node.type == 'condition':
                        pass
                    elif current_node.type == 'loop':
                        pass
                    elif current_node.type == 'wait':
                        await self._execute_wait_node(current_node)

                except ExecutionError as e:
                    await self._add_trace(
                        current_node.id,
                        current_node.type,
                        'error',
                        self.state.variables,
                        str(e)
                    )
                    if self._on_node_error:
                        await self._on_node_error(current_node.id, str(e), dict(self.state.variables))
                    await self._set_status('error')
                    raise

                await self._add_trace(
                    current_node.id,
                    current_node.type,
                    'exit',
                    self.state.variables
                )

                if self._on_node_exit:
                    await self._on_node_exit(current_node.id, dict(self.state.variables))

                await self._check_pause(current_node)
                if self._should_stop:
                    break

                if current_node.type == 'condition':
                    result = await self._execute_condition_node(current_node)
                    current_node = self._get_next_node(current_node, result=result)

                elif current_node.type == 'loop':
                    result = await self._execute_loop_node(current_node)
                    if result:
                        self.state.loopCounts[current_node.id] = self.state.loopCounts.get(current_node.id, 0) + 1
                        current_node = self._get_next_node(current_node, handle='loop')
                    else:
                        current_node = self._get_next_node(current_node, handle='exit')

                elif current_node.type == 'start':
                    current_node = self._get_next_node(current_node)

                else:
                    current_node = self._get_next_node(current_node)

            if current_node.type == 'end' and not self._should_stop:
                self.state.currentNodeId = current_node.id
                self.state.snapshots[current_node.id] = dict(self.state.variables)
                await self._add_trace(
                    current_node.id,
                    current_node.type,
                    'enter',
                    self.state.variables
                )
                if self._on_node_enter:
                    await self._on_node_enter(current_node.id, dict(self.state.variables))
                await self._execute_end_node(current_node)
                await self._add_trace(
                    current_node.id,
                    current_node.type,
                    'exit',
                    self.state.variables
                )
                if self._on_node_exit:
                    await self._on_node_exit(current_node.id, dict(self.state.variables))

            if self._should_stop:
                await self._set_status('stopped')
            else:
                await self._set_status('completed')

            if self._on_completed:
                await self._on_completed(dict(self.state.variables), list(self.state.trace))

        except ExecutionError as e:
            await self._set_status('error')
            raise

        return self.state

    def get_state(self) -> ExecutionState:
        return self.state
