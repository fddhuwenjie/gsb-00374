import os
import time
import copy
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, HTTPException, Query, Body
from pydantic import BaseModel
from models.flow import Execution, ExecutionState, FlowDefinition, TraceLog
from storage.execution_store import ExecutionStore
from storage.flow_store import FlowStore
from storage.trace_store import TraceStore
from engine.executor import FlowExecutor, ExecutionError

router = APIRouter(prefix="/api/executions", tags=["executions"])

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
execution_store = ExecutionStore(os.path.join(BASE_DIR, "flows", "executions"))
flow_store = FlowStore(os.path.join(BASE_DIR, "flows"))
trace_store = TraceStore(os.path.join(BASE_DIR, "flows", "traces"))


class ReplayRequest(BaseModel):
    fromNodeId: Optional[str] = None
    overrides: Optional[Dict[str, Any]] = None


class StartExecutionRequest(BaseModel):
    flowId: str
    variables: Optional[Dict[str, Any]] = None


def save_execution_from_state(flow_id: str, state: ExecutionState) -> str:
    execution = Execution(
        id=execution_store.create_execution_id(),
        flowId=flow_id,
        status=state.status,
        startedAt=state.trace[0].timestamp if state.trace else time.time(),
        finishedAt=state.trace[-1].timestamp if state.trace else time.time(),
        variables=dict(state.variables),
        trace=[log.model_dump() for log in state.trace],
        snapshots=dict(state.snapshots)
    )
    return execution_store.save_execution(execution)


@router.get("", response_model=List[Execution])
async def list_executions(flow_id: Optional[str] = Query(None, alias="flowId")):
    return execution_store.list_executions(flow_id)


@router.get("/{execution_id}", response_model=Execution)
async def get_execution(execution_id: str):
    execution = execution_store.get_execution(execution_id)
    if not execution:
        raise HTTPException(status_code=404, detail="Execution not found")
    return execution


@router.get("/{execution_id}/trace")
async def get_execution_trace(execution_id: str):
    execution = execution_store.get_execution(execution_id)
    if not execution:
        raise HTTPException(status_code=404, detail="Execution not found")
    return {"trace": execution.trace}


@router.get("/{execution_id}/snapshots")
async def get_execution_snapshots(execution_id: str):
    execution = execution_store.get_execution(execution_id)
    if not execution:
        raise HTTPException(status_code=404, detail="Execution not found")
    return {"snapshots": execution.snapshots}


@router.get("/{execution_id}/snapshot/{node_id}")
async def get_execution_snapshot(execution_id: str, node_id: str):
    execution = execution_store.get_execution(execution_id)
    if not execution:
        raise HTTPException(status_code=404, detail="Execution not found")

    snapshot = execution.snapshots.get(node_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail=f"No snapshot found for node {node_id}")

    return {"nodeId": node_id, "snapshot": snapshot}


@router.post("/{execution_id}/replay")
async def replay_execution(execution_id: str, request: ReplayRequest):
    execution = execution_store.get_execution(execution_id)
    if not execution:
        raise HTTPException(status_code=404, detail="Execution not found")

    flow = flow_store.get_flow(execution.flowId)
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")

    trace_logs = [TraceLog(**log) for log in execution.trace]

    start_ctx = {}
    start_node_id = request.fromNodeId

    if start_node_id:
        snapshot = execution.snapshots.get(start_node_id)
        if snapshot is None:
            raise HTTPException(
                status_code=400,
                detail=f"No snapshot available for node {start_node_id}. "
                       f"Available nodes: {list(execution.snapshots.keys())}"
            )
        start_ctx = copy.deepcopy(snapshot)
    else:
        if trace_logs:
            first_enter = next((log for log in trace_logs if log.action == 'enter'), None)
            if first_enter:
                start_ctx = copy.deepcopy(first_enter.variables)

    if request.overrides:
        start_ctx.update(request.overrides)

    executor = FlowExecutor(flow, flow_store=flow_store)
    executor.state.variables = dict(start_ctx)

    async def save_on_completed(variables, trace):
        save_execution_from_state(execution.flowId, executor.state)

    executor.set_callbacks(on_completed=save_on_completed)

    try:
        if start_node_id:
            result_state = await _replay_from_node(executor, flow, start_node_id)
        else:
            result_state = await executor.execute()

        new_execution_id = save_execution_from_state(execution.flowId, result_state)

        return {
            "success": True,
            "originalExecutionId": execution_id,
            "newExecutionId": new_execution_id,
            "startedFrom": start_node_id or "start",
            "overrides": request.overrides,
            "status": result_state.status,
            "variables": dict(result_state.variables)
        }

    except ExecutionError as e:
        raise HTTPException(status_code=500, detail=str(e))


async def _replay_from_node(executor: FlowExecutor, flow: FlowDefinition, from_node_id: str) -> ExecutionState:
    start_node = None
    for node in flow.nodes:
        if node.id == from_node_id:
            start_node = node
            break

    if not start_node:
        raise ExecutionError(f"Node {from_node_id} not found in flow")

    try:
        executor.validate()
    except Exception as e:
        raise ExecutionError(f"Flow validation failed: {e}")

    await executor._set_status('running')
    current_node = start_node

    try:
        while current_node.type != 'end' and not executor._should_stop:
            await executor._check_pause()
            if executor._should_stop:
                break

            executor.state.currentNodeId = current_node.id

            await executor._add_trace(
                current_node.id,
                current_node.type,
                'enter',
                executor.state.variables
            )

            if executor._on_node_enter:
                await executor._on_node_enter(current_node.id, dict(executor.state.variables))

            await executor._check_pause()
            if executor._should_stop:
                break

            try:
                if current_node.type == 'start':
                    await executor._execute_start_node(current_node)
                elif current_node.type == 'task':
                    await executor._execute_with_retry(current_node, executor._execute_task_node)
                elif current_node.type == 'condition':
                    pass
                elif current_node.type == 'loop':
                    pass
                elif current_node.type == 'wait':
                    await executor._execute_wait_node(current_node)
                elif current_node.type == 'http':
                    await executor._execute_with_retry(current_node, executor._execute_http_node)
                elif current_node.type == 'sql':
                    await executor._execute_with_retry(current_node, executor._execute_sql_node)
                elif current_node.type == 'parallel':
                    await executor._execute_parallel_node(current_node)
                elif current_node.type == 'subflow':
                    await executor._execute_with_retry(current_node, executor._execute_subflow_node)
                elif current_node.type == 'trycatch':
                    await executor._execute_trycatch_node(current_node)

            except ExecutionError as e:
                await executor._add_trace(
                    current_node.id,
                    current_node.type,
                    'error',
                    executor.state.variables,
                    str(e)
                )
                if executor._on_node_error:
                    await executor._on_node_error(current_node.id, str(e), dict(executor.state.variables))
                await executor._set_status('error')
                raise

            await executor._add_trace(
                current_node.id,
                current_node.type,
                'exit',
                executor.state.variables
            )

            if executor._on_node_exit:
                await executor._on_node_exit(current_node.id, dict(executor.state.variables))

            await executor._check_pause()
            if executor._should_stop:
                break

            if current_node.type == 'condition':
                result = await executor._execute_condition_node(current_node)
                current_node = executor._get_next_node(current_node, result=result)
            elif current_node.type == 'loop':
                result = await executor._execute_loop_node(current_node)
                if result:
                    executor.state.loopCounts[current_node.id] = executor.state.loopCounts.get(current_node.id, 0) + 1
                    current_node = executor._get_next_node(current_node, handle='loop')
                else:
                    current_node = executor._get_next_node(current_node, handle='exit')
            elif current_node.type == 'start':
                current_node = executor._get_next_node(current_node)
            else:
                current_node = executor._get_next_node(current_node)

        if current_node.type == 'end' and not executor._should_stop:
            executor.state.currentNodeId = current_node.id
            await executor._add_trace(
                current_node.id,
                current_node.type,
                'enter',
                executor.state.variables
            )
            if executor._on_node_enter:
                await executor._on_node_enter(current_node.id, dict(executor.state.variables))
            await executor._execute_end_node(current_node)
            await executor._add_trace(
                current_node.id,
                current_node.type,
                'exit',
                executor.state.variables
            )
            if executor._on_node_exit:
                await executor._on_node_exit(current_node.id, dict(executor.state.variables))

        if executor._should_stop:
            await executor._set_status('stopped')
        else:
            await executor._set_status('completed')

        if executor._on_completed:
            await executor._on_completed(dict(executor.state.variables), list(executor.state.trace))

    except ExecutionError as e:
        await executor._set_status('error')
        raise

    return executor.state


@router.post("")
async def start_execution(request: StartExecutionRequest):
    flow = flow_store.get_flow(request.flowId)
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")

    executor = FlowExecutor(flow, flow_store=flow_store)

    if request.variables:
        for key, value in request.variables.items():
            executor.set_variable(key, value)

    async def save_on_completed(variables, trace):
        save_execution_from_state(request.flowId, executor.state)

    executor.set_callbacks(on_completed=save_on_completed)

    try:
        state = await executor.execute()
        execution_id = save_execution_from_state(request.flowId, state)

        return {
            "success": True,
            "executionId": execution_id,
            "status": state.status,
            "variables": dict(state.variables)
        }
    except ExecutionError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{execution_id}")
async def delete_execution(execution_id: str):
    success = execution_store.delete_execution(execution_id)
    if not success:
        raise HTTPException(status_code=404, detail="Execution not found")
    return {"success": True}
