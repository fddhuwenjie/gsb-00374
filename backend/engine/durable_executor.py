import asyncio
import hashlib
import json
import os
import sqlite3
import time
import uuid
from typing import Any, Callable, Coroutine, Dict, List, Optional, Set

import httpx

from engine import state_machine as sm
from engine.ast_eval import evaluate_expression
from engine.event_bus import EventBus
from engine.idempotency import SideEffectRegistry
from engine.sandbox import execute_python_sandbox, SandboxError
from engine.validator import FlowValidator
from models.flow import FlowDefinition, FlowNode
from storage.event_store import EventStore
from storage.flow_store import FlowStore


MAX_LOOP_COUNT = 10000
SIDE_EFFECT_NODE_TYPES = {"http", "sql", "file_write"}

APPROVAL_WAIT_INTERVAL = 0.2


class DurableExecutionError(Exception):
    pass


class DurableFlowExecutor:
    def __init__(
        self,
        execution_id: str,
        flow: FlowDefinition,
        event_store: EventStore,
        flow_store: Optional[FlowStore] = None,
        event_bus: Optional[EventBus] = None,
        side_effects: Optional[SideEffectRegistry] = None,
    ):
        self.execution_id = execution_id
        self.flow = flow
        self.store = event_store
        self.flow_store = flow_store
        self.bus = event_bus or EventBus()
        self.side_effects = side_effects or SideEffectRegistry(event_store)

        self.nodes: Dict[str, FlowNode] = {n.id: n for n in flow.nodes}
        self.edges: List = flow.edges
        self.outgoing: Dict[str, List] = {}
        self.incoming: Dict[str, List] = {}
        for edge in self.edges:
            self.outgoing.setdefault(edge.source, []).append(edge)
            self.incoming.setdefault(edge.target, []).append(edge)

        self._cancel_requested = False
        self._pause_requested = False
        self._wake_event = asyncio.Event()
        self._wake_event.set()
        self._completed = asyncio.Event()
        self._completed_flag = False
        self._executor_generation = 0
        self._current_branch_id: Optional[str] = None
        self._current_parallel_node: Optional[str] = None

    def _publish(self, event: Dict[str, Any]) -> None:
        self.bus.publish(self.execution_id, event)

    def _load_state(self) -> Dict[str, Any]:
        row = self.store.get_execution_row(self.execution_id)
        if not row:
            raise DurableExecutionError(f"Execution {self.execution_id} not found")
        return row

    def _is_current(self, row: Optional[Dict[str, Any]] = None) -> bool:
        row = row or self._load_state()
        return int(row["executor_generation"]) == self._executor_generation

    def _guard_current(self) -> None:
        row = self._load_state()
        if sm.is_terminal(row["status"]) or not self._is_current(row):
            raise asyncio.CancelledError()
        if row["status"] == sm.CANCELLED:
            raise asyncio.CancelledError()

    def _publish_snapshot(self, seq: int, snapshot: Dict[str, Any]) -> None:
        self._publish({"type": "snapshot", "seq": seq, "snapshot": snapshot})

    def _publish_event(self, seq: int, event: Dict[str, Any]) -> None:
        self._publish({"type": "event", "seq": seq, "event": event})

    def _transition(self, to_state: str, *, expected_states: Optional[List[str]] = None, **kwargs) -> int:
        _, seq, snapshot = self.store.transition(
            self.execution_id,
            to_state,
            expected_states=expected_states,
            **kwargs,
        )
        self._publish_snapshot(seq, snapshot)
        event = self.store.get_events_since(self.execution_id, seq - 1)[0]
        self._publish_event(seq, event)
        return seq

    def _runtime_event(self, seq: int, snapshot: Dict[str, Any], event_type: str,
                       node_id: str, attempt: int, generation: int,
                       variables: Dict[str, Any]) -> None:
        self._publish_snapshot(seq, snapshot)
        self._publish_event(seq, {
            "seq": seq,
            "type": event_type,
            "nodeId": node_id,
            "attempt": attempt,
            "payload": {"generation": generation, "variables": variables},
            "timestamp": time.time(),
        })

    def _checkpoint(self, node_id: str, variables: Dict[str, Any],
                    loop_counts: Dict[str, int], attempt: int,
                    generation: int) -> int:
        seq, snapshot = self.store.checkpoint_node(
            self.execution_id, node_id, variables, loop_counts, attempt, generation
        )
        self._runtime_event(seq, snapshot, "nodeCheckpoint", node_id, attempt, generation, variables)
        return seq

    async def request_pause(self) -> None:
        row = self._load_state()
        if not sm.command_allowed("pause", row["status"]):
            return
        if row["status"] == sm.RUNNING:
            self._pause_requested = True
            self._wake_event.clear()
            self._transition(sm.PAUSING, expected_states=[sm.RUNNING])
        elif row["status"] == sm.PAUSING:
            self._pause_requested = True

    async def request_resume(self) -> None:
        row = self._load_state()
        if not sm.command_allowed("resume", row["status"]):
            return
        if row["status"] in (sm.PAUSED, sm.PAUSING, sm.RETRY_WAIT):
            self._pause_requested = False
            self._wake_event.set()
            if row["status"] != sm.RETRY_WAIT:
                self._transition(
                    sm.RUNNING,
                    expected_states=[sm.PAUSED, sm.PAUSING],
                )

    async def request_cancel(self) -> None:
        self._cancel_requested = True
        self._pause_requested = False
        self._wake_event.set()
        self._completed.set()

    async def request_retry(self) -> None:
        self._cancel_requested = False
        self._pause_requested = False
        self._wake_event.set()
        self._completed.set()

    async def _wait_if_paused(self) -> bool:
        row = self._load_state()
        if not self._is_current(row) or sm.is_terminal(row["status"]):
            return False
        if row["status"] == sm.PAUSING:
            self._transition(sm.PAUSED, expected_states=[sm.PAUSING])
            row = self._load_state()
        if row["status"] == sm.PAUSED:
            self._publish({"type": "paused"})
            while True:
                row = self._load_state()
                if not self._is_current(row) or sm.is_terminal(row["status"]):
                    return False
                if row["status"] != sm.PAUSED:
                    break
                try:
                    await asyncio.wait_for(self._wake_event.wait(), timeout=0.1)
                    self._wake_event.clear()
                except asyncio.TimeoutError:
                    pass
            row = self._load_state()
            if row["status"] == sm.PAUSED:
                self._transition(sm.RUNNING, expected_states=[sm.PAUSED])
            return self._is_current(row) and not sm.is_terminal(row["status"])
        return not self._cancel_requested and self._is_current(row)

    def _get_start_node(self) -> FlowNode:
        for node in self.flow.nodes:
            if node.type == "start":
                return node
        raise DurableExecutionError("No start node found")

    def _get_next_node(self, current: FlowNode, result: Optional[bool] = None,
                       handle: Optional[str] = None) -> Optional[FlowNode]:
        outgoing = self.outgoing.get(current.id, [])
        if current.type == "condition":
            handle_str = "true" if result else "false"
            for edge in outgoing:
                if edge.sourceHandle == handle_str:
                    return self.nodes[edge.target]
            raise DurableExecutionError(f"No {handle_str} branch for condition {current.id}")
        if current.type == "loop":
            if handle == "loop":
                for edge in outgoing:
                    if edge.sourceHandle == "loop":
                        return self.nodes[edge.target]
            else:
                for edge in outgoing:
                    if edge.sourceHandle is None or edge.sourceHandle != "loop":
                        return self.nodes[edge.target]
            raise DurableExecutionError(f"No edge for loop {current.id}")
        if len(outgoing) == 1:
            return self.nodes[outgoing[0].target]
        raise DurableExecutionError(f"Expected 1 outgoing edge for {current.id}")

    async def _sleep_with_cancel(self, seconds: float) -> bool:
        waited = 0.0
        while waited < seconds:
            self._guard_current()
            step = min(0.05, seconds - waited)
            try:
                await asyncio.wait_for(self._completed.wait(), timeout=step)
                return False
            except asyncio.TimeoutError:
                waited += step
        self._guard_current()
        return True

    def _node_succeeded(self, node_id: str, generation: int) -> Optional[Dict[str, Any]]:
        row = self.store.get_last_node_attempt(self.execution_id, node_id)
        if row and int(row.get("generation", -1)) == generation and row["status"] == "succeeded":
            result = row.get("result")
            if isinstance(result, str):
                try:
                    result = json.loads(result)
                except Exception:
                    result = None
            return result or {}
        return None

    def _recover_branch_position(self, parallel_node_id: str, branch_start_id: str,
                                 generation: int) -> tuple[Optional[FlowNode], Dict[str, Any]]:
        existing = self.store.get_branch_state(
            self.execution_id, parallel_node_id, branch_start_id, generation
        )
        if existing and existing["status"] == "succeeded":
            result = existing.get("result") or {}
            return None, result.get("variables", {}) if isinstance(result, dict) else {}

        current = self.nodes.get(branch_start_id)
        variables: Dict[str, Any] = {}
        while current:
            succeeded = self._node_succeeded(current.id, generation)
            if not succeeded:
                return current, variables
            variables = succeeded.get("variables", variables)
            self.store.upsert_branch_state(
                self.execution_id, parallel_node_id, branch_start_id, generation,
                "running", completed_node_id=current.id,
            )
            if current.data.anchorId or current.type == "end":
                return None, variables
            if current.type == "condition":
                result = evaluate_expression(
                    current.data.expression, {**variables, "ctx": variables}
                )
                current = self._get_next_node(current, result=bool(result))
            elif current.type == "loop":
                result = evaluate_expression(
                    current.data.expression, {**variables, "ctx": variables}
                )
                current = self._get_next_node(
                    current, handle="loop" if result else "exit"
                )
            else:
                current = self._get_next_node(current)
        return None, variables

    async def _execute_task(self, node: FlowNode, variables: Dict[str, Any],
                            attempt: int, generation: int) -> Dict[str, Any]:
        code = node.data.code
        if not code:
            return variables
        loop = asyncio.get_event_loop()
        try:
            result = await loop.run_in_executor(
                None, execute_python_sandbox, code, dict(variables), 30
            )
            return result
        except SandboxError as e:
            raise DurableExecutionError(f"Task failed: {e}")

    async def _execute_http(self, node: FlowNode, variables: Dict[str, Any],
                            attempt: int, generation: int) -> Dict[str, Any]:
        config = node.data.httpConfig
        if not config:
            raise DurableExecutionError("HTTP config is empty")

        async def _do_request():
            timeout = config.timeout or 30.0
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.request(
                    method=config.method,
                    url=config.url,
                    headers=config.headers or {},
                    content=config.body,
                )
                result = {
                    "status_code": response.status_code,
                    "headers": dict(response.headers),
                    "text": response.text,
                    "json": None,
                }
                try:
                    result["json"] = response.json()
                except Exception:
                    pass
                if response.status_code >= 400:
                    raise DurableExecutionError(
                        f"HTTP request failed with status {response.status_code}: {response.text[:200]}"
                    )
                return result

        result = await self.side_effects.run_once(
            self.execution_id, node.id, attempt, "http", _do_request, generation
        )
        self._guard_current()
        variables = dict(variables)
        variables[node.id + "_result"] = result
        return variables

    async def _execute_sql(self, node: FlowNode, variables: Dict[str, Any],
                           attempt: int, generation: int) -> Dict[str, Any]:
        config = node.data.sqlConfig
        if not config:
            raise DurableExecutionError("SQL config is empty")

        async def _do_sql():
            loop = asyncio.get_event_loop()
            def _run():
                conn = sqlite3.connect(config.connectionString)
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                cur.execute(config.query, config.params or [])
                q = config.query.strip().upper()
                if q.startswith("SELECT"):
                    rows = cur.fetchall()
                    res = [dict(r) for r in rows]
                else:
                    conn.commit()
                    res = {"rowcount": cur.rowcount, "lastrowid": cur.lastrowid}
                cur.close()
                conn.close()
                return res
            return await loop.run_in_executor(None, _run)

        result = await self.side_effects.run_once(
            self.execution_id, node.id, attempt, "sql", _do_sql, generation
        )
        self._guard_current()
        variables = dict(variables)
        variables[node.id + "_result"] = result
        return variables

    async def _execute_file_write(self, node: FlowNode, variables: Dict[str, Any],
                                   attempt: int, generation: int) -> Dict[str, Any]:
        config = node.data.fileWriteConfig
        if not config:
            raise DurableExecutionError("File write config is empty")

        path = config.path
        content = config.content or ""
        mode = config.mode or "w"

        async def _do_write():
            def _run():
                os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
                with open(path, mode, encoding="utf-8") as f:
                    f.write(content)
                return {"path": path, "bytes": len(content.encode("utf-8"))}
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, _run)

        result = await self.side_effects.run_once(
            self.execution_id, node.id, attempt, "file_write", _do_write, generation
        )
        self._guard_current()
        variables = dict(variables)
        variables[node.id + "_result"] = result
        return variables

    async def _execute_wait(self, node: FlowNode, variables: Dict[str, Any],
                            attempt: int, generation: int) -> Dict[str, Any]:
        seconds = node.data.seconds or 0
        if seconds > 0:
            ok = await self._sleep_with_cancel(seconds)
            if not ok:
                raise asyncio.CancelledError()
        return variables

    def _generate_approval_token(self, execution_id: str, node_id: str,
                                  attempt: int, generation: int) -> str:
        raw = f"{execution_id}:{node_id}:{attempt}:{generation}:{uuid.uuid4().hex}"
        return hashlib.sha256(raw.encode()).hexdigest()[:32]

    async def _execute_approval(self, node: FlowNode, variables: Dict[str, Any],
                                 attempt: int, generation: int) -> Dict[str, Any]:
        config = node.data.approvalConfig
        prompt = config.prompt if config else ""
        timeout_seconds = config.timeoutSeconds if config else 3600.0
        flow_version = None
        row = self.store.get_execution_row(self.execution_id)
        if row:
            flow_version = row.get("flow_version")
        branch_id = self._current_branch_id

        pending = self.store.get_pending_approval(
            self.execution_id, node.id, generation, branch_id
        )
        if pending is None:
            existing_row = self.store._conn.execute(
                "SELECT * FROM approvals WHERE execution_id = ? AND node_id = ? "
                "AND generation = ? AND (? IS NULL AND branch_id IS NULL OR branch_id = ?) "
                "ORDER BY created_at DESC LIMIT 1",
                (self.execution_id, node.id, generation, branch_id, branch_id),
            ).fetchone()
            if existing_row and existing_row["status"] in ("approved", "rejected", "expired", "cancelled"):
                approval = dict(existing_row)
                if approval["status"] == "approved":
                    result = {
                        "approved": True,
                        "approver": approval["responded_by"],
                        "comment": approval["response_comment"],
                    }
                    cur = self._load_state()
                    if cur["status"] == sm.AWAITING_APPROVAL:
                        self._transition(
                            sm.RUNNING, expected_states=[sm.AWAITING_APPROVAL],
                            payload={"approvalId": approval["approval_id"], "result": "approved", "generation": generation},
                        )
                    variables = dict(variables)
                    variables[node.id + "_result"] = result
                    return variables
                if approval["status"] == "rejected":
                    result = {
                        "approved": False,
                        "approver": approval["responded_by"],
                        "comment": approval["response_comment"],
                    }
                    variables[node.id + "_result"] = result
                    self.store.update_runtime_state(
                        self.execution_id, variables=variables,
                        current_node_id=node.id,
                        event_type="approval_rejected",
                        node_id=node.id, attempt=attempt, generation=generation,
                        payload={"approvalId": approval["approval_id"], "result": "rejected", "generation": generation},
                    )
                    raise DurableExecutionError(
                        f"Approval rejected by {approval['responded_by']}: {approval['response_comment'] or ''}"
                    )
                if approval["status"] == "expired":
                    raise DurableExecutionError(
                        f"Approval timed out at deadline {approval['deadline']}"
                    )
                raise asyncio.CancelledError()

            token = self._generate_approval_token(
                self.execution_id, node.id, attempt, generation
            )
            approval_id = f"appr_{uuid.uuid4().hex[:16]}"
            deadline = time.time() + timeout_seconds
            self.store.create_approval(
                approval_id, self.execution_id, node.id, attempt, generation,
                flow_version, branch_id, token, prompt, deadline,
            )
            pending = self.store.get_approval(approval_id)

        token = pending["token"]
        deadline = pending["deadline"]
        approval_id = pending["approval_id"]

        current_row = self._load_state()
        if current_row["status"] != sm.AWAITING_APPROVAL:
            seq = self._transition(
                sm.AWAITING_APPROVAL,
                expected_states=[sm.RUNNING],
                payload={
                    "approvalId": approval_id,
                    "nodeId": node.id,
                    "token": token,
                    "prompt": prompt,
                    "deadline": deadline,
                    "generation": generation,
                    "branchId": branch_id,
                },
            )
        else:
            seq = self.store.get_latest_seq(self.execution_id)
            snap = self.store.snapshot(self.execution_id)
            self._publish_snapshot(seq, snap)
        self._publish({
            "type": "approval_requested",
            "seq": seq,
            "approvalId": approval_id,
            "executionId": self.execution_id,
            "nodeId": node.id,
            "attempt": attempt,
            "generation": generation,
            "flowVersion": flow_version,
            "branchId": branch_id,
            "token": token,
            "prompt": prompt,
            "deadline": deadline,
        })

        while True:
            self._guard_current()
            approval = self.store.get_approval(approval_id)
            if approval is None:
                raise DurableExecutionError("Approval record disappeared")
            status = approval["status"]
            if status == "approved":
                result = {
                    "approved": True,
                    "approver": approval["responded_by"],
                    "comment": approval["response_comment"],
                }
                self._transition(
                    sm.RUNNING,
                    expected_states=[sm.AWAITING_APPROVAL],
                    payload={"approvalId": approval_id, "result": "approved", "generation": generation},
                )
                variables = dict(variables)
                variables[node.id + "_result"] = result
                return variables
            if status == "rejected":
                result = {
                    "approved": False,
                    "approver": approval["responded_by"],
                    "comment": approval["response_comment"],
                }
                variables[node.id + "_result"] = result
                self.store.update_runtime_state(
                    self.execution_id, variables=variables,
                    current_node_id=node.id,
                    event_type="approval_rejected",
                    node_id=node.id, attempt=attempt, generation=generation,
                    payload={"approvalId": approval_id, "result": "rejected", "generation": generation},
                )
                raise DurableExecutionError(
                    f"Approval rejected by {approval['responded_by']}: {approval['response_comment'] or ''}"
                )
            if status == "expired":
                raise DurableExecutionError(
                    f"Approval timed out at deadline {approval['deadline']}"
                )
            if status == "cancelled":
                raise asyncio.CancelledError()
            now = time.time()
            if now >= deadline:
                self.store.expire_approval(approval_id)
                continue
            wait_time = min(APPROVAL_WAIT_INTERVAL, deadline - now)
            try:
                await asyncio.wait_for(self._completed.wait(), timeout=wait_time)
            except asyncio.TimeoutError:
                pass
            else:
                if self._cancel_requested or not self._is_current():
                    raise asyncio.CancelledError()

    async def _execute_node_body(self, node: FlowNode, variables: Dict[str, Any],
                                 attempt: int, generation: int) -> Dict[str, Any]:
        if node.type == "task":
            return await self._execute_task(node, variables, attempt, generation)
        if node.type == "http":
            return await self._execute_http(node, variables, attempt, generation)
        if node.type == "sql":
            return await self._execute_sql(node, variables, attempt, generation)
        if node.type == "file_write":
            return await self._execute_file_write(node, variables, attempt, generation)
        if node.type == "approval":
            return await self._execute_approval(node, variables, attempt, generation)
        if node.type == "wait":
            return await self._execute_wait(node, variables, attempt, generation)
        if node.type == "start":
            return variables
        return variables

    async def _run_parallel_branch(self, parallel_node_id: str, branch_id: str,
                                   variables: Dict[str, Any], generation: int) -> None:
        prev_branch = self._current_branch_id
        prev_parallel = self._current_parallel_node
        self._current_branch_id = branch_id
        self._current_parallel_node = parallel_node_id
        try:
            await self._run_parallel_branch_inner(parallel_node_id, branch_id, variables, generation)
        finally:
            self._current_branch_id = prev_branch
            self._current_parallel_node = prev_parallel

    async def _run_parallel_branch_inner(self, parallel_node_id: str, branch_id: str,
                                          variables: Dict[str, Any], generation: int) -> None:
        current, branch_vars = self._recover_branch_position(
            parallel_node_id, branch_id, generation
        )
        if current is None:
            return

        while current and current.type != "end" and not self._cancel_requested:
            self._guard_current()
            node_id = current.id
            attempt = self.store.get_or_resume_attempt(
                self.execution_id, node_id, generation
            )
            try:
                seq, _ = self.store.update_runtime_state(
                    self.execution_id,
                    current_node_id=node_id,
                    event_type="node_enter",
                    node_id=node_id,
                    attempt=attempt,
                    generation=generation,
                    payload={"generation": generation, "parallel": parallel_node_id, "branch": branch_id},
                )
                branch_vars = await self._execute_node_body(
                    current, branch_vars, attempt, generation
                )
                self._guard_current()
                self.store.finish_node_attempt(
                    self.execution_id, node_id, attempt, "succeeded",
                    generation=generation, result={"variables": branch_vars}
                )
                self.store.checkpoint_node(
                    self.execution_id, node_id, branch_vars, {}, attempt, generation
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.store.finish_node_attempt(
                    self.execution_id, node_id, attempt, "failed",
                    generation=generation, error=str(e)
                )
                self.store.upsert_branch_state(
                    self.execution_id, parallel_node_id, branch_id, generation,
                    "failed", {"error": str(e)}, completed_node_id=node_id,
                )
                raise

            self.store.upsert_branch_state(
                self.execution_id, parallel_node_id, branch_id, generation,
                "running", completed_node_id=node_id,
            )

            if current.data.anchorId:
                break

            if current.type == "condition":
                result = evaluate_expression(
                    current.data.expression, {**branch_vars, "ctx": branch_vars}
                )
                current = self._get_next_node(current, result=bool(result))
            elif current.type == "loop":
                result = evaluate_expression(
                    current.data.expression, {**branch_vars, "ctx": branch_vars}
                )
                if result:
                    current = self._get_next_node(current, handle="loop")
                else:
                    current = self._get_next_node(current, handle="exit")
            else:
                current = self._get_next_node(current)

        self._guard_current()
        self.store.upsert_branch_state(
            self.execution_id, parallel_node_id, branch_id, generation,
            "succeeded", {"variables": branch_vars},
        )

    async def _execute_parallel(self, node: FlowNode, variables: Dict[str, Any],
                                attempt: int, generation: int) -> Dict[str, Any]:
        config = node.data.parallelConfig
        if not config or not config.branchNodeIds:
            return variables

        branch_ids = config.branchNodeIds
        for bid in branch_ids:
            existing = self.store.get_branch_state(
                self.execution_id, node.id, bid, generation
            )
            if not existing or existing["status"] != "succeeded":
                self.store.upsert_branch_state(
                    self.execution_id, node.id, bid, generation, "running"
                )

        tasks = [
            asyncio.create_task(
                self._run_parallel_branch(node.id, bid, variables, generation)
            )
            for bid in branch_ids
        ]
        try:
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
            for t in pending:
                t.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        except Exception:
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        for t in tasks:
            exc = t.exception()
            if exc:
                for t2 in tasks:
                    if not t2.done():
                        t2.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                raise exc
        self._guard_current()

        branches = self.store.get_branches_for_generation(
            self.execution_id, node.id, generation
        )
        by_id = {b["branch_id"]: b for b in branches}
        statuses = {bid: by_id.get(bid, {}).get("status") for bid in branch_ids}

        if any(s != "succeeded" for s in statuses.values()):
            failed = [bid for bid, s in statuses.items() if s != "succeeded"]
            raise DurableExecutionError(f"Parallel branches failed: {failed}")

        merged = dict(variables)
        result_by_branch = {}
        for bid in branch_ids:
            res = by_id[bid].get("result") or {}
            bvars = res.get("variables", {})
            result_by_branch[bid] = bvars
            for key, value in bvars.items():
                merged[key] = value
        merged[node.id + "_result"] = result_by_branch
        return merged

    def _get_retry_config(self, node: FlowNode):
        rc = node.data.retry
        if not rc or rc.maxAttempts <= 1:
            return None
        return rc

    async def _execute_with_retry(self, node: FlowNode, variables: Dict[str, Any],
                                  generation: int,
                                  initial_attempt: Optional[int] = None) -> Dict[str, Any]:
        rc = self._get_retry_config(node)
        max_attempts = rc.maxAttempts if rc else 1
        delay = rc.delaySeconds if rc else 0
        backoff = rc.backoff if rc else "fixed"
        max_delay = rc.maxDelaySeconds if rc else 0

        last_error = None
        for index in range(max_attempts):
            if index == 0 and initial_attempt is not None:
                attempt = initial_attempt
            else:
                attempt = self.store.begin_node_attempt(
                    self.execution_id, node.id, generation
                )
            try:
                result = await self._execute_node_body(
                    node, variables, attempt, generation
                )
                self._guard_current()
                self.store.finish_node_attempt(
                    self.execution_id, node.id, attempt, "succeeded",
                    generation=generation, result={"variables": result}
                )
                return result
            except asyncio.CancelledError:
                raise
            except Exception as e:
                last_error = e
                self.store.finish_node_attempt(
                    self.execution_id, node.id, attempt, "failed",
                    generation=generation, error=str(e)
                )
                if index < max_attempts - 1 and not self._cancel_requested:
                    self._transition(
                        sm.RETRY_WAIT,
                        expected_states=[sm.RUNNING],
                        payload={"nodeId": node.id, "error": str(e)},
                    )
                    sleep_time = delay
                    if backoff == "exponential":
                        sleep_time = min(delay * (2 ** index), max_delay) if max_delay else delay * (2 ** index)
                    try:
                        await asyncio.wait_for(self._wake_event.wait(), timeout=sleep_time)
                        self._wake_event.clear()
                    except asyncio.TimeoutError:
                        pass
                    self._guard_current()
                    row = self._load_state()
                    if row["status"] == sm.RETRY_WAIT:
                        self._transition(sm.RUNNING, expected_states=[sm.RETRY_WAIT])
                    if backoff == "exponential":
                        delay = sleep_time
                else:
                    raise

        raise last_error  # type: ignore[misc]

    async def run(self) -> None:
        try:
            row = self._load_state()
            if row["status"] not in (sm.QUEUED, sm.RUNNING, sm.PAUSING, sm.RETRY_WAIT, sm.AWAITING_APPROVAL):
                return
            self._executor_generation = int(row["executor_generation"])
            await self._execute_loop()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            try:
                row = self._load_state()
                if not sm.is_terminal(row["status"]) and self._is_current(row):
                    self._transition(sm.FAILED, error=str(e))
            except Exception:
                pass
        finally:
            self._completed_flag = True
            self._completed.set()

    def _resume_current_node(self, row: Dict[str, Any], generation: int) -> FlowNode:
        start_node = self._get_start_node()
        resume_from = row.get("resume_from_node_id")
        if resume_from:
            last = self.store.get_last_node_attempt(self.execution_id, resume_from)
            if last and int(last.get("generation", -1)) == generation and last["status"] == "succeeded":
                nxt = self._get_next_node(self.nodes[resume_from])
                return nxt if nxt else self.nodes[resume_from]
            return self.nodes.get(resume_from, start_node)
        return start_node

    async def _execute_loop(self) -> None:
        row = self._load_state()
        variables = dict(row["variables"])
        loop_counts = dict(row["loop_counts"])
        generation = row["generation"]
        current = self._resume_current_node(row, generation)

        while current and current.type != "end" and not self._cancel_requested:
            if not await self._wait_if_paused():
                return
            self._guard_current()

            node = current
            node_id = node.id
            attempt = self.store.get_or_resume_attempt(
                self.execution_id, node_id, generation
            )

            seq, snapshot = self.store.update_runtime_state(
                self.execution_id,
                current_node_id=node_id,
                variables=variables,
                loop_counts=loop_counts,
                event_type="node_enter",
                node_id=node_id,
                attempt=attempt,
                generation=generation,
                payload={"generation": generation},
            )
            self._runtime_event(seq, snapshot, "nodeEnter", node_id, attempt, generation, variables)

            try:
                if node.type == "parallel":
                    variables = await self._execute_parallel(
                        node, variables, attempt, generation
                    )
                    self._guard_current()
                    self.store.finish_node_attempt(
                        self.execution_id, node.id, attempt, "succeeded",
                        generation=generation, result={"variables": variables}
                    )
                elif node.type in ("task", "http", "sql", "file_write", "wait", "start"):
                    variables = await self._execute_with_retry(
                        node, variables, generation, initial_attempt=attempt
                    )
                elif node.type == "approval":
                    variables = await self._execute_node_body(
                        node, variables, attempt, generation
                    )
                    self._guard_current()
                    self.store.finish_node_attempt(
                        self.execution_id, node.id, attempt, "succeeded",
                        generation=generation, result={"variables": variables}
                    )
                else:
                    variables = await self._execute_node_body(
                        node, variables, attempt, generation
                    )
                    self._guard_current()
                    self.store.finish_node_attempt(
                        self.execution_id, node.id, attempt, "succeeded",
                        generation=generation, result={"variables": variables}
                    )
            except asyncio.CancelledError:
                return
            except Exception as e:
                seq, snapshot = self.store.update_runtime_state(
                    self.execution_id,
                    variables=variables,
                    event_type="node_error",
                    node_id=node_id,
                    attempt=attempt,
                    generation=generation,
                    payload={"error": str(e), "generation": generation},
                )
                self._runtime_event(seq, snapshot, "nodeError", node_id, attempt, generation, variables)
                self.store.finish_node_attempt(
                    self.execution_id, node_id, attempt, "failed",
                    generation=generation, error=str(e)
                )
                raise

            seq = self._checkpoint(node_id, variables, loop_counts, attempt, generation)

            if not await self._wait_if_paused():
                return

            if node.type == "condition":
                result = evaluate_expression(
                    node.data.expression, {**variables, "ctx": variables}
                )
                nxt = self._get_next_node(node, result=bool(result))
            elif node.type == "loop":
                result = evaluate_expression(
                    node.data.expression, {**variables, "ctx": variables}
                )
                if result:
                    cnt = loop_counts.get(node_id, 0) + 1
                    if cnt >= MAX_LOOP_COUNT:
                        raise DurableExecutionError(f"Loop {node_id} exceeded max iterations")
                    loop_counts[node_id] = cnt
                    nxt = self._get_next_node(node, handle="loop")
                else:
                    nxt = self._get_next_node(node, handle="exit")
            else:
                nxt = self._get_next_node(node)

            current = nxt

        if self._cancel_requested:
            return
        self._guard_current()

        if current and current.type == "end":
            seq, snapshot = self.store.update_runtime_state(
                self.execution_id,
                current_node_id=current.id,
                variables=variables,
                loop_counts=loop_counts,
                event_type="node_enter",
                node_id=current.id,
                generation=generation,
            )
            self._runtime_event(seq, snapshot, "nodeEnter", current.id, 0, generation, variables)
            self._checkpoint(current.id, variables, loop_counts, 0, generation)

        row = self._load_state()
        if not self._is_current(row) or sm.is_terminal(row["status"]):
            return
        if row["status"] == sm.PAUSING:
            self._transition(sm.PAUSED, expected_states=[sm.PAUSING])
            return
        self._transition(sm.SUCCEEDED, expected_states=[sm.RUNNING])

    async def wait_until_done(self, timeout: Optional[float] = None) -> bool:
        try:
            await asyncio.wait_for(self._completed.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False
