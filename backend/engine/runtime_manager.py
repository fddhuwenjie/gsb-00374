import asyncio
import json
import logging
from typing import Dict, Optional

from engine import state_machine as sm
from engine.durable_executor import DurableFlowExecutor
from engine.event_bus import EventBus
from engine.flow_versioning import diff_versions, flow_config_hash, node_config_hash
from engine.idempotency import SideEffectRegistry
from models.flow import FlowDefinition
from storage.event_store import EventStore
from storage.flow_store import FlowStore


logger = logging.getLogger(__name__)


class FlowVersionMissingError(Exception):
    pass


class RuntimeManager:
    def __init__(self, event_store: EventStore, flow_store: FlowStore,
                 event_bus: Optional[EventBus] = None):
        self.store = event_store
        self.flow_store = flow_store
        self.bus = event_bus or EventBus()
        self.side_effects = SideEffectRegistry(event_store)
        self._executors: Dict[str, DurableFlowExecutor] = {}
        self._tasks: Dict[str, asyncio.Task] = {}
        self._lock = asyncio.Lock()

    def get_executor(self, execution_id: str) -> Optional[DurableFlowExecutor]:
        return self._executors.get(execution_id)

    def is_running(self, execution_id: str) -> bool:
        task = self._tasks.get(execution_id)
        return task is not None and not task.done()

    def _load_flow(self, flow_id: str) -> FlowDefinition:
        flow = self.flow_store.get_flow(flow_id)
        if not flow:
            raise KeyError(f"Flow {flow_id} not found")
        return flow

    def _persist_flow_definition(self, flow: FlowDefinition) -> FlowDefinition:
        existing_flow = self.flow_store.get_flow(flow.id)
        if not existing_flow:
            return self.flow_store.create_flow(flow)
        return self.flow_store.update_flow(flow.id, flow) or existing_flow

    def _ensure_version(self, flow: FlowDefinition) -> int:
        latest = self.store.get_latest_flow_version(flow.id)
        config_hash = flow_config_hash(flow)
        if latest is not None:
            latest_row = self.store.get_flow_version(flow.id, latest)
            if latest_row and latest_row["node_config_hash"] == config_hash:
                return latest
        next_version = (latest or 0) + 1
        self.store.save_flow_version(
            flow.id, next_version, flow.name,
            json.dumps(flow.model_dump(), ensure_ascii=False),
            config_hash,
        )
        return next_version

    def _get_version_flow(self, flow_id: str, version: int) -> FlowDefinition:
        row = self.store.get_flow_version(flow_id, version)
        if not row:
            raise FlowVersionMissingError(
                f"Flow version {flow_id}@{version} not found; cannot resume execution"
            )
        return FlowDefinition(**row["definition_obj"])

    def list_versions(self, flow_id: str):
        return self.store.list_flow_versions(flow_id)

    def get_version_diff(self, flow_id: str, from_version: int, to_version: int):
        old_flow = self._get_version_flow(flow_id, from_version)
        new_flow = self._get_version_flow(flow_id, to_version)
        return diff_versions(old_flow, new_flow)

    def save_flow_as_new_version(self, flow: FlowDefinition) -> int:
        self._persist_flow_definition(flow)
        return self._ensure_version(flow)

    def delete_flow_safely(self, flow_id: str) -> bool:
        if self.store.any_version_has_active_executions(flow_id):
            return False
        self.store.delete_all_flow_versions(flow_id)
        return self.flow_store.delete_flow(flow_id)

    def delete_flow_version_safely(self, flow_id: str, version: int) -> bool:
        if self.store.version_has_active_executions(flow_id, version):
            return False
        return self.store.delete_flow_version(flow_id, version)

    async def start_execution(self, flow_id: str, variables: Optional[dict] = None,
                              execution_id: Optional[str] = None,
                              flow: Optional[FlowDefinition] = None,
                              bind_version: Optional[int] = None) -> str:
        if flow is None:
            if bind_version is not None:
                flow = self._get_version_flow(flow_id, bind_version)
            else:
                flow = self._load_flow(flow_id)
        else:
            try:
                self._persist_flow_definition(flow)
            except Exception:
                pass

        flow_version = bind_version if bind_version is not None else self._ensure_version(flow)
        config_hash = flow_config_hash(flow)

        if execution_id is None:
            execution_id = self.store.create_execution(
                flow.id, variables,
                flow_version=flow_version, node_config_hash=config_hash,
            )
        else:
            existing = self.store.get_execution_row(execution_id)
            if not existing:
                self.store.create_execution(
                    flow.id, variables, execution_id=execution_id,
                    flow_version=flow_version, node_config_hash=config_hash,
                )
            elif existing.get("flow_version") is None:
                pass

        async with self._lock:
            if self.is_running(execution_id):
                return execution_id
            row = self.store.get_execution_row(execution_id)
            if row and row["status"] in sm.TERMINAL_STATES:
                return execution_id
            snapshot = self.store.claim_execution_start(execution_id)
            if snapshot is None:
                return execution_id
            executor = DurableFlowExecutor(
                execution_id=execution_id,
                flow=flow,
                event_store=self.store,
                flow_store=self.flow_store,
                event_bus=self.bus,
                side_effects=self.side_effects,
            )
            self._executors[execution_id] = executor
            task = asyncio.create_task(self._run_and_cleanup(execution_id, executor))
            self._tasks[execution_id] = task
            self.bus.publish(execution_id, {"type": "snapshot", "seq": snapshot["latestSeq"], "snapshot": snapshot})
        return execution_id

    async def _run_and_cleanup(self, execution_id: str,
                               executor: DurableFlowExecutor) -> None:
        try:
            await executor.run()
        except Exception as e:
            logger.exception("Execution %s failed: %s", execution_id, e)
        finally:
            async with self._lock:
                self._tasks.pop(execution_id, None)
                self._executors.pop(execution_id, None)

    def _load_flow_for_execution(self, row) -> FlowDefinition:
        flow_id = row["flow_id"]
        flow_version = row.get("flow_version")
        if flow_version is not None:
            return self._get_version_flow(flow_id, int(flow_version))
        return self._load_flow(flow_id)

    def _respond_to_approval(self, execution_id: str, command: str, row,
                              comment: Optional[str] = None,
                              approver: Optional[str] = None,
                              token: Optional[str] = None) -> bool:
        pending = self.store.list_pending_approvals(execution_id)
        if not pending:
            return False
        approval = pending[0]
        if token is not None and approval["token"] != token:
            return False
        if approval["flow_version"] is not None and row.get("flow_version") is not None:
            if int(approval["flow_version"]) != int(row["flow_version"]):
                return False
        status = "approved" if command == "approve" else "rejected"
        result = self.store.respond_to_approval(
            approval["token"], status, approver or "system", comment
        )
        if result is None:
            return False
        if result["status"] != status:
            return False
        self.bus.publish(execution_id, {
            "type": "approval_response",
            "executionId": execution_id,
            "approvalId": approval["approval_id"],
            "status": status,
            "approver": approver,
            "comment": comment,
        })
        return True

    async def send_command(self, execution_id: str, command: str,
                           command_id: Optional[str] = None,
                           **kwargs) -> bool:
        if command_id:
            claim = self.store.claim_command(execution_id, command, command_id)
            if claim != "new":
                return claim in ("accepted", "duplicate")

        row = self.store.get_execution_row(execution_id)
        if not row:
            if command_id:
                self.store.complete_command(command_id, False)
            raise KeyError(f"Execution {execution_id} not found")

        accepted = False
        try:
            if not sm.command_allowed(command, row["status"]):
                return False

            if command == "cancel":
                _, _, snapshot = self.store.transition(
                    execution_id,
                    sm.CANCELLED,
                    expected_states=[sm.QUEUED, sm.RUNNING, sm.PAUSING, sm.PAUSED, sm.RETRY_WAIT, sm.AWAITING_APPROVAL],
                    increment_executor_generation=True,
                )
                self.store.cancel_approvals_for_execution(execution_id)
                executor = self._executors.get(execution_id)
                if executor:
                    await executor.request_cancel()
                self.bus.publish(execution_id, {"type": "snapshot", "seq": snapshot["latestSeq"], "snapshot": snapshot})
                accepted = True
                return True

            if command in ("approve", "reject"):
                if row["status"] != sm.AWAITING_APPROVAL:
                    return False
                return self._respond_to_approval(
                    execution_id, command, row,
                    comment=kwargs.get("comment"),
                    approver=kwargs.get("approver"),
                    token=kwargs.get("token"),
                )

            if command == "retry":
                flow = self._load_flow_for_execution(row)
                _, _, snapshot = self.store.transition(
                    execution_id,
                    sm.QUEUED,
                    expected_states=[sm.FAILED, sm.CANCELLED],
                    increment_generation=True,
                    increment_executor_generation=True,
                )
                executor = self._executors.get(execution_id)
                if executor:
                    await executor.request_retry()
                bound_version = int(row["flow_version"]) if row.get("flow_version") is not None else None
                await self.start_execution(
                    row["flow_id"], execution_id=execution_id,
                    flow=flow, bind_version=bound_version,
                )
                self.bus.publish(execution_id, {"type": "snapshot", "seq": snapshot["latestSeq"], "snapshot": snapshot})
                accepted = True
                return True

            if command in ("start", "resume") and row["status"] == sm.QUEUED:
                flow = self._load_flow_for_execution(row)
                bound_version = int(row["flow_version"]) if row.get("flow_version") is not None else None
                await self.start_execution(
                    row["flow_id"], execution_id=execution_id,
                    flow=flow, bind_version=bound_version,
                )
                accepted = True
                return True

            executor = self._executors.get(execution_id)
            if not executor:
                if row["status"] == sm.RETRY_WAIT and command == "resume":
                    self.store.transition(
                        execution_id,
                        sm.RUNNING,
                        expected_states=[sm.RETRY_WAIT],
                        increment_executor_generation=True,
                    )
                    flow = self._load_flow_for_execution(row)
                    bound_version = int(row["flow_version"]) if row.get("flow_version") is not None else None
                    await self.start_execution(
                        row["flow_id"], execution_id=execution_id,
                        flow=flow, bind_version=bound_version,
                    )
                    accepted = True
                    return True
                return False

            if command == "pause":
                await executor.request_pause()
            elif command == "resume":
                await executor.request_resume()
            accepted = True
            return True
        finally:
            if command_id:
                self.store.complete_command(command_id, accepted)

    async def recover_all(self) -> int:
        recoverable = self.store.list_recoverable()
        count = 0
        for row in recoverable:
            eid = row["execution_id"]
            if self.is_running(eid):
                continue
            if row["status"] == sm.PAUSING:
                try:
                    _, _, snapshot = self.store.transition(
                        eid,
                        sm.PAUSED,
                        expected_states=[sm.PAUSING],
                    )
                    self.bus.publish(eid, {"type": "snapshot", "seq": snapshot["latestSeq"], "snapshot": snapshot})
                except Exception:
                    pass
                continue
            if row["status"] == sm.PAUSED:
                continue
            if row["status"] == sm.AWAITING_APPROVAL:
                try:
                    flow = self._load_flow_for_execution(row)
                except (KeyError, FlowVersionMissingError) as e:
                    logger.warning("Cannot recover execution %s: %s", eid, e)
                    continue
                bound_version = int(row["flow_version"]) if row.get("flow_version") is not None else None
                await self.start_execution(
                    row["flow_id"], execution_id=eid, flow=flow,
                    bind_version=bound_version,
                )
                count += 1
                continue
            try:
                flow = self._load_flow_for_execution(row)
            except (KeyError, FlowVersionMissingError) as e:
                logger.warning("Cannot recover execution %s: %s", eid, e)
                continue
            bound_version = int(row["flow_version"]) if row.get("flow_version") is not None else None
            await self.start_execution(
                row["flow_id"], execution_id=eid, flow=flow,
                bind_version=bound_version,
            )
            count += 1
        return count

    async def shutdown(self) -> None:
        for executor in self._executors.values():
            executor._cancel_requested = True
            executor._wake_event.set()
            executor._completed.set()
        tasks = list(self._tasks.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
