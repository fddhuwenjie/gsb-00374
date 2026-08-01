import asyncio
import time
from datetime import datetime
from typing import Dict, Optional, Callable, Awaitable
from storage.trigger_store import TriggerStore
from storage.flow_store import FlowStore
from engine.executor import FlowExecutor
from engine.cron_parser import CronExpression, CronParseError
from models.flow import Trigger, Execution


class TriggerScheduler:
    def __init__(self, trigger_store: TriggerStore, flow_store: FlowStore,
                 on_flow_triggered: Optional[Callable[[str, dict], Awaitable[None]]] = None):
        self.trigger_store = trigger_store
        self.flow_store = flow_store
        self.on_flow_triggered = on_flow_triggered
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._last_check: Dict[str, float] = {}

    async def start(self):
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run_loop(self):
        while self._running:
            try:
                await self._check_cron_triggers()
            except Exception as e:
                print(f"[TriggerScheduler] Error checking cron triggers: {e}")
            await asyncio.sleep(30)

    async def _check_cron_triggers(self):
        now = datetime.now()
        triggers = self.trigger_store.list_triggers()

        for trigger in triggers:
            if not trigger.enabled or trigger.type != 'cron':
                continue

            try:
                cron = CronExpression(trigger.cronExpression)
                last_check = self._last_check.get(trigger.id, 0)

                for minute_offset in range(30):
                    check_time = now.replace(second=0, microsecond=0)
                    check_time = check_time.replace(minute=(check_time.minute - minute_offset) % 60)
                    if minute_offset > check_time.minute:
                        check_time = check_time.replace(hour=check_time.hour - 1)

                    check_timestamp = check_time.timestamp()
                    if check_timestamp <= last_check:
                        break

                    if cron.matches(check_time):
                        self._last_check[trigger.id] = time.time()
                        await self._trigger_flow(trigger, {'triggered_at': check_time.isoformat()})
                        break

            except CronParseError as e:
                print(f"[TriggerScheduler] Invalid cron expression for trigger {trigger.id}: {e}")
            except Exception as e:
                print(f"[TriggerScheduler] Error processing trigger {trigger.id}: {e}")

    async def trigger_webhook(self, webhook_path: str, payload: dict) -> Optional[Trigger]:
        trigger = self.trigger_store.get_trigger_by_webhook_path(webhook_path)
        if not trigger or not trigger.enabled:
            return None

        await self._trigger_flow(trigger, {'webhook_payload': payload})
        return trigger

    async def trigger_flow_completed(self, source_flow_id: str, execution_result: dict):
        triggers = self.trigger_store.get_triggers_by_source_flow(source_flow_id)
        for trigger in triggers:
            try:
                await self._trigger_flow(trigger, {
                    'source_flow_id': source_flow_id,
                    'execution_result': execution_result
                })
            except Exception as e:
                print(f"[TriggerScheduler] Error triggering flow from completion: {e}")

    async def _trigger_flow(self, trigger: Trigger, initial_vars: dict):
        flow = self.flow_store.get_flow(trigger.flowId)
        if not flow:
            print(f"[TriggerScheduler] Flow {trigger.flowId} not found for trigger {trigger.id}")
            return

        if self.on_flow_triggered:
            await self.on_flow_triggered(trigger.flowId, initial_vars)

        try:
            executor = FlowExecutor(flow, flow_store=self.flow_store)
            for key, value in initial_vars.items():
                executor.set_variable(key, value)

            await executor.execute()
        except Exception as e:
            print(f"[TriggerScheduler] Error executing triggered flow: {e}")

    def get_next_runs(self, trigger_id: str, count: int = 5) -> Optional[list]:
        trigger = self.trigger_store.get_trigger(trigger_id)
        if not trigger or trigger.type != 'cron' or not trigger.enabled:
            return None

        try:
            cron = CronExpression(trigger.cronExpression)
            return [dt.isoformat() for dt in cron.next_runs(count)]
        except CronParseError:
            return None
