from typing import Any, Awaitable, Callable, Dict, Optional

from storage.event_store import EventStore


class SideEffectRegistry:
    def __init__(self, store: EventStore):
        self._store = store

    def idempotency_key(self, execution_id: str, node_id: str, attempt: int) -> str:
        return self._store.idempotency_key(execution_id, node_id, attempt)

    def count_calls(self, execution_id: str, node_id: str,
                    side_effect_name: Optional[str] = None) -> int:
        return self._store.count_side_effect_calls(
            execution_id, node_id, side_effect_name
        )

    async def run_once(
        self,
        execution_id: str,
        node_id: str,
        attempt: int,
        side_effect_name: str,
        func: Callable[[], Awaitable[Any]],
        generation: int = 0,
    ) -> Any:
        existing = self._store.get_side_effect(execution_id, node_id, attempt)
        if existing and existing["status"] == "completed":
            return existing["result"]

        key, claimed = self._store.begin_side_effect(
            execution_id, node_id, attempt, generation
        )
        if claimed:
            self._store.record_side_effect_call(
                execution_id, node_id, attempt, side_effect_name, generation
            )
            try:
                result = await func()
            except Exception as e:
                self._store.fail_side_effect(execution_id, node_id, attempt, str(e))
                raise
            self._store.complete_side_effect(execution_id, node_id, attempt, result)
            return result

        existing = self._store.get_side_effect(execution_id, node_id, attempt)
        if existing and existing["status"] == "completed":
            return existing["result"]

        self._store.record_side_effect_call(
            execution_id, node_id, attempt, side_effect_name, generation
        )
        try:
            result = await func()
        except Exception as e:
            self._store.fail_side_effect(execution_id, node_id, attempt, str(e))
            raise
        self._store.complete_side_effect(execution_id, node_id, attempt, result)
        return result

    def peek_result(self, execution_id: str, node_id: str, attempt: int) -> Optional[Dict[str, Any]]:
        return self._store.get_side_effect(execution_id, node_id, attempt)
