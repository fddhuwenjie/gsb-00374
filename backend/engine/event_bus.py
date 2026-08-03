import asyncio
from typing import Any, Callable, Dict, List, Optional, Set


class EventBus:
    def __init__(self):
        self._subscribers: Dict[str, Set[Callable[[Dict[str, Any]], Any]]] = {}

    def subscribe(self, execution_id: str, callback: Callable[[Dict[str, Any]], Any]) -> None:
        if execution_id not in self._subscribers:
            self._subscribers[execution_id] = set()
        self._subscribers[execution_id].add(callback)

    def unsubscribe(self, execution_id: str, callback: Callable[[Dict[str, Any]], Any]) -> None:
        if execution_id in self._subscribers:
            self._subscribers[execution_id].discard(callback)
            if not self._subscribers[execution_id]:
                del self._subscribers[execution_id]

    def publish(self, execution_id: str, event: Dict[str, Any]) -> None:
        callbacks = self._subscribers.get(execution_id, set()).copy()
        for cb in callbacks:
            try:
                res = cb(event)
                if asyncio.iscoroutine(res):
                    asyncio.ensure_future(res)
            except Exception:
                pass

    def has_subscribers(self, execution_id: str) -> bool:
        return bool(self._subscribers.get(execution_id))
