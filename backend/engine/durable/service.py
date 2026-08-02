"""Process-wide singleton wiring for the durable engine.

REST endpoints, the WebSocket monitor and the app share one :class:`DurableEngine`
bound to the backend ``flows`` directory. Tests construct their own engine with a
temp directory instead of importing this module.
"""

import os
from typing import Optional

from engine.durable.engine import DurableEngine

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DURABLE_ROOT = os.path.join(BASE_DIR, "flows")

_engine: Optional[DurableEngine] = None


def get_engine() -> DurableEngine:
    global _engine
    if _engine is None:
        _engine = DurableEngine(DURABLE_ROOT)
    return _engine
