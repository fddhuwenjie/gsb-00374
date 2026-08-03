import os
import sys
from typing import Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

from routers.flows import router as flows_router
from routers.triggers import router as triggers_router
from routers.executions import router as executions_router
from ws.execute import websocket_endpoint
from storage.trigger_store import TriggerStore
from storage.flow_store import FlowStore
from storage.event_store import EventStore
from engine.event_bus import EventBus
from engine.runtime_manager import RuntimeManager
from engine.trigger_scheduler import TriggerScheduler


scheduler: Optional[TriggerScheduler] = None


def create_app(data_dir: Optional[str] = None) -> FastAPI:
    app = FastAPI(
        title="Flow Editor API",
        description="Visual Flow Editor and Execution Engine API",
        version="3.0.0",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    flows_dir = data_dir or os.path.join(BASE_DIR, "flows")
    os.makedirs(flows_dir, exist_ok=True)
    db_path = os.path.join(flows_dir, "events.db")

    flow_store = FlowStore(flows_dir)
    event_store = EventStore(db_path)
    event_bus = EventBus()
    runtime_manager = RuntimeManager(event_store, flow_store, event_bus)
    trigger_store = TriggerStore(flows_dir)

    app.state.flow_store = flow_store
    app.state.event_store = event_store
    app.state.event_bus = event_bus
    app.state.runtime_manager = runtime_manager
    app.state.trigger_store = trigger_store
    app.state.flows_dir = flows_dir

    app.add_websocket_route("/ws/execute", websocket_endpoint)
    app.include_router(flows_router)
    app.include_router(triggers_router)
    app.include_router(executions_router)

    @app.on_event("startup")
    async def startup_event():
        global scheduler

        async def on_flow_triggered(flow_id: str, vars: dict):
            try:
                await runtime_manager.start_execution(flow_id, variables=vars)
            except Exception:
                pass

        scheduler = TriggerScheduler(trigger_store, flow_store, on_flow_triggered)
        await scheduler.start()
        await runtime_manager.recover_all()

    @app.on_event("shutdown")
    async def shutdown_event():
        global scheduler
        if scheduler:
            await scheduler.stop()
            scheduler = None
        await runtime_manager.shutdown()
        event_store.close()

    @app.get("/api/health")
    async def health_check():
        return {"status": "ok", "version": "3.0.0"}

    @app.get("/api/scheduler/status")
    async def scheduler_status():
        return {"running": scheduler is not None and getattr(scheduler, "_running", False)}

    frontend_dist = os.path.join(BASE_DIR, "..", "frontend", "dist")
    if os.path.exists(frontend_dist):
        app.mount("/", StaticFiles(directory=frontend_dist, html=True), name="static")

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
