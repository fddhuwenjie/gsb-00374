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
from storage.versioned_flow_store import VersionedFlowStore
from engine.trigger_scheduler import TriggerScheduler
from runtime import get_runtime

scheduler: Optional[TriggerScheduler] = None

app = FastAPI(
    title="Flow Editor API",
    description="Visual Flow Editor and Persistent Execution Engine API",
    version="3.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_websocket_route("/ws/execute", websocket_endpoint)
app.include_router(flows_router)
app.include_router(triggers_router)
app.include_router(executions_router)


@app.on_event("startup")
async def startup_event():
    global scheduler
    runtime = get_runtime()
    trigger_store = TriggerStore(os.path.join(runtime.flows_dir, "triggers"))
    flow_store = VersionedFlowStore(runtime.flows_dir)

    async def on_flow_triggered(flow_id: str, vars: dict):
        flow = flow_store.get_flow(flow_id)
        if flow is None:
            return
        await runtime.manager.start_execution(flow, variables=vars)

    scheduler = TriggerScheduler(trigger_store, flow_store, on_flow_triggered)
    await scheduler.start()

    # Recover executions left in-flight by a previous process.
    await runtime.manager.recover_all()


@app.on_event("shutdown")
async def shutdown_event():
    global scheduler
    if scheduler:
        await scheduler.stop()
        scheduler = None


@app.get("/api/health")
async def health_check():
    runtime = get_runtime()
    return {
        "status": "ok",
        "version": "3.0.0",
        "flowsDir": runtime.flows_dir,
    }


@app.get("/api/scheduler/status")
async def scheduler_status():
    global scheduler
    return {"running": scheduler is not None and scheduler._running}


frontend_dist = os.path.join(BASE_DIR, "..", "frontend", "dist")
if os.path.exists(frontend_dist):
    app.mount("/", StaticFiles(directory=frontend_dist, html=True), name="static")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
