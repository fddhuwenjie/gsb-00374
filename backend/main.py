import os
import sys
import contextlib
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
from engine.trigger_scheduler import TriggerScheduler

scheduler: Optional[TriggerScheduler] = None

app = FastAPI(
    title="Flow Editor API",
    description="Visual Flow Editor and Execution Engine API with Triggers, Debugging, and Time Travel",
    version="2.0.0"
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
    trigger_store = TriggerStore(os.path.join(BASE_DIR, "flows"))
    flow_store = FlowStore(os.path.join(BASE_DIR, "flows"))

    async def on_flow_triggered(flow_id: str, vars: dict):
        pass

    scheduler = TriggerScheduler(trigger_store, flow_store, on_flow_triggered)
    await scheduler.start()


@app.on_event("shutdown")
async def shutdown_event():
    global scheduler
    if scheduler:
        await scheduler.stop()
        scheduler = None


@app.get("/api/health")
async def health_check():
    return {"status": "ok", "message": "Flow Engine API is running", "version": "2.0.0"}


@app.get("/api/scheduler/status")
async def scheduler_status():
    global scheduler
    return {
        "running": scheduler is not None and scheduler._running
    }


frontend_dist = os.path.join(BASE_DIR, "..", "frontend", "dist")
if os.path.exists(frontend_dist):
    app.mount("/", StaticFiles(directory=frontend_dist, html=True), name="static")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
