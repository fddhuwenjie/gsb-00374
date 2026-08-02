import threading
import time

import pytest
import uvicorn

import main as main_module
from runtime import Runtime, set_runtime


@pytest.fixture
def ws_server_url(tmp_path):
    """Run WebSocket integration tests against an isolated random port."""
    runtime = Runtime(flows_dir=str(tmp_path / "flows"))
    set_runtime(runtime)

    config = uvicorn.Config(
        main_module.app,
        host="127.0.0.1",
        port=0,
        log_level="error",
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    deadline = time.time() + 10
    while not server.started and thread.is_alive() and time.time() < deadline:
        time.sleep(0.05)
    if not server.started:
        server.should_exit = True
        thread.join(timeout=5)
        set_runtime(None)
        pytest.fail("WebSocket test server did not start")

    port = next(
        sock.getsockname()[1]
        for listener in server.servers
        for sock in listener.sockets
    )
    try:
        yield f"ws://127.0.0.1:{port}/ws/execute"
    finally:
        server.should_exit = True
        thread.join(timeout=5)
        set_runtime(None)
