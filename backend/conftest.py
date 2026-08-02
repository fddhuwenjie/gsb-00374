import os
import socket
import subprocess
import sys
import time

import pytest

BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))
SERVER_PORT = 8000


def _port_open(port: int) -> bool:
    with socket.socket() as s:
        s.settimeout(0.2)
        return s.connect_ex(('127.0.0.1', port)) == 0


@pytest.fixture(scope='session', autouse=True)
def live_server():
    """Self-starting fixture: legacy WebSocket tests expect a server on
    localhost:8000. Start one for the test session if none is running."""
    if _port_open(SERVER_PORT):
        yield
        return
    proc = subprocess.Popen(
        [sys.executable, '-m', 'uvicorn', 'main:app',
         '--host', '127.0.0.1', '--port', str(SERVER_PORT)],
        cwd=BACKEND_DIR,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        for _ in range(150):
            if _port_open(SERVER_PORT):
                break
            if proc.poll() is not None:
                raise RuntimeError('uvicorn exited during startup')
            time.sleep(0.2)
        else:
            raise RuntimeError('uvicorn failed to start in time')
        yield
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            proc.kill()
