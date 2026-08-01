import asyncio
import json
import time
import sys
import subprocess

try:
    import websockets
except ImportError:
    print('Installing websockets...')
    subprocess.run([sys.executable, '-m', 'pip', 'install', 'websockets', '--quiet'])
    import websockets


async def test_websocket():
    uri = 'ws://localhost:8000/ws/execute'
    
    flow = {
        'id': 'test-ws',
        'name': 'WS Test',
        'createdAt': time.time(),
        'updatedAt': time.time(),
        'nodes': [
            {'id': 'start', 'type': 'start', 'position': {'x': 100, 'y': 100}, 'data': {'label': 'Start'}},
            {'id': 'task1', 'type': 'task', 'position': {'x': 100, 'y': 200}, 'data': {'label': 'Test', 'code': 'ctx["x"] = 42'}},
            {'id': 'end', 'type': 'end', 'position': {'x': 100, 'y': 300}, 'data': {'label': 'End'}}
        ],
        'edges': [
            {'id': 'e1', 'source': 'start', 'target': 'task1'},
            {'id': 'e2', 'source': 'task1', 'target': 'end'}
        ]
    }
    
    print('Testing WebSocket communication...')
    
    async with websockets.connect(uri) as websocket:
        print('✓ WebSocket connected')
        
        await websocket.send(json.dumps({'type': 'execute', 'flow': flow}))
        print('✓ Execute command sent')
        
        messages = []
        start_time = time.time()
        
        while time.time() - start_time < 10:
            try:
                response = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                msg = json.loads(response)
                messages.append(msg)
                var_str = str(msg.get('variables', {}))
                print(f'  ← {msg["type"]} {msg.get("nodeId", "")} vars={var_str[:60]}')
                
                if msg['type'] == 'completed':
                    print('\n=== WebSocket Test PASSED! ===')
                    print(f'Final variables: {msg["variables"]}')
                    assert msg['variables'].get('x') == 42
                    print('✓ x = 42 ✓')
                    return True
                    
            except asyncio.TimeoutError:
                continue
                
    return False

if __name__ == '__main__':
    result = asyncio.run(test_websocket())
    sys.exit(0 if result else 1)
