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


async def test_variable_direct_access():
    uri = 'ws://localhost:8000/ws/execute'
    
    flow = {
        'id': 'test-direct-vars',
        'name': 'Direct Variable Access Test',
        'createdAt': time.time(),
        'updatedAt': time.time(),
        'nodes': [
            {'id': 'start', 'type': 'start', 'position': {'x': 100, 'y': 100}, 'data': {'label': 'Start'}},
            {'id': 'task1', 'type': 'task', 'position': {'x': 100, 'y': 200}, 'data': {'label': 'Set x=10', 'code': 'ctx["x"] = 10'}},
            {'id': 'condition', 'type': 'condition', 'position': {'x': 100, 'y': 300}, 'data': {'label': 'x > 5', 'expression': 'x > 5'}},
            {'id': 'task_true', 'type': 'task', 'position': {'x': 0, 'y': 400}, 'data': {'label': 'True', 'code': 'ctx["result"] = "yes"'}},
            {'id': 'task_false', 'type': 'task', 'position': {'x': 200, 'y': 400}, 'data': {'label': 'False', 'code': 'ctx["result"] = "no"'}},
            {'id': 'end', 'type': 'end', 'position': {'x': 100, 'y': 500}, 'data': {'label': 'End'}}
        ],
        'edges': [
            {'id': 'e1', 'source': 'start', 'target': 'task1'},
            {'id': 'e2', 'source': 'task1', 'target': 'condition'},
            {'id': 'e3', 'source': 'condition', 'target': 'task_true', 'sourceHandle': 'true'},
            {'id': 'e4', 'source': 'condition', 'target': 'task_false', 'sourceHandle': 'false'},
            {'id': 'e5', 'source': 'task_true', 'target': 'end'},
            {'id': 'e6', 'source': 'task_false', 'target': 'end'}
        ]
    }
    
    print('Testing WebSocket with direct variable access (x > 5)...')
    
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
                print(f'  ← {msg["type"]:12} {msg.get("nodeId", ""):12} vars={var_str[:60]}')
                
                if msg['type'] == 'completed':
                    print('\n=== Direct Variable Test PASSED! ===')
                    print(f'Final variables: {msg["variables"]}')
                    assert msg['variables'].get('x') == 10, f'x should be 10, got {msg["variables"].get("x")}'
                    assert msg['variables'].get('result') == 'yes', f'result should be yes, got {msg["variables"].get("result")}'
                    print('✓ x = 10 ✓')
                    print('✓ result = yes ✓')
                    print('✓ Direct variable access "x > 5" works correctly! ✓')
                    return True
                    
            except asyncio.TimeoutError:
                continue
                
    return False


async def test_loop_direct_access():
    uri = 'ws://localhost:8000/ws/execute'
    
    flow = {
        'id': 'test-loop-direct',
        'name': 'Loop Direct Variable Test',
        'createdAt': time.time(),
        'updatedAt': time.time(),
        'nodes': [
            {'id': 'start', 'type': 'start', 'position': {'x': 100, 'y': 100}, 'data': {'label': 'Start'}},
            {'id': 'task_init', 'type': 'task', 'position': {'x': 100, 'y': 200}, 'data': {'label': 'i = 0', 'code': 'ctx["i"] = 0'}},
            {'id': 'loop', 'type': 'loop', 'position': {'x': 100, 'y': 300}, 'data': {'label': 'i < 3', 'expression': 'i < 3'}},
            {'id': 'loop_anchor', 'type': 'task', 'position': {'x': 100, 'y': 350}, 'data': {'label': 'Anchor', 'code': 'pass'}},
            {'id': 'task_inc', 'type': 'task', 'position': {'x': 100, 'y': 400}, 'data': {'label': 'i = i + 1', 'code': 'ctx["i"] = ctx["i"] + 1'}},
            {'id': 'end', 'type': 'end', 'position': {'x': 100, 'y': 500}, 'data': {'label': 'End'}}
        ],
        'edges': [
            {'id': 'e1', 'source': 'start', 'target': 'task_init'},
            {'id': 'e2', 'source': 'task_init', 'target': 'loop'},
            {'id': 'e3', 'source': 'loop', 'target': 'loop_anchor', 'sourceHandle': 'loop'},
            {'id': 'e4', 'source': 'loop', 'target': 'end', 'sourceHandle': None},
            {'id': 'e5', 'source': 'loop_anchor', 'target': 'task_inc'},
            {'id': 'e6', 'source': 'task_inc', 'target': 'loop'}
        ]
    }
    
    print('\nTesting WebSocket with loop direct variable access (i < 3)...')
    
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
                print(f'  ← {msg["type"]:12} {msg.get("nodeId", ""):12} vars={var_str[:60]}')
                
                if msg['type'] == 'completed':
                    print('\n=== Loop Direct Variable Test PASSED! ===')
                    print(f'Final variables: {msg["variables"]}')
                    assert msg['variables'].get('i') == 3, f'i should be 3, got {msg["variables"].get("i")}'
                    print('✓ i = 3 ✓')
                    print('✓ Direct variable access "i < 3" works correctly! ✓')
                    return True
                    
            except asyncio.TimeoutError:
                continue
                
    return False


async def main():
    result1 = await test_variable_direct_access()
    result2 = await test_loop_direct_access()
    
    if result1 and result2:
        print('\n=== All Direct Variable Access Tests PASSED! ===')
        return True
    return False

if __name__ == '__main__':
    result = asyncio.run(main())
    sys.exit(0 if result else 1)
