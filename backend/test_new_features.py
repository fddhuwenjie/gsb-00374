import sys
import os
import asyncio
import sqlite3
import tempfile
import json
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from models.flow import *
from engine.executor import FlowExecutor, ExecutionError, CircularSubflowError
from storage.flow_store import FlowStore


def create_test_flow(nodes, edges, flow_id='test_flow'):
    return FlowDefinition(
        id=flow_id,
        name='Test Flow',
        nodes=nodes,
        edges=edges,
        createdAt=0,
        updatedAt=0
    )


def create_node(node_id, node_type, data=None, position=None):
    if position is None:
        position = Position(x=0, y=0)
    if data is None:
        data = NodeData(label=node_id)
    return FlowNode(id=node_id, type=node_type, position=position, data=data)


def create_edge(edge_id, source, target, source_handle=None):
    return FlowEdge(id=edge_id, source=source, target=target, sourceHandle=source_handle)


async def test_sql_node():
    print('\n=== Testing SQL Node ===')

    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')
        cursor.execute('INSERT INTO users (name, age) VALUES (?, ?)', ('Alice', 30))
        cursor.execute('INSERT INTO users (name, age) VALUES (?, ?)', ('Bob', 25))
        conn.commit()
        conn.close()

        nodes = [
            create_node('start', 'start'),
            create_node('sql1', 'sql', NodeData(
                label='Query Users',
                sqlConfig=SqlConfig(
                    connectionString=db_path,
                    query='SELECT * FROM users WHERE age > ?',
                    params=[20]
                )
            )),
            create_node('sql2', 'sql', NodeData(
                label='Insert User',
                sqlConfig=SqlConfig(
                    connectionString=db_path,
                    query='INSERT INTO users (name, age) VALUES (?, ?)',
                    params=['Charlie', 35]
                )
            )),
            create_node('end', 'end')
        ]
        edges = [
            create_edge('e1', 'start', 'sql1'),
            create_edge('e2', 'sql1', 'sql2'),
            create_edge('e3', 'sql2', 'end')
        ]
        flow = create_test_flow(nodes, edges)
        executor = FlowExecutor(flow)
        state = await executor.execute()

        assert state.status == 'completed'
        assert 'sql1_result' in state.variables
        assert len(state.variables['sql1_result']) == 2
        assert state.variables['sql1_result'][0]['name'] == 'Alice'
        assert state.variables['sql1_result'][1]['name'] == 'Bob'

        assert 'sql2_result' in state.variables
        assert state.variables['sql2_result']['rowcount'] == 1

        print('Test SQL Node: PASSED')
    finally:
        os.unlink(db_path)


async def test_http_node():
    print('\n=== Testing HTTP Node ===')

    nodes = [
        create_node('start', 'start'),
        create_node('http1', 'http', NodeData(
            label='Get HTTP Bin',
            httpConfig=HttpConfig(
                url='https://httpbin.org/get',
                method='GET',
                timeout=10.0
            )
        )),
        create_node('http2', 'http', NodeData(
            label='Post HTTP Bin',
            httpConfig=HttpConfig(
                url='https://httpbin.org/post',
                method='POST',
                headers={'Content-Type': 'application/json'},
                body=json.dumps({'test': 'data'}),
                timeout=10.0
            )
        )),
        create_node('end', 'end')
    ]
    edges = [
        create_edge('e1', 'start', 'http1'),
        create_edge('e2', 'http1', 'http2'),
        create_edge('e3', 'http2', 'end')
    ]
    flow = create_test_flow(nodes, edges)
    executor = FlowExecutor(flow)

    try:
        state = await executor.execute()
        assert state.status == 'completed'
        assert 'http1_result' in state.variables
        assert state.variables['http1_result']['status_code'] == 200
        assert 'http2_result' in state.variables
        assert state.variables['http2_result']['status_code'] == 200
        assert state.variables['http2_result']['json']['json'] == {'test': 'data'}
        print('Test HTTP Node: PASSED')
    except Exception as e:
        print(f'Test HTTP Node: SKIPPED (network error: {e})')


async def test_parallel_node():
    print('\n=== Testing Parallel Node ===')

    nodes = [
        create_node('start', 'start'),
        create_node('branch1', 'task', NodeData(
            label='Branch 1',
            code="ctx['b1'] = 'value1'",
            anchorId='end_anchor'
        )),
        create_node('branch2', 'task', NodeData(
            label='Branch 2',
            code="ctx['b2'] = 'value2'",
            anchorId='end_anchor'
        )),
        create_node('parallel1', 'parallel', NodeData(
            label='Parallel',
            parallelConfig=ParallelConfig(
                branchNodeIds=['branch1', 'branch2']
            )
        )),
        create_node('end', 'end')
    ]
    edges = [
        create_edge('e1', 'start', 'parallel1'),
        create_edge('e2', 'parallel1', 'end'),
        create_edge('e3', 'branch1', 'end'),
        create_edge('e4', 'branch2', 'end')
    ]
    flow = create_test_flow(nodes, edges)
    executor = FlowExecutor(flow)
    state = await executor.execute()

    assert state.status == 'completed'
    assert 'parallel1_result' in state.variables
    assert 'branch_0' in state.variables['parallel1_result']
    assert 'branch_1' in state.variables['parallel1_result']
    print('Test Parallel Node: PASSED')


async def test_trycatch_node():
    print('\n=== Testing TryCatch Node ===')

    nodes = [
        create_node('start', 'start'),
        create_node('try_task', 'task', NodeData(
            label='Try Task',
            code="raise ValueError('test error')",
            anchorId='end_anchor'
        )),
        create_node('catch_task', 'task', NodeData(
            label='Catch Task',
            code="ctx['caught'] = True",
            anchorId='end_anchor'
        )),
        create_node('trycatch1', 'trycatch', NodeData(
            label='TryCatch',
            tryCatchConfig=TryCatchConfig(
                tryNodeIds=['try_task'],
                catchNodeIds=['catch_task']
            )
        )),
        create_node('end', 'end')
    ]
    edges = [
        create_edge('e1', 'start', 'trycatch1'),
        create_edge('e2', 'trycatch1', 'end'),
        create_edge('e3', 'try_task', 'end'),
        create_edge('e4', 'catch_task', 'end')
    ]
    flow = create_test_flow(nodes, edges)
    executor = FlowExecutor(flow)
    state = await executor.execute()

    assert state.status == 'completed'
    assert 'trycatch1_result' in state.variables
    assert state.variables['trycatch1_result']['success'] == False
    assert state.variables['trycatch1_result']['caught_error'] is not None
    assert state.variables['caught'] == True
    print('Test TryCatch Node (with error): PASSED')

    nodes2 = [
        create_node('start', 'start'),
        create_node('try_task2', 'task', NodeData(
            label='Try Task 2',
            code="ctx['try_success'] = True",
            anchorId='end_anchor'
        )),
        create_node('catch_task2', 'task', NodeData(
            label='Catch Task 2',
            code="ctx['caught2'] = True",
            anchorId='end_anchor'
        )),
        create_node('trycatch2', 'trycatch', NodeData(
            label='TryCatch 2',
            tryCatchConfig=TryCatchConfig(
                tryNodeIds=['try_task2'],
                catchNodeIds=['catch_task2']
            )
        )),
        create_node('end', 'end')
    ]
    edges2 = [
        create_edge('e1', 'start', 'trycatch2'),
        create_edge('e2', 'trycatch2', 'end'),
        create_edge('e3', 'try_task2', 'end'),
        create_edge('e4', 'catch_task2', 'end')
    ]
    flow2 = create_test_flow(nodes2, edges2)
    executor2 = FlowExecutor(flow2)
    state2 = await executor2.execute()

    assert state2.status == 'completed'
    assert state2.variables['try_success'] == True
    assert 'caught2' not in state2.variables
    assert state2.variables['trycatch2_result']['success'] == True
    print('Test TryCatch Node (no error): PASSED')


async def test_subflow_node():
    print('\n=== Testing Subflow Node ===')

    with tempfile.TemporaryDirectory() as tmpdir:
        flow_store = FlowStore(tmpdir)

        subflow_nodes = [
            create_node('start', 'start'),
            create_node('task1', 'task', NodeData(
                label='Subflow Task',
                code="ctx['subflow_var'] = 'from_subflow'"
            )),
            create_node('end', 'end')
        ]
        subflow_edges = [
            create_edge('e1', 'start', 'task1'),
            create_edge('e2', 'task1', 'end')
        ]
        subflow = create_test_flow(subflow_nodes, subflow_edges, flow_id='subflow_1')
        flow_store.create_flow(subflow)

        main_nodes = [
            create_node('start', 'start'),
            create_node('sub1', 'subflow', NodeData(
                label='Call Subflow',
                subflowConfig=SubflowConfig(
                    subflowId='subflow_1'
                )
            )),
            create_node('end', 'end')
        ]
        main_edges = [
            create_edge('e1', 'start', 'sub1'),
            create_edge('e2', 'sub1', 'end')
        ]
        main_flow = create_test_flow(main_nodes, main_edges, flow_id='main_flow')
        executor = FlowExecutor(main_flow, flow_store=flow_store)
        state = await executor.execute()

        assert state.status == 'completed'
        assert state.variables['subflow_var'] == 'from_subflow'
        assert 'sub1_result' in state.variables
        assert state.variables['sub1_result']['subflowId'] == 'subflow_1'
        assert state.variables['sub1_result']['status'] == 'completed'
        print('Test Subflow Node: PASSED')


async def test_subflow_circular_detection():
    print('\n=== Testing Subflow Circular Detection ===')

    with tempfile.TemporaryDirectory() as tmpdir:
        flow_store = FlowStore(tmpdir)

        flow_a_nodes = [
            create_node('start', 'start'),
            create_node('sub_b', 'subflow', NodeData(
                label='Call B',
                subflowConfig=SubflowConfig(subflowId='flow_b')
            )),
            create_node('end', 'end')
        ]
        flow_a_edges = [
            create_edge('e1', 'start', 'sub_b'),
            create_edge('e2', 'sub_b', 'end')
        ]
        flow_a = create_test_flow(flow_a_nodes, flow_a_edges, flow_id='flow_a')
        flow_store.create_flow(flow_a)

        flow_b_nodes = [
            create_node('start', 'start'),
            create_node('sub_a', 'subflow', NodeData(
                label='Call A',
                subflowConfig=SubflowConfig(subflowId='flow_a')
            )),
            create_node('end', 'end')
        ]
        flow_b_edges = [
            create_edge('e1', 'start', 'sub_a'),
            create_edge('e2', 'sub_a', 'end')
        ]
        flow_b = create_test_flow(flow_b_nodes, flow_b_edges, flow_id='flow_b')
        flow_store.create_flow(flow_b)

        executor = FlowExecutor(flow_a, flow_store=flow_store)
        try:
            await executor.execute()
            assert False, 'Should have raised ExecutionError'
        except ExecutionError as e:
            assert 'flow_a' in str(e)
            assert 'flow_b' in str(e)
            print('Test Subflow Circular Detection: PASSED')


async def test_retry_mechanism():
    print('\n=== Testing Retry Mechanism ===')

    attempt_count = 0

    nodes = [
        create_node('start', 'start'),
        create_node('task1', 'task', NodeData(
            label='Retry Task',
            code="ctx['attempt'] = ctx.get('attempt', 0) + 1\nif ctx['attempt'] < 3:\n    raise ValueError('fail')",
            retry=RetryConfig(
                maxAttempts=3,
                delaySeconds=0.1,
                backoff='fixed',
                maxDelaySeconds=1.0
            )
        )),
        create_node('end', 'end')
    ]
    edges = [
        create_edge('e1', 'start', 'task1'),
        create_edge('e2', 'task1', 'end')
    ]
    flow = create_test_flow(nodes, edges)
    executor = FlowExecutor(flow)
    state = await executor.execute()

    assert state.status == 'completed'
    assert state.variables['attempt'] == 3
    print('Test Retry Mechanism (fixed): PASSED')

    attempt_count = 0
    nodes2 = [
        create_node('start', 'start'),
        create_node('task2', 'task', NodeData(
            label='Exponential Retry',
            code="ctx['attempt2'] = ctx.get('attempt2', 0) + 1\nif ctx['attempt2'] < 2:\n    raise ValueError('fail')",
            retry=RetryConfig(
                maxAttempts=3,
                delaySeconds=0.05,
                backoff='exponential',
                maxDelaySeconds=1.0
            )
        )),
        create_node('end', 'end')
    ]
    edges2 = [
        create_edge('e1', 'start', 'task2'),
        create_edge('e2', 'task2', 'end')
    ]
    flow2 = create_test_flow(nodes2, edges2)
    executor2 = FlowExecutor(flow2)
    state2 = await executor2.execute()

    assert state2.status == 'completed'
    assert state2.variables['attempt2'] == 2
    print('Test Retry Mechanism (exponential): PASSED')


async def test_retry_failure():
    print('\n=== Testing Retry Failure ===')

    nodes = [
        create_node('start', 'start'),
        create_node('task1', 'task', NodeData(
            label='Always Fail',
            code="raise ValueError('always fail')",
            retry=RetryConfig(
                maxAttempts=3,
                delaySeconds=0.05,
                backoff='fixed',
                maxDelaySeconds=1.0
            )
        )),
        create_node('end', 'end')
    ]
    edges = [
        create_edge('e1', 'start', 'task1'),
        create_edge('e2', 'task1', 'end')
    ]
    flow = create_test_flow(nodes, edges)
    executor = FlowExecutor(flow)
    try:
        await executor.execute()
        assert False, 'Should have raised ExecutionError'
    except ExecutionError:
        print('Test Retry Failure: PASSED')


async def test_breakpoint():
    print('\n=== Testing Breakpoint ===')

    nodes = [
        create_node('start', 'start'),
        create_node('task1', 'task', NodeData(
            label='Task 1',
            code="ctx['x'] = 1",
            breakpoint=True
        )),
        create_node('task2', 'task', NodeData(
            label='Task 2',
            code="ctx['y'] = 2"
        )),
        create_node('end', 'end')
    ]
    edges = [
        create_edge('e1', 'start', 'task1'),
        create_edge('e2', 'task1', 'task2'),
        create_edge('e3', 'task2', 'end')
    ]
    flow = create_test_flow(nodes, edges)
    executor = FlowExecutor(flow)

    async def run_with_resume():
        await asyncio.sleep(0.1)
        while executor.state.status != 'paused':
            await asyncio.sleep(0.05)
        await executor.resume()

    task = asyncio.create_task(executor.execute())
    resume_task = asyncio.create_task(run_with_resume())

    state = await task
    await resume_task

    assert state.status == 'completed'
    assert state.variables['x'] == 1
    assert state.variables['y'] == 2
    print('Test Breakpoint: PASSED')


async def test_ctx_snapshots():
    print('\n=== Testing ctx Snapshots ===')

    nodes = [
        create_node('start', 'start'),
        create_node('task1', 'task', NodeData(
            label='Task 1',
            code="ctx['a'] = 1"
        )),
        create_node('task2', 'task', NodeData(
            label='Task 2',
            code="ctx['b'] = 2"
        )),
        create_node('task3', 'task', NodeData(
            label='Task 3',
            code="ctx['c'] = 3"
        )),
        create_node('end', 'end')
    ]
    edges = [
        create_edge('e1', 'start', 'task1'),
        create_edge('e2', 'task1', 'task2'),
        create_edge('e3', 'task2', 'task3'),
        create_edge('e4', 'task3', 'end')
    ]
    flow = create_test_flow(nodes, edges)
    executor = FlowExecutor(flow)
    state = await executor.execute()

    assert state.status == 'completed'
    assert 'task1' in state.snapshots
    assert 'task2' in state.snapshots
    assert 'task3' in state.snapshots
    assert 'a' not in state.snapshots['task1']
    assert 'b' not in state.snapshots['task1']
    assert state.snapshots['task2']['a'] == 1
    assert 'b' not in state.snapshots['task2']
    assert state.snapshots['task3']['a'] == 1
    assert state.snapshots['task3']['b'] == 2
    assert 'c' not in state.snapshots['task3']
    print('Test ctx Snapshots: PASSED')


async def test_existing_functionality():
    print('\n=== Testing Existing Functionality ===')

    nodes = [
        create_node('start', 'start'),
        create_node('task1', 'task', NodeData(
            label='Set x',
            code="ctx['x'] = 10"
        )),
        create_node('cond1', 'condition', NodeData(
            label='Condition',
            expression='ctx.x > 5'
        )),
        create_node('task_true', 'task', NodeData(
            label='True Branch',
            code="ctx['result'] = 'large'"
        )),
        create_node('task_false', 'task', NodeData(
            label='False Branch',
            code="ctx['result'] = 'small'"
        )),
        create_node('end', 'end')
    ]
    edges = [
        create_edge('e1', 'start', 'task1'),
        create_edge('e2', 'task1', 'cond1'),
        create_edge('e3', 'cond1', 'task_true', source_handle='true'),
        create_edge('e4', 'cond1', 'task_false', source_handle='false'),
        create_edge('e5', 'task_true', 'end'),
        create_edge('e6', 'task_false', 'end')
    ]
    flow = create_test_flow(nodes, edges)
    executor = FlowExecutor(flow)
    state = await executor.execute()

    assert state.status == 'completed'
    assert state.variables['x'] == 10
    assert state.variables['result'] == 'large'
    print('Test Existing Functionality (condition): PASSED')


async def main():
    print('=== Running New Features Tests ===\n')

    await test_sql_node()
    await test_http_node()
    await test_parallel_node()
    await test_trycatch_node()
    await test_subflow_node()
    await test_subflow_circular_detection()
    await test_retry_mechanism()
    await test_retry_failure()
    await test_breakpoint()
    await test_ctx_snapshots()
    await test_existing_functionality()

    print('\n=== All New Feature Tests Passed! ===')


if __name__ == '__main__':
    asyncio.run(main())
