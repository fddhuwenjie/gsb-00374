import sys
import os
import asyncio
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from models.flow import (
    FlowDefinition, FlowNode, FlowEdge, Position, NodeData
)
from engine.executor import FlowExecutor, ExecutionError


def create_test_flow_1():
    """Start → Task(x=1) → Condition(x>0) → Task(x=x+1) → End"""
    now = 1234567890.0
    return FlowDefinition(
        id="test-flow-1",
        name="Condition Test",
        createdAt=now,
        updatedAt=now,
        nodes=[
            FlowNode(
                id="start",
                type="start",
                position=Position(x=100, y=100),
                data=NodeData(label="Start")
            ),
            FlowNode(
                id="task1",
                type="task",
                position=Position(x=100, y=200),
                data=NodeData(
                    label="Set x=1",
                    code='ctx["x"] = 1'
                )
            ),
            FlowNode(
                id="condition",
                type="condition",
                position=Position(x=100, y=300),
                data=NodeData(
                    label="x > 0",
                    expression="x > 0"
                )
            ),
            FlowNode(
                id="task2",
                type="task",
                position=Position(x=0, y=400),
                data=NodeData(
                    label="x = x + 1",
                    code='ctx["x"] = ctx["x"] + 1'
                )
            ),
            FlowNode(
                id="task3",
                type="task",
                position=Position(x=200, y=400),
                data=NodeData(
                    label="x = x - 1",
                    code='ctx["x"] = ctx["x"] - 1'
                )
            ),
            FlowNode(
                id="end",
                type="end",
                position=Position(x=100, y=500),
                data=NodeData(label="End")
            )
        ],
        edges=[
            FlowEdge(id="e1", source="start", target="task1"),
            FlowEdge(id="e2", source="task1", target="condition"),
            FlowEdge(id="e3", source="condition", target="task2", sourceHandle="true"),
            FlowEdge(id="e4", source="condition", target="task3", sourceHandle="false"),
            FlowEdge(id="e5", source="task2", target="end"),
            FlowEdge(id="e6", source="task3", target="end")
        ]
    )


def create_test_flow_2():
    """Start → Task(i=0) → Loop(i<5) → Task(i=i+1) → End"""
    now = 1234567890.0
    return FlowDefinition(
        id="test-flow-2",
        name="Loop Test",
        createdAt=now,
        updatedAt=now,
        nodes=[
            FlowNode(
                id="start",
                type="start",
                position=Position(x=100, y=100),
                data=NodeData(label="Start")
            ),
            FlowNode(
                id="task_init",
                type="task",
                position=Position(x=100, y=200),
                data=NodeData(
                    label="i = 0",
                    code='ctx["i"] = 0'
                )
            ),
            FlowNode(
                id="loop",
                type="loop",
                position=Position(x=100, y=300),
                data=NodeData(
                    label="i < 5",
                    expression="i < 5",
                    anchorId="loop_anchor"
                )
            ),
            FlowNode(
                id="loop_anchor",
                type="task",
                position=Position(x=100, y=350),
                data=NodeData(
                    label="Anchor",
                    code='pass'
                )
            ),
            FlowNode(
                id="task_inc",
                type="task",
                position=Position(x=100, y=400),
                data=NodeData(
                    label="i = i + 1",
                    code='ctx["i"] = ctx["i"] + 1'
                )
            ),
            FlowNode(
                id="end",
                type="end",
                position=Position(x=100, y=500),
                data=NodeData(label="End")
            )
        ],
        edges=[
            FlowEdge(id="e1", source="start", target="task_init"),
            FlowEdge(id="e2", source="task_init", target="loop"),
            FlowEdge(id="e3", source="loop", target="loop_anchor", sourceHandle="loop"),
            FlowEdge(id="e4", source="loop", target="end", sourceHandle=None),
            FlowEdge(id="e5", source="loop_anchor", target="task_inc"),
            FlowEdge(id="e6", source="task_inc", target="loop")
        ]
    )


async def test_flow_1():
    print("=== Test 1: Condition Branch ===")
    flow = create_test_flow_1()
    
    node_enter_order = []
    async def on_node_enter(node_id, variables):
        node_enter_order.append(node_id)
        print(f"  Enter: {node_id}, x={variables.get('x')}")
    
    async def on_node_exit(node_id, variables):
        print(f"  Exit:  {node_id}, x={variables.get('x')}")
    
    executor = FlowExecutor(flow)
    executor.set_callbacks(
        on_node_enter=on_node_enter,
        on_node_exit=on_node_exit
    )
    
    state = await executor.execute()
    print(f"  Final x = {state.variables.get('x')}")
    print(f"  Status = {state.status}")
    
    assert state.status == 'completed', f"Expected completed, got {state.status}"
    assert state.variables.get('x') == 2, f"Expected x=2, got {state.variables.get('x')}"
    assert 'start' in node_enter_order
    assert 'task1' in node_enter_order
    assert 'condition' in node_enter_order
    assert 'task2' in node_enter_order
    assert 'task3' not in node_enter_order
    assert 'end' in node_enter_order
    
    print("  PASSED!\n")


async def test_flow_2():
    print("=== Test 2: Loop Execution ===")
    flow = create_test_flow_2()
    
    loop_count = 0
    async def on_node_enter(node_id, variables):
        nonlocal loop_count
        if node_id == 'loop':
            loop_count += 1
        print(f"  Enter: {node_id}, i={variables.get('i')}")
    
    executor = FlowExecutor(flow)
    executor.set_callbacks(on_node_enter=on_node_enter)
    
    state = await executor.execute()
    print(f"  Final i = {state.variables.get('i')}")
    print(f"  Loop count = {loop_count}")
    print(f"  Status = {state.status}")
    
    assert state.status == 'completed', f"Expected completed, got {state.status}"
    assert state.variables.get('i') == 5, f"Expected i=5, got {state.variables.get('i')}"
    assert loop_count == 6, f"Expected loop entered 6 times (5 true + 1 false), got {loop_count}"
    
    print("  PASSED!\n")


async def test_sandbox_security():
    print("=== Test 3: Sandbox Security ===")
    
    flow = FlowDefinition(
        id="test-sandbox",
        name="Sandbox Test",
        createdAt=1234567890.0,
        updatedAt=1234567890.0,
        nodes=[
            FlowNode(id="start", type="start", position=Position(x=100, y=100), data=NodeData(label="Start")),
            FlowNode(
                id="task_bad",
                type="task",
                position=Position(x=100, y=200),
                data=NodeData(
                    label="Bad Code",
                    code='__import__("os")'
                )
            ),
            FlowNode(id="end", type="end", position=Position(x=100, y=300), data=NodeData(label="End"))
        ],
        edges=[
            FlowEdge(id="e1", source="start", target="task_bad"),
            FlowEdge(id="e2", source="task_bad", target="end")
        ]
    )
    
    executor = FlowExecutor(flow)
    try:
        state = await executor.execute()
        print(f"  Status = {state.status}")
        assert state.status == 'error', f"Expected error status, got {state.status}"
        print("  PASSED: __import__ was blocked\n")
    except ExecutionError as e:
        print(f"  Caught expected error: {e}")
        print("  PASSED!\n")


async def test_pause_resume():
    print("=== Test 4: Pause/Resume ===")
    flow = create_test_flow_1()
    
    paused = False
    
    async def pause_after_task1(node_id, variables):
        nonlocal paused
        print(f"  Enter: {node_id}, x={variables.get('x')}")
        if node_id == 'condition' and not paused:
            paused = True
            print("  -> Pausing at condition node")
            await executor.pause()
    
    executor = FlowExecutor(flow)
    executor.set_callbacks(on_node_enter=pause_after_task1)
    
    exec_task = asyncio.create_task(executor.execute())
    
    await asyncio.sleep(0.5)
    assert paused, "Should have paused"
    assert executor.state.status == 'paused', f"Expected paused, got {executor.state.status}"
    print(f"  Paused at node: {executor.state.currentNodeId}, x={executor.state.variables.get('x')}")
    
    executor.set_variable('x', 10)
    print("  -> Modified x to 10 while paused")
    
    print("  -> Resuming")
    await executor.resume()
    
    state = await exec_task
    print(f"  Final x = {state.variables.get('x')}")
    print(f"  Status = {state.status}")
    
    assert state.status == 'completed'
    assert state.variables.get('x') == 11, f"Expected x=11 (10+1), got {state.variables.get('x')}"
    
    print("  PASSED!\n")


async def main():
    await test_flow_1()
    await test_flow_2()
    await test_sandbox_security()
    await test_pause_resume()
    print("=== All Acceptance Tests Passed! ===")


if __name__ == "__main__":
    asyncio.run(main())
