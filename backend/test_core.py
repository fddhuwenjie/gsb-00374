import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from models.flow import *
from engine.ast_eval import evaluate_expression, ASTEvaluationError
from engine.sandbox import execute_python_sandbox, SandboxError
from engine.validator import FlowValidator, ValidationError
from engine.executor import FlowExecutor, ExecutionError

print('=== Testing AST Evaluator ===')
ctx = {'ctx': {'x': 5, 'y': 10, 'name': 'test'}}

result = evaluate_expression('ctx.x > 0', ctx)
print(f'Test 1: ctx.x > 0 = {result}')
assert result == True

result = evaluate_expression('x > 0', ctx['ctx'])
print(f'Test 1b: x > 0 = {result}')
assert result == True

result = evaluate_expression('ctx.x + ctx.y', ctx)
print(f'Test 2: ctx.x + ctx.y = {result}')
assert result == 15

result = evaluate_expression('x + y', ctx['ctx'])
print(f'Test 2b: x + y = {result}')
assert result == 15

result = evaluate_expression('ctx.x * 2 + 3', ctx)
print(f'Test 3: ctx.x * 2 + 3 = {result}')
assert result == 13

result = evaluate_expression('x * 2 + 3', ctx['ctx'])
print(f'Test 3b: x * 2 + 3 = {result}')
assert result == 13

result = evaluate_expression('ctx.x == 5 and ctx.y > 5', ctx)
print(f'Test 4: ctx.x == 5 and ctx.y > 5 = {result}')
assert result == True

result = evaluate_expression('x == 5 and y > 5', ctx['ctx'])
print(f'Test 4b: x == 5 and y > 5 = {result}')
assert result == True

result = evaluate_expression('ctx["x"]', ctx)
print(f'Test 5: ctx["x"] = {result}')
assert result == 5

result = evaluate_expression('x', ctx['ctx'])
print(f'Test 5b: x = {result}')
assert result == 5

try:
    evaluate_expression('__import__("os")', ctx)
    print('Test 6: FAILED - should have raised error')
except ASTEvaluationError as e:
    print(f'Test 6: __import__ blocked - OK')

try:
    evaluate_expression('exit()', ctx)
    print('Test 7: FAILED - should have raised error')
except ASTEvaluationError as e:
    print(f'Test 7: function call blocked - OK')

print('\\n=== Testing Sandbox ===')
ctx = {'x': 1}
result = execute_python_sandbox('ctx["x"] = ctx["x"] + 1', ctx)
print(f'Test 1: x = {result["x"]}')
assert result['x'] == 2

ctx = {'value': 10}
code = '''
ctx['value'] = ctx['value'] * 2
ctx['doubled'] = ctx['value']
'''
result = execute_python_sandbox(code, ctx)
print(f'Test 2: value={result["value"]}, doubled={result["doubled"]}')
assert result['value'] == 20
assert result['doubled'] == 20

try:
    execute_python_sandbox('__import__("os")', {})
    print('Test 3: FAILED - should have raised error')
except SandboxError as e:
    print(f'Test 3: __import__ blocked - OK: {e}')

try:
    execute_python_sandbox('import os', {})
    print('Test 4: FAILED - should have raised error')
except SandboxError as e:
    print(f'Test 4: import blocked - OK: {e}')

try:
    execute_python_sandbox('open("/etc/passwd")', {})
    print('Test 5: FAILED - should have raised error')
except SandboxError as e:
    print(f'Test 5: open blocked - OK: {e}')

ctx = {'x': 0}
code = '''
ctx['sin'] = math.sin(ctx['x'])
'''
result = execute_python_sandbox(code, ctx)
print(f'Test 6: math.sin(0) = {result["sin"]}')
assert abs(result['sin'] - 0.0) < 0.001

print('\\n=== All Tests Passed! ===')
