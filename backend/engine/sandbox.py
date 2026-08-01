import math
import datetime
import threading
import queue
from typing import Any, Dict
from RestrictedPython import compile_restricted, safe_globals
from RestrictedPython.Eval import default_guarded_getitem
from RestrictedPython.Guards import (
    full_write_guard,
    guarded_iter_unpack_sequence,
    guarded_unpack_sequence,
)


class SandboxError(Exception):
    pass


class TimeoutError(SandboxError):
    pass


class InfiniteLoopError(SandboxError):
    pass


def _raise_import_error():
    raise SandboxError("Import statements are not allowed in sandbox")


def _build_safe_globals(ctx: Dict[str, Any]) -> Dict[str, Any]:
    safe_globals_dict = safe_globals.copy()

    safe_globals_dict.update({
        '_getattr_': getattr,
        '_getitem_': default_guarded_getitem,
        '_write_': full_write_guard,
        '_iter_unpack_sequence_': guarded_iter_unpack_sequence,
        '_unpack_sequence_': guarded_unpack_sequence,
        '_print_': lambda *args, **kwargs: None,
        'math': math,
        'datetime': datetime,
        'ctx': ctx,
        '__builtins__': {
            'True': True,
            'False': False,
            'None': None,
            'abs': abs,
            'bool': bool,
            'int': int,
            'float': float,
            'str': str,
            'list': list,
            'tuple': tuple,
            'dict': dict,
            'len': len,
            'min': min,
            'max': max,
            'sum': sum,
            'round': round,
            'range': range,
            'sorted': sorted,
            'reversed': reversed,
            'enumerate': enumerate,
            'zip': zip,
            'map': map,
            'filter': filter,
            'all': all,
            'any': any,
            'isinstance': isinstance,
            'type': type,
        },
        '__import__': lambda *args, **kwargs: _raise_import_error(),
    })

    return safe_globals_dict


def _check_for_imports(code: str) -> None:
    forbidden_patterns = [
        '__import__',
        'import ',
        'from ',
        'exec(',
        'eval(',
        'compile(',
        'open(',
        'file(',
        'globals()',
        'locals()',
        'vars()',
        'dir()',
        'help()',
        'os.',
        'sys.',
        'subprocess',
        'builtins',
        '.__class__',
        '.__bases__',
        '.__subclasses__',
        '.__mro__',
    ]
    for pattern in forbidden_patterns:
        if pattern in code:
            raise SandboxError(f"Forbidden pattern detected: '{pattern}'")


def _execute_in_thread(code: str, ctx: Dict[str, Any], result_queue: queue.Queue) -> None:
    try:
        byte_code = compile_restricted(code, '<sandbox>', 'exec')
        safe_globals_dict = _build_safe_globals(ctx)
        local_vars = {'ctx': ctx}
        exec(byte_code, safe_globals_dict, local_vars)
        updated_ctx = local_vars.get('ctx', ctx)
        ctx.update(updated_ctx)
        result_queue.put(('success', ctx))
    except SandboxError as e:
        result_queue.put(('error', e))
    except Exception as e:
        result_queue.put(('error', SandboxError(f"Sandbox execution error: {type(e).__name__}: {e}")))


def execute_python_sandbox(code: str, ctx: Dict[str, Any], timeout: int = 5) -> Dict[str, Any]:
    if not code or not code.strip():
        return ctx

    _check_for_imports(code)

    result_queue: queue.Queue = queue.Queue()
    thread = threading.Thread(
        target=_execute_in_thread,
        args=(code, ctx, result_queue)
    )
    thread.daemon = True
    thread.start()
    thread.join(timeout)

    if thread.is_alive():
        raise TimeoutError(f"Code execution timed out after {timeout} seconds")

    try:
        status, result = result_queue.get_nowait()
    except queue.Empty:
        raise SandboxError("No result from sandbox execution")

    if status == 'error':
        raise result

    return result
