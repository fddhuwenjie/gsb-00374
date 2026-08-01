import ast
import math
import datetime
from typing import Any, Dict, Set


class ASTEvaluationError(Exception):
    pass


ALLOWED_BIN_OPS: Set[type] = {
    ast.Add, ast.Sub, ast.Mult, ast.Div, ast.FloorDiv,
    ast.Mod, ast.Eq, ast.NotEq, ast.Lt, ast.LtE,
    ast.Gt, ast.GtE, ast.And, ast.Or, ast.Pow
}

ALLOWED_UNARY_OPS: Set[type] = {
    ast.Not, ast.USub, ast.UAdd
}


def _safe_name(name: str) -> bool:
    return not name.startswith('_')


def _eval_node(node: ast.AST, ctx: Dict[str, Any]) -> Any:
    if isinstance(node, ast.Expression):
        return _eval_node(node.body, ctx)

    elif isinstance(node, ast.BinOp):
        left = _eval_node(node.left, ctx)
        right = _eval_node(node.right, ctx)
        op_type = type(node.op)

        if op_type not in ALLOWED_BIN_OPS:
            raise ASTEvaluationError(f"Unsupported binary operator: {op_type.__name__}")

        if isinstance(node.op, ast.Add):
            return left + right
        elif isinstance(node.op, ast.Sub):
            return left - right
        elif isinstance(node.op, ast.Mult):
            return left * right
        elif isinstance(node.op, ast.Div):
            return left / right
        elif isinstance(node.op, ast.FloorDiv):
            return left // right
        elif isinstance(node.op, ast.Mod):
            return left % right
        elif isinstance(node.op, ast.Pow):
            return left ** right
        elif isinstance(node.op, ast.Eq):
            return left == right
        elif isinstance(node.op, ast.NotEq):
            return left != right
        elif isinstance(node.op, ast.Lt):
            return left < right
        elif isinstance(node.op, ast.LtE):
            return left <= right
        elif isinstance(node.op, ast.Gt):
            return left > right
        elif isinstance(node.op, ast.GtE):
            return left >= right
        elif isinstance(node.op, ast.And):
            return bool(left) and bool(right)
        elif isinstance(node.op, ast.Or):
            return bool(left) or bool(right)

    elif isinstance(node, ast.UnaryOp):
        operand = _eval_node(node.operand, ctx)
        op_type = type(node.op)

        if op_type not in ALLOWED_UNARY_OPS:
            raise ASTEvaluationError(f"Unsupported unary operator: {op_type.__name__}")

        if isinstance(node.op, ast.Not):
            return not bool(operand)
        elif isinstance(node.op, ast.USub):
            return -operand
        elif isinstance(node.op, ast.UAdd):
            return +operand

    elif isinstance(node, ast.Constant):
        return node.value

    elif isinstance(node, ast.Name):
        name = node.id
        if not _safe_name(name):
            raise ASTEvaluationError(f"Access to private name '{name}' is not allowed")
        if name not in ctx:
            raise ASTEvaluationError(f"Name '{name}' is not defined in context")
        return ctx[name]

    elif isinstance(node, ast.Subscript):
        value = _eval_node(node.value, ctx)
        if isinstance(node.slice, ast.Constant):
            key = node.slice.value
        elif isinstance(node.slice, ast.Name):
            key = node.slice.id
            if not _safe_name(key):
                raise ASTEvaluationError(f"Access to private key '{key}' is not allowed")
        elif isinstance(node.slice, ast.Slice):
            lower = _eval_node(node.slice.lower, ctx) if node.slice.lower else None
            upper = _eval_node(node.slice.upper, ctx) if node.slice.upper else None
            step = _eval_node(node.slice.step, ctx) if node.slice.step else None
            return value[lower:upper:step]
        else:
            raise ASTEvaluationError("Unsupported subscript type")

        if hasattr(value, '__getitem__'):
            return value[key]
        raise ASTEvaluationError(f"Cannot subscript value of type {type(value).__name__}")

    elif isinstance(node, ast.Attribute):
        value = _eval_node(node.value, ctx)
        attr = node.attr

        if not _safe_name(attr):
            raise ASTEvaluationError(f"Access to private attribute '{attr}' is not allowed")

        if isinstance(value, dict):
            if attr in value:
                return value[attr]
            raise ASTEvaluationError(f"Key '{attr}' not found in context dict")

        if hasattr(value, attr):
            return getattr(value, attr)

        raise ASTEvaluationError(f"Attribute '{attr}' not found on {type(value).__name__}")

    elif isinstance(node, ast.Compare):
        left = _eval_node(node.left, ctx)
        result = True
        current = left

        for op, comparator in zip(node.ops, node.comparators):
            right = _eval_node(comparator, ctx)
            op_type = type(op)

            if op_type not in ALLOWED_BIN_OPS:
                raise ASTEvaluationError(f"Unsupported comparison operator: {op_type.__name__}")

            if isinstance(op, ast.Eq):
                comp_result = current == right
            elif isinstance(op, ast.NotEq):
                comp_result = current != right
            elif isinstance(op, ast.Lt):
                comp_result = current < right
            elif isinstance(op, ast.LtE):
                comp_result = current <= right
            elif isinstance(op, ast.Gt):
                comp_result = current > right
            elif isinstance(op, ast.GtE):
                comp_result = current >= right
            else:
                raise ASTEvaluationError(f"Unsupported comparison: {op_type.__name__}")

            result = result and comp_result
            if not result:
                break
            current = right

        return result

    elif isinstance(node, ast.BoolOp):
        values = [_eval_node(v, ctx) for v in node.values]
        if isinstance(node.op, ast.And):
            return all(bool(v) for v in values)
        elif isinstance(node.op, ast.Or):
            return any(bool(v) for v in values)

    elif isinstance(node, ast.IfExp):
        test = _eval_node(node.test, ctx)
        if bool(test):
            return _eval_node(node.body, ctx)
        else:
            return _eval_node(node.orelse, ctx)

    elif isinstance(node, ast.Tuple):
        return tuple(_eval_node(elt, ctx) for elt in node.elts)

    elif isinstance(node, ast.List):
        return [_eval_node(elt, ctx) for elt in node.elts]

    elif isinstance(node, ast.Dict):
        return {
            _eval_node(key, ctx): _eval_node(value, ctx)
            for key, value in zip(node.keys, node.values)
        }

    else:
        raise ASTEvaluationError(
            f"Unsupported AST node type: {type(node).__name__}. "
            f"Only basic expressions, comparisons, and context access are allowed."
        )


def evaluate_expression(expression: str, ctx: Dict[str, Any]) -> Any:
    if not expression or not expression.strip():
        raise ASTEvaluationError("Empty expression")

    try:
        tree = ast.parse(expression, mode='eval')
    except SyntaxError as e:
        raise ASTEvaluationError(f"Syntax error in expression: {e}")

    for node in ast.walk(tree):
        if isinstance(node, (ast.Call, ast.Import, ast.ImportFrom, ast.Lambda,
                           ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef,
                           ast.Yield, ast.YieldFrom, ast.Await,
                           ast.GeneratorExp, ast.ListComp, ast.SetComp, ast.DictComp,
                           ast.Starred, ast.NamedExpr)):
            raise ASTEvaluationError(
                f"Unsupported construct: {type(node).__name__}. "
                f"Function calls, imports, comprehensions, and definitions are not allowed."
            )

    return _eval_node(tree, ctx)
