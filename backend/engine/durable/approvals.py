"""Human approval requests and expirable recovery tokens.

An approval request is raised when the runner reaches an ``approval`` node (or an
approval step inside a parallel branch). It is bound immutably to the tuple that
identifies exactly *which* piece of work is waiting:

    executionId + nodeId + branchId + generation + flowVersion

so a decision can only ever release the matching work. In a parallel node each
branch has its own ``branchId`` and the current ``generation`` (the parallel
node's attempt), so approving one branch never releases a sibling branch or a
stale generation.

Recovery tokens
---------------
A *recovery token* is the opaque handle a human (or the UI) presents when they
approve/reject. It embeds the full binding plus a monotonically increasing
``deadline`` (epoch seconds). Tokens are signed with an HMAC over the durable
root so a token cannot be forged or point at a different execution. A token is
*stale* if its embedded ``(nodeId, generation, attempt)`` no longer matches the
live pending request (e.g. the node was retried into a new generation), and
*expired* if ``now > deadline``. Both are rejected as illegal by the state
machine layer rather than silently applied.

The token is deliberately self-describing so that, after a service restart, a
token issued before the crash still resolves against the recovered pending
approval as long as the binding still matches and it has not expired.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from typing import Any, Dict, Optional


# Decision outcomes. ``approved`` resumes the flow; ``rejected``/``timeout`` fail
# the approval (and therefore the execution / branch).
DECISION_APPROVED = "approved"
DECISION_REJECTED = "rejected"
DECISION_TIMEOUT = "timeout"

VALID_DECISIONS = {DECISION_APPROVED, DECISION_REJECTED, DECISION_TIMEOUT}


def approval_id(
    execution_id: str,
    node_id: str,
    branch_id: Optional[str],
    generation: int,
    attempt: int,
    flow_version: Optional[int],
) -> str:
    """Deterministic id binding a request to a single unit of waiting work.

    Two requests differing in any of executionId/nodeId/branchId/generation/
    attempt/flowVersion get distinct ids, so a decision for one can never be
    mistakenly applied to another (e.g. a different parallel branch or a retried
    generation).
    """
    raw = "|".join(
        [
            execution_id,
            node_id,
            branch_id or "-",
            str(generation),
            str(attempt),
            str(flow_version if flow_version is not None else "-"),
        ]
    )
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
    return f"appr_{digest}"


def _secret(root_dir: str) -> bytes:
    # A stable per-store secret. Derived from the root path so tokens are
    # verifiable across restarts of the same deployment without a key file.
    return hashlib.sha256(("durable-approval::" + root_dir).encode("utf-8")).digest()


def issue_token(
    root_dir: str,
    binding: Dict[str, Any],
    deadline: float,
) -> str:
    """Encode + sign a recovery token for a pending approval.

    ``binding`` must contain executionId, nodeId, branchId, generation, attempt,
    flowVersion and approvalId. ``deadline`` is epoch seconds after which the
    token (and the pending request) is considered expired.
    """
    payload = dict(binding)
    payload["deadline"] = deadline
    body = base64.urlsafe_b64encode(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).decode("ascii")
    sig = hmac.new(_secret(root_dir), body.encode("ascii"), hashlib.sha256).hexdigest()[:32]
    return f"{body}.{sig}"


class TokenError(Exception):
    pass


def decode_token(root_dir: str, token: str) -> Dict[str, Any]:
    """Verify a token's signature and return its payload.

    Raises :class:`TokenError` for a malformed or forged token. Expiry and
    staleness are decided by the caller against the *live* pending state, not
    here, so those remain state-machine decisions.
    """
    try:
        body, sig = token.rsplit(".", 1)
    except ValueError as e:
        raise TokenError("malformed token") from e
    expected = hmac.new(_secret(root_dir), body.encode("ascii"), hashlib.sha256).hexdigest()[:32]
    if not hmac.compare_digest(sig, expected):
        raise TokenError("bad signature")
    try:
        payload = json.loads(base64.urlsafe_b64decode(body.encode("ascii")).decode("utf-8"))
    except Exception as e:
        raise TokenError("undecodable token") from e
    return payload


def is_expired(deadline: Optional[float], now: Optional[float] = None) -> bool:
    if deadline is None:
        return False
    return (now if now is not None else time.time()) > deadline
