import json
import os
import sqlite3
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from engine import state_machine as sm


SCHEMA = """
CREATE TABLE IF NOT EXISTS executions (
    execution_id TEXT PRIMARY KEY,
    flow_id TEXT NOT NULL,
    status TEXT NOT NULL,
    current_node_id TEXT,
    variables TEXT NOT NULL DEFAULT '{}',
    loop_counts TEXT NOT NULL DEFAULT '{}',
    resume_from_node_id TEXT,
    generation INTEGER NOT NULL DEFAULT 0,
    executor_generation INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    finished_at REAL,
    error TEXT
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    from_state TEXT,
    to_state TEXT,
    node_id TEXT,
    attempt INTEGER,
    generation INTEGER,
    payload TEXT NOT NULL DEFAULT '{}',
    created_at REAL NOT NULL,
    UNIQUE(execution_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_events_exec_seq ON events(execution_id, seq);

CREATE TABLE IF NOT EXISTS node_attempts (
    execution_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    status TEXT NOT NULL,
    generation INTEGER NOT NULL DEFAULT 0,
    started_at REAL NOT NULL,
    finished_at REAL,
    result TEXT,
    error TEXT,
    PRIMARY KEY (execution_id, node_id, attempt)
);

CREATE INDEX IF NOT EXISTS idx_node_attempts_latest
ON node_attempts(execution_id, node_id, generation, attempt);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    idempotency_key TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    generation INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    result TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS side_effect_calls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    generation INTEGER NOT NULL DEFAULT 0,
    side_effect_name TEXT NOT NULL,
    created_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_se_calls ON side_effect_calls(execution_id, node_id, attempt);

CREATE TABLE IF NOT EXISTS commands (
    command_id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL,
    command TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS branch_state (
    execution_id TEXT NOT NULL,
    parallel_node_id TEXT NOT NULL,
    branch_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    status TEXT NOT NULL,
    result TEXT,
    completed_node_id TEXT,
    PRIMARY KEY (execution_id, parallel_node_id, branch_id, generation)
);

CREATE TABLE IF NOT EXISTS seq_tracker (
    execution_id TEXT PRIMARY KEY,
    last_seq INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS flow_versions (
    flow_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    name TEXT NOT NULL,
    definition TEXT NOT NULL,
    node_config_hash TEXT NOT NULL,
    created_at REAL NOT NULL,
    PRIMARY KEY (flow_id, version)
);

CREATE INDEX IF NOT EXISTS idx_flow_versions_flow ON flow_versions(flow_id, version);

CREATE TABLE IF NOT EXISTS approvals (
    approval_id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    generation INTEGER NOT NULL DEFAULT 0,
    flow_version INTEGER,
    branch_id TEXT,
    token TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    prompt TEXT,
    deadline REAL NOT NULL,
    responded_by TEXT,
    response_comment TEXT,
    created_at REAL NOT NULL,
    responded_at REAL
);

CREATE INDEX IF NOT EXISTS idx_approvals_exec ON approvals(execution_id, node_id, status);
CREATE INDEX IF NOT EXISTS idx_approvals_token ON approvals(token);
CREATE INDEX IF NOT EXISTS idx_approvals_deadline ON approvals(status, deadline);
"""


class EventStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self._conn = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._conn.execute("PRAGMA busy_timeout=5000;")
        self._conn.execute("PRAGMA foreign_keys=ON;")
        self._conn.executescript(SCHEMA)
        self._ensure_schema_columns()

    def _ensure_schema_columns(self) -> None:
        existing = {
            row["name"]
            for row in self._conn.execute("PRAGMA table_info(executions)").fetchall()
        }
        if "executor_generation" not in existing:
            self._conn.execute(
                "ALTER TABLE executions ADD COLUMN executor_generation INTEGER NOT NULL DEFAULT 0"
            )
        existing_events = {
            row["name"]
            for row in self._conn.execute("PRAGMA table_info(events)").fetchall()
        }
        if "generation" not in existing_events:
            self._conn.execute("ALTER TABLE events ADD COLUMN generation INTEGER")
        existing_effects = {
            row["name"]
            for row in self._conn.execute("PRAGMA table_info(side_effect_calls)").fetchall()
        }
        if "generation" not in existing_effects:
            self._conn.execute(
                "ALTER TABLE side_effect_calls ADD COLUMN generation INTEGER NOT NULL DEFAULT 0"
            )
        existing_idem = {
            row["name"]
            for row in self._conn.execute("PRAGMA table_info(idempotency_keys)").fetchall()
        }
        if "generation" not in existing_idem:
            self._conn.execute(
                "ALTER TABLE idempotency_keys ADD COLUMN generation INTEGER NOT NULL DEFAULT 0"
            )
        if "updated_at" not in existing_idem:
            self._conn.execute("ALTER TABLE idempotency_keys ADD COLUMN updated_at REAL")
            self._conn.execute("UPDATE idempotency_keys SET updated_at = created_at WHERE updated_at IS NULL")
        existing_branches = {
            row["name"]
            for row in self._conn.execute("PRAGMA table_info(branch_state)").fetchall()
        }
        if "completed_node_id" not in existing_branches:
            self._conn.execute("ALTER TABLE branch_state ADD COLUMN completed_node_id TEXT")
        existing_exec_cols = {
            row["name"]
            for row in self._conn.execute("PRAGMA table_info(executions)").fetchall()
        }
        if "flow_version" not in existing_exec_cols:
            self._conn.execute("ALTER TABLE executions ADD COLUMN flow_version INTEGER")
        if "node_config_hash" not in existing_exec_cols:
            self._conn.execute("ALTER TABLE executions ADD COLUMN node_config_hash TEXT")

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass

    def _now(self) -> float:
        return time.time()

    def create_execution(self, flow_id: str, variables: Optional[Dict[str, Any]] = None,
                         execution_id: Optional[str] = None,
                         flow_version: Optional[int] = None,
                         node_config_hash: Optional[str] = None) -> str:
        eid = execution_id or f"exec_{uuid.uuid4().hex}"
        now = self._now()
        self._conn.execute(
            "INSERT INTO executions (execution_id, flow_id, status, variables, created_at, updated_at, "
            "flow_version, node_config_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (eid, flow_id, sm.QUEUED, json.dumps(variables or {}), now, now,
             flow_version, node_config_hash),
        )
        self._conn.execute(
            "INSERT OR IGNORE INTO seq_tracker (execution_id, last_seq) VALUES (?, 0)",
            (eid,),
        )
        self.append_event(eid, "execution_created", None, sm.QUEUED, payload={
            "flowId": flow_id,
            "flowVersion": flow_version,
            "nodeConfigHash": node_config_hash,
        })
        return eid

    def get_execution_row(self, execution_id: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM executions WHERE execution_id = ?", (execution_id,)
        ).fetchone()
        if not row:
            return None
        d = dict(row)
        d["variables"] = json.loads(d.get("variables") or "{}")
        d["loop_counts"] = json.loads(d.get("loop_counts") or "{}")
        return d

    def list_executions(self, flow_id: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        if flow_id:
            rows = self._conn.execute(
                "SELECT * FROM executions WHERE flow_id = ? ORDER BY created_at DESC LIMIT ?",
                (flow_id, limit),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM executions ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
        result = []
        for row in rows:
            d = dict(row)
            d["variables"] = json.loads(d.get("variables") or "{}")
            d["loop_counts"] = json.loads(d.get("loop_counts") or "{}")
            result.append(d)
        return result

    def list_recoverable(self) -> List[Dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT * FROM executions WHERE status IN (?, ?, ?, ?, ?)",
            (sm.QUEUED, sm.RUNNING, sm.PAUSING, sm.RETRY_WAIT, sm.AWAITING_APPROVAL),
        ).fetchall()
        result = []
        for row in rows:
            d = dict(row)
            d["variables"] = json.loads(d.get("variables") or "{}")
            d["loop_counts"] = json.loads(d.get("loop_counts") or "{}")
            result.append(d)
        return result

    def _alloc_seq_locked(self, execution_id: str) -> int:
        cur = self._conn.execute(
            "INSERT INTO seq_tracker (execution_id, last_seq) VALUES (?, 1) "
            "ON CONFLICT(execution_id) DO UPDATE SET last_seq = last_seq + 1 "
            "RETURNING last_seq",
            (execution_id,),
        )
        row = cur.fetchone()
        return int(row["last_seq"])

    def _alloc_seq(self, execution_id: str) -> int:
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            seq = self._alloc_seq_locked(execution_id)
            self._conn.execute("COMMIT")
            return seq
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def append_event(self, execution_id: str, event_type: str,
                     from_state: Optional[str], to_state: Optional[str],
                     node_id: Optional[str] = None, attempt: Optional[int] = None,
                     generation: Optional[int] = None,
                     payload: Optional[Dict[str, Any]] = None) -> int:
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            next_seq = self._alloc_seq_locked(execution_id)
            self._conn.execute(
                "INSERT INTO events "
                "(execution_id, seq, event_type, from_state, to_state, node_id, attempt, generation, payload, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    execution_id, next_seq, event_type, from_state, to_state,
                    node_id, attempt, generation, json.dumps(payload or {}), self._now(),
                ),
            )
            self._conn.execute("COMMIT")
            return next_seq
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def get_events_since(self, execution_id: str, since_seq: int = 0) -> List[Dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT * FROM events WHERE execution_id = ? AND seq > ? ORDER BY seq ASC",
            (execution_id, since_seq),
        ).fetchall()
        return [self._event_row_to_dict(r) for r in rows]

    def get_all_events(self, execution_id: str) -> List[Dict[str, Any]]:
        return self.get_events_since(execution_id, 0)

    def get_latest_seq(self, execution_id: str) -> int:
        row = self._conn.execute(
            "SELECT COALESCE(MAX(seq), 0) AS s FROM events WHERE execution_id = ?",
            (execution_id,),
        ).fetchone()
        return int(row["s"])

    def _event_row_to_dict(self, row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "seq": row["seq"],
            "type": row["event_type"],
            "fromState": row["from_state"],
            "toState": row["to_state"],
            "nodeId": row["node_id"],
            "attempt": row["attempt"],
            "generation": row["generation"],
            "payload": json.loads(row["payload"] or "{}"),
            "timestamp": row["created_at"],
        }

    def transition(self, execution_id: str, to_state: str,
                   payload: Optional[Dict[str, Any]] = None,
                   set_resume_from: Optional[str] = None,
                   increment_generation: bool = False,
                   increment_executor_generation: bool = False,
                   error: Optional[str] = None,
                   expected_states: Optional[List[str]] = None) -> Tuple[str, int, Dict[str, Any]]:
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            row = self.get_execution_row(execution_id)
            if not row:
                raise KeyError(f"Execution {execution_id} not found")
            from_state = row["status"]
            if expected_states is not None and from_state not in expected_states:
                raise sm.IllegalTransitionError(from_state, to_state)
            sm.assert_transition(from_state, to_state)

            next_gen = row["generation"] + (1 if increment_generation else 0)
            next_executor_gen = row["executor_generation"] + (1 if increment_executor_generation else 0)
            resume_from = set_resume_from if set_resume_from is not None else row["resume_from_node_id"]
            finished_at = self._now() if sm.is_terminal(to_state) else None
            now = self._now()
            next_payload = dict(payload or {})
            next_payload.update({
                "generation": next_gen,
                "executorGeneration": next_executor_gen,
                "allowedActions": sorted(sm.allowed_actions(to_state)),
            })

            cur = self._conn.execute(
                "UPDATE executions SET status = ?, updated_at = ?, resume_from_node_id = ?, "
                "generation = ?, executor_generation = ?, finished_at = COALESCE(?, finished_at), "
                "error = COALESCE(?, error) WHERE execution_id = ?",
                (to_state, now, resume_from, next_gen, next_executor_gen,
                 finished_at, error, execution_id),
            )
            if cur.rowcount != 1:
                raise RuntimeError(f"Failed to update execution {execution_id}")
            seq = self._append_event_locked(
                execution_id, "state_transition", from_state, to_state,
                generation=next_gen, payload=next_payload,
            )
            snapshot = self._snapshot_locked(execution_id)
            self._conn.execute("COMMIT")
            return to_state, seq, snapshot
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def claim_execution_start(self, execution_id: str) -> Optional[Dict[str, Any]]:
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            row = self.get_execution_row(execution_id)
            if not row:
                self._conn.execute("ROLLBACK")
                return None
            if row["status"] not in (sm.QUEUED, sm.RUNNING, sm.RETRY_WAIT, sm.AWAITING_APPROVAL):
                self._conn.execute("ROLLBACK")
                return None
            now = self._now()
            next_executor_generation = int(row["executor_generation"]) + 1
            if row["status"] == sm.AWAITING_APPROVAL:
                self._conn.execute(
                    "UPDATE executions SET updated_at = ?, "
                    "executor_generation = executor_generation + 1 WHERE execution_id = ?",
                    (now, execution_id),
                )
            else:
                self._conn.execute(
                    "UPDATE executions SET status = ?, updated_at = ?, "
                    "executor_generation = executor_generation + 1 WHERE execution_id = ?",
                    (sm.RUNNING, now, execution_id),
                )
                self._append_event_locked(
                    execution_id, "state_transition", row["status"], sm.RUNNING,
                    payload={
                        "generation": row["generation"],
                        "executorGeneration": next_executor_generation,
                    },
                )
            snapshot = self._snapshot_locked(execution_id)
            self._conn.execute("COMMIT")
            return snapshot
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def claim_executor(self, execution_id: str, executor_generation: int) -> bool:
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            row = self.get_execution_row(execution_id)
            if not row:
                self._conn.execute("ROLLBACK")
                return False
            current = int(row["executor_generation"])
            status = row["status"]
            if current != executor_generation or status in sm.TERMINAL_STATES or status == sm.PAUSED:
                self._conn.execute("ROLLBACK")
                return False
            self._conn.execute("COMMIT")
            return True
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def is_executor_current(self, execution_id: str, executor_generation: int) -> bool:
        row = self.get_execution_row(execution_id)
        return bool(row and int(row["executor_generation"]) == executor_generation)

    def _append_event_locked(self, execution_id: str, event_type: str,
                             from_state: Optional[str], to_state: Optional[str],
                             node_id: Optional[str] = None, attempt: Optional[int] = None,
                             generation: Optional[int] = None,
                             payload: Optional[Dict[str, Any]] = None) -> int:
        next_seq = self._alloc_seq_locked(execution_id)
        self._conn.execute(
            "INSERT INTO events "
            "(execution_id, seq, event_type, from_state, to_state, node_id, attempt, generation, payload, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                execution_id, next_seq, event_type, from_state, to_state,
                node_id, attempt, generation, json.dumps(payload or {}), self._now(),
            ),
        )
        return next_seq

    def update_runtime_state(self, execution_id: str, *,
                             current_node_id: Optional[str] = None,
                             variables: Optional[Dict[str, Any]] = None,
                             loop_counts: Optional[Dict[str, int]] = None,
                             resume_from_node_id: Optional[str] = None,
                             event_type: str = "state_updated",
                             node_id: Optional[str] = None,
                             attempt: Optional[int] = None,
                             generation: Optional[int] = None,
                             payload: Optional[Dict[str, Any]] = None) -> Tuple[int, Dict[str, Any]]:
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            row = self.get_execution_row(execution_id)
            if not row:
                raise KeyError(f"Execution {execution_id} not found")
            new_vars = variables if variables is not None else row["variables"]
            new_loops = loop_counts if loop_counts is not None else row["loop_counts"]
            new_current = current_node_id if current_node_id is not None else row["current_node_id"]
            new_resume = resume_from_node_id if resume_from_node_id is not None else row["resume_from_node_id"]
            event_generation = generation if generation is not None else row["generation"]
            self._conn.execute(
                "UPDATE executions SET current_node_id = ?, variables = ?, loop_counts = ?, "
                "resume_from_node_id = ?, updated_at = ? WHERE execution_id = ?",
                (new_current, json.dumps(new_vars), json.dumps(new_loops),
                 new_resume, self._now(), execution_id),
            )
            event_payload = dict(payload or {})
            event_payload.setdefault("generation", event_generation)
            seq = self._append_event_locked(
                execution_id, event_type, row["status"], row["status"],
                node_id=node_id, attempt=attempt,
                generation=event_generation, payload=event_payload,
            )
            snapshot = self._snapshot_locked(execution_id)
            self._conn.execute("COMMIT")
            return seq, snapshot
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def checkpoint_node(self, execution_id: str, node_id: str, variables: Dict[str, Any],
                        loop_counts: Dict[str, int], attempt: int,
                        generation: int) -> Tuple[int, Dict[str, Any]]:
        return self.update_runtime_state(
            execution_id,
            current_node_id=node_id,
            variables=variables,
            loop_counts=loop_counts,
            resume_from_node_id=node_id,
            event_type="node_checkpoint",
            node_id=node_id,
            attempt=attempt,
            generation=generation,
            payload={"generation": generation},
        )

    def begin_node_attempt(self, execution_id: str, node_id: str, generation: int) -> int:
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            row = self._conn.execute(
                "SELECT COALESCE(MAX(attempt), 0) + 1 AS next_attempt "
                "FROM node_attempts WHERE execution_id = ? AND node_id = ? AND generation = ?",
                (execution_id, node_id, generation),
            ).fetchone()
            attempt = int(row["next_attempt"])
            self._conn.execute(
                "INSERT OR REPLACE INTO node_attempts "
                "(execution_id, node_id, attempt, status, generation, started_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (execution_id, node_id, attempt, "running", generation, self._now()),
            )
            self._append_event_locked(
                execution_id, "node_attempt_started", None, None,
                node_id=node_id, attempt=attempt, generation=generation,
                payload={"generation": generation},
            )
            self._conn.execute("COMMIT")
            return attempt
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def get_or_resume_attempt(self, execution_id: str, node_id: str, generation: int) -> int:
        row = self._conn.execute(
            "SELECT attempt FROM node_attempts WHERE execution_id = ? AND node_id = ? "
            "AND status = 'running' AND generation = ? ORDER BY attempt DESC LIMIT 1",
            (execution_id, node_id, generation),
        ).fetchone()
        if row:
            return int(row["attempt"])
        return self.begin_node_attempt(execution_id, node_id, generation)

    def get_latest_attempt(self, execution_id: str, node_id: str, generation: int) -> Optional[int]:
        row = self._conn.execute(
            "SELECT attempt FROM node_attempts WHERE execution_id = ? AND node_id = ? "
            "AND generation = ? ORDER BY attempt DESC LIMIT 1",
            (execution_id, node_id, generation),
        ).fetchone()
        return int(row["attempt"]) if row else None

    def finish_node_attempt(self, execution_id: str, node_id: str, attempt: int,
                            status: str, generation: Optional[int] = None,
                            result: Any = None, error: Optional[str] = None) -> None:
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            self._conn.execute(
                "UPDATE node_attempts SET status = ?, finished_at = ?, result = ?, error = ? "
                "WHERE execution_id = ? AND node_id = ? AND attempt = ?",
                (status, self._now(), json.dumps(result) if result is not None else None,
                 error, execution_id, node_id, attempt),
            )
            row = self.get_execution_row(execution_id)
            event_generation = generation if generation is not None else (row["generation"] if row else 0)
            self._append_event_locked(
                execution_id, f"node_attempt_{status}", None, None,
                node_id=node_id, attempt=attempt, generation=event_generation,
                payload={"result": result, "error": error, "generation": event_generation},
            )
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def get_last_node_attempt(self, execution_id: str, node_id: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM node_attempts WHERE execution_id = ? AND node_id = ? "
            "ORDER BY attempt DESC LIMIT 1",
            (execution_id, node_id),
        ).fetchone()
        if not row:
            return None
        d = dict(row)
        if d.get("result"):
            try:
                d["result"] = json.loads(d["result"])
            except Exception:
                pass
        return d

    def get_last_succeeded_node(self, execution_id: str, candidate_ids: List[str]) -> Optional[str]:
        if not candidate_ids:
            return None
        placeholders = ",".join("?" for _ in candidate_ids)
        rows = self._conn.execute(
            f"SELECT node_id, MAX(attempt) AS attempt FROM node_attempts "
            f"WHERE execution_id = ? AND status = 'succeeded' AND node_id IN ({placeholders}) "
            f"GROUP BY node_id",
            (execution_id, *candidate_ids),
        ).fetchall()
        succeeded = {row["node_id"] for row in rows}
        for node_id in candidate_ids:
            if node_id in succeeded:
                return node_id
        return None

    def idempotency_key(self, execution_id: str, node_id: str, attempt: int) -> str:
        return f"{execution_id}:{node_id}:{attempt}"

    def begin_side_effect(self, execution_id: str, node_id: str, attempt: int,
                          generation: int = 0) -> Tuple[str, bool]:
        key = self.idempotency_key(execution_id, node_id, attempt)
        now = self._now()
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO idempotency_keys "
                "(idempotency_key, execution_id, node_id, attempt, generation, status, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (key, execution_id, node_id, attempt, generation, "started", now, now),
            )
            claimed = cur.rowcount > 0
            self._conn.execute("COMMIT")
            return key, claimed
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def complete_side_effect(self, execution_id: str, node_id: str, attempt: int,
                             result: Any) -> None:
        key = self.idempotency_key(execution_id, node_id, attempt)
        self._conn.execute(
            "UPDATE idempotency_keys SET status = ?, result = ?, updated_at = ? "
            "WHERE idempotency_key = ? AND status != ?",
            ("completed", json.dumps(result), self._now(), key, "completed"),
        )

    def fail_side_effect(self, execution_id: str, node_id: str, attempt: int,
                         error: str) -> None:
        key = self.idempotency_key(execution_id, node_id, attempt)
        self._conn.execute(
            "UPDATE idempotency_keys SET status = ?, result = ?, updated_at = ? "
            "WHERE idempotency_key = ? AND status NOT IN (?, ?)",
            ("failed", json.dumps({"error": error}), self._now(), key, "completed", "failed"),
        )

    def get_side_effect(self, execution_id: str, node_id: str, attempt: int) -> Optional[Dict[str, Any]]:
        key = self.idempotency_key(execution_id, node_id, attempt)
        row = self._conn.execute(
            "SELECT * FROM idempotency_keys WHERE idempotency_key = ?", (key,)
        ).fetchone()
        if not row:
            return None
        d = dict(row)
        if d.get("result"):
            try:
                d["result"] = json.loads(d["result"])
            except Exception:
                pass
        return d

    def record_side_effect_call(self, execution_id: str, node_id: str, attempt: int,
                                side_effect_name: str, generation: int = 0) -> None:
        self._conn.execute(
            "INSERT INTO side_effect_calls "
            "(execution_id, node_id, attempt, generation, side_effect_name, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (execution_id, node_id, attempt, generation, side_effect_name, self._now()),
        )

    def count_side_effect_calls(self, execution_id: str, node_id: str,
                                side_effect_name: Optional[str] = None) -> int:
        if side_effect_name is not None:
            row = self._conn.execute(
                "SELECT COUNT(*) AS c FROM side_effect_calls "
                "WHERE execution_id = ? AND node_id = ? AND side_effect_name = ?",
                (execution_id, node_id, side_effect_name),
            ).fetchone()
        else:
            row = self._conn.execute(
                "SELECT COUNT(*) AS c FROM side_effect_calls "
                "WHERE execution_id = ? AND node_id = ?",
                (execution_id, node_id),
            ).fetchone()
        return int(row["c"])

    def claim_command(self, execution_id: str, command: str, command_id: str) -> str:
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            row = self._conn.execute(
                "SELECT status FROM commands WHERE command_id = ?", (command_id,)
            ).fetchone()
            if row:
                existing_status = row["status"]
                if existing_status in ("accepted", "rejected"):
                    self._conn.execute("COMMIT")
                    return existing_status
                if existing_status == "processing":
                    self._conn.execute("COMMIT")
                    return "duplicate"
            self._conn.execute(
                "INSERT INTO commands (command_id, execution_id, command, status, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (command_id, execution_id, command, "processing", self._now()),
            )
            self._conn.execute("COMMIT")
            return "new"
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def complete_command(self, command_id: str, accepted: bool) -> None:
        self._conn.execute(
            "UPDATE commands SET status = ? WHERE command_id = ? AND status = ?",
            ("accepted" if accepted else "rejected", command_id, "processing"),
        )

    def is_command_processed(self, command_id: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM commands WHERE command_id = ?", (command_id,)
        ).fetchone()
        return row is not None

    def upsert_branch_state(self, execution_id: str, parallel_node_id: str,
                            branch_id: str, generation: int, status: str,
                            result: Any = None, completed_node_id: Optional[str] = None) -> None:
        self._conn.execute(
            "INSERT INTO branch_state "
            "(execution_id, parallel_node_id, branch_id, generation, status, result, completed_node_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(execution_id, parallel_node_id, branch_id, generation) "
            "DO UPDATE SET status = excluded.status, result = excluded.result, "
            "completed_node_id = COALESCE(excluded.completed_node_id, branch_state.completed_node_id)",
            (execution_id, parallel_node_id, branch_id, generation, status,
             json.dumps(result) if result is not None else None, completed_node_id),
        )

    def get_branch_state(self, execution_id: str, parallel_node_id: str,
                         branch_id: str, generation: int) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM branch_state WHERE execution_id = ? AND parallel_node_id = ? "
            "AND branch_id = ? AND generation = ?",
            (execution_id, parallel_node_id, branch_id, generation),
        ).fetchone()
        if not row:
            return None
        d = dict(row)
        if d.get("result"):
            try:
                d["result"] = json.loads(d["result"])
            except Exception:
                pass
        return d

    def get_branches_for_generation(self, execution_id: str, parallel_node_id: str,
                                    generation: int) -> List[Dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT * FROM branch_state WHERE execution_id = ? AND parallel_node_id = ? "
            "AND generation = ? ORDER BY branch_id ASC",
            (execution_id, parallel_node_id, generation),
        ).fetchall()
        result = []
        for row in rows:
            d = dict(row)
            if d.get("result"):
                try:
                    d["result"] = json.loads(d["result"])
                except Exception:
                    pass
            result.append(d)
        return result

    def _snapshot_locked(self, execution_id: str) -> Dict[str, Any]:
        row = self._conn.execute(
            "SELECT * FROM executions WHERE execution_id = ?", (execution_id,)
        ).fetchone()
        if not row:
            raise KeyError(f"Execution {execution_id} not found")
        variables = json.loads(row["variables"] or "{}")
        loop_counts = json.loads(row["loop_counts"] or "{}")
        latest_seq = self._conn.execute(
            "SELECT COALESCE(MAX(seq), 0) AS s FROM events WHERE execution_id = ?",
            (execution_id,),
        ).fetchone()["s"]
        pending_approvals = self._conn.execute(
            "SELECT approval_id, node_id, attempt, generation, flow_version, branch_id, "
            "token, prompt, deadline, created_at FROM approvals "
            "WHERE execution_id = ? AND status = 'pending' ORDER BY created_at",
            (execution_id,),
        ).fetchall()
        approval_info = None
        if pending_approvals:
            a = pending_approvals[0]
            approval_info = {
                "approvalId": a["approval_id"],
                "nodeId": a["node_id"],
                "attempt": a["attempt"],
                "generation": a["generation"],
                "flowVersion": a["flow_version"],
                "branchId": a["branch_id"],
                "token": a["token"],
                "prompt": a["prompt"],
                "deadline": a["deadline"],
                "createdAt": a["created_at"],
            }
        return {
            "executionId": execution_id,
            "flowId": row["flow_id"],
            "status": row["status"],
            "currentNodeId": row["current_node_id"],
            "resumeFromNodeId": row["resume_from_node_id"],
            "variables": variables,
            "loopCounts": loop_counts,
            "generation": row["generation"],
            "executorGeneration": row["executor_generation"],
            "flowVersion": row["flow_version"] if "flow_version" in row.keys() else None,
            "nodeConfigHash": row["node_config_hash"] if "node_config_hash" in row.keys() else None,
            "latestSeq": int(latest_seq),
            "allowedActions": sorted(sm.allowed_actions(row["status"])),
            "createdAt": row["created_at"],
            "updatedAt": row["updated_at"],
            "finishedAt": row["finished_at"],
            "error": row["error"],
            "pendingApproval": approval_info,
        }

    def snapshot(self, execution_id: str) -> Dict[str, Any]:
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            snap = self._snapshot_locked(execution_id)
            self._conn.execute("COMMIT")
            return snap
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def delete_execution(self, execution_id: str) -> bool:
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            self._conn.execute("DELETE FROM events WHERE execution_id = ?", (execution_id,))
            self._conn.execute("DELETE FROM node_attempts WHERE execution_id = ?", (execution_id,))
            self._conn.execute("DELETE FROM idempotency_keys WHERE execution_id = ?", (execution_id,))
            self._conn.execute("DELETE FROM side_effect_calls WHERE execution_id = ?", (execution_id,))
            self._conn.execute("DELETE FROM commands WHERE execution_id = ?", (execution_id,))
            self._conn.execute("DELETE FROM branch_state WHERE execution_id = ?", (execution_id,))
            self._conn.execute("DELETE FROM seq_tracker WHERE execution_id = ?", (execution_id,))
            cur = self._conn.execute("DELETE FROM executions WHERE execution_id = ?", (execution_id,))
            self._conn.execute("COMMIT")
            return cur.rowcount > 0
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def save_flow_version(self, flow_id: str, version: int, name: str,
                          definition: str, node_config_hash: str) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO flow_versions (flow_id, version, name, definition, node_config_hash, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (flow_id, version, name, definition, node_config_hash, self._now()),
        )

    def get_flow_version(self, flow_id: str, version: int) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM flow_versions WHERE flow_id = ? AND version = ?",
            (flow_id, version),
        ).fetchone()
        if not row:
            return None
        d = dict(row)
        d["definition_obj"] = json.loads(d["definition"])
        return d

    def get_latest_flow_version(self, flow_id: str) -> Optional[int]:
        row = self._conn.execute(
            "SELECT MAX(version) AS v FROM flow_versions WHERE flow_id = ?",
            (flow_id,),
        ).fetchone()
        v = row["v"] if row else None
        return int(v) if v is not None else None

    def list_flow_versions(self, flow_id: str) -> List[Dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT flow_id, version, name, node_config_hash, created_at FROM flow_versions "
            "WHERE flow_id = ? ORDER BY version DESC",
            (flow_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def delete_flow_version(self, flow_id: str, version: int) -> bool:
        cur = self._conn.execute(
            "DELETE FROM flow_versions WHERE flow_id = ? AND version = ?",
            (flow_id, version),
        )
        return cur.rowcount > 0

    def version_has_active_executions(self, flow_id: str, version: int) -> bool:
        row = self._conn.execute(
            "SELECT COUNT(*) AS c FROM executions WHERE flow_id = ? AND flow_version = ? "
            "AND status NOT IN (?, ?, ?)",
            (flow_id, version, sm.SUCCEEDED, sm.FAILED, sm.CANCELLED),
        ).fetchone()
        return int(row["c"]) > 0

    def any_version_has_active_executions(self, flow_id: str) -> bool:
        row = self._conn.execute(
            "SELECT COUNT(*) AS c FROM executions WHERE flow_id = ? "
            "AND status NOT IN (?, ?, ?)",
            (flow_id, sm.SUCCEEDED, sm.FAILED, sm.CANCELLED),
        ).fetchone()
        return int(row["c"]) > 0

    def delete_all_flow_versions(self, flow_id: str) -> int:
        cur = self._conn.execute("DELETE FROM flow_versions WHERE flow_id = ?", (flow_id,))
        return cur.rowcount

    def create_approval(self, approval_id: str, execution_id: str, node_id: str,
                        attempt: int, generation: int, flow_version: Optional[int],
                        branch_id: Optional[str], token: str, prompt: str,
                        deadline: float) -> None:
        self._conn.execute(
            "INSERT INTO approvals (approval_id, execution_id, node_id, attempt, generation, "
            "flow_version, branch_id, token, status, prompt, deadline, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)",
            (approval_id, execution_id, node_id, attempt, generation, flow_version,
             branch_id, token, prompt, deadline, self._now()),
        )

    def get_approval(self, approval_id: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM approvals WHERE approval_id = ?", (approval_id,)
        ).fetchone()
        return dict(row) if row else None

    def get_approval_by_token(self, token: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM approvals WHERE token = ?", (token,)
        ).fetchone()
        return dict(row) if row else None

    def get_pending_approval(self, execution_id: str, node_id: str,
                             generation: int, branch_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        if branch_id is not None:
            row = self._conn.execute(
                "SELECT * FROM approvals WHERE execution_id = ? AND node_id = ? "
                "AND generation = ? AND branch_id = ? AND status = 'pending' "
                "ORDER BY created_at DESC LIMIT 1",
                (execution_id, node_id, generation, branch_id),
            ).fetchone()
        else:
            row = self._conn.execute(
                "SELECT * FROM approvals WHERE execution_id = ? AND node_id = ? "
                "AND generation = ? AND branch_id IS NULL AND status = 'pending' "
                "ORDER BY created_at DESC LIMIT 1",
                (execution_id, node_id, generation),
            ).fetchone()
        return dict(row) if row else None

    def respond_to_approval(self, token: str, status: str,
                            responded_by: Optional[str], comment: Optional[str]) -> Optional[Dict[str, Any]]:
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            row = self._conn.execute(
                "SELECT * FROM approvals WHERE token = ?", (token,)
            ).fetchone()
            if not row:
                self._conn.execute("COMMIT")
                return None
            existing = dict(row)
            if existing["status"] != "pending":
                self._conn.execute("COMMIT")
                return existing
            self._conn.execute(
                "UPDATE approvals SET status = ?, responded_by = ?, response_comment = ?, "
                "responded_at = ? WHERE token = ? AND status = 'pending'",
                (status, responded_by, comment, self._now(), token),
            )
            self._conn.execute("COMMIT")
            return self.get_approval_by_token(token)
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def list_pending_approvals(self, execution_id: Optional[str] = None) -> List[Dict[str, Any]]:
        if execution_id:
            rows = self._conn.execute(
                "SELECT * FROM approvals WHERE execution_id = ? AND status = 'pending' "
                "ORDER BY deadline",
                (execution_id,),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM approvals WHERE status = 'pending' ORDER BY deadline"
            ).fetchall()
        return [dict(r) for r in rows]

    def list_expired_approvals(self, now: Optional[float] = None) -> List[Dict[str, Any]]:
        ts = now if now is not None else self._now()
        rows = self._conn.execute(
            "SELECT * FROM approvals WHERE status = 'pending' AND deadline <= ?",
            (ts,),
        ).fetchall()
        return [dict(r) for r in rows]

    def expire_approval(self, approval_id: str) -> bool:
        cur = self._conn.execute(
            "UPDATE approvals SET status = 'expired', responded_at = ? "
            "WHERE approval_id = ? AND status = 'pending'",
            (self._now(), approval_id),
        )
        return cur.rowcount > 0

    def cancel_approvals_for_execution(self, execution_id: str) -> int:
        cur = self._conn.execute(
            "UPDATE approvals SET status = 'cancelled', responded_at = ? "
            "WHERE execution_id = ? AND status = 'pending'",
            (self._now(), execution_id),
        )
        return cur.rowcount
