"""Integration tests for immutable workflow definition versioning.

Run with:  python -m pytest backend/test_durable_versioning.py -v

Builds on the durable engine (same event seq, execution generation and
idempotency keys) and proves that changing a flow definition never pollutes
executions already bound to an earlier version. Covers:

  * immutable binding: flowVersion + content/config hashes pinned per execution
  * stable diff: added / removed / config-changed nodes vs edge (wiring) changes
  * edit-while-running concurrency: editing mid-run creates a new version only
  * old-version recovery + retry uses the ORIGINAL spec, not the latest
  * recovery is refused when the bound version is missing
  * version deletion is blocked while executions still bind to it
  * export / import round-trips versions and rejects tampered bundles
  * definition changes leave an existing execution's seq / generation /
    idempotency keys untouched
"""

import asyncio
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from engine.durable.engine import DurableEngine
from engine.durable.event_store import EventStore, RecoveredState
from engine.durable.state_machine import ExecState
from engine.durable.effects import idempotency_key
from engine.durable.versioning import (
    FlowVersionStore,
    MissingVersionError,
    VersionInUseError,
    content_hash,
    node_config_hash,
    diff_specs,
)


@pytest.fixture
def tmp_root(tmp_path):
    return str(tmp_path / "flows")


def seqs(events):
    return [e["seq"] for e in events]


def flow_v1():
    return {
        "id": "orders",
        "nodes": {
            "s": {"type": "start", "next": "a"},
            "a": {"type": "task", "next": "b", "ops": [{"set": {"x": 1}}]},
            "b": {"type": "task", "next": "e", "ops": [{"incr": "x"}]},
            "e": {"type": "end"},
        },
    }


def flow_v2_config_change():
    # Same shape as v1 but node "a" has a different config (op value).
    f = flow_v1()
    f["nodes"]["a"]["ops"] = [{"set": {"x": 99}}]
    return f


def flow_v2_add_remove_edge():
    # Insert a new node "c" between b and e (adds node + rewires edges).
    return {
        "id": "orders",
        "nodes": {
            "s": {"type": "start", "next": "a"},
            "a": {"type": "task", "next": "b", "ops": [{"set": {"x": 1}}]},
            "b": {"type": "task", "next": "c", "ops": [{"incr": "x"}]},
            "c": {"type": "task", "next": "e", "ops": [{"incr": "x"}]},
            "e": {"type": "end"},
        },
    }


# ---------------------------------------------------------------------------
# 1. immutable binding + hashes
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_execution_binds_immutable_version_and_hashes(tmp_root):
    engine = DurableEngine(tmp_root)
    ex = await engine.create("o1", flow_v1(), {})
    await engine.start("o1")
    await engine.wait("o1")

    snap = engine.snapshot("o1")
    assert snap["state"] == ExecState.SUCCEEDED
    assert snap["flowVersion"] == 1
    assert snap["flowId"] == "orders"
    # The header pins the exact content hash of v1.
    assert snap["contentHash"] == content_hash(flow_v1())

    # The version record stores per-node config hashes.
    record = engine.get_version("orders", 1)
    assert record["nodeHashes"]["a"] == node_config_hash(flow_v1()["nodes"]["a"])


@pytest.mark.asyncio
async def test_identical_edit_does_not_bump_version(tmp_root):
    engine = DurableEngine(tmp_root)
    r1 = engine.register_flow(flow_v1())
    r2 = engine.edit_flow(flow_v1())  # identical content
    assert r1["version"] == 1
    assert r2["version"] == 1  # no new version for a no-op edit


# ---------------------------------------------------------------------------
# 2. stable diff classification
# ---------------------------------------------------------------------------

def test_diff_distinguishes_config_vs_edge_vs_add_remove():
    # config-only change
    d = diff_specs(flow_v1(), flow_v2_config_change())
    assert d.changed_nodes == ["a"]
    assert d.added_nodes == [] and d.removed_nodes == []
    assert d.added_edges == [] and d.removed_edges == []

    # add node + rewire edges
    d2 = diff_specs(flow_v1(), flow_v2_add_remove_edge())
    assert d2.added_nodes == ["c"]
    assert d2.removed_nodes == []
    # node "b" only changed its wiring (next), NOT its config -> not "changed"
    assert "b" not in d2.changed_nodes
    # edges: b->e removed, b->c and c->e added
    assert ("b", "next", "e") in d2.removed_edges
    assert ("b", "next", "c") in d2.added_edges
    assert ("c", "next", "e") in d2.added_edges


@pytest.mark.asyncio
async def test_diff_versions_via_engine(tmp_root):
    engine = DurableEngine(tmp_root)
    engine.register_flow(flow_v1())
    engine.edit_flow(flow_v2_add_remove_edge())
    diff = engine.diff_versions("orders", 1, 2)
    assert diff["addedNodes"] == ["c"]
    assert ["b", "next", "e"] in diff["removedEdges"]


# ---------------------------------------------------------------------------
# 3. edit-while-running concurrency
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_edit_while_running_creates_new_version_only(tmp_root):
    slow_flow = {
        "id": "orders",
        "nodes": {
            "s": {"type": "start", "next": "a"},
            "a": {"type": "task", "next": "b", "steps": 20, "stepDelay": 0.02,
                  "ops": [{"set": {"x": 1}}]},
            "b": {"type": "task", "next": "e", "ops": [{"incr": "x"}]},
            "e": {"type": "end"},
        },
    }
    engine = DurableEngine(tmp_root)
    await engine.create("run1", slow_flow, {})
    await engine.start("run1")

    # While run1 is mid-flight on v1, edit the flow into v2.
    await asyncio.sleep(0.05)
    edited = flow_v2_config_change()
    edited["nodes"]["a"]["steps"] = 20
    edited["nodes"]["a"]["stepDelay"] = 0.02
    rec = engine.edit_flow(edited)
    assert rec["version"] == 2  # a brand new version

    await engine.wait("run1")
    snap = engine.snapshot("run1")

    # The running execution is unaffected: still bound to v1, ran v1's op (x=1+1).
    assert snap["state"] == ExecState.SUCCEEDED
    assert snap["flowVersion"] == 1
    assert snap["variables"]["x"] == 2  # v1 semantics, not v2's x=99

    # A new execution created now binds to the latest (v2) semantics (x=99+1).
    await engine.create("run2", flow_id="orders")  # binds latest = v2
    await engine.start("run2")
    await engine.wait("run2")
    snap2 = engine.snapshot("run2")
    assert snap2["flowVersion"] == 2
    assert snap2["variables"]["x"] == 100


# ---------------------------------------------------------------------------
# 4. old-version recovery + retry uses the original spec
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_old_version_recovery_uses_original_spec(tmp_root):
    v1 = {
        "id": "orders",
        "nodes": {
            "s": {"type": "start", "next": "a"},
            "a": {"type": "task", "next": "b", "ops": [{"set": {"x": 10}}]},
            "b": {"type": "task", "next": "c", "steps": 20, "stepDelay": 0.02,
                  "ops": [{"incr": "x"}]},
            "c": {"type": "task", "next": "e", "ops": [{"incr": "x"}]},
            "e": {"type": "end"},
        },
    }
    engine1 = DurableEngine(tmp_root)
    await engine1.create("rec1", v1, {})
    await engine1.start("rec1")
    # Crash mid node "b".
    await asyncio.sleep(0.05)
    task = engine1._tasks.get("rec1")
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Edit the flow to a wildly different v2 AFTER the crash.
    v2 = {
        "id": "orders",
        "nodes": {
            "s": {"type": "start", "next": "a"},
            "a": {"type": "task", "next": "e", "ops": [{"set": {"x": -999}}]},
            "e": {"type": "end"},
        },
    }
    engine1.edit_flow(v2)
    assert engine1.versions.latest_version("orders") == 2

    # Recover in a fresh engine: must resume against v1, not v2.
    engine2 = DurableEngine(tmp_root)
    ex = await engine2.recover("rec1")
    assert ex.flow.spec == v1  # original definition
    await engine2.start("rec1")
    await engine2.wait("rec1")

    snap = engine2.snapshot("rec1")
    assert snap["state"] == ExecState.SUCCEEDED
    assert snap["flowVersion"] == 1
    # v1 semantics: 10 + 1 (b) + 1 (c) == 12, not v2's x=-999.
    assert snap["variables"]["x"] == 12
    boundary_nodes = [e["nodeId"] for e in snap["events"] if e["kind"] == "node_boundary"]
    for n in ("a", "b", "c", "e"):
        assert boundary_nodes.count(n) == 1


@pytest.mark.asyncio
async def test_retry_after_edit_uses_original_version(tmp_root):
    v1 = {
        "id": "orders",
        "nodes": {
            "s": {"type": "start", "next": "flaky"},
            "flaky": {
                "type": "task",
                "next": "e",
                "fail_times": 5,  # exhausts retries -> fails
                "retry": {"maxAttempts": 2, "delaySeconds": 0.0},
                "ops": [{"set": {"ok": True}}],
            },
            "e": {"type": "end"},
        },
    }
    engine = DurableEngine(tmp_root)
    await engine.create("retry1", v1, {})
    await engine.start("retry1")
    await engine.wait("retry1")
    snap = engine.snapshot("retry1")
    assert snap["state"] == ExecState.FAILED
    assert snap["flowVersion"] == 1

    # Edit to a v2 that would never fail; the failed execution stays on v1.
    v2 = json.loads(json.dumps(v1))
    v2["nodes"]["flaky"]["fail_times"] = 0
    engine.edit_flow(v2)

    # Recover the failed execution: it is terminal and still bound to v1.
    engine2 = DurableEngine(tmp_root)
    ex = await engine2.recover("retry1")
    assert ex.flow.spec["nodes"]["flaky"]["fail_times"] == 5  # v1, not v2
    snap2 = engine2.snapshot("retry1")
    assert snap2["flowVersion"] == 1
    assert snap2["state"] == ExecState.FAILED


# ---------------------------------------------------------------------------
# 5. recovery refused when bound version missing
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_recovery_refused_when_version_missing(tmp_root):
    engine = DurableEngine(tmp_root)
    await engine.create("miss1", flow_v1(), {})
    await engine.start("miss1")
    await engine.wait("miss1")

    # Force-delete the bound version behind the protection (simulate corruption).
    engine.versions.delete_version("orders", 1)

    engine2 = DurableEngine(tmp_root)
    with pytest.raises(MissingVersionError):
        await engine2.recover("miss1")


# ---------------------------------------------------------------------------
# 6. version deletion protection
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_delete_version_blocked_while_in_use(tmp_root):
    engine = DurableEngine(tmp_root)
    await engine.create("use1", flow_v1(), {})
    await engine.start("use1")
    await engine.wait("use1")

    # v1 is bound by execution use1 -> deletion must be refused.
    with pytest.raises(VersionInUseError) as ei:
        engine.delete_version("orders", 1)
    assert "use1" in ei.value.execution_ids

    # Detection is disk-based, so a fresh engine enforces it too.
    engine2 = DurableEngine(tmp_root)
    assert engine2.executions_using("orders", 1) == ["use1"]

    # A version that nothing binds to can be deleted freely.
    engine.edit_flow(flow_v2_config_change())  # creates v2, unused
    res = engine.delete_version("orders", 2)
    assert res["deleted"] is True


# ---------------------------------------------------------------------------
# 7. export / import
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_export_import_roundtrip(tmp_root, tmp_path):
    engine = DurableEngine(tmp_root)
    engine.register_flow(flow_v1())
    engine.edit_flow(flow_v2_add_remove_edge())
    bundle = engine.export_flow("orders")
    assert bundle["latest"] == 2
    assert len(bundle["versions"]) == 2

    # Import into a completely separate engine/root.
    other_root = str(tmp_path / "other")
    engine2 = DurableEngine(other_root)
    result = engine2.import_flow(bundle)
    assert result["importedVersions"] == [1, 2]
    assert engine2.versions.latest_version("orders") == 2
    # Imported specs are byte-for-byte the originals (hash verified).
    assert engine2.get_version("orders", 1)["contentHash"] == content_hash(flow_v1())

    # An execution created from the imported v1 runs the original semantics.
    await engine2.create("imp1", flow_id="orders", flow_version=1)
    await engine2.start("imp1")
    await engine2.wait("imp1")
    assert engine2.snapshot("imp1")["variables"]["x"] == 2


def test_import_rejects_tampered_bundle(tmp_root):
    engine = DurableEngine(tmp_root)
    engine.register_flow(flow_v1())
    bundle = engine.export_flow("orders")
    # Tamper with the spec but leave the old contentHash -> must be rejected.
    bundle["versions"][0]["spec"]["nodes"]["a"]["ops"] = [{"set": {"x": 777}}]
    with pytest.raises(ValueError):
        DurableEngine(tmp_root + "_2").import_flow(bundle)


# ---------------------------------------------------------------------------
# 8. definition change does not pollute existing execution
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_definition_change_does_not_pollute_seq_gen_or_keys(tmp_root):
    call_count = {"n": 0}

    async def performer(node, variables):
        call_count["n"] += 1
        return {"status": 200, "call": call_count["n"]}

    v1 = {
        "id": "orders",
        "nodes": {
            "s": {"type": "start", "next": "call"},
            "call": {"type": "http", "next": "p", "url": "http://x", "resultVar": "r"},
            "p": {
                "type": "parallel",
                "next": "e",
                "required": ["x", "y"],
                "branches": {"x": {"value": 1}, "y": {"value": 2, "work": 0.01}},
            },
            "e": {"type": "end"},
        },
    }
    engine = DurableEngine(tmp_root, http_performer=performer)
    await engine.create("np1", v1, {})
    await engine.start("np1")
    await engine.wait("np1")

    snap_before = engine.snapshot("np1")
    seqs_before = seqs(snap_before["events"])
    # The http effect used executionId+nodeId+attempt=1.
    expected_key = idempotency_key("np1", "call", 1)
    effects_before = [e for e in snap_before["events"] if e["kind"] == "effect"]
    assert effects_before[0]["key"] == expected_key
    gen_before = snap_before["variables"]["p_result"]["generation"]

    # Now heavily edit the flow into v2 (config + structure changes).
    engine.edit_flow(flow_v2_add_remove_edge())
    assert engine.versions.latest_version("orders") == 2

    # Re-read the existing execution's log: identical seqs, generation, keys.
    snap_after = engine.snapshot("np1")
    assert seqs(snap_after["events"]) == seqs_before
    assert snap_after["flowVersion"] == 1
    effects_after = [e for e in snap_after["events"] if e["kind"] == "effect"]
    assert effects_after[0]["key"] == expected_key
    assert snap_after["variables"]["p_result"]["generation"] == gen_before
    # And no extra outbound HTTP call was provoked by the edit.
    assert call_count["n"] == 1


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
