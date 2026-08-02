import { useCallback, useEffect, useRef, useState } from 'react';
import type {
  DurableCommand,
  DurableEvent,
  DurableServerFrame,
  DurableState,
  PendingApproval,
} from '../types/durable';

/**
 * Live monitor for one durable execution.
 *
 * Correctness properties enforced client-side:
 *  - We track the highest applied `seq` (a high-water mark). Any incoming event
 *    with `seq <= lastSeq` is ignored, so duplicate or out-of-order delivery can
 *    never make the UI go backwards.
 *  - On (re)connect we send `{ lastSeq }` so the server backfills exactly the
 *    events we missed; a fresh join sends 0 and gets the full history.
 *  - Button availability (including approve/reject) is taken verbatim from the
 *    server's `allowedCommands`, so the UI can only offer transitions the server
 *    would accept.
 *  - Pending approvals carry a server-issued recovery token; approve/reject POST
 *    that token back so stale/expired/duplicate responses are rejected server
 *    side rather than trusted from the client.
 */
export function useDurableMonitor(executionId: string | null) {
  const [state, setState] = useState<DurableState | null>(null);
  const [variables, setVariables] = useState<Record<string, unknown>>({});
  const [events, setEvents] = useState<DurableEvent[]>([]);
  const [allowedCommands, setAllowedCommands] = useState<DurableCommand[]>([]);
  const [connected, setConnected] = useState(false);
  const [flowVersion, setFlowVersion] = useState<number | null>(null);
  const [flowId, setFlowId] = useState<string | null>(null);
  const [pendingApprovals, setPendingApprovals] = useState<PendingApproval[]>([]);

  const lastSeqRef = useRef(0);
  const wsRef = useRef<WebSocket | null>(null);
  const seenRef = useRef<Set<number>>(new Set());

  // Re-fetch the authoritative pending-approval list (with fresh tokens).
  const refreshApprovals = useCallback(async () => {
    if (!executionId) return;
    try {
      const res = await fetch(`/api/durable/${executionId}/approvals`);
      if (res.ok) {
        const data = await res.json();
        setPendingApprovals(data.pendingApprovals ?? []);
      }
    } catch {
      /* transient network error -- next event will retrigger */
    }
  }, [executionId]);

  const applyEvent = useCallback(
    (ev: DurableEvent) => {
      // Guard against replays / reordering: never regress the high-water mark.
      if (ev.seq <= lastSeqRef.current || seenRef.current.has(ev.seq)) {
        return;
      }
      lastSeqRef.current = ev.seq;
      seenRef.current.add(ev.seq);

      setEvents((prev) => [...prev, ev]);

      if (ev.kind === 'state' && ev.state) {
        setState(ev.state);
      }
      if (ev.kind === 'node_boundary' && ev.variables) {
        setVariables(ev.variables);
      }
      // Any approval lifecycle event changes the pending set -> refresh tokens.
      if (ev.kind === 'approval_requested' || ev.kind === 'approval_resolved') {
        void refreshApprovals();
      }
    },
    [refreshApprovals]
  );

  useEffect(() => {
    if (!executionId) return;

    lastSeqRef.current = 0;
    seenRef.current = new Set();
    setEvents([]);
    setState(null);
    setVariables({});
    setFlowVersion(null);
    setFlowId(null);
    setPendingApprovals([]);

    const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const url = `${proto}//${window.location.host}/ws/durable/${executionId}`;
    const ws = new WebSocket(url);
    wsRef.current = ws;

    ws.onopen = () => {
      setConnected(true);
      // Resume from where we left off (0 for a fresh mount).
      ws.send(JSON.stringify({ lastSeq: lastSeqRef.current }));
    };

    ws.onmessage = (evt) => {
      const frame = JSON.parse(evt.data) as DurableServerFrame;
      if (frame.type === 'snapshot') {
        setState(frame.state);
        setVariables(frame.variables);
        setAllowedCommands(frame.allowedCommands);
        setFlowVersion(frame.flowVersion ?? null);
        setFlowId(frame.flowId ?? null);
        setPendingApprovals(frame.pendingApprovals ?? []);
        // Backfill: apply every event beyond our high-water mark in order.
        frame.events.forEach(applyEvent);
        if (frame.lastSeq > lastSeqRef.current) {
          lastSeqRef.current = frame.lastSeq;
        }
      } else if (frame.type === 'event') {
        applyEvent(frame.event);
        if (frame.allowedCommands) {
          setAllowedCommands(frame.allowedCommands);
        }
      }
      // 'ping' frames are keep-alives and need no handling.
    };

    ws.onclose = () => setConnected(false);
    ws.onerror = () => setConnected(false);

    return () => {
      ws.close();
      wsRef.current = null;
    };
  }, [executionId, applyEvent]);

  const sendCommand = useCallback(
    async (command: DurableCommand) => {
      if (!executionId) return;
      const res = await fetch(`/api/durable/${executionId}/command`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ command }),
      });
      if (res.ok) {
        const data = await res.json();
        // Trust the server's post-command allowed set immediately.
        if (Array.isArray(data.allowedCommands)) {
          setAllowedCommands(data.allowedCommands);
        }
        if (data.state) setState(data.state);
      }
    },
    [executionId]
  );

  // Respond to a specific pending approval with its recovery token. The server
  // rejects stale / expired / duplicate tokens, so the UI can post optimistically.
  const respondApproval = useCallback(
    async (token: string, decision: 'approved' | 'rejected', approver?: string) => {
      if (!executionId) return { accepted: false };
      const res = await fetch(`/api/durable/${executionId}/approvals`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ token, decision, approver }),
      });
      const data = res.ok ? await res.json() : { accepted: false };
      if (data.state) setState(data.state);
      if (Array.isArray(data.allowedCommands)) setAllowedCommands(data.allowedCommands);
      void refreshApprovals();
      return data;
    },
    [executionId, refreshApprovals]
  );

  return {
    state,
    variables,
    events,
    allowedCommands,
    connected,
    flowVersion,
    flowId,
    pendingApprovals,
    sendCommand,
    respondApproval,
  };
}
