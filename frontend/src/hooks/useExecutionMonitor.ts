import { useEffect, useRef, useState } from 'react';
import type {
  MonitorMessage,
  V2Command,
  V2Event,
  V2ExecutionStatus,
  V2PendingApproval,
  V2Snapshot,
} from '../types/flow';

export interface ExecutionMonitorState {
  connected: boolean;
  executionId: string | null;
  status: V2ExecutionStatus | null;
  seq: number;
  allowedCommands: V2Command[];
  allowedTransitions: V2ExecutionStatus[];
  variables: Record<string, any>;
  currentNodeId: string | null;
  completedNodes: string[];
  lastError: string | null;
  pendingApprovals: V2PendingApproval[];
}

const INITIAL_STATE: ExecutionMonitorState = {
  connected: false,
  executionId: null,
  status: null,
  seq: 0,
  allowedCommands: [],
  allowedTransitions: [],
  variables: {},
  currentNodeId: null,
  completedNodes: [],
  lastError: null,
  pendingApprovals: [],
};

/**
 * Monitoring channel with sequence-based replay.
 *
 * - On (re)connect the client asks for events newer than the last seq it
 *   applied (`since`), receives a snapshot first, then ordered increments.
 * - Events are applied in strict seq order through a small gap buffer.
 * - Duplicate or out-of-date events (seq <= last applied) are dropped, so
 *   the UI never regresses.
 */
export function useExecutionMonitor(executionId: string | null) {
  const [state, setState] = useState<ExecutionMonitorState>(INITIAL_STATE);
  const lastSeqRef = useRef(0);
  const bufferRef = useRef<Map<number, V2Event>>(new Map());

  useEffect(() => {
    if (!executionId) {
      setState(INITIAL_STATE);
      return;
    }

    lastSeqRef.current = 0;
    bufferRef.current = new Map();
    let ws: WebSocket | null = null;
    let stopped = false;
    let reconnectTimer: number | null = null;

    const applySnapshot = (snapshot: V2Snapshot) => {
      setState({
        connected: true,
        executionId: snapshot.executionId,
        status: snapshot.status,
        seq: snapshot.seq,
        allowedCommands: snapshot.allowedCommands,
        allowedTransitions: snapshot.allowedTransitions,
        variables: snapshot.variables,
        currentNodeId: snapshot.currentNodeId,
        completedNodes: snapshot.completedNodes,
        lastError: snapshot.lastError,
        pendingApprovals: snapshot.pendingApprovals ?? [],
      });
    };

    const applyEvent = (event: V2Event) => {
      setState((prev) => {
        const next = { ...prev, seq: event.seq };
        if (event.type === 'status' && event.toStatus) {
          next.status = event.toStatus;
          if (event.allowedCommands) next.allowedCommands = event.allowedCommands;
          if (event.allowedTransitions) next.allowedTransitions = event.allowedTransitions;
        }
        if (event.type === 'node_completed' && event.nodeId) {
          next.currentNodeId = event.nodeId;
          next.completedNodes = [...prev.completedNodes, event.nodeId];
          if (event.variables) next.variables = event.variables;
        }
        if (event.type === 'node_failed') {
          next.lastError = (event.error as string) ?? 'node failed';
        }
        if (event.type === 'approval_requested') {
          // a newer request for the same node supersedes the older one
          next.pendingApprovals = [
            ...prev.pendingApprovals.filter((p) => p.nodeId !== event.nodeId),
            {
              nodeId: event.nodeId as string,
              attempt: event.attempt,
              generation: event.generation,
              token: event.token as string,
              approvers: (event.approvers as string[]) ?? [],
              deadline: event.deadline as number,
              flowVersion: (event.flowVersion as number) ?? null,
            },
          ];
        }
        if (event.type === 'approval_resolved') {
          next.pendingApprovals = prev.pendingApprovals.filter(
            (p) => p.token !== event.token
          );
        }
        return next;
      });
    };

    const flushBuffer = () => {
      // Apply buffered events strictly in order, stopping at the first gap.
      while (bufferRef.current.has(lastSeqRef.current + 1)) {
        const event = bufferRef.current.get(lastSeqRef.current + 1)!;
        bufferRef.current.delete(event.seq);
        lastSeqRef.current = event.seq;
        applyEvent(event);
      }
    };

    const connect = () => {
      const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      const url = `${proto}//${window.location.host}/ws/v2/executions/${executionId}?since=${lastSeqRef.current}`;
      ws = new WebSocket(url);

      ws.onopen = () => setState((prev) => ({ ...prev, connected: true }));

      ws.onmessage = (msg) => {
        let data: MonitorMessage;
        try {
          data = JSON.parse(msg.data);
        } catch {
          return;
        }
        if (data.type === 'snapshot') {
          // A snapshot older than what we already applied must not regress UI.
          if (data.seq >= lastSeqRef.current) {
            lastSeqRef.current = data.seq;
            bufferRef.current.clear();
            applySnapshot(data);
          }
        } else if (data.type === 'event') {
          const event = data.event;
          if (typeof event.seq !== 'number') return;
          if (event.seq <= lastSeqRef.current) return; // duplicate / old
          bufferRef.current.set(event.seq, event);
          flushBuffer();
        }
      };

      ws.onclose = () => {
        setState((prev) => ({ ...prev, connected: false }));
        if (!stopped) {
          // Reconnect: the server replays everything after lastSeqRef.
          reconnectTimer = window.setTimeout(connect, 1500);
        }
      };
    };

    connect();

    return () => {
      stopped = true;
      if (reconnectTimer !== null) window.clearTimeout(reconnectTimer);
      ws?.close();
    };
  }, [executionId]);

  return state;
}
