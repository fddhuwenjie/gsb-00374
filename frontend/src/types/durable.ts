// Types for the durable execution engine monitor.
// Mirrors backend/engine/durable/state_machine.py and ws/durable_monitor.py.

export type DurableState =
  | 'queued'
  | 'running'
  | 'pausing'
  | 'paused'
  | 'awaiting_approval'
  | 'retry_wait'
  | 'succeeded'
  | 'failed'
  | 'cancelled';

export type DurableCommand = 'start' | 'pause' | 'resume' | 'approve' | 'reject' | 'cancel';

export interface PendingApproval {
  approvalId: string;
  nodeId?: string;
  branchId?: string | null;
  generation?: number;
  attempt?: number;
  flowVersion?: number | null;
  deadline?: number | null;
  token: string;
}

export interface DurableEvent {
  seq: number;
  kind: string;
  ts: number;
  // state events
  prevState?: DurableState;
  state?: DurableState;
  // node events
  nodeId?: string;
  attempt?: number;
  error?: string;
  variables?: Record<string, unknown>;
  // effect events
  key?: string;
  result?: unknown;
  // parallel branch events
  parallelNodeId?: string;
  branchId?: string;
  generation?: number;
  value?: unknown;
  // approval events
  approvalId?: string;
  decision?: string;
  approver?: string | null;
  deadline?: number | null;
}

export interface DurableSnapshot {
  type: 'snapshot';
  executionId: string;
  flowId?: string | null;
  flowVersion?: number | null;
  contentHash?: string | null;
  state: DurableState;
  prevState?: DurableState | null;
  lastSeq: number;
  variables: Record<string, unknown>;
  completedNodes: string[];
  allowedCommands: DurableCommand[];
  pendingApprovals?: PendingApproval[];
  events: DurableEvent[];
}

export interface DurableEventFrame {
  type: 'event';
  event: DurableEvent;
  allowedCommands?: DurableCommand[];
}

export interface DurablePingFrame {
  type: 'ping';
}

export type DurableServerFrame =
  | DurableSnapshot
  | DurableEventFrame
  | DurablePingFrame;
