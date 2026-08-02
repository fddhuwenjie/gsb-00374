export type NodeType = 'start' | 'end' | 'task' | 'condition' | 'loop' | 'wait' | 'http' | 'sql' | 'parallel' | 'subflow' | 'trycatch' | 'filewrite' | 'approval';
export type ExecutionStatus = 'idle' | 'running' | 'paused' | 'stopped' | 'completed' | 'error';
export type TraceAction = 'enter' | 'exit' | 'error';
export type EdgeHandle = 'true' | 'false' | 'loop' | 'catch';

// ---- persistent (v2) execution engine types ----
export type V2ExecutionStatus =
  | 'queued'
  | 'running'
  | 'pausing'
  | 'paused'
  | 'retry_wait'
  | 'awaiting_approval'
  | 'succeeded'
  | 'failed'
  | 'cancelled';

export type V2Command = 'pause' | 'resume' | 'cancel' | 'retry' | 'approve' | 'reject';

export interface V2PendingApproval {
  nodeId: string;
  attempt: any;
  generation: any;
  token: string;
  approvers: string[];
  deadline: number;
  flowVersion: number | null;
}

export interface V2Event {
  seq: number;
  executionId: string;
  ts: number;
  type: 'created' | 'status' | 'node_started' | 'node_completed' | 'node_failed' | 'side_effect' | 'command' | 'approval_requested' | 'approval_resolved';
  fromStatus?: V2ExecutionStatus | null;
  toStatus?: V2ExecutionStatus;
  nodeId?: string | null;
  attempt?: number;
  allowedCommands?: V2Command[];
  allowedTransitions?: V2ExecutionStatus[];
  [key: string]: any;
}

export interface V2Snapshot {
  executionId: string;
  flowId: string;
  status: V2ExecutionStatus;
  seq: number;
  allowedTransitions: V2ExecutionStatus[];
  allowedCommands: V2Command[];
  variables: Record<string, any>;
  currentNodeId: string | null;
  completedNodes: string[];
  lastError: string | null;
  pendingApprovals?: V2PendingApproval[];
}

export type MonitorMessage =
  | ({ type: 'snapshot' } & V2Snapshot)
  | { type: 'event'; event: V2Event }
  | { type: 'error'; message: string };

export interface Position {
  x: number;
  y: number;
}

export interface RetryConfig {
  maxAttempts: number;
  delaySeconds: number;
  backoff: 'fixed' | 'exponential';
  maxDelaySeconds: number;
}

export interface HttpConfig {
  url: string;
  method: 'GET' | 'POST' | 'PUT' | 'DELETE' | 'PATCH';
  headers: Record<string, string>;
  body: string;
  timeout: number;
}

export interface SqlConfig {
  connectionString: string;
  query: string;
  params: any[];
}

export interface ParallelConfig {
  branchNodeIds: string[];
}

export interface SubflowConfig {
  subflowId: string;
}

export interface TryCatchConfig {
  tryNodeIds: string[];
  catchNodeIds: string[];
}

export interface NodeData {
  label: string;
  code?: string;
  expression?: string;
  seconds?: number;
  anchorId?: string;
  retry?: RetryConfig;
  httpConfig?: HttpConfig;
  sqlConfig?: SqlConfig;
  parallelConfig?: ParallelConfig;
  subflowConfig?: SubflowConfig;
  tryCatchConfig?: TryCatchConfig;
  fileConfig?: { path: string; content: string };
  approvalConfig?: { approvers: string[]; timeoutSeconds: number };
  breakpoint?: boolean;
}

export interface FlowNode {
  id: string;
  type: NodeType;
  position: Position;
  data: NodeData;
}

export interface FlowEdge {
  id: string;
  source: string;
  target: string;
  sourceHandle?: EdgeHandle;
}

export interface FlowDefinition {
  id: string;
  name: string;
  nodes: FlowNode[];
  edges: FlowEdge[];
  createdAt: number;
  updatedAt: number;
}

export interface TraceLog {
  timestamp: number;
  nodeId: string;
  nodeType: NodeType;
  action: TraceAction;
  variables: Record<string, any>;
  message?: string;
}

export interface ExecutionState {
  flowId: string;
  status: ExecutionStatus;
  currentNodeId: string | null;
  variables: Record<string, any>;
  trace: TraceLog[];
  loopCounts: Record<string, number>;
  snapshots: Record<string, Record<string, any>>;
}

export interface Execution {
  id: string;
  flowId: string;
  status: ExecutionStatus;
  startedAt: number;
  finishedAt: number;
  variables: Record<string, any>;
  trace: TraceLog[];
  snapshots: Record<string, Record<string, any>>;
}

export interface Trigger {
  id: string;
  flowId: string;
  type: 'cron' | 'webhook' | 'flow_completed';
  cronExpression: string;
  webhookPath: string;
  sourceFlowId: string;
  enabled: boolean;
  createdAt: number;
}

export type ClientMessage =
  | { type: 'execute'; flow: FlowDefinition }
  | { type: 'pause' }
  | { type: 'resume' }
  | { type: 'step' }
  | { type: 'stop' }
  | { type: 'setVariable'; name: string; value: any }
  | { type: 'setBreakpoint'; nodeId: string; enabled: boolean }
  | { type: 'stepInto' }
  | { type: 'stepOut' }
  | { type: 'evaluate'; expression: string };

export type ServerMessage =
  | { type: 'nodeEnter'; nodeId: string; variables: Record<string, any>; callDepth: number }
  | { type: 'nodeExit'; nodeId: string; variables: Record<string, any>; callDepth: number }
  | { type: 'nodeError'; nodeId: string; error: string; variables: Record<string, any>; callDepth: number }
  | { type: 'status'; status: ExecutionStatus; variables: Record<string, any> }
  | { type: 'trace'; log: TraceLog }
  | { type: 'completed'; variables: Record<string, any>; trace: TraceLog[] }
  | { type: 'error'; message: string }
  | { type: 'breakpointUpdated'; nodeId: string; enabled: boolean; breakpoints: string[] }
  | { type: 'breakpointHit'; nodeId: string; variables: Record<string, any>; callDepth: number }
  | { type: 'debugPaused'; reason: string; nodeId: string; callDepth: number; variables: Record<string, any> }
  | { type: 'evaluateResult'; expression: string; result?: any; error?: string; success: boolean };
