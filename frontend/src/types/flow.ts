export type NodeType = 'start' | 'end' | 'task' | 'condition' | 'loop' | 'wait' | 'http' | 'sql' | 'file_write' | 'approval' | 'parallel' | 'subflow' | 'trycatch';
export type ExecutionStatus =
  | 'idle'
  | 'queued'
  | 'running'
  | 'pausing'
  | 'paused'
  | 'awaiting_approval'
  | 'retry_wait'
  | 'succeeded'
  | 'failed'
  | 'cancelled'
  | 'stopped'
  | 'completed'
  | 'error';
export type TraceAction = 'enter' | 'exit' | 'error';
export type EdgeHandle = 'true' | 'false' | 'loop' | 'catch';
export type CommandType = 'start' | 'pause' | 'resume' | 'cancel' | 'retry' | 'approve' | 'reject';

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

export interface FileWriteConfig {
  path: string;
  content: string;
  mode: string;
}

export interface ApprovalConfig {
  prompt: string;
  approvers: string[];
  timeoutSeconds: number;
}

export interface PendingApproval {
  approvalId: string;
  nodeId: string;
  attempt: number;
  generation: number;
  flowVersion: number | null;
  branchId: string | null;
  token: string;
  prompt: string;
  deadline: number;
  createdAt: number;
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
  fileWriteConfig?: FileWriteConfig;
  approvalConfig?: ApprovalConfig;
  parallelConfig?: ParallelConfig;
  subflowConfig?: SubflowConfig;
  tryCatchConfig?: TryCatchConfig;
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

export interface ExecutionEvent {
  seq: number;
  type: string;
  fromState?: string | null;
  toState?: string | null;
  nodeId?: string | null;
  attempt?: number | null;
  generation?: number | null;
  payload: Record<string, any> & {
    generation?: number;
    executorGeneration?: number;
    allowedActions?: CommandType[];
  };
  timestamp: number;
}

export interface ExecutionSnapshot {
  executionId: string;
  flowId: string;
  status: ExecutionStatus;
  currentNodeId: string | null;
  resumeFromNodeId: string | null;
  variables: Record<string, any>;
  loopCounts: Record<string, number>;
  generation: number;
  executorGeneration?: number;
  latestSeq: number;
  allowedActions: CommandType[];
  flowVersion?: number | null;
  nodeConfigHash?: string | null;
  pendingApproval?: PendingApproval | null;
  createdAt: number;
  updatedAt: number;
  finishedAt: number | null;
  error: string | null;
}

export interface ExecutionState {
  flowId: string;
  status: ExecutionStatus;
  currentNodeId: string | null;
  variables: Record<string, any>;
  trace: TraceLog[];
  loopCounts: Record<string, number>;
  snapshots: Record<string, Record<string, any>>;
  allowedActions: CommandType[];
  executionId: string | null;
  latestSeq: number;
  generation: number;
  executorGeneration?: number;
  pendingApproval?: PendingApproval | null;
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
  | { type: 'execute'; flow: FlowDefinition; variables?: Record<string, any>; commandId?: string }
  | { type: 'subscribe'; executionId: string; sinceSeq?: number }
  | { type: 'command'; command: CommandType; executionId?: string; commandId?: string; token?: string; approver?: string; comment?: string }
  | { type: 'pause'; commandId?: string }
  | { type: 'resume'; commandId?: string }
  | { type: 'stop'; commandId?: string }
  | { type: 'step' }
  | { type: 'setVariable'; name: string; value: any }
  | { type: 'setBreakpoint'; nodeId: string; enabled: boolean }
  | { type: 'stepInto' }
  | { type: 'stepOut' }
  | { type: 'evaluate'; expression: string }
  | { type: 'ping' };

export type ServerMessage =
  | { type: 'snapshot'; snapshot: ExecutionSnapshot }
  | { type: 'event'; event: ExecutionEvent; seq?: number }
  | { type: 'commandResult'; command: string; commandId?: string; accepted: boolean; reason?: string }
  | { type: 'paused' }
  | { type: 'error'; message: string }
  | { type: 'pong' }
  | { type: 'nodeEnter'; nodeId: string; variables: Record<string, any>; callDepth?: number }
  | { type: 'nodeExit'; nodeId: string; variables: Record<string, any>; callDepth?: number }
  | { type: 'nodeError'; nodeId: string; error: string; variables: Record<string, any>; callDepth?: number }
  | { type: 'status'; status: ExecutionStatus; variables: Record<string, any> }
  | { type: 'trace'; log: TraceLog }
  | { type: 'completed'; variables: Record<string, any>; trace: TraceLog[] }
  | { type: 'breakpointUpdated'; nodeId: string; enabled: boolean; breakpoints: string[] }
  | { type: 'breakpointHit'; nodeId: string; variables: Record<string, any>; callDepth: number }
  | { type: 'debugPaused'; reason: string; nodeId: string; callDepth: number; variables: Record<string, any> }
  | { type: 'evaluateResult'; expression: string; result?: any; error?: string; success: boolean }
  | { type: 'approval_requested'; approvalId: string; executionId: string; nodeId: string; token: string; prompt: string; deadline: number; generation: number; flowVersion?: number | null; branchId?: string | null; attempt: number }
  | { type: 'approval_response'; executionId: string; approvalId: string; status: 'approved' | 'rejected'; approver?: string; comment?: string };
