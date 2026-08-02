export type NodeType = 'start' | 'end' | 'task' | 'condition' | 'loop' | 'wait' | 'http' | 'sql' | 'file' | 'parallel' | 'subflow' | 'trycatch' | 'approval';
export type ExecutionStatus = 'queued' | 'running' | 'pausing' | 'paused' | 'retry_wait' | 'awaiting_approval' | 'succeeded' | 'failed' | 'cancelled';
export type TraceAction = 'enter' | 'exit' | 'error';
export type EdgeHandle = 'true' | 'false' | 'loop' | 'catch';
export type ControlAction = 'pause' | 'resume' | 'cancel' | 'step';

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
  body?: string;
  timeout?: number;
}

export interface SqlConfig {
  connectionString: string;
  query: string;
  params: any[];
}

export interface FileWriteConfig {
  path: string;
  content: string;
  mode: 'write' | 'append';
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

export interface ApprovalConfig {
  approvers: string[];
  timeoutSeconds?: number;
  description?: string;
}

export type ApprovalOutcome = 'pending' | 'approved' | 'rejected' | 'expired' | 'cancelled';

export interface ApprovalRequest {
  token: string;
  executionId: string;
  nodeId: string;
  attempt: number;
  flowVersion: number;
  generation: number;
  approvers: string[];
  description?: string;
  createdAt: number;
  deadline: number;
  status: ApprovalOutcome;
  respondedBy?: string;
  respondedAt?: number;
  comment?: string;
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
  fileConfig?: FileWriteConfig;
  parallelConfig?: ParallelConfig;
  subflowConfig?: SubflowConfig;
  tryCatchConfig?: TryCatchConfig;
  approvalConfig?: ApprovalConfig;
  breakpoint?: boolean;
  interruptible?: boolean;
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

export interface StateEvent {
  seq: number;
  executionId: string;
  timestamp: number;
  eventType: string;
  fromState?: ExecutionStatus;
  toState?: ExecutionStatus;
  nodeId?: string;
  attempt?: number;
  generation?: number;
  idempotencyKey?: string;
  requestId?: string;
  payload: Record<string, any>;
}

export interface ExecutionSnapshot {
  executionId: string;
  flowId: string;
  flowVersion: number;
  nodeConfigHash: string;
  status: ExecutionStatus;
  seq: number;
  timestamp: number;
  currentNodeId?: string | null;
  resumeFromNodeId?: string | null;
  variables: Record<string, any>;
  trace: TraceLog[];
  loopCounts: Record<string, number>;
  nodeAttempts: Record<string, number>;
  generation: number;
  completedNodes: string[];
  pauseRequested: boolean;
  cancelRequested: boolean;
  allowedActions: ControlAction[];
  retryUntil?: number | null;
  retryNodeId?: string | null;
  retryAttempt?: number | null;
  retryDelay?: number | null;
  parallelBranches: Record<string, any>;
  lastError?: string | null;
  stepMode: boolean;
  pendingApproval?: ApprovalRequest | null;
}

export interface ExecutionState {
  flowId: string;
  executionId: string | null;
  status: ExecutionStatus | 'idle';
  seq: number;
  currentNodeId: string | null;
  variables: Record<string, any>;
  trace: TraceLog[];
  loopCounts: Record<string, number>;
  allowedActions: ControlAction[];
  completedNodes: string[];
  lastError: string | null;
  flowVersion: number;
  pendingApproval: ApprovalRequest | null;
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

export interface ClientMessage {
  type: 'execute' | 'subscribe' | 'pause' | 'resume' | 'cancel' | 'step' | 'stepInto' | 'stepOut' | 'setVariable' | 'setBreakpoint' | 'evaluate' | 'approve' | 'reject';
  flow?: FlowDefinition;
  variables?: Record<string, any>;
  executionId?: string;
  requestId?: string;
  afterSeq?: number;
  name?: string;
  value?: any;
  nodeId?: string;
  enabled?: boolean;
  expression?: string;
  token?: string;
  responder?: string;
  comment?: string;
}

export interface ServerMessage {
  type: 'subscribed' | 'event' | 'commandResult' | 'snapshot' | 'error' | 'breakpointUpdated' | 'evaluateResult';
  executionId?: string;
  snapshot?: ExecutionSnapshot;
  event?: StateEvent;
  command?: string;
  accepted?: boolean;
  reason?: string;
  status?: string;
  requestId?: string;
  message?: string;
  nodeId?: string;
  enabled?: boolean;
  breakpoints?: string[];
  expression?: string;
  result?: any;
  error?: string;
  success?: boolean;
  token?: string;
  decision?: string;
  allowedActions?: ControlAction[];
}
