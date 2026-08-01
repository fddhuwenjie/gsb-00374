export type NodeType = 'start' | 'end' | 'task' | 'condition' | 'loop' | 'wait' | 'http' | 'sql' | 'parallel' | 'subflow' | 'trycatch';
export type ExecutionStatus = 'idle' | 'running' | 'paused' | 'stopped' | 'completed' | 'error';
export type TraceAction = 'enter' | 'exit' | 'error';
export type EdgeHandle = 'true' | 'false' | 'loop' | 'catch';

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
