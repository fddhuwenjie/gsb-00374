import time
from typing import Literal, Optional, Dict, Any, List
from pydantic import BaseModel, Field

NodeType = Literal['start', 'end', 'task', 'condition', 'loop', 'wait', 'http', 'sql', 'file', 'parallel', 'subflow', 'trycatch', 'approval']
ExecutionStatus = Literal[
    'idle', 'stopped', 'completed', 'error',
    'queued', 'running', 'pausing', 'paused',
    'retry_wait', 'awaiting_approval',
    'succeeded', 'failed', 'cancelled'
]
TraceAction = Literal['enter', 'exit', 'error']
EdgeHandle = Literal['true', 'false', 'loop', 'catch']
BackoffType = Literal['fixed', 'exponential']
HttpMethod = Literal['GET', 'POST', 'PUT', 'DELETE', 'PATCH']
TriggerType = Literal['cron', 'webhook', 'flow_completed']
EventType = Literal[
    'transition', 'nodeEnter', 'nodeExit', 'nodeError',
    'trace', 'sideEffect', 'snapshot', 'recover',
    'command', 'parallelJoin', 'queued', 'checkpoint',
    'approvalRequested', 'approvalResponded', 'approvalExpired', 'approvalCancelled'
]
ControlAction = Literal['pause', 'resume', 'cancel', 'step', 'approve', 'reject']


class Position(BaseModel):
    x: float
    y: float


class RetryConfig(BaseModel):
    maxAttempts: int
    delaySeconds: float
    backoff: BackoffType
    maxDelaySeconds: float


class HttpConfig(BaseModel):
    url: str
    method: HttpMethod
    headers: Dict[str, str] = Field(default_factory=dict)
    body: Optional[str] = None
    timeout: Optional[float] = None


class SqlConfig(BaseModel):
    connectionString: str
    query: str
    params: List[Any] = Field(default_factory=list)


class FileWriteConfig(BaseModel):
    path: str
    content: str
    mode: Literal['write', 'append'] = 'write'


class ParallelConfig(BaseModel):
    branchNodeIds: List[str]


class SubflowConfig(BaseModel):
    subflowId: str


class TryCatchConfig(BaseModel):
    tryNodeIds: List[str]
    catchNodeIds: List[str]


class ApprovalConfig(BaseModel):
    approvers: List[str] = Field(default_factory=list)
    timeoutSeconds: Optional[float] = 600
    description: Optional[str] = None


class NodeData(BaseModel):
    label: str
    code: Optional[str] = None
    expression: Optional[str] = None
    seconds: Optional[float] = None
    anchorId: Optional[str] = None
    retry: Optional[RetryConfig] = None
    httpConfig: Optional[HttpConfig] = None
    sqlConfig: Optional[SqlConfig] = None
    fileConfig: Optional[FileWriteConfig] = None
    parallelConfig: Optional[ParallelConfig] = None
    subflowConfig: Optional[SubflowConfig] = None
    tryCatchConfig: Optional[TryCatchConfig] = None
    approvalConfig: Optional[ApprovalConfig] = None
    breakpoint: Optional[bool] = None
    interruptible: Optional[bool] = None


class FlowNode(BaseModel):
    id: str
    type: NodeType
    position: Position
    data: NodeData


class FlowEdge(BaseModel):
    id: str
    source: str
    target: str
    sourceHandle: Optional[EdgeHandle] = None


class FlowDefinition(BaseModel):
    id: str
    name: str
    nodes: List[FlowNode]
    edges: List[FlowEdge]
    createdAt: float
    updatedAt: float
    version: Optional[int] = None


class FlowVersionMeta(BaseModel):
    version: int
    flowId: str
    name: str
    createdAt: float
    nodeConfigHash: str
    comment: Optional[str] = None


class FlowVersion(BaseModel):
    meta: FlowVersionMeta
    definition: FlowDefinition


class FlowDiff(BaseModel):
    addedNodes: List[str] = Field(default_factory=list)
    removedNodes: List[str] = Field(default_factory=list)
    changedNodes: List[str] = Field(default_factory=list)
    addedEdges: List[str] = Field(default_factory=list)
    removedEdges: List[str] = Field(default_factory=list)
    changedEdges: List[str] = Field(default_factory=list)
    renamed: bool = False
    nameBefore: Optional[str] = None
    nameAfter: Optional[str] = None


class TraceLog(BaseModel):
    timestamp: float
    nodeId: str
    nodeType: NodeType
    action: TraceAction
    variables: Dict[str, Any] = Field(default_factory=dict)
    message: Optional[str] = None


class ExecutionState(BaseModel):
    flowId: str
    status: ExecutionStatus = 'queued'
    currentNodeId: Optional[str] = None
    variables: Dict[str, Any] = Field(default_factory=dict)
    trace: List[TraceLog] = Field(default_factory=list)
    loopCounts: Dict[str, int] = Field(default_factory=dict)
    snapshots: Dict[str, Dict[str, Any]] = Field(default_factory=dict)


class StateEvent(BaseModel):
    seq: int
    executionId: str
    timestamp: float
    eventType: EventType
    fromState: Optional[ExecutionStatus] = None
    toState: Optional[ExecutionStatus] = None
    nodeId: Optional[str] = None
    attempt: Optional[int] = None
    generation: Optional[int] = None
    idempotencyKey: Optional[str] = None
    requestId: Optional[str] = None
    payload: Dict[str, Any] = Field(default_factory=dict)


class ControlCommand(BaseModel):
    command: ControlAction
    requestId: str
    executionId: str
    timestamp: float = Field(default_factory=time.time)


class NodeCheckpoint(BaseModel):
    nodeId: str
    variables: Dict[str, Any] = Field(default_factory=dict)
    loopCounts: Dict[str, int] = Field(default_factory=dict)
    attempt: int = 0
    completedNodes: List[str] = Field(default_factory=list)
    generation: int = 0


class ExecutionSnapshot(BaseModel):
    executionId: str
    flowId: str
    flowVersion: int = 1
    nodeConfigHash: str = ''
    status: ExecutionStatus
    seq: int
    timestamp: float
    currentNodeId: Optional[str] = None
    resumeFromNodeId: Optional[str] = None
    variables: Dict[str, Any] = Field(default_factory=dict)
    trace: List[TraceLog] = Field(default_factory=list)
    loopCounts: Dict[str, int] = Field(default_factory=dict)
    nodeAttempts: Dict[str, int] = Field(default_factory=dict)
    generation: int = 0
    completedNodes: List[str] = Field(default_factory=list)
    pauseRequested: bool = False
    cancelRequested: bool = False
    allowedActions: List[ControlAction] = Field(default_factory=list)
    retryUntil: Optional[float] = None
    retryNodeId: Optional[str] = None
    retryAttempt: Optional[int] = None
    retryDelay: Optional[float] = None
    parallelBranches: Dict[str, Any] = Field(default_factory=dict)
    lastError: Optional[str] = None
    stepMode: bool = False
    pendingApproval: Optional['ApprovalRequest'] = None


class SideEffectRecord(BaseModel):
    key: str
    executionId: str
    nodeId: str
    attempt: int
    generation: int
    timestamp: float
    result: Any = None


ApprovalOutcome = Literal['pending', 'approved', 'rejected', 'expired', 'cancelled']


class ApprovalRequest(BaseModel):
    """A pending approval request bound to a specific execution, node,
    attempt, flow version and (for parallel branches) generation."""
    token: str
    executionId: str
    nodeId: str
    attempt: int
    flowVersion: int
    generation: int = 0
    approvers: List[str] = Field(default_factory=list)
    description: Optional[str] = None
    createdAt: float
    deadline: float
    status: ApprovalOutcome = 'pending'
    respondedBy: Optional[str] = None
    respondedAt: Optional[float] = None
    comment: Optional[str] = None


class ApprovalResponse(BaseModel):
    token: str
    executionId: str
    decision: Literal['approved', 'rejected']
    responder: Optional[str] = None
    comment: Optional[str] = None
    requestId: Optional[str] = None


class PersistedExecutionRecord(BaseModel):
    executionId: str
    flowId: str
    flow: FlowDefinition
    flowVersion: int = 1
    nodeConfigHash: str = ''
    status: ExecutionStatus = 'queued'
    seq: int = 0
    snapshot: ExecutionSnapshot
    sideEffects: Dict[str, SideEffectRecord] = Field(default_factory=dict)
    approvals: List['ApprovalRequest'] = Field(default_factory=list)
    seenRequestIds: List[str] = Field(default_factory=list)
    createdAt: float = Field(default_factory=time.time)
    updatedAt: float = Field(default_factory=time.time)


class ExecutionMeta(BaseModel):
    executionId: str
    flowId: str
    flow: FlowDefinition
    initialVariables: Dict[str, Any] = Field(default_factory=dict)
    createdAt: float


class Execution(BaseModel):
    id: str
    flowId: str
    status: ExecutionStatus
    startedAt: float
    finishedAt: Optional[float] = None
    variables: Dict[str, Any] = Field(default_factory=dict)
    trace: List[Any] = Field(default_factory=list)
    snapshots: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    seq: int = 0


class Trigger(BaseModel):
    id: str
    flowId: str
    type: TriggerType
    cronExpression: Optional[str] = None
    webhookPath: Optional[str] = None
    sourceFlowId: Optional[str] = None
    enabled: bool
    createdAt: float


ExecutionSnapshot.model_rebuild()
PersistedExecutionRecord.model_rebuild()
