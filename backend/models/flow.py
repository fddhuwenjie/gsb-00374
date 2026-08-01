from typing import Literal, Optional, Dict, Any, List
from pydantic import BaseModel, Field

NodeType = Literal['start', 'end', 'task', 'condition', 'loop', 'wait', 'http', 'sql', 'parallel', 'subflow', 'trycatch']
ExecutionStatus = Literal['idle', 'running', 'paused', 'stopped', 'completed', 'error']
TraceAction = Literal['enter', 'exit', 'error']
EdgeHandle = Literal['true', 'false', 'loop', 'catch']
BackoffType = Literal['fixed', 'exponential']
HttpMethod = Literal['GET', 'POST', 'PUT', 'DELETE', 'PATCH']
TriggerType = Literal['cron', 'webhook', 'flow_completed']


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


class ParallelConfig(BaseModel):
    branchNodeIds: List[str]


class SubflowConfig(BaseModel):
    subflowId: str


class TryCatchConfig(BaseModel):
    tryNodeIds: List[str]
    catchNodeIds: List[str]


class NodeData(BaseModel):
    label: str
    code: Optional[str] = None
    expression: Optional[str] = None
    seconds: Optional[float] = None
    anchorId: Optional[str] = None
    retry: Optional[RetryConfig] = None
    httpConfig: Optional[HttpConfig] = None
    sqlConfig: Optional[SqlConfig] = None
    parallelConfig: Optional[ParallelConfig] = None
    subflowConfig: Optional[SubflowConfig] = None
    tryCatchConfig: Optional[TryCatchConfig] = None
    breakpoint: Optional[bool] = None


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


class TraceLog(BaseModel):
    timestamp: float
    nodeId: str
    nodeType: NodeType
    action: TraceAction
    variables: Dict[str, Any] = Field(default_factory=dict)
    message: Optional[str] = None


class ExecutionState(BaseModel):
    flowId: str
    status: ExecutionStatus
    currentNodeId: Optional[str] = None
    variables: Dict[str, Any] = Field(default_factory=dict)
    trace: List[TraceLog] = Field(default_factory=list)
    loopCounts: Dict[str, int] = Field(default_factory=dict)
    snapshots: Dict[str, Dict[str, Any]] = Field(default_factory=dict)


class Execution(BaseModel):
    id: str
    flowId: str
    status: ExecutionStatus
    startedAt: float
    finishedAt: Optional[float] = None
    variables: Dict[str, Any] = Field(default_factory=dict)
    trace: List[TraceLog] = Field(default_factory=list)
    snapshots: Dict[str, Dict[str, Any]] = Field(default_factory=dict)


class Trigger(BaseModel):
    id: str
    flowId: str
    type: TriggerType
    cronExpression: Optional[str] = None
    webhookPath: Optional[str] = None
    sourceFlowId: Optional[str] = None
    enabled: bool
    createdAt: float
