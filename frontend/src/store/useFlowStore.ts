import { create } from 'zustand';
import type {
  FlowNode,
  FlowEdge,
  FlowDefinition,
  ExecutionState,
  ExecutionStatus,
  TraceLog,
  NodeType,
  ExecutionSnapshot,
  ExecutionEvent,
  CommandType,
} from '../types/flow';

interface FlowStore {
  nodes: FlowNode[];
  edges: FlowEdge[];
  selectedNodeId: string | null;
  executionState: ExecutionState;
  activeNodeId: string | null;
  flowName: string;
  flowId: string;
  wsConnected: boolean;
  errorMessage: string | null;
  flows: FlowDefinition[];

  setNodes: (nodes: FlowNode[]) => void;
  setEdges: (edges: FlowEdge[]) => void;
  addNode: (node: FlowNode) => void;
  updateNode: (id: string, data: Partial<FlowNode>) => void;
  updateNodeData: (id: string, data: Partial<FlowNode['data']>) => void;
  deleteNode: (id: string) => void;
  setSelectedNodeId: (id: string | null) => void;
  setActiveNodeId: (id: string | null) => void;
  setFlowName: (name: string) => void;
  setWsConnected: (connected: boolean) => void;
  setErrorMessage: (message: string | null) => void;
  fetchFlows: () => Promise<void>;

  applySnapshot: (snapshot: ExecutionSnapshot) => void;
  applyEvent: (event: ExecutionEvent) => void;
  updateExecutionStatus: (status: ExecutionStatus, variables?: Record<string, any>) => void;
  updateVariables: (variables: Record<string, any>) => void;
  addTraceLog: (log: TraceLog) => void;
  setVariable: (name: string, value: any) => void;
  resetExecution: () => void;

  getFlowDefinition: () => FlowDefinition;
  loadFlowDefinition: (flow: FlowDefinition) => void;
  clearFlow: () => void;
}

const generateId = (): string => {
  return `node_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
};

const initialExecutionState: ExecutionState = {
  flowId: '',
  status: 'idle',
  currentNodeId: null,
  variables: {},
  trace: [],
  loopCounts: {},
  snapshots: {},
  allowedActions: [],
  executionId: null,
  latestSeq: 0,
  generation: 0,
  executorGeneration: 0,
  pendingApproval: null,
};

export const useFlowStore = create<FlowStore>((set, get) => ({
  nodes: [],
  edges: [],
  selectedNodeId: null,
  executionState: initialExecutionState,
  activeNodeId: null,
  flowName: 'Untitled Flow',
  flowId: generateId(),
  wsConnected: false,
  errorMessage: null,
  flows: [],

  setNodes: (nodes) => set({ nodes }),
  setEdges: (edges) => set({ edges }),

  addNode: (node) =>
    set((state) => ({
      nodes: [...state.nodes, node],
    })),

  updateNode: (id, data) =>
    set((state) => ({
      nodes: state.nodes.map((n) =>
        n.id === id ? { ...n, ...data } : n
      ),
    })),

  updateNodeData: (id, data) =>
    set((state) => ({
      nodes: state.nodes.map((n) =>
        n.id === id ? { ...n, data: { ...n.data, ...data } } : n
      ),
    })),

  deleteNode: (id) =>
    set((state) => ({
      nodes: state.nodes.filter((n) => n.id !== id),
      edges: state.edges.filter((e) => e.source !== id && e.target !== id),
      selectedNodeId: state.selectedNodeId === id ? null : state.selectedNodeId,
    })),

  setSelectedNodeId: (id) => set({ selectedNodeId: id }),
  setActiveNodeId: (id) => set({ activeNodeId: id }),
  setFlowName: (name) => set({ flowName: name }),
  setWsConnected: (connected) => set({ wsConnected: connected }),
  setErrorMessage: (message) => set({ errorMessage: message }),

  fetchFlows: async () => {
    try {
      const response = await fetch('/api/flows');
      if (response.ok) {
        const flows = await response.json();
        set({ flows });
      }
    } catch (error) {
      console.error('Failed to fetch flows:', error);
    }
  },

  applySnapshot: (snapshot) =>
    set((state) => {
      const nextSeq = Math.max(state.executionState.latestSeq, snapshot.latestSeq);
      if (snapshot.latestSeq < state.executionState.latestSeq && snapshot.executionId === state.executionState.executionId) {
        return state;
      }
      return {
        activeNodeId: snapshot.currentNodeId,
        executionState: {
          ...state.executionState,
          flowId: snapshot.flowId,
          status: snapshot.status,
          currentNodeId: snapshot.currentNodeId,
          variables: snapshot.variables,
          loopCounts: snapshot.loopCounts,
          allowedActions: snapshot.allowedActions as CommandType[],
          executionId: snapshot.executionId,
          latestSeq: nextSeq,
          generation: snapshot.generation,
          executorGeneration: snapshot.executorGeneration ?? 0,
          pendingApproval: snapshot.pendingApproval ?? null,
        },
      };
    }),

  applyEvent: (event) =>
    set((state) => {
      if (event.seq <= state.executionState.latestSeq) {
        return state;
      }
      const updates: Partial<ExecutionState> = {
        latestSeq: event.seq,
      };
      if (event.toState) {
        updates.status = event.toState as ExecutionStatus;
        if (event.toState !== 'awaiting_approval') {
          updates.pendingApproval = null;
        }
      }
      if (event.payload?.allowedActions) {
        updates.allowedActions = event.payload.allowedActions as CommandType[];
      }
      if (event.payload?.generation !== undefined) {
        updates.generation = event.payload.generation;
      }
      if (event.payload?.executorGeneration !== undefined) {
        updates.executorGeneration = event.payload.executorGeneration;
      }
      if (event.nodeId) {
        updates.currentNodeId = event.nodeId;
      }
      const payloadVars = event.payload?.variables;
      if (payloadVars && typeof payloadVars === 'object') {
        updates.variables = payloadVars;
      }
      return {
        executionState: { ...state.executionState, ...updates },
      };
    }),

  updateExecutionStatus: (status, variables) =>
    set((state) => ({
      executionState: {
        ...state.executionState,
        status,
        variables: variables ?? state.executionState.variables,
      },
    })),

  updateVariables: (variables) =>
    set((state) => ({
      executionState: {
        ...state.executionState,
        variables,
      },
    })),

  addTraceLog: (log) =>
    set((state) => ({
      executionState: {
        ...state.executionState,
        trace: [...state.executionState.trace, log],
      },
    })),

  setVariable: (name, value) =>
    set((state) => ({
      executionState: {
        ...state.executionState,
        variables: {
          ...state.executionState.variables,
          [name]: value,
        },
      },
    })),

  resetExecution: () =>
    set({
      executionState: { ...initialExecutionState, flowId: get().flowId },
      activeNodeId: null,
      errorMessage: null,
    }),

  getFlowDefinition: () => {
    const state = get();
    const now = Date.now();
    return {
      id: state.flowId,
      name: state.flowName,
      nodes: state.nodes,
      edges: state.edges,
      createdAt: now,
      updatedAt: now,
    };
  },

  loadFlowDefinition: (flow) =>
    set({
      flowId: flow.id,
      flowName: flow.name,
      nodes: flow.nodes,
      edges: flow.edges,
      executionState: { ...initialExecutionState, flowId: flow.id },
      selectedNodeId: null,
      activeNodeId: null,
    }),

  clearFlow: () =>
    set({
      nodes: [],
      edges: [],
      selectedNodeId: null,
      executionState: { ...initialExecutionState, flowId: '' },
      activeNodeId: null,
      flowName: 'Untitled Flow',
      flowId: generateId(),
    }),
}));

export const generateNodeId = generateId;

export const createNewNode = (
  type: NodeType,
  position: { x: number; y: number }
): FlowNode => {
  const id = generateId();
  const labels: Record<NodeType, string> = {
    start: 'Start',
    end: 'End',
    task: 'Task',
    condition: 'Condition',
    loop: 'Loop',
    wait: 'Wait',
    http: 'HTTP',
    sql: 'SQL',
    file_write: 'File Write',
    approval: 'Approval',
    parallel: 'Parallel',
    subflow: 'Subflow',
    trycatch: 'TryCatch',
  };

  const data: FlowNode['data'] = {
    label: labels[type],
    breakpoint: false,
  };

  if (type === 'task') {
    data.code = '# Write your Python code here\n# Access variables via ctx["name"]\n';
  }
  if (type === 'condition') {
    data.expression = 'ctx.x > 0';
  }
  if (type === 'loop') {
    data.expression = 'ctx.i < 10';
  }
  if (type === 'wait') {
    data.seconds = 1;
  }
  if (type === 'http') {
    data.httpConfig = {
      url: 'https://api.example.com',
      method: 'GET',
      headers: {},
      body: '',
      timeout: 30,
    };
  }
  if (type === 'sql') {
    data.sqlConfig = {
      connectionString: 'sqlite:///data.db',
      query: 'SELECT * FROM table_name',
      params: [],
    };
  }
  if (type === 'file_write') {
    data.fileWriteConfig = {
      path: 'output.txt',
      content: '',
      mode: 'w',
    };
  }
  if (type === 'approval') {
    data.approvalConfig = {
      prompt: 'Please approve this step',
      approvers: [],
      timeoutSeconds: 3600,
    };
  }
  if (type === 'parallel') {
    data.parallelConfig = {
      branchNodeIds: [],
    };
  }
  if (type === 'subflow') {
    data.subflowConfig = {
      subflowId: '',
    };
  }
  if (type === 'trycatch') {
    data.tryCatchConfig = {
      tryNodeIds: [],
      catchNodeIds: [],
    };
  }

  return {
    id,
    type,
    position,
    data,
  };
};
