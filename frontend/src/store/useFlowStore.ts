import { create } from 'zustand';
import type {
  FlowNode,
  FlowEdge,
  FlowDefinition,
  ExecutionState,
  ExecutionStatus,
  TraceLog,
  NodeType,
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
      executionState: { ...initialExecutionState, flowId: get().flowId, snapshots: {} },
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
      executionState: { ...initialExecutionState, flowId: flow.id, snapshots: {} },
      selectedNodeId: null,
      activeNodeId: null,
    }),

  clearFlow: () =>
    set({
      nodes: [],
      edges: [],
      selectedNodeId: null,
      executionState: { ...initialExecutionState, flowId: '', snapshots: {} },
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
