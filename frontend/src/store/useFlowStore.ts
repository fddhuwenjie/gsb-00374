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
  StateEvent,
  ControlAction,
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
  applyEvent: (event: StateEvent) => void;
  setExecutionId: (id: string | null) => void;
  updateExecutionStatus: (status: ExecutionStatus | 'idle', variables?: Record<string, any>) => void;
  updateVariables: (variables: Record<string, any>) => void;
  addTraceLog: (log: TraceLog) => void;
  setVariable: (name: string, value: any) => void;
  resetExecution: () => void;
  isActionAllowed: (action: ControlAction) => boolean;

  getFlowDefinition: () => FlowDefinition;
  loadFlowDefinition: (flow: FlowDefinition) => void;
  clearFlow: () => void;
}

const generateId = (): string => {
  return `node_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
};

const initialExecutionState: ExecutionState = {
  flowId: '',
  executionId: null,
  status: 'idle',
  seq: 0,
  currentNodeId: null,
  variables: {},
  trace: [],
  loopCounts: {},
  allowedActions: [],
  completedNodes: [],
  lastError: null,
  flowVersion: 1,
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
    set((state) => ({
      executionState: {
        ...state.executionState,
        executionId: snapshot.executionId,
        flowId: snapshot.flowId,
        status: snapshot.status,
        seq: snapshot.seq,
        currentNodeId: snapshot.currentNodeId ?? null,
        variables: { ...snapshot.variables },
        trace: [...snapshot.trace],
        loopCounts: { ...snapshot.loopCounts },
        allowedActions: [...snapshot.allowedActions],
        completedNodes: [...snapshot.completedNodes],
        lastError: snapshot.lastError ?? null,
        flowVersion: snapshot.flowVersion ?? 1,
        pendingApproval: snapshot.pendingApproval ?? null,
      },
      activeNodeId: snapshot.currentNodeId ?? state.activeNodeId,
    })),

  applyEvent: (event) =>
    set((state) => {
      if (event.seq <= state.executionState.seq) {
        return state;
      }

      const next: ExecutionState = {
        ...state.executionState,
        seq: event.seq,
      };

      if (event.toState) {
        next.status = event.toState;
      }

      if (event.eventType === 'nodeEnter' && event.nodeId) {
        next.currentNodeId = event.nodeId;
        return { executionState: next, activeNodeId: event.nodeId };
      }

      if (event.eventType === 'nodeExit' && event.nodeId) {
        if (event.payload?.next) {
          next.currentNodeId = event.payload.next as string;
        }
      }

      if (event.eventType === 'nodeError' && event.nodeId) {
        next.currentNodeId = event.nodeId;
        next.lastError = event.payload?.error ?? 'Unknown error';
        return { executionState: next, activeNodeId: event.nodeId };
      }

      if (event.eventType === 'approvalRequested') {
        next.pendingApproval = {
          token: event.payload?.token,
          executionId: event.executionId,
          nodeId: event.nodeId ?? '',
          attempt: event.attempt ?? 1,
          flowVersion: event.payload?.flowVersion ?? 1,
          generation: event.generation ?? 0,
          approvers: event.payload?.approvers ?? [],
          description: event.payload?.description,
          createdAt: event.timestamp,
          deadline: event.payload?.deadline ?? 0,
          status: 'pending',
        };
      }

      if (event.eventType === 'approvalResponded' || event.eventType === 'approvalExpired' || event.eventType === 'approvalCancelled') {
        if (next.pendingApproval && next.pendingApproval.token === event.payload?.token) {
          next.pendingApproval = null;
        }
      }

      if (event.eventType === 'trace') {
        return state;
      }

      if (event.eventType === 'transition' && event.toState) {
        const terminalStates = ['succeeded', 'failed', 'cancelled'];
        if (terminalStates.includes(event.toState)) {
          next.pendingApproval = null;
          return {
            executionState: next,
            activeNodeId: null,
          };
        }
        if (event.toState !== 'awaiting_approval') {
          next.pendingApproval = state.executionState.pendingApproval;
        }
      }

      return { executionState: next };
    }),

  setExecutionId: (id) =>
    set((state) => ({
      executionState: { ...state.executionState, executionId: id },
    })),

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

  isActionAllowed: (action) =>
    get().executionState.allowedActions.includes(action),

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
    file: 'File',
    parallel: 'Parallel',
    subflow: 'Subflow',
    trycatch: 'TryCatch',
    approval: 'Approval',
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
  if (type === 'file') {
    data.fileConfig = {
      path: 'output.txt',
      content: '',
      mode: 'write',
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
  if (type === 'approval') {
    data.approvalConfig = {
      approvers: [],
      timeoutSeconds: 600,
      description: '',
    };
  }

  return {
    id,
    type,
    position,
    data,
  };
};
