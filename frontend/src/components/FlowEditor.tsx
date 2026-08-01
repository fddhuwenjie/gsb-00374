import React, { useCallback, useRef, useMemo } from 'react';
import ReactFlow, {
  Background,
  Controls,
  MiniMap,
  addEdge,
  useNodesState,
  useEdgesState,
  BackgroundVariant,
  type Connection,
  type Edge,
  type Node,
  type ReactFlowInstance,
} from 'reactflow';
import 'reactflow/dist/style.css';
import { useFlowStore, createNewNode, generateNodeId } from '../store/useFlowStore';
import {
  StartNode,
  EndNode,
  TaskNode,
  ConditionNode,
  LoopNode,
  WaitNode,
  HttpNode,
  SqlNode,
  ParallelNode,
  SubflowNode,
  TryCatchNode,
} from './nodes';
import type { NodeType, FlowEdge } from '../types/flow';
import { handleColors, generateEdgeId } from '../utils/flowUtils';

const nodeTypes = {
  start: StartNode,
  end: EndNode,
  task: TaskNode,
  condition: ConditionNode,
  loop: LoopNode,
  wait: WaitNode,
  http: HttpNode,
  sql: SqlNode,
  parallel: ParallelNode,
  subflow: SubflowNode,
  trycatch: TryCatchNode,
};

interface FlowEditorProps {
  onDragStart: (event: React.DragEvent, nodeType: NodeType) => void;
  onDragOver: (event: React.DragEvent) => void;
  onDrop: (event: React.DragEvent) => void;
}

export const FlowEditor: React.FC<FlowEditorProps> = ({
  onDragStart,
  onDragOver,
  onDrop,
}) => {
  const reactFlowWrapper = useRef<HTMLDivElement>(null);
  const [reactFlowInstance, setReactFlowInstance] = React.useState<ReactFlowInstance | null>(null);

  const {
    nodes: storeNodes,
    edges: storeEdges,
    selectedNodeId,
    activeNodeId,
    setNodes,
    setEdges,
    addNode,
    updateNode,
    deleteNode,
    setSelectedNodeId,
  } = useFlowStore();

  const nodesWithActive = useMemo(
    () =>
      storeNodes.map((node) => ({
        ...node,
        data: {
          ...node.data,
          isActive: node.id === activeNodeId,
        },
        style: {
          opacity: activeNodeId && node.id !== activeNodeId ? 0.6 : 1,
          transition: 'opacity 0.2s',
        },
      })),
    [storeNodes, activeNodeId]
  );

  const edgesWithAnimation = useMemo(
    () =>
      storeEdges.map((edge) => ({
        ...edge,
        animated: activeNodeId !== null,
        style: {
          stroke: edge.sourceHandle ? handleColors[edge.sourceHandle] : '#6b7280',
          strokeWidth: 2,
        },
        markerEnd: {
          type: 'arrowclosed' as any,
          color: edge.sourceHandle ? handleColors[edge.sourceHandle] : '#6b7280',
        },
      })),
    [storeEdges, activeNodeId]
  );

  const [nodes, setNodesState, onNodesChange] = useNodesState(nodesWithActive as any);
  const [edges, setEdgesState, onEdgesChange] = useEdgesState(edgesWithAnimation as any);

  React.useEffect(() => {
    setNodesState(nodesWithActive);
  }, [nodesWithActive, setNodesState]);

  React.useEffect(() => {
    setEdgesState(edgesWithAnimation as any);
  }, [edgesWithAnimation, setEdgesState]);

  const onConnect = useCallback(
    (params: Connection) => {
      const newEdge: FlowEdge = {
        id: generateEdgeId(params.source || '', params.target || '', params.sourceHandle || undefined),
        source: params.source || '',
        target: params.target || '',
        sourceHandle: params.sourceHandle as 'true' | 'false' | 'loop' | undefined,
      };
      setEdgesState((eds) => addEdge(newEdge as any, eds));
    },
    [setEdgesState]
  );

  const onInit = useCallback((instance: ReactFlowInstance) => {
    setReactFlowInstance(instance);
  }, []);

  const handleDrop = useCallback(
    (event: React.DragEvent) => {
      event.preventDefault();

      const type = event.dataTransfer.getData('application/reactflow') as NodeType;
      if (!type || !reactFlowWrapper.current) {
        return;
      }

      const bounds = reactFlowWrapper.current.getBoundingClientRect();
      const position = reactFlowInstance?.screenToFlowPosition({
        x: event.clientX - bounds.left,
        y: event.clientY - bounds.top,
      }) || { x: 0, y: 0 };

      const newNode = createNewNode(type, position);
      addNode(newNode);
    },
    [reactFlowInstance, addNode]
  );

  const handleDragOver = useCallback((event: React.DragEvent) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = 'move';
  }, []);

  const handleNodeDoubleClick = useCallback(
    (_: React.MouseEvent, node: Node) => {
      setSelectedNodeId(node.id);
    },
    [setSelectedNodeId]
  );

  const handleNodeDragStop = useCallback(
    (_: React.MouseEvent, node: Node) => {
      updateNode(node.id, { position: node.position });
    },
    [updateNode]
  );

  const handleKeyDown = useCallback(
    (event: React.KeyboardEvent) => {
      if (event.key === 'Delete' && selectedNodeId) {
        const node = storeNodes.find((n) => n.id === selectedNodeId);
        if (node && node.type !== 'start' && node.type !== 'end') {
          deleteNode(selectedNodeId);
        }
      }
      if (event.key === 'Escape') {
        setSelectedNodeId(null);
      }
    },
    [selectedNodeId, storeNodes, deleteNode, setSelectedNodeId]
  );

  return (
    <div
      ref={reactFlowWrapper}
      className="flex-1 bg-slate-900"
      onDragOver={handleDragOver || handleDragOver}
      onDrop={handleDrop || handleDrop}
      onKeyDown={handleKeyDown}
      tabIndex={0}
    >
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        onInit={onInit}
        onNodeDoubleClick={handleNodeDoubleClick}
        onNodeDragStop={handleNodeDragStop}
        onPaneClick={() => setSelectedNodeId(null)}
        onNodeClick={(_, node) => setSelectedNodeId(node.id)}
        nodeTypes={nodeTypes}
        fitView
        attributionPosition="bottom-right"
        defaultEdgeOptions={{
          type: 'smoothstep',
          animated: false,
        }}
      >
        <Background variant={BackgroundVariant.Dots} gap={20} size={1} color="#334155" />
        <Controls
          className="bg-slate-800 border border-slate-700 rounded-lg"
          position="bottom-left"
        />
        <MiniMap
          className="bg-slate-800 border border-slate-700 rounded-lg"
          position="bottom-right"
          nodeStrokeWidth={3}
          zoomable
          pannable
        />
      </ReactFlow>
    </div>
  );
};
