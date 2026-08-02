import React, { useCallback, useRef, useEffect, useState } from 'react';
import { Toolbar } from './components/Toolbar';
import { Sidebar } from './components/Sidebar';
import { FlowEditor } from './components/FlowEditor';
import { Properties } from './components/Properties';
import { Monitor } from './components/Monitor';
import { TriggersTab } from './components/triggers/TriggersTab';
import { HistoryTab } from './components/history/HistoryTab';
import { DebugTab } from './components/debug/DebugTab';
import { useFlowStore, createNewNode } from './store/useFlowStore';
import { useWebSocket } from './hooks/useWebSocket';
import type { NodeType, ServerMessage, ClientMessage, FlowDefinition } from './types/flow';
import { Clock, History, Bug, GitBranch, Check, X, Shield } from 'lucide-react';

const WS_URL = `${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${window.location.host}/ws/execute`;
const API_URL = '/api';

type BottomTab = 'monitor' | 'triggers' | 'history' | 'debug';

function App() {
  const {
    nodes,
    edges,
    flowName,
    flowId,
    executionState,
    wsConnected,
    errorMessage,
    setFlowName,
    setWsConnected,
    setErrorMessage,
    applySnapshot,
    applyEvent,
    setExecutionId,
    resetExecution,
    getFlowDefinition,
    loadFlowDefinition,
    clearFlow,
    addNode,
    flows,
    fetchFlows,
    isActionAllowed,
  } = useFlowStore();

  const [bottomTab, setBottomTab] = useState<BottomTab>('monitor');
  const [breakpoints, setBreakpoints] = useState<string[]>([]);
  const [evaluateResults, setEvaluateResults] = useState<Map<string, any>>(new Map());
  const fileInputRef = useRef<HTMLInputElement>(null);
  const currentExecutionIdRef = useRef<string | null>(null);
  const lastSeqRef = useRef<number>(0);

  useEffect(() => {
    fetchFlows();
  }, [fetchFlows]);

  const handleMessage = useCallback(
    (message: ServerMessage) => {
      switch (message.type) {
        case 'subscribed': {
          if (message.snapshot) {
            lastSeqRef.current = message.snapshot.seq;
            applySnapshot(message.snapshot);
          }
          if (message.executionId) {
            currentExecutionIdRef.current = message.executionId;
            setExecutionId(message.executionId);
          }
          break;
        }
        case 'snapshot': {
          if (message.snapshot) {
            if (message.snapshot.seq >= lastSeqRef.current) {
              lastSeqRef.current = message.snapshot.seq;
              applySnapshot(message.snapshot);
            }
          }
          break;
        }
        case 'event': {
          if (message.event) {
            const evt = message.event;
            if (evt.seq > lastSeqRef.current) {
              lastSeqRef.current = evt.seq;
              applyEvent(evt);
            }
          }
          break;
        }
        case 'commandResult': {
          if (message.accepted === false) {
            setErrorMessage(message.reason || 'Command rejected');
          }
          if (message.snapshot) {
            if (message.snapshot.seq >= lastSeqRef.current) {
              lastSeqRef.current = message.snapshot.seq;
              applySnapshot(message.snapshot);
            }
          }
          break;
        }
        case 'error':
          setErrorMessage(message.message || 'Unknown error');
          break;
        case 'breakpointUpdated':
          if (message.enabled && message.nodeId) {
            setBreakpoints(prev => [...new Set([...prev, message.nodeId!])]);
          } else if (message.nodeId) {
            setBreakpoints(prev => prev.filter(id => id !== message.nodeId));
          }
          break;
        case 'evaluateResult':
          setEvaluateResults(prev => {
            const next = new Map(prev);
            next.set(message.expression!, {
              result: message.result,
              error: message.error,
              success: message.success,
            });
            return next;
          });
          break;
      }
    },
    [applySnapshot, applyEvent, setExecutionId, setErrorMessage]
  );

  const { send, connect } = useWebSocket({
    url: WS_URL,
    onMessage: handleMessage,
    onOpen: () => {
      setWsConnected(true);
      if (currentExecutionIdRef.current) {
        send({
          type: 'subscribe',
          executionId: currentExecutionIdRef.current,
          afterSeq: lastSeqRef.current,
        });
      }
    },
    onClose: () => setWsConnected(false),
    onError: () => setWsConnected(false),
  });

  const sendMessage = useCallback(
    (message: ClientMessage) => {
      send(message);
    },
    [send]
  );

  const handleSetBreakpoint = useCallback(
    (nodeId: string, enabled: boolean) => {
      sendMessage({ type: 'setBreakpoint', nodeId, enabled });
    },
    [sendMessage]
  );

  const handleEvaluate = useCallback(
    async (expression: string) => {
      return new Promise<any>((resolve) => {
        sendMessage({ type: 'evaluate', expression });
        setTimeout(() => resolve({ error: 'Timeout' }), 5000);
      });
    },
    [sendMessage]
  );

  const onDragStart = useCallback(
    (event: React.DragEvent, nodeType: NodeType) => {
      event.dataTransfer.setData('application/reactflow', nodeType);
      event.dataTransfer.effectAllowed = 'move';
    },
    []
  );

  const onDragOver = useCallback((event: React.DragEvent) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = 'move';
  }, []);

  const onDrop = useCallback(
    (event: React.DragEvent) => {
      event.preventDefault();

      const type = event.dataTransfer.getData('application/reactflow') as NodeType;
      if (!type) return;

      const rect = (event.currentTarget as HTMLElement).getBoundingClientRect();
      const x = event.clientX - rect.left - 70;
      const y = event.clientY - rect.top - 30;

      const newNode = createNewNode(type, { x, y });
      addNode(newNode);
    },
    [addNode]
  );

  const handleRun = useCallback(() => {
    if (executionState.status !== 'idle') {
      resetExecution();
      lastSeqRef.current = 0;
      currentExecutionIdRef.current = null;
    }
    const flow = getFlowDefinition();
    const requestId = crypto.randomUUID();
    sendMessage({ type: 'execute', flow, requestId });
  }, [executionState.status, getFlowDefinition, sendMessage, resetExecution]);

  const handlePause = useCallback(() => {
    if (!isActionAllowed('pause')) return;
    sendMessage({
      type: 'pause',
      executionId: currentExecutionIdRef.current ?? undefined,
      requestId: crypto.randomUUID(),
    });
  }, [sendMessage, isActionAllowed]);

  const handleResume = useCallback(() => {
    if (!isActionAllowed('resume')) return;
    sendMessage({
      type: 'resume',
      executionId: currentExecutionIdRef.current ?? undefined,
      requestId: crypto.randomUUID(),
    });
  }, [sendMessage, isActionAllowed]);

  const handleStep = useCallback(() => {
    if (!isActionAllowed('step')) return;
    sendMessage({
      type: 'step',
      executionId: currentExecutionIdRef.current ?? undefined,
      requestId: crypto.randomUUID(),
    });
  }, [sendMessage, isActionAllowed]);

  const handleStop = useCallback(() => {
    if (!isActionAllowed('cancel')) return;
    sendMessage({
      type: 'cancel',
      executionId: currentExecutionIdRef.current ?? undefined,
      requestId: crypto.randomUUID(),
    });
  }, [sendMessage, isActionAllowed]);

  const handleApprove = useCallback((comment?: string) => {
    const pending = executionState.pendingApproval;
    if (!pending) return;
    sendMessage({
      type: 'approve',
      executionId: currentExecutionIdRef.current ?? pending.executionId,
      token: pending.token,
      responder: 'reviewer',
      comment,
      requestId: crypto.randomUUID(),
    });
  }, [sendMessage, executionState.pendingApproval]);

  const handleReject = useCallback((comment?: string) => {
    const pending = executionState.pendingApproval;
    if (!pending) return;
    sendMessage({
      type: 'reject',
      executionId: currentExecutionIdRef.current ?? pending.executionId,
      token: pending.token,
      responder: 'reviewer',
      comment,
      requestId: crypto.randomUUID(),
    });
  }, [sendMessage, executionState.pendingApproval]);

  const handleSave = useCallback(async () => {
    try {
      const flow = getFlowDefinition();
      const response = await fetch(`${API_URL}/flows/${flow.id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(flow),
      });

      if (!response.ok) {
        await fetch(`${API_URL}/flows`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(flow),
        });
      }
      alert('Flow saved successfully!');
      fetchFlows();
    } catch (error) {
      alert('Failed to save flow: ' + (error as Error).message);
    }
  }, [getFlowDefinition, fetchFlows]);

  const handleLoad = useCallback(() => {
    fileInputRef.current?.click();
  }, []);

  const handleFileLoad = useCallback(
    async (event: React.ChangeEvent<HTMLInputElement>) => {
      const file = event.target.files?.[0];
      if (!file) return;

      try {
        const text = await file.text();
        const flow = JSON.parse(text) as FlowDefinition;
        loadFlowDefinition(flow);
        fetchFlows();
        alert('Flow loaded successfully!');
      } catch (error) {
        alert('Failed to load flow: ' + (error as Error).message);
      }
      event.target.value = '';
    },
    [loadFlowDefinition, fetchFlows]
  );

  const handleExport = useCallback(() => {
    const flow = getFlowDefinition();
    const data = JSON.stringify(flow, null, 2);
    const blob = new Blob([data], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${flow.name || 'flow'}_${flow.id}.json`;
    a.click();
    URL.revokeObjectURL(url);
  }, [getFlowDefinition]);

  const handleClear = useCallback(() => {
    if (confirm('Clear all nodes and edges?')) {
      clearFlow();
      resetExecution();
      currentExecutionIdRef.current = null;
      lastSeqRef.current = 0;
    }
  }, [clearFlow, resetExecution]);

  useEffect(() => {
    if (executionState.status === 'idle' && nodes.length === 0) {
      const startNode = createNewNode('start', { x: 100, y: 200 });
      const endNode = createNewNode('end', { x: 400, y: 200 });
      addNode(startNode);
      addNode(endNode);
    }
  }, []);

  useEffect(() => {
    connect();
  }, [connect]);

  const bottomTabs: { id: BottomTab; label: string; icon: React.ReactNode }[] = [
    { id: 'monitor', label: 'Monitor', icon: <GitBranch size={14} /> },
    { id: 'triggers', label: 'Triggers', icon: <Clock size={14} /> },
    { id: 'history', label: 'History', icon: <History size={14} /> },
    { id: 'debug', label: 'Debug', icon: <Bug size={14} /> },
  ];

  const renderBottomPanel = () => {
    switch (bottomTab) {
      case 'monitor':
        return <Monitor />;
      case 'triggers':
        return <TriggersTab flowId={flowId} flows={flows} />;
      case 'history':
        return <HistoryTab flowId={flowId} />;
      case 'debug':
        return (
          <DebugTab
            nodes={nodes}
            executionStatus={executionState.status}
            currentNodeId={executionState.currentNodeId}
            variables={executionState.variables}
            breakpoints={breakpoints}
            onSendMessage={sendMessage}
            onSetBreakpoint={handleSetBreakpoint}
            onEvaluate={handleEvaluate}
          />
        );
    }
  };

  return (
    <div className="h-screen flex flex-col bg-slate-900 text-white overflow-hidden">
      <Toolbar
        onRun={handleRun}
        onPause={handlePause}
        onResume={handleResume}
        onStep={handleStep}
        onStop={handleStop}
        onSave={handleSave}
        onLoad={handleLoad}
        onExport={handleExport}
        onClear={handleClear}
        status={executionState.status}
        allowedActions={executionState.allowedActions}
        wsConnected={wsConnected}
        flowName={flowName}
        onFlowNameChange={setFlowName}
      />

      {errorMessage && (
        <div className="bg-red-500/20 border-b border-red-500 px-4 py-2 text-red-400 text-sm flex items-center justify-between">
          <span>{errorMessage}</span>
          <button
            onClick={() => setErrorMessage(null)}
            className="text-red-300 hover:text-white"
          >
            ✕
          </button>
        </div>
      )}

      {executionState.status === 'awaiting_approval' && executionState.pendingApproval && (
        <div className="bg-amber-500/20 border-b border-amber-500 px-4 py-3 text-amber-200">
          <div className="flex items-center justify-between max-w-full">
            <div className="flex items-center gap-3 min-w-0">
              <Shield size={20} className="text-amber-400 shrink-0" />
              <div className="min-w-0">
                <div className="font-semibold text-sm">Approval Required</div>
                <div className="text-xs text-amber-300/80 truncate">
                  Node: {executionState.pendingApproval.nodeId}
                  {executionState.pendingApproval.description && ` — ${executionState.pendingApproval.description}`}
                </div>
                <div className="text-xs text-amber-300/60 mt-0.5">
                  Approvers: {executionState.pendingApproval.approvers.join(', ') || 'Any'}
                  {' · '}
                  Timeout: {executionState.pendingApproval.deadline
                    ? new Date(executionState.pendingApproval.deadline * 1000).toLocaleTimeString()
                    : 'None'}
                  {' · '}
                  v{executionState.pendingApproval.flowVersion}
                </div>
              </div>
            </div>
            <div className="flex items-center gap-2 shrink-0 ml-4">
              <button
                onClick={() => handleApprove()}
                className="flex items-center gap-1.5 px-4 py-1.5 bg-green-600 hover:bg-green-500 text-white text-sm font-medium rounded transition-colors"
              >
                <Check size={16} /> Approve
              </button>
              <button
                onClick={() => handleReject()}
                className="flex items-center gap-1.5 px-4 py-1.5 bg-red-600 hover:bg-red-500 text-white text-sm font-medium rounded transition-colors"
              >
                <X size={16} /> Reject
              </button>
            </div>
          </div>
        </div>
      )}

      <div className="flex-1 flex overflow-hidden">
        <Sidebar onDragStart={onDragStart} />

        <div className="flex-1 flex flex-col overflow-hidden">
          <FlowEditor
            onDragStart={onDragStart}
            onDragOver={onDragOver}
            onDrop={onDrop}
          />
        </div>

        <Properties nodes={nodes} />
      </div>

      <div className="h-[400px] border-t border-slate-700 flex flex-col bg-slate-800">
        <div className="flex border-b border-slate-700 bg-slate-800">
          {bottomTabs.map((tab) => (
            <button
              key={tab.id}
              onClick={() => setBottomTab(tab.id)}
              className={`px-4 py-2 text-sm font-medium flex items-center gap-2 border-b-2 transition-colors ${
                bottomTab === tab.id
                  ? 'border-blue-500 text-blue-400 bg-slate-700/50'
                  : 'border-transparent text-slate-400 hover:text-white hover:bg-slate-700/30'
              }`}
            >
              {tab.icon}
              {tab.label}
              {tab.id === 'triggers' && (
                <span className="text-xs bg-blue-500/20 text-blue-400 px-1.5 py-0.5 rounded">
                  NEW
                </span>
              )}
              {tab.id === 'history' && (
                <span className="text-xs bg-purple-500/20 text-purple-400 px-1.5 py-0.5 rounded">
                  NEW
                </span>
              )}
              {tab.id === 'debug' && (
                <span className="text-xs bg-red-500/20 text-red-400 px-1.5 py-0.5 rounded">
                  NEW
                </span>
              )}
            </button>
          ))}
        </div>
        <div className="flex-1 overflow-hidden">
          {renderBottomPanel()}
        </div>
      </div>

      <input
        type="file"
        ref={fileInputRef}
        accept=".json"
        onChange={handleFileLoad}
        className="hidden"
      />
    </div>
  );
}

export default App;
