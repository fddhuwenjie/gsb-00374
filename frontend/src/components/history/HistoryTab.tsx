import React, { useState, useEffect, useCallback } from 'react';
import { History, Play, Trash2, ChevronRight, RotateCcw, Eye, X } from 'lucide-react';
import type { Execution, TraceLog } from '../../types/flow';
import { formatTimestamp, formatValue } from '../../utils/flowUtils';

const API_URL = '/api';

interface HistoryTabProps {
  flowId: string;
  onReplay?: (executionId: string, fromNodeId?: string, overrides?: Record<string, any>) => void;
}

export const HistoryTab: React.FC<HistoryTabProps> = ({ flowId, onReplay }) => {
  const [executions, setExecutions] = useState<Execution[]>([]);
  const [selectedExecution, setSelectedExecution] = useState<Execution | null>(null);
  const [showReplayModal, setShowReplayModal] = useState(false);
  const [replayFromNode, setReplayFromNode] = useState<string>('');
  const [replayOverrides, setReplayOverrides] = useState<string>('');
  const [selectedTraceIndex, setSelectedTraceIndex] = useState<number | null>(null);

  const fetchExecutions = useCallback(async () => {
    try {
      const res = await fetch(`${API_URL}/executions?flowId=${flowId}`);
      if (res.ok) {
        const data = await res.json();
        setExecutions(data);
      }
    } catch (e) {
      console.error('Failed to fetch executions:', e);
    }
  }, [flowId]);

  useEffect(() => {
    if (flowId) {
      fetchExecutions();
    }
  }, [flowId, fetchExecutions]);

  const deleteExecution = async (executionId: string) => {
    if (!confirm('Delete this execution history?')) return;
    try {
      await fetch(`${API_URL}/executions/${executionId}`, { method: 'DELETE' });
      if (selectedExecution?.id === executionId) {
        setSelectedExecution(null);
      }
      fetchExecutions();
    } catch (e) {
      console.error('Failed to delete execution:', e);
    }
  };

  const handleReplay = async () => {
    if (!selectedExecution) return;

    try {
      let overrides: Record<string, any> | undefined;
      if (replayOverrides.trim()) {
        overrides = JSON.parse(replayOverrides);
      }

      const res = await fetch(`${API_URL}/executions/${selectedExecution.id}/replay`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          fromNodeId: replayFromNode || undefined,
          overrides,
        }),
      });

      if (res.ok) {
        const data = await res.json();
        setShowReplayModal(false);
        setReplayFromNode('');
        setReplayOverrides('');
        fetchExecutions();
        alert(`Replay successful! New execution: ${data.newExecutionId}`);
      } else {
        const err = await res.json();
        alert(err.detail || 'Replay failed');
      }
    } catch (e) {
      alert('Invalid JSON in overrides');
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'completed': return 'text-green-400 bg-green-500/20';
      case 'error': return 'text-red-400 bg-red-500/20';
      case 'running': return 'text-blue-400 bg-blue-500/20';
      case 'paused': return 'text-yellow-400 bg-yellow-500/20';
      case 'stopped': return 'text-slate-400 bg-slate-500/20';
      default: return 'text-slate-400 bg-slate-500/20';
    }
  };

  const getActionColor = (action: string) => {
    switch (action) {
      case 'enter': return 'text-blue-400';
      case 'exit': return 'text-green-400';
      case 'error': return 'text-red-400';
      default: return 'text-slate-400';
    }
  };

  const snapshotNodes = selectedExecution ? Object.keys(selectedExecution.snapshots || {}) : [];

  return (
    <div className="h-full flex flex-col bg-slate-800">
      <div className="p-4 border-b border-slate-700 flex items-center justify-between">
        <h3 className="text-white font-bold flex items-center gap-2">
          <History size={18} className="text-purple-400" />
          Execution History
        </h3>
        <button
          onClick={fetchExecutions}
          className="px-3 py-1.5 bg-slate-600 text-white rounded hover:bg-slate-500 text-sm"
        >
          Refresh
        </button>
      </div>

      <div className="flex-1 flex overflow-hidden">
        <div className="w-1/2 border-r border-slate-700 overflow-y-auto">
          {executions.length === 0 ? (
            <div className="text-slate-500 text-sm text-center py-8">
              No executions yet. Run a flow to see history.
            </div>
          ) : (
            <div className="divide-y divide-slate-700">
              {executions.map((exec) => (
                <div
                  key={exec.id}
                  className={`p-3 cursor-pointer hover:bg-slate-700/30 ${
                    selectedExecution?.id === exec.id ? 'bg-slate-700/50' : ''
                  }`}
                  onClick={() => {
                    setSelectedExecution(exec);
                    setSelectedTraceIndex(null);
                  }}
                >
                  <div className="flex items-start justify-between">
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2">
                        <span className={`px-2 py-0.5 rounded text-xs font-medium ${getStatusColor(exec.status)}`}>
                          {exec.status}
                        </span>
                        <span className="text-xs text-slate-500 font-mono truncate">
                          {exec.id}
                        </span>
                      </div>
                      <div className="text-xs text-slate-400 mt-1">
                        {new Date(exec.startedAt).toLocaleString()}
                      </div>
                      <div className="text-xs text-slate-500 mt-0.5">
                        {exec.trace?.length || 0} nodes executed
                      </div>
                    </div>
                    <div className="flex items-center gap-1 ml-2">
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          setSelectedExecution(exec);
                          setShowReplayModal(true);
                        }}
                        className="p-1.5 rounded text-slate-400 hover:bg-blue-500/20 hover:text-blue-400"
                        title="Replay"
                      >
                        <RotateCcw size={14} />
                      </button>
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          deleteExecution(exec.id);
                        }}
                        className="p-1.5 rounded text-slate-400 hover:bg-red-500/20 hover:text-red-400"
                        title="Delete"
                      >
                        <Trash2 size={14} />
                      </button>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>

        <div className="w-1/2 flex flex-col overflow-hidden">
          {selectedExecution ? (
            <>
              <div className="p-3 border-b border-slate-700 bg-slate-700/30">
                <div className="flex items-center justify-between">
                  <div>
                    <div className="text-white text-sm font-medium">Execution Details</div>
                    <div className="text-xs text-slate-400 font-mono mt-0.5">{selectedExecution.id}</div>
                  </div>
                  <button
                    onClick={() => setShowReplayModal(true)}
                    className="px-3 py-1.5 bg-blue-500 text-white rounded hover:bg-blue-600 text-xs flex items-center gap-1"
                  >
                    <RotateCcw size={12} />
                    Replay
                  </button>
                </div>
                <div className="grid grid-cols-2 gap-2 mt-3">
                  <div className="text-xs">
                    <span className="text-slate-400">Status:</span>
                    <span className={`ml-1 px-1.5 py-0.5 rounded ${getStatusColor(selectedExecution.status)}`}>
                      {selectedExecution.status}
                    </span>
                  </div>
                  <div className="text-xs">
                    <span className="text-slate-400">Duration:</span>
                    <span className="text-white ml-1">
                      {((selectedExecution.finishedAt - selectedExecution.startedAt) / 1000).toFixed(2)}s
                    </span>
                  </div>
                  <div className="text-xs">
                    <span className="text-slate-400">Nodes:</span>
                    <span className="text-white ml-1">{selectedExecution.trace?.length || 0}</span>
                  </div>
                  <div className="text-xs">
                    <span className="text-slate-400">Snapshots:</span>
                    <span className="text-white ml-1">{snapshotNodes.length}</span>
                  </div>
                </div>
                {snapshotNodes.length > 0 && (
                  <div className="mt-3">
                    <div className="text-xs text-slate-400 mb-1">Snapshot Nodes:</div>
                    <div className="flex flex-wrap gap-1">
                      {snapshotNodes.map((nodeId) => (
                        <span
                          key={nodeId}
                          className="px-2 py-0.5 bg-slate-600 rounded text-xs text-slate-300 font-mono"
                        >
                          {nodeId}
                        </span>
                      ))}
                    </div>
                  </div>
                )}
              </div>

              <div className="flex-1 overflow-y-auto">
                <div className="p-2">
                  <div className="text-xs text-slate-400 mb-2 px-2">Trace Log:</div>
                  <div className="space-y-1">
                    {selectedExecution.trace?.map((log: TraceLog, index: number) => (
                      <div
                        key={index}
                        className={`p-2 rounded text-xs cursor-pointer ${
                          selectedTraceIndex === index ? 'bg-slate-600' : 'hover:bg-slate-700/50'
                        }`}
                        onClick={() => setSelectedTraceIndex(index)}
                      >
                        <div className="flex items-center gap-2">
                          <span className={`${getActionColor(log.action)} font-medium`}>
                            {log.action.toUpperCase()}
                          </span>
                          <span className="text-slate-300 font-mono">{log.nodeId}</span>
                          <span className="text-slate-500 text-[10px]">
                            {formatTimestamp(log.timestamp * 1000)}
                          </span>
                        </div>
                        {log.message && (
                          <div className="text-red-400 text-[11px] mt-0.5">{log.message}</div>
                        )}
                        {selectedTraceIndex === index && (
                          <div className="mt-2 p-2 bg-slate-800 rounded border border-slate-600">
                            <div className="text-slate-400 text-[10px] mb-1">Variables:</div>
                            <pre className="text-[11px] text-slate-300 overflow-x-auto">
                              {JSON.stringify(log.variables, null, 2)}
                            </pre>
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            </>
          ) : (
            <div className="flex-1 flex items-center justify-center text-slate-500 text-sm">
              Select an execution to view details
            </div>
          )}
        </div>
      </div>

      {showReplayModal && selectedExecution && (
        <div className="absolute inset-0 bg-black/50 flex items-center justify-center z-50">
          <div className="bg-slate-800 rounded-lg border border-slate-700 p-6 w-full max-w-md mx-4">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-white font-bold flex items-center gap-2">
                <RotateCcw size={18} className="text-blue-400" />
                Time Travel Replay
              </h3>
              <button
                onClick={() => setShowReplayModal(false)}
                className="p-1 hover:bg-slate-700 rounded text-slate-400"
              >
                <X size={18} />
              </button>
            </div>

            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-slate-300 mb-1">
                  Start From Node (optional)
                </label>
                <select
                  value={replayFromNode}
                  onChange={(e) => setReplayFromNode(e.target.value)}
                  className="w-full bg-slate-600 text-white px-3 py-2 rounded border border-slate-500 text-sm"
                >
                  <option value="">From beginning</option>
                  {snapshotNodes.map((nodeId) => (
                    <option key={nodeId} value={nodeId}>{nodeId}</option>
                  ))}
                </select>
                <div className="text-xs text-slate-500 mt-1">
                  Only nodes with snapshots are available
                </div>
              </div>

              <div>
                <label className="block text-sm font-medium text-slate-300 mb-1">
                  Variable Overrides (JSON, optional)
                </label>
                <textarea
                  value={replayOverrides}
                  onChange={(e) => setReplayOverrides(e.target.value)}
                  className="w-full bg-slate-600 text-white px-3 py-2 rounded border border-slate-500 text-sm font-mono h-24"
                  placeholder='{"x": 100, "name": "test"}'
                />
                <div className="text-xs text-slate-500 mt-1">
                  These values will override the snapshot variables
                </div>
              </div>

              <div className="flex gap-2">
                <button
                  onClick={handleReplay}
                  className="flex-1 px-3 py-2 bg-blue-500 text-white rounded hover:bg-blue-600 text-sm font-medium"
                >
                  Start Replay
                </button>
                <button
                  onClick={() => setShowReplayModal(false)}
                  className="px-3 py-2 bg-slate-600 text-white rounded hover:bg-slate-500 text-sm"
                >
                  Cancel
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};
