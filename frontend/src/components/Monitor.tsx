import React, { useState } from 'react';
import {
  ChevronUp,
  ChevronDown,
  Variable,
  ScrollText,
  Download,
  Edit2,
  Check,
  X,
  AlertCircle,
  ShieldQuestion,
  Clock,
} from 'lucide-react';
import { useFlowStore } from '../store/useFlowStore';
import { formatValue, formatTimestamp } from '../utils/flowUtils';
import type { TraceLog } from '../types/flow';

export const Monitor: React.FC<{ onApproval?: (decision: 'approve' | 'reject', token: string, comment?: string) => void }> = ({ onApproval }) => {
  const [isExpanded, setIsExpanded] = useState(true);
  const [activeTab, setActiveTab] = useState<'variables' | 'trace'>('variables');
  const [editingVar, setEditingVar] = useState<string | null>(null);
  const [editValue, setEditValue] = useState('');

  const { executionState, setVariable } = useFlowStore();
  const { variables, trace, status, allowedActions, pendingApproval } = executionState;

  const canApprove = allowedActions.includes('approve');
  const canReject = allowedActions.includes('reject');
  const [comment, setComment] = useState('');

  const sendApproval = (decision: 'approve' | 'reject') => {
    const token = pendingApproval?.token;
    if (!token || !onApproval) return;
    onApproval(decision, token, comment || undefined);
    setComment('');
  };

  const formatDeadline = (ts: number) => {
    const remaining = Math.max(0, ts - Date.now());
    const mins = Math.floor(remaining / 60000);
    const secs = Math.floor((remaining % 60000) / 1000);
    return `${mins}m ${secs}s remaining`;
  };

  const startEdit = (name: string, value: any) => {
    setEditingVar(name);
    setEditValue(JSON.stringify(value));
  };

  const saveEdit = () => {
    if (editingVar) {
      try {
        const parsed = JSON.parse(editValue);
        setVariable(editingVar, parsed);
        setEditingVar(null);
      } catch {
        alert('Invalid JSON value');
      }
    }
  };

  const cancelEdit = () => {
    setEditingVar(null);
    setEditValue('');
  };

  const exportTrace = () => {
    const data = JSON.stringify(trace, null, 2);
    const blob = new Blob([data], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `trace_${Date.now()}.json`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const getActionColor = (action: TraceLog['action']) => {
    switch (action) {
      case 'enter':
        return 'text-blue-400';
      case 'exit':
        return 'text-slate-400';
      case 'error':
        return 'text-red-400';
      default:
        return 'text-slate-400';
    }
  };

  const getActionIcon = (action: TraceLog['action']) => {
    switch (action) {
      case 'enter':
        return '→';
      case 'exit':
        return '←';
      case 'error':
        return <AlertCircle size={12} />;
      default:
        return '•';
    }
  };

  return (
    <div className="bg-slate-800 border-t border-slate-700">
      <div
        className="h-10 px-4 flex items-center justify-between cursor-pointer hover:bg-slate-700/50 transition-colors"
        onClick={() => setIsExpanded(!isExpanded)}
      >
        <div className="flex items-center gap-4">
          {isExpanded ? <ChevronDown size={16} /> : <ChevronUp size={16} />}
          <span className="text-white font-semibold text-sm">Execution Monitor</span>
          <span
            className={`text-xs px-2 py-0.5 rounded ${
              status === 'running'
                ? 'bg-green-500/20 text-green-400'
                : status === 'pausing'
                ? 'bg-orange-500/20 text-orange-400'
                : status === 'paused'
                ? 'bg-yellow-500/20 text-yellow-400'
                : status === 'awaiting_approval'
                ? 'bg-purple-500/20 text-purple-400'
                : status === 'retry_wait'
                ? 'bg-orange-500/20 text-orange-400'
                : status === 'queued'
                ? 'bg-blue-500/20 text-blue-400'
                : status === 'succeeded' || status === 'completed'
                ? 'bg-emerald-500/20 text-emerald-400'
                : status === 'failed' || status === 'error'
                ? 'bg-red-500/20 text-red-400'
                : status === 'cancelled' || status === 'stopped'
                ? 'bg-slate-600 text-slate-400'
                : 'bg-slate-600 text-slate-400'
            }`}
          >
            {status}
          </span>
        </div>

        {isExpanded && (
          <div className="flex items-center gap-2" onClick={(e) => e.stopPropagation()}>
            <button
              onClick={() => setActiveTab('variables')}
              className={`px-3 py-1 rounded text-xs font-medium transition-colors flex items-center gap-1 ${
                activeTab === 'variables'
                  ? 'bg-blue-500/20 text-blue-400'
                  : 'text-slate-400 hover:text-white'
              }`}
            >
              <Variable size={14} />
              Variables
            </button>
            <button
              onClick={() => setActiveTab('trace')}
              className={`px-3 py-1 rounded text-xs font-medium transition-colors flex items-center gap-1 ${
                activeTab === 'trace'
                  ? 'bg-blue-500/20 text-blue-400'
                  : 'text-slate-400 hover:text-white'
              }`}
            >
              <ScrollText size={14} />
              Trace ({trace.length})
            </button>
            {trace.length > 0 && (
              <button
                onClick={exportTrace}
                className="p-1.5 rounded hover:bg-slate-700 text-slate-400 hover:text-white transition-colors"
                title="Export Trace"
              >
                <Download size={14} />
              </button>
            )}
          </div>
        )}
      </div>

      {isExpanded && pendingApproval && (canApprove || canReject) && (
        <div className="px-4 py-3 bg-purple-500/10 border-b border-purple-500/30">
          <div className="flex items-start gap-3">
            <ShieldQuestion size={18} className="text-purple-400 mt-0.5 flex-shrink-0" />
            <div className="flex-1 min-w-0">
              <div className="text-purple-300 font-semibold text-sm">Approval Required</div>
              <div className="text-slate-300 text-sm mt-0.5">{pendingApproval.prompt || 'Please review and respond'}</div>
              <div className="flex items-center gap-3 mt-1.5 text-xs text-slate-400">
                <span className="flex items-center gap-1">
                  <Clock size={11} />
                  {formatDeadline(pendingApproval.deadline)}
                </span>
                <span>node: <code className="text-slate-300">{pendingApproval.nodeId}</code></span>
                {pendingApproval.branchId && (
                  <span>branch: <code className="text-slate-300">{pendingApproval.branchId}</code></span>
                )}
                <span>v{pendingApproval.flowVersion ?? '-'}</span>
              </div>
              <div className="flex items-center gap-2 mt-2">
                <input
                  type="text"
                  value={comment}
                  onChange={(e) => setComment(e.target.value)}
                  placeholder="Comment (optional)..."
                  className="flex-1 bg-slate-700/60 text-white px-2.5 py-1 rounded text-xs border border-slate-600 focus:outline-none focus:border-purple-500"
                />
                {canApprove && (
                  <button
                    onClick={() => sendApproval('approve')}
                    className="px-3 py-1 rounded bg-emerald-500/20 text-emerald-400 hover:bg-emerald-500/30 text-xs font-medium transition-colors flex items-center gap-1"
                  >
                    <Check size={13} />
                    Approve
                  </button>
                )}
                {canReject && (
                  <button
                    onClick={() => sendApproval('reject')}
                    className="px-3 py-1 rounded bg-red-500/20 text-red-400 hover:bg-red-500/30 text-xs font-medium transition-colors flex items-center gap-1"
                  >
                    <X size={13} />
                    Reject
                  </button>
                )}
              </div>
            </div>
          </div>
        </div>
      )}

      {isExpanded && (
        <div className="h-56 overflow-auto">
          {activeTab === 'variables' ? (
            <div className="p-2">
              {Object.keys(variables).length === 0 ? (
                <div className="text-slate-500 text-sm text-center py-8">
                  No variables yet
                </div>
              ) : (
                <table className="w-full text-sm">
                  <thead className="sticky top-0 bg-slate-800">
                    <tr className="text-slate-400 text-left">
                      <th className="px-3 py-2 font-medium">Name</th>
                      <th className="px-3 py-2 font-medium">Value</th>
                      <th className="px-3 py-2 font-medium w-20">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {Object.entries(variables).map(([name, value]) => (
                      <tr key={name} className="border-t border-slate-700 hover:bg-slate-700/30">
                        <td className="px-3 py-2 text-blue-400 font-mono">{name}</td>
                        <td className="px-3 py-2">
                          {editingVar === name ? (
                            <input
                              type="text"
                              value={editValue}
                              onChange={(e) => setEditValue(e.target.value)}
                              className="w-full bg-slate-700 text-white px-2 py-1 rounded text-xs font-mono border border-blue-500 focus:outline-none"
                              autoFocus
                              onKeyDown={(e) => {
                                if (e.key === 'Enter') saveEdit();
                                if (e.key === 'Escape') cancelEdit();
                              }}
                            />
                          ) : (
                            <code className="text-slate-300 font-mono text-xs">
                              {formatValue(value)}
                            </code>
                          )}
                        </td>
                        <td className="px-3 py-2">
                          {editingVar === name ? (
                            <div className="flex gap-1">
                              <button
                                onClick={saveEdit}
                                className="p-1 hover:bg-green-500/20 text-green-400 rounded"
                              >
                                <Check size={14} />
                              </button>
                              <button
                                onClick={cancelEdit}
                                className="p-1 hover:bg-red-500/20 text-red-400 rounded"
                              >
                                <X size={14} />
                              </button>
                            </div>
                          ) : (
                            <button
                              onClick={() => startEdit(name, value)}
                              className="p-1 hover:bg-blue-500/20 text-blue-400 rounded disabled:opacity-50"
                              disabled={status === 'running'}
                              title={status === 'running' ? 'Pause to edit' : 'Edit variable'}
                            >
                              <Edit2 size={14} />
                            </button>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          ) : (
            <div className="p-2 space-y-1">
              {trace.length === 0 ? (
                <div className="text-slate-500 text-sm text-center py-8">
                  No trace logs yet
                </div>
              ) : (
                trace.map((log, index) => (
                  <div
                    key={index}
                    className="px-3 py-2 text-xs font-mono bg-slate-700/30 rounded flex items-start gap-2"
                  >
                    <span className="text-slate-500 flex-shrink-0">
                      {formatTimestamp(log.timestamp)}
                    </span>
                    <span className={getActionColor(log.action)}>
                      {getActionIcon(log.action)}
                    </span>
                    <span className="text-white font-semibold">{log.nodeType}</span>
                    <span className="text-slate-400">{log.nodeId}</span>
                    <span className="text-slate-500">{log.action}</span>
                    {log.message && (
                      <span className="text-red-400 ml-auto">{log.message}</span>
                    )}
                  </div>
                ))
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
};
