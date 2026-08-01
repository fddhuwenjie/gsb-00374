import React, { useState, useEffect, useCallback, useRef } from 'react';
import { Bug, Play, Pause, StepForward, ArrowRight, ArrowUpRight, X, Plus, Trash2, RefreshCw } from 'lucide-react';
import type { FlowNode, ClientMessage } from '../../types/flow';
import { nodeTypeColors } from '../../utils/flowUtils';

interface WatchExpression {
  id: string;
  expression: string;
  result?: any;
  error?: string;
}

interface DebugTabProps {
  nodes: FlowNode[];
  executionStatus: string;
  currentNodeId: string | null;
  variables: Record<string, any>;
  breakpoints: string[];
  onSendMessage: (message: ClientMessage) => void;
  onSetBreakpoint: (nodeId: string, enabled: boolean) => void;
  onEvaluate: (expression: string) => Promise<any>;
}

export const DebugTab: React.FC<DebugTabProps> = ({
  nodes,
  executionStatus,
  currentNodeId,
  variables,
  breakpoints,
  onSendMessage,
  onSetBreakpoint,
  onEvaluate,
}) => {
  const [watchExpressions, setWatchExpressions] = useState<WatchExpression[]>([]);
  const [newExpression, setNewExpression] = useState('');
  const [callDepth, setCallDepth] = useState(0);
  const [isPaused, setIsPaused] = useState(false);
  const evalTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const breakpointNodes = nodes.filter(n => breakpoints.includes(n.id));
  const isDebugging = executionStatus === 'running' || executionStatus === 'paused';

  useEffect(() => {
    setIsPaused(executionStatus === 'paused');
  }, [executionStatus]);

  const evaluateAllWatches = useCallback(async () => {
    for (const watch of watchExpressions) {
      try {
        const result = await onEvaluate(watch.expression);
        setWatchExpressions(prev =>
          prev.map(w =>
            w.id === watch.id
              ? { ...w, result: result?.result, error: result?.error }
              : w
          )
        );
      } catch (e) {
        setWatchExpressions(prev =>
          prev.map(w =>
            w.id === watch.id
              ? { ...w, error: 'Evaluation failed' }
              : w
          )
        );
      }
    }
  }, [watchExpressions, onEvaluate]);

  useEffect(() => {
    if (isPaused && watchExpressions.length > 0) {
      evaluateAllWatches();
    }
  }, [isPaused, watchExpressions.length, evaluateAllWatches]);

  const addWatchExpression = () => {
    if (!newExpression.trim()) return;
    const id = `watch_${Date.now()}`;
    setWatchExpressions(prev => [...prev, { id, expression: newExpression.trim() }]);
    setNewExpression('');
  };

  const removeWatchExpression = (id: string) => {
    setWatchExpressions(prev => prev.filter(w => w.id !== id));
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      addWatchExpression();
    }
  };

  const handleStepInto = () => {
    onSendMessage({ type: 'stepInto' });
  };

  const handleStepOut = () => {
    onSendMessage({ type: 'stepOut' });
  };

  const toggleBreakpoint = (nodeId: string) => {
    const enabled = !breakpoints.includes(nodeId);
    onSetBreakpoint(nodeId, enabled);
  };

  const formatValue = (value: any): string => {
    if (value === null) return 'null';
    if (value === undefined) return 'undefined';
    if (typeof value === 'string') return `"${value}"`;
    if (typeof value === 'object') {
      try {
        return JSON.stringify(value, null, 1).replace(/\n/g, ' ');
      } catch {
        return String(value);
      }
    }
    return String(value);
  };

  const sortedBreakpoints = [...breakpointNodes].sort((a, b) => {
    const aActive = a.id === currentNodeId ? 1 : 0;
    const bActive = b.id === currentNodeId ? 1 : 0;
    return bActive - aActive;
  });

  return (
    <div className="h-full flex flex-col bg-slate-800">
      <div className="p-4 border-b border-slate-700 flex items-center justify-between">
        <h3 className="text-white font-bold flex items-center gap-2">
          <Bug size={18} className="text-red-400" />
          Debugger
        </h3>
        <div className="flex items-center gap-2">
          {isDebugging && (
            <div className="flex items-center gap-1">
              <button
                onClick={() => onSendMessage({ type: 'pause' })}
                disabled={executionStatus !== 'running'}
                className="p-1.5 rounded bg-slate-700 text-slate-300 hover:bg-slate-600 disabled:opacity-50"
                title="Pause"
              >
                <Pause size={16} />
              </button>
              <button
                onClick={() => onSendMessage({ type: 'resume' })}
                disabled={executionStatus !== 'paused'}
                className="p-1.5 rounded bg-slate-700 text-slate-300 hover:bg-slate-600 disabled:opacity-50"
                title="Resume"
              >
                <Play size={16} />
              </button>
              <button
                onClick={handleStepInto}
                disabled={!isPaused}
                className="p-1.5 rounded bg-slate-700 text-slate-300 hover:bg-slate-600 disabled:opacity-50"
                title="Step Into"
              >
                <StepForward size={16} />
              </button>
              <button
                onClick={handleStepOut}
                disabled={!isPaused || callDepth <= 0}
                className="p-1.5 rounded bg-slate-700 text-slate-300 hover:bg-slate-600 disabled:opacity-50"
                title="Step Out"
              >
                <ArrowUpRight size={16} />
              </button>
            </div>
          )}
        </div>
      </div>

      {isPaused && (
        <div className="px-4 py-2 bg-yellow-500/10 border-b border-yellow-500/30 flex items-center justify-between">
          <div className="flex items-center gap-2 text-yellow-400 text-sm">
            <span className="animate-pulse">●</span>
            <span>Paused at</span>
            <code className="bg-slate-700 px-1.5 py-0.5 rounded text-xs font-mono">
              {currentNodeId || 'unknown'}
            </code>
          </div>
          {callDepth > 0 && (
            <div className="text-xs text-slate-400">
              Call depth: <span className="text-white font-mono">{callDepth}</span>
            </div>
          )}
        </div>
      )}

      <div className="flex-1 flex overflow-hidden">
        <div className="w-1/2 border-r border-slate-700 flex flex-col overflow-hidden">
          <div className="px-4 py-2 border-b border-slate-700 bg-slate-700/30">
            <div className="text-white text-sm font-medium flex items-center justify-between">
              <span>Breakpoints</span>
              <span className="text-xs text-slate-400">{breakpoints.length} set</span>
            </div>
          </div>

          <div className="flex-1 overflow-y-auto">
            {breakpoints.length === 0 ? (
              <div className="text-slate-500 text-sm text-center py-6 px-4">
                <p>No breakpoints set.</p>
                <p className="text-xs mt-1">Right-click a node or use the toggle below to add breakpoints.</p>
              </div>
            ) : (
              <div className="divide-y divide-slate-700">
                {sortedBreakpoints.map((node) => {
                  const colors = nodeTypeColors[node.type];
                  return (
                    <div
                      key={node.id}
                      className={`p-3 flex items-center gap-3 hover:bg-slate-700/30 ${
                        node.id === currentNodeId ? 'bg-yellow-500/10' : ''
                      }`}
                    >
                      <button
                        onClick={() => toggleBreakpoint(node.id)}
                        className={`w-4 h-4 rounded-full border-2 flex items-center justify-center flex-shrink-0 ${
                          breakpoints.includes(node.id)
                            ? 'bg-red-500 border-red-500'
                            : 'border-slate-500 hover:border-slate-400'
                        }`}
                      >
                        {breakpoints.includes(node.id) && (
                          <div className="w-2 h-2 bg-white rounded-full" />
                        )}
                      </button>
                      <div
                        className="w-2 h-2 rounded-full flex-shrink-0"
                        style={{ backgroundColor: colors.bg }}
                      />
                      <div className="flex-1 min-w-0">
                        <div className="text-white text-sm truncate">{node.data.label}</div>
                        <div className="text-xs text-slate-500 font-mono truncate">{node.id}</div>
                      </div>
                      {node.id === currentNodeId && (
                        <span className="text-xs text-yellow-400 flex items-center gap-1">
                          <ArrowRight size={12} />
                          Here
                        </span>
                      )}
                    </div>
                  );
                })}
              </div>
            )}

            <div className="p-3 border-t border-slate-700">
              <div className="text-xs text-slate-400 mb-2">Toggle breakpoint:</div>
              <select
                value=""
                onChange={(e) => {
                  if (e.target.value) {
                    toggleBreakpoint(e.target.value);
                    e.target.value = '';
                  }
                }}
                className="w-full bg-slate-600 text-white px-3 py-1.5 rounded border border-slate-500 text-xs"
              >
                <option value="">Select a node...</option>
                {nodes.filter(n => n.type !== 'start' && n.type !== 'end').map((node) => (
                  <option key={node.id} value={node.id}>
                    {node.data.label} ({node.type})
                  </option>
                ))}
              </select>
            </div>
          </div>
        </div>

        <div className="w-1/2 flex flex-col overflow-hidden">
          <div className="px-4 py-2 border-b border-slate-700 bg-slate-700/30">
            <div className="text-white text-sm font-medium">Variable Monitor</div>
          </div>

          <div className="flex-1 overflow-y-auto">
            <div className="p-3 border-b border-slate-700">
              <div className="text-xs text-slate-400 mb-2">Watch Expressions</div>
              <div className="flex gap-2">
                <input
                  type="text"
                  value={newExpression}
                  onChange={(e) => setNewExpression(e.target.value)}
                  onKeyDown={handleKeyDown}
                  placeholder="e.g., ctx.x + ctx.y"
                  className="flex-1 bg-slate-600 text-white px-3 py-1.5 rounded border border-slate-500 text-xs font-mono focus:outline-none focus:border-blue-500"
                  disabled={!isDebugging}
                />
                <button
                  onClick={addWatchExpression}
                  disabled={!isDebugging || !newExpression.trim()}
                  className="px-3 py-1.5 bg-blue-500 text-white rounded hover:bg-blue-600 text-xs disabled:opacity-50"
                >
                  <Plus size={14} />
                </button>
              </div>
              <div className="text-[10px] text-slate-500 mt-1">
                Enter any AST expression using ctx.variable syntax
              </div>
            </div>

            {watchExpressions.length > 0 && (
              <div className="divide-y divide-slate-700">
                {watchExpressions.map((watch) => (
                  <div key={watch.id} className="p-3 hover:bg-slate-700/30">
                    <div className="flex items-center justify-between mb-1">
                      <code className="text-xs text-blue-300 font-mono truncate flex-1">
                        {watch.expression}
                      </code>
                      <div className="flex items-center gap-1">
                        <button
                          onClick={async () => {
                            const result = await onEvaluate(watch.expression);
                            setWatchExpressions(prev =>
                              prev.map(w =>
                                w.id === watch.id
                                  ? { ...w, result: result?.result, error: result?.error }
                                  : w
                              )
                            );
                          }}
                          disabled={!isDebugging}
                          className="p-1 rounded hover:bg-slate-600 text-slate-400 disabled:opacity-50"
                          title="Re-evaluate"
                        >
                          <RefreshCw size={12} />
                        </button>
                        <button
                          onClick={() => removeWatchExpression(watch.id)}
                          className="p-1 rounded hover:bg-red-500/20 text-slate-400 hover:text-red-400"
                          title="Remove"
                        >
                          <X size={12} />
                        </button>
                      </div>
                    </div>
                    {watch.error ? (
                      <div className="text-xs text-red-400 font-mono">{watch.error}</div>
                    ) : watch.result !== undefined ? (
                      <div className="text-xs text-green-300 font-mono break-all">
                        → {formatValue(watch.result)}
                      </div>
                    ) : (
                      <div className="text-xs text-slate-500 italic">
                        {isPaused ? 'Evaluating...' : 'Run and pause to evaluate'}
                      </div>
                    )}
                  </div>
                ))}
              </div>
            )}

            <div className="p-3">
              <div className="text-xs text-slate-400 mb-2">Current Variables</div>
              {Object.keys(variables).length === 0 ? (
                <div className="text-xs text-slate-500 italic">No variables in context</div>
              ) : (
                <div className="space-y-1 max-h-60 overflow-y-auto">
                  {Object.entries(variables)
                    .filter(([key]) => !key.startsWith('_'))
                    .map(([key, value]) => (
                      <div
                        key={key}
                        className="flex items-center gap-2 text-xs p-1.5 rounded hover:bg-slate-700/30"
                      >
                        <span className="text-blue-300 font-mono flex-shrink-0">{key}</span>
                        <span className="text-slate-500 flex-shrink-0">=</span>
                        <span className="text-slate-300 font-mono truncate flex-1">
                          {formatValue(value)}
                        </span>
                      </div>
                    ))}
                </div>
              )}
            </div>
          </div>
        </div>
      </div>

      <div className="p-3 border-t border-slate-700 bg-slate-700/30">
        <div className="flex items-center gap-4 text-xs text-slate-400">
          <div>
            Status: <span className={`font-medium ${
              executionStatus === 'running' ? 'text-green-400' :
              executionStatus === 'paused' ? 'text-yellow-400' :
              executionStatus === 'error' ? 'text-red-400' :
              executionStatus === 'completed' ? 'text-blue-400' : 'text-slate-500'
            }`}>{executionStatus}</span>
          </div>
          <div>
            Variables: <span className="text-white font-mono">{Object.keys(variables).length}</span>
          </div>
          <div>
            Breakpoints: <span className="text-white font-mono">{breakpoints.length}</span>
          </div>
        </div>
      </div>
    </div>
  );
};
