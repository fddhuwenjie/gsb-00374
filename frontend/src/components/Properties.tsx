import React, { useCallback } from 'react';
import CodeMirror from '@uiw/react-codemirror';
import { python } from '@codemirror/lang-python';
import { sql } from '@codemirror/lang-sql';
import { oneDark } from '@codemirror/theme-one-dark';
import { X, Settings, Type, Code, GitBranch, RotateCcw, Clock, Globe, Database, Zap, Boxes, Shield, RefreshCw, Bug, Plus, Trash2 } from 'lucide-react';
import { useFlowStore } from '../store/useFlowStore';
import type { FlowNode, NodeType } from '../types/flow';

interface PropertiesProps {
  nodes: FlowNode[];
}

const nodeIcons: Record<NodeType, React.ReactNode> = {
  start: <Type size={16} />,
  end: <Type size={16} />,
  task: <Code size={16} />,
  condition: <GitBranch size={16} />,
  loop: <RotateCcw size={16} />,
  wait: <Clock size={16} />,
  http: <Globe size={16} />,
  sql: <Database size={16} />,
  parallel: <Zap size={16} />,
  subflow: <Boxes size={16} />,
  trycatch: <Shield size={16} />,
};

const nodeTypeTitles: Record<NodeType, string> = {
  start: 'Start Node',
  end: 'End Node',
  task: 'Task Node',
  condition: 'Condition Node',
  loop: 'Loop Node',
  wait: 'Wait Node',
  http: 'HTTP Node',
  sql: 'SQL Node',
  parallel: 'Parallel Node',
  subflow: 'Subflow Node',
  trycatch: 'TryCatch Node',
};

export const Properties: React.FC<PropertiesProps> = () => {
  const { selectedNodeId, nodes, updateNodeData, setSelectedNodeId, deleteNode, flows, flowId } = useFlowStore();

  const selectedNode = nodes.find((n) => n.id === selectedNodeId);

  const updateNestedData = (nodeId: string, path: string, value: any) => {
    const node = nodes.find(n => n.id === nodeId);
    if (!node) return;
    const keys = path.split('.');
    let current: any = { ...node.data };
    let ref = current;
    for (let i = 0; i < keys.length - 1; i++) {
      ref[keys[i]] = { ...ref[keys[i]] };
      ref = ref[keys[i]];
    }
    ref[keys[keys.length - 1]] = value;
    updateNodeData(nodeId, current);
  };

  const onCodeChange = useCallback(
    (value: string) => {
      if (selectedNodeId) {
        updateNodeData(selectedNodeId, { code: value });
      }
    },
    [selectedNodeId, updateNodeData]
  );

  const onSqlChange = useCallback(
    (value: string) => {
      if (selectedNodeId) {
        updateNestedData(selectedNodeId, 'sqlConfig.query', value);
      }
    },
    [selectedNodeId]
  );

  const updateArrayItem = (path: string, index: number, value: string) => {
    if (!selectedNodeId) return;
    const node = nodes.find(n => n.id === selectedNodeId);
    if (!node) return;
    const keys = path.split('.');
    let current: any = { ...node.data };
    let ref = current;
    for (let i = 0; i < keys.length; i++) {
      ref[keys[i]] = Array.isArray(ref[keys[i]]) ? [...ref[keys[i]]] : { ...ref[keys[i]] };
      ref = ref[keys[i]];
    }
    ref[index] = value;
    updateNodeData(selectedNodeId, current);
  };

  const addArrayItem = (path: string) => {
    if (!selectedNodeId) return;
    const node = nodes.find(n => n.id === selectedNodeId);
    if (!node) return;
    const keys = path.split('.');
    let current: any = { ...node.data };
    let ref = current;
    for (let i = 0; i < keys.length; i++) {
      ref[keys[i]] = Array.isArray(ref[keys[i]]) ? [...ref[keys[i]]] : { ...ref[keys[i]] };
      ref = ref[keys[i]];
    }
    ref.push('');
    updateNodeData(selectedNodeId, current);
  };

  const removeArrayItem = (path: string, index: number) => {
    if (!selectedNodeId) return;
    const node = nodes.find(n => n.id === selectedNodeId);
    if (!node) return;
    const keys = path.split('.');
    let current: any = { ...node.data };
    let ref = current;
    for (let i = 0; i < keys.length; i++) {
      ref[keys[i]] = Array.isArray(ref[keys[i]]) ? [...ref[keys[i]]] : { ...ref[keys[i]] };
      ref = ref[keys[i]];
    }
    ref.splice(index, 1);
    updateNodeData(selectedNodeId, current);
  };

  if (!selectedNode) {
    return (
      <div className="w-80 bg-slate-800 border-l border-slate-700 p-4 overflow-y-auto">
        <h2 className="text-white font-bold text-lg mb-4 flex items-center gap-2">
          <Settings size={20} className="text-blue-400" />
          Properties
        </h2>
        <div className="text-slate-500 text-sm text-center py-8">
          Select a node to edit its properties
        </div>
      </div>
    );
  }

  const { type, data } = selectedNode;
  const showRetryConfig = ['task', 'http', 'sql', 'subflow'].includes(type);
  const showBreakpoint = type !== 'start' && type !== 'end';

  const renderMultiInput = (path: string, label: string, values: string[]) => (
    <div>
      <label className="block text-sm font-medium text-slate-300 mb-2">
        {label}
      </label>
      <div className="space-y-2">
        {values.map((value, index) => (
          <div key={index} className="flex gap-2">
            <input
              type="text"
              value={value}
              onChange={(e) => updateArrayItem(path, index, e.target.value)}
              className="flex-1 bg-slate-700 text-white px-3 py-2 rounded border border-slate-600 focus:outline-none focus:border-blue-500 text-sm font-mono"
              placeholder="Node ID"
            />
            <button
              onClick={() => removeArrayItem(path, index)}
              className="p-2 text-red-400 hover:bg-red-500/20 rounded transition-colors"
            >
              <Trash2 size={16} />
            </button>
          </div>
        ))}
        <button
          onClick={() => addArrayItem(path)}
          className="w-full flex items-center justify-center gap-2 px-3 py-2 bg-slate-700/50 text-slate-400 rounded hover:bg-slate-700 hover:text-slate-300 transition-colors text-sm border border-dashed border-slate-600"
        >
          <Plus size={16} />
          Add
        </button>
      </div>
    </div>
  );

  return (
    <div className="w-80 bg-slate-800 border-l border-slate-700 overflow-y-auto">
      <div className="p-4 border-b border-slate-700">
        <div className="flex items-center justify-between mb-2">
          <h2 className="text-white font-bold text-lg flex items-center gap-2">
            {nodeIcons[type]}
            {nodeTypeTitles[type]}
          </h2>
          <button
            onClick={() => setSelectedNodeId(null)}
            className="p-1 hover:bg-slate-700 rounded text-slate-400 hover:text-white transition-colors"
          >
            <X size={16} />
          </button>
        </div>
        <div className="text-xs text-slate-500 font-mono truncate">
          ID: {selectedNode.id}
        </div>
      </div>

      <div className="p-4 space-y-4">
        <div>
          <label className="block text-sm font-medium text-slate-300 mb-2">
            Label
          </label>
          <input
            type="text"
            value={data.label}
            onChange={(e) => updateNodeData(selectedNode.id, { label: e.target.value })}
            className="w-full bg-slate-700 text-white px-3 py-2 rounded border border-slate-600 focus:outline-none focus:border-blue-500 text-sm"
          />
        </div>

        {type === 'task' && (
          <div>
            <label className="block text-sm font-medium text-slate-300 mb-2">
              Python Code
            </label>
            <div className="border border-slate-600 rounded overflow-hidden">
              <CodeMirror
                value={data.code || ''}
                height="200px"
                theme={oneDark}
                extensions={[python()]}
                onChange={onCodeChange}
                basicSetup={{
                  lineNumbers: true,
                  highlightActiveLineGutter: true,
                  highlightActiveLine: true,
                }}
              />
            </div>
            <div className="mt-2 text-xs text-slate-500">
              Access variables via <code className="bg-slate-700 px-1 rounded">ctx["name"]</code>
            </div>
          </div>
        )}

        {(type === 'condition' || type === 'loop') && (
          <div>
            <label className="block text-sm font-medium text-slate-300 mb-2">
              Expression
            </label>
            <input
              type="text"
              value={data.expression || ''}
              onChange={(e) => updateNodeData(selectedNode.id, { expression: e.target.value })}
              className="w-full bg-slate-700 text-white px-3 py-2 rounded border border-slate-600 focus:outline-none focus:border-blue-500 text-sm font-mono"
              placeholder="e.g., ctx.x > 0"
            />
            <div className="mt-2 text-xs text-slate-500">
              Use <code className="bg-slate-700 px-1 rounded">ctx.variable</code> to access context
            </div>
          </div>
        )}

        {type === 'wait' && (
          <div>
            <label className="block text-sm font-medium text-slate-300 mb-2">
              Wait Time (seconds)
            </label>
            <input
              type="number"
              min="0"
              step="0.1"
              value={data.seconds ?? 1}
              onChange={(e) => updateNodeData(selectedNode.id, { seconds: parseFloat(e.target.value) })}
              className="w-full bg-slate-700 text-white px-3 py-2 rounded border border-slate-600 focus:outline-none focus:border-blue-500 text-sm"
            />
          </div>
        )}

        {type === 'loop' && (
          <div>
            <label className="block text-sm font-medium text-slate-300 mb-2">
              Loop Anchor Node ID
            </label>
            <input
              type="text"
              value={data.anchorId || ''}
              onChange={(e) => updateNodeData(selectedNode.id, { anchorId: e.target.value })}
              className="w-full bg-slate-700 text-white px-3 py-2 rounded border border-slate-600 focus:outline-none focus:border-blue-500 text-sm font-mono"
              placeholder="Node ID to loop back to"
            />
            <div className="mt-2 text-xs text-slate-500">
              The node ID that the loop should return to when condition is true
            </div>
          </div>
        )}

        {type === 'http' && (
          <>
            <div className="pt-2 border-t border-slate-700">
              <h3 className="text-sm font-medium text-slate-300 mb-3 flex items-center gap-2">
                <Globe size={14} className="text-blue-400" />
                HTTP Configuration
              </h3>
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-300 mb-2">
                URL
              </label>
              <input
                type="text"
                value={data.httpConfig?.url || ''}
                onChange={(e) => updateNestedData(selectedNode.id, 'httpConfig.url', e.target.value)}
                className="w-full bg-slate-700 text-white px-3 py-2 rounded border border-slate-600 focus:outline-none focus:border-blue-500 text-sm font-mono"
                placeholder="https://api.example.com"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-300 mb-2">
                Method
              </label>
              <select
                value={data.httpConfig?.method || 'GET'}
                onChange={(e) => updateNestedData(selectedNode.id, 'httpConfig.method', e.target.value)}
                className="w-full bg-slate-700 text-white px-3 py-2 rounded border border-slate-600 focus:outline-none focus:border-blue-500 text-sm"
              >
                <option value="GET">GET</option>
                <option value="POST">POST</option>
                <option value="PUT">PUT</option>
                <option value="DELETE">DELETE</option>
                <option value="PATCH">PATCH</option>
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-300 mb-2">
                Headers (JSON)
              </label>
              <textarea
                value={JSON.stringify(data.httpConfig?.headers || {}, null, 2)}
                onChange={(e) => {
                  try {
                    const parsed = JSON.parse(e.target.value);
                    updateNestedData(selectedNode.id, 'httpConfig.headers', parsed);
                  } catch {
                  }
                }}
                className="w-full bg-slate-700 text-white px-3 py-2 rounded border border-slate-600 focus:outline-none focus:border-blue-500 text-sm font-mono h-24 resize-none"
                placeholder='{"Authorization": "Bearer token"}'
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-300 mb-2">
                Body
              </label>
              <textarea
                value={data.httpConfig?.body || ''}
                onChange={(e) => updateNestedData(selectedNode.id, 'httpConfig.body', e.target.value)}
                className="w-full bg-slate-700 text-white px-3 py-2 rounded border border-slate-600 focus:outline-none focus:border-blue-500 text-sm font-mono h-24 resize-none"
                placeholder="Request body"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-300 mb-2">
                Timeout (seconds)
              </label>
              <input
                type="number"
                min="1"
                value={data.httpConfig?.timeout ?? 30}
                onChange={(e) => updateNestedData(selectedNode.id, 'httpConfig.timeout', parseInt(e.target.value) || 30)}
                className="w-full bg-slate-700 text-white px-3 py-2 rounded border border-slate-600 focus:outline-none focus:border-blue-500 text-sm"
              />
            </div>
          </>
        )}

        {type === 'sql' && (
          <>
            <div className="pt-2 border-t border-slate-700">
              <h3 className="text-sm font-medium text-slate-300 mb-3 flex items-center gap-2">
                <Database size={14} className="text-green-400" />
                SQL Configuration
              </h3>
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-300 mb-2">
                Connection String
              </label>
              <input
                type="text"
                value={data.sqlConfig?.connectionString || ''}
                onChange={(e) => updateNestedData(selectedNode.id, 'sqlConfig.connectionString', e.target.value)}
                className="w-full bg-slate-700 text-white px-3 py-2 rounded border border-slate-600 focus:outline-none focus:border-blue-500 text-sm font-mono"
                placeholder="sqlite:///data.db"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-300 mb-2">
                Query
              </label>
              <div className="border border-slate-600 rounded overflow-hidden">
                <CodeMirror
                  value={data.sqlConfig?.query || ''}
                  height="150px"
                  theme={oneDark}
                  extensions={[sql()]}
                  onChange={onSqlChange}
                  basicSetup={{
                    lineNumbers: true,
                    highlightActiveLineGutter: true,
                    highlightActiveLine: true,
                  }}
                />
              </div>
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-300 mb-2">
                Params (JSON array)
              </label>
              <textarea
                value={JSON.stringify(data.sqlConfig?.params || [], null, 2)}
                onChange={(e) => {
                  try {
                    const parsed = JSON.parse(e.target.value);
                    if (Array.isArray(parsed)) {
                      updateNestedData(selectedNode.id, 'sqlConfig.params', parsed);
                    }
                  } catch {
                  }
                }}
                className="w-full bg-slate-700 text-white px-3 py-2 rounded border border-slate-600 focus:outline-none focus:border-blue-500 text-sm font-mono h-20 resize-none"
                placeholder="[param1, param2]"
              />
            </div>
          </>
        )}

        {type === 'parallel' && (
          <>
            <div className="pt-2 border-t border-slate-700">
              <h3 className="text-sm font-medium text-slate-300 mb-3 flex items-center gap-2">
                <Zap size={14} className="text-yellow-400" />
                Parallel Configuration
              </h3>
            </div>
            {renderMultiInput('parallelConfig.branchNodeIds', 'Branch Node IDs', data.parallelConfig?.branchNodeIds || [])}
          </>
        )}

        {type === 'subflow' && (
          <>
            <div className="pt-2 border-t border-slate-700">
              <h3 className="text-sm font-medium text-slate-300 mb-3 flex items-center gap-2">
                <Boxes size={14} className="text-purple-400" />
                Subflow Configuration
              </h3>
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-300 mb-2">
                Subflow
              </label>
              <select
                value={data.subflowConfig?.subflowId || ''}
                onChange={(e) => updateNestedData(selectedNode.id, 'subflowConfig.subflowId', e.target.value)}
                className="w-full bg-slate-700 text-white px-3 py-2 rounded border border-slate-600 focus:outline-none focus:border-blue-500 text-sm"
              >
                <option value="">Select a flow...</option>
                {flows
                  .filter(f => f.id !== flowId)
                  .map((flow) => (
                    <option key={flow.id} value={flow.id}>
                      {flow.name}
                    </option>
                  ))}
              </select>
              {data.subflowConfig?.subflowId && (
                <div className="mt-2 text-xs text-slate-500 font-mono">
                  ID: {data.subflowConfig.subflowId}
                </div>
              )}
            </div>
          </>
        )}

        {type === 'trycatch' && (
          <>
            <div className="pt-2 border-t border-slate-700">
              <h3 className="text-sm font-medium text-slate-300 mb-3 flex items-center gap-2">
                <Shield size={14} className="text-orange-400" />
                TryCatch Configuration
              </h3>
            </div>
            {renderMultiInput('tryCatchConfig.tryNodeIds', 'Try Node IDs', data.tryCatchConfig?.tryNodeIds || [])}
            {renderMultiInput('tryCatchConfig.catchNodeIds', 'Catch Node IDs', data.tryCatchConfig?.catchNodeIds || [])}
          </>
        )}

        {showRetryConfig && (
          <>
            <div className="pt-2 border-t border-slate-700">
              <h3 className="text-sm font-medium text-slate-300 mb-3 flex items-center gap-2">
                <RefreshCw size={14} className="text-cyan-400" />
                Retry Configuration
              </h3>
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-300 mb-2">
                Max Attempts
              </label>
              <input
                type="number"
                min="1"
                value={data.retry?.maxAttempts ?? 3}
                onChange={(e) => updateNestedData(selectedNode.id, 'retry.maxAttempts', parseInt(e.target.value) || 1)}
                className="w-full bg-slate-700 text-white px-3 py-2 rounded border border-slate-600 focus:outline-none focus:border-blue-500 text-sm"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-300 mb-2">
                Delay Seconds
              </label>
              <input
                type="number"
                min="0"
                step="0.1"
                value={data.retry?.delaySeconds ?? 1}
                onChange={(e) => updateNestedData(selectedNode.id, 'retry.delaySeconds', parseFloat(e.target.value) || 0)}
                className="w-full bg-slate-700 text-white px-3 py-2 rounded border border-slate-600 focus:outline-none focus:border-blue-500 text-sm"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-300 mb-2">
                Backoff
              </label>
              <select
                value={data.retry?.backoff || 'fixed'}
                onChange={(e) => updateNestedData(selectedNode.id, 'retry.backoff', e.target.value as 'fixed' | 'exponential')}
                className="w-full bg-slate-700 text-white px-3 py-2 rounded border border-slate-600 focus:outline-none focus:border-blue-500 text-sm"
              >
                <option value="fixed">Fixed</option>
                <option value="exponential">Exponential</option>
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-300 mb-2">
                Max Delay Seconds
              </label>
              <input
                type="number"
                min="0"
                step="0.1"
                value={data.retry?.maxDelaySeconds ?? 60}
                onChange={(e) => updateNestedData(selectedNode.id, 'retry.maxDelaySeconds', parseFloat(e.target.value) || 0)}
                className="w-full bg-slate-700 text-white px-3 py-2 rounded border border-slate-600 focus:outline-none focus:border-blue-500 text-sm"
              />
            </div>
          </>
        )}

        {showBreakpoint && (
          <>
            <div className="pt-2 border-t border-slate-700">
              <h3 className="text-sm font-medium text-slate-300 mb-3 flex items-center gap-2">
                <Bug size={14} className="text-red-400" />
                Debug
              </h3>
            </div>
            <div className="flex items-center gap-3">
              <input
                type="checkbox"
                id="breakpoint"
                checked={data.breakpoint ?? false}
                onChange={(e) => updateNodeData(selectedNode.id, { breakpoint: e.target.checked })}
                className="w-4 h-4 rounded border-slate-600 bg-slate-700 text-blue-500 focus:ring-blue-500 focus:ring-offset-0"
              />
              <label htmlFor="breakpoint" className="text-sm text-slate-300 cursor-pointer">
                Enable Breakpoint
              </label>
            </div>
          </>
        )}

        {(type !== 'start' && type !== 'end') && (
          <button
            onClick={() => {
              deleteNode(selectedNode.id);
              setSelectedNodeId(null);
            }}
            className="w-full mt-4 px-4 py-2 bg-red-500/20 text-red-400 rounded hover:bg-red-500/30 transition-colors text-sm font-medium"
          >
            Delete Node
          </button>
        )}
      </div>
    </div>
  );
};
