import React from 'react';
import {
  Play,
  Pause,
  SkipForward,
  Square,
  Save,
  FolderOpen,
  Download,
  Trash2,
  Wifi,
  WifiOff,
  RotateCcw,
  PlayCircle,
} from 'lucide-react';
import type { ExecutionStatus, CommandType } from '../types/flow';

interface ToolbarProps {
  onRun: () => void;
  onPause: () => void;
  onResume: () => void;
  onCancel: () => void;
  onRetry: () => void;
  onStep: () => void;
  onSave: () => void;
  onLoad: () => void;
  onExport: () => void;
  onClear: () => void;
  status: ExecutionStatus;
  allowedActions: CommandType[];
  wsConnected: boolean;
  flowName: string;
  onFlowNameChange: (name: string) => void;
}

const statusColors: Record<string, string> = {
  running: 'text-green-400',
  pausing: 'text-orange-400',
  paused: 'text-yellow-400',
  awaiting_approval: 'text-purple-400',
  retry_wait: 'text-orange-400',
  queued: 'text-blue-400',
  succeeded: 'text-emerald-400',
  failed: 'text-red-400',
  cancelled: 'text-slate-400',
  completed: 'text-blue-400',
  error: 'text-red-400',
  stopped: 'text-slate-400',
  idle: 'text-slate-300',
};

export const Toolbar: React.FC<ToolbarProps> = ({
  onRun,
  onPause,
  onResume,
  onCancel,
  onRetry,
  onStep,
  onSave,
  onLoad,
  onExport,
  onClear,
  status,
  allowedActions,
  wsConnected,
  flowName,
  onFlowNameChange,
}) => {
  const canStart = allowedActions.includes('start');
  const canPause = allowedActions.includes('pause');
  const canResume = allowedActions.includes('resume');
  const canCancel = allowedActions.includes('cancel');
  const canRetry = allowedActions.includes('retry');
  const isIdle = status === 'idle';

  return (
    <div className="h-14 bg-slate-800 border-b border-slate-700 px-4 flex items-center justify-between">
      <div className="flex items-center gap-4">
        <div className="flex items-center gap-2">
          <div
            className="w-3 h-3 rounded-full"
            style={{ backgroundColor: wsConnected ? '#10b981' : '#ef4444' }}
          />
          <span className="text-slate-400 text-sm flex items-center gap-1">
            {wsConnected ? <Wifi size={14} /> : <WifiOff size={14} />}
            {wsConnected ? 'Connected' : 'Disconnected'}
          </span>
        </div>

        <div className="h-6 w-px bg-slate-600" />

        <input
          type="text"
          value={flowName}
          onChange={(e) => onFlowNameChange(e.target.value)}
          className="bg-slate-700 text-white px-3 py-1.5 rounded text-sm border border-slate-600 focus:outline-none focus:border-blue-500 w-48"
          placeholder="Flow name..."
        />

        <div className="text-slate-500 text-sm px-2">
          Status:{' '}
          <span className={`font-semibold ${statusColors[status] || 'text-slate-300'}`}>
            {status}
          </span>
        </div>
      </div>

      <div className="flex items-center gap-2">
        <div className="flex items-center gap-1 bg-slate-700/50 rounded-lg p-1">
          {(isIdle || canStart) && (
            <button
              onClick={onRun}
              className="p-2 rounded hover:bg-green-500/20 text-green-400 transition-colors"
              title="Run"
            >
              <Play size={18} />
            </button>
          )}

          {canPause && (
            <button
              onClick={onPause}
              className="p-2 rounded hover:bg-yellow-500/20 text-yellow-400 transition-colors"
              title="Pause"
            >
              <Pause size={18} />
            </button>
          )}

          {canResume && (
            <button
              onClick={onResume}
              className="p-2 rounded hover:bg-green-500/20 text-green-400 transition-colors"
              title="Resume"
            >
              <PlayCircle size={18} />
            </button>
          )}

          {canCancel && (
            <button
              onClick={onCancel}
              className="p-2 rounded hover:bg-red-500/20 text-red-400 transition-colors"
              title="Cancel"
            >
              <Square size={18} />
            </button>
          )}

          {canRetry && (
            <button
              onClick={onRetry}
              className="p-2 rounded hover:bg-blue-500/20 text-blue-400 transition-colors"
              title="Retry"
            >
              <RotateCcw size={18} />
            </button>
          )}

          {(canPause || canResume) && (
            <button
              onClick={onStep}
              className="p-2 rounded hover:bg-blue-500/20 text-blue-400 transition-colors"
              title="Step"
            >
              <SkipForward size={18} />
            </button>
          )}
        </div>

        <div className="h-6 w-px bg-slate-600" />

        <div className="flex items-center gap-1">
          <button
            onClick={onSave}
            className="p-2 rounded hover:bg-blue-500/20 text-blue-400 transition-colors"
            title="Save"
          >
            <Save size={18} />
          </button>
          <button
            onClick={onLoad}
            className="p-2 rounded hover:bg-blue-500/20 text-blue-400 transition-colors"
            title="Load"
          >
            <FolderOpen size={18} />
          </button>
          <button
            onClick={onExport}
            className="p-2 rounded hover:bg-blue-500/20 text-blue-400 transition-colors"
            title="Export JSON"
          >
            <Download size={18} />
          </button>
          <button
            onClick={onClear}
            className="p-2 rounded hover:bg-red-500/20 text-red-400 transition-colors"
            title="Clear Canvas"
          >
            <Trash2 size={18} />
          </button>
        </div>
      </div>
    </div>
  );
};
