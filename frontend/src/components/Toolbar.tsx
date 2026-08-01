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
} from 'lucide-react';
import type { ExecutionStatus } from '../types/flow';

interface ToolbarProps {
  onRun: () => void;
  onPause: () => void;
  onResume: () => void;
  onStep: () => void;
  onStop: () => void;
  onSave: () => void;
  onLoad: () => void;
  onExport: () => void;
  onClear: () => void;
  status: ExecutionStatus;
  wsConnected: boolean;
  flowName: string;
  onFlowNameChange: (name: string) => void;
}

export const Toolbar: React.FC<ToolbarProps> = ({
  onRun,
  onPause,
  onResume,
  onStep,
  onStop,
  onSave,
  onLoad,
  onExport,
  onClear,
  status,
  wsConnected,
  flowName,
  onFlowNameChange,
}) => {
  const isRunning = status === 'running';
  const isPaused = status === 'paused';
  const isIdle = status === 'idle' || status === 'completed' || status === 'stopped' || status === 'error';

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
          <span
            className={`font-semibold ${
              status === 'running'
                ? 'text-green-400'
                : status === 'paused'
                ? 'text-yellow-400'
                : status === 'completed'
                ? 'text-blue-400'
                : status === 'error'
                ? 'text-red-400'
                : status === 'stopped'
                ? 'text-slate-400'
                : 'text-slate-300'
            }`}
          >
            {status}
          </span>
        </div>
      </div>

      <div className="flex items-center gap-2">
        <div className="flex items-center gap-1 bg-slate-700/50 rounded-lg p-1">
          {isIdle && (
            <button
              onClick={onRun}
              className="p-2 rounded hover:bg-green-500/20 text-green-400 transition-colors"
              title="Run"
            >
              <Play size={18} />
            </button>
          )}

          {isRunning && (
            <button
              onClick={onPause}
              className="p-2 rounded hover:bg-yellow-500/20 text-yellow-400 transition-colors"
              title="Pause"
            >
              <Pause size={18} />
            </button>
          )}

          {isPaused && (
            <button
              onClick={onResume}
              className="p-2 rounded hover:bg-green-500/20 text-green-400 transition-colors"
              title="Resume"
            >
              <Play size={18} />
            </button>
          )}

          {(isRunning || isPaused) && (
            <button
              onClick={onStep}
              className="p-2 rounded hover:bg-blue-500/20 text-blue-400 transition-colors"
              title="Step"
            >
              <SkipForward size={18} />
            </button>
          )}

          {(isRunning || isPaused) && (
            <button
              onClick={onStop}
              className="p-2 rounded hover:bg-red-500/20 text-red-400 transition-colors"
              title="Stop"
            >
              <Square size={18} />
            </button>
          )}

          {!isIdle && (
            <button
              onClick={onRun}
              className="p-2 rounded hover:bg-blue-500/20 text-blue-400 transition-colors"
              title="Restart"
            >
              <RotateCcw size={18} />
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
