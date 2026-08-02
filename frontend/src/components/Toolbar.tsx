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
  Server,
  X,
} from 'lucide-react';
import type { ExecutionStatus, V2Command, V2ExecutionStatus } from '../types/flow';

export interface ServerControl {
  status: V2ExecutionStatus | null;
  allowedCommands: V2Command[];
  onCommand: (command: V2Command) => void;
  onDetach: () => void;
}

interface ToolbarProps {
  onRun: () => void;
  onRunPersistent: () => void;
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
  serverControl?: ServerControl | null;
}

export const Toolbar: React.FC<ToolbarProps> = ({
  onRun,
  onRunPersistent,
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
  serverControl,
}) => {
  const isRunning = status === 'running';
  const isPaused = status === 'paused';
  const isIdle = status === 'idle' || status === 'completed' || status === 'stopped' || status === 'error';

  // Server-driven mode: every button's enabled/visible state comes from the
  // allowed command set reported by the server, never from local inference.
  const serverMode = serverControl != null;
  const allowed = new Set(serverControl?.allowedCommands ?? []);
  const displayStatus = serverMode ? serverControl.status ?? 'queued' : status;

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
              displayStatus === 'running'
                ? 'text-green-400'
                : displayStatus === 'paused' || displayStatus === 'pausing'
                ? 'text-yellow-400'
                : displayStatus === 'completed' || displayStatus === 'succeeded'
                ? 'text-blue-400'
                : displayStatus === 'error' || displayStatus === 'failed'
                ? 'text-red-400'
                : displayStatus === 'stopped' || displayStatus === 'cancelled'
                ? 'text-slate-400'
                : displayStatus === 'retry_wait'
                ? 'text-orange-400'
                : 'text-slate-300'
            }`}
          >
            {displayStatus}
            {serverMode && <span className="text-slate-500 text-xs ml-1">(persistent)</span>}
          </span>
        </div>
      </div>

      <div className="flex items-center gap-2">
        <div className="flex items-center gap-1 bg-slate-700/50 rounded-lg p-1">
          {serverMode ? (
            <>
              {allowed.has('pause') && (
                <button
                  onClick={() => serverControl.onCommand('pause')}
                  className="p-2 rounded hover:bg-yellow-500/20 text-yellow-400 transition-colors"
                  title="Pause (persistent)"
                >
                  <Pause size={18} />
                </button>
              )}
              {allowed.has('resume') && (
                <button
                  onClick={() => serverControl.onCommand('resume')}
                  className="p-2 rounded hover:bg-green-500/20 text-green-400 transition-colors"
                  title="Resume (persistent)"
                >
                  <Play size={18} />
                </button>
              )}
              {allowed.has('retry') && (
                <button
                  onClick={() => serverControl.onCommand('retry')}
                  className="p-2 rounded hover:bg-blue-500/20 text-blue-400 transition-colors"
                  title="Retry failed execution"
                >
                  <RotateCcw size={18} />
                </button>
              )}
              {allowed.has('cancel') && (
                <button
                  onClick={() => serverControl.onCommand('cancel')}
                  className="p-2 rounded hover:bg-red-500/20 text-red-400 transition-colors"
                  title="Cancel (persistent)"
                >
                  <Square size={18} />
                </button>
              )}
              <button
                onClick={serverControl.onDetach}
                className="p-2 rounded hover:bg-slate-500/20 text-slate-400 transition-colors"
                title="Detach monitor"
              >
                <X size={18} />
              </button>
            </>
          ) : (
            <>
              {isIdle && (
                <>
                  <button
                    onClick={onRun}
                    className="p-2 rounded hover:bg-green-500/20 text-green-400 transition-colors"
                    title="Run"
                  >
                    <Play size={18} />
                  </button>
                  <button
                    onClick={onRunPersistent}
                    className="p-2 rounded hover:bg-emerald-500/20 text-emerald-400 transition-colors"
                    title="Run persistent (resumable, idempotent)"
                  >
                    <Server size={18} />
                  </button>
                </>
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
            </>
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
