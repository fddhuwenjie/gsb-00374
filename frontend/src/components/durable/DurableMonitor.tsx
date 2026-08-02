import React, { useState } from 'react';
import { Play, Pause, Square, RotateCw, Check, X } from 'lucide-react';
import { useDurableMonitor } from '../../hooks/useDurableMonitor';
import type { DurableCommand, DurableState } from '../../types/durable';

const STATE_COLORS: Record<DurableState, string> = {
  queued: 'text-slate-300',
  running: 'text-green-400',
  pausing: 'text-amber-400',
  paused: 'text-yellow-400',
  awaiting_approval: 'text-purple-400',
  retry_wait: 'text-orange-400',
  succeeded: 'text-blue-400',
  failed: 'text-red-400',
  cancelled: 'text-slate-400',
};

const COMMAND_META: Record<
  DurableCommand,
  { label: string; icon: React.ReactNode; cls: string }
> = {
  start: { label: 'Start', icon: <Play size={16} />, cls: 'text-green-400 hover:bg-green-500/20' },
  resume: { label: 'Resume', icon: <Play size={16} />, cls: 'text-green-400 hover:bg-green-500/20' },
  pause: { label: 'Pause', icon: <Pause size={16} />, cls: 'text-yellow-400 hover:bg-yellow-500/20' },
  approve: { label: 'Approve', icon: <Check size={16} />, cls: 'text-green-400 hover:bg-green-500/20' },
  reject: { label: 'Reject', icon: <X size={16} />, cls: 'text-red-400 hover:bg-red-500/20' },
  cancel: { label: 'Cancel', icon: <Square size={16} />, cls: 'text-red-400 hover:bg-red-500/20' },
};

const COMMAND_ORDER: DurableCommand[] = ['start', 'resume', 'pause', 'approve', 'reject', 'cancel'];

function fmtDeadline(deadline?: number | null): string {
  if (deadline == null) return 'no deadline';
  const secs = Math.round(deadline - Date.now() / 1000);
  return secs > 0 ? `expires in ${secs}s` : 'expired';
}

/**
 * Durable execution monitor panel.
 *
 * Buttons are rendered strictly from the server-provided `allowedCommands`, so
 * the control surface is driven by the server's state machine rather than
 * client-side guesses. Approvals are acted on per-token (approve/reject) so the
 * server can reject stale / expired / duplicate responses. The event stream is
 * rendered by monotonic `seq`.
 */
export const DurableMonitor: React.FC = () => {
  const [inputId, setInputId] = useState('');
  const [executionId, setExecutionId] = useState<string | null>(null);
  const {
    state,
    variables,
    events,
    allowedCommands,
    connected,
    flowVersion,
    flowId,
    pendingApprovals,
    sendCommand,
    respondApproval,
  } = useDurableMonitor(executionId);

  return (
    <div className="h-full flex flex-col p-3 gap-3 text-sm overflow-hidden">
      <div className="flex items-center gap-2">
        <input
          value={inputId}
          onChange={(e) => setInputId(e.target.value)}
          placeholder="Execution ID (e.g. dexec_...)"
          className="bg-slate-700 text-white px-3 py-1.5 rounded border border-slate-600 focus:outline-none focus:border-blue-500 flex-1"
        />
        <button
          onClick={() => setExecutionId(inputId.trim() || null)}
          className="px-3 py-1.5 rounded bg-blue-600 hover:bg-blue-500 text-white flex items-center gap-1"
        >
          <RotateCw size={14} /> Attach
        </button>
        <div className="flex items-center gap-1 text-xs">
          <span
            className="w-2 h-2 rounded-full"
            style={{ backgroundColor: connected ? '#10b981' : '#ef4444' }}
          />
          {connected ? 'live' : 'offline'}
        </div>
      </div>

      <div className="flex items-center gap-3">
        <span className="text-slate-400">State:</span>
        <span className={`font-semibold ${state ? STATE_COLORS[state] : 'text-slate-500'}`}>
          {state ?? '—'}
        </span>

        {flowVersion != null && (
          <span
            className="text-xs bg-indigo-500/20 text-indigo-300 px-2 py-0.5 rounded"
            title={`Bound to immutable ${flowId ?? 'flow'} v${flowVersion}`}
          >
            {flowId ? `${flowId} ` : ''}v{flowVersion}
          </span>
        )}

        <div className="flex items-center gap-1 ml-4 bg-slate-700/40 rounded-lg p-1">
          {COMMAND_ORDER.filter((c) => allowedCommands.includes(c)).map((cmd) => {
            const meta = COMMAND_META[cmd];
            // approve/reject need a token, so route them through the first
            // pending approval when there is exactly one; otherwise the
            // per-approval buttons below are the way to act on a specific one.
            const isApprovalCmd = cmd === 'approve' || cmd === 'reject';
            const onClick = () => {
              if (isApprovalCmd && pendingApprovals.length === 1) {
                void respondApproval(
                  pendingApprovals[0].token,
                  cmd === 'approve' ? 'approved' : 'rejected'
                );
              } else if (!isApprovalCmd) {
                void sendCommand(cmd);
              }
            };
            return (
              <button
                key={cmd}
                onClick={onClick}
                disabled={isApprovalCmd && pendingApprovals.length !== 1}
                className={`p-1.5 rounded flex items-center gap-1 transition-colors ${meta.cls} disabled:opacity-40`}
                title={meta.label}
              >
                {meta.icon}
                <span className="text-xs">{meta.label}</span>
              </button>
            );
          })}
          {allowedCommands.length === 0 && (
            <span className="text-xs text-slate-500 px-2">no actions</span>
          )}
        </div>
      </div>

      {pendingApprovals.length > 0 && (
        <div className="bg-purple-500/10 border border-purple-500/40 rounded p-2">
          <div className="text-purple-300 text-xs mb-1 font-semibold">
            Pending approvals ({pendingApprovals.length})
          </div>
          <div className="flex flex-col gap-1">
            {pendingApprovals.map((a) => (
              <div key={a.approvalId} className="flex items-center gap-2 text-xs">
                <span className="text-slate-300 flex-1 truncate">
                  {a.nodeId}
                  {a.branchId ? `/${a.branchId}` : ''} @gen{a.generation} —{' '}
                  <span className="text-purple-300">{fmtDeadline(a.deadline)}</span>
                </span>
                <button
                  onClick={() => respondApproval(a.token, 'approved')}
                  className="px-2 py-0.5 rounded text-green-400 hover:bg-green-500/20 flex items-center gap-1"
                >
                  <Check size={12} /> Approve
                </button>
                <button
                  onClick={() => respondApproval(a.token, 'rejected')}
                  className="px-2 py-0.5 rounded text-red-400 hover:bg-red-500/20 flex items-center gap-1"
                >
                  <X size={12} /> Reject
                </button>
              </div>
            ))}
          </div>
        </div>
      )}

      <div className="flex-1 grid grid-cols-2 gap-3 overflow-hidden">
        <div className="flex flex-col overflow-hidden">
          <div className="text-slate-400 text-xs mb-1">Event log (by seq)</div>
          <div className="flex-1 overflow-auto bg-slate-900/60 rounded border border-slate-700 font-mono text-xs">
            {events.map((ev) => (
              <div
                key={ev.seq}
                className="px-2 py-1 border-b border-slate-800 flex gap-2"
              >
                <span className="text-slate-500 w-8 text-right">#{ev.seq}</span>
                <span className="text-cyan-400 w-36">{ev.kind}</span>
                <span className="text-slate-300 flex-1 truncate">
                  {ev.kind === 'state'
                    ? `${ev.prevState} → ${ev.state}`
                    : ev.kind === 'approval_resolved'
                    ? `${ev.nodeId}${ev.branchId ? `/${ev.branchId}` : ''} → ${ev.decision}${
                        ev.approver ? ` by ${ev.approver}` : ''
                      }`
                    : ev.nodeId
                    ? `${ev.nodeId}${ev.attempt ? ` #${ev.attempt}` : ''}${
                        ev.branchId ? `/${ev.branchId}@gen${ev.generation}` : ''
                      }`
                    : ev.key
                    ? `effect ${String(ev.key).slice(0, 10)}…`
                    : ''}
                </span>
              </div>
            ))}
            {events.length === 0 && (
              <div className="p-3 text-slate-500">No events. Attach an execution.</div>
            )}
          </div>
        </div>

        <div className="flex flex-col overflow-hidden">
          <div className="text-slate-400 text-xs mb-1">Variables</div>
          <pre className="flex-1 overflow-auto bg-slate-900/60 rounded border border-slate-700 p-2 text-xs text-green-300">
            {JSON.stringify(variables, null, 2)}
          </pre>
        </div>
      </div>
    </div>
  );
};
