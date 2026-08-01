import React from 'react';
import { Handle, Position, NodeProps } from 'reactflow';
import { nodeTypeColors } from '../../utils/flowUtils';
import type { NodeData } from '../../types/flow';

interface CustomNodeProps extends NodeProps<NodeData> {
  isActive?: boolean;
}

export const StartNode: React.FC<CustomNodeProps> = ({ data, selected, isActive }) => {
  const colors = nodeTypeColors.start;
  return (
    <div
      className={`relative px-4 py-2 rounded-full font-semibold text-sm transition-all duration-200
        ${selected ? 'ring-2 ring-white ring-offset-2 ring-offset-slate-900' : ''}
        ${isActive ? 'animate-pulse ring-4 ring-blue-400' : ''}
      `}
      style={{
        backgroundColor: colors.bg,
        color: colors.text,
        minWidth: '80px',
        textAlign: 'center',
      }}
    >
      <Handle
        type="source"
        position={Position.Right}
        className="!w-3 !h-3 !bg-white !border-2 !border-slate-700"
      />
      {data.label}
    </div>
  );
};

export const EndNode: React.FC<CustomNodeProps> = ({ data, selected, isActive }) => {
  const colors = nodeTypeColors.end;
  return (
    <div
      className={`relative px-4 py-2 rounded-full font-semibold text-sm transition-all duration-200
        ${selected ? 'ring-2 ring-white ring-offset-2 ring-offset-slate-900' : ''}
        ${isActive ? 'animate-pulse ring-4 ring-blue-400' : ''}
      `}
      style={{
        backgroundColor: colors.bg,
        color: colors.text,
        minWidth: '80px',
        textAlign: 'center',
      }}
    >
      <Handle
        type="target"
        position={Position.Left}
        className="!w-3 !h-3 !bg-white !border-2 !border-slate-700"
      />
      {data.label}
    </div>
  );
};

export const TaskNode: React.FC<CustomNodeProps> = ({ data, selected, isActive }) => {
  const colors = nodeTypeColors.task;
  return (
    <div
      className={`relative px-4 py-3 rounded-lg font-medium text-sm transition-all duration-200 min-w-[140px]
        ${selected ? 'ring-2 ring-white ring-offset-2 ring-offset-slate-900' : ''}
        ${isActive ? 'ring-4 ring-blue-400' : ''}
      `}
      style={{
        backgroundColor: colors.bg,
        color: colors.text,
        borderLeft: `4px solid ${colors.border}`,
      }}
    >
      <Handle
        type="target"
        position={Position.Left}
        className="!w-3 !h-3 !bg-white !border-2 !border-slate-700"
      />
      <div className="font-semibold">{data.label}</div>
      {data.code && (
        <div className="text-xs mt-1 opacity-80 truncate max-w-[160px] font-mono">
          {data.code.split('\n')[0]}
        </div>
      )}
      <Handle
        type="source"
        position={Position.Right}
        className="!w-3 !h-3 !bg-white !border-2 !border-slate-700"
      />
    </div>
  );
};

export const ConditionNode: React.FC<CustomNodeProps> = ({ data, selected, isActive }) => {
  const colors = nodeTypeColors.condition;
  return (
    <div
      className={`relative transition-all duration-200
        ${selected ? 'ring-2 ring-white ring-offset-2 ring-offset-slate-900' : ''}
        ${isActive ? 'ring-4 ring-blue-400' : ''}
      `}
      style={{
        minWidth: '160px',
      }}
    >
      <Handle
        type="target"
        position={Position.Left}
        className="!w-3 !h-3 !bg-white !border-2 !border-slate-700"
      />
      <div
        className="px-4 py-4 font-medium text-sm text-center"
        style={{
          backgroundColor: colors.bg,
          color: colors.text,
          clipPath: 'polygon(10% 0%, 90% 0%, 100% 50%, 90% 100%, 10% 100%, 0% 50%)',
        }}
      >
        <div className="font-semibold">{data.label}</div>
        {data.expression && (
          <div className="text-xs mt-1 font-mono px-2">
            {data.expression}
          </div>
        )}
      </div>
      <div className="flex justify-between px-2 mt-1">
        <span className="text-xs text-green-400">True</span>
        <span className="text-xs text-red-400">False</span>
      </div>
      <Handle
        type="source"
        position={Position.Right}
        id="true"
        className="!w-3 !h-3 !bg-green-500 !border-2 !border-slate-700 !top-[40%]"
      />
      <Handle
        type="source"
        position={Position.Right}
        id="false"
        className="!w-3 !h-3 !bg-red-500 !border-2 !border-slate-700 !top-[60%]"
      />
    </div>
  );
};

export const LoopNode: React.FC<CustomNodeProps> = ({ data, selected, isActive }) => {
  const colors = nodeTypeColors.loop;
  return (
    <div
      className={`relative px-4 py-3 rounded-lg font-medium text-sm transition-all duration-200 min-w-[140px]
        ${selected ? 'ring-2 ring-white ring-offset-2 ring-offset-slate-900' : ''}
        ${isActive ? 'ring-4 ring-blue-400' : ''}
      `}
      style={{
        backgroundColor: colors.bg,
        color: colors.text,
        borderTop: '4px solid ' + colors.border,
        borderBottom: '4px solid ' + colors.border,
      }}
    >
      <Handle
        type="target"
        position={Position.Left}
        className="!w-3 !h-3 !bg-white !border-2 !border-slate-700"
      />
      <div className="font-semibold">{data.label}</div>
      {data.expression && (
        <div className="text-xs mt-1 opacity-80 font-mono">
          {data.expression}
        </div>
      )}
      <Handle
        type="source"
        position={Position.Top}
        id="loop"
        className="!w-3 !h-3 !bg-purple-400 !border-2 !border-slate-700"
      />
      <Handle
        type="source"
        position={Position.Right}
        className="!w-3 !h-3 !bg-white !border-2 !border-slate-700"
      />
    </div>
  );
};

export const WaitNode: React.FC<CustomNodeProps> = ({ data, selected, isActive }) => {
  const colors = nodeTypeColors.wait;
  return (
    <div
      className={`relative px-4 py-3 rounded-lg font-medium text-sm transition-all duration-200 min-w-[120px]
        ${selected ? 'ring-2 ring-white ring-offset-2 ring-offset-slate-900' : ''}
        ${isActive ? 'ring-4 ring-blue-400' : ''}
      `}
      style={{
        backgroundColor: colors.bg,
        color: colors.text,
        borderRadius: '20px',
      }}
    >
      <Handle
        type="target"
        position={Position.Left}
        className="!w-3 !h-3 !bg-white !border-2 !border-slate-700"
      />
      <div className="text-center">
        <div className="font-semibold">{data.label}</div>
        {data.seconds !== undefined && (
          <div className="text-xs mt-1 opacity-80">
            {data.seconds}s
          </div>
        )}
      </div>
      <Handle
        type="source"
        position={Position.Right}
        className="!w-3 !h-3 !bg-white !border-2 !border-slate-700"
      />
    </div>
  );
};

export const HttpNode: React.FC<CustomNodeProps> = ({ data, selected, isActive }) => {
  const colors = nodeTypeColors.http;
  return (
    <div
      className={`relative px-4 py-3 rounded-lg font-medium text-sm transition-all duration-200 min-w-[140px]
        ${selected ? 'ring-2 ring-white ring-offset-2 ring-offset-slate-900' : ''}
        ${isActive ? 'ring-4 ring-blue-400' : ''}
        ${data.breakpoint ? 'ring-2 ring-red-500' : ''}
      `}
      style={{
        backgroundColor: colors.bg,
        color: colors.text,
        borderLeft: `4px solid ${colors.border}`,
      }}
    >
      {data.breakpoint && (
        <div className="absolute -top-2 -right-2 w-4 h-4 bg-red-500 rounded-full border-2 border-white flex items-center justify-center text-[10px]">
          ●
        </div>
      )}
      <Handle
        type="target"
        position={Position.Left}
        className="!w-3 !h-3 !bg-white !border-2 !border-slate-700"
      />
      <div className="font-semibold flex items-center gap-2">
        <span>🌐</span>
        {data.label}
      </div>
      {data.httpConfig?.url && (
        <div className="text-xs mt-1 opacity-80 truncate max-w-[160px] font-mono">
          {data.httpConfig.method} {data.httpConfig.url.substring(0, 30)}...
        </div>
      )}
      <Handle
        type="source"
        position={Position.Right}
        className="!w-3 !h-3 !bg-white !border-2 !border-slate-700"
      />
    </div>
  );
};

export const SqlNode: React.FC<CustomNodeProps> = ({ data, selected, isActive }) => {
  const colors = nodeTypeColors.sql;
  return (
    <div
      className={`relative px-4 py-3 rounded-lg font-medium text-sm transition-all duration-200 min-w-[140px]
        ${selected ? 'ring-2 ring-white ring-offset-2 ring-offset-slate-900' : ''}
        ${isActive ? 'ring-4 ring-blue-400' : ''}
        ${data.breakpoint ? 'ring-2 ring-red-500' : ''}
      `}
      style={{
        backgroundColor: colors.bg,
        color: colors.text,
        borderLeft: `4px solid ${colors.border}`,
      }}
    >
      {data.breakpoint && (
        <div className="absolute -top-2 -right-2 w-4 h-4 bg-red-500 rounded-full border-2 border-white flex items-center justify-center text-[10px]">
          ●
        </div>
      )}
      <Handle
        type="target"
        position={Position.Left}
        className="!w-3 !h-3 !bg-white !border-2 !border-slate-700"
      />
      <div className="font-semibold flex items-center gap-2">
        <span>🗄️</span>
        {data.label}
      </div>
      {data.sqlConfig?.query && (
        <div className="text-xs mt-1 opacity-80 truncate max-w-[160px] font-mono">
          {data.sqlConfig.query.substring(0, 30)}...
        </div>
      )}
      <Handle
        type="source"
        position={Position.Right}
        className="!w-3 !h-3 !bg-white !border-2 !border-slate-700"
      />
    </div>
  );
};

export const ParallelNode: React.FC<CustomNodeProps> = ({ data, selected, isActive }) => {
  const colors = nodeTypeColors.parallel;
  return (
    <div
      className={`relative px-4 py-3 rounded-lg font-medium text-sm transition-all duration-200 min-w-[160px]
        ${selected ? 'ring-2 ring-white ring-offset-2 ring-offset-slate-900' : ''}
        ${isActive ? 'ring-4 ring-blue-400' : ''}
        ${data.breakpoint ? 'ring-2 ring-red-500' : ''}
      `}
      style={{
        backgroundColor: colors.bg,
        color: colors.text,
        borderTop: `4px solid ${colors.border}`,
        borderBottom: `4px solid ${colors.border}`,
      }}
    >
      {data.breakpoint && (
        <div className="absolute -top-2 -right-2 w-4 h-4 bg-red-500 rounded-full border-2 border-white flex items-center justify-center text-[10px]">
          ●
        </div>
      )}
      <Handle
        type="target"
        position={Position.Left}
        className="!w-3 !h-3 !bg-white !border-2 !border-slate-700"
      />
      <div className="font-semibold flex items-center gap-2">
        <span>⚡</span>
        {data.label}
      </div>
      {data.parallelConfig?.branchNodeIds && (
        <div className="text-xs mt-1 opacity-80">
          {data.parallelConfig.branchNodeIds.length} branches
        </div>
      )}
      <Handle
        type="source"
        position={Position.Right}
        className="!w-3 !h-3 !bg-white !border-2 !border-slate-700"
      />
    </div>
  );
};

export const SubflowNode: React.FC<CustomNodeProps> = ({ data, selected, isActive }) => {
  const colors = nodeTypeColors.subflow;
  return (
    <div
      className={`relative px-4 py-3 rounded-lg font-medium text-sm transition-all duration-200 min-w-[140px]
        ${selected ? 'ring-2 ring-white ring-offset-2 ring-offset-slate-900' : ''}
        ${isActive ? 'ring-4 ring-blue-400' : ''}
        ${data.breakpoint ? 'ring-2 ring-red-500' : ''}
      `}
      style={{
        backgroundColor: colors.bg,
        color: colors.text,
        border: `2px dashed ${colors.border}`,
      }}
    >
      {data.breakpoint && (
        <div className="absolute -top-2 -right-2 w-4 h-4 bg-red-500 rounded-full border-2 border-white flex items-center justify-center text-[10px]">
          ●
        </div>
      )}
      <Handle
        type="target"
        position={Position.Left}
        className="!w-3 !h-3 !bg-white !border-2 !border-slate-700"
      />
      <div className="font-semibold flex items-center gap-2">
        <span>📦</span>
        {data.label}
      </div>
      {data.subflowConfig?.subflowId && (
        <div className="text-xs mt-1 opacity-80 truncate max-w-[160px] font-mono">
          → {data.subflowConfig.subflowId}
        </div>
      )}
      <Handle
        type="source"
        position={Position.Right}
        className="!w-3 !h-3 !bg-white !border-2 !border-slate-700"
      />
    </div>
  );
};

export const TryCatchNode: React.FC<CustomNodeProps> = ({ data, selected, isActive }) => {
  const colors = nodeTypeColors.trycatch;
  return (
    <div
      className={`relative px-4 py-3 rounded-lg font-medium text-sm transition-all duration-200 min-w-[160px]
        ${selected ? 'ring-2 ring-white ring-offset-2 ring-offset-slate-900' : ''}
        ${isActive ? 'ring-4 ring-blue-400' : ''}
        ${data.breakpoint ? 'ring-2 ring-red-500' : ''}
      `}
      style={{
        backgroundColor: colors.bg,
        color: colors.text,
        borderLeft: `4px solid ${colors.border}`,
      }}
    >
      {data.breakpoint && (
        <div className="absolute -top-2 -right-2 w-4 h-4 bg-red-500 rounded-full border-2 border-white flex items-center justify-center text-[10px]">
          ●
        </div>
      )}
      <Handle
        type="target"
        position={Position.Left}
        className="!w-3 !h-3 !bg-white !border-2 !border-slate-700"
      />
      <div className="font-semibold flex items-center gap-2">
        <span>🛡️</span>
        {data.label}
      </div>
      <div className="text-xs mt-1 opacity-80 flex gap-2">
        <span className="text-green-300">✓ Try</span>
        <span className="text-red-300">✕ Catch</span>
      </div>
      <Handle
        type="source"
        position={Position.Right}
        id="true"
        className="!w-3 !h-3 !bg-green-400 !border-2 !border-slate-700 !top-[40%]"
      />
      <Handle
        type="source"
        position={Position.Right}
        id="catch"
        className="!w-3 !h-3 !bg-pink-500 !border-2 !border-slate-700 !top-[60%]"
      />
    </div>
  );
};
