import React from 'react';
import { Play, Square, GitBranch, RotateCcw, Clock, CircleDot, CircleOff, Globe, Database, Zap, Boxes, Shield } from 'lucide-react';
import type { NodeType } from '../types/flow';
import { nodeTypeColors, nodeTypeLabels, nodeTypeDescriptions } from '../utils/flowUtils';

interface SidebarProps {
  onDragStart: (event: React.DragEvent, nodeType: NodeType) => void;
}

const nodeIcons: Record<NodeType, React.ReactNode> = {
  start: <Play size={16} />,
  end: <Square size={16} />,
  task: <CircleDot size={16} />,
  condition: <GitBranch size={16} />,
  loop: <RotateCcw size={16} />,
  wait: <Clock size={16} />,
  http: <Globe size={16} />,
  sql: <Database size={16} />,
  parallel: <Zap size={16} />,
  subflow: <Boxes size={16} />,
  trycatch: <Shield size={16} />,
};

const nodeTypes: NodeType[] = ['start', 'end', 'task', 'condition', 'loop', 'wait', 'http', 'sql', 'parallel', 'subflow', 'trycatch'];

export const Sidebar: React.FC<SidebarProps> = ({ onDragStart }) => {
  return (
    <div className="w-60 bg-slate-800 border-r border-slate-700 p-4 overflow-y-auto">
      <h2 className="text-white font-bold text-lg mb-4 flex items-center gap-2">
        <CircleOff size={20} className="text-blue-400" />
        Node Palette
      </h2>
      <div className="space-y-3">
        {nodeTypes.map((type) => {
          const colors = nodeTypeColors[type];
          return (
            <div
              key={type}
              draggable
              onDragStart={(e) => onDragStart(e, type)}
              className="cursor-grab active:cursor-grabbing p-3 rounded-lg border-2 transition-all duration-200 hover:scale-105 hover:shadow-lg"
              style={{
                backgroundColor: colors.bg + '20',
                borderColor: colors.border,
              }}
            >
              <div
                className="flex items-center gap-2 font-semibold text-sm mb-1"
                style={{ color: colors.bg }}
              >
                {nodeIcons[type]}
                {nodeTypeLabels[type]}
              </div>
              <div className="text-xs text-slate-400">
                {nodeTypeDescriptions[type]}
              </div>
            </div>
          );
        })}
      </div>
      <div className="mt-6 p-3 bg-slate-700/50 rounded-lg">
        <h3 className="text-white font-semibold text-sm mb-2">Tips</h3>
        <ul className="text-xs text-slate-400 space-y-1">
          <li>• Drag nodes to canvas</li>
          <li>• Click and drag handles to connect</li>
          <li>• Double-click to edit node</li>
          <li>• Press Delete to remove</li>
        </ul>
      </div>
    </div>
  );
};
