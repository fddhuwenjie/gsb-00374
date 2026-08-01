import type { NodeType, EdgeHandle } from '../types/flow';

export const nodeTypeColors: Record<NodeType, { bg: string; border: string; text: string }> = {
  start: { bg: '#10b981', border: '#059669', text: '#ffffff' },
  end: { bg: '#ef4444', border: '#dc2626', text: '#ffffff' },
  task: { bg: '#3b82f6', border: '#2563eb', text: '#ffffff' },
  condition: { bg: '#f59e0b', border: '#d97706', text: '#1f2937' },
  loop: { bg: '#8b5cf6', border: '#7c3aed', text: '#ffffff' },
  wait: { bg: '#6b7280', border: '#4b5563', text: '#ffffff' },
  http: { bg: '#0ea5e9', border: '#0284c7', text: '#ffffff' },
  sql: { bg: '#a855f7', border: '#9333ea', text: '#ffffff' },
  parallel: { bg: '#f97316', border: '#ea580c', text: '#ffffff' },
  subflow: { bg: '#14b8a6', border: '#0d9488', text: '#ffffff' },
  trycatch: { bg: '#ec4899', border: '#db2777', text: '#ffffff' },
};

export const nodeTypeLabels: Record<NodeType, string> = {
  start: 'Start',
  end: 'End',
  task: 'Task',
  condition: 'Condition',
  loop: 'Loop',
  wait: 'Wait',
  http: 'HTTP',
  sql: 'SQL',
  parallel: 'Parallel',
  subflow: 'Subflow',
  trycatch: 'TryCatch',
};

export const nodeTypeDescriptions: Record<NodeType, string> = {
  start: 'Entry point of the flow',
  end: 'Exit point of the flow',
  task: 'Execute Python code in sandbox',
  condition: 'If-else branch based on expression',
  loop: 'While loop based on expression',
  wait: 'Wait for specified seconds',
  http: 'Send HTTP request',
  sql: 'Execute SQL query',
  parallel: 'Parallel fork-join branches',
  subflow: 'Execute nested subflow',
  trycatch: 'Try-catch error handling',
};

export const handleColors: Record<EdgeHandle | string, string> = {
  true: '#10b981',
  false: '#ef4444',
  loop: '#8b5cf6',
  catch: '#ec4899',
};

export const generateEdgeId = (source: string, target: string, handle?: string): string => {
  return `edge_${source}_${target}_${handle || 'default'}_${Date.now()}`;
};

export const formatValue = (value: any): string => {
  if (value === null) return 'null';
  if (value === undefined) return 'undefined';
  if (typeof value === 'string') return `"${value}"`;
  if (typeof value === 'object') return JSON.stringify(value);
  return String(value);
};

export const formatTimestamp = (timestamp: number): string => {
  return new Date(timestamp).toLocaleTimeString();
};
