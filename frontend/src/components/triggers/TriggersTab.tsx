import React, { useState, useEffect, useCallback } from 'react';
import { Plus, Trash2, Play, Pause, Clock, Globe, GitMerge, ChevronRight } from 'lucide-react';
import type { Trigger } from '../../types/flow';
import { nodeTypeLabels } from '../../utils/flowUtils';

const API_URL = '/api';

interface TriggersTabProps {
  flowId: string;
  flows: Array<{ id: string; name: string }>;
}

export const TriggersTab: React.FC<TriggersTabProps> = ({ flowId, flows }) => {
  const [triggers, setTriggers] = useState<Trigger[]>([]);
  const [showForm, setShowForm] = useState(false);
  const [editingTrigger, setEditingTrigger] = useState<Trigger | null>(null);
  const [formData, setFormData] = useState({
    type: 'cron' as 'cron' | 'webhook' | 'flow_completed',
    cronExpression: '*/5 * * * *',
    webhookPath: '',
    sourceFlowId: '',
    enabled: true,
  });
  const [cronValidation, setCronValidation] = useState<{ valid: boolean; nextRuns?: string[]; error?: string } | null>(null);

  const fetchTriggers = useCallback(async () => {
    try {
      const res = await fetch(`${API_URL}/triggers?flowId=${flowId}`);
      if (res.ok) {
        const data = await res.json();
        setTriggers(data);
      }
    } catch (e) {
      console.error('Failed to fetch triggers:', e);
    }
  }, [flowId]);

  useEffect(() => {
    if (flowId) {
      fetchTriggers();
    }
  }, [flowId, fetchTriggers]);

  const validateCron = useCallback(async (expr: string) => {
    if (!expr) {
      setCronValidation(null);
      return;
    }
    try {
      const res = await fetch(`${API_URL}/triggers/validate-cron`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ expression: expr }),
      });
      const data = await res.json();
      setCronValidation(data);
    } catch (e) {
      setCronValidation({ valid: false, error: 'Validation failed' });
    }
  }, []);

  useEffect(() => {
    if (formData.type === 'cron') {
      validateCron(formData.cronExpression);
    } else {
      setCronValidation(null);
    }
  }, [formData.cronExpression, formData.type, validateCron]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      const payload = {
        flowId,
        ...formData,
        webhookPath: formData.type === 'webhook' && !formData.webhookPath.startsWith('/')
          ? '/' + formData.webhookPath
          : formData.webhookPath,
      };

      let res;
      if (editingTrigger) {
        res = await fetch(`${API_URL}/triggers/${editingTrigger.id}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
      } else {
        res = await fetch(`${API_URL}/triggers`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
      }

      if (res.ok) {
        setShowForm(false);
        setEditingTrigger(null);
        setFormData({
          type: 'cron',
          cronExpression: '*/5 * * * *',
          webhookPath: '',
          sourceFlowId: '',
          enabled: true,
        });
        fetchTriggers();
      } else {
        const err = await res.json();
        alert(err.detail || 'Failed to save trigger');
      }
    } catch (e) {
      alert('Failed to save trigger');
    }
  };

  const toggleTrigger = async (triggerId: string, enabled: boolean) => {
    try {
      await fetch(`${API_URL}/triggers/${triggerId}/toggle`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ enabled: !enabled }),
      });
      fetchTriggers();
    } catch (e) {
      console.error('Failed to toggle trigger:', e);
    }
  };

  const deleteTrigger = async (triggerId: string) => {
    if (!confirm('Delete this trigger?')) return;
    try {
      await fetch(`${API_URL}/triggers/${triggerId}`, { method: 'DELETE' });
      fetchTriggers();
    } catch (e) {
      console.error('Failed to delete trigger:', e);
    }
  };

  const getTriggerIcon = (type: string) => {
    switch (type) {
      case 'cron': return <Clock size={16} />;
      case 'webhook': return <Globe size={16} />;
      case 'flow_completed': return <GitMerge size={16} />;
      default: return null;
    }
  };

  const getTriggerDescription = (trigger: Trigger) => {
    switch (trigger.type) {
      case 'cron': return `Cron: ${trigger.cronExpression}`;
      case 'webhook': return `Webhook: ${trigger.webhookPath}`;
      case 'flow_completed':
        const srcFlow = flows.find(f => f.id === trigger.sourceFlowId);
        return `On complete: ${srcFlow?.name || trigger.sourceFlowId}`;
      default: return '';
    }
  };

  return (
    <div className="h-full flex flex-col bg-slate-800">
      <div className="p-4 border-b border-slate-700 flex items-center justify-between">
        <h3 className="text-white font-bold flex items-center gap-2">
          <Clock size={18} className="text-blue-400" />
          Triggers
        </h3>
        <button
          onClick={() => { setShowForm(true); setEditingTrigger(null); }}
          className="px-3 py-1.5 bg-blue-500 text-white rounded hover:bg-blue-600 text-sm flex items-center gap-1"
        >
          <Plus size={14} />
          Add Trigger
        </button>
      </div>

      {showForm && (
        <div className="p-4 border-b border-slate-700 bg-slate-700/50">
          <form onSubmit={handleSubmit} className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-slate-300 mb-1">Type</label>
              <select
                value={formData.type}
                onChange={(e) => setFormData({ ...formData, type: e.target.value as any })}
                className="w-full bg-slate-600 text-white px-3 py-2 rounded border border-slate-500 text-sm"
              >
                <option value="cron">Scheduled (Cron)</option>
                <option value="webhook">Webhook</option>
                <option value="flow_completed">On Flow Complete</option>
              </select>
            </div>

            {formData.type === 'cron' && (
              <>
                <div>
                  <label className="block text-sm font-medium text-slate-300 mb-1">Cron Expression</label>
                  <input
                    type="text"
                    value={formData.cronExpression}
                    onChange={(e) => setFormData({ ...formData, cronExpression: e.target.value })}
                    className={`w-full bg-slate-600 text-white px-3 py-2 rounded border text-sm font-mono ${
                      cronValidation?.valid === false ? 'border-red-500' : 'border-slate-500'
                    }`}
                    placeholder="*/5 * * * *"
                  />
                  {cronValidation && (
                    <div className={`mt-1 text-xs ${cronValidation.valid ? 'text-green-400' : 'text-red-400'}`}>
                      {cronValidation.valid ? (
                        <>
                          ✓ Valid. Next runs:
                          <ul className="mt-1 text-slate-300">
                            {cronValidation.nextRuns?.slice(0, 3).map((t, i) => (
                              <li key={i}>• {new Date(t).toLocaleString()}</li>
                            ))}
                          </ul>
                        </>
                      ) : (
                        `✗ ${cronValidation.error}`
                      )}
                    </div>
                  )}
                  <div className="mt-1 text-xs text-slate-400">
                    Format: minute hour day month weekday (e.g., "0 9 * * 1-5" for 9AM weekdays)
                  </div>
                </div>
              </>
            )}

            {formData.type === 'webhook' && (
              <div>
                <label className="block text-sm font-medium text-slate-300 mb-1">Webhook Path</label>
                <input
                  type="text"
                  value={formData.webhookPath}
                  onChange={(e) => setFormData({ ...formData, webhookPath: e.target.value })}
                  className="w-full bg-slate-600 text-white px-3 py-2 rounded border border-slate-500 text-sm font-mono"
                  placeholder="/my-webhook"
                />
                <div className="mt-1 text-xs text-slate-400">
                  POST to {window.location.origin}/api/triggers/webhook{formData.webhookPath || '/...'}
                </div>
              </div>
            )}

            {formData.type === 'flow_completed' && (
              <div>
                <label className="block text-sm font-medium text-slate-300 mb-1">Source Flow</label>
                <select
                  value={formData.sourceFlowId}
                  onChange={(e) => setFormData({ ...formData, sourceFlowId: e.target.value })}
                  className="w-full bg-slate-600 text-white px-3 py-2 rounded border border-slate-500 text-sm"
                >
                  <option value="">Select a flow...</option>
                  {flows.filter(f => f.id !== flowId).map(f => (
                    <option key={f.id} value={f.id}>{f.name}</option>
                  ))}
                </select>
              </div>
            )}

            <div className="flex items-center gap-2">
              <input
                type="checkbox"
                id="enabled"
                checked={formData.enabled}
                onChange={(e) => setFormData({ ...formData, enabled: e.target.checked })}
                className="rounded"
              />
              <label htmlFor="enabled" className="text-sm text-slate-300">Enabled</label>
            </div>

            <div className="flex gap-2">
              <button
                type="submit"
                className="flex-1 px-3 py-2 bg-blue-500 text-white rounded hover:bg-blue-600 text-sm"
              >
                {editingTrigger ? 'Update' : 'Create'}
              </button>
              <button
                type="button"
                onClick={() => { setShowForm(false); setEditingTrigger(null); }}
                className="px-3 py-2 bg-slate-600 text-white rounded hover:bg-slate-500 text-sm"
              >
                Cancel
              </button>
            </div>
          </form>
        </div>
      )}

      <div className="flex-1 overflow-y-auto">
        {triggers.length === 0 ? (
          <div className="text-slate-500 text-sm text-center py-8">
            No triggers yet. Click "Add Trigger" to create one.
          </div>
        ) : (
          <div className="divide-y divide-slate-700">
            {triggers.map((trigger) => (
              <div key={trigger.id} className="p-4 hover:bg-slate-700/30">
                <div className="flex items-start justify-between">
                  <div className="flex items-center gap-3">
                    <div className={`p-2 rounded ${trigger.enabled ? 'bg-green-500/20 text-green-400' : 'bg-slate-600 text-slate-400'}`}>
                      {getTriggerIcon(trigger.type)}
                    </div>
                    <div>
                      <div className="text-white text-sm font-medium">
                        {trigger.type.charAt(0).toUpperCase() + trigger.type.slice(1)} Trigger
                      </div>
                      <div className="text-xs text-slate-400 mt-0.5">
                        {getTriggerDescription(trigger)}
                      </div>
                      <div className="text-xs text-slate-500 mt-0.5">
                        {nodeTypeLabels.task}: {flows.find(f => f.id === trigger.flowId)?.name || trigger.flowId}
                      </div>
                    </div>
                  </div>
                  <div className="flex items-center gap-1">
                    <button
                      onClick={() => toggleTrigger(trigger.id, trigger.enabled)}
                      className={`p-1.5 rounded ${trigger.enabled ? 'text-green-400 hover:bg-green-500/20' : 'text-slate-400 hover:bg-slate-600'}`}
                      title={trigger.enabled ? 'Pause' : 'Resume'}
                    >
                      {trigger.enabled ? <Pause size={14} /> : <Play size={14} />}
                    </button>
                    <button
                      onClick={() => deleteTrigger(trigger.id)}
                      className="p-1.5 rounded text-slate-400 hover:bg-red-500/20 hover:text-red-400"
                      title="Delete"
                    >
                      <Trash2 size={14} />
                    </button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
};
