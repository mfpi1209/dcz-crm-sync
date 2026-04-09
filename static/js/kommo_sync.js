// ===========================================================================
// SYNC COMERCIAL — Kommo CRM
// ===========================================================================
let _kommoActChart = null;
let _kommoTaskId = null;
let _kommoPolling = null;

const _kommoColors = [
    '#6366f1', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6',
    '#ec4899', '#14b8a6', '#f97316', '#06b6d4', '#84cc16',
    '#a855f7', '#22d3ee', '#fb923c', '#4ade80', '#f43f5e',
];

function escHtml(s) {
    if (s == null || s === undefined) return '';
    return String(s)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}

const _FUNNEL_GRADIENTS = {
    aguardando_inscricao: { from: '#3b82f6', to: '#6366f1', border: 'border-blue-500/30',   shadow: 'shadow-blue-500/20' },
    inscricao:            { from: '#6366f1', to: '#8b5cf6', border: 'border-indigo-500/30', shadow: 'shadow-indigo-500/20' },
    processo_seletivo:    { from: '#8b5cf6', to: '#a855f7', border: 'border-violet-500/30', shadow: 'shadow-violet-500/20' },
    em_processo:          { from: '#06b6d4', to: '#0ea5e9', border: 'border-cyan-500/30',   shadow: 'shadow-cyan-500/20' },
    aprovado_reprovado:   { from: '#f59e0b', to: '#f97316', border: 'border-amber-500/30',  shadow: 'shadow-amber-500/20' },
    aceite:               { from: '#10b981', to: '#14b8a6', border: 'border-emerald-500/30', shadow: 'shadow-emerald-500/20' },
};

async function loadKommoSync() {
    _kommoRefreshFunnel(false);

    try {
        const hours = document.getElementById('kommo-hours').value;
        const [statusRes, stagesRes, changesRes] = await Promise.all([
            api('/api/kommo/status'),
            api('/api/kommo/leads-by-stage'),
            api('/api/kommo/recent-changes?hours=' + hours),
        ]);

        const status = await statusRes.json();
        const stages = await stagesRes.json();
        const changes = await changesRes.json();

        if (status.ok) _kommoRenderStatus(status.data);
        if (stages.ok) _kommoRenderStagesTable(stages.data);
        if (changes.ok) _kommoRenderChanges(changes.data);
    } catch (e) {
        console.error('Erro ao carregar Sync Comercial:', e);
    }
}

async function _kommoRefreshFunnel(force) {
    const btn = document.getElementById('kommo-funnel-refresh-btn');
    if (btn) { btn.disabled = true; btn.style.opacity = '0.5'; }

    try {
        const url = '/api/kommo/funnel-live' + (force ? '?force=1' : '');
        const res = await api(url);
        const d = await res.json();
        if (d.ok) {
            _renderFunnelCards(d.data, 'kommo-funnel');
        } else {
            console.error('funnel-live error:', d.error);
        }
    } catch (e) {
        console.error('funnel-live fetch error:', e);
    } finally {
        if (btn) { btn.disabled = false; btn.style.opacity = '1'; }
    }
}

function _renderFunnelCards(data, prefix) {
    const newEl = document.getElementById(prefix + '-new');
    const totalEl = document.getElementById(prefix + '-total');
    if (newEl) newEl.textContent = (data.new_today || 0).toLocaleString('pt-BR');
    if (totalEl) totalEl.textContent = (data.total || 0).toLocaleString('pt-BR');

    const tsEl = document.getElementById(prefix + '-ts');
    if (tsEl) {
        const label = data.fetched_at ? `Live ${data.fetched_at}` : '';
        tsEl.textContent = label;
    }

    const container = document.getElementById(prefix + '-cards');
    if (!container) return;

    const highlight = (data.stages || []).filter(s => s.highlight);
    if (!highlight.length) {
        container.innerHTML = '<div class="col-span-full text-center py-8 text-slate-500 text-sm">Nenhum dado de funil</div>';
        return;
    }

    container.innerHTML = highlight.map(s => {
        const g = _FUNNEL_GRADIENTS[s.key] || { from: '#64748b', to: '#475569', border: 'border-slate-500/30', shadow: 'shadow-slate-500/20' };

        let deltaHtml = '';
        if (s.delta !== 0 && s.delta !== undefined) {
            const sign = s.delta > 0 ? '+' : '';
            const color = s.delta > 0 ? 'text-emerald-600 dark:text-emerald-400' : 'text-red-600 dark:text-red-400';
            const bgColor = s.delta > 0 ? 'bg-emerald-50 dark:bg-emerald-500/10' : 'bg-red-50 dark:bg-red-500/10';
            const arrow = s.delta > 0 ? 'trending_up' : 'trending_down';
            deltaHtml = `<span class="${color} ${bgColor} text-xs font-bold flex items-center gap-0.5 px-2 py-0.5 rounded-full"><span class="material-symbols-outlined text-sm">${arrow}</span> ${sign}${s.delta}</span>`;
        } else {
            deltaHtml = '<span class="text-slate-400 dark:text-slate-600 text-xs">—</span>';
        }

        let deltaPctHtml = '';
        if (s.delta_pct !== 0 && s.delta_pct !== undefined) {
            const sign = s.delta_pct > 0 ? '+' : '';
            const color = s.delta_pct > 0 ? 'text-emerald-500 dark:text-emerald-400/70' : 'text-red-500 dark:text-red-400/70';
            deltaPctHtml = `<span class="${color} text-[10px]">${sign}${s.delta_pct}%</span>`;
        }

        return `
        <div class="bg-white dark:bg-slate-800/50 rounded-xl border border-slate-200 dark:border-slate-700/50 shadow-sm
                    hover:shadow-md transition-all duration-300 cursor-default overflow-hidden">
            <div class="h-1 rounded-t-xl" style="background:linear-gradient(90deg, ${g.from}, ${g.to})"></div>
            <div class="p-5">
                <div class="flex items-start justify-between mb-3">
                    <p class="text-[10px] font-bold uppercase tracking-widest" style="color:${g.from}">${s.label}</p>
                    <span class="text-[10px] text-slate-400 dark:text-slate-500 font-mono">${s.pct || 0}%</span>
                </div>
                <p class="text-3xl font-black text-slate-900 dark:text-white font-display mb-2">${s.count.toLocaleString('pt-BR')}</p>
                <div class="flex items-center gap-2">
                    <span class="text-[10px] text-slate-400 dark:text-slate-500">D0:</span>
                    ${deltaHtml}
                    ${deltaPctHtml}
                </div>
            </div>
        </div>`;
    }).join('');
}

function _kommoRenderStatus(d) {
    document.getElementById('kommo-kpi-leads').textContent = (d.leads_count || 0).toLocaleString('pt-BR');
    document.getElementById('kommo-kpi-contacts').textContent = (d.contacts_count || 0).toLocaleString('pt-BR');

    const tbody = document.getElementById('kommo-entities-tbody');
    const entities = d.entities || [];
    if (!entities.length) {
        tbody.innerHTML = '<tr><td colspan="4" class="py-4 text-center text-slate-500">Nenhum dado de sync encontrado</td></tr>';
        return;
    }

    const entityLabels = { leads: 'Leads', contacts: 'Contatos', pipelines: 'Pipelines', custom_fields: 'Custom Fields' };

    tbody.innerHTML = entities.map(e => {
        const lastSync = e.last_sync_at ? new Date(e.last_sync_at).toLocaleString('pt-BR') : '—';
        const statusCls = e.status === 'success' ? 'text-emerald-400' : e.status === 'error' ? 'text-red-400' : 'text-slate-400';
        const statusIcon = e.status === 'success' ? '●' : e.status === 'error' ? '✕' : '○';
        return `<tr class="border-b border-slate-800/40 hover:bg-slate-800/30 transition">
            <td class="py-2 pr-2 font-medium">${entityLabels[e.entity_type] || e.entity_type}</td>
            <td class="py-2 pr-2 text-xs text-slate-400">${lastSync}</td>
            <td class="py-2 pr-2 text-right font-bold">${(e.records_synced || 0).toLocaleString('pt-BR')}</td>
            <td class="py-2 text-xs ${statusCls}">${statusIcon} ${e.status || '—'}</td>
        </tr>`;
    }).join('');
}

function _kommoRenderChanges(d) {
    document.getElementById('kommo-kpi-updated').textContent = (d.leads_updated || 0).toLocaleString('pt-BR');
    document.getElementById('kommo-kpi-updated-sub').textContent = `Leads (últimas ${d.hours}h)`;
    document.getElementById('kommo-kpi-won').textContent = (d.won_leads || 0).toLocaleString('pt-BR');
    document.getElementById('kommo-kpi-won-sub').textContent = `Ganhos (últimas ${d.hours}h)`;

    const canvas = document.getElementById('kommo-activity-chart');
    if (_kommoActChart) { _kommoActChart.destroy(); _kommoActChart = null; }

    const byStage = d.updated_by_stage || [];
    if (!byStage.length) return;

    const labels = byStage.map(s => s.stage_name);
    const values = byStage.map(s => s.total);

    _kommoActChart = new Chart(canvas, {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                label: 'Leads atualizados',
                data: values,
                backgroundColor: labels.map((_, i) => _kommoColors[i % _kommoColors.length] + '99'),
                borderColor: labels.map((_, i) => _kommoColors[i % _kommoColors.length]),
                borderWidth: 1,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            indexAxis: 'y',
            plugins: {
                legend: { display: false },
                tooltip: { callbacks: { label: ctx => ` ${ctx.parsed.x.toLocaleString('pt-BR')} leads` } }
            },
            scales: {
                x: { ticks: { color: '#64748b' }, grid: { color: '#1e293b' }, beginAtZero: true },
                y: { ticks: { color: '#94a3b8', font: { size: 11 } }, grid: { display: false } }
            }
        }
    });
}

function _kommoRenderStagesTable(data) {
    const totalAll = data.reduce((s, d) => s + d.total, 0);
    const tbody = document.getElementById('kommo-stages-tbody');
    if (!data.length) {
        tbody.innerHTML = '<tr><td colspan="4" class="py-4 text-center text-slate-500">Nenhum dado</td></tr>';
        return;
    }
    tbody.innerHTML = data.map(s => {
        const pct = totalAll > 0 ? ((s.total / totalAll) * 100).toFixed(1) : '0';
        return `<tr class="border-b border-slate-800/40 hover:bg-slate-800/30 transition">
            <td class="py-2 pr-2 text-xs text-slate-400">${s.pipeline_name}</td>
            <td class="py-2 pr-2 font-medium">${s.stage_name}</td>
            <td class="py-2 pr-2 text-right font-bold text-white">${s.total.toLocaleString('pt-BR')}</td>
            <td class="py-2 text-right text-xs text-slate-400">${pct}%</td>
        </tr>`;
    }).join('');
}

async function _kommoStartSync(mode) {
    const btnD = document.getElementById('kommo-btn-delta');
    const btnF = document.getElementById('kommo-btn-full');
    btnD.disabled = true; btnF.disabled = true;
    btnD.style.opacity = '0.5'; btnF.style.opacity = '0.5';

    const wrap = document.getElementById('kommo-progress-wrap');
    wrap.classList.remove('hidden');
    document.getElementById('kommo-progress-bar').style.width = '0%';
    document.getElementById('kommo-progress-pct').textContent = '0%';
    document.getElementById('kommo-progress-label').textContent = 'Iniciando...';
    const logEl0 = document.getElementById('kommo-sync-log');
    if (logEl0) logEl0.innerHTML = '<p class="text-xs text-slate-500">Conectando ao servidor...</p>';

    try {
        const res = await api('/api/kommo/sync', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ mode }),
        });
        const d = await res.json().catch(() => ({}));
        if (!res.ok || !d.ok) {
            toast(d.error || ('HTTP ' + res.status) || 'Erro ao iniciar sync', 'error');
            _kommoResetButtons();
            if (logEl0) logEl0.innerHTML = '<p class="text-xs text-red-400">' + escHtml(d.error || 'Falha ao iniciar') + '</p>';
            return;
        }
        _kommoTaskId = d.task_id;
        _kommoPollTask();
    } catch (e) {
        toast('Erro: ' + e.message, 'error');
        _kommoResetButtons();
    }
}

function _kommoPollTask() {
    if (_kommoPolling) clearInterval(_kommoPolling);

    const tick = async () => {
        if (!_kommoTaskId) return;
        try {
            const res = await api('/api/kommo/task/' + _kommoTaskId);
            const d = await res.json().catch(() => ({}));
            if (!res.ok || !d.ok || !d.data) {
                clearInterval(_kommoPolling);
                _kommoPolling = null;
                const msg = d.error || (res.status === 404
                    ? 'Tarefa não encontrada (servidor reiniciou?). Inicie o sync de novo.'
                    : 'Erro ao ler status (' + res.status + ')');
                toast(msg, 'error');
                _kommoResetButtons();
                const le = document.getElementById('kommo-sync-log');
                if (le) le.innerHTML = '<p class="text-xs text-red-400">' + escHtml(msg) + '</p>';
                return;
            }

            const t = d.data;
            document.getElementById('kommo-progress-bar').style.width = t.progress + '%';
            document.getElementById('kommo-progress-pct').textContent = t.progress + '%';
            document.getElementById('kommo-progress-label').textContent = t.message || '...';

            const logEl = document.getElementById('kommo-sync-log');
            if (t.log && t.log.length) {
                logEl.innerHTML = t.log.map(l =>
                    `<p class="text-xs font-mono text-slate-400"><span class="text-slate-600">${l.time || ''}</span> ${escHtml(l.msg || '')}</p>`
                ).join('');
                logEl.scrollTop = logEl.scrollHeight;
            }

            if (t.status === 'completed' || t.status === 'error' || t.status === 'cancelled') {
                clearInterval(_kommoPolling);
                _kommoPolling = null;
                _kommoResetButtons();

                if (t.status === 'completed') {
                    document.getElementById('kommo-progress-label').textContent = 'Concluído!';
                    document.getElementById('kommo-progress-bar').className =
                        document.getElementById('kommo-progress-bar').className.replace('from-emerald-500 to-teal-400', 'from-emerald-400 to-green-400');
                    loadKommoSync();
                } else {
                    document.getElementById('kommo-progress-label').textContent = 'Erro: ' + (t.message || '');
                    document.getElementById('kommo-progress-bar').className =
                        document.getElementById('kommo-progress-bar').className.replace('from-emerald-500 to-teal-400', 'from-red-500 to-red-400');
                }
            }
        } catch (e) {
            console.error('Poll error:', e);
        }
    };

    tick();
    _kommoPolling = setInterval(tick, 1500);
}

function _kommoResetButtons() {
    const btnD = document.getElementById('kommo-btn-delta');
    const btnF = document.getElementById('kommo-btn-full');
    btnD.disabled = false; btnF.disabled = false;
    btnD.style.opacity = '1'; btnF.style.opacity = '1';
    document.getElementById('kommo-progress-wrap')?.classList.add('hidden');
}

async function _kommoCancelSync() {
    const btn = document.getElementById('kommo-btn-cancel');
    if (btn) { btn.disabled = true; btn.textContent = 'Cancelando...'; }
    try {
        const res = await api('/api/kommo/sync/cancel', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ task_id: _kommoTaskId }),
        });
        const d = await res.json();
        if (d.ok) {
            toast('Sync cancelado.', 'info');
            clearInterval(_kommoPolling);
            _kommoPolling = null;
            document.getElementById('kommo-progress-label').textContent = 'Cancelado.';
            document.getElementById('kommo-progress-bar').className =
                document.getElementById('kommo-progress-bar').className
                    .replace('from-emerald-500 to-teal-400', 'from-red-500 to-red-400');
            setTimeout(() => _kommoResetButtons(), 2000);
        } else {
            toast(d.error || 'Erro ao cancelar', 'error');
            if (btn) { btn.disabled = false; btn.innerHTML = '<svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg> Cancelar'; }
        }
    } catch (e) {
        toast('Erro: ' + e.message, 'error');
    }
}
