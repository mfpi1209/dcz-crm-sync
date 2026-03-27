// ===========================================================================
// SYNC COMERCIAL — Kommo CRM
// ===========================================================================
let _kommoActChartId = 'kommo-activity-chart';
let _kommoTaskId = null;
let _kommoPolling = null;

const _kommoColors = [
    '#6366f1', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6',
    '#ec4899', '#14b8a6', '#f97316', '#06b6d4', '#84cc16',
    '#a855f7', '#22d3ee', '#fb923c', '#4ade80', '#f43f5e',
];

const _FUNNEL_GRADIENTS = {
    aguardando_inscricao: { from: '#3b82f6', to: '#6366f1', border: 'border-blue-500/30',   shadow: 'shadow-blue-500/20' },
    inscricao:            { from: '#6366f1', to: '#8b5cf6', border: 'border-indigo-500/30', shadow: 'shadow-indigo-500/20' },
    processo_seletivo:    { from: '#8b5cf6', to: '#a855f7', border: 'border-violet-500/30', shadow: 'shadow-violet-500/20' },
    em_processo:          { from: '#06b6d4', to: '#0ea5e9', border: 'border-indigo-500/30',   shadow: 'shadow-indigo-500/20' },
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

let _kommoPollLiveTimer = null;

async function _kommoRefreshFunnel(force) {
    const btn = document.getElementById('kommo-funnel-refresh-btn');
    if (btn) { btn.disabled = true; btn.style.opacity = '0.5'; }
    if (_kommoPollLiveTimer) { clearTimeout(_kommoPollLiveTimer); _kommoPollLiveTimer = null; }

    try {
        const url = '/api/kommo/funnel-live' + (force ? '?force=1' : '');
        const res = await api(url);
        const d = await res.json();
        if (d.ok && d.data) {
            _renderFunnelCards(d.data, 'kommo-funnel');
            if (d.source === 'pg' || d.source === 'cache') {
                _kommoPollLiveTimer = setTimeout(() => _kommoPollForLive(), 2500);
            }
        } else {
            console.error('funnel-live error:', d.error);
        }
    } catch (e) {
        console.error('funnel-live fetch error:', e);
    } finally {
        if (btn) { btn.disabled = false; btn.style.opacity = '1'; }
    }
}

async function _kommoPollForLive() {
    try {
        const res = await api('/api/kommo/funnel-live?poll=1');
        const d = await res.json();
        if (d.ok && d.data && d.source === 'live') {
            _renderFunnelCards(d.data, 'kommo-funnel');
        } else if (d.source === 'pending') {
            _kommoPollLiveTimer = setTimeout(() => _kommoPollForLive(), 3000);
        }
    } catch (e) {
        console.error('poll-live error:', e);
    }
}

function _renderFunnelCards(data, prefix) {
    const newEl = document.getElementById(prefix + '-new');
    const totalEl = document.getElementById(prefix + '-total');
    if (newEl) newEl.textContent = (data.new_today || 0).toLocaleString('pt-BR');
    if (totalEl) totalEl.textContent = (data.total || 0).toLocaleString('pt-BR');

    const tsEl = document.getElementById(prefix + '-ts');
    if (tsEl) {
        const src = data.source || '';
        const badge = src === 'live'
            ? '<span class="inline-flex items-center gap-1 text-[9px] font-bold text-emerald-500 bg-emerald-500/10 px-1.5 py-0.5 rounded-full"><span class="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse"></span>LIVE</span>'
            : src === 'pg'
            ? '<span class="inline-flex items-center gap-1 text-[9px] font-bold text-amber-500 bg-amber-500/10 px-1.5 py-0.5 rounded-full">SYNC</span>'
            : '';
        const time = data.fetched_at || '';
        tsEl.innerHTML = `${badge} <span class="text-gray-500 text-[10px] font-mono">${time}</span>`;
    }

    const container = document.getElementById(prefix + '-cards');
    if (!container) return;

    const highlight = (data.stages || []).filter(s => s.highlight);
    if (!highlight.length) {
        container.innerHTML = '<div class="w-full text-center py-8 text-gray-500 text-sm">Nenhum dado de funil</div>';
        return;
    }

    container.innerHTML = highlight.map(s => {
        const g = _FUNNEL_GRADIENTS[s.key] || { from: '#64748b', to: '#475569' };

        let deltaHtml = '';
        if (s.delta !== 0 && s.delta !== undefined) {
            const sign = s.delta > 0 ? '+' : '';
            const badgeCls = s.delta > 0 ? 'tremor-badge-emerald' : 'tremor-badge-rose';
            const arrow = s.delta > 0 ? 'trending_up' : 'trending_down';
            deltaHtml = `<span class="tremor-badge ${badgeCls} gap-0.5"><span class="material-symbols-outlined text-xs">${arrow}</span>${sign}${s.delta}</span>`;
        } else {
            deltaHtml = '<span class="text-gray-400 dark:text-gray-600 text-xs">—</span>';
        }

        let deltaPctHtml = '';
        if (s.delta_pct !== 0 && s.delta_pct !== undefined) {
            const sign = s.delta_pct > 0 ? '+' : '';
            const color = s.delta_pct > 0 ? 'text-emerald-500 dark:text-emerald-400' : 'text-red-500 dark:text-red-400';
            deltaPctHtml = `<span class="${color} text-[10px]">${sign}${s.delta_pct}%</span>`;
        }

        return `
        <div class="tremor-card overflow-hidden !p-0 cursor-default
                    min-w-[160px] flex-shrink-0 snap-start lg:min-w-0 lg:flex-shrink">
            <div class="h-1" style="background:linear-gradient(90deg, ${g.from}, ${g.to})"></div>
            <div class="p-4">
                <div class="flex items-start justify-between mb-2">
                    <p class="text-[10px] font-semibold uppercase tracking-wider" style="color:${g.from}">${s.label}</p>
                    <span class="tremor-badge tremor-badge-gray">${s.pct || 0}%</span>
                </div>
                <p class="tremor-metric mb-2">${s.count.toLocaleString('pt-BR')}</p>
                <div class="flex items-center gap-2">
                    <span class="tremor-sublabel">D0:</span>
                    ${deltaHtml}
                    ${deltaPctHtml}
                </div>
            </div>
        </div>`;
    }).join('');

    _renderFunnelDots(container, highlight.length);
}

function _renderFunnelDots(container, count) {
    const dotsEl = document.getElementById('kommo-funnel-dots');
    if (!dotsEl || count <= 2) { if (dotsEl) dotsEl.innerHTML = ''; return; }

    dotsEl.innerHTML = Array.from({ length: count }, (_, i) =>
        `<span class="w-1.5 h-1.5 rounded-full transition-all ${i === 0 ? 'bg-indigo-500 w-3' : 'bg-gray-400/40'}" data-dot="${i}"></span>`
    ).join('');

    let ticking = false;
    container.addEventListener('scroll', () => {
        if (ticking) return;
        ticking = true;
        requestAnimationFrame(() => {
            const scrollLeft = container.scrollLeft;
            const cardW = container.firstElementChild?.offsetWidth || 160;
            const active = Math.round(scrollLeft / (cardW + 12));
            dotsEl.querySelectorAll('[data-dot]').forEach((dot, i) => {
                dot.className = `w-1.5 h-1.5 rounded-full transition-all ${i === active ? 'bg-indigo-500 w-3' : 'bg-gray-400/40'}`;
            });
            ticking = false;
        });
    }, { passive: true });
}

function _kommoRenderStatus(d) {
    document.getElementById('kommo-kpi-leads').textContent = (d.leads_count || 0).toLocaleString('pt-BR');
    document.getElementById('kommo-kpi-contacts').textContent = (d.contacts_count || 0).toLocaleString('pt-BR');

    const tbody = document.getElementById('kommo-entities-tbody');
    const entities = d.entities || [];
    if (!entities.length) {
        tbody.innerHTML = '<tr><td colspan="4" class="py-4 text-center text-gray-500">Nenhum dado de sync encontrado</td></tr>';
        return;
    }

    const entityLabels = { leads: 'Leads', contacts: 'Contatos', pipelines: 'Pipelines', custom_fields: 'Custom Fields' };

    tbody.innerHTML = entities.map(e => {
        const lastSync = e.last_sync_at ? new Date(e.last_sync_at).toLocaleString('pt-BR') : '—';
        const statusCls = e.status === 'success' ? 'text-emerald-400' : e.status === 'error' ? 'text-red-400' : 'text-gray-400';
        const statusIcon = e.status === 'success' ? '●' : e.status === 'error' ? '✕' : '○';
        return `<tr class="border-b border-[var(--border)] hover:bg-gray-100 dark:hover:bg-gray-800/30 transition">
            <td class="py-2 pr-2 font-medium">${entityLabels[e.entity_type] || e.entity_type}</td>
            <td class="py-2 pr-2 text-xs text-gray-400">${lastSync}</td>
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

    const byStage = d.updated_by_stage || [];
    if (!byStage.length) return;

    const labels = byStage.map(s => s.stage_name);
    const values = byStage.map(s => s.total);

    const chart = eInit(_kommoActChartId);
    if (!chart) return;
    chart.setOption({
        backgroundColor: 'transparent',
        grid: { ...eBaseGrid(), left: 16, right: 24 },
        tooltip: { ...eTooltip('axis'), valueFormatter: v => v.toLocaleString('pt-BR') + ' leads' },
        xAxis: eValueAxis(),
        yAxis: { ...eCategoryAxis(labels), inverse: true },
        series: [{
            type: 'bar', data: values.map((v, i) => ({ value: v, itemStyle: { color: _kommoColors[i % _kommoColors.length] } })),
            barWidth: '60%', itemStyle: { borderRadius: [0, 4, 4, 0] },
        }],
        animationDuration: 600,
    }, true);
}

function _kommoRenderStagesTable(data) {
    const totalAll = data.reduce((s, d) => s + d.total, 0);
    const tbody = document.getElementById('kommo-stages-tbody');
    if (!data.length) {
        tbody.innerHTML = '<tr><td colspan="4" class="py-4 text-center text-gray-500">Nenhum dado</td></tr>';
        return;
    }
    tbody.innerHTML = data.map(s => {
        const pct = totalAll > 0 ? ((s.total / totalAll) * 100).toFixed(1) : '0';
        return `<tr class="border-b border-[var(--border)] hover:bg-gray-100 dark:hover:bg-gray-800/30 transition">
            <td class="py-2 pr-2 text-xs text-gray-400">${s.pipeline_name}</td>
            <td class="py-2 pr-2 font-medium">${s.stage_name}</td>
            <td class="py-2 pr-2 text-right font-bold text-[var(--text-primary)]">${s.total.toLocaleString('pt-BR')}</td>
            <td class="py-2 text-right text-xs text-gray-400">${pct}%</td>
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

    try {
        const res = await api('/api/kommo/sync', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ mode }),
        });
        const d = await res.json();
        if (!d.ok) {
            toast(d.error || 'Erro ao iniciar sync', 'error');
            _kommoResetButtons();
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
    _kommoPolling = setInterval(async () => {
        try {
            const res = await api('/api/kommo/task/' + _kommoTaskId);
            const d = await res.json();
            if (!d.ok) return;

            const t = d.data;
            document.getElementById('kommo-progress-bar').style.width = t.progress + '%';
            document.getElementById('kommo-progress-pct').textContent = t.progress + '%';
            document.getElementById('kommo-progress-label').textContent = t.message || '...';

            const logEl = document.getElementById('kommo-sync-log');
            if (t.log && t.log.length) {
                logEl.innerHTML = t.log.map(l =>
                    `<p class="text-xs font-mono text-gray-400"><span class="text-gray-600">${l.time || ''}</span> ${l.msg || ''}</p>`
                ).join('');
                logEl.scrollTop = logEl.scrollHeight;
            }

            if (t.status === 'completed' || t.status === 'error') {
                clearInterval(_kommoPolling);
                _kommoPolling = null;
                _kommoResetButtons();

                if (t.status === 'completed') {
                    document.getElementById('kommo-progress-label').textContent = 'Concluído!';
                    document.getElementById('kommo-progress-bar').className =
                        document.getElementById('kommo-progress-bar').className.replace('from-emerald-500 to-indigo-400', 'from-emerald-400 to-green-400');
                    loadKommoSync();
                } else {
                    document.getElementById('kommo-progress-label').textContent = 'Erro: ' + (t.message || '');
                    document.getElementById('kommo-progress-bar').className =
                        document.getElementById('kommo-progress-bar').className.replace('from-emerald-500 to-indigo-400', 'from-red-500 to-red-400');
                }
            }
        } catch (e) {
            console.error('Poll error:', e);
        }
    }, 3000);
}

function _kommoResetButtons() {
    const btnD = document.getElementById('kommo-btn-delta');
    const btnF = document.getElementById('kommo-btn-full');
    btnD.disabled = false; btnF.disabled = false;
    btnD.style.opacity = '1'; btnF.style.opacity = '1';
}
