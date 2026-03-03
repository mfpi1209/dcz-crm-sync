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

const _FUNNEL_STAGES = {
    aguardando_inscricao: { id: 99045180, el: 'kommo-funnel-aguardando' },
    inscricao:            { id: 48539249, el: 'kommo-funnel-inscricao' },
    processo_seletivo:    { id: 48566195, el: 'kommo-funnel-procseletivo' },
    em_processo:          { id: 48566198, el: 'kommo-funnel-emprocesso' },
    aprovado_reprovado:   { id: 48566201, el: 'kommo-funnel-aprovado' },
    aceite:               { id: 48566207, el: 'kommo-funnel-aceite' },
};

async function loadKommoSync() {
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
        if (stages.ok) _kommoRenderFunnelCards(stages.data, status.ok ? status.data.new_today : 0);
        if (stages.ok) _kommoRenderStagesTable(stages.data);
        if (changes.ok) _kommoRenderChanges(changes.data);
    } catch (e) {
        console.error('Erro ao carregar Sync Comercial:', e);
    }
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

function _kommoRenderFunnelCards(stagesData, newToday) {
    document.getElementById('kommo-funnel-new').textContent = (newToday || 0).toLocaleString('pt-BR');

    const byId = {};
    stagesData.forEach(s => { byId[s.stage_id] = s.total; });

    Object.values(_FUNNEL_STAGES).forEach(cfg => {
        const el = document.getElementById(cfg.el);
        if (el) el.textContent = (byId[cfg.id] || 0).toLocaleString('pt-BR');
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

    try {
        const res = await api('/api/kommo/sync', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ mode }),
        });
        const d = await res.json();
        if (!d.ok) {
            alert(d.error || 'Erro ao iniciar sync');
            _kommoResetButtons();
            return;
        }
        _kommoTaskId = d.task_id;
        _kommoPollTask();
    } catch (e) {
        alert('Erro: ' + e.message);
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
                    `<p class="text-xs font-mono text-slate-400"><span class="text-slate-600">${l.time || ''}</span> ${l.msg || ''}</p>`
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
    }, 3000);
}

function _kommoResetButtons() {
    const btnD = document.getElementById('kommo-btn-delta');
    const btnF = document.getElementById('kommo-btn-full');
    btnD.disabled = false; btnF.disabled = false;
    btnD.style.opacity = '1'; btnF.style.opacity = '1';
}
