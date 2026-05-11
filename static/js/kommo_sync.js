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
    pagamento_confirmado: { from: '#059669', to: '#047857', border: 'border-emerald-600/40', shadow: 'shadow-emerald-600/20' },
};

const _FUNNEL_VISUAL_ORDER = [
    'aguardando_inscricao', 'inscricao', 'processo_seletivo',
    'em_processo', 'aprovado_reprovado', 'aceite', 'pagamento_confirmado',
];

function _fmtKCount(n) {
    n = Number(n) || 0;
    if (n >= 1000) {
        const v = n / 1000;
        return (v >= 10 ? v.toFixed(0) : v.toFixed(1).replace(/\.0$/, '')) + 'k';
    }
    return n.toLocaleString('pt-BR');
}

function _renderFunnelVisual(data, prefix) {
    const wrap = document.getElementById(prefix + '-visual');
    if (!wrap) return;

    const stages = data.stages || [];
    if (!stages.length) { wrap.innerHTML = ''; return; }

    const byKey = {};
    stages.forEach(s => { byKey[s.key] = s; });

    const ordered = _FUNNEL_VISUAL_ORDER
        .map(k => byKey[k])
        .filter(Boolean);
    if (!ordered.length) { wrap.innerHTML = ''; return; }

    const lost = byKey.sem_resposta;

    // Para o gráfico considera o maior valor entre TODOS os estágios,
    // inclusive Perdidos, pra ele nunca explodir e quebrar a escala.
    const maxCount = Math.max(
        ...ordered.map(s => s.count || 0),
        lost ? (lost.count || 0) : 0,
        1,
    );
    // Escala suavizada (raiz quadrada): comprime a diferença entre o maior
    // e o menor estágio, evitando barras minúsculas com texto cortado.
    const heightFor = (count) => {
        const ratio = (count || 0) / maxCount;
        const pct = Math.sqrt(Math.max(0, ratio)) * 100;
        return Math.max(28, Math.min(100, Math.round(pct)));
    };

    const aceiteCount = (byKey.aceite && byKey.aceite.count) || 0;
    const startCount = ordered[0]?.count || 0;
    const conversaoGlobal = startCount > 0 ? ((aceiteCount / startCount) * 100).toFixed(1) : '0.0';

    const visualLabels = {
        aguardando_inscricao: 'Aguardando Inscrição',
        inscricao: 'Inscrição',
        processo_seletivo: 'Seletivo',
        em_processo: 'Em Processo',
        aprovado_reprovado: 'Aprovados',
        aceite: 'Aceite',
        pagamento_confirmado: 'Pagamento',
        sem_resposta: 'Sem Resposta',
    };

    const stageBars = ordered.map((s, i) => {
        const g = _FUNNEL_GRADIENTS[s.key] || { from: '#64748b', to: '#475569' };
        const pctHeight = heightFor(s.count);
        const isActive = s.key === 'aceite';
        const valueLabel = _fmtKCount(s.count || 0);
        const ringCls = isActive ? `ring-2 ring-offset-2 ring-offset-[var(--bg-card)] ring-emerald-500` : '';

        return `
        <div class="flex-1 min-w-0 min-h-0 flex flex-col group cursor-default">
            <div class="text-center mb-2 min-h-[28px] px-0.5">
                <p class="text-[9px] font-bold uppercase tracking-wider leading-tight ${isActive ? 'text-emerald-600 dark:text-emerald-400' : 'text-slate-500 dark:text-slate-400'}">${visualLabels[s.key] || s.label}</p>
            </div>
            <div class="flex-1 flex items-end min-h-0">
                <div class="rounded-t-xl w-full flex flex-col items-center justify-center px-1 transition-all duration-500 group-hover:brightness-110 ${ringCls}"
                     style="height:${pctHeight}%; background: linear-gradient(180deg, ${g.from}, ${g.to}); box-shadow: 0 -4px 18px ${g.from}30;">
                    <span class="text-white font-extrabold text-base sm:text-lg leading-none">${valueLabel}</span>
                    ${s.pct != null ? `<span class="text-white/80 text-[10px] mt-1 font-semibold">${s.pct}%</span>` : ''}
                </div>
            </div>
        </div>`;
    }).map((bar, i) => {
        if (i === ordered.length - 1) return bar;
        return bar + `
        <div class="w-4 sm:w-5 flex items-end pb-3 shrink-0">
            <span class="material-symbols-outlined text-slate-400 dark:text-slate-600 text-base">chevron_right</span>
        </div>`;
    }).join('');

    let lostHtml = '';
    if (lost) {
        const lostPctHeight = heightFor(lost.count);
        lostHtml = `
        <div class="hidden md:flex w-px self-stretch mx-2 bg-slate-200 dark:bg-slate-700"></div>
        <div class="w-20 sm:w-24 flex flex-col min-h-0">
            <div class="text-center mb-2 min-h-[28px] px-0.5">
                <p class="text-[9px] font-bold uppercase tracking-wider leading-tight text-rose-500 dark:text-rose-400">Perdidos</p>
            </div>
            <div class="flex-1 flex items-end min-h-0">
                <div class="rounded-t-xl w-full flex flex-col items-center justify-center px-1 border"
                     style="height:${lostPctHeight}%;
                            background: linear-gradient(180deg, #fb7185, #e11d48);
                            border-color: rgba(225, 29, 72, 0.45);
                            box-shadow: 0 -4px 18px rgba(225, 29, 72, 0.20);">
                    <span class="material-symbols-outlined text-white text-sm">close</span>
                    <span class="text-white font-extrabold text-base leading-none">${_fmtKCount(lost.count || 0)}</span>
                    ${lost.pct != null ? `<span class="text-white/80 text-[10px] mt-1 font-semibold">${lost.pct}%</span>` : ''}
                </div>
            </div>
        </div>`;
    }

    wrap.innerHTML = `
    <div class="flex items-center justify-between mb-3">
        <p class="text-[10px] font-bold uppercase tracking-[.18em] text-slate-500 dark:text-slate-400">Visualização do funil</p>
        <div class="flex items-center gap-2 text-xs">
            <span class="text-[10px] font-bold uppercase tracking-wider text-slate-400 dark:text-slate-500">Conversão global</span>
            <span class="text-sm font-extrabold text-emerald-600 dark:text-emerald-400 tabular-nums">${conversaoGlobal}%</span>
        </div>
    </div>
    <div class="flex items-stretch gap-1 h-52 sm:h-60 overflow-hidden">
        ${stageBars}
        ${lostHtml}
    </div>`;
}

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

    // KPI row extras (aceite + conversão)
    const stages = data.stages || [];
    const byKey = {};
    stages.forEach(s => { byKey[s.key] = s; });

    const aceiteEl = document.getElementById(prefix + '-aceite');
    const aceiteTrendEl = document.getElementById(prefix + '-aceite-trend');
    const aceiteDeltaEl = document.getElementById(prefix + '-aceite-delta');
    if (aceiteEl) {
        const ace = byKey.aceite;
        aceiteEl.textContent = (ace?.count || 0).toLocaleString('pt-BR');
        if (aceiteTrendEl && aceiteDeltaEl && ace && ace.delta !== undefined && ace.delta !== 0) {
            const positive = ace.delta > 0;
            aceiteTrendEl.classList.remove('up', 'down');
            aceiteTrendEl.classList.add(positive ? 'up' : 'down');
            aceiteTrendEl.style.display = '';
            const icon = aceiteTrendEl.querySelector('.ms-icon');
            if (icon) icon.textContent = positive ? 'trending_up' : 'trending_down';
            aceiteDeltaEl.textContent = (positive ? '+' : '') + ace.delta;
        } else if (aceiteTrendEl) {
            aceiteTrendEl.style.display = 'none';
        }
    }
    const convEl = document.getElementById(prefix + '-conversao');
    if (convEl) {
        const start = byKey.aguardando_inscricao?.count || 0;
        const aceite = byKey.aceite?.count || 0;
        const pct = start > 0 ? ((aceite / start) * 100).toFixed(1) : '0.0';
        convEl.textContent = pct + '%';
    }

    _renderFunnelVisual(data, prefix);

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
                    hover:shadow-md transition-all duration-300 cursor-default overflow-hidden min-w-0">
            <div class="h-1 rounded-t-xl" style="background:linear-gradient(90deg, ${g.from}, ${g.to})"></div>
            <div class="p-4 lg:p-5">
                <p class="text-[10px] font-bold uppercase tracking-wider leading-tight mb-3 break-words"
                   style="color:${g.from}">${s.label}</p>
                <p class="text-3xl font-black text-slate-900 dark:text-white font-display leading-none mb-3">${s.count.toLocaleString('pt-BR')}</p>
                <div class="flex items-center justify-between gap-2 flex-wrap">
                    <div class="flex items-center gap-1.5 min-w-0">
                        <span class="text-[10px] text-slate-400 dark:text-slate-500 shrink-0">D0:</span>
                        ${deltaHtml}
                        ${deltaPctHtml}
                    </div>
                    <span class="text-[10px] text-slate-400 dark:text-slate-500 font-mono shrink-0">${s.pct || 0}%</span>
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
        const statusCls = e.status === 'success' ? 'text-emerald-600 dark:text-emerald-400' : e.status === 'error' ? 'text-red-600 dark:text-red-400' : 'text-slate-500 dark:text-slate-400';
        const statusIcon = e.status === 'success' ? '●' : e.status === 'error' ? '✕' : '○';
        return `<tr class="border-b border-slate-200 dark:border-slate-800/40 hover:bg-slate-50 dark:hover:bg-slate-800/30 transition">
            <td class="py-2 pr-2 font-medium">${entityLabels[e.entity_type] || e.entity_type}</td>
            <td class="py-2 pr-2 text-xs text-slate-500 dark:text-slate-400">${lastSync}</td>
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

    const dark = document.documentElement.classList.contains('dark');
    const tick = dark ? '#94a3b8' : '#64748b';
    const gridX = dark ? '#1e293b' : '#e2e8f0';
    const tickY = dark ? '#94a3b8' : '#475569';

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
                x: { ticks: { color: tick }, grid: { color: gridX }, beginAtZero: true },
                y: { ticks: { color: tickY, font: { size: 11 } }, grid: { display: false } }
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
        return `<tr class="border-b border-slate-200 dark:border-slate-800/40 hover:bg-slate-50 dark:hover:bg-slate-800/30 transition">
            <td class="py-2 pr-2 text-xs text-slate-500 dark:text-slate-400">${s.pipeline_name}</td>
            <td class="py-2 pr-2 font-medium">${s.stage_name}</td>
            <td class="py-2 pr-2 text-right font-bold text-[#00346f] dark:text-white">${s.total.toLocaleString('pt-BR')}</td>
            <td class="py-2 text-right text-xs text-slate-500 dark:text-slate-400">${pct}%</td>
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
