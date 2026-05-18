// =============================================================================
// Painel Supervisor Comercial — substitui o Dashboard Acadêmico p/ a categoria
// =============================================================================

let _dsPerfChart = null;

function _dsCategoria() {
    return (document.body.dataset.categoria || '').toLowerCase().trim();
}

function isSupervisorComercial() {
    return _dsCategoria() === 'supervisor comercial';
}

function _dsFmtN(v, decimals = 0) {
    const n = Number(v) || 0;
    return n.toLocaleString('pt-BR', {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals,
    });
}

function _dsFmtPct(v) {
    if (v === null || v === undefined) return '—';
    const sign = v > 0 ? '+' : '';
    return `${sign}${Number(v).toFixed(1)}%`;
}

function _dsFmtDayLabel(iso) {
    const m = /^\d{4}-(\d{2})-(\d{2})$/.exec(iso || '');
    return m ? `${m[2]}/${m[1]}` : iso;
}

async function loadDashboardSupervisor() {
    // No primeiro load (datas vazias), deixa o backend decidir o período
    // com base na campanha mais recente cadastrada. Em loads subsequentes,
    // respeita as datas que o supervisor escolheu nos inputs.
    const iniVal = document.getElementById('ds-dt-ini')?.value || '';
    const fimVal = document.getElementById('ds-dt-fim')?.value || '';
    const params = new URLSearchParams();
    if (iniVal) params.set('dt_ini', iniVal);
    if (fimVal) params.set('dt_fim', fimVal);
    const qs = params.toString();

    try {
        const [resAgg, resFunnel] = await Promise.all([
            api('/api/dashboard/supervisor' + (qs ? `?${qs}` : '')),
            api('/api/kommo/funnel-live'),
        ]);
        const agg = await resAgg.json();
        const funnel = await resFunnel.json();

        if (!agg.ok) {
            console.error('supervisor dashboard error:', agg.error);
            return;
        }

        if (agg.periodo) {
            const ini = document.getElementById('ds-dt-ini');
            const fim = document.getElementById('ds-dt-fim');
            if (ini && agg.periodo.dt_ini) ini.value = agg.periodo.dt_ini;
            if (fim && agg.periodo.dt_fim) fim.value = agg.periodo.dt_fim;
        }

        _dsRenderKpis(agg.kpis);
        _dsRenderPerfChart(agg.performance_diaria);
        _dsRenderMedias(agg.medias);
        _dsRenderRanking(agg.ranking, agg.campanha);
        _dsRenderTotais(agg.totais, agg.periodo?.label, agg.campanha);

        if (funnel?.ok && typeof _renderFunnelVisual === 'function') {
            _renderFunnelVisual(funnel.data, 'ds-funnel');
        }

        if (typeof _dismissBootSplash === 'function') _dismissBootSplash();
    } catch (e) {
        console.error('loadDashboardSupervisor', e);
    }
}

function _dsRenderKpis(kpis) {
    const wrap = document.getElementById('ds-kpis');
    if (!wrap) return;

    const cards = [
        { title: 'Novos Leads',        data: kpis.novos_leads,       icon: 'group_add',       color: 'indigo',  unit: 'no período' },
        { title: 'Vendas',             data: kpis.vendas,            icon: 'shopping_bag',    color: 'amber',   unit: 'matrículas' },
        { title: 'Aceites Pendentes',  data: kpis.aceites_pendentes, icon: 'pending_actions', color: 'emerald', unit: 'na fila' },
        { title: 'Leads Parados',      data: kpis.leads_parados,     icon: 'hourglass_empty', color: 'rose',    unit: '+24h sem mover' },
    ];

    const colorMap = {
        indigo:  'bg-indigo-500/10  text-indigo-600  dark:text-indigo-400',
        amber:   'bg-amber-500/10   text-amber-600   dark:text-amber-400',
        emerald: 'bg-emerald-500/10 text-emerald-600 dark:text-emerald-400',
        rose:    'bg-rose-500/10    text-rose-600    dark:text-rose-400',
    };

    wrap.innerHTML = cards.map(c => {
        const valor = _dsFmtN(c.data?.valor || 0);
        const delta = c.data?.delta_pct;
        let trend = '';
        if (delta !== null && delta !== undefined) {
            const up = delta >= 0;
            const trendCls = up ? 'text-emerald-600 dark:text-emerald-400' : 'text-rose-600 dark:text-rose-400';
            const icon = up ? 'trending_up' : 'trending_down';
            trend = `<span class="inline-flex items-center gap-0.5 text-[11px] font-bold ${trendCls}">
                <span class="material-symbols-outlined text-[14px] leading-none">${icon}</span>
                ${_dsFmtPct(delta)}
            </span>`;
        }
        return `
            <div class="bg-white dark:bg-slate-800/50 rounded-2xl border border-slate-200 dark:border-slate-700/50 shadow-sm p-4 sm:p-5">
                <div class="flex items-start justify-between gap-2 mb-2">
                    <span class="w-9 h-9 rounded-lg flex items-center justify-center shrink-0 ${colorMap[c.color]}">
                        <span class="material-symbols-outlined text-[18px]">${c.icon}</span>
                    </span>
                    ${trend}
                </div>
                <p class="text-[10px] font-bold uppercase tracking-[.15em] text-slate-500 leading-tight mb-1">${c.title}</p>
                <p class="text-2xl sm:text-3xl font-extrabold tabular-nums text-slate-900 dark:text-white">${valor}</p>
                <p class="text-[10px] text-slate-500 mt-1">${c.unit}</p>
            </div>`;
    }).join('');
}

function _dsRenderMedias(m) {
    const set = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = v; };
    set('ds-medlead-atual', _dsFmtN(m?.leads_dia?.atual || 0, 1));
    set('ds-medlead-6m',    _dsFmtN(m?.leads_dia?.m6    || 0, 1));
    set('ds-medlead-1a',    _dsFmtN(m?.leads_dia?.y1    || 0, 1));
    set('ds-vendas-atual',  _dsFmtN(m?.vendas_total?.atual || 0));
    set('ds-vendas-6m',     _dsFmtN(m?.vendas_total?.m6    || 0));
    set('ds-vendas-1a',     _dsFmtN(m?.vendas_total?.y1    || 0));
}

function _dsRenderPerfChart(perf) {
    const ctx = document.getElementById('ds-perf-chart');
    if (!ctx || typeof Chart === 'undefined') return;
    if (_dsPerfChart) { _dsPerfChart.destroy(); _dsPerfChart = null; }

    const labels = (perf?.labels || []).map(_dsFmtDayLabel);
    const isLight = !document.documentElement.classList.contains('dark');
    const grid = isLight ? 'rgba(15,23,42,0.08)' : 'rgba(148,163,184,0.12)';
    const tickColor = isLight ? '#475569' : '#94a3b8';

    _dsPerfChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels,
            datasets: [
                {
                    label: 'Leads',
                    data: perf?.leads || [],
                    borderColor: '#6366f1',
                    backgroundColor: 'rgba(99,102,241,0.12)',
                    fill: true,
                    tension: 0.35,
                    borderWidth: 2.5,
                    pointRadius: 0,
                    pointHoverRadius: 5,
                },
                {
                    label: 'Vendas',
                    data: perf?.vendas || [],
                    borderColor: '#f59e0b',
                    backgroundColor: 'rgba(245,158,11,0.12)',
                    fill: true,
                    tension: 0.35,
                    borderWidth: 2.5,
                    pointRadius: 0,
                    pointHoverRadius: 5,
                },
                {
                    label: 'Leads 1 ano atrás',
                    data: perf?.leads_yoy || [],
                    borderColor: 'rgba(99,102,241,0.6)',
                    backgroundColor: 'transparent',
                    borderDash: [4, 4],
                    fill: false,
                    tension: 0.35,
                    borderWidth: 1.5,
                    pointRadius: 0,
                },
                {
                    label: 'Vendas 1 ano atrás',
                    data: perf?.vendas_yoy || [],
                    borderColor: 'rgba(245,158,11,0.6)',
                    backgroundColor: 'transparent',
                    borderDash: [4, 4],
                    fill: false,
                    tension: 0.35,
                    borderWidth: 1.5,
                    pointRadius: 0,
                },
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: 'rgba(15,23,42,0.95)',
                    borderColor: 'rgba(148,163,184,0.2)',
                    borderWidth: 1,
                    titleColor: '#fff',
                    bodyColor: '#e2e8f0',
                },
            },
            scales: {
                x: {
                    grid: { color: grid, drawBorder: false },
                    ticks: { color: tickColor, font: { size: 10 }, maxRotation: 0, autoSkip: true, maxTicksLimit: 12 },
                },
                y: {
                    beginAtZero: true,
                    grid: { color: grid, drawBorder: false },
                    ticks: { color: tickColor, font: { size: 10 } },
                },
            },
        },
    });
}

function _dsTierBadge(tier) {
    const map = {
        supermeta:     { bg: 'bg-amber-500/15',  fg: 'text-amber-700 dark:text-amber-300',  border: 'border-amber-500/30',  label: 'SUPER' },
        meta:          { bg: 'bg-emerald-500/15',fg: 'text-emerald-700 dark:text-emerald-300',border: 'border-emerald-500/30', label: 'META' },
        intermediaria: { bg: 'bg-cyan-500/15',   fg: 'text-cyan-700 dark:text-cyan-300',    border: 'border-cyan-500/30',   label: 'INTER' },
        base:          { bg: 'bg-slate-500/10',  fg: 'text-slate-600 dark:text-slate-300',  border: 'border-slate-500/20',  label: 'BASE' },
    };
    const c = map[tier] || map.base;
    return `<span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-[10px] font-bold uppercase tracking-wider border ${c.bg} ${c.fg} ${c.border}">${c.label}</span>`;
}

function _dsTierBarColor(tier) {
    return ({
        supermeta:     'bg-amber-500',
        meta:          'bg-emerald-500',
        intermediaria: 'bg-cyan-500',
        base:          'bg-slate-400',
    })[tier] || 'bg-slate-400';
}

function _dsRenderRanking(rows, campanha) {
    const tbody = document.getElementById('ds-ranking-tbody');
    if (!tbody) return;
    const camp = document.getElementById('ds-camp-label');
    if (camp) camp.textContent = campanha?.nome ? `Campanha: ${campanha.nome}` : '';

    if (!rows || !rows.length) {
        tbody.innerHTML = `<tr><td colspan="7" class="py-8 text-center text-slate-500">Sem dados de consultores no período.</td></tr>`;
        return;
    }

    tbody.innerHTML = rows.map(r => {
        const targetTxt = r.meta_target > 0 ? `Próx: ${r.meta_target}` : '';
        const conv = (r.conversao || 0).toFixed(1) + '%';
        const barCls = _dsTierBarColor(r.tier);
        const badge = _dsTierBadge(r.tier);
        return `
            <tr class="border-b border-slate-100 dark:border-slate-700/40 hover:bg-slate-50 dark:hover:bg-slate-800/40 transition-colors">
                <td class="py-3 px-4 font-bold text-primary">#${r.posicao}</td>
                <td class="py-3 px-4 text-slate-900 dark:text-white">${r.nome || '—'}</td>
                <td class="py-3 px-4 text-right tabular-nums font-semibold">${_dsFmtN(r.vendas)}</td>
                <td class="py-3 px-4 text-right tabular-nums text-slate-500">${_dsFmtN(r.leads)}</td>
                <td class="py-3 px-4 text-right tabular-nums font-semibold text-emerald-600 dark:text-emerald-400">${conv}</td>
                <td class="py-3 px-4 text-center">${badge}</td>
                <td class="py-3 px-4">
                    <div class="flex items-center gap-2">
                        <div class="flex-1 h-1.5 bg-slate-200 dark:bg-slate-700/40 rounded-full overflow-hidden">
                            <div class="${barCls} h-full transition-all" style="width:${Math.min(100, r.progresso || 0)}%"></div>
                        </div>
                        <span class="text-[10px] text-slate-500 tabular-nums shrink-0 w-9 text-right">${r.progresso || 0}%</span>
                    </div>
                    <span class="text-[9px] text-slate-400">${targetTxt}</span>
                </td>
            </tr>`;
    }).join('');
}

function _dsRenderTotais(t, periodoLabel, campanha) {
    const set = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = v; };
    set('ds-total-vendas',   _dsFmtN(t?.total_vendas || 0));
    set('ds-media-analista', _dsFmtN(t?.media_analista || 0, 1));

    const meta = document.getElementById('ds-meta-global');
    const restam = document.getElementById('ds-meta-restam');
    if (meta) {
        if (t?.meta_global_pct === null || t?.meta_global_pct === undefined) {
            meta.textContent = '—';
            if (restam) restam.textContent = 'Sem metas no período';
        } else {
            meta.textContent = t.meta_global_pct + '%';
            if (restam) restam.textContent = t.meta_global_pct >= 100 ? 'Meta atingida' : `Restam ${100 - t.meta_global_pct}%`;
        }
    }
    set('ds-status-campanha', t?.status_campanha || '—');

    const iconEl = document.getElementById('ds-status-icon');
    const subEl = document.getElementById('ds-status-sub');
    const iconMap = {
        'Início':         { icon: 'rocket_launch', cls: 'text-cyan-300' },
        'Construção':     { icon: 'construction',  cls: 'text-sky-300'  },
        'Aceleração':     { icon: 'bolt',          cls: 'text-amber-300'},
        'Meta batida':    { icon: 'verified',      cls: 'text-emerald-300' },
        'Encerrado':      { icon: 'flag',          cls: 'text-slate-400'},
        'Aguardando':     { icon: 'schedule',      cls: 'text-slate-400'},
        'Em andamento':   { icon: 'play_circle',   cls: 'text-cyan-300' },
    };
    if (iconEl) {
        const m = iconMap[t?.status_campanha] || iconMap['Aceleração'];
        iconEl.textContent = m.icon;
        iconEl.className = 'material-symbols-outlined text-[20px] ' + m.cls;
    }
    if (subEl) subEl.textContent = campanha?.nome ? campanha.nome : (periodoLabel || '');
}
