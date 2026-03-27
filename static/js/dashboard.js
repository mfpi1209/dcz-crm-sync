// ---------------------------------------------------------------------------
// Dashboard
// ---------------------------------------------------------------------------
async function loadDashboard() {
    _dashRefreshFunnel(false);

    try {
        const res = await api('/api/dashboard');
        const d = await res.json();
        if (d.error) {
            console.warn('Dashboard API error:', d.error);
        }
        const snapInfo = document.getElementById('dash-snap-info');
        const statusEl = document.getElementById('dash-process-status');
        if (snapInfo) {
            if (d.snapshot) {
                snapInfo.textContent = d.snapshot.filename + ' \u2014 ' + d.snapshot.row_count.toLocaleString('pt-BR') + ' registros (' + d.snapshot.uploaded_at + ')';
            } else {
                snapInfo.textContent = 'Nenhum snapshot de matriculados carregado';
                snapInfo.classList.add('text-amber-400');
            }
        }
        if (statusEl) {
            if (d.sync_running) {
                statusEl.innerHTML = '<span class="inline-block w-2.5 h-2.5 rounded-full bg-indigo-400 animate-pulse"></span> Sync...';
            } else if (d.update_running) {
                statusEl.innerHTML = '<span class="inline-block w-2.5 h-2.5 rounded-full bg-amber-400 animate-pulse"></span> Update...';
            } else {
                statusEl.innerHTML = '<span class="green-dot"></span> Conectado';
            }
        }
        if (d.diag) {
            console.info('[Dashboard diag] negocio:', d.diag.negocio_vals, '| nivel:', d.diag.nivel_vals, '| tipo_matricula:', d.diag.tipo_vals);
        }
    } catch (err) {
        console.error('Dashboard load error:', err);
    }
    populateCicloFilter();
    loadStudentMetrics();
    loadTimeline();
    loadCicloMaster();
    _loadInadimplenciaCard();
}

async function _dashRefreshFunnel(force) {
    const btn = document.getElementById('dash-funnel-refresh-btn');
    if (btn) { btn.disabled = true; btn.style.opacity = '0.5'; }

    try {
        const url = '/api/kommo/funnel-live' + (force ? '?force=1' : '');
        const res = await api(url);
        const d = await res.json();
        if (d.ok) {
            _renderFunnelCards(d.data, 'dash-funnel');
        } else {
            console.error('dash funnel-live error:', d.error);
        }
    } catch (e) {
        console.error('dash funnel-live fetch error:', e);
    } finally {
        if (btn) { btn.disabled = false; btn.style.opacity = '1'; }
    }
}

// ---------------------------------------------------------------------------
// Timeline Charts (drill-down)
// ---------------------------------------------------------------------------
let _tlGranularity = 'month';
let _tlDrillMonth = null;

const _tlColors = {
    novos:       '#6366f1',
    rematricula: '#10b981',
    regresso:    '#f59e0b',
    recompra:    '#06b6d4',
    total:       '#8b5cf6',
    calouros_agg:'#6366f1',
};
let _tlMode = 'agregado';
let _tlLastSeries = {};

function toggleTlMode() {
    _tlMode = _tlMode === 'agregado' ? 'detalhado' : 'agregado';
    document.getElementById('tl-mode-btn').textContent = _tlMode === 'agregado' ? 'Ver Detalhado' : 'Ver Agregado';
    _renderGeralChart();
}

function _eSeries(name, data, color) {
    return {
        name, data, type: 'line', smooth: 0.35,
        symbol: 'circle', symbolSize: 6,
        lineStyle: { width: 2, color },
        itemStyle: { color },
        areaStyle: { color: new echarts.graphic.LinearGradient(0,0,0,1,[{offset:0,color:color+'20'},{offset:1,color:color+'02'}]) },
    };
}

function _renderGeralChart() {
    const s = _tlLastSeries;
    const labels = window._tlGeralLabels || [];
    const rematLabel = document.getElementById('tl-remat-label')?.textContent || 'Rematrículas';

    const chart = eInit('chart-geral');
    if (!chart) return;

    let series;
    if (_tlMode === 'agregado') {
        const novos = s.novos || [];
        const regresso = s.regresso || [];
        const recompra = s.recompra || [];
        const calouros = novos.map((v, i) => (v || 0) + (regresso[i] || 0) + (recompra[i] || 0));
        series = [
            _eSeries('Calouros (N+Rg+Rc)', calouros, _tlColors.calouros_agg),
            _eSeries(rematLabel, s.rematricula || [], _tlColors.rematricula),
            _eSeries('Total', s.total || [], _tlColors.total),
        ];
    } else {
        series = [
            _eSeries('Novos', s.novos || [], _tlColors.novos),
            _eSeries(rematLabel, s.rematricula || [], _tlColors.rematricula),
            _eSeries('Regresso', s.regresso || [], _tlColors.regresso),
            _eSeries('Recompra', s.recompra || [], _tlColors.recompra),
            _eSeries('Total', s.total || [], _tlColors.total),
        ];
    }

    chart.setOption({
        backgroundColor: 'transparent',
        grid: eBaseGrid(),
        tooltip: { ...eTooltip(), valueFormatter: v => (v||0).toLocaleString('pt-BR') },
        xAxis: eCategoryAxis(labels),
        yAxis: eValueAxis(),
        series,
        animationDuration: 600,
        animationEasing: 'cubicOut',
    }, true);

    chart.getZr().off('click');
    if (_tlGranularity === 'month') {
        chart.on('click', params => { if (params.dataIndex !== undefined) timelineDrillDown(params.dataIndex); });
    }
}

function _formatLabel(period, gran) {
    if (gran === 'month') {
        const [y, m] = period.split('-');
        const months = ['jan','fev','mar','abr','mai','jun','jul','ago','set','out','nov','dez'];
        return months[parseInt(m)-1] + ' ' + y;
    }
    const [y, m, d] = period.split('-');
    return parseInt(d) + '/' + parseInt(m);
}

async function loadTimeline(from, to) {
    const nivel = document.getElementById('tl-nivel').value;
    const params = new URLSearchParams({ granularity: _tlGranularity });
    if (nivel) params.set('nivel', nivel);
    if (from) params.set('from', from);
    if (to) params.set('to', to);

    try {
        const res = await api('/api/dashboard/timeline?' + params);
        const d = await res.json();
        if (d.error) return;

        const labels = (d.periods || []).map(p => _formatLabel(p, _tlGranularity));
        const rawPeriods = d.periods || [];
        const s = d.series || {};
        const fmt = n => (n||0).toLocaleString('pt-BR');

        const isPosOnly = nivel === 'Pós-Graduação';
        const rematLbl = document.getElementById('tl-remat-label');
        if (rematLbl) rematLbl.textContent = isPosOnly ? 'Veteranos' : 'Rematrículas';

        _tlLastSeries = s;
        window._tlGeralLabels = labels;

        const sum = arr => (arr || []).reduce((a,b) => a+b, 0);
        const novosSum = sum(s.novos);
        const regressoSum = sum(s.regresso);
        const recompraSum = sum(s.recompra);
        const rematSum = sum(s.rematricula);

        if (_tlMode === 'agregado') {
            document.getElementById('tl-novos-label').textContent = 'Calouros (N+Rg+Rc)';
            document.getElementById('tl-novos-total').textContent = fmt(novosSum + regressoSum + recompraSum);
            document.getElementById('tl-leg-regresso').classList.add('hidden');
            document.getElementById('tl-leg-recompra').classList.add('hidden');
        } else {
            document.getElementById('tl-novos-label').textContent = 'Novos';
            document.getElementById('tl-novos-total').textContent = fmt(novosSum);
            document.getElementById('tl-leg-regresso').classList.remove('hidden');
            document.getElementById('tl-leg-recompra').classList.remove('hidden');
        }
        document.getElementById('tl-remat-total').textContent = fmt(rematSum);
        document.getElementById('tl-regresso-total').textContent = fmt(regressoSum);
        document.getElementById('tl-recompra-total').textContent = fmt(recompraSum);
        document.getElementById('tl-total-total').textContent = fmt(sum(s.total));

        _renderGeralChart();

        document.getElementById('tl-period-label').textContent =
            _tlGranularity === 'day' && _tlDrillMonth ? _tlDrillMonth : (d.range ? d.range.from + ' → ' + d.range.to : '');

        document.getElementById('tl-drillup').classList.toggle('hidden', _tlGranularity !== 'day');

        window._tlRawPeriods = rawPeriods;
    } catch (e) { console.error('Timeline error:', e); }
}

function timelineDrillDown(index) {
    const period = window._tlRawPeriods?.[index];
    if (!period || _tlGranularity !== 'month') return;
    const [y, m] = period.split('-');
    const from = `${y}-${m}-01`;
    const lastDay = new Date(parseInt(y), parseInt(m), 0).getDate();
    const to = `${y}-${m}-${String(lastDay).padStart(2,'0')}`;
    _tlGranularity = 'day';
    _tlDrillMonth = period;
    loadTimeline(from, to);
}

function timelineDrillUp() {
    _tlGranularity = 'month';
    _tlDrillMonth = null;
    loadTimeline();
}

// ---------------------------------------------------------------------------
// Ciclo Master Panel
// ---------------------------------------------------------------------------
let _cicloMasterData = null;

async function loadCicloMaster() {
    const loading = document.getElementById('ciclo-master-loading');
    const empty = document.getElementById('ciclo-master-empty');
    const content = document.getElementById('ciclo-master-content');
    loading.classList.remove('hidden');
    empty.classList.add('hidden');
    content.classList.add('hidden');

    try {
        const _nivelParam = document.getElementById('ciclo-filter-nivel').value;
        const res = await api('/api/dashboard/ciclos' + (_nivelParam ? '?nivel=' + encodeURIComponent(_nivelParam) : ''));
        const d = await res.json();
        if (d.error) { loading.textContent = 'Erro: ' + d.error; return; }

        _cicloMasterData = d;
        loading.classList.add('hidden');

        if (!(d.ciclos || []).length && !(d.comparisons)) {
            empty.classList.remove('hidden');
            return;
        }

        content.classList.remove('hidden');
        renderCicloMaster(d);
    } catch (e) {
        loading.textContent = 'Erro ao carregar ciclos.';
        console.error(e);
    }
}

function renderCicloMaster(data) {
    const fmt = n => (n || 0).toLocaleString('pt-BR');
    const pct = (cur, prev) => {
        if (!prev && !cur) return { txt: '—', cls: 'text-gray-600' };
        if (!prev) return { txt: '+100%', cls: 'text-emerald-400' };
        const d = ((cur - prev) / prev * 100);
        return { txt: (d >= 0 ? '+' : '') + d.toFixed(1) + '%', cls: d > 0 ? 'text-emerald-400' : d < 0 ? 'text-rose-400' : 'text-gray-400' };
    };

    const nivelFilter = document.getElementById('ciclo-filter-nivel').value;
    const isPosOnly = nivelFilter === 'Pós-Graduação';
    const rematLabel = isPosOnly ? 'Veteranos' : 'Rematr.';
    const rematLabelFull = isPosOnly ? 'Veteranos' : 'Rematrículas';

    const cmp = data.comparisons || {};
    const ytd = cmp.ytd?.current || {grand_total:0, totals:{}};
    const ytdP = cmp.ytd_prev?.current || {grand_total:0, totals:{}};
    const m6 = cmp.m6?.current || {grand_total:0, totals:{}};
    const m6P = cmp.m6_prev?.current || {grand_total:0, totals:{}};

    const ytdChg = pct(ytd.grand_total, ytdP.grand_total);
    const m6Chg = pct(m6.grand_total, m6P.grand_total);

    function temporalCard(label, period, cur, prev, accent, bgFrom, bgTo, borderC) {
        const total = cur.grand_total || 0;
        const prevTotal = prev.grand_total || 0;
        const ch = pct(total, prevTotal);
        const t = cur.totals || {};
        return `<div class="tremor-card">
            <div class="flex items-center justify-between mb-1">
                <span class="text-[10px] font-medium text-${accent} uppercase tracking-wider">${label}</span>
                <span class="tremor-badge ${ch.cls.includes('emerald') ? 'tremor-badge-emerald' : ch.cls.includes('rose') ? 'tremor-badge-rose' : 'tremor-badge-gray'}">${ch.txt}</span>
            </div>
            <p class="tremor-sublabel mb-1.5">${period}</p>
            <p class="text-xl font-bold text-gray-900 dark:text-gray-50 mb-2">${fmt(total)}</p>
            <div class="grid grid-cols-2 gap-x-2 gap-y-0.5 text-[10px]">
                <div class="flex justify-between"><span class="text-gray-500">Novos</span><span class="text-gray-700 dark:text-gray-300 font-medium">${fmt(t.novos||0)}</span></div>
                <div class="flex justify-between"><span class="text-gray-500">${rematLabel}</span><span class="text-gray-700 dark:text-gray-300 font-medium">${fmt(t.rematricula||0)}</span></div>
                <div class="flex justify-between"><span class="text-gray-500">Regresso</span><span class="text-gray-700 dark:text-gray-300 font-medium">${fmt(t.regresso||0)}</span></div>
                <div class="flex justify-between"><span class="text-gray-500">Recompra</span><span class="text-gray-700 dark:text-gray-300 font-medium">${fmt(t.recompra||0)}</span></div>
            </div>
            <div class="mt-2 pt-2 border-t border-[var(--border)] text-[9px] text-gray-400">vs anterior: <span class="text-gray-600 dark:text-gray-400 font-medium">${fmt(prevTotal)}</span></div>
        </div>`;
    }

    document.getElementById('ciclo-temporal-cards').innerHTML =
        temporalCard(cmp.ytd?.label||'YTD', cmp.ytd?.period||'', ytd, ytdP, 'indigo-400', 'indigo-500', 'blue-500', 'indigo-500') +
        temporalCard(cmp.ytd_prev?.label||'YTD Ant.', cmp.ytd_prev?.period||'', ytdP, {grand_total:0,totals:{}}, 'gray-400', 'gray-500', 'gray-600', 'gray-600') +
        temporalCard(cmp.m6?.label||'6 meses', cmp.m6?.period||'', m6, m6P, 'indigo-400', 'indigo-500', 'indigo-500', 'indigo-500') +
        temporalCard(cmp.m6_prev?.label||'6m Ant.', cmp.m6_prev?.period||'', m6P, {grand_total:0,totals:{}}, 'gray-400', 'gray-500', 'gray-600', 'gray-600');

    // --- Collapsible cycle cards ---
    const filtered = data.ciclos || [];
    const maxTotal = Math.max(...filtered.map(c => c.grand_total), 1);

    const colors = ['indigo', 'violet', 'amber', 'emerald', 'rose', 'blue'];

    document.getElementById('ciclo-cards').innerHTML = filtered.map((c, i) => {
        const color = colors[i % colors.length];
        const prev = filtered[i + 1];
        const chg = prev ? pct(c.grand_total, prev.grand_total) : null;
        const barW = Math.round(c.grand_total / maxTotal * 100);
        const id = 'ciclo-expand-' + i;
        const t = c.totals || {};
        const sits = Object.entries(c.by_situacao || {}).slice(0, 6);
        const polos = Object.entries(c.by_polo || {}).slice(0, 8);

        const cardIsPos = (c.nivel || '').includes('Pós');
        const cardRematShort = cardIsPos ? 'Veteranos' : 'Rematr.';
        const cardRematFull  = cardIsPos ? 'Veteranos' : 'Rematrículas';

        return `<div class="tremor-card overflow-hidden !p-0">
            <button onclick="document.getElementById('${id}').classList.toggle('hidden')" class="w-full px-5 py-3.5 flex items-center justify-between hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-all">
                <div class="flex items-center gap-4 min-w-0">
                    <div class="flex items-center gap-2">
                        <span class="dot-indicator bg-${color}-500"></span>
                        <span class="text-xs font-semibold text-gray-900 dark:text-gray-50 uppercase tracking-wider">${esc(c.nome)}</span>
                        <span class="tremor-badge tremor-badge-gray">${esc(c.nivel)}</span>
                    </div>
                    <div class="flex items-center gap-3 text-[11px]">
                        <span class="text-gray-500">Novos <span class="text-gray-700 dark:text-gray-300 font-medium">${fmt(t.novos||0)}</span></span>
                        <span class="text-gray-500">${cardRematShort} <span class="text-gray-700 dark:text-gray-300 font-medium">${fmt(t.rematricula||0)}</span></span>
                        <span class="text-gray-500">Regresso <span class="text-gray-700 dark:text-gray-300 font-medium">${fmt(t.regresso||0)}</span></span>
                        <span class="text-gray-500">Recompra <span class="text-gray-700 dark:text-gray-300 font-medium">${fmt(t.recompra||0)}</span></span>
                    </div>
                </div>
                <div class="flex items-center gap-3 flex-shrink-0">
                    <span class="text-lg font-semibold text-gray-900 dark:text-gray-50">${fmt(c.grand_total)}</span>
                    ${chg ? `<span class="tremor-badge ${chg.cls.includes('emerald') ? 'tremor-badge-emerald' : chg.cls.includes('rose') ? 'tremor-badge-rose' : 'tremor-badge-gray'}">${chg.txt}</span>` : ''}
                    <span class="material-symbols-outlined text-base text-gray-400">expand_more</span>
                </div>
            </button>
            <div class="tremor-bar !rounded-none !h-[2px]"><div class="tremor-bar-fill bg-${color}-500 !rounded-none" style="width:${barW}%"></div></div>
            <div id="${id}" class="hidden px-5 py-4 bg-[var(--bg-card)]">
                <div class="grid grid-cols-2 lg:grid-cols-3 gap-4">
                    <div>
                        <p class="text-[10px] font-medium text-gray-500 uppercase tracking-wider mb-2">Por Tipo</p>
                        <div class="space-y-1 text-[12px]">
                            <div class="flex justify-between"><span class="text-gray-500 dark:text-gray-400">Novos (Calouros)</span><span class="text-gray-900 dark:text-gray-50 font-mono">${fmt(t.novos||0)}</span></div>
                            <div class="flex justify-between"><span class="text-gray-500 dark:text-gray-400">${cardRematFull}</span><span class="text-gray-900 dark:text-gray-50 font-mono">${fmt(t.rematricula||0)}</span></div>
                            <div class="flex justify-between"><span class="text-gray-500 dark:text-gray-400">Regresso</span><span class="text-gray-900 dark:text-gray-50 font-mono">${fmt(t.regresso||0)}</span></div>
                            <div class="flex justify-between"><span class="text-gray-500 dark:text-gray-400">Recompra</span><span class="text-gray-900 dark:text-gray-50 font-mono">${fmt(t.recompra||0)}</span></div>
                            <div class="flex justify-between border-t border-[var(--border)] pt-1 mt-1"><span class="text-gray-900 dark:text-gray-50 font-semibold">Total</span><span class="text-gray-900 dark:text-gray-50 font-mono font-semibold">${fmt(c.grand_total)}</span></div>
                        </div>
                    </div>
                    <div>
                        <p class="text-[10px] font-medium text-gray-500 uppercase tracking-wider mb-2">Por Situação</p>
                        <div class="space-y-1 text-[12px]">${sits.map(([k,v]) => {
                            const sp = c.grand_total ? Math.round(v/c.grand_total*100) : 0;
                            return `<div class="flex items-center gap-2"><span class="text-gray-500 dark:text-gray-400 flex-1 truncate">${esc(k)}</span><span class="text-gray-900 dark:text-gray-50 font-mono">${fmt(v)}</span><span class="text-gray-400 dark:text-gray-600 text-[10px] w-8 text-right">${sp}%</span></div>`;
                        }).join('')}</div>
                    </div>
                    <div>
                        <p class="text-[10px] font-medium text-gray-500 uppercase tracking-wider mb-2">Top Polos</p>
                        <div class="space-y-1 text-[12px]">${polos.map(([k,v]) => {
                            const pp = c.grand_total ? Math.round(v/c.grand_total*100) : 0;
                            return `<div class="flex items-center gap-2"><span class="text-gray-500 dark:text-gray-400 flex-1 truncate">${esc(k)}</span><span class="text-gray-900 dark:text-gray-50 font-mono">${fmt(v)}</span><span class="text-gray-400 dark:text-gray-600 text-[10px] w-8 text-right">${pp}%</span></div>`;
                        }).join('')}</div>
                    </div>
                </div>
            </div>
        </div>`;
    }).join('');

    const diag = document.getElementById('ciclo-diag');
    if (diag && data.distinct_nivels) {
        const dn = data.distinct_nivels;
        const cfgNivels = [...new Set((data.config || []).map(c => c.nivel))];
        const missing = Object.keys(dn).filter(n => !cfgNivels.includes(n));
        if (missing.length) {
            diag.innerHTML = missing.map(n =>
                `<span class="text-amber-500">⚠ Existem ${dn[n].toLocaleString('pt-BR')} negócios com nível "${n}" mas nenhum ciclo configurado para esse nível.</span>`
            ).join('<br>');
        } else {
            diag.innerHTML = '';
        }
    }
}

let _ciclosConfig = [];

async function populateCicloFilter() {
    try {
        const res = await api('/api/ciclos');
        const list = await res.json();
        _ciclosConfig = list || [];
        const sel = document.getElementById('students-ciclo');
        if (!sel) return;
        const names = [...new Set(list.map(c => c.nome))].sort().reverse();
        sel.innerHTML = '<option value="">Todos os ciclos</option>' +
            names.map(n => `<option value="${esc(n)}">${esc(n)}</option>`).join('');
        if (names.length > 0) {
            sel.value = names[0];
            applyCicloFilter();
        }
    } catch(e) { console.error('Erro ao carregar ciclos:', e); }
}

function applyCicloFilter() {
    const ciclo = document.getElementById('students-ciclo').value;
    if (ciclo) {
        const matching = _ciclosConfig.filter(c => c.nome === ciclo);
        if (matching.length) {
            const starts = matching.map(c => c.dt_inicio).sort();
            const ends = matching.map(c => c.dt_fim).sort().reverse();
            document.getElementById('students-from').value = starts[0];
            document.getElementById('students-to').value = ends[0];
        }
    } else {
        document.getElementById('students-from').value = '';
        document.getElementById('students-to').value = '';
    }
    loadStudentMetrics();
}

// ---------------------------------------------------------------------------
// Filtro ativo por tipo / situação (cards clicáveis)
// ---------------------------------------------------------------------------
let _stuActiveTipo = null;
let _stuActiveSituacao = null;

const _TIPO_LABELS = {
    novos_agg: 'Novos (Calouros+Regresso+Recompra)',
    novos: 'Calouros',
    rematricula: 'Rematrículas',
    regresso: 'Regresso',
    recompra: 'Recompra',
};

function _stuToggleTipo(tipo) {
    _stuActiveTipo = _stuActiveTipo === tipo ? null : tipo;
    loadStudentMetrics();
    _loadInadimplenciaCard();
}

function _stuToggleSituacao(sit) {
    if (_stuActiveSituacao === sit) {
        _stuActiveSituacao = null;
        document.getElementById('students-situacao').value = '';
    } else {
        _stuActiveSituacao = sit;
        document.getElementById('students-situacao').value = sit;
    }
    loadStudentMetrics();
    _loadInadimplenciaCard();
}

function _stuClearTipoSitFilter() {
    _stuActiveTipo = null;
    _stuActiveSituacao = null;
    document.getElementById('students-situacao').value = '';
    loadStudentMetrics();
    _loadInadimplenciaCard();
}

function _stuUpdateActiveFilterBar() {
    const bar = document.getElementById('stu-active-filter-bar');
    const text = document.getElementById('stu-active-filter-text');
    if (!bar) return;
    const parts = [];
    if (_stuActiveTipo) parts.push('Tipo: ' + (_TIPO_LABELS[_stuActiveTipo] || _stuActiveTipo));
    if (_stuActiveSituacao) parts.push('Situação: ' + _stuActiveSituacao);
    if (parts.length) {
        text.textContent = 'Filtrando por: ' + parts.join(' · ');
        bar.classList.remove('hidden');
    } else {
        bar.classList.add('hidden');
    }
}

async function loadStudentMetrics() {
    const dtFrom = document.getElementById('students-from').value;
    const dtTo = document.getElementById('students-to').value;
    const nivel = document.getElementById('students-nivel').value;
    const situacao = document.getElementById('students-situacao').value;
    const ciclo = document.getElementById('students-ciclo').value;
    const params = new URLSearchParams();
    if (ciclo) params.set('ciclo', ciclo);
    if (dtFrom) params.set('from', dtFrom);
    if (dtTo) params.set('to', dtTo);
    if (nivel) params.set('nivel', nivel);
    if (situacao) params.set('situacao', situacao);
    if (_stuActiveTipo) params.set('tipo', _stuActiveTipo);

    const stuContainer = document.getElementById('stu-tipo-cards');
    if (stuContainer) stuContainer.innerHTML = `
        <div class="grid grid-cols-1 md:grid-cols-2 gap-6 mb-4">
            <div class="skeleton skeleton-card p-6"><div class="skeleton skeleton-title"></div><div class="skeleton skeleton-text w-3/4"></div><div class="skeleton" style="height:36px;width:50%;margin-top:12px"></div></div>
            <div class="skeleton skeleton-card p-6"><div class="skeleton skeleton-title"></div><div class="skeleton skeleton-text w-3/4"></div><div class="skeleton" style="height:36px;width:50%;margin-top:12px"></div></div>
        </div>`;

    try {
        const res = await api('/api/dashboard/students?' + params);
        const d = await res.json();
        if (d.error) {
            console.warn('Student metrics error:', d.error);
            if (stuContainer) stuContainer.innerHTML = '<div class="text-center py-4 text-rose-400 text-sm">Erro ao carregar: ' + esc(d.error) + '</div>';
            return;
        }

        const fmt = n => (n || 0).toLocaleString('pt-BR');
        const t = d.totals || {};
        const gt = d.grand_total || 0;

        const stuIsPosOnly = nivel === 'Pós-Graduação';
        const stuRematLabel = stuIsPosOnly ? 'Veteranos' : 'Rematrículas';

        const novosAgg = (t.novos || 0) + (t.regresso || 0) + (t.recompra || 0);
        const remat = t.rematricula || 0;

        const isNovosAgg = _stuActiveTipo === 'novos_agg';
        const isRemat = _stuActiveTipo === 'rematricula';
        const isNovos = _stuActiveTipo === 'novos';
        const isRegresso = _stuActiveTipo === 'regresso';
        const isRecompra = _stuActiveTipo === 'recompra';

        const ringActive = 'ring-2 ring-offset-2 ring-offset-[var(--bg-main)]';

        const pctNovos = gt ? Math.round(novosAgg / gt * 100) : 0;
        const pctRemat = gt ? Math.round(remat / gt * 100) : 0;

        stuContainer.innerHTML = `
            <div class="flex items-center justify-end mb-3">
                <div class="tremor-badge tremor-badge-gray gap-2 px-3 py-1.5 text-sm">
                    <span class="material-symbols-outlined text-base text-indigo-500 dark:text-indigo-400">groups</span>
                    <span class="text-[10px] text-gray-500 uppercase tracking-wider font-medium">Total</span>
                    <span class="text-base font-semibold text-gray-900 dark:text-gray-50">${fmt(gt)}</span>
                </div>
            </div>
            <!-- Category Bar -->
            <div class="category-bar mb-6">
                <div class="bg-blue-500" style="width:${pctNovos}%" title="Novos ${pctNovos}%"></div>
                <div class="bg-emerald-500" style="width:${pctRemat}%" title="${esc(stuRematLabel)} ${pctRemat}%"></div>
            </div>
            <div class="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
                <!-- KPI: Novos -->
                <div class="tremor-card relative cursor-pointer transition-all ${isNovosAgg ? ringActive + ' ring-blue-500' : ''}"
                     onclick="_stuToggleTipo('novos_agg')">
                    <div class="flex items-center justify-between mb-3">
                        <p class="tremor-label">Novos</p>
                        <span class="tremor-badge tremor-badge-blue">${pctNovos}%</span>
                    </div>
                    <p class="tremor-sublabel mb-1">Calouros + Regresso + Recompra</p>
                    <p class="tremor-metric" data-count="${novosAgg}">0</p>
                    <div class="grid grid-cols-3 gap-2 mt-4 pt-4 border-t border-[var(--border)]">
                        <div class="rounded-tremor-default px-3 py-2 cursor-pointer transition-all ${isNovos ? 'bg-blue-50 dark:bg-blue-500/15 ring-1 ring-blue-200 dark:ring-blue-500/30' : 'bg-gray-50 dark:bg-gray-800 hover:bg-gray-100 dark:hover:bg-gray-700'}"
                             onclick="event.stopPropagation(); _stuToggleTipo('novos')">
                            <p class="text-[9px] text-gray-500 dark:text-gray-400 uppercase tracking-wider font-medium">Calouros</p>
                            <p class="text-lg font-semibold text-gray-900 dark:text-gray-50" data-count="${t.novos || 0}">0</p>
                        </div>
                        <div class="rounded-tremor-default px-3 py-2 cursor-pointer transition-all ${isRegresso ? 'bg-amber-50 dark:bg-amber-500/15 ring-1 ring-amber-200 dark:ring-amber-500/30' : 'bg-gray-50 dark:bg-gray-800 hover:bg-gray-100 dark:hover:bg-gray-700'}"
                             onclick="event.stopPropagation(); _stuToggleTipo('regresso')">
                            <p class="text-[9px] text-amber-600 dark:text-amber-400 uppercase tracking-wider font-medium">Regresso</p>
                            <p class="text-lg font-semibold text-gray-900 dark:text-gray-50" data-count="${t.regresso || 0}">0</p>
                        </div>
                        <div class="rounded-tremor-default px-3 py-2 cursor-pointer transition-all ${isRecompra ? 'bg-indigo-50 dark:bg-indigo-500/15 ring-1 ring-indigo-200 dark:ring-indigo-500/30' : 'bg-gray-50 dark:bg-gray-800 hover:bg-gray-100 dark:hover:bg-gray-700'}"
                             onclick="event.stopPropagation(); _stuToggleTipo('recompra')">
                            <p class="text-[9px] text-indigo-600 dark:text-indigo-400 uppercase tracking-wider font-medium">Recompra</p>
                            <p class="text-lg font-semibold text-gray-900 dark:text-gray-50" data-count="${t.recompra || 0}">0</p>
                        </div>
                    </div>
                </div>
                <!-- KPI: Rematrículas -->
                <div class="tremor-card relative cursor-pointer transition-all ${isRemat ? ringActive + ' ring-emerald-500' : ''}"
                     onclick="_stuToggleTipo('rematricula')">
                    <div class="flex items-center justify-between mb-3">
                        <p class="tremor-label">${esc(stuRematLabel)}</p>
                        <span class="tremor-badge tremor-badge-emerald">${pctRemat}%</span>
                    </div>
                    <p class="tremor-sublabel mb-1">Renovações de matrícula</p>
                    <p class="tremor-metric" data-count="${remat}">0</p>
                </div>
            </div>`;

        countUpAll(stuContainer);
        staggerCards(stuContainer);
        highlightNumbers(stuContainer);
        _renderSituacaoCardsClickable('stu-by-situacao', d.by_situacao);
        renderBreakdownBars('stu-by-nivel', d.by_nivel);
        renderPoloRanking('stu-by-polo', d.by_polo);
        renderBreakdown('stu-by-turma', d.by_turma);
        renderBreakdown('stu-by-ciclo', d.by_ciclo);

        _stuUpdateActiveFilterBar();

        const badge = document.getElementById('stu-filter-badge');
        const parts = [];
        if (ciclo) parts.push(`Ciclo ${ciclo}`);
        if (nivel) parts.push(nivel);
        if (dtFrom || dtTo) parts.push(`${dtFrom || '…'} → ${dtTo || '…'}`);
        if (parts.length) {
            badge.textContent = parts.join(' · ');
            badge.classList.remove('hidden');
        } else {
            badge.classList.add('hidden');
        }
    } catch (err) {
        console.error('Student metrics error:', err);
        if (stuContainer) stuContainer.innerHTML = '<div class="text-center py-4 text-rose-400 text-sm">Erro ao carregar métricas</div>';
    }
}

function renderBreakdown(elId, data) {
    const el = document.getElementById(elId);
    if (!data || !Object.keys(data).length) { el.textContent = '—'; return; }
    const total = Object.values(data).reduce((a, b) => a + b, 0);
    el.innerHTML = Object.entries(data).map(([k, v]) => {
        const pct = total ? Math.round(v / total * 100) : 0;
        return `<div class="flex items-center justify-between gap-2">
            <div class="flex items-center gap-2 min-w-0 flex-1">
                <span class="truncate text-gray-700 dark:text-gray-300">${esc(k)}</span>
                <div class="flex-1 progress-bar-bg min-w-[40px] !h-1.5">
                    <div class="progress-bar-fill bg-primary" style="width:${pct}%"></div>
                </div>
            </div>
            <span class="text-xs font-mono text-gray-600 dark:text-gray-400 whitespace-nowrap">${v.toLocaleString('pt-BR')} <span class="text-gray-400 dark:text-gray-600">(${pct}%)</span></span>
        </div>`;
    }).join('');
}

const _sitMeta = {
    'em curso': {
        from: 'emerald-500', to: 'green-500', text: 'emerald', bg: 'emerald', primary: true,
        icon: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 14l9-5-9-5-9 5 9 5z"/><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 14l6.16-3.422a12.083 12.083 0 01.665 6.479A11.952 11.952 0 0012 20.055a11.952 11.952 0 00-6.824-2.998 12.078 12.078 0 01.665-6.479L12 14z"/><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 14l9-5-9-5-9 5 9 5zm0 0l6.16-3.422a12.083 12.083 0 01.665 6.479A11.952 11.952 0 0012 20.055a11.952 11.952 0 00-6.824-2.998 12.078 12.078 0 01.665-6.479L12 14zm-4 6v-7.5l4-2.222"/>',
        desc: 'Alunos ativos cursando',
    },
    'cancelado': {
        from: 'rose-500', to: 'red-600', text: 'rose', bg: 'rose',
        icon: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1"/>',
        desc: 'Evadiram do curso',
    },
    'trancado': {
        from: 'amber-500', to: 'orange-500', text: 'amber', bg: 'amber',
        icon: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 9v6m4-6v6m7-3a9 9 0 11-18 0 9 9 0 0118 0z"/>',
        desc: 'Interromperam o curso',
    },
    'transferido': {
        from: 'violet-500', to: 'purple-600', text: 'violet', bg: 'violet',
        icon: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 7h12m0 0l-4-4m4 4l-4 4m0 6H4m0 0l4 4m-4-4l4-4"/>',
        desc: 'Foram para outro polo',
    },
    '_default': {
        from: 'gray-500', to: 'gray-600', text: 'gray', bg: 'gray',
        icon: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8.228 9c.549-1.165 2.03-2 3.772-2 2.21 0 4 1.343 4 3 0 1.4-1.278 2.575-3.006 2.907-.542.104-.994.54-.994 1.093m0 3h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/>',
        desc: '',
    },
};
const _sitOrder = ['em curso', 'cancelado', 'trancado', 'transferido'];

function _sitLookup(k) { return _sitMeta[k.toLowerCase()] || _sitMeta['_default']; }

function renderSituacaoCards(elId, data) {
    _renderSituacaoCardsClickable(elId, data);
}

const _sitIcons = {
    'em curso': 'school',
    'cancelado': 'cancel',
    'trancado': 'pause_circle',
    'transferido': 'swap_horiz',
    '_default': 'help',
};

function _renderSituacaoCardsClickable(elId, data) {
    const el = document.getElementById(elId);
    if (!data || !Object.keys(data).length) { el.innerHTML = '<span class="text-gray-500 text-sm col-span-4">—</span>'; return; }
    const total = Object.values(data).reduce((a, b) => a + b, 0);
    const keys = Object.keys(data);
    const ordered = _sitOrder
        .map(sk => keys.find(k => k.toLowerCase() === sk))
        .filter(Boolean)
        .concat(keys.filter(k => !_sitOrder.includes(k.toLowerCase())));

    const ringActive = 'ring-2 ring-offset-2 ring-offset-[var(--bg-main)]';

    el.innerHTML = ordered.map(k => {
        const v = data[k];
        const pct = total ? Math.round(v / total * 100) : 0;
        const c = _sitLookup(k);
        const icon = _sitIcons[k.toLowerCase()] || _sitIcons['_default'];
        const isActive = _stuActiveSituacao === k;
        const activeRing = isActive ? `${ringActive} ring-${c.text}-500` : '';

        return `<div class="tremor-card cursor-pointer transition-all ${activeRing}"
                     onclick="_stuToggleSituacao('${esc(k)}')">
            <div class="flex items-center gap-2 mb-3">
                <span class="dot-indicator bg-${c.from}"></span>
                <p class="tremor-label flex-1">${esc(k)}</p>
                <span class="tremor-badge tremor-badge-${c.bg === 'emerald' ? 'emerald' : c.bg === 'rose' ? 'rose' : c.bg === 'amber' ? 'amber' : 'gray'}">${pct}%</span>
            </div>
            <p class="tremor-metric" data-count="${v}">0</p>
            <div class="tremor-bar mt-3">
                <div class="tremor-bar-fill bg-${c.from}" style="width:${Math.min(pct,100)}%"></div>
            </div>
        </div>`;
    }).join('');
    countUpAll(el);
    staggerCards(el);
}

function renderBreakdownBars(elId, data) {
    const el = document.getElementById(elId);
    if (!data || !Object.keys(data).length) { el.textContent = '—'; return; }
    const total = Object.values(data).reduce((a, b) => a + b, 0);
    el.innerHTML = Object.entries(data).map(([k, v]) => {
        const pct = total ? Math.round(v / total * 100) : 0;
        return `<div class="flex items-center justify-between gap-3">
            <div class="flex items-center gap-2 min-w-0 flex-1">
                <span class="truncate text-sm text-gray-700 dark:text-gray-300">${esc(k)}</span>
                <div class="flex-1 progress-bar-bg min-w-[60px] overflow-hidden !h-2">
                    <div class="progress-bar-fill bg-primary" style="width:${pct}%"></div>
                </div>
            </div>
            <span class="text-sm font-mono text-gray-900 text-[var(--text-primary)] font-semibold whitespace-nowrap">${v.toLocaleString('pt-BR')} <span class="text-gray-400 dark:text-gray-500 text-xs">(${pct}%)</span></span>
        </div>`;
    }).join('');
}

function renderPoloRanking(elId, data) {
    const el = document.getElementById(elId);
    if (!data || !Object.keys(data).length) { el.textContent = '—'; return; }
    const entries = Object.entries(data).sort((a, b) => b[1] - a[1]);
    const total = entries.reduce((s, e) => s + e[1], 0);
    const maxVal = entries[0][1];

    const colors = [
        'from-indigo-500 to-blue-500',
        'from-blue-500 to-cyan-500',
        'from-emerald-500 to-teal-500',
        'from-amber-500 to-orange-500',
        'from-pink-500 to-rose-500',
        'from-violet-500 to-purple-500',
    ];

    el.innerHTML = entries.map(([name, count], i) => {
        const pct = total ? (count / total * 100).toFixed(1) : 0;
        const barW = maxVal ? Math.max((count / maxVal * 100), 2) : 0;
        const color = colors[i % colors.length];
        const rank = i + 1;
        const medal = rank <= 3 ? ['🥇','🥈','🥉'][rank - 1] : '';
        const isBold = rank <= 3 ? 'font-semibold' : '';

        return `<div class="flex items-center gap-3 py-2.5 ${i > 0 ? 'border-t border-[var(--border)]' : ''}">
            <span class="w-6 text-center text-xs font-mono ${rank <= 3 ? 'text-[var(--text-primary)]' : 'text-gray-400 dark:text-gray-500'} ${isBold}">${medal || rank}</span>
            <div class="flex-1 min-w-0">
                <div class="flex items-center justify-between mb-1">
                    <span class="text-sm ${isBold} text-gray-700 dark:text-gray-200 truncate">${esc(name)}</span>
                    <span class="text-sm font-mono text-[var(--text-primary)] ${isBold} whitespace-nowrap ml-3">${count.toLocaleString('pt-BR')}</span>
                </div>
                <div class="flex items-center gap-2">
                    <div class="flex-1 h-1.5 rounded-full bg-gray-200 dark:bg-gray-800 overflow-hidden">
                        <div class="h-full rounded-full bg-gradient-to-r ${color} transition-all duration-500" style="width:${barW}%"></div>
                    </div>
                    <span class="text-[10px] font-mono text-gray-400 dark:text-gray-500 w-10 text-right">${pct}%</span>
                </div>
            </div>
        </div>`;
    }).join('');
}

function clearStudentFilter() {
    document.getElementById('students-ciclo').value = '';
    document.getElementById('students-from').value = '';
    document.getElementById('students-to').value = '';
    document.getElementById('students-nivel').value = '';
    document.getElementById('students-situacao').value = '';
    document.getElementById('stu-filter-badge').classList.add('hidden');
    _stuActiveTipo = null;
    _stuActiveSituacao = null;
    loadStudentMetrics();
    _loadInadimplenciaCard();
}

// ---------------------------------------------------------------------------
// Saúde Financeira (Lista de Alunos) — cards clicáveis no Dashboard
// ---------------------------------------------------------------------------
let _inadGeneration = 0;

function _inadToggleCard(key) {
    navigate('inadimplencia');
}

function _inadRenderCards() {
    const container = document.getElementById('dash-inad-cards');
    if (!container || !window._inadLatest) return;
    const d = window._inadLatest;
    const fmt = n => (n || 0).toLocaleString('pt-BR');
    const pct = d.pct_inadimplencia || 0;
    const pctAdim = d.total_alunos ? ((d.adimplentes / d.total_alunos) * 100).toFixed(1) : '0';

    container.innerHTML = `
        <div class="tremor-card cursor-pointer transition-all" onclick="_inadToggleCard('total')">
            <div class="flex items-center gap-2 mb-2">
                <span class="dot-indicator bg-indigo-500"></span>
                <p class="tremor-label">Total Alunos</p>
            </div>
            <p class="tremor-metric" data-count="${d.total_alunos || 0}">0</p>
        </div>
        <div class="tremor-card cursor-pointer transition-all" onclick="_inadToggleCard('adim')">
            <div class="flex items-center gap-2 mb-2">
                <span class="dot-indicator bg-emerald-500"></span>
                <p class="tremor-label flex-1">Adimplentes</p>
                <span class="tremor-badge tremor-badge-emerald">${pctAdim.replace('.', ',')}%</span>
            </div>
            <p class="tremor-metric text-emerald-600 dark:text-emerald-400" data-count="${d.adimplentes || 0}">0</p>
        </div>
        <div class="tremor-card cursor-pointer transition-all" onclick="_inadToggleCard('inadim')">
            <div class="flex items-center gap-2 mb-2">
                <span class="dot-indicator bg-amber-500"></span>
                <p class="tremor-label flex-1">Inadimplentes</p>
                <span class="tremor-badge tremor-badge-amber">${pct.toFixed(1).replace('.', ',')}%</span>
            </div>
            <p class="tremor-metric text-amber-600 dark:text-amber-400" data-count="${d.inadimplentes || 0}">0</p>
        </div>
        <div class="tremor-card cursor-pointer transition-all" onclick="_inadToggleCard('pct')">
            <div class="flex items-center gap-2 mb-2">
                <span class="dot-indicator bg-rose-500"></span>
                <p class="tremor-label">% Inadimplência</p>
            </div>
            <p class="tremor-metric">${pct.toFixed(1).replace('.', ',')}%</p>
            <div class="tremor-bar mt-3">
                <div class="tremor-bar-fill bg-gradient-to-r from-amber-500 to-rose-500" style="width:${Math.min(pct, 100)}%"></div>
            </div>
        </div>`;

    countUpAll(container);
    staggerCards(container);
    highlightNumbers(container);
}

async function _loadInadimplenciaCard() {
    const gen = ++_inadGeneration;

    const section = document.getElementById('dash-inadimplencia-card');
    if (!section) return;

    const cardsEl = document.getElementById('dash-inad-cards');
    if (cardsEl) cardsEl.style.opacity = '0.5';

    const tipo = _stuActiveTipo;
    const situacao = _stuActiveSituacao;
    const nivelEl = document.getElementById('students-nivel');
    const nivel = nivelEl ? nivelEl.value : '';

    try {
        const p = new URLSearchParams();
        if (tipo) p.set('tipo', tipo);
        if (situacao) p.set('situacao', situacao);
        if (nivel) p.set('nivel', nivel);
        const qs = p.toString();
        const url = '/api/lista-alunos/latest' + (qs ? '?' + qs : '');

        const res = await api(url);

        if (gen !== _inadGeneration) return;

        const d = await res.json();

        if (gen !== _inadGeneration) return;

        if (!d.ok && d.error) {
            if (cardsEl) cardsEl.style.opacity = '1';
            return;
        }
        if (!d.ok || !d.has_data) { section.classList.add('hidden'); return; }

        section.classList.remove('hidden');
        window._inadLatest = d;
        _inadRenderCards();

        const dateEl = document.getElementById('dash-inad-date');
        if (dateEl && d.snapshot) {
            let label = d.snapshot.uploaded_at;
            const filterParts = [];
            if (d.filtered_tipo) {
                const tipoLabels = { novos: 'Calouros', rematricula: 'Rematrículas', regresso: 'Regresso', recompra: 'Recompra', novos_agg: 'Novos (Calouros+Regresso+Recompra)' };
                filterParts.push(tipoLabels[d.filtered_tipo] || d.filtered_tipo);
            }
            if (d.filtered_situacao) filterParts.push(d.filtered_situacao);
            if (d.filtered_nivel) filterParts.push(d.filtered_nivel);
            if (filterParts.length) label += '  ·  Filtro: ' + filterParts.join(' + ');
            dateEl.textContent = label;
        }
    } catch (e) {
        if (cardsEl) cardsEl.style.opacity = '1';
    }
}
