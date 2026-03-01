// ---------------------------------------------------------------------------
// Dashboard
// ---------------------------------------------------------------------------
async function loadDashboard() {
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
}

// ---------------------------------------------------------------------------
// Timeline Charts (drill-down)
// ---------------------------------------------------------------------------
const _tlCharts = {};
let _tlGranularity = 'month';
let _tlDrillMonth = null;

const _tlColors = {
    novos:       { line: '#3b82f6', bg: 'rgba(59,130,246,0.06)' },
    rematricula: { line: '#10b981', bg: 'rgba(16,185,129,0.06)' },
    regresso:    { line: '#f59e0b', bg: 'rgba(245,158,11,0.06)' },
    recompra:    { line: '#06b6d4', bg: 'rgba(6,182,212,0.06)' },
    total:       { line: '#2563eb', bg: 'rgba(37,99,235,0.08)' },
    calouros_agg:{ line: '#3b82f6', bg: 'rgba(59,130,246,0.06)' },
};
let _tlMode = 'agregado';
let _tlLastSeries = {};

function toggleTlMode() {
    _tlMode = _tlMode === 'agregado' ? 'detalhado' : 'agregado';
    document.getElementById('tl-mode-btn').textContent = _tlMode === 'agregado' ? 'Ver Detalhado' : 'Ver Agregado';
    _renderGeralChart();
}

function _buildChartOpts() {
    return {
        responsive: true, maintainAspectRatio: false, animation: { duration: 400 },
        interaction: { mode: 'index', intersect: false },
        onClick: (evt, elements) => { if (elements.length && _tlGranularity === 'month') timelineDrillDown(elements[0].index); },
        plugins: {
            legend: { display: false },
            tooltip: {
                backgroundColor: 'rgba(15,23,42,0.95)', borderColor: 'rgba(100,116,139,0.3)', borderWidth: 1,
                titleFont: { family: 'Inter', size: 11 }, bodyFont: { family: 'JetBrains Mono', size: 12 },
                callbacks: { label: c => c.dataset.label + ': ' + c.parsed.y.toLocaleString('pt-BR') },
            },
        },
        scales: {
            x: { grid: { color: 'rgba(100,116,139,0.08)' }, ticks: { color: '#64748b', font: { size: 10, family: 'Inter' }, maxRotation: 0 } },
            y: { grid: { color: 'rgba(100,116,139,0.08)' }, ticks: { color: '#64748b', font: { size: 10, family: 'JetBrains Mono' },
                callback: v => v >= 1000 ? (v/1000).toFixed(v%1000?1:0)+'k' : v } },
        },
    };
}

function _dsCfg(color, label) {
    return {
        label, data: [], borderColor: color.line, backgroundColor: color.bg,
        borderWidth: 2, pointRadius: 3, pointHoverRadius: 6, pointBackgroundColor: color.line,
        fill: false, tension: 0.35,
    };
}

function _renderGeralChart() {
    const s = _tlLastSeries;
    const labels = window._tlGeralLabels || [];
    const rematLabel = document.getElementById('tl-remat-label')?.textContent || 'Rematrículas';

    if (_tlCharts['chart-geral']) { _tlCharts['chart-geral'].destroy(); delete _tlCharts['chart-geral']; }
    const ctx = document.getElementById('chart-geral');
    if (!ctx) return;

    let datasets;
    if (_tlMode === 'agregado') {
        const novos = s.novos || [];
        const regresso = s.regresso || [];
        const recompra = s.recompra || [];
        const calouros = novos.map((v, i) => (v || 0) + (regresso[i] || 0) + (recompra[i] || 0));
        datasets = [
            { ..._dsCfg(_tlColors.calouros_agg, 'Calouros (Novos+Regresso+Recompra)'), data: calouros },
            { ..._dsCfg(_tlColors.rematricula, rematLabel), data: s.rematricula || [] },
            { ..._dsCfg(_tlColors.total, 'Total'), data: s.total || [] },
        ];
    } else {
        datasets = [
            { ..._dsCfg(_tlColors.novos, 'Novos'), data: s.novos || [] },
            { ..._dsCfg(_tlColors.rematricula, rematLabel), data: s.rematricula || [] },
            { ..._dsCfg(_tlColors.regresso, 'Regresso'), data: s.regresso || [] },
            { ..._dsCfg(_tlColors.recompra, 'Recompra'), data: s.recompra || [] },
            { ..._dsCfg(_tlColors.total, 'Total'), data: s.total || [] },
        ];
    }

    const chart = new Chart(ctx, { type: 'line', data: { labels, datasets }, options: _buildChartOpts() });
    _tlCharts['chart-geral'] = chart;
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
        if (!prev && !cur) return { txt: '—', cls: 'text-slate-600' };
        if (!prev) return { txt: '+100%', cls: 'text-emerald-400' };
        const d = ((cur - prev) / prev * 100);
        return { txt: (d >= 0 ? '+' : '') + d.toFixed(1) + '%', cls: d > 0 ? 'text-emerald-400' : d < 0 ? 'text-rose-400' : 'text-slate-400' };
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
        return `<div class="rounded-xl p-3.5 bg-gradient-to-br from-${bgFrom}/10 to-${bgTo}/10 border border-${borderC}/20">
            <div class="flex items-center justify-between mb-1">
                <span class="text-[10px] font-bold text-${accent} uppercase tracking-wider">${label}</span>
                <span class="text-[10px] font-bold ${ch.cls}">${ch.txt}</span>
            </div>
            <p class="text-[9px] text-slate-600 mb-1.5">${period}</p>
            <p class="text-xl font-bold text-white font-display mb-1.5">${fmt(total)}</p>
            <div class="grid grid-cols-2 gap-x-2 gap-y-0.5 text-[10px]">
                <div class="flex justify-between"><span class="text-slate-500">Novos</span><span class="text-slate-300 font-medium">${fmt(t.novos||0)}</span></div>
                <div class="flex justify-between"><span class="text-slate-500">${rematLabel}</span><span class="text-slate-300 font-medium">${fmt(t.rematricula||0)}</span></div>
                <div class="flex justify-between"><span class="text-slate-500">Regresso</span><span class="text-slate-300 font-medium">${fmt(t.regresso||0)}</span></div>
                <div class="flex justify-between"><span class="text-slate-500">Recompra</span><span class="text-slate-300 font-medium">${fmt(t.recompra||0)}</span></div>
            </div>
            <div class="mt-1.5 pt-1.5 border-t border-slate-700/20 text-[9px] text-slate-600">vs anterior: <span class="text-slate-400 font-medium">${fmt(prevTotal)}</span></div>
        </div>`;
    }

    document.getElementById('ciclo-temporal-cards').innerHTML =
        temporalCard(cmp.ytd?.label||'YTD', cmp.ytd?.period||'', ytd, ytdP, 'indigo-400', 'indigo-500', 'blue-500', 'indigo-500') +
        temporalCard(cmp.ytd_prev?.label||'YTD Ant.', cmp.ytd_prev?.period||'', ytdP, {grand_total:0,totals:{}}, 'slate-400', 'slate-500', 'slate-600', 'slate-600') +
        temporalCard(cmp.m6?.label||'6 meses', cmp.m6?.period||'', m6, m6P, 'cyan-400', 'cyan-500', 'teal-500', 'cyan-500') +
        temporalCard(cmp.m6_prev?.label||'6m Ant.', cmp.m6_prev?.period||'', m6P, {grand_total:0,totals:{}}, 'slate-400', 'slate-500', 'slate-600', 'slate-600');

    // --- Collapsible cycle cards ---
    const filtered = data.ciclos || [];
    const maxTotal = Math.max(...filtered.map(c => c.grand_total), 1);

    const colors = ['cyan', 'violet', 'amber', 'emerald', 'rose', 'indigo'];

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

        return `<div class="rounded-xl border border-${color}-500/20 overflow-hidden">
            <button onclick="document.getElementById('${id}').classList.toggle('hidden')" class="w-full px-4 py-3 flex items-center justify-between bg-gradient-to-r from-${color}-500/8 to-transparent hover:from-${color}-500/12 transition-all">
                <div class="flex items-center gap-4 min-w-0">
                    <div class="flex items-center gap-2">
                        <span class="text-xs font-bold text-${color}-400 uppercase tracking-wider">${esc(c.nome)}</span>
                        <span class="text-[10px] text-slate-500">${esc(c.nivel)}</span>
                    </div>
                    <div class="flex items-center gap-3 text-[11px]">
                        <span class="text-slate-500">Novos <span class="text-slate-300 font-medium">${fmt(t.novos||0)}</span></span>
                        <span class="text-slate-500">${cardRematShort} <span class="text-slate-300 font-medium">${fmt(t.rematricula||0)}</span></span>
                        <span class="text-slate-500">Regresso <span class="text-slate-300 font-medium">${fmt(t.regresso||0)}</span></span>
                        <span class="text-slate-500">Recompra <span class="text-slate-300 font-medium">${fmt(t.recompra||0)}</span></span>
                    </div>
                </div>
                <div class="flex items-center gap-3 flex-shrink-0">
                    <span class="text-lg font-bold text-white font-display">${fmt(c.grand_total)}</span>
                    ${chg ? `<span class="text-[10px] font-bold ${chg.cls}">${chg.txt}</span>` : ''}
                    <svg class="w-4 h-4 text-slate-500 transition-transform" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/></svg>
                </div>
            </button>
            <div class="relative h-0.5 bg-slate-800/40"><div class="h-0.5 bg-gradient-to-r from-${color}-500/60 to-${color}-500/20" style="width:${barW}%"></div></div>
            <div id="${id}" class="hidden px-4 py-3 bg-slate-900/30">
                <div class="grid grid-cols-2 lg:grid-cols-3 gap-4">
                    <div>
                        <p class="text-[10px] font-bold text-slate-500 uppercase tracking-wider mb-2">Por Tipo</p>
                        <div class="space-y-1 text-[12px]">
                            <div class="flex justify-between"><span class="text-slate-400">Novos (Calouros)</span><span class="text-white font-mono">${fmt(t.novos||0)}</span></div>
                            <div class="flex justify-between"><span class="text-slate-400">${cardRematFull}</span><span class="text-white font-mono">${fmt(t.rematricula||0)}</span></div>
                            <div class="flex justify-between"><span class="text-slate-400">Regresso</span><span class="text-white font-mono">${fmt(t.regresso||0)}</span></div>
                            <div class="flex justify-between"><span class="text-slate-400">Recompra</span><span class="text-white font-mono">${fmt(t.recompra||0)}</span></div>
                            <div class="flex justify-between border-t border-slate-700/30 pt-1 mt-1"><span class="text-white font-bold">Total</span><span class="text-white font-mono font-bold">${fmt(c.grand_total)}</span></div>
                        </div>
                    </div>
                    <div>
                        <p class="text-[10px] font-bold text-slate-500 uppercase tracking-wider mb-2">Por Situação</p>
                        <div class="space-y-1 text-[12px]">${sits.map(([k,v]) => {
                            const sp = c.grand_total ? Math.round(v/c.grand_total*100) : 0;
                            return `<div class="flex items-center gap-2"><span class="text-slate-400 flex-1 truncate">${esc(k)}</span><span class="text-white font-mono">${fmt(v)}</span><span class="text-slate-600 text-[10px] w-8 text-right">${sp}%</span></div>`;
                        }).join('')}</div>
                    </div>
                    <div>
                        <p class="text-[10px] font-bold text-slate-500 uppercase tracking-wider mb-2">Top Polos</p>
                        <div class="space-y-1 text-[12px]">${polos.map(([k,v]) => {
                            const pp = c.grand_total ? Math.round(v/c.grand_total*100) : 0;
                            return `<div class="flex items-center gap-2"><span class="text-slate-400 flex-1 truncate">${esc(k)}</span><span class="text-white font-mono">${fmt(v)}</span><span class="text-slate-600 text-[10px] w-8 text-right">${pp}%</span></div>`;
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

    const stuContainer = document.getElementById('stu-tipo-cards');
    if (stuContainer) stuContainer.innerHTML = '<div class="col-span-full text-center py-8 text-slate-500"><svg class="w-6 h-6 animate-spin inline-block mr-2 text-slate-600" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"/><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"/></svg>Carregando métricas...</div>';

    try {
        const res = await api('/api/dashboard/students?' + params);
        const d = await res.json();
        if (d.error) {
            console.warn('Student metrics error:', d.error);
            if (stuContainer) stuContainer.innerHTML = '<div class="col-span-full text-center py-4 text-rose-400 text-sm">Erro ao carregar: ' + esc(d.error) + '</div>';
            return;
        }

        const fmt = n => (n || 0).toLocaleString('pt-BR');
        const gt = d.grand_total || 1;
        const det = d.by_tipo_detail || {};

        const stuIsPosOnly = nivel === 'Pós-Graduação';
        const stuRematLabel = stuIsPosOnly ? 'Veteranos' : 'Rematrículas';
        const tipoCards = [
            { key: 'novos', label: 'Novos (Calouros)', accent: 'indigo', bgFrom: 'indigo-500', bgTo: 'blue-500', border: 'indigo-500', icon: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M18 9v3m0 0v3m0-3h3m-3 0h-3m-2-5a4 4 0 11-8 0 4 4 0 018 0zM3 20a6 6 0 0112 0v1H3v-1z"/>' },
            { key: 'rematricula', label: stuRematLabel, accent: 'emerald', bgFrom: 'emerald-500', bgTo: 'teal-500', border: 'emerald-500', icon: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/>' },
            { key: 'regresso', label: 'Regresso', accent: 'amber', bgFrom: 'amber-500', bgTo: 'orange-500', border: 'amber-500', icon: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 10h10a8 8 0 018 8v2M3 10l6 6m-6-6l6-6"/>' },
            { key: 'recompra', label: 'Recompra', accent: 'cyan', bgFrom: 'cyan-500', bgTo: 'sky-500', border: 'cyan-500', icon: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8c-1.657 0-3 .895-3 2s1.343 2 3 2 3 .895 3 2-1.343 2-3 2m0-8c1.11 0 2.08.402 2.599 1M12 8V7m0 1v8m0 0v1m0-1c-1.11 0-2.08-.402-2.599-1M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/>' },
            { key: '_total', label: 'Total Geral', accent: 'violet', bgFrom: 'violet-500', bgTo: 'pink-500', border: 'violet-500', icon: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0z"/>' },
        ];

        document.getElementById('stu-tipo-cards').innerHTML = tipoCards.map(tc => {
            const val = tc.key === '_total' ? d.grand_total : (d.totals[tc.key] || 0);
            const pctVal = tc.key === '_total' ? 100 : (gt ? Math.round(val / gt * 100) : 0);
            return `<div class="glass-card p-5 relative overflow-hidden">
                <div class="absolute top-0 left-0 w-1 h-full bg-gradient-to-b from-${tc.bgFrom} to-${tc.bgTo}"></div>
                <div class="flex items-center gap-3 mb-3">
                    <div class="w-10 h-10 rounded-xl bg-${tc.accent}-500/15 flex items-center justify-center">
                        <svg class="w-5 h-5 text-${tc.accent}-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">${tc.icon}</svg>
                    </div>
                    <div class="flex-1 min-w-0">
                        <p class="text-[10px] font-bold text-slate-500 uppercase tracking-wider">${tc.label}</p>
                        <span class="text-[11px] font-bold text-${tc.accent}-400">${pctVal}%</span>
                    </div>
                </div>
                <p class="text-4xl font-bold text-white font-display leading-tight">${fmt(val)}</p>
            </div>`;
        }).join('');

        renderSituacaoCards('stu-by-situacao', d.by_situacao);
        renderBreakdownBars('stu-by-nivel', d.by_nivel);
        renderBreakdownBars('stu-by-polo', d.by_polo);
        renderBreakdown('stu-by-turma', d.by_turma);
        renderBreakdown('stu-by-ciclo', d.by_ciclo);

        const badge = document.getElementById('stu-filter-badge');
        const parts = [];
        if (ciclo) parts.push(`Ciclo ${ciclo}`);
        if (nivel) parts.push(nivel);
        if (situacao) parts.push(situacao);
        if (dtFrom || dtTo) parts.push(`${dtFrom || '…'} → ${dtTo || '…'}`);
        if (parts.length) {
            badge.textContent = parts.join(' · ');
            badge.classList.remove('hidden');
        } else {
            badge.classList.add('hidden');
        }
    } catch (err) {
        console.error('Student metrics error:', err);
        if (stuContainer) stuContainer.innerHTML = '<div class="col-span-full text-center py-4 text-rose-400 text-sm">Erro ao carregar métricas</div>';
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
                <span class="truncate">${esc(k)}</span>
                <div class="flex-1 h-1.5 rounded-full bg-slate-700/50 min-w-[40px]">
                    <div class="h-1.5 rounded-full bg-gradient-to-r from-indigo-500 to-violet-500" style="width:${pct}%"></div>
                </div>
            </div>
            <span class="text-xs font-mono text-slate-400 whitespace-nowrap">${v.toLocaleString('pt-BR')} <span class="text-slate-600">(${pct}%)</span></span>
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
        from: 'slate-500', to: 'slate-600', text: 'slate', bg: 'slate',
        icon: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8.228 9c.549-1.165 2.03-2 3.772-2 2.21 0 4 1.343 4 3 0 1.4-1.278 2.575-3.006 2.907-.542.104-.994.54-.994 1.093m0 3h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/>',
        desc: '',
    },
};
const _sitOrder = ['em curso', 'cancelado', 'trancado', 'transferido'];

function _sitLookup(k) { return _sitMeta[k.toLowerCase()] || _sitMeta['_default']; }

function renderSituacaoCards(elId, data) {
    const el = document.getElementById(elId);
    if (!data || !Object.keys(data).length) { el.innerHTML = '<span class="text-slate-500 text-sm col-span-4">—</span>'; return; }
    const total = Object.values(data).reduce((a, b) => a + b, 0);
    const keys = Object.keys(data);
    const ordered = _sitOrder
        .map(sk => keys.find(k => k.toLowerCase() === sk))
        .filter(Boolean)
        .concat(keys.filter(k => !_sitOrder.includes(k.toLowerCase())));
    el.innerHTML = ordered.map(k => {
        const v = data[k];
        const pct = total ? Math.round(v / total * 100) : 0;
        const c = _sitLookup(k);
        const highlight = c.primary ? 'ring-1 ring-emerald-500/30' : '';
        return `<div class="glass-card p-4 relative overflow-hidden ${highlight}">
            <div class="absolute top-0 left-0 w-1.5 h-full bg-gradient-to-b from-${c.from} to-${c.to}"></div>
            <div class="flex items-center gap-3 mb-3">
                <div class="w-10 h-10 rounded-xl bg-${c.bg}-500/15 flex items-center justify-center ${c.primary ? 'ring-1 ring-emerald-500/20' : ''}">
                    <svg class="w-5 h-5 text-${c.text}-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">${c.icon}</svg>
                </div>
                <div class="flex-1 min-w-0">
                    <div class="flex items-center justify-between">
                        <span class="text-xs font-bold text-${c.text}-400 uppercase tracking-wider">${esc(k)}</span>
                        <span class="text-[10px] font-bold text-${c.text}-400 bg-${c.bg}-500/10 px-2 py-0.5 rounded-full">${pct}%</span>
                    </div>
                    <p class="text-[10px] text-slate-500 mt-0.5">${c.desc}</p>
                </div>
            </div>
            <p class="text-3xl font-bold text-white font-display mb-2">${v.toLocaleString('pt-BR')}</p>
            <div class="w-full h-1.5 rounded-full bg-slate-700/50">
                <div class="h-1.5 rounded-full bg-gradient-to-r from-${c.from} to-${c.to} transition-all" style="width:${Math.min(pct,100)}%"></div>
            </div>
        </div>`;
    }).join('');
}

function renderBreakdownBars(elId, data) {
    const el = document.getElementById(elId);
    if (!data || !Object.keys(data).length) { el.textContent = '—'; return; }
    const total = Object.values(data).reduce((a, b) => a + b, 0);
    el.innerHTML = Object.entries(data).map(([k, v]) => {
        const pct = total ? Math.round(v / total * 100) : 0;
        return `<div class="flex items-center justify-between gap-3">
            <div class="flex items-center gap-2 min-w-0 flex-1">
                <span class="truncate text-sm text-slate-300">${esc(k)}</span>
                <div class="flex-1 h-2 rounded-full bg-slate-700/50 min-w-[60px]">
                    <div class="h-2 rounded-full bg-gradient-to-r from-indigo-500 to-violet-500 transition-all" style="width:${pct}%"></div>
                </div>
            </div>
            <span class="text-sm font-mono text-white font-semibold whitespace-nowrap">${v.toLocaleString('pt-BR')} <span class="text-slate-500 text-xs">(${pct}%)</span></span>
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
    loadStudentMetrics();
}
