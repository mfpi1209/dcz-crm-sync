// =============================================================================
// Painel Supervisor Acadêmico — substitui o Dashboard Acadêmico p/ a categoria
// =============================================================================

let _dsaTimelineChart = null;
let _dsaGran = 'month';

function isSupervisorAcademico() {
    const cat = (document.body.dataset.categoria || '').toLowerCase().trim();
    return cat === 'supervisor acadêmico' || cat === 'supervisor academico';
}

function _dsaFmtN(v, decimals = 0) {
    const n = Number(v) || 0;
    return n.toLocaleString('pt-BR', {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals,
    });
}

function dsaSetGran(gran) {
    _dsaGran = gran;
    document.querySelectorAll('.dsa-gran-btn').forEach(btn => {
        btn.classList.toggle('is-active', btn.dataset.gran === gran);
    });
    _dsaLoadTimeline();
}

async function loadDashboardSupervisorAcademico() {
    const nivelEl = document.getElementById('dsa-nivel');
    const nivel = nivelEl ? nivelEl.value : '';
    const params = new URLSearchParams();
    if (nivel) params.set('nivel', nivel);

    await Promise.all([
        _dsaLoadStudents(params.toString()),
        _dsaLoadInadimplencia(params.toString()),
        _dsaLoadDistribuicao(),
        _dsaLoadTimeline(),
    ]);

    if (typeof _dismissBootSplash === 'function') _dismissBootSplash();
}

// ── carregamento individual ────────────────────────────────────────────────

async function _dsaLoadStudents(qs) {
    try {
        const res = await api('/api/dashboard/students' + (qs ? '?' + qs : ''));
        const d = await res.json();

        const totals = d.totals || {};
        const total = d.grand_total || 0;
        const remats = totals.rematricula || 0;
        const sit = d.by_situacao || {};
        const emCurso = sit['Em Curso'] || sit['EM CURSO'] || sit['em curso'] || 0;
        const concluintes = sit['Concluído'] || sit['Concluido'] || sit['CONCLUIDO'] || sit['CONCLUÍDO'] || 0;

        _dsaSetKpi(0, { title: 'Matriculados', value: _dsaFmtN(emCurso), sub: 'Em curso', icon: 'school', color: 'indigo' });
        _dsaSetKpi(2, { title: 'Rematrículas', value: _dsaFmtN(remats), sub: total > 0 ? `${((remats / total) * 100).toFixed(1)}% do total` : '—', icon: 'autorenew', color: 'emerald' });
        _dsaSetKpi(4, { title: 'Concluintes', value: _dsaFmtN(concluintes), sub: 'No snapshot', icon: 'workspace_premium', color: 'amber' });

        const poloBox = document.getElementById('dsa-by-polo');
        if (poloBox) {
            const polos = d.by_polo || {};
            const entries = Object.entries(polos).sort((a, b) => b[1] - a[1]).slice(0, 8);
            if (!entries.length) {
                poloBox.innerHTML = '<p class="text-xs text-slate-500">Sem dados</p>';
            } else {
                const max = Math.max(...entries.map(e => e[1]), 1);
                poloBox.innerHTML = entries.map(([nome, qtd]) => `
                    <div>
                        <div class="flex items-center justify-between text-xs mb-1">
                            <span class="text-slate-700 dark:text-slate-300 truncate">${nome}</span>
                            <span class="font-bold text-slate-900 dark:text-white tabular-nums">${_dsaFmtN(qtd)}</span>
                        </div>
                        <div class="h-1.5 bg-slate-100 dark:bg-slate-800/60 rounded-full overflow-hidden">
                            <div class="h-full bg-primary" style="width:${(qtd / max * 100).toFixed(1)}%"></div>
                        </div>
                    </div>`).join('');
            }
        }

        // Situação
        const situEl = document.getElementById('dsa-by-situacao');
        if (situEl) {
            const sit = d.by_situacao || {};
            const palette = {
                'Em Curso':       { bg: 'bg-emerald-500/10', fg: 'text-emerald-600 dark:text-emerald-400' },
                'Cancelado':      { bg: 'bg-rose-500/10',    fg: 'text-rose-600 dark:text-rose-400' },
                'Trancado':       { bg: 'bg-amber-500/10',   fg: 'text-amber-600 dark:text-amber-400' },
                'Transferido':    { bg: 'bg-cyan-500/10',    fg: 'text-cyan-600 dark:text-cyan-400' },
                'Concluído':      { bg: 'bg-violet-500/10',  fg: 'text-violet-600 dark:text-violet-400' },
                'Concluido':      { bg: 'bg-violet-500/10',  fg: 'text-violet-600 dark:text-violet-400' },
            };
            const items = Object.entries(sit);
            if (!items.length) {
                situEl.innerHTML = '<p class="text-xs text-slate-500 col-span-2">Sem dados</p>';
            } else {
                situEl.innerHTML = items.map(([k, v]) => {
                    const c = palette[k] || { bg: 'bg-slate-500/10', fg: 'text-slate-600 dark:text-slate-300' };
                    return `<div class="rounded-xl p-3 ${c.bg}">
                        <p class="text-[10px] font-bold uppercase tracking-wider ${c.fg}">${k}</p>
                        <p class="text-xl font-extrabold tabular-nums ${c.fg} mt-1">${_dsaFmtN(v)}</p>
                    </div>`;
                }).join('');
            }
        }

    } catch (e) {
        console.error('dsa students', e);
    }
}

async function _dsaLoadInadimplencia(qs) {
    try {
        const res = await api('/api/lista-alunos/latest' + (qs ? '?' + qs : ''));
        const d = await res.json();
        if (!d.ok || !d.has_data) {
            _dsaSetKpi(1, { title: 'Inadimplentes', value: '—', sub: 'sem snapshot', icon: 'account_balance_wallet', color: 'rose' });
            return;
        }
        const inadim = d.inadimplentes || 0;
        const total = d.total_alunos || 0;
        const pct = d.pct_inadimplencia ?? (total > 0 ? (inadim / total * 100) : 0);
        _dsaSetKpi(1, {
            title: 'Inadimplentes',
            value: _dsaFmtN(inadim),
            sub: total > 0 ? `${pct.toFixed(1)}% de ${_dsaFmtN(total)}` : '—',
            icon: 'account_balance_wallet',
            color: 'rose',
        });

        const snap = document.getElementById('dsa-snap-info');
        if (snap && d.snapshot) {
            snap.innerHTML = `<span class="material-symbols-outlined text-sm text-slate-400">database</span>${d.snapshot.uploaded_at} · ${_dsaFmtN(d.snapshot.row_count)} registros`;
        }
    } catch (e) {
        console.error('dsa inadimplencia', e);
    }
}

async function _dsaLoadDistribuicao() {
    try {
        const res = await api('/api/distribuicao');
        const d = await res.json();
        const captura = d.fila_atendimento ?? 0;
        const distList = d.distribuicao || [];
        const ativos = distList.filter(x => x.status === 'Ativo');
        const totalAtend = distList.reduce((a, x) => a + (parseInt(x.fila) || 0), 0);

        const setN = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = _dsaFmtN(v); };
        setN('dsa-fila-captura', captura);
        setN('dsa-total-atendidos', totalAtend);

        const bar = document.getElementById('dsa-fila-captura-bar');
        if (bar) {
            const max = Math.max(captura, totalAtend, 50);
            bar.style.width = Math.min(100, (captura / max * 100)).toFixed(0) + '%';
        }

        // KPI #4 — Fila de Atendimento
        _dsaSetKpi(3, {
            title: 'Fila de Atendimento',
            value: _dsaFmtN(captura),
            sub: `${ativos.length} distribuidores ativos`,
            icon: 'group',
            color: 'cyan',
        });
    } catch (e) {
        console.error('dsa distribuicao', e);
        _dsaSetKpi(3, { title: 'Fila de Atendimento', value: '—', sub: 'serviço fora', icon: 'group', color: 'cyan' });
    }
}

function _dsaFormatPeriodLabel(p, gran) {
    if (!p) return p;
    const meses = ['Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez'];
    const mMonth = /^(\d{4})-(\d{2})$/.exec(p);
    if (mMonth) return `${meses[parseInt(mMonth[2],10) - 1]}/${mMonth[1].slice(2)}`;
    const mDay = /^(\d{4})-(\d{2})-(\d{2})$/.exec(p);
    if (mDay) return `${mDay[3]}/${mDay[2]}`;
    return p;
}

async function _dsaLoadTimeline() {
    try {
        const nivel = document.getElementById('dsa-nivel')?.value || '';
        const params = new URLSearchParams();
        params.set('granularity', _dsaGran);
        if (nivel) params.set('nivel', nivel);

        const res = await api('/api/dashboard/timeline?' + params.toString());
        const d = await res.json();

        const periods = d.periods || [];
        const series = d.series || {};
        const novos    = series.novos       || [];
        const remat    = series.rematricula || [];
        const regresso = series.regresso    || [];
        const recompra = series.recompra    || [];

        const labels = periods.map(p => _dsaFormatPeriodLabel(p, _dsaGran));

        const setN = (id, arr) => { const el = document.getElementById(id); if (el) el.textContent = _dsaFmtN((arr || []).reduce((a, b) => a + (Number(b) || 0), 0)); };
        setN('dsa-tot-novos', novos);
        setN('dsa-tot-remat', remat);
        setN('dsa-tot-regresso', regresso);
        setN('dsa-tot-recompra', recompra);

        _dsaRenderTimelineChart(labels, novos, remat, regresso, recompra);
    } catch (e) {
        console.error('dsa timeline', e);
    }
}

function _dsaRenderTimelineChart(labels, novos, remat, regresso, recompra) {
    const ctx = document.getElementById('dsa-timeline-chart');
    if (!ctx || typeof Chart === 'undefined') return;
    if (_dsaTimelineChart) { _dsaTimelineChart.destroy(); _dsaTimelineChart = null; }

    const isLight = !document.documentElement.classList.contains('dark');
    const grid = isLight ? 'rgba(15,23,42,0.08)' : 'rgba(148,163,184,0.12)';
    const tickColor = isLight ? '#475569' : '#94a3b8';

    _dsaTimelineChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels,
            datasets: [
                { label: 'Calouros',     data: novos,    backgroundColor: 'rgba(99,102,241,0.85)',  borderRadius: 6, stack: 'mat' },
                { label: 'Rematrículas', data: remat,    backgroundColor: 'rgba(16,185,129,0.85)',  borderRadius: 6, stack: 'mat' },
                { label: 'Regresso',     data: regresso, backgroundColor: 'rgba(245,158,11,0.85)',  borderRadius: 6, stack: 'mat' },
                { label: 'Recompra',     data: recompra, backgroundColor: 'rgba(6,182,212,0.85)',   borderRadius: 6, stack: 'mat' },
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: { display: false },
                tooltip: { backgroundColor: 'rgba(15,23,42,0.95)', titleColor: '#fff', bodyColor: '#e2e8f0', borderColor: 'rgba(148,163,184,0.2)', borderWidth: 1 },
            },
            scales: {
                x: { stacked: true, grid: { color: grid, drawBorder: false }, ticks: { color: tickColor, font: { size: 10 }, maxRotation: 0, autoSkip: true, maxTicksLimit: 14 } },
                y: { stacked: true, beginAtZero: true, grid: { color: grid, drawBorder: false }, ticks: { color: tickColor, font: { size: 10 } } },
            },
        },
    });
}

// ── KPI helper ─────────────────────────────────────────────────────────────

function _dsaSetKpi(idx, c) {
    const wrap = document.getElementById('dsa-kpis');
    if (!wrap) return;
    const colorMap = {
        indigo:  'bg-indigo-500/10  text-indigo-600  dark:text-indigo-400',
        amber:   'bg-amber-500/10   text-amber-600   dark:text-amber-400',
        emerald: 'bg-emerald-500/10 text-emerald-600 dark:text-emerald-400',
        rose:    'bg-rose-500/10    text-rose-600    dark:text-rose-400',
        cyan:    'bg-cyan-500/10    text-cyan-600    dark:text-cyan-400',
    };

    const cards = wrap.children;
    if (!cards[idx]) return;
    cards[idx].outerHTML = `
        <div class="bg-white dark:bg-slate-800/50 rounded-2xl border border-slate-200 dark:border-slate-700/50 shadow-sm p-4 sm:p-5">
            <span class="w-9 h-9 rounded-lg flex items-center justify-center ${colorMap[c.color] || colorMap.indigo} mb-3">
                <span class="material-symbols-outlined text-[18px]">${c.icon}</span>
            </span>
            <p class="text-[10px] font-bold uppercase tracking-[.15em] text-slate-500 leading-tight mb-1">${c.title}</p>
            <p class="text-2xl sm:text-3xl font-extrabold tabular-nums text-slate-900 dark:text-white">${c.value}</p>
            <p class="text-[10px] text-slate-500 mt-1">${c.sub}</p>
        </div>`;
}
