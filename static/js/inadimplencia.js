// ===========================================================================
// INADIMPLÊNCIA - Histórico, Comparação e Análise
// ===========================================================================
// ECharts instances managed by eInit()

const _inadColors = [
    '#f59e0b', '#6366f1', '#10b981', '#ef4444', '#8b5cf6',
    '#ec4899', '#14b8a6', '#f97316', '#06b6d4', '#84cc16',
];

function _inadClearDates() {
    document.getElementById('inad-date-from').value = '';
    document.getElementById('inad-date-to').value = '';
    loadInadimplencia();
}

async function loadInadimplencia() {
    _loadSaudeFinanceira();
    try {
        const df = document.getElementById('inad-date-from').value;
        const dt = document.getElementById('inad-date-to').value;
        let url = '/api/inadimplencia/historico';
        const qs = [];
        if (df) qs.push('date_from=' + df);
        if (dt) qs.push('date_to=' + dt);
        if (qs.length) url += '?' + qs.join('&');

        const res = await api(url);
        const d = await res.json();
        const series = d.series || [];

        _renderInadKPIs(series);
        _renderInadTrendChart(series);
        _renderInadHistoryChart(series);
        _renderInadBreakdownChart(series);
        _renderInadComparison(series);
        _renderInadTable(series);
    } catch(e) {
        console.error('Erro ao carregar inadimplência:', e);
    }
}

function _varBadge(current, previous) {
    if (previous === 0 && current === 0) return '<span class="text-gray-500">—</span>';
    if (previous === 0) return '<span class="text-red-400">+' + current.toLocaleString('pt-BR') + ' novo(s)</span>';
    const diff = current - previous;
    const pct = ((diff / previous) * 100).toFixed(1);
    if (diff > 0) return `<span class="text-red-400">+${diff.toLocaleString('pt-BR')} (+${pct}%)</span>`;
    if (diff < 0) return `<span class="text-emerald-400">${diff.toLocaleString('pt-BR')} (${pct}%)</span>`;
    return '<span class="text-gray-400">Sem variação</span>';
}

function _renderInadKPIs(series) {
    const totalEl = document.getElementById('inad-total');
    const snapEl = document.getElementById('inad-snap-count');
    const gradEl = document.getElementById('inad-grad');
    const posEl = document.getElementById('inad-pos');
    const totalVar = document.getElementById('inad-total-var');
    const gradVar = document.getElementById('inad-grad-var');
    const posVar = document.getElementById('inad-pos-var');
    const rangeEl = document.getElementById('inad-snap-range');

    snapEl.textContent = series.length.toLocaleString('pt-BR');

    if (series.length === 0) {
        totalEl.textContent = '—';
        gradEl.textContent = '—';
        posEl.textContent = '—';
        totalVar.innerHTML = '<span class="text-gray-500">Nenhum snapshot</span>';
        gradVar.innerHTML = '';
        posVar.innerHTML = '';
        rangeEl.textContent = 'Nenhum snapshot';
        return;
    }

    const last = series[series.length - 1];
    const first = series[0];
    const prev = series.length >= 2 ? series[series.length - 2] : null;

    totalEl.textContent = last.total.toLocaleString('pt-BR');
    gradEl.textContent = (last.by_nivel['Graduação'] || 0).toLocaleString('pt-BR');
    posEl.textContent = (last.by_nivel['Pós-Graduação'] || 0).toLocaleString('pt-BR');

    if (prev) {
        totalVar.innerHTML = _varBadge(last.total, prev.total);
        gradVar.innerHTML = _varBadge(last.by_nivel['Graduação'] || 0, prev.by_nivel['Graduação'] || 0);
        posVar.innerHTML = _varBadge(last.by_nivel['Pós-Graduação'] || 0, prev.by_nivel['Pós-Graduação'] || 0);
    } else {
        totalVar.innerHTML = '<span class="text-gray-500">' + last.date + '</span>';
        gradVar.innerHTML = '';
        posVar.innerHTML = '';
    }

    if (series.length >= 2) {
        rangeEl.textContent = first.date.split(' ')[0] + ' → ' + last.date.split(' ')[0];
    } else {
        rangeEl.textContent = last.date;
    }
}

function _renderInadTrendChart(series) {
    const el = document.getElementById('inad-trend-chart');
    const emptyMsg = document.getElementById('inad-trend-empty');

    if (series.length < 2) {
        emptyMsg.classList.remove('hidden');
        if (el) el.style.display = 'none';
        return;
    }
    emptyMsg.classList.add('hidden');
    if (el) el.style.display = 'block';

    const chart = eInit('inad-trend-chart');
    if (!chart) return;

    const labels = series.map(s => s.date.split(' ')[0]);
    const totals = series.map(s => s.total);
    const grads = series.map(s => s.by_nivel['Graduação'] || 0);
    const poss = series.map(s => s.by_nivel['Pós-Graduação'] || 0);

    chart.setOption({
        backgroundColor: 'transparent',
        grid: eBaseGrid(),
        tooltip: { ...eTooltip(), valueFormatter: v => (v||0).toLocaleString('pt-BR') },
        legend: { bottom: 0, textStyle: { color: eThemeColors().textColor, fontSize: 11 } },
        xAxis: { ...eCategoryAxis(labels), axisLabel: { ...eCategoryAxis(labels).axisLabel, rotate: 30 } },
        yAxis: eValueAxis({ min: 0 }),
        series: [
            { name: 'Total', type: 'line', data: totals, smooth: 0.3, symbol: 'circle', symbolSize: 6, lineStyle: { width: 2.5, color: '#f59e0b' }, itemStyle: { color: '#f59e0b' }, areaStyle: { color: 'rgba(245,158,11,0.1)' } },
            { name: 'Graduação', type: 'line', data: grads, smooth: 0.3, symbol: 'circle', symbolSize: 4, lineStyle: { width: 1.5, color: '#38bdf8', type: 'dashed' }, itemStyle: { color: '#38bdf8' } },
            { name: 'Pós-Graduação', type: 'line', data: poss, smooth: 0.3, symbol: 'circle', symbolSize: 4, lineStyle: { width: 1.5, color: '#a78bfa', type: 'dashed' }, itemStyle: { color: '#a78bfa' } },
        ],
        animationDuration: 600,
    }, true);
}

function _renderInadHistoryChart(series) {
    const el = document.getElementById('inad-history-chart');
    const emptyMsg = document.getElementById('inad-chart-empty');
    const filterNivel = document.getElementById('inad-filter-nivel').value;
    const groupBy = document.getElementById('inad-group-by').value;

    if (series.length < 1) {
        emptyMsg.classList.remove('hidden');
        if (el) el.style.display = 'none';
        return;
    }
    emptyMsg.classList.add('hidden');
    if (el) el.style.display = 'block';

    const chart = eInit('inad-history-chart');
    if (!chart) return;

    const labels = series.map(s => s.date.split(' ')[0]);
    const groupKey = `by_${groupBy}`;
    const allKeys = new Set();
    series.forEach(s => Object.keys(s[groupKey] || {}).forEach(k => allKeys.add(k)));
    let keys = [...allKeys].sort();
    if (filterNivel && groupBy === 'nivel') keys = keys.filter(k => k === filterNivel);

    chart.setOption({
        backgroundColor: 'transparent',
        grid: eBaseGrid(),
        tooltip: { ...eTooltip(), valueFormatter: v => (v||0).toLocaleString('pt-BR') },
        legend: { bottom: 0, textStyle: { color: eThemeColors().textColor, fontSize: 11 } },
        xAxis: { ...eCategoryAxis(labels), axisLabel: { ...eCategoryAxis(labels).axisLabel, rotate: 30 } },
        yAxis: eValueAxis({ min: 0 }),
        series: keys.map((key, i) => ({
            name: key || 'N/I', type: 'bar', stack: 'total',
            data: series.map(s => (s[groupKey] || {})[key] || 0),
            itemStyle: { color: _inadColors[i % _inadColors.length], borderRadius: i === keys.length-1 ? [2,2,0,0] : 0 },
        })),
        animationDuration: 600,
    }, true);
}

function _renderInadBreakdownChart(series) {
    const titleEl = document.getElementById('inad-breakdown-title');
    const groupBy = document.getElementById('inad-group-by').value;
    const groupLabels = { nivel: 'Distribuição por Nível', tipo: 'Distribuição por Tipo', turma: 'Distribuição por Turma' };
    titleEl.textContent = groupLabels[groupBy] || 'Distribuição';

    if (series.length === 0) return;

    const chart = eInit('inad-breakdown-chart');
    if (!chart) return;

    const last = series[series.length - 1];
    const groupKey = `by_${groupBy}`;
    const group = last[groupKey] || {};
    const sorted = Object.entries(group).sort((a, b) => b[1] - a[1]);

    chart.setOption({
        backgroundColor: 'transparent',
        tooltip: { ...eTooltip('item'), formatter: p => `${p.name}: ${p.value.toLocaleString('pt-BR')} (${p.percent}%)` },
        legend: { orient: 'vertical', right: 0, top: 'center', textStyle: { color: eThemeColors().textColor, fontSize: 11 } },
        series: [{
            type: 'pie', radius: ['50%', '75%'], center: ['35%', '50%'],
            label: { show: false }, emphasis: { label: { show: true, fontSize: 13, fontWeight: 'bold' } },
            data: sorted.map(([k, v], i) => ({ name: k || 'N/I', value: v, itemStyle: { color: _inadColors[i % _inadColors.length] } })),
            itemStyle: { borderRadius: 4, borderColor: eThemeColors().bg === 'transparent' ? undefined : eThemeColors().bg, borderWidth: 2 },
        }],
        animationDuration: 600,
    }, true);
}

function _renderInadComparison(series) {
    const card = document.getElementById('inad-comparison-card');
    const body = document.getElementById('inad-comparison-body');

    if (series.length < 2) {
        card.style.display = 'none';
        return;
    }
    card.style.display = '';

    const first = series[0];
    const last = series[series.length - 1];
    const groupBy = document.getElementById('inad-group-by').value;
    const groupKey = `by_${groupBy}`;
    const groupLabels = { nivel: 'Nível', tipo: 'Tipo de Aluno', turma: 'Turma' };

    const allKeys = new Set();
    Object.keys(first[groupKey] || {}).forEach(k => allKeys.add(k));
    Object.keys(last[groupKey] || {}).forEach(k => allKeys.add(k));
    const keys = [...allKeys].sort();

    let html = `
        <div class="glass-card p-4 bg-gray-100 dark:bg-gray-800/30 rounded-xl">
            <p class="text-[10px] font-bold text-gray-500 uppercase tracking-wider mb-3">Primeiro: ${first.date.split(' ')[0]}</p>
            <p class="text-xl font-bold text-[var(--text-primary)]">${first.total.toLocaleString('pt-BR')}</p>
        </div>
        <div class="glass-card p-4 bg-gray-100 dark:bg-gray-800/30 rounded-xl">
            <p class="text-[10px] font-bold text-gray-500 uppercase tracking-wider mb-3">Último: ${last.date.split(' ')[0]}</p>
            <p class="text-xl font-bold text-[var(--text-primary)]">${last.total.toLocaleString('pt-BR')}</p>
        </div>
        <div class="glass-card p-4 bg-gray-100 dark:bg-gray-800/30 rounded-xl">
            <p class="text-[10px] font-bold text-gray-500 uppercase tracking-wider mb-3">Variação Total</p>
            <p class="text-xl font-bold">${_varBadge(last.total, first.total)}</p>
        </div>`;

    if (keys.length > 0 && keys.length <= 10) {
        html += `<div class="md:col-span-3 mt-2">
            <p class="text-xs font-semibold text-gray-400 mb-2">Detalhamento por ${groupLabels[groupBy] || groupBy}</p>
            <div class="grid grid-cols-2 md:grid-cols-4 gap-3">`;
        keys.forEach(k => {
            const v1 = (first[groupKey] || {})[k] || 0;
            const v2 = (last[groupKey] || {})[k] || 0;
            html += `<div class="p-3 rounded-lg bg-gray-100 dark:bg-gray-800/40">
                <p class="text-[10px] text-gray-500 font-medium truncate" title="${k}">${k || 'N/I'}</p>
                <p class="text-sm font-bold text-[var(--text-primary)] mt-1">${v1.toLocaleString('pt-BR')} → ${v2.toLocaleString('pt-BR')}</p>
                <p class="text-xs mt-0.5">${_varBadge(v2, v1)}</p>
            </div>`;
        });
        html += `</div></div>`;
    }

    body.innerHTML = html;
}

function _renderInadTable(series) {
    const container = document.getElementById('inad-history-tbody')?.closest('table')?.parentElement;
    if (!container) return;
    container.id = container.id || 'inad-table-container';

    if (series.length === 0) {
        container.innerHTML = '<p class="py-4 text-center text-gray-500 text-sm">Nenhum snapshot encontrado.</p>';
        return;
    }

    const reversed = series.slice().reverse();
    const rows = reversed.map((s, idx) => {
        const realIdx = series.length - 1 - idx;
        const prev = realIdx > 0 ? series[realIdx - 1] : null;
        return {
            date: s.date,
            nivel: s.snap_nivel || 'Todos',
            total: s.total,
            grad: s.by_nivel['Graduação'] || 0,
            pos: s.by_nivel['Pós-Graduação'] || 0,
            var_total: prev ? s.total - prev.total : null,
            _prev: prev,
        };
    });

    renderSortableTable(container.id, {
        columns: [
            { key: 'date', label: 'Data' },
            { key: 'nivel', label: 'Nível', render: v => {
                const cls = v.includes('Pós') ? 'tremor-badge-rose' : v === 'Todos' ? 'tremor-badge-gray' : 'tremor-badge-blue';
                return `<span class="tremor-badge ${cls}">${esc(v)}</span>`;
            }},
            { key: 'total', label: 'Total', align: 'right', render: v => `<span class="font-semibold">${v.toLocaleString('pt-BR')}</span>` },
            { key: 'grad', label: 'Graduação', align: 'right', render: v => `<span class="text-blue-500">${v.toLocaleString('pt-BR')}</span>` },
            { key: 'pos', label: 'Pós-Grad.', align: 'right', render: v => `<span class="text-violet-500">${v.toLocaleString('pt-BR')}</span>` },
            { key: 'var_total', label: 'Variação', align: 'right', render: (v, r) => r._prev ? _varBadge(r.total, r._prev.total) : '—' },
        ],
        rows,
        pageSize: 15,
    });
}

// ===========================================================================
// SAÚDE FINANCEIRA (Lista de Alunos)
// ===========================================================================

async function _loadSaudeFinanceira() {
    const section = document.getElementById('inad-saude-section');
    if (!section) return;

    try {
        const res = await api('/api/lista-alunos/historico');
        const d = await res.json();
        const series = d.series || [];

        if (!series.length) { section.classList.add('hidden'); return; }
        section.classList.remove('hidden');

        const last = series[series.length - 1];
        _renderSfKPIs(last);
        _renderSfTrendChart(series);
        _renderSfPolos(last.inad_by_polo || {}, last.total_alunos || 1);
        _renderSfHistoryTable(series);

        const dateEl = document.getElementById('inad-saude-date');
        if (dateEl) dateEl.textContent = last.date;
    } catch (e) {
        console.error('Erro ao carregar saúde financeira:', e);
    }
}

function _renderSfKPIs(data) {
    const fmt = n => (n || 0).toLocaleString('pt-BR');
    document.getElementById('inad-sf-total').textContent = fmt(data.total_alunos);
    document.getElementById('inad-sf-adim').textContent = fmt(data.adimplentes);
    document.getElementById('inad-sf-inadim').textContent = fmt(data.inadimplentes);

    const pct = data.pct_inadimplencia || 0;
    document.getElementById('inad-sf-pct').textContent = pct.toFixed(1).replace('.', ',') + '%';
    document.getElementById('inad-sf-bar').style.width = Math.min(pct, 100) + '%';

    const pctAdim = data.total_alunos ? ((data.adimplentes / data.total_alunos) * 100).toFixed(1) : '0';
    document.getElementById('inad-sf-adim-pct').textContent = pctAdim.replace('.', ',') + '% do total';
    document.getElementById('inad-sf-inadim-pct').textContent = pct.toFixed(1).replace('.', ',') + '% do total';
}

function _renderSfTrendChart(series) {
    const el = document.getElementById('inad-sf-trend-chart');
    const emptyMsg = document.getElementById('inad-sf-trend-empty');
    if (!el) return;

    if (series.length < 2) {
        if (emptyMsg) emptyMsg.classList.remove('hidden');
        el.style.display = 'none';
        return;
    }
    if (emptyMsg) emptyMsg.classList.add('hidden');
    el.style.display = 'block';

    const chart = eInit('inad-sf-trend-chart');
    if (!chart) return;

    const labels = series.map(s => s.date.split(' ')[0]);
    const pcts = series.map(s => s.pct_inadimplencia || 0);
    const inadims = series.map(s => s.inadimplentes || 0);
    const t = eThemeColors();

    chart.setOption({
        backgroundColor: 'transparent',
        grid: eBaseGrid(),
        tooltip: eTooltip(),
        legend: { bottom: 0, textStyle: { color: t.textColor, fontSize: 11 } },
        xAxis: { ...eCategoryAxis(labels), axisLabel: { ...eCategoryAxis(labels).axisLabel, rotate: 30 } },
        yAxis: [
            { ...eValueAxis({ formatter: v => v + '%' }), position: 'left', axisLabel: { color: '#f59e0b', fontSize: 10 } },
            { ...eValueAxis(), position: 'right', splitLine: { show: false }, axisLabel: { color: '#ef4444', fontSize: 10 } },
        ],
        series: [
            { name: '% Inadimplência', type: 'line', yAxisIndex: 0, data: pcts, smooth: 0.3, symbol: 'circle', symbolSize: 6, lineStyle: { width: 2.5, color: '#f59e0b' }, itemStyle: { color: '#f59e0b' }, areaStyle: { color: 'rgba(245,158,11,0.1)' } },
            { name: 'Inadimplentes (abs)', type: 'line', yAxisIndex: 1, data: inadims, smooth: 0.3, symbol: 'circle', symbolSize: 4, lineStyle: { width: 1.5, color: '#ef4444', type: 'dashed' }, itemStyle: { color: '#ef4444' } },
        ],
        animationDuration: 600,
    }, true);
}

function _renderSfPolos(inadByPolo, totalAlunos) {
    const el = document.getElementById('inad-sf-polos');
    if (!el) return;
    const entries = Object.entries(inadByPolo).sort((a, b) => b[1] - a[1]).slice(0, 10);
    if (!entries.length) { el.textContent = '—'; return; }
    const maxVal = entries[0][1];

    el.innerHTML = entries.map(([polo, count]) => {
        const w = Math.round((count / maxVal) * 100);
        return `<div class="flex items-center gap-2">
            <span class="text-[11px] text-gray-400 truncate w-32 flex-shrink-0" title="${esc(polo)}">${esc(polo.replace(/^\d+\s*[-–]\s*/, ''))}</span>
            <div class="flex-1 h-2 rounded-full bg-gray-700/50">
                <div class="h-2 rounded-full bg-gradient-to-r from-amber-500 to-rose-500 transition-all" style="width:${w}%"></div>
            </div>
            <span class="text-[11px] font-mono text-[var(--text-primary)] font-semibold w-10 text-right">${count.toLocaleString('pt-BR')}</span>
        </div>`;
    }).join('');
}

function _renderSfHistoryTable(series) {
    const tbody = document.getElementById('inad-sf-history-tbody');
    if (!tbody) return;
    const container = tbody.closest('table')?.parentElement;
    if (!container) return;
    container.id = container.id || 'inad-sf-table-container';

    if (!series.length) {
        container.innerHTML = '<p class="py-4 text-center text-gray-500 text-sm">Nenhum snapshot encontrado.</p>';
        return;
    }

    const reversed = series.slice().reverse();
    const rows = reversed.map((s, idx) => {
        const realIdx = series.length - 1 - idx;
        const prev = realIdx > 0 ? series[realIdx - 1] : null;
        return {
            date: s.date,
            total: s.total_alunos || 0,
            adim: s.adimplentes || 0,
            inadim: s.inadimplentes || 0,
            pct: s.pct_inadimplencia || 0,
            _prev: prev,
        };
    });

    renderSortableTable(container.id, {
        columns: [
            { key: 'date', label: 'Data' },
            { key: 'total', label: 'Total', align: 'right', render: v => `<span class="font-semibold">${v.toLocaleString('pt-BR')}</span>` },
            { key: 'adim', label: 'Adimplentes', align: 'right', render: v => `<span class="text-emerald-500">${v.toLocaleString('pt-BR')}</span>` },
            { key: 'inadim', label: 'Inadimplentes', align: 'right', render: v => `<span class="text-amber-500">${v.toLocaleString('pt-BR')}</span>` },
            { key: 'pct', label: '% Inadim.', align: 'right', render: v => v.toFixed(1).replace('.', ',') + '%' },
            { key: 'inadim', label: 'Variação', align: 'right', render: (v, r) => r._prev ? _varBadge(r.inadim, r._prev.inadimplentes || 0) : '—' },
        ],
        rows,
        pageSize: 15,
    });
}
