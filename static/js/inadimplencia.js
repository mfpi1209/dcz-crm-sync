// ===========================================================================
// INADIMPLÊNCIA - Histórico, Comparação e Análise
// ===========================================================================
let _inadTrendChart = null;
let _inadHistoryChart = null;
let _inadBreakdownChart = null;
let _inadSfTrendChart = null;

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
    if (previous === 0 && current === 0) return '<span class="text-slate-500">—</span>';
    if (previous === 0) return '<span class="text-red-400">+' + current.toLocaleString('pt-BR') + ' novo(s)</span>';
    const diff = current - previous;
    const pct = ((diff / previous) * 100).toFixed(1);
    if (diff > 0) return `<span class="text-red-400">+${diff.toLocaleString('pt-BR')} (+${pct}%)</span>`;
    if (diff < 0) return `<span class="text-emerald-400">${diff.toLocaleString('pt-BR')} (${pct}%)</span>`;
    return '<span class="text-slate-400">Sem variação</span>';
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
        totalVar.innerHTML = '<span class="text-slate-500">Nenhum snapshot</span>';
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
        totalVar.innerHTML = '<span class="text-slate-500">' + last.date + '</span>';
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
    const canvas = document.getElementById('inad-trend-chart');
    const emptyMsg = document.getElementById('inad-trend-empty');

    if (_inadTrendChart) { _inadTrendChart.destroy(); _inadTrendChart = null; }

    if (series.length < 2) {
        emptyMsg.classList.remove('hidden');
        canvas.style.display = 'none';
        return;
    }
    emptyMsg.classList.add('hidden');
    canvas.style.display = 'block';

    const labels = series.map(s => s.date.split(' ')[0]);
    const totals = series.map(s => s.total);
    const grads = series.map(s => s.by_nivel['Graduação'] || 0);
    const poss = series.map(s => s.by_nivel['Pós-Graduação'] || 0);

    _inadTrendChart = new Chart(canvas, {
        type: 'line',
        data: {
            labels,
            datasets: [
                {
                    label: 'Total',
                    data: totals,
                    borderColor: '#f59e0b',
                    backgroundColor: 'rgba(245,158,11,0.1)',
                    fill: true,
                    tension: 0.3,
                    borderWidth: 2.5,
                    pointRadius: 4,
                    pointBackgroundColor: '#f59e0b',
                },
                {
                    label: 'Graduação',
                    data: grads,
                    borderColor: '#38bdf8',
                    borderWidth: 1.5,
                    borderDash: [5, 3],
                    tension: 0.3,
                    pointRadius: 3,
                    pointBackgroundColor: '#38bdf8',
                },
                {
                    label: 'Pós-Graduação',
                    data: poss,
                    borderColor: '#a78bfa',
                    borderWidth: 1.5,
                    borderDash: [5, 3],
                    tension: 0.3,
                    pointRadius: 3,
                    pointBackgroundColor: '#a78bfa',
                },
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: { position: 'bottom', labels: { color: '#94a3b8', padding: 12, usePointStyle: true } },
                tooltip: {
                    callbacks: {
                        afterBody: (items) => {
                            const idx = items[0].dataIndex;
                            if (idx === 0) return '';
                            const prev = totals[idx - 1];
                            const curr = totals[idx];
                            const diff = curr - prev;
                            const pct = prev > 0 ? ((diff / prev) * 100).toFixed(1) : '—';
                            const sign = diff >= 0 ? '+' : '';
                            return `Variação: ${sign}${diff.toLocaleString('pt-BR')} (${sign}${pct}%)`;
                        }
                    }
                }
            },
            scales: {
                x: { ticks: { color: '#64748b', maxRotation: 45 }, grid: { color: '#1e293b' } },
                y: { ticks: { color: '#64748b' }, grid: { color: '#1e293b' }, beginAtZero: true }
            }
        }
    });
}

function _renderInadHistoryChart(series) {
    const canvas = document.getElementById('inad-history-chart');
    const emptyMsg = document.getElementById('inad-chart-empty');
    const filterNivel = document.getElementById('inad-filter-nivel').value;
    const groupBy = document.getElementById('inad-group-by').value;

    if (_inadHistoryChart) { _inadHistoryChart.destroy(); _inadHistoryChart = null; }

    if (series.length < 1) {
        emptyMsg.classList.remove('hidden');
        canvas.style.display = 'none';
        return;
    }
    emptyMsg.classList.add('hidden');
    canvas.style.display = 'block';

    const labels = series.map(s => s.date.split(' ')[0]);
    const groupKey = `by_${groupBy}`;

    const allKeys = new Set();
    series.forEach(s => {
        const group = s[groupKey] || {};
        Object.keys(group).forEach(k => allKeys.add(k));
    });

    let keys = [...allKeys].sort();
    if (filterNivel && groupBy === 'nivel') {
        keys = keys.filter(k => k === filterNivel);
    }

    const datasets = keys.map((key, i) => ({
        label: key || 'N/I',
        data: series.map(s => (s[groupKey] || {})[key] || 0),
        backgroundColor: _inadColors[i % _inadColors.length] + '99',
        borderColor: _inadColors[i % _inadColors.length],
        borderWidth: 1,
    }));

    _inadHistoryChart = new Chart(canvas, {
        type: 'bar',
        data: { labels, datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { position: 'bottom', labels: { color: '#94a3b8', padding: 12, usePointStyle: true } },
                tooltip: {
                    callbacks: {
                        label: (ctx) => ` ${ctx.dataset.label}: ${ctx.parsed.y.toLocaleString('pt-BR')}`,
                    }
                }
            },
            scales: {
                x: { stacked: true, ticks: { color: '#64748b', maxRotation: 45 }, grid: { color: '#1e293b' } },
                y: { stacked: true, ticks: { color: '#64748b' }, grid: { color: '#1e293b' }, beginAtZero: true }
            }
        }
    });
}

function _renderInadBreakdownChart(series) {
    const canvas = document.getElementById('inad-breakdown-chart');
    const titleEl = document.getElementById('inad-breakdown-title');
    const groupBy = document.getElementById('inad-group-by').value;

    if (_inadBreakdownChart) { _inadBreakdownChart.destroy(); _inadBreakdownChart = null; }

    const groupLabels = { nivel: 'Distribuição por Nível', tipo: 'Distribuição por Tipo', turma: 'Distribuição por Turma' };
    titleEl.textContent = groupLabels[groupBy] || 'Distribuição';

    if (series.length === 0) return;

    const last = series[series.length - 1];
    const groupKey = `by_${groupBy}`;
    const group = last[groupKey] || {};
    const sorted = Object.entries(group).sort((a, b) => b[1] - a[1]);
    const lbls = sorted.map(e => e[0] || 'N/I');
    const vals = sorted.map(e => e[1]);
    const total = vals.reduce((a, b) => a + b, 0);

    _inadBreakdownChart = new Chart(canvas, {
        type: 'doughnut',
        data: {
            labels: lbls,
            datasets: [{ data: vals, backgroundColor: lbls.map((_, i) => _inadColors[i % _inadColors.length]), borderWidth: 0 }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            cutout: '60%',
            plugins: {
                legend: { position: 'right', labels: { color: '#94a3b8', padding: 10, usePointStyle: true, pointStyleWidth: 8 } },
                tooltip: {
                    callbacks: {
                        label: (ctx) => {
                            const pct = total > 0 ? (ctx.parsed / total * 100).toFixed(1) : '0';
                            return ` ${ctx.label}: ${ctx.parsed.toLocaleString('pt-BR')} (${pct}%)`;
                        }
                    }
                }
            }
        }
    });
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
        <div class="glass-card p-4 bg-slate-800/30 rounded-xl">
            <p class="text-[10px] font-bold text-slate-500 uppercase tracking-wider mb-3">Primeiro: ${first.date.split(' ')[0]}</p>
            <p class="text-xl font-bold text-white">${first.total.toLocaleString('pt-BR')}</p>
        </div>
        <div class="glass-card p-4 bg-slate-800/30 rounded-xl">
            <p class="text-[10px] font-bold text-slate-500 uppercase tracking-wider mb-3">Último: ${last.date.split(' ')[0]}</p>
            <p class="text-xl font-bold text-white">${last.total.toLocaleString('pt-BR')}</p>
        </div>
        <div class="glass-card p-4 bg-slate-800/30 rounded-xl">
            <p class="text-[10px] font-bold text-slate-500 uppercase tracking-wider mb-3">Variação Total</p>
            <p class="text-xl font-bold">${_varBadge(last.total, first.total)}</p>
        </div>`;

    if (keys.length > 0 && keys.length <= 10) {
        html += `<div class="md:col-span-3 mt-2">
            <p class="text-xs font-semibold text-slate-400 mb-2">Detalhamento por ${groupLabels[groupBy] || groupBy}</p>
            <div class="grid grid-cols-2 md:grid-cols-4 gap-3">`;
        keys.forEach(k => {
            const v1 = (first[groupKey] || {})[k] || 0;
            const v2 = (last[groupKey] || {})[k] || 0;
            html += `<div class="p-3 rounded-lg bg-slate-800/40">
                <p class="text-[10px] text-slate-500 font-medium truncate" title="${k}">${k || 'N/I'}</p>
                <p class="text-sm font-bold text-white mt-1">${v1.toLocaleString('pt-BR')} → ${v2.toLocaleString('pt-BR')}</p>
                <p class="text-xs mt-0.5">${_varBadge(v2, v1)}</p>
            </div>`;
        });
        html += `</div></div>`;
    }

    body.innerHTML = html;
}

function _renderInadTable(series) {
    const tbody = document.getElementById('inad-history-tbody');
    if (series.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" class="py-4 text-center text-slate-500">Nenhum snapshot encontrado. Faça upload na aba Distribuição.</td></tr>';
        return;
    }

    const reversed = series.slice().reverse();
    tbody.innerHTML = reversed.map((s, idx) => {
        const grad = s.by_nivel['Graduação'] || 0;
        const pos = s.by_nivel['Pós-Graduação'] || 0;
        const nivelLabel = s.snap_nivel || 'Todos';
        const realIdx = series.length - 1 - idx;
        const prev = realIdx > 0 ? series[realIdx - 1] : null;
        const varHtml = prev ? _varBadge(s.total, prev.total) : '<span class="text-slate-600">—</span>';

        return `<tr class="border-b border-slate-800/40 hover:bg-slate-800/30 transition">
            <td class="py-2 pr-2 text-xs text-slate-400">${s.date}</td>
            <td class="py-2 pr-2 text-xs"><span class="px-2 py-0.5 rounded-full text-[10px] font-bold ${nivelLabel.includes('Pós') ? 'bg-purple-500/15 text-purple-400' : nivelLabel === 'Todos' ? 'bg-slate-500/15 text-slate-400' : 'bg-sky-500/15 text-sky-400'}">${nivelLabel}</span></td>
            <td class="py-2 pr-2 text-right font-bold text-white">${s.total.toLocaleString('pt-BR')}</td>
            <td class="py-2 pr-2 text-right text-sky-400">${grad.toLocaleString('pt-BR')}</td>
            <td class="py-2 pr-2 text-right text-purple-400">${pos.toLocaleString('pt-BR')}</td>
            <td class="py-2 text-right text-xs">${varHtml}</td>
        </tr>`;
    }).join('');
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
    const canvas = document.getElementById('inad-sf-trend-chart');
    const emptyMsg = document.getElementById('inad-sf-trend-empty');
    if (!canvas) return;

    if (_inadSfTrendChart) { _inadSfTrendChart.destroy(); _inadSfTrendChart = null; }

    if (series.length < 2) {
        if (emptyMsg) emptyMsg.classList.remove('hidden');
        canvas.style.display = 'none';
        return;
    }
    if (emptyMsg) emptyMsg.classList.add('hidden');
    canvas.style.display = 'block';

    const labels = series.map(s => s.date.split(' ')[0]);
    const pcts = series.map(s => s.pct_inadimplencia || 0);
    const inadims = series.map(s => s.inadimplentes || 0);

    _inadSfTrendChart = new Chart(canvas, {
        type: 'line',
        data: {
            labels,
            datasets: [
                {
                    label: '% Inadimplência',
                    data: pcts,
                    borderColor: '#f59e0b',
                    backgroundColor: 'rgba(245,158,11,0.1)',
                    fill: true,
                    tension: 0.3,
                    borderWidth: 2.5,
                    pointRadius: 4,
                    pointBackgroundColor: '#f59e0b',
                    yAxisID: 'y',
                },
                {
                    label: 'Inadimplentes (abs)',
                    data: inadims,
                    borderColor: '#ef4444',
                    borderWidth: 1.5,
                    borderDash: [5, 3],
                    tension: 0.3,
                    pointRadius: 3,
                    pointBackgroundColor: '#ef4444',
                    yAxisID: 'y1',
                },
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: { position: 'bottom', labels: { color: '#94a3b8', padding: 12, usePointStyle: true } },
                tooltip: {
                    callbacks: {
                        label: c => c.dataset.yAxisID === 'y'
                            ? `${c.dataset.label}: ${c.parsed.y.toFixed(1)}%`
                            : `${c.dataset.label}: ${c.parsed.y.toLocaleString('pt-BR')}`,
                    },
                },
            },
            scales: {
                x: { ticks: { color: '#64748b', maxRotation: 45 }, grid: { color: '#1e293b' } },
                y: {
                    type: 'linear', position: 'left',
                    ticks: { color: '#f59e0b', callback: v => v + '%' },
                    grid: { color: '#1e293b' },
                },
                y1: {
                    type: 'linear', position: 'right',
                    ticks: { color: '#ef4444' },
                    grid: { drawOnChartArea: false },
                },
            },
        },
    });
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
            <span class="text-[11px] text-slate-400 truncate w-32 flex-shrink-0" title="${esc(polo)}">${esc(polo.replace(/^\d+\s*[-–]\s*/, ''))}</span>
            <div class="flex-1 h-2 rounded-full bg-slate-700/50">
                <div class="h-2 rounded-full bg-gradient-to-r from-amber-500 to-rose-500 transition-all" style="width:${w}%"></div>
            </div>
            <span class="text-[11px] font-mono text-white font-semibold w-10 text-right">${count.toLocaleString('pt-BR')}</span>
        </div>`;
    }).join('');
}

function _renderSfHistoryTable(series) {
    const tbody = document.getElementById('inad-sf-history-tbody');
    if (!tbody) return;
    if (!series.length) {
        tbody.innerHTML = '<tr><td colspan="6" class="py-4 text-center text-slate-500">Nenhum snapshot encontrado.</td></tr>';
        return;
    }

    const reversed = series.slice().reverse();
    tbody.innerHTML = reversed.map((s, idx) => {
        const realIdx = series.length - 1 - idx;
        const prev = realIdx > 0 ? series[realIdx - 1] : null;
        const varHtml = prev
            ? _varBadge(s.inadimplentes, prev.inadimplentes)
            : '<span class="text-slate-600">—</span>';

        return `<tr class="border-b border-slate-800/40 hover:bg-slate-800/30 transition">
            <td class="py-2 pr-2 text-xs text-slate-400">${s.date}</td>
            <td class="py-2 pr-2 text-right font-bold text-white">${(s.total_alunos || 0).toLocaleString('pt-BR')}</td>
            <td class="py-2 pr-2 text-right text-emerald-400">${(s.adimplentes || 0).toLocaleString('pt-BR')}</td>
            <td class="py-2 pr-2 text-right text-amber-400">${(s.inadimplentes || 0).toLocaleString('pt-BR')}</td>
            <td class="py-2 pr-2 text-right text-white">${(s.pct_inadimplencia || 0).toFixed(1).replace('.', ',')}%</td>
            <td class="py-2 text-right text-xs">${varHtml}</td>
        </tr>`;
    }).join('');
}
