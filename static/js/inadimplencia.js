// ===========================================================================
// INADIMPLÊNCIA - Histórico e Análise
// ===========================================================================
let _inadHistoryChart = null;
let _inadBreakdownChart = null;

const _inadColors = [
    '#f59e0b', '#6366f1', '#10b981', '#ef4444', '#8b5cf6',
    '#ec4899', '#14b8a6', '#f97316', '#06b6d4', '#84cc16',
];

async function loadInadimplencia() {
    try {
        const res = await api('/api/inadimplencia/historico');
        const d = await res.json();
        const series = d.series || [];

        _renderInadKPIs(series);
        _renderInadHistoryChart(series);
        _renderInadBreakdownChart(series);
        _renderInadTable(series);
    } catch(e) {
        console.error('Erro ao carregar inadimplência:', e);
    }
}

function _renderInadKPIs(series) {
    const totalEl = document.getElementById('inad-total');
    const snapEl = document.getElementById('inad-snap-count');
    const gradEl = document.getElementById('inad-grad');
    const posEl = document.getElementById('inad-pos');
    const subEl = document.getElementById('inad-total-sub');

    snapEl.textContent = series.length.toLocaleString('pt-BR');

    if (series.length === 0) {
        totalEl.textContent = '—';
        gradEl.textContent = '—';
        posEl.textContent = '—';
        subEl.textContent = 'Nenhum snapshot';
        return;
    }

    const last = series[series.length - 1];
    totalEl.textContent = last.total.toLocaleString('pt-BR');
    subEl.textContent = last.date;
    gradEl.textContent = (last.by_nivel['Graduação'] || 0).toLocaleString('pt-BR');
    posEl.textContent = (last.by_nivel['Pós-Graduação'] || 0).toLocaleString('pt-BR');
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

    const labels = series.map(s => s.date);
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

    const datasets = keys.map((key, i) => {
        const data = series.map(s => {
            const group = s[groupKey] || {};
            if (filterNivel && groupBy !== 'nivel') {
                return group[key] || 0;
            }
            return group[key] || 0;
        });
        return {
            label: key || 'N/I',
            data,
            backgroundColor: _inadColors[i % _inadColors.length] + '99',
            borderColor: _inadColors[i % _inadColors.length],
            borderWidth: 1,
        };
    });

    _inadHistoryChart = new Chart(canvas, {
        type: series.length === 1 ? 'bar' : 'bar',
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
                x: {
                    stacked: true,
                    ticks: { color: '#64748b', maxRotation: 45 },
                    grid: { color: '#1e293b' }
                },
                y: {
                    stacked: true,
                    ticks: { color: '#64748b' },
                    grid: { color: '#1e293b' },
                    beginAtZero: true,
                }
            }
        }
    });
}

function _renderInadBreakdownChart(series) {
    const canvas = document.getElementById('inad-breakdown-chart');
    const titleEl = document.getElementById('inad-breakdown-title');
    const groupBy = document.getElementById('inad-group-by').value;

    if (_inadBreakdownChart) { _inadBreakdownChart.destroy(); _inadBreakdownChart = null; }

    const groupLabels = { nivel: 'Distribuição por Nível', tipo: 'Distribuição por Tipo de Aluno', turma: 'Distribuição por Turma / Série' };
    titleEl.textContent = groupLabels[groupBy] || 'Distribuição';

    if (series.length === 0) return;

    const last = series[series.length - 1];
    const groupKey = `by_${groupBy}`;
    const group = last[groupKey] || {};
    const sortedEntries = Object.entries(group).sort((a, b) => b[1] - a[1]);
    const labels = sortedEntries.map(e => e[0] || 'N/I');
    const values = sortedEntries.map(e => e[1]);
    const total = values.reduce((a, b) => a + b, 0);

    _inadBreakdownChart = new Chart(canvas, {
        type: 'doughnut',
        data: {
            labels,
            datasets: [{
                data: values,
                backgroundColor: labels.map((_, i) => _inadColors[i % _inadColors.length]),
                borderWidth: 0,
            }]
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

function _renderInadTable(series) {
    const tbody = document.getElementById('inad-history-tbody');
    if (series.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" class="py-4 text-center text-slate-500">Nenhum snapshot de inadimplentes encontrado. Faça upload na aba Atualização.</td></tr>';
        return;
    }

    tbody.innerHTML = series.slice().reverse().map(s => {
        const grad = s.by_nivel['Graduação'] || 0;
        const pos = s.by_nivel['Pós-Graduação'] || 0;
        const nivelLabel = s.snap_nivel || 'N/I';
        return `<tr class="border-b border-slate-800/40 hover:bg-slate-800/30 transition">
            <td class="py-2 pr-2 text-xs text-slate-400">${s.date}</td>
            <td class="py-2 pr-2 text-xs"><span class="px-2 py-0.5 rounded-full text-[10px] font-bold ${nivelLabel.includes('Pós') ? 'bg-purple-500/15 text-purple-400' : nivelLabel === 'N/I' ? 'bg-slate-500/15 text-slate-400' : 'bg-sky-500/15 text-sky-400'}">${nivelLabel}</span></td>
            <td class="py-2 pr-2 text-right font-bold text-white">${s.total.toLocaleString('pt-BR')}</td>
            <td class="py-2 pr-2 text-right text-sky-400">${grad.toLocaleString('pt-BR')}</td>
            <td class="py-2 text-right text-purple-400">${pos.toLocaleString('pt-BR')}</td>
        </tr>`;
    }).join('');
}
