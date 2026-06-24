// ---------------------------------------------------------------------------
// Rematrícula — conversão (matriculados / novo ciclo) + iframe fila SIAA
// ---------------------------------------------------------------------------
const _REMAT_CICLO_KEY = 'dash_ciclo_v2';

let _rematChart = null;
let _rematDailyChart = null;
let _rematGranularity = 'month';
let _rematDrillMonth = null;
let _rematRawPeriods = [];
let _rematFetchGen = 0;
let _rematDailyFetchGen = 0;

function loadRematricula() {
    populateRematCicloFilter().then(() => loadRematCharts());
}

function loadRematCharts() {
    _rematGranularity = 'month';
    _rematDrillMonth = null;
    document.getElementById('remat-tl-drillup')?.classList.add('hidden');
    loadRematTimeline();
    loadRematDailyTimeline();
}

function rematClearDates() {
    const fromEl = document.getElementById('remat-from');
    const toEl = document.getElementById('remat-to');
    if (fromEl) fromEl.value = '';
    if (toEl) toEl.value = '';
    loadRematCharts();
}

async function populateRematCicloFilter() {
    const sel = document.getElementById('remat-ciclo');
    if (!sel) return;
    try {
        const res = await api('/api/dashboard/ciclos-distinct');
        const list = await res.json();
        const arr = Array.isArray(list) ? list : [];
        sel.innerHTML =
            '<option value="">Todos os ciclos</option>' +
            arr
                .map((c) => {
                    const n = c.nome;
                    const tot = c.total != null ? ` (${Number(c.total).toLocaleString('pt-BR')})` : '';
                    return `<option value="${esc(n)}">${esc(n)}${tot}</option>`;
                })
                .join('');
        const names = arr.map((c) => c.nome);
        const saved = localStorage.getItem(_REMAT_CICLO_KEY);
        if (saved && names.includes(saved)) {
            sel.value = saved;
        } else if (arr[0]?.nome) {
            sel.value = arr[0].nome;
        }
    } catch (e) {
        console.error('remat ciclos:', e);
        sel.innerHTML = '<option value="">Erro ao carregar ciclos</option>';
    }
}

function _rematFilters() {
    const ciclo = document.getElementById('remat-ciclo')?.value || '';
    const nivel = document.getElementById('remat-nivel')?.value || '';
    const from = document.getElementById('remat-from')?.value || '';
    const to = document.getElementById('remat-to')?.value || '';
    if (ciclo) localStorage.setItem(_REMAT_CICLO_KEY, ciclo);
    else localStorage.removeItem(_REMAT_CICLO_KEY);
    const isPosOnly = nivel === 'Pós-Graduação';
    const labelText = isPosOnly ? 'Veteranos' : 'Rematrículas concluídas';
    return { ciclo, nivel, from, to, labelText, isPosOnly };
}

function _rematFormatLabel(period, gran) {
    if (gran === 'month') {
        const [y, m] = period.split('-');
        const months = ['jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez'];
        return months[parseInt(m, 10) - 1] + ' ' + y;
    }
    const [, m, d] = period.split('-');
    return parseInt(d, 10) + '/' + parseInt(m, 10);
}

function _rematUpdateHero(remat, ciclo, range) {
    const total = remat.reduce((a, b) => a + (Number(b) || 0), 0);
    const daysWithData = remat.filter((v) => Number(v) > 0).length;
    const peak = remat.length ? Math.max(...remat.map((v) => Number(v) || 0)) : 0;
    const avg = daysWithData ? Math.round(total / daysWithData) : 0;

    const setText = (id, val) => {
        const el = document.getElementById(id);
        if (el) el.textContent = val;
    };
    setText('remat-hero-total', total.toLocaleString('pt-BR'));
    setText('remat-hero-avg', avg.toLocaleString('pt-BR') + '/dia');
    setText('remat-hero-peak', peak.toLocaleString('pt-BR'));

    const sub = document.getElementById('remat-hero-sub');
    if (sub) {
        const parts = [];
        if (ciclo) parts.push('ciclo ' + ciclo);
        if (range?.from && range?.to) parts.push(range.from + ' → ' + range.to);
        sub.textContent = parts.length ? parts.join(' · ') : 'tipo rematrícula no relatório de matriculados';
    }
}

function _rematChartOpts(onClick) {
    return {
        responsive: true,
        maintainAspectRatio: false,
        animation: { duration: 400 },
        interaction: { mode: 'index', intersect: false },
        onClick,
        plugins: {
            legend: { display: false },
            tooltip: {
                backgroundColor: 'rgba(15,23,42,0.95)',
                borderColor: 'rgba(100,116,139,0.3)',
                borderWidth: 1,
                titleFont: { family: 'Inter', size: 11 },
                bodyFont: { family: 'JetBrains Mono', size: 12 },
                callbacks: {
                    label: (c) => c.dataset.label + ': ' + c.parsed.y.toLocaleString('pt-BR'),
                },
            },
        },
        scales: {
            x: {
                grid: { color: 'rgba(100,116,139,0.08)' },
                ticks: {
                    color: '#64748b',
                    font: { size: 10, family: 'Inter' },
                    maxRotation: 0,
                    autoSkip: true,
                    maxTicksLimit: 18,
                },
            },
            y: {
                beginAtZero: true,
                grid: { color: 'rgba(100,116,139,0.08)' },
                ticks: {
                    color: '#64748b',
                    font: { size: 10, family: 'JetBrains Mono' },
                    callback: (v) => (v >= 1000 ? (v / 1000).toFixed(v % 1000 ? 1 : 0) + 'k' : v),
                },
            },
        },
    };
}

function _rematRenderLineChart(canvasId, chartRef, labels, data, labelText, opts) {
    const ctx = document.getElementById(canvasId);
    if (!ctx || typeof Chart === 'undefined') return chartRef;
    if (chartRef) {
        chartRef.destroy();
        chartRef = null;
    }
    return new Chart(ctx, {
        type: 'line',
        data: {
            labels,
            datasets: [{
                label: labelText,
                data,
                borderColor: '#10b981',
                backgroundColor: 'rgba(16,185,129,0.08)',
                borderWidth: 2,
                pointRadius: labels.length > 60 ? 0 : 2,
                pointHoverRadius: 5,
                pointBackgroundColor: '#10b981',
                fill: true,
                tension: 0.25,
            }],
        },
        options: opts,
    });
}

function _rematTimelineParams(extraFrom, extraTo) {
    const { ciclo, nivel, from, to, labelText } = _rematFilters();
    const params = new URLSearchParams({ granularity: _rematGranularity });
    if (nivel) params.set('nivel', nivel);
    if (ciclo) params.set('ciclo', ciclo);
    const df = extraFrom || from;
    const dt = extraTo || to;
    if (df) params.set('from', df);
    if (dt) params.set('to', dt);
    return { params, ciclo, labelText };
}

async function loadRematTimeline(from, to) {
    const gen = ++_rematFetchGen;
    const loading = document.getElementById('remat-tl-loading');
    if (loading) loading.classList.remove('hidden');

    const { params, ciclo, labelText } = _rematTimelineParams(from, to);

    try {
        const res = await api('/api/dashboard/timeline?' + params.toString());
        const d = await res.json();
        if (gen !== _rematFetchGen) return;
        if (d.error) {
            console.warn('[Remat timeline]', d.error);
            return;
        }

        const periods = d.periods || [];
        const remat = (d.series || {}).rematricula || [];
        const labels = periods.map((p) => _rematFormatLabel(p, _rematGranularity));
        _rematRawPeriods = periods;

        _rematChart = _rematRenderLineChart(
            'remat-chart-timeline',
            _rematChart,
            labels,
            remat,
            labelText,
            _rematChartOpts((evt, elements) => {
                if (elements.length && _rematGranularity === 'month') {
                    rematTimelineDrillDown(elements[0].index);
                }
            })
        );

        const badge = document.getElementById('remat-tl-ciclo-badge');
        if (badge) {
            if (ciclo) {
                badge.textContent = 'Ciclo ' + ciclo;
                badge.classList.remove('hidden');
            } else {
                badge.classList.add('hidden');
            }
        }

        const rangeTxt = d.range ? d.range.from + ' → ' + d.range.to : '';
        const periodEl = document.getElementById('remat-tl-period-label');
        if (periodEl) {
            periodEl.textContent =
                _rematGranularity === 'day' && _rematDrillMonth
                    ? _rematDrillMonth
                    : [ciclo ? 'Ciclo ' + ciclo : '', rangeTxt].filter(Boolean).join(' · ');
        }

        document.getElementById('remat-tl-drillup')?.classList.toggle('hidden', _rematGranularity !== 'day');
    } catch (e) {
        console.error('Remat timeline error:', e);
    } finally {
        if (gen === _rematFetchGen && loading) loading.classList.add('hidden');
    }
}

async function loadRematDailyTimeline() {
    const gen = ++_rematDailyFetchGen;
    const loading = document.getElementById('remat-daily-loading');
    if (loading) loading.classList.remove('hidden');

    const { ciclo, nivel, from, to, labelText } = _rematFilters();
    const params = new URLSearchParams({ granularity: 'day' });
    if (nivel) params.set('nivel', nivel);
    if (ciclo) params.set('ciclo', ciclo);
    if (from) params.set('from', from);
    if (to) params.set('to', to);

    try {
        const res = await api('/api/dashboard/timeline?' + params.toString());
        const d = await res.json();
        if (gen !== _rematDailyFetchGen) return;
        if (d.error) {
            console.warn('[Remat daily]', d.error);
            return;
        }

        const periods = d.periods || [];
        const remat = (d.series || {}).rematricula || [];
        const labels = periods.map((p) => _rematFormatLabel(p, 'day'));

        _rematUpdateHero(remat, ciclo, d.range);

        _rematDailyChart = _rematRenderLineChart(
            'remat-chart-daily',
            _rematDailyChart,
            labels,
            remat,
            labelText,
            _rematChartOpts(null)
        );

        const rangeTxt = d.range ? d.range.from + ' → ' + d.range.to : '';
        const periodEl = document.getElementById('remat-daily-period-label');
        if (periodEl) {
            periodEl.textContent = [ciclo ? 'Ciclo ' + ciclo : '', rangeTxt].filter(Boolean).join(' · ');
        }
    } catch (e) {
        console.error('Remat daily error:', e);
    } finally {
        if (gen === _rematDailyFetchGen && loading) loading.classList.add('hidden');
    }
}

function rematTimelineDrillDown(index) {
    const period = _rematRawPeriods[index];
    if (!period || _rematGranularity !== 'month') return;
    const [y, m] = period.split('-');
    const from = `${y}-${m}-01`;
    const lastDay = new Date(parseInt(y, 10), parseInt(m, 10), 0).getDate();
    const to = `${y}-${m}-${String(lastDay).padStart(2, '0')}`;
    _rematGranularity = 'day';
    _rematDrillMonth = period;
    loadRematTimeline(from, to);
}

function rematTimelineDrillUp() {
    _rematGranularity = 'month';
    _rematDrillMonth = null;
    loadRematTimeline();
}

function rematReloadIframe() {
    const iframe = document.getElementById('remat-iframe');
    if (!iframe) return;
    const url = iframe.getAttribute('src');
    iframe.setAttribute('src', 'about:blank');
    setTimeout(() => iframe.setAttribute('src', url), 30);
}
