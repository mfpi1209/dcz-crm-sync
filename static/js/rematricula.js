// ---------------------------------------------------------------------------
// Rematrícula — evolução temporal (relatório matriculados) + iframe SIAA
// ---------------------------------------------------------------------------
const _REMAT_CICLO_KEY = 'dash_ciclo_v2';

let _rematChart = null;
let _rematDailyChart = null;
let _rematGranularity = 'month';
let _rematDrillMonth = null;
let _rematRawPeriods = [];
let _rematFetchGen = 0;
let _rematDailyFetchGen = 0;
let _rematDailyMap = {};
let _rematDailyRange = { from: null, to: null };
let _rematCalMonth = null; // { year, month } 0-based
let _rematCalSelected = null;

function loadRematricula() {
    populateRematCicloFilter().then(() => loadRematCharts());
}

function loadRematCharts() {
    _rematGranularity = 'month';
    _rematDrillMonth = null;
    _rematCalMonth = null;
    _rematCalSelected = null;
    document.getElementById('remat-tl-drillup')?.classList.add('hidden');
    loadRematTimeline();
    loadRematDailyTimeline();
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
    if (ciclo) localStorage.setItem(_REMAT_CICLO_KEY, ciclo);
    else localStorage.removeItem(_REMAT_CICLO_KEY);
    const isPosOnly = nivel === 'Pós-Graduação';
    const labelText = isPosOnly ? 'Veteranos' : 'Rematrículas';
    return { ciclo, nivel, labelText, isPosOnly };
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

async function loadRematTimeline(from, to) {
    const gen = ++_rematFetchGen;
    const loading = document.getElementById('remat-tl-loading');
    if (loading) loading.classList.remove('hidden');

    const { ciclo, nivel, labelText } = _rematFilters();
    const params = new URLSearchParams({ granularity: _rematGranularity });
    if (nivel) params.set('nivel', nivel);
    if (ciclo) params.set('ciclo', ciclo);
    if (from) params.set('from', from);
    if (to) params.set('to', to);

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

        const labelEl = document.getElementById('remat-tl-label');
        if (labelEl) labelEl.textContent = labelText;

        const total = remat.reduce((a, b) => a + (Number(b) || 0), 0);
        const totalEl = document.getElementById('remat-tl-total');
        if (totalEl) totalEl.textContent = total.toLocaleString('pt-BR');

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

    const { ciclo, nivel, labelText } = _rematFilters();
    const params = new URLSearchParams({ granularity: 'day' });
    if (nivel) params.set('nivel', nivel);
    if (ciclo) params.set('ciclo', ciclo);

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
        _rematDailyMap = {};
        periods.forEach((p, i) => {
            _rematDailyMap[p] = Number(remat[i]) || 0;
        });
        _rematDailyRange = d.range ? { from: d.range.from, to: d.range.to } : { from: null, to: null };
        _rematInitCalendarMonth();
        _rematRenderCalendar();
        const labels = periods.map((p) => _rematFormatLabel(p, 'day'));

        const labelEl = document.getElementById('remat-daily-label');
        if (labelEl) labelEl.textContent = labelText + ' / dia';

        const total = remat.reduce((a, b) => a + (Number(b) || 0), 0);
        const daysWithData = remat.filter((v) => Number(v) > 0).length;
        const peak = remat.length ? Math.max(...remat.map((v) => Number(v) || 0)) : 0;
        const avg = daysWithData ? Math.round(total / daysWithData) : 0;

        const setText = (id, val) => {
            const el = document.getElementById(id);
            if (el) el.textContent = val;
        };
        setText('remat-daily-total', total.toLocaleString('pt-BR'));
        setText('remat-daily-avg', avg.toLocaleString('pt-BR') + '/dia');
        setText('remat-daily-peak', peak.toLocaleString('pt-BR'));

        _rematDailyChart = _rematRenderLineChart(
            'remat-chart-daily',
            _rematDailyChart,
            labels,
            remat,
            labelText,
            _rematChartOpts(null)
        );

        const rangeTxt = d.range ? d.range.from + ' → ' + d.range.to : '';
        setText(
            'remat-daily-period-label',
            [ciclo ? 'Ciclo ' + ciclo : '', rangeTxt].filter(Boolean).join(' · ')
        );
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

const _REMAT_MONTH_NAMES = [
    'Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
    'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro',
];
const _REMAT_DAY_NAMES = ['Seg', 'Ter', 'Qua', 'Qui', 'Sex', 'Sáb', 'Dom'];

function _rematInitCalendarMonth() {
    if (_rematCalMonth) return;
    const today = new Date().toLocaleDateString('sv-SE');
    const { from, to } = _rematDailyRange;
    let pick = today;
    if (from && pick < from) pick = from;
    if (to && pick > to) pick = to;
    if (!from && !to && Object.keys(_rematDailyMap).length) {
        pick = Object.keys(_rematDailyMap).sort().pop();
    }
    const dt = new Date(pick + 'T12:00:00');
    _rematCalMonth = { year: dt.getFullYear(), month: dt.getMonth() };
}

function rematCalPrevMonth() {
    if (!_rematCalMonth) return;
    const d = new Date(_rematCalMonth.year, _rematCalMonth.month - 1, 1);
    _rematCalMonth = { year: d.getFullYear(), month: d.getMonth() };
    _rematRenderCalendar();
}

function rematCalNextMonth() {
    if (!_rematCalMonth) return;
    const d = new Date(_rematCalMonth.year, _rematCalMonth.month + 1, 1);
    _rematCalMonth = { year: d.getFullYear(), month: d.getMonth() };
    _rematRenderCalendar();
}

function rematCalSelectDay(dateStr) {
    _rematCalSelected = dateStr;
    _rematRenderCalendar();
}

function _rematCalCellStyle(count, maxInMonth) {
    if (!count) {
        return {
            bg: 'var(--bg-elevated, #1a2942)',
            border: 'rgba(100,116,139,0.25)',
            text: '#64748b',
        };
    }
    const t = maxInMonth > 0 ? Math.min(1, count / maxInMonth) : 0.5;
    const alpha = 0.15 + t * 0.55;
    return {
        bg: `rgba(16,185,129,${alpha})`,
        border: `rgba(16,185,129,${0.35 + t * 0.45})`,
        text: t > 0.5 ? '#ecfdf5' : '#6ee7b7',
    };
}

function _rematRenderCalendar() {
    const wrap = document.getElementById('remat-calendar');
    const titleEl = document.getElementById('remat-cal-title');
    const detailEl = document.getElementById('remat-cal-detail');
    if (!wrap || !_rematCalMonth) return;

    const { year, month } = _rematCalMonth;
    if (titleEl) titleEl.textContent = `${_REMAT_MONTH_NAMES[month]} ${year}`;

    const lastDay = new Date(year, month + 1, 0).getDate();
    let startDow = new Date(year, month, 1).getDay() - 1;
    if (startDow < 0) startDow = 6;

    const today = new Date().toLocaleDateString('sv-SE');
    const { from, to } = _rematDailyRange;
    let maxInMonth = 0;

    for (let day = 1; day <= lastDay; day++) {
        const dateStr = `${year}-${String(month + 1).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
        maxInMonth = Math.max(maxInMonth, _rematDailyMap[dateStr] || 0);
    }

    let html = _REMAT_DAY_NAMES.map(
        (dn) =>
            `<div class="text-center text-[9px] font-bold text-slate-500 uppercase tracking-wider pb-1">${dn}</div>`
    ).join('');

    for (let i = 0; i < startDow; i++) {
        html += '<div class="aspect-square"></div>';
    }

    for (let day = 1; day <= lastDay; day++) {
        const dateStr = `${year}-${String(month + 1).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
        const count = _rematDailyMap[dateStr] || 0;
        const inRange = (!from || dateStr >= from) && (!to || dateStr <= to);
        const isToday = dateStr === today;
        const isSelected = dateStr === _rematCalSelected;
        const st = _rematCalCellStyle(inRange ? count : 0, maxInMonth);
        const opacity = inRange ? '1' : '0.35';
        const ring = isSelected
            ? 'ring-2 ring-emerald-400'
            : isToday
              ? 'ring-2 ring-cyan-400'
              : '';

        html += `<button type="button"
            onclick="rematCalSelectDay('${dateStr}')"
            class="aspect-square rounded-lg flex flex-col items-center justify-center gap-0.5 transition-all hover:scale-105 hover:z-10 ${ring} ${inRange ? 'cursor-pointer' : 'cursor-default'}"
            style="opacity:${opacity};background:${st.bg};border:1px solid ${st.border}"
            title="${dateStr}: ${count.toLocaleString('pt-BR')} rematrículas">
            <span class="text-[11px] font-bold leading-none" style="color:${st.text}">${day}</span>
            ${inRange ? `<span class="text-[9px] font-mono font-semibold leading-none" style="color:${st.text}">${count || '·'}</span>` : ''}
        </button>`;
    }

    wrap.innerHTML = html;

    if (detailEl) {
        if (_rematCalSelected && _rematDailyMap[_rematCalSelected] != null) {
            const n = _rematDailyMap[_rematCalSelected] || 0;
            const [y, m, d] = _rematCalSelected.split('-');
            detailEl.innerHTML =
                `<strong class="text-slate-700 dark:text-slate-200">${d}/${m}/${y}</strong>: ` +
                `<span class="font-mono font-bold text-emerald-600 dark:text-emerald-400">${n.toLocaleString('pt-BR')}</span> rematrículas`;
        } else {
            detailEl.textContent = 'Clique em um dia para ver o total de rematrículas.';
        }
    }
}
