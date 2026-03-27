// ---------------------------------------------------------------------------
// Minha Performance — Book do Agente
// ---------------------------------------------------------------------------
let _mpChartDaily = null;

const _mpFmt = v => (v || 0).toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
const _mpDias = ['Seg', 'Ter', 'Qua', 'Qui', 'Sex', 'Sáb', 'Dom'];

function _mpFmtDate(s) {
    if (!s) return '';
    const p = s.split('-');
    return p.length === 3 ? `${p[2]}/${p[1]}/${p[0]}` : s;
}

async function loadMinhaPerformance() {
    const loading = document.getElementById('mp-loading');
    const noLink = document.getElementById('mp-no-link');
    const content = document.getElementById('mp-content');
    const noCamp = document.getElementById('mp-no-campanha');
    loading.classList.remove('hidden');
    noLink.classList.add('hidden');
    content.classList.add('hidden');
    noCamp.classList.add('hidden');

    try {
        const me = await (await api('/api/me')).json();
        const kommoUid = me.kommo_user_id;

        if (!kommoUid && me.role !== 'admin') {
            loading.classList.add('hidden');
            noLink.classList.remove('hidden');
            return;
        }

        const qs = kommoUid ? `?kommo_uid=${kommoUid}` : '';
        const [insightsRes, histRes] = await Promise.all([
            api('/api/minha-performance/insights' + qs),
            api('/api/minha-performance/historico' + qs),
        ]);

        const ins = await insightsRes.json();
        const hist = await histRes.json();

        loading.classList.add('hidden');

        if (!ins.ok && ins.error) {
            noLink.classList.remove('hidden');
            return;
        }
        if (!ins.campanha) {
            noCamp.classList.remove('hidden');
            return;
        }

        content.classList.remove('hidden');
        _mpRenderBanner(ins);
        _mpRenderScoreboard(ins);
        _mpRenderMomentum(ins);
        _mpRenderToday(ins);
        _mpRenderPremiacao(ins);
        _mpRenderHeatmap(ins);
        _mpRenderTimeline(ins);
        _mpRenderDetailTable(ins.matriculas || []);
        _mpRenderHistorico(hist.historico || []);

    } catch (e) {
        loading.classList.add('hidden');
        console.error('loadMinhaPerformance', e);
    }
}

// ---------------------------------------------------------------------------
// S1: Banner
// ---------------------------------------------------------------------------
function _mpRenderBanner(d) {
    document.getElementById('mp-banner-camp-nome').textContent = d.campanha.nome;
    document.getElementById('mp-banner-datas').textContent = `${_mpFmtDate(d.campanha.dt_inicio)} — ${_mpFmtDate(d.campanha.dt_fim)}`;
    document.getElementById('mp-banner-total').textContent = (d.total_matriculas || 0).toLocaleString('pt-BR');
    document.getElementById('mp-banner-dias').textContent = d.dias_restantes;
    document.getElementById('mp-banner-msg').textContent = d.mensagem || '';

    const pct = d.pct || 0;
    const pctEl = document.getElementById('mp-banner-pct-text');
    pctEl.textContent = `${Math.round(pct)}% da meta`;
    if (pct >= 100) pctEl.className = 'text-lg font-bold text-emerald-400';
    else if (pct >= 70) pctEl.className = 'text-lg font-bold text-blue-400';
    else if (pct >= 40) pctEl.className = 'text-lg font-bold text-amber-400';
    else pctEl.className = 'text-lg font-bold text-red-400';

    const tierBadge = document.getElementById('mp-banner-tier-badge');
    if (d.tier) {
        const map = {
            supermeta: { label: 'SUPERMETA', cls: 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30' },
            meta: { label: 'META', cls: 'bg-blue-500/20 text-blue-400 border border-blue-500/30' },
            intermediaria: { label: 'INTERMEDIÁRIA', cls: 'bg-amber-500/20 text-amber-400 border border-amber-500/30' },
        };
        const t = map[d.tier] || map.meta;
        tierBadge.textContent = t.label;
        tierBadge.className = `px-4 py-1.5 rounded-full text-sm font-bold ${t.cls}`;
        tierBadge.classList.remove('hidden');
    } else {
        tierBadge.classList.add('hidden');
    }
}

// ---------------------------------------------------------------------------
// S2: Scoreboard Multi-Tier
// ---------------------------------------------------------------------------
function _mpRenderScoreboard(d) {
    const metas = d.metas || {};
    const total = d.total_matriculas || 0;
    const superVal = metas.supermeta || 0;
    const maxRef = superVal > 0 ? Math.max(superVal * 1.15, total * 1.05) : Math.max((metas.meta || 1) * 1.3, total * 1.05);

    const fillPct = Math.min(100, (total / maxRef) * 100);
    const fill = document.getElementById('mp-tier-fill');
    const tierColor = d.tier === 'supermeta' ? 'linear-gradient(90deg, #f59e0b, #3b82f6, #10b981)' :
                      d.tier === 'meta' ? 'linear-gradient(90deg, #f59e0b, #3b82f6)' :
                      d.tier === 'intermediaria' ? 'linear-gradient(90deg, #f59e0b, #d97706)' :
                      'linear-gradient(90deg, #64748b, #94a3b8)';
    fill.style.background = tierColor;
    requestAnimationFrame(() => { fill.style.width = fillPct + '%'; });

    const track = document.getElementById('mp-tier-track');
    const markersEl = document.getElementById('mp-tier-markers');
    markersEl.innerHTML = '';

    const tiers = [
        { key: 'intermediaria', val: metas.intermediaria, label: 'Inter', color: '#f59e0b' },
        { key: 'meta', val: metas.meta, label: 'Meta', color: '#3b82f6' },
        { key: 'supermeta', val: metas.supermeta, label: 'Super', color: '#10b981' },
    ];

    tiers.forEach(t => {
        if (!t.val || t.val <= 0) return;
        const pct = (t.val / maxRef) * 100;
        const marker = document.createElement('div');
        marker.className = 'mp-tier-marker';
        marker.style.left = pct + '%';
        marker.style.position = 'absolute';

        const label = document.createElement('span');
        label.className = 'mp-tier-label';
        label.style.left = pct + '%';
        label.style.color = t.color;
        label.textContent = t.label;

        const valEl = document.createElement('span');
        valEl.className = 'mp-tier-val';
        valEl.style.left = pct + '%';
        valEl.textContent = t.val;

        markersEl.appendChild(marker);
        markersEl.appendChild(label);
        markersEl.appendChild(valEl);
    });
    markersEl.style.position = 'relative';

    const badgeInter = document.getElementById('mp-badge-inter');
    const badgeMeta = document.getElementById('mp-badge-meta');
    const badgeSuper = document.getElementById('mp-badge-super');

    _mpSetBadge(badgeInter, total >= (metas.intermediaria || Infinity), 'amber');
    _mpSetBadge(badgeMeta, total >= (metas.meta || Infinity), 'blue');
    _mpSetBadge(badgeSuper, total >= (metas.supermeta || Infinity), 'emerald');

    document.getElementById('mp-badge-inter-val').textContent = metas.intermediaria ? `${metas.intermediaria} mat` : '—';
    document.getElementById('mp-badge-meta-val').textContent = metas.meta ? `${metas.meta} mat` : '—';
    document.getElementById('mp-badge-super-val').textContent = metas.supermeta ? `${metas.supermeta} mat` : '—';
}

function _mpSetBadge(el, achieved, color) {
    if (achieved) {
        el.className = `inline-flex items-center gap-1 px-3 py-1.5 rounded-full text-xs font-bold bg-${color}-500/20 text-${color}-400 border border-${color}-500/30 shadow-lg shadow-${color}-500/10`;
    } else {
        el.className = 'inline-flex items-center gap-1 px-3 py-1.5 rounded-full text-xs font-bold bg-slate-800 text-slate-500 border border-slate-700/30';
    }
}

// ---------------------------------------------------------------------------
// S3: Momentum
// ---------------------------------------------------------------------------
function _mpRenderMomentum(d) {
    const paceEl = document.getElementById('mp-pace-atual');
    paceEl.textContent = d.pace_atual;

    const paceCard = document.getElementById('mp-pace-card');
    if (d.pace_atual >= (d.pace_meta || 999)) {
        paceEl.className = 'text-3xl font-black text-emerald-400 font-display';
    } else if (d.pace_atual >= (d.pace_inter || 999)) {
        paceEl.className = 'text-3xl font-black text-amber-400 font-display';
    } else {
        paceEl.className = 'text-3xl font-black text-red-400 font-display';
    }

    let neededPace = d.pace_meta;
    let neededLabel = 'p/ META';
    const metas = d.metas || {};
    const total = d.total_matriculas || 0;
    if (metas.supermeta > 0 && total >= (metas.meta || 0)) {
        neededPace = d.pace_super;
        neededLabel = 'p/ SUPERMETA';
    } else if (metas.meta > 0 && total >= (metas.intermediaria || 0) && total < (metas.meta || 0)) {
        neededPace = d.pace_meta;
        neededLabel = 'p/ META';
    } else if (metas.intermediaria > 0 && total < (metas.intermediaria || 0)) {
        neededPace = d.pace_inter;
        neededLabel = 'p/ INTERMEDIÁRIA';
    }

    document.getElementById('mp-pace-needed').textContent = neededPace || '—';
    document.getElementById('mp-pace-needed-label').textContent = neededLabel;

    document.getElementById('mp-projecao').textContent = d.projecao || 0;
    const projTierEl = document.getElementById('mp-projecao-tier');
    if (d.projecao_tier) {
        const tierLabels = { supermeta: 'SUPERMETA', meta: 'META', intermediaria: 'INTERMEDIÁRIA' };
        const tierColors = { supermeta: 'text-emerald-400', meta: 'text-blue-400', intermediaria: 'text-amber-400' };
        projTierEl.textContent = `Projeção: ${tierLabels[d.projecao_tier] || d.projecao_tier}`;
        projTierEl.className = `text-[10px] mt-1 font-bold ${tierColors[d.projecao_tier] || 'text-slate-400'}`;
    } else {
        projTierEl.textContent = 'Abaixo dos tiers';
        projTierEl.className = 'text-[10px] mt-1 text-red-400';
    }

    document.getElementById('mp-dias-rest').textContent = d.dias_restantes || 0;
    const totalDays = d.dias_restantes + (d.dias_uteis_restantes ? Math.max(1, d.total_matriculas ? 1 : 1) : 1);
    const elapsed = Math.max(0, 100 - ((d.dias_restantes || 0) / Math.max(totalDays, 1)) * 100);
    const dtIni = new Date(d.campanha.dt_inicio);
    const dtFim = new Date(d.campanha.dt_fim);
    const totalCampDays = Math.max(1, (dtFim - dtIni) / 86400000);
    const elapsedPct = Math.min(100, Math.max(0, ((totalCampDays - (d.dias_restantes || 0)) / totalCampDays) * 100));
    document.getElementById('mp-dias-bar').style.width = elapsedPct + '%';
}

// ---------------------------------------------------------------------------
// S4: Desafio de Hoje
// ---------------------------------------------------------------------------
function _mpRenderToday(d) {
    const h = d.hoje || {};
    const dowNames = ['Seg', 'Ter', 'Qua', 'Qui', 'Sex', 'Sáb', 'Dom'];
    document.getElementById('mp-today-dow').textContent = dowNames[h.dia_semana] || '—';
    document.getElementById('mp-today-meta').textContent = h.meta || 0;
    document.getElementById('mp-today-realizadas').textContent = h.realizadas || 0;

    const meta = h.meta || 0;
    const feitas = h.realizadas || 0;
    const falta = Math.max(0, meta - feitas);

    if (meta > 0) {
        if (feitas >= meta) {
            document.getElementById('mp-today-bonus-text').textContent =
                `Meta batida! +${_mpFmt(h.bonus_extra)}/extra`;
            document.getElementById('mp-today-card').classList.remove('mp-pulse-border');
            document.getElementById('mp-today-card').style.borderColor = 'rgba(16,185,129,0.4)';
        } else {
            document.getElementById('mp-today-bonus-text').textContent =
                `Bata ${meta} para ganhar ${_mpFmt(h.bonus_fixo)} + ${_mpFmt(h.bonus_extra)}/extra`;
            document.getElementById('mp-today-card').classList.add('mp-pulse-border');
            document.getElementById('mp-today-card').style.borderColor = '';
        }
    } else {
        document.getElementById('mp-today-bonus-text').textContent = 'Sem meta configurada para hoje';
        document.getElementById('mp-today-card').classList.remove('mp-pulse-border');
        document.getElementById('mp-today-card').style.borderColor = 'rgba(100,116,139,0.2)';
    }

    const ring = document.getElementById('mp-today-ring');
    const circ = 2 * Math.PI * 42;
    const pct = meta > 0 ? Math.min(1, feitas / meta) : 0;
    requestAnimationFrame(() => { ring.style.strokeDashoffset = circ - pct * circ; });
}

// ---------------------------------------------------------------------------
// S5: Premiação
// ---------------------------------------------------------------------------
function _mpRenderPremiacao(d) {
    const p = d.premiacao || {};
    document.getElementById('mp-prem-tier').textContent = _mpFmt(p.tier_bonus);
    const tierLabel = d.tier ? d.tier.toUpperCase() : 'Nenhum';
    const vpmat = p.tier_valor_por_mat || 0;
    document.getElementById('mp-prem-tier-detail').textContent =
        d.tier ? `${tierLabel} — ${_mpFmt(vpmat)}/mat x ${d.total_matriculas}` : 'Nenhum tier atingido';

    document.getElementById('mp-prem-daily').textContent = _mpFmt(p.daily_bonus);
    document.getElementById('mp-prem-daily-detail').textContent =
        `${p.daily_dias_batidos || 0}/${p.daily_dias_total || 0} dias batidos`;

    document.getElementById('mp-prem-receb').textContent = _mpFmt(p.receb_bonus);
    document.getElementById('mp-prem-receb-detail').textContent =
        (p.receb_total_valor || 0) > 0 ? `Sobre ${_mpFmt(p.receb_total_valor)} recebidos` : 'Sem dados de recebimento';

    document.getElementById('mp-prem-total').textContent = _mpFmt(p.total);
}

// ---------------------------------------------------------------------------
// S6: Heatmap
// ---------------------------------------------------------------------------
function _mpRenderHeatmap(d) {
    const heatmap = d.heatmap || [];
    const wrap = document.getElementById('mp-heatmap');
    wrap.innerHTML = '';

    document.getElementById('mp-streak-text').textContent = d.sequencia > 0
        ? `${d.sequencia} dia${d.sequencia > 1 ? 's' : ''} consecutivo${d.sequencia > 1 ? 's' : ''}!`
        : 'Inicie sua sequência!';

    if (!heatmap.length) return;

    const weeks = [];
    let currentWeek = [];
    const firstDow = heatmap[0].dia_semana;
    for (let i = 0; i < firstDow; i++) currentWeek.push(null);

    heatmap.forEach(day => {
        if (currentWeek.length === 7) {
            weeks.push(currentWeek);
            currentWeek = [];
        }
        currentWeek.push(day);
    });
    while (currentWeek.length < 7) currentWeek.push(null);
    weeks.push(currentWeek);

    weeks.forEach(week => {
        const row = document.createElement('div');
        row.className = 'flex gap-1';
        week.forEach(day => {
            const cell = document.createElement('div');
            cell.className = 'mp-heatmap-cell';
            if (!day) {
                cell.className += ' mp-heatmap-future';
                cell.style.opacity = '0.3';
                cell.textContent = '';
            } else {
                const cls = {
                    hit: 'mp-heatmap-hit',
                    partial: 'mp-heatmap-partial',
                    miss: 'mp-heatmap-miss',
                    future: 'mp-heatmap-future',
                }[day.status] || 'mp-heatmap-miss';
                cell.className += ` ${cls}`;
                cell.textContent = day.realizadas != null ? day.realizadas : '';
                cell.title = `${_mpFmtDate(day.data)} — ${day.realizadas ?? '?'}/${day.meta} mat`;
            }
            row.appendChild(cell);
        });
        wrap.appendChild(row);
    });
}

// ---------------------------------------------------------------------------
// S7: Timeline Chart
// ---------------------------------------------------------------------------
function _mpRenderTimeline(d) {
    const breakdown = (d.premiacao || {}).daily_breakdown || [];
    const canvas = document.getElementById('mp-chart-daily');
    if (!canvas) return;
    if (_mpChartDaily) { _mpChartDaily.destroy(); _mpChartDaily = null; }
    if (!breakdown.length) return;

    const labels = breakdown.map(b => _mpFmtDate(b.data));
    const realizadas = breakdown.map(b => b.realizadas);
    const metas = breakdown.map(b => b.meta);

    _mpChartDaily = new Chart(canvas, {
        type: 'bar',
        data: {
            labels,
            datasets: [
                {
                    label: 'Matrículas',
                    data: realizadas,
                    backgroundColor: breakdown.map(b => b.realizadas >= b.meta ? 'rgba(16,185,129,0.6)' : 'rgba(245,158,11,0.4)'),
                    borderRadius: 4,
                    barPercentage: 0.7,
                },
                {
                    label: 'Meta diária',
                    data: metas,
                    type: 'line',
                    borderColor: 'rgba(99,102,241,0.6)',
                    borderDash: [4, 4],
                    borderWidth: 2,
                    pointRadius: 0,
                    fill: false,
                },
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: true, labels: { color: '#94a3b8', font: { size: 10 } } },
            },
            scales: {
                x: { ticks: { color: '#64748b', font: { size: 9 }, maxRotation: 45 }, grid: { display: false } },
                y: { beginAtZero: true, ticks: { color: '#64748b', font: { size: 10 }, stepSize: 1 }, grid: { color: 'rgba(100,116,139,0.1)' } },
            },
        },
    });
}

// ---------------------------------------------------------------------------
// S8: Detalhado
// ---------------------------------------------------------------------------
function _mpRenderDetailTable(matriculas) {
    const tbody = document.getElementById('mp-detail-tbody');
    const countEl = document.getElementById('mp-detail-count');
    countEl.textContent = `(${matriculas.length})`;

    if (!matriculas.length) {
        tbody.innerHTML = '<tr><td colspan="4" class="px-5 py-6 text-center text-slate-600">Nenhuma matrícula no período</td></tr>';
        return;
    }
    tbody.innerHTML = matriculas.map(m => {
        const rgm = m.rgm || '--';
        const nivel = m.nivel || '--';
        const mod = m.modalidade || '--';
        const dt = m.data_matricula ? _mpFmtDate(m.data_matricula) : '--';
        return `<tr class="mp-detail-row" data-rgm="${rgm}">
            <td class="px-4 py-2 text-slate-300 font-mono text-xs">${rgm}</td>
            <td class="px-4 py-2 text-slate-400 text-xs">${nivel}</td>
            <td class="px-4 py-2 text-slate-400 text-xs">${mod}</td>
            <td class="px-4 py-2 text-slate-400 text-xs">${dt}</td>
        </tr>`;
    }).join('');
}

function mpFilterTable() {
    const q = (document.getElementById('mp-search-rgm').value || '').trim().toLowerCase();
    document.querySelectorAll('.mp-detail-row').forEach(row => {
        row.style.display = !q || (row.dataset.rgm || '').toLowerCase().includes(q) ? '' : 'none';
    });
}

// ---------------------------------------------------------------------------
// S9: Histórico
// ---------------------------------------------------------------------------
function _mpRenderHistorico(historico) {
    const wrap = document.getElementById('mp-historico');
    if (!historico.length) {
        wrap.innerHTML = '<p class="text-slate-600 text-xs">Nenhum histórico disponível</p>';
        return;
    }
    wrap.innerHTML = historico.map(h => {
        const tierMap = {
            supermeta: { label: 'SUPERMETA', cls: 'text-emerald-400 bg-emerald-500/10 border-emerald-500/20' },
            meta: { label: 'META', cls: 'text-blue-400 bg-blue-500/10 border-blue-500/20' },
            intermediaria: { label: 'INTERM.', cls: 'text-amber-400 bg-amber-500/10 border-amber-500/20' },
        };
        const t = tierMap[h.tier] || { label: 'Abaixo', cls: 'text-red-400 bg-red-500/10 border-red-500/20' };
        const meta = h.metas?.meta || 0;
        const pctStr = meta > 0 ? `${Math.round(h.total_matriculas / meta * 100)}%` : '--';
        const isActive = h.ativa;
        return `<div class="rounded-xl p-4 border ${t.cls} min-w-[200px] ${isActive ? 'ring-1 ring-amber-500/30' : ''}">
            <div class="flex items-center justify-between mb-2">
                <span class="text-xs font-bold text-white">${h.nome}</span>
                ${isActive ? '<span class="text-[9px] bg-amber-500/20 text-amber-400 px-1.5 py-0.5 rounded-full font-bold">ATIVA</span>' : ''}
            </div>
            <p class="text-[10px] text-slate-500">${_mpFmtDate(h.dt_inicio)} — ${_mpFmtDate(h.dt_fim)}</p>
            <div class="flex items-baseline gap-2 mt-2">
                <span class="text-xl font-black text-white font-display">${h.total_matriculas}</span>
                <span class="text-xs text-slate-500">/ ${meta}</span>
                <span class="text-xs font-bold ${t.cls.split(' ')[0]}">${pctStr}</span>
            </div>
            <p class="text-[10px] font-bold mt-1 ${t.cls.split(' ')[0]}">${t.label}</p>
            <div class="mt-2 pt-2 border-t border-slate-700/20 space-y-0.5">
                <p class="text-[10px] text-slate-400">Tier: <span class="font-bold text-amber-300">${_mpFmt(h.tier_bonus)}</span></p>
                <p class="text-[10px] text-slate-400">Diária: <span class="font-bold text-cyan-300">${_mpFmt(h.daily_bonus)}</span> <span class="text-slate-600">(${h.dias_batidos}/${h.dias_total}d)</span></p>
                <p class="text-[10px] font-bold text-emerald-400 mt-1">Total: ${_mpFmt(h.total_premiacao)}</p>
            </div>
        </div>`;
    }).join('');
}
