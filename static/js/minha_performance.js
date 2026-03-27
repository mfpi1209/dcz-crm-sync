/* ═══════════════  Minha Performance — Book Motivacional  ═══════════════ */

let _mpChartDaily = null;
let _mpSelectedUid = null;
let _mpMyUid = null;
let _mpIsAdmin = false;
let _mpAgentsLoaded = false;

const _mpFmt = v => Number(v||0).toLocaleString('pt-BR',{style:'currency',currency:'BRL'});
const _mpFmtN = v => Number(v||0).toLocaleString('pt-BR',{minimumFractionDigits:0,maximumFractionDigits:0});
const _mpDias = ['Seg','Ter','Qua','Qui','Sex','Sáb','Dom'];

function _mpFmtDate(d) {
    if (!d) return '';
    const p = String(d).split('-');
    return p.length === 3 ? `${p[2]}/${p[1]}/${p[0]}` : d;
}

function navigateToPerformance(kommoUid) {
    _mpSelectedUid = kommoUid || null;
    navigate('minha_performance', { uid: kommoUid });
}

/* ── Entry ── */
async function loadMinhaPerformance(params) {
    if (params?.uid) _mpSelectedUid = Number(params.uid);

    const loading = document.getElementById('mp-loading');
    const noLink = document.getElementById('mp-no-link');
    const noCamp = document.getElementById('mp-no-campanha');
    const content = document.getElementById('mp-content');
    const adminBar = document.getElementById('mp-admin-bar');
    [loading, noLink, noCamp, content].forEach(el => { if(el) el.classList.add('hidden'); });
    if (loading) loading.classList.remove('hidden');

    try {
        const meRes = await api('/api/me');
        const me = await meRes.json();
        const kommoUid = me?.kommo_user_id;
        _mpIsAdmin = me?.role === 'admin';
        _mpMyUid = kommoUid;

        if (_mpIsAdmin && adminBar) {
            adminBar.classList.remove('hidden');
            _mpLoadAgentSelector();
        } else if (adminBar) {
            adminBar.classList.add('hidden');
        }

        const effectiveUid = (_mpIsAdmin && _mpSelectedUid) ? _mpSelectedUid : kommoUid;

        if (!effectiveUid && !_mpIsAdmin) {
            if (loading) loading.classList.add('hidden');
            if (noLink) noLink.classList.remove('hidden');
            return;
        }

        _mpUpdateAdminViewingState(effectiveUid);

        const qs = effectiveUid ? `?kommo_uid=${effectiveUid}` : '';
        const [insightsRes, histRes] = await Promise.all([
            api(`/api/minha-performance/insights${qs}`),
            api(`/api/minha-performance/historico${qs}`),
        ]);
        const insights = await insightsRes.json();
        const hist = await histRes.json();

        if (loading) loading.classList.add('hidden');

        if (!insights?.campanha) {
            if (noCamp) noCamp.classList.remove('hidden');
            return;
        }

        if (content) content.classList.remove('hidden');

        _mpRenderHero(insights);
        _mpRenderDesbloqueie(insights);
        _mpRenderPixDia(insights);
        _mpRenderMomentum(insights);
        _mpRenderStreak(insights);
        _mpRenderFinanceiro(insights);
        _mpRenderTimeline(insights);
        _mpRenderTable(insights);
        _mpRenderHistorico(hist?.historico || []);

    } catch(e) {
        console.error('loadMinhaPerformance', e);
        if (loading) loading.classList.add('hidden');
        if (noCamp) { noCamp.classList.remove('hidden'); noCamp.querySelector('p').textContent = 'Erro ao carregar dados.'; }
    }
}

/* ── Admin: agent selector ── */
async function _mpLoadAgentSelector() {
    if (_mpAgentsLoaded) return;
    const sel = document.getElementById('mp-agent-select');
    if (!sel) return;
    try {
        const res = await api('/api/minha-performance/agentes');
        const d = await res.json();
        const agents = d?.agentes || [];
        sel.innerHTML = '<option value="">Selecione um agente...</option>' +
            agents.map(a => `<option value="${a.kommo_uid}">${a.name}</option>`).join('');
        if (_mpSelectedUid) sel.value = String(_mpSelectedUid);
        _mpAgentsLoaded = true;
    } catch(e) { console.error('_mpLoadAgentSelector', e); }
}

function _mpUpdateAdminViewingState(effectiveUid) {
    const backBtn = document.getElementById('mp-admin-back');
    const viewing = document.getElementById('mp-admin-viewing');
    const sel = document.getElementById('mp-agent-select');
    if (!_mpIsAdmin) return;

    const isViewingOther = effectiveUid && effectiveUid !== _mpMyUid;
    if (backBtn) backBtn.classList.toggle('hidden', !isViewingOther);

    if (sel && effectiveUid) sel.value = String(effectiveUid);

    if (viewing && isViewingOther) {
        const opt = sel?.querySelector(`option[value="${effectiveUid}"]`);
        viewing.textContent = opt ? `Visualizando: ${opt.textContent}` : `Visualizando: ID ${effectiveUid}`;
        viewing.classList.remove('hidden');
    } else if (viewing) {
        viewing.classList.add('hidden');
    }
}

function mpAdminSelectAgent() {
    const sel = document.getElementById('mp-agent-select');
    const uid = sel?.value ? Number(sel.value) : null;
    _mpSelectedUid = uid;
    loadMinhaPerformance();
}

function mpAdminBackToSelf() {
    _mpSelectedUid = null;
    const sel = document.getElementById('mp-agent-select');
    if (sel) sel.value = '';
    loadMinhaPerformance();
}

/* ═══ S1: Hero ═══ */
function _mpRenderHero(d) {
    const hero = document.getElementById('mp-hero');
    const prem = d.premiacao || {};
    const total = prem.total || 0;
    const tier = d.tier;

    if (hero) {
        hero.className = hero.className.replace(/mp-tier-\w+/g, '');
        if (tier === 'supermeta') hero.classList.add('mp-tier-gold');
        else if (tier === 'meta') hero.classList.add('mp-tier-silver');
        else if (tier === 'intermediaria') hero.classList.add('mp-tier-bronze');
        else hero.classList.add('mp-tier-gray');
    }

    const el = id => document.getElementById(id);
    el('mp-hero-campanha').textContent = d.campanha?.nome || '';
    el('mp-hero-saldo').textContent = _mpFmt(total);

    const maxPotencial = _mpCalcMaxPotencial(d);
    el('mp-hero-potencial').textContent = maxPotencial > total ? _mpFmt(maxPotencial) : '';

    const tierLabels = { intermediaria: 'Intermediária', meta: 'Meta', supermeta: 'Supermeta' };
    const badge = el('mp-hero-tier-badge');
    if (tier) {
        badge.textContent = tierLabels[tier] || tier;
        badge.className = 'px-3 py-1 text-xs font-bold rounded-full ';
        if (tier === 'supermeta') badge.className += 'bg-amber-500/30 text-amber-300';
        else if (tier === 'meta') badge.className += 'bg-slate-400/20 text-slate-300';
        else badge.className += 'bg-orange-500/20 text-orange-300';
    } else {
        badge.textContent = 'Sem tier';
        badge.className = 'px-3 py-1 text-xs font-bold rounded-full bg-slate-700/40 text-slate-500';
    }

    el('mp-hero-mat').textContent = `${d.total_matriculas} matrículas`;
    el('mp-hero-dias').textContent = `${d.dias_restantes} dias restantes`;
    el('mp-hero-msg').textContent = d.mensagem || '';
}

function _mpCalcMaxPotencial(d) {
    const prem = d.premiacao || {};
    const desb = prem.desbloqueie || [];
    if (!desb.length) return prem.total || 0;
    const maxTier = desb.reduce((max, t) => Math.max(max, t.ganho_total), 0);
    return maxTier + (prem.daily_bonus || 0) + (prem.receb_bonus || 0);
}

/* ═══ S2: Desbloqueie Mais ═══ */
function _mpRenderDesbloqueie(d) {
    const wrap = document.getElementById('mp-desbloqueie');
    const wrapOuter = document.getElementById('mp-desbloqueie-wrap');
    if (!wrap) return;
    const desb = d.premiacao?.desbloqueie || [];

    if (!desb.length || desb.every(t => t.atingido)) {
        if (wrapOuter) wrapOuter.classList.add('hidden');
        return;
    }
    if (wrapOuter) wrapOuter.classList.remove('hidden');

    const tierLabels = { intermediaria: 'Intermediária', meta: 'Meta', supermeta: 'Supermeta' };
    const tierColors = {
        intermediaria: { border: 'border-orange-500/30', text: 'text-orange-400', bg: 'bg-orange-500/10' },
        meta: { border: 'border-slate-400/30', text: 'text-slate-300', bg: 'bg-slate-400/10' },
        supermeta: { border: 'border-amber-500/30', text: 'text-amber-400', bg: 'bg-amber-500/10' },
    };

    const proximoNaoAtingido = desb.find(t => !t.atingido);

    wrap.innerHTML = desb.filter(t => !t.atingido).map(t => {
        const colors = tierColors[t.tier] || tierColors.meta;
        const isProximo = t === proximoNaoAtingido;
        const pulseClass = isProximo ? 'mp-pulse' : '';
        return `<div class="glass-card p-4 ${colors.border} border ${pulseClass} relative overflow-hidden">
            ${isProximo ? '<div class="absolute top-0 right-0 px-2 py-0.5 text-[9px] font-bold bg-emerald-500/20 text-emerald-400 rounded-bl-lg">MAIS PRÓXIMO</div>' : ''}
            <p class="text-[10px] ${colors.text} uppercase font-bold tracking-wider mb-1">${tierLabels[t.tier]}</p>
            <p class="text-2xl font-black text-white mb-1">+${_mpFmt(t.ganho_adicional)}</p>
            <p class="text-xs text-slate-400">Faltam <span class="font-bold text-white">${t.falta}</span> matrículas</p>
            <p class="text-[10px] text-slate-600 mt-1">${_mpFmt(t.valor_por_mat)}/mat · Total: ${_mpFmt(t.ganho_total)}</p>
        </div>`;
    }).join('');
}

/* ═══ S3: PIX do Dia ═══ */
function _mpRenderPixDia(d) {
    const hoje = d.hoje || {};
    const meta = hoje.meta || 0;
    const feitas = hoje.realizadas || 0;
    const fixo = hoje.bonus_fixo || 0;
    const extra = hoje.bonus_extra || 0;

    const pct = meta > 0 ? Math.min(feitas / meta, 1) : 0;
    const circum = 314.16;
    const offset = circum * (1 - pct);

    const ringFill = document.getElementById('mp-ring-fill');
    const ringValue = document.getElementById('mp-ring-value');
    const ringLabel = document.getElementById('mp-ring-label');
    if (ringFill) {
        ringFill.setAttribute('stroke-dashoffset', offset);
        ringFill.setAttribute('stroke', feitas >= meta && meta > 0 ? '#10b981' : '#3b82f6');
    }
    if (ringValue) ringValue.textContent = feitas;
    if (ringLabel) ringLabel.textContent = meta > 0 ? `de ${meta}` : 'sem meta';

    const status = document.getElementById('mp-pix-status');
    const detail = document.getElementById('mp-pix-detail');
    const valor = document.getElementById('mp-pix-valor');

    if (meta <= 0) {
        if (status) status.textContent = 'Sem meta diária hoje';
        if (detail) detail.textContent = 'Nenhuma meta PIX configurada para hoje.';
        if (valor) valor.textContent = '';
        return;
    }

    if (feitas >= meta) {
        const ganho = fixo + extra * Math.max(0, feitas - meta);
        if (status) { status.textContent = 'PIX Garantido!'; status.className = 'text-base font-bold text-emerald-400 mb-1'; }
        if (detail) detail.textContent = feitas > meta
            ? `Meta batida! +${feitas - meta} extra × ${_mpFmt(extra)} cada`
            : 'Parabéns, meta do dia batida!';
        if (valor) valor.textContent = _mpFmt(ganho);
    } else {
        const falta = meta - feitas;
        if (status) { status.textContent = `Faltam ${falta} para o PIX!`; status.className = 'text-base font-bold text-cyan-400 mb-1'; }
        if (detail) detail.textContent = `Bata ${meta} matrículas hoje e garanta seu PIX!`;
        if (valor) valor.textContent = `Prêmio: ${_mpFmt(fixo)}`;
    }
}

/* ═══ S4: Momentum ═══ */
function _mpRenderMomentum(d) {
    const el = id => document.getElementById(id);
    el('mp-pace').textContent = d.pace_atual?.toFixed(1) || '0';
    el('mp-pace-needed').textContent = d.pace_meta > 900 ? '--' : (d.pace_meta?.toFixed(1) || '0');

    el('mp-projecao-mat').textContent = `${d.projecao || 0} mat`;

    const projTier = el('mp-projecao-tier');
    const tierLabels = { intermediaria: 'Intermediária', meta: 'Meta', supermeta: 'Supermeta' };
    if (d.projecao_tier) {
        projTier.textContent = tierLabels[d.projecao_tier] || d.projecao_tier;
        projTier.className = 'text-[10px] text-emerald-400';
    } else {
        projTier.textContent = 'Abaixo da meta';
        projTier.className = 'text-[10px] text-red-400';
    }

    el('mp-projecao-fin').textContent = _mpFmt(d.projecao_financeira || 0);
}

/* ═══ S5: Streak + Heatmap ═══ */
function _mpRenderStreak(d) {
    const el = id => document.getElementById(id);
    el('mp-streak-num').textContent = d.sequencia || 0;
    el('mp-streak-label').textContent = d.sequencia > 0
        ? `${d.sequencia} dia${d.sequencia > 1 ? 's' : ''} consecutivo${d.sequencia > 1 ? 's' : ''}!`
        : 'Inicie sua sequência hoje!';

    const heatmap = d.heatmap || [];
    const wrap = document.getElementById('mp-heatmap');
    if (!wrap || !heatmap.length) return;

    const prem = d.premiacao || {};
    const breakdown = prem.daily_breakdown || [];
    const breakdownMap = {};
    breakdown.forEach(b => { breakdownMap[b.data] = b; });

    wrap.innerHTML = heatmap.map(h => {
        const cls = `mp-heat-${h.status}`;
        const bd = breakdownMap[h.data];
        const tooltip = h.status === 'future' ? 'Futuro'
            : `${_mpFmtDate(h.data)}: ${h.realizadas||0}/${h.meta||0}${bd ? ` · ${_mpFmt(bd.total)}` : ''}`;
        return `<div class="${cls} w-4 h-4 rounded-sm cursor-default" title="${tooltip}"></div>`;
    }).join('');
}

/* ═══ S6: Resumo Financeiro ═══ */
function _mpRenderFinanceiro(d) {
    const wrap = document.getElementById('mp-financeiro');
    if (!wrap) return;
    const prem = d.premiacao || {};
    const items = [
        { label: 'Bônus Tier', value: prem.tier_bonus || 0, color: 'bg-amber-500' },
        { label: 'PIX Diários', value: prem.daily_bonus || 0, color: 'bg-cyan-500' },
        { label: 'Recebimentos', value: prem.receb_bonus || 0, color: 'bg-violet-500' },
    ];
    const total = prem.total || 0;
    const maxVal = Math.max(...items.map(i => i.value), 1);

    wrap.innerHTML = items.map(i => {
        const pct = Math.round((i.value / maxVal) * 100);
        return `<div class="flex items-center gap-3">
            <span class="text-xs text-slate-400 w-24 flex-shrink-0">${i.label}</span>
            <div class="flex-1 bg-slate-700/30 rounded-full h-3 overflow-hidden">
                <div class="${i.color} h-full rounded-full transition-all duration-700" style="width:${pct}%"></div>
            </div>
            <span class="text-xs font-bold text-white w-24 text-right">${_mpFmt(i.value)}</span>
        </div>`;
    }).join('') + `
        <div class="flex items-center justify-between pt-3 border-t border-slate-700/30">
            <span class="text-sm font-bold text-emerald-400">TOTAL</span>
            <span class="text-xl font-black text-emerald-400">${_mpFmt(total)}</span>
        </div>`;
}

/* ═══ S7: Timeline ═══ */
function _mpRenderTimeline(d) {
    const canvas = document.getElementById('mp-chart-daily');
    if (!canvas || typeof Chart === 'undefined') return;
    if (_mpChartDaily) { _mpChartDaily.destroy(); _mpChartDaily = null; }

    const breakdown = d.premiacao?.daily_breakdown || [];
    if (!breakdown.length) return;

    const labels = breakdown.map(b => _mpFmtDate(b.data));
    const matData = breakdown.map(b => b.realizadas);
    const metaData = breakdown.map(b => b.meta);
    const bonusData = breakdown.map(b => b.total);

    _mpChartDaily = new Chart(canvas, {
        type: 'bar',
        data: {
            labels,
            datasets: [
                { label: 'Matrículas', data: matData, backgroundColor: 'rgba(16,185,129,.6)', borderRadius: 4, order: 2 },
                { label: 'Meta', data: metaData, type: 'line', borderColor: '#f59e0b', borderWidth: 2, pointRadius: 0, borderDash: [4,4], fill: false, order: 1 },
                { label: 'Bônus R$', data: bonusData, type: 'line', borderColor: '#06b6d4', borderWidth: 2, pointRadius: 2, fill: false, order: 0, yAxisID: 'y1' },
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: { legend: { labels: { color: '#94a3b8', font: { size: 10 } } } },
            scales: {
                x: { ticks: { color: '#64748b', font: { size: 9 }, maxRotation: 45 }, grid: { display: false } },
                y: { ticks: { color: '#64748b', font: { size: 9 } }, grid: { color: 'rgba(148,163,184,.1)' } },
                y1: { position: 'right', ticks: { color: '#06b6d4', font: { size: 9 }, callback: v => 'R$'+v }, grid: { display: false } },
            },
        },
    });
}

/* ═══ S8: Tabela de matrículas ═══ */
function _mpRenderTable(d) {
    const tbody = document.getElementById('mp-mat-tbody');
    const count = document.getElementById('mp-mat-count');
    const mats = d.matriculas || [];
    if (count) count.textContent = `${mats.length} registro${mats.length !== 1 ? 's' : ''}`;
    if (!tbody) return;
    tbody.innerHTML = mats.map(m => `<tr class="border-b border-slate-800/50 mp-mat-row" data-rgm="${(m.rgm||'').toLowerCase()}">
        <td class="py-1.5 px-2 text-slate-300">${m.rgm||'-'}</td>
        <td class="py-1.5 px-2 text-slate-400">${m.nivel||'-'}</td>
        <td class="py-1.5 px-2 text-slate-400">${m.modalidade||'-'}</td>
        <td class="py-1.5 px-2 text-slate-400">${_mpFmtDate(m.data_matricula)}</td>
    </tr>`).join('') || '<tr><td colspan="4" class="py-4 text-center text-slate-600 text-xs">Nenhuma matrícula</td></tr>';
}

function mpFilterTable() {
    const q = (document.getElementById('mp-search-rgm')?.value || '').toLowerCase();
    document.querySelectorAll('.mp-mat-row').forEach(row => {
        row.style.display = !q || row.dataset.rgm.includes(q) ? '' : 'none';
    });
}

/* ═══ S9: Histórico ═══ */
function _mpRenderHistorico(hist) {
    const wrap = document.getElementById('mp-historico');
    const wrapOuter = document.getElementById('mp-historico-wrap');
    if (!wrap) return;
    if (!hist.length) {
        if (wrapOuter) wrapOuter.classList.add('hidden');
        return;
    }
    if (wrapOuter) wrapOuter.classList.remove('hidden');

    const tierLabels = { intermediaria: 'Intermediária', meta: 'Meta', supermeta: 'Supermeta' };

    wrap.innerHTML = hist.filter(h => !h.ativa).map(h => `
        <div class="glass-card p-4 min-w-[200px] flex-shrink-0 border border-slate-700/30">
            <p class="text-xs font-semibold text-white mb-1">${h.nome}</p>
            <p class="text-[10px] text-slate-500">${_mpFmtDate(h.dt_inicio)} — ${_mpFmtDate(h.dt_fim)}</p>
            <div class="flex items-baseline gap-2 mt-2">
                <span class="text-lg font-bold text-white">${h.total_matriculas}</span>
                <span class="text-[10px] text-slate-500">matrículas</span>
            </div>
            <p class="text-xs ${h.tier ? 'text-emerald-400' : 'text-slate-600'}">${h.tier ? tierLabels[h.tier] : 'Sem tier'}</p>
            <p class="text-sm font-bold text-emerald-400 mt-1">${_mpFmt(h.total_premiacao)}</p>
        </div>
    `).join('') || '<p class="text-xs text-slate-600">Nenhuma campanha anterior</p>';
}
