/* ═══════════════  Minha Performance v2 — Redesign  ═══════════════ */

let _mpCharts = {};
let _mpSelectedUid = null;
let _mpMyUid = null;
let _mpIsAdmin = false;
let _mpAgentsLoaded = false;

const _mpFmt  = v => Number(v||0).toLocaleString('pt-BR',{style:'currency',currency:'BRL'});
const _mpFmtN = v => Number(v||0).toLocaleString('pt-BR',{minimumFractionDigits:0,maximumFractionDigits:0});

function _mpFmtDate(d) {
    if (!d) return '';
    const p = String(d).split('-');
    return p.length === 3 ? `${p[2]}/${p[1]}/${p[0]}` : d;
}

function _mpDestroyCharts() {
    Object.values(_mpCharts).forEach(c => { try { c.destroy(); } catch(e) {} });
    _mpCharts = {};
}

function _mpCountUp(elId, endVal, opts = {}) {
    const el = document.getElementById(elId);
    if (!el) return;
    if (typeof countUp !== 'undefined' && countUp.CountUp) {
        const defaults = { duration: 1.8, useGrouping: true, separator: '.', decimal: ',', enableScrollSpy: false };
        const cu = new countUp.CountUp(elId, endVal, { ...defaults, ...opts });
        if (!cu.error) cu.start(); else el.textContent = opts.formattedValue || endVal;
    } else {
        el.textContent = opts.formattedValue || endVal;
    }
}

function _mpSparkline(containerId, data, color) {
    const el = document.getElementById(containerId);
    if (!el || typeof ApexCharts === 'undefined' || !data.length) { if(el) el.innerHTML = ''; return; }
    const chart = new ApexCharts(el, {
        chart: { type: 'area', height: 35, sparkline: { enabled: true }, animations: { enabled: true, easing: 'easeinout', speed: 800 } },
        series: [{ data }],
        stroke: { width: 2, curve: 'smooth' },
        fill: { type: 'gradient', gradient: { opacityFrom: .4, opacityTo: .05 } },
        colors: [color || '#10b981'],
        tooltip: { enabled: false },
    });
    chart.render();
    _mpCharts['spark_' + containerId] = chart;
}

function navigateToPerformance(kommoUid) {
    _mpSelectedUid = kommoUid || null;
    navigate('minha_performance', { uid: kommoUid });
}

/* ── Entry ── */
async function loadMinhaPerformance(params) {
    if (params?.uid) _mpSelectedUid = Number(params.uid);

    _mpDestroyCharts();
    _mpMatLoaded = false;
    _mpCurrentTab = 'performance';

    const loading  = document.getElementById('mp-loading');
    const noLink   = document.getElementById('mp-no-link');
    const noCamp   = document.getElementById('mp-no-campanha');
    const content  = document.getElementById('mp-content');
    const adminBar = document.getElementById('mp-admin-bar');
    const matContent = document.getElementById('mp-mat-content');
    const tabs = document.getElementById('mp-tabs');
    [loading, noLink, noCamp, content, matContent, tabs].forEach(el => { if(el) el.classList.add('hidden'); });
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
        if (tabs) tabs.classList.remove('hidden');

        _mpRenderHero(insights);
        _mpRenderPixDia(insights);
        _mpRenderRanking(insights);
        _mpRenderConquistas(insights);
        _mpRenderDesbloqueie(insights);
        _mpRenderMomentum(insights);
        _mpRenderStreak(insights);
        _mpRenderCalendar(insights);
        _mpRenderTierProgress(insights);
        _mpRenderFinanceiro(insights);
        _mpRenderTimeline(insights);
        _mpRenderHistorico(hist?.historico || []);

    } catch(e) {
        console.error('loadMinhaPerformance', e);
        if (loading) loading.classList.add('hidden');
        if (noCamp) { noCamp.classList.remove('hidden'); noCamp.querySelector('p').textContent = 'Erro ao carregar dados.'; }
    }
}

/* ── Admin ── */
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


/* ═══ S1: Hero + Gauge ═══ */
function _mpRenderHero(d) {
    const hero = document.getElementById('mp-hero');
    const prem = d.premiacao || {};
    const total = prem.total || 0;
    const tier = d.tier;
    const metas = d.metas || {};
    const totalMat = d.total_matriculas || 0;

    if (hero) {
        hero.className = hero.className.replace(/mp-tier-\w+/g, '');
        if (tier === 'supermeta')      hero.classList.add('mp-tier-gold');
        else if (tier === 'meta')      hero.classList.add('mp-tier-silver');
        else if (tier === 'intermediaria') hero.classList.add('mp-tier-bronze');
        else                           hero.classList.add('mp-tier-base');
    }

    const el = id => document.getElementById(id);
    el('mp-hero-campanha').textContent = d.campanha?.nome || '';

    _mpCountUp('mp-hero-saldo', total, { prefix: 'R$ ', decimalPlaces: 2, formattedValue: _mpFmt(total) });

    const maxPotencial = _mpCalcMaxPotencial(d);
    const potWrap = el('mp-hero-potencial-wrap');
    if (maxPotencial > total && potWrap) {
        potWrap.classList.remove('hidden');
        el('mp-hero-potencial').textContent = _mpFmt(maxPotencial);
    } else if (potWrap) {
        potWrap.classList.add('hidden');
    }

    const tierLabels = { base: 'Base', intermediaria: 'Intermediária', meta: 'Meta', supermeta: 'Supermeta' };
    const badge = el('mp-hero-tier-badge');
    badge.textContent = (tierLabels[tier] || tier || 'Base').toUpperCase();
    badge.className = 'px-4 py-1.5 text-xs font-black rounded-full tracking-wide ';
    if (tier === 'supermeta')      badge.className += 'bg-amber-400/25 text-amber-200 border border-amber-400/40';
    else if (tier === 'meta')      badge.className += 'bg-blue-400/25 text-blue-200 border border-blue-400/40';
    else if (tier === 'intermediaria') badge.className += 'bg-orange-400/25 text-orange-200 border border-orange-400/40';
    else                           badge.className += 'bg-emerald-400/20 text-emerald-300 border border-emerald-400/30';

    el('mp-hero-mat').textContent = `${totalMat} matrículas`;
    el('mp-hero-dias').textContent = `${d.dias_restantes || 0} dias restantes`;

    const msgEl = el('mp-hero-msg');
    if (msgEl) msgEl.innerHTML = d.mensagem || '';

    // ApexCharts semi-circle gauge
    const sup = metas.supermeta || metas.meta || metas.intermediaria || 1;
    const pct = Math.min(Math.round((totalMat / sup) * 100), 100);

    const gaugeColors = {
        base:          ['#10b981'],
        intermediaria: ['#f97316'],
        meta:          ['#3b82f6'],
        supermeta:     ['#f59e0b'],
    };

    const gaugeEl = document.getElementById('mp-hero-gauge');
    if (gaugeEl && typeof ApexCharts !== 'undefined') {
        const chart = new ApexCharts(gaugeEl, {
            chart: { type: 'radialBar', height: 220, background: 'transparent', animations: { enabled: true, speed: 1200 } },
            series: [pct],
            colors: gaugeColors[tier] || gaugeColors.base,
            plotOptions: {
                radialBar: {
                    startAngle: -135,
                    endAngle: 135,
                    hollow: { size: '62%', background: 'transparent' },
                    track: { background: 'rgba(255,255,255,.06)', strokeWidth: '100%' },
                    dataLabels: {
                        name: { show: true, fontSize: '11px', color: 'rgba(255,255,255,.5)', offsetY: -12, formatter: () => `${totalMat} / ${sup}` },
                        value: { show: true, fontSize: '32px', fontWeight: 800, color: '#fff', offsetY: 4, formatter: () => `${pct}%` }
                    }
                }
            },
            fill: {
                type: 'gradient',
                gradient: { shade: 'dark', shadeIntensity: .15, gradientToColors: ['#10b981'], stops: [0, 100] }
            },
            stroke: { lineCap: 'round' },
        });
        chart.render();
        _mpCharts.heroGauge = chart;
    }
}

function _mpCalcMaxPotencial(d) {
    const prem = d.premiacao || {};
    const desb = prem.desbloqueie || [];
    if (!desb.length) return prem.total || 0;
    const maxTier = desb.reduce((max, t) => Math.max(max, t.ganho_total), 0);
    return maxTier + (prem.daily_bonus || 0) + (prem.receb_bonus || 0);
}


/* ═══ PIX do Dia ═══ */
function _mpRenderPixDia(d) {
    const hoje = d.hoje || {};
    const meta = hoje.meta || 0;
    const realizadasHoje = hoje.realizadas || 0;
    const aceitesFila = hoje.aceites_fila || 0;
    const aceitesHoje = hoje.aceites_hoje || 0;
    const fixo = hoje.bonus_fixo || 0;
    const extra = hoje.bonus_extra || 0;
    const pct = meta > 0 ? Math.min(Math.round((realizadasHoje / meta) * 100), 100) : 0;
    const metaBatida = realizadasHoje >= meta && meta > 0;

    // ApexCharts radialBar
    const chartEl = document.getElementById('mp-pix-chart');
    if (chartEl && typeof ApexCharts !== 'undefined') {
        const ringColor = metaBatida ? '#10b981' : '#06b6d4';
        const chart = new ApexCharts(chartEl, {
            chart: { type: 'radialBar', height: 175, width: 175, background: 'transparent', animations: { enabled: true, speed: 1000 } },
            series: [pct],
            colors: [ringColor],
            plotOptions: {
                radialBar: {
                    hollow: { size: '58%', background: 'transparent' },
                    track: { background: 'rgba(255,255,255,.06)', strokeWidth: '100%' },
                    dataLabels: {
                        name: { show: true, fontSize: '11px', color: 'rgba(255,255,255,.45)', offsetY: -8, formatter: () => meta > 0 ? `de ${meta}` : 'sem meta' },
                        value: { show: true, fontSize: '36px', fontWeight: 900, color: '#fff', offsetY: 6, formatter: () => `${realizadasHoje}` }
                    }
                }
            },
            fill: {
                type: 'gradient',
                gradient: { shade: 'dark', shadeIntensity: .2, gradientToColors: [metaBatida ? '#34d399' : '#22d3ee'], stops: [0, 100] }
            },
            stroke: { lineCap: 'round' },
        });
        chart.render();
        _mpCharts.pixRing = chart;
    }

    const status = document.getElementById('mp-pix-status');
    const detail = document.getElementById('mp-pix-detail');
    const valor = document.getElementById('mp-pix-valor');
    const aceitesBadge = document.getElementById('mp-aceites-badge');

    if (aceitesBadge) {
        if (aceitesFila > 0) {
            aceitesBadge.classList.remove('hidden');
            aceitesBadge.innerHTML = `
                <span class="material-symbols-outlined text-sm text-purple-400">pending</span>
                <span class="text-[10px] text-purple-300 font-medium">${aceitesFila} aceite${aceitesFila > 1 ? 's' : ''} na fila total</span>
            `;
        } else {
            aceitesBadge.classList.add('hidden');
        }
    }

    const ontemRealizadas = hoje.ontem_realizadas || 0;
    let yesterdayHtml = '';
    if (realizadasHoje > ontemRealizadas && ontemRealizadas >= 0) {
        yesterdayHtml = `<p class="text-[10px] text-emerald-400 font-semibold mt-1">📈 +${realizadasHoje - ontemRealizadas} a mais que ontem — continue assim!</p>`;
    } else if (realizadasHoje === ontemRealizadas && realizadasHoje > 0) {
        yesterdayHtml = `<p class="text-[10px] text-amber-400 font-semibold mt-1">⚡ Mesmo ritmo de ontem — hora de ultrapassar!</p>`;
    } else if (realizadasHoje < ontemRealizadas && ontemRealizadas > 0) {
        yesterdayHtml = `<p class="text-[10px] text-orange-400 font-semibold mt-1">🔥 Ontem você fez ${ontemRealizadas} — bora superar!</p>`;
    }

    if (meta <= 0) {
        if (status) status.textContent = 'Sem meta diária hoje';
        let noMetaMsg = realizadasHoje > 0
            ? `Você já fez ${realizadasHoje} hoje mesmo sem meta!`
            : 'Nenhuma meta PIX configurada para hoje.';
        if (aceitesFila > 0) noMetaMsg += ` (${aceitesFila} aceite${aceitesFila > 1 ? 's' : ''} na fila total)`;
        if (detail) detail.innerHTML = noMetaMsg + yesterdayHtml;
        if (valor) valor.textContent = '';
        return;
    }

    if (metaBatida) {
        const excedente = Math.max(0, realizadasHoje - meta);
        const ganho = fixo + extra * excedente;
        if (status) { status.textContent = '🎉 PIX Garantido!'; status.className = 'text-lg font-black text-emerald-400 mb-1'; }
        let msgParts = `Hoje: ${realizadasHoje} (mat + aceites do dia)`;
        if (excedente > 0) {
            msgParts += ` · +${excedente} extra × ${_mpFmt(extra)} cada`;
        } else {
            msgParts += ' — meta batida!';
        }
        if (detail) detail.innerHTML = msgParts + yesterdayHtml;

        _mpCountUp('mp-pix-valor', ganho, { prefix: 'R$ ', decimalPlaces: 2, duration: 2.2, formattedValue: _mpFmt(ganho) });

        if (typeof confetti === 'function') {
            setTimeout(() => {
                confetti({ particleCount: 80, spread: 70, origin: { y: .7 }, colors: ['#10b981','#34d399','#6ee7b7','#fbbf24','#f59e0b'] });
            }, 600);
        }
    } else {
        const falta = meta - realizadasHoje;
        if (status) { status.textContent = `Faltam ${falta} para o PIX!`; status.className = 'text-lg font-black text-cyan-300 mb-1'; }
        if (detail) detail.innerHTML = `Hoje: ${realizadasHoje}/${meta} (mat + aceites do dia)` + yesterdayHtml;
        if (valor) valor.textContent = `Prêmio: ${_mpFmt(fixo)}`;
    }
}


/* ═══ Ranking ═══ */
function _mpRenderRanking(d) {
    const card = document.getElementById('mp-ranking-card');
    const content = document.getElementById('mp-ranking-content');
    if (!card || !content) return;
    const rk = d.ranking;
    if (!rk || !rk.total_agentes) { card.classList.add('hidden'); return; }
    card.classList.remove('hidden');

    const pos = rk.posicao;
    const total = rk.total_agentes;
    const diff = rk.diferenca_lider;
    const myMat = rk.minhas_mat || 0;
    const myAce = rk.meus_aceites || 0;
    const myTotal = rk.meu_total || (myMat + myAce);
    const media = rk.media_time || 0;

    const medalCfg = {
        1: { icon: 'emoji_events', gradient: 'from-amber-500/30 to-amber-900/10', border: 'border-amber-400/50', iconColor: 'text-amber-400', label: '🏆 Você lidera o ranking!', labelColor: 'text-amber-400' },
        2: { icon: 'workspace_premium', gradient: 'from-slate-300/20 to-slate-700/10', border: 'border-slate-300/40', iconColor: 'text-slate-200', label: '🥈 Vice-líder!', labelColor: 'text-slate-300' },
        3: { icon: 'workspace_premium', gradient: 'from-orange-500/25 to-orange-900/10', border: 'border-orange-400/40', iconColor: 'text-orange-400', label: '🥉 Top 3! Pódio!', labelColor: 'text-orange-400' },
    };
    const m = medalCfg[pos];

    let motivacao = '';
    if (pos === 1) motivacao = '<p class="text-xs text-amber-300/80 mt-2">Ninguém te alcançou! Continue dominando! 🔥</p>';
    else if (diff > 0 && diff <= 3) motivacao = `<p class="text-xs text-cyan-400 font-semibold mt-2">🔥 Quase lá! Só <strong>${diff}</strong> para o topo!</p>`;
    else if (diff > 0 && diff <= 10) motivacao = `<p class="text-xs text-slate-400 mt-2">Faltam <strong>${diff}</strong> para o 1°. Cada matrícula conta! 💪</p>`;
    else if (diff > 0) motivacao = `<p class="text-xs text-slate-500 mt-2">${diff} atrás do líder — foco e consistência! 🚀</p>`;

    const scoreDetail = `<p class="text-[10px] text-slate-500 mt-1">${myMat} mat${myAce > 0 ? ' + ' + myAce + ' aceite' + (myAce > 1 ? 's' : '') : ''}</p>`;

    let mediaHtml = '';
    if (media > 0) {
        const diffMedia = myTotal - media;
        const absDiff = Math.abs(diffMedia).toFixed(1);
        if (diffMedia > 1) {
            mediaHtml = `
                <div class="mt-3 p-2.5 rounded-lg bg-emerald-500/10 border border-emerald-500/15">
                    <p class="text-[10px] text-slate-400">Média do time: <strong class="text-white">${media}</strong></p>
                    <p class="text-xs text-emerald-400 font-semibold mt-0.5">📈 Você está ${absDiff} acima da média! Continue assim!</p>
                </div>`;
        } else if (diffMedia >= -1) {
            mediaHtml = `
                <div class="mt-3 p-2.5 rounded-lg bg-amber-500/10 border border-amber-500/15">
                    <p class="text-[10px] text-slate-400">Média do time: <strong class="text-white">${media}</strong></p>
                    <p class="text-xs text-amber-400 font-semibold mt-0.5">⚡ Você está na média do time — dá pra mais!</p>
                </div>`;
        } else {
            mediaHtml = `
                <div class="mt-3 p-2.5 rounded-lg bg-orange-500/10 border border-orange-500/15">
                    <p class="text-[10px] text-slate-400">Média do time: <strong class="text-white">${media}</strong></p>
                    <p class="text-xs text-orange-400 font-semibold mt-0.5">🔥 Você está ${absDiff} abaixo da média — bora reverter esse jogo!</p>
                </div>`;
        }
    }

    content.innerHTML = `
        <div class="flex items-center gap-5">
            <div class="w-20 h-20 rounded-2xl flex items-center justify-center border-2 bg-gradient-to-br ${m ? m.gradient + ' ' + m.border : 'from-slate-700/40 to-slate-800/40 border-slate-600/30'} shadow-lg">
                ${m ? `<span class="material-symbols-outlined text-4xl ${m.iconColor}">${m.icon}</span>`
                    : `<span class="text-3xl font-black text-slate-300">${pos}°</span>`}
            </div>
            <div class="flex-1">
                <p class="text-3xl font-black text-white mp-stat-value">${pos}°</p>
                <p class="text-sm text-slate-500">de ${total}</p>
                ${scoreDetail}
                ${m ? `<p class="text-xs font-bold ${m.labelColor} mt-1">${m.label}</p>` : ''}
                ${motivacao}
            </div>
        </div>
        ${mediaHtml}`;
}


/* ═══ Conquistas ═══ */
function _mpRenderConquistas(d) {
    const card = document.getElementById('mp-conquistas-card');
    const grid = document.getElementById('mp-conquistas-grid');
    if (!card || !grid) return;

    const achieved = d.conquistas || [];
    if (!achieved.length) {
        card.classList.remove('hidden');
        grid.innerHTML = `
            <div class="w-full text-center py-6">
                <span class="material-symbols-outlined text-4xl text-emerald-600/40 mb-2">rocket_launch</span>
                <p class="text-sm text-slate-400 font-semibold">Suas conquistas aparecem aqui! 🚀</p>
                <p class="text-[10px] text-slate-600 mt-1">Faça matrículas, bata metas e suba no ranking.</p>
            </div>`;
        return;
    }
    card.classList.remove('hidden');

    const colorMap = {
        primeira_mat:   { bg: '#065f46', border: '#10b981', icon: '#6ee7b7', glow: '#10b981' },
        streak_3:       { bg: '#78350f', border: '#f59e0b', icon: '#fcd34d', glow: '#f59e0b' },
        streak_5:       { bg: '#7c2d12', border: '#f97316', icon: '#fdba74', glow: '#f97316' },
        streak_7:       { bg: '#7f1d1d', border: '#ef4444', icon: '#fca5a5', glow: '#ef4444' },
        meta_batida:    { bg: '#1e3a5f', border: '#3b82f6', icon: '#93c5fd', glow: '#3b82f6' },
        supermeta:      { bg: '#713f12', border: '#eab308', icon: '#fef08a', glow: '#eab308' },
        meta_antecipada:{ bg: '#164e63', border: '#06b6d4', icon: '#67e8f9', glow: '#06b6d4' },
        melhor_dia:     { bg: '#831843', border: '#ec4899', icon: '#f9a8d4', glow: '#ec4899' },
        top_3:          { bg: '#451a03', border: '#d97706', icon: '#fbbf24', glow: '#d97706' },
    };
    const defaultColor = { bg: '#4a1d96', border: '#a855f7', icon: '#d8b4fe', glow: '#a855f7' };

    grid.innerHTML = achieved.map((a, i) => {
        const c = colorMap[a.id] || defaultColor;
        return `<div class="flex flex-col items-center gap-2 p-3 rounded-xl w-[88px] border-2 shadow-lg transition-all hover:scale-110 cursor-default mp-enter" style="background:${c.bg};border-color:${c.border};box-shadow:0 0 18px ${c.glow}44,0 4px 12px rgba(0,0,0,.3);animation-delay:${i*.08}s" title="${a.desc || a.nome}">
            <span class="material-symbols-outlined text-3xl" style="color:${c.icon};filter:drop-shadow(0 0 8px ${c.glow})">${a.icone}</span>
            <span class="text-[9px] text-center leading-tight font-bold" style="color:${c.icon}">${a.nome}</span>
        </div>`;
    }).join('');
}


/* ═══ Desbloqueie Mais ═══ */
function _mpRenderDesbloqueie(d) {
    const wrap = document.getElementById('mp-desbloqueie');
    const wrapOuter = document.getElementById('mp-desbloqueie-wrap');
    if (!wrap) return;
    const desb = d.premiacao?.desbloqueie || [];
    if (!desb.length || desb.every(t => t.atingido)) { if (wrapOuter) wrapOuter.classList.add('hidden'); return; }
    if (wrapOuter) wrapOuter.classList.remove('hidden');

    const tierLabels = { intermediaria: 'Intermediária', meta: 'Meta', supermeta: 'Supermeta' };
    const tierColors = {
        intermediaria: { border: 'border-orange-500/25', text: 'text-orange-400', glow: 'shadow-orange-500/10' },
        meta:          { border: 'border-blue-500/25', text: 'text-blue-400', glow: 'shadow-blue-500/10' },
        supermeta:     { border: 'border-amber-500/25', text: 'text-amber-400', glow: 'shadow-amber-500/10' },
    };
    const proximoNaoAtingido = desb.find(t => !t.atingido);

    wrap.innerHTML = desb.filter(t => !t.atingido).map(t => {
        const c = tierColors[t.tier] || tierColors.meta;
        const isProximo = t === proximoNaoAtingido;
        return `<div class="mp-card p-4 ${c.border} ${isProximo ? 'mp-pulse' : ''} ${c.glow} relative overflow-hidden">
            ${isProximo ? '<div class="absolute top-0 right-0 px-2 py-0.5 text-[9px] font-bold bg-emerald-500/20 text-emerald-400 rounded-bl-lg">PRÓXIMO</div>' : ''}
            <p class="text-[10px] ${c.text} uppercase font-bold tracking-wider mb-1">${tierLabels[t.tier]}</p>
            <p class="text-2xl font-black text-white mb-1">+${_mpFmt(t.ganho_adicional)}</p>
            <p class="text-xs text-slate-400">Faltam <span class="font-bold text-white">${t.falta}</span> matrículas</p>
            <p class="text-[10px] text-slate-600 mt-1">${_mpFmt(t.valor_por_mat)}/mat · Total: ${_mpFmt(t.ganho_total)}</p>
        </div>`;
    }).join('');
}


/* ═══ Momentum + Sparklines ═══ */
function _mpRenderMomentum(d) {
    const el = id => document.getElementById(id);

    _mpCountUp('mp-pace', d.pace_atual || 0, { decimalPlaces: 1, formattedValue: (d.pace_atual||0).toFixed(1) });
    _mpCountUp('mp-pace-needed', d.pace_meta > 900 ? 0 : (d.pace_meta || 0), {
        decimalPlaces: 1,
        formattedValue: d.pace_meta > 900 ? '--' : (d.pace_meta||0).toFixed(1)
    });
    _mpCountUp('mp-projecao-mat', d.projecao || 0, { suffix: ' mat', formattedValue: `${d.projecao||0} mat` });

    const projTier = el('mp-projecao-tier');
    const tierLabels = { base: 'Base', intermediaria: 'Intermediária', meta: 'Meta', supermeta: 'Supermeta' };
    if (d.projecao_tier && d.projecao_tier !== 'base') {
        projTier.textContent = tierLabels[d.projecao_tier] || d.projecao_tier;
        projTier.className = 'text-[10px] text-emerald-400 font-semibold';
    } else {
        projTier.textContent = 'Abaixo da meta';
        projTier.className = 'text-[10px] text-red-400';
    }

    _mpCountUp('mp-projecao-fin', d.projecao_financeira || 0, { prefix: 'R$ ', decimalPlaces: 2, formattedValue: _mpFmt(d.projecao_financeira || 0) });

    // Sparklines from heatmap data
    const heatmap = (d.heatmap || []).filter(h => h.status !== 'future' && h.realizadas != null);
    const last7 = heatmap.slice(-7);
    const sparkData = last7.map(h => h.realizadas || 0);
    const metaData = last7.map(h => h.meta || 0);

    if (sparkData.length >= 2) {
        _mpSparkline('mp-spark-pace', sparkData, '#10b981');
        _mpSparkline('mp-spark-needed', metaData, '#f59e0b');

        let accum = [];
        let sum = 0;
        sparkData.forEach(v => { sum += v; accum.push(sum); });
        _mpSparkline('mp-spark-proj', accum, '#3b82f6');

        const finData = last7.map(h => {
            const bd = (d.premiacao?.daily_breakdown || []).find(b => b.data === h.data);
            return bd ? bd.total : 0;
        });
        _mpSparkline('mp-spark-fin', finData, '#10b981');
    }
}


/* ═══ Streak + Heatmap ═══ */
function _mpRenderStreak(d) {
    const el = id => document.getElementById(id);
    const seq = d.sequencia || 0;
    const nivel = d.streak_nivel;
    const streakNum = el('mp-streak-num');

    if (streakNum) {
        _mpCountUp('mp-streak-num', seq, { formattedValue: String(seq) });
        if (nivel === 'imparavel')   streakNum.className = 'text-4xl font-black text-transparent bg-clip-text bg-gradient-to-r from-purple-400 to-pink-400 mp-stat-value';
        else if (nivel === 'em_chamas') streakNum.className = 'text-4xl font-black text-transparent bg-clip-text bg-gradient-to-r from-orange-400 to-red-500 mp-stat-value';
        else if (nivel === 'aquecendo') streakNum.className = 'text-4xl font-black text-amber-400 mp-stat-value';
        else streakNum.className = 'text-4xl font-black text-slate-400 mp-stat-value';
    }

    const nivelLabels = { aquecendo: 'Aquecendo!', em_chamas: 'Em Chamas!', imparavel: 'IMPARÁVEL!' };
    const nivelIcons  = { aquecendo: 'local_fire_department', em_chamas: 'whatshot', imparavel: 'bolt' };

    const streakLabel = el('mp-streak-label');
    let labelText = seq > 0
        ? `${seq} dia${seq > 1 ? 's' : ''} consecutivo${seq > 1 ? 's' : ''} batendo a meta diária`
        : 'Inicie sua sequência hoje! (mat + aceites contam)';
    if (nivel) labelText += ` — ${nivelLabels[nivel]}`;
    if (streakLabel) streakLabel.textContent = labelText;

    const nivelWrap = document.getElementById('mp-streak-nivel');
    if (nivelWrap) {
        if (nivel) {
            nivelWrap.classList.remove('hidden');
            const nColor = nivel === 'imparavel' ? 'text-purple-400' : nivel === 'em_chamas' ? 'text-orange-400' : 'text-amber-400';
            nivelWrap.innerHTML = `
                <span class="material-symbols-outlined text-sm ${nColor}">${nivelIcons[nivel]}</span>
                <span class="text-[10px] font-bold ${nColor}">${nivelLabels[nivel]}</span>`;
        } else {
            nivelWrap.classList.add('hidden');
        }
    }

    const heatmap = d.heatmap || [];
    const wrap = document.getElementById('mp-heatmap');
    if (!wrap || !heatmap.length) return;

    const breakdown = (d.premiacao || {}).daily_breakdown || [];
    const breakdownMap = {};
    breakdown.forEach(b => { breakdownMap[b.data] = b; });

    wrap.innerHTML = heatmap.map(h => {
        const cls = `mp-heat-${h.status}`;
        const bd = breakdownMap[h.data];
        const mat = h.mat || 0;
        const ace = h.aceites || 0;
        let detail = `${h.realizadas||0}`;
        if (h.meta) detail += `/${h.meta}`;
        if (ace > 0) detail += ` (${mat}m+${ace}a)`;
        const tooltip = h.status === 'future' ? 'Futuro'
            : `${_mpFmtDate(h.data)}: ${detail}${bd ? ' · ' + _mpFmt(bd.total) : ''}`;
        return `<div class="${cls} w-4 h-4 rounded-sm cursor-default transition-transform hover:scale-150" title="${tooltip}"></div>`;
    }).join('');
}


/* ═══ Calendário de Resultados ═══ */
function _mpRenderCalendar(d) {
    const wrap = document.getElementById('mp-calendar');
    if (!wrap) return;
    const heatmap = d.heatmap || [];
    if (!heatmap.length) { wrap.innerHTML = '<p class="text-xs text-slate-600">Sem dados</p>'; return; }

    const breakdown = (d.premiacao?.daily_breakdown || []);
    const bdMap = {};
    breakdown.forEach(b => { bdMap[b.data] = b; });
    const hmMap = {};
    heatmap.forEach(h => { hmMap[h.data] = h; });

    const dtIni = d.campanha?.dt_inicio;
    const dtFim = d.campanha?.dt_fim;
    if (!dtIni || !dtFim) { wrap.innerHTML = ''; return; }

    const todayStr = new Date().toLocaleDateString('sv-SE');
    const startMonth = new Date(new Date(dtIni + 'T00:00:00').getFullYear(), new Date(dtIni + 'T00:00:00').getMonth(), 1);
    const endMonth   = new Date(new Date(dtFim + 'T00:00:00').getFullYear(), new Date(dtFim + 'T00:00:00').getMonth() + 1, 0);
    const dayNames = ['Seg','Ter','Qua','Qui','Sex','Sáb','Dom'];
    const monthNames = ['Janeiro','Fevereiro','Março','Abril','Maio','Junho','Julho','Agosto','Setembro','Outubro','Novembro','Dezembro'];

    let months = [];
    let cur = new Date(startMonth);
    while (cur <= endMonth) { months.push({ year: cur.getFullYear(), month: cur.getMonth() }); cur.setMonth(cur.getMonth() + 1); }

    const sc = {
        hit:     { bg: '#064e3b', bg2: '#065f46', border: '#10b981', text: '#6ee7b7', glow: '0 0 16px rgba(16,185,129,.25)' },
        partial: { bg: '#78350f', bg2: '#92400e', border: '#f59e0b', text: '#fcd34d', glow: 'none' },
        miss:    { bg: '#450a0a', bg2: '#7f1d1d', border: '#ef4444', text: '#fca5a5', glow: 'none' },
        rest:    { bg: '#0f172a', bg2: '#1e293b', border: '#334155', text: '#475569', glow: 'none' },
        future:  { bg: '#0f172a', bg2: '#1e293b', border: '#1e293b', text: '#334155', glow: 'none' },
    };

    const html = months.map(({ year, month }) => {
        const lastDay = new Date(year, month + 1, 0).getDate();
        let startDow = new Date(year, month, 1).getDay() - 1;
        if (startDow < 0) startDow = 6;

        let cells = '';
        for (let i = 0; i < startDow; i++) cells += '<div class="h-16"></div>';

        for (let day = 1; day <= lastDay; day++) {
            const dateStr = `${year}-${String(month+1).padStart(2,'0')}-${String(day).padStart(2,'0')}`;
            const h = hmMap[dateStr];
            const bd = bdMap[dateStr];
            const isToday = dateStr === todayStr;
            const inRange = dateStr >= dtIni && dateStr <= dtFim;

            if (!inRange) {
                cells += `<div class="h-16 rounded-lg flex items-center justify-center opacity-15"><span class="text-[10px] text-slate-700">${day}</span></div>`;
                continue;
            }

            const status = h?.status || 'future';
            const c = sc[status] || sc.future;
            const realizadas = h?.realizadas;
            const matCount = h?.mat ?? 0;
            const aceCount = h?.aceites ?? 0;
            const meta = h?.meta ?? 0;
            const bonus = bd ? bd.total : 0;
            const pct = (meta > 0 && realizadas != null) ? Math.min(100, Math.round((realizadas / meta) * 100)) : 0;

            const tipData = JSON.stringify({ dateStr, status, matCount, aceCount, realizadas: realizadas ?? 0, meta, bonus, pct, isToday }).replace(/"/g, '&quot;');

            let ratioHtml = '';
            let statusIcon = '';
            if (status !== 'future' && status !== 'rest' && realizadas != null) {
                if (meta > 0) ratioHtml = `<span class="text-[10px] font-bold opacity-80" style="color:${c.text}">${realizadas}/${meta}</span>`;
                else if (realizadas > 0) ratioHtml = `<span class="text-[10px] font-bold opacity-80" style="color:${c.text}">${realizadas}</span>`;
                statusIcon = status === 'hit' ? '✅' : status === 'partial' ? '⚡' : status === 'miss' ? '❌' : '';
            }

            let miniBar = '';
            if (meta > 0 && status !== 'future') {
                miniBar = `<div class="w-full h-[3px] rounded-full mt-auto" style="background:${c.border}20">
                    <div class="h-full rounded-full" style="width:${pct}%;background:${c.border}"></div>
                </div>`;
            }

            const todayCls = isToday ? `ring-2 ring-cyan-400 shadow-lg shadow-cyan-500/20` : '';

            cells += `<div class="mp-cal-cell h-16 rounded-lg flex flex-col items-center justify-center gap-0.5 cursor-pointer relative transition-all duration-150 hover:brightness-125 hover:scale-105 hover:z-20 ${todayCls} overflow-hidden"
                style="background:linear-gradient(145deg,${c.bg},${c.bg2});border:1px solid ${c.border}40;box-shadow:${c.glow}" data-tip="${tipData}">
                ${isToday ? '<span class="absolute top-0.5 right-0.5 w-1.5 h-1.5 rounded-full bg-cyan-400 animate-pulse"></span>' : ''}
                <span class="text-sm font-black leading-none" style="color:${c.text}">${day}</span>
                ${ratioHtml}
                ${statusIcon ? `<span class="text-[8px] leading-none">${statusIcon}</span>` : ''}
                ${miniBar}
            </div>`;
        }

        return `<div class="mb-5 last:mb-0">
            <p class="text-sm font-bold text-slate-200 mb-3 flex items-center gap-2">
                <span class="material-symbols-outlined text-base text-indigo-400">date_range</span>
                ${monthNames[month]} ${year}
            </p>
            <div class="grid grid-cols-7 gap-2">
                ${dayNames.map(dn => `<div class="text-center text-[10px] text-slate-500 font-bold pb-2 uppercase tracking-widest">${dn}</div>`).join('')}
                ${cells}
            </div>
        </div>`;
    }).join('');

    const legend = `<div class="flex flex-wrap items-center justify-center gap-5 mt-4 pt-3 border-t border-slate-700/20 text-[10px]">
        <span class="flex items-center gap-1.5"><span class="w-3 h-3 rounded" style="background:#065f46;border:1px solid #10b981"></span><span class="text-emerald-400 font-medium">Bateu ✅</span></span>
        <span class="flex items-center gap-1.5"><span class="w-3 h-3 rounded" style="background:#92400e;border:1px solid #f59e0b"></span><span class="text-amber-400 font-medium">Parcial ⚡</span></span>
        <span class="flex items-center gap-1.5"><span class="w-3 h-3 rounded" style="background:#7f1d1d;border:1px solid #ef4444"></span><span class="text-red-400 font-medium">Não bateu ❌</span></span>
        <span class="flex items-center gap-1.5"><span class="w-3 h-3 rounded" style="background:#1e293b;border:1px solid #334155"></span><span class="text-slate-500 font-medium">Futuro</span></span>
        <span class="flex items-center gap-1.5"><span class="w-3 h-3 rounded ring-2 ring-cyan-400" style="background:#1e293b"></span><span class="text-cyan-400 font-medium">Hoje</span></span>
    </div>`;

    wrap.innerHTML = html + legend;
    wrap.style.position = 'relative';

    let tipEl = document.getElementById('mp-cal-tip');
    if (tipEl) tipEl.remove();
    tipEl = document.createElement('div');
    tipEl.id = 'mp-cal-tip';
    tipEl.className = 'fixed z-[9999] pointer-events-none transition-all duration-150';
    tipEl.style.cssText = 'opacity:0;transform:translateY(4px) scale(.96);min-width:220px;max-width:280px;';
    document.body.appendChild(tipEl);

    _mpCalendarTooltips(wrap, tipEl);
}

function _mpCalendarTooltips(wrap, tip) {
    const sm = {
        hit:     { icon: 'emoji_events', color: '#6ee7b7', accent: '#10b981', label: 'Meta batida! 🎉' },
        partial: { icon: 'trending_up',  color: '#fcd34d', accent: '#f59e0b', label: 'Quase lá! 💪' },
        miss:    { icon: 'trending_down', color: '#fca5a5', accent: '#ef4444', label: 'Não bateu 😤' },
        rest:    { icon: 'bedtime',       color: '#64748b', accent: '#475569', label: 'Dia de descanso' },
        future:  { icon: 'schedule',      color: '#64748b', accent: '#475569', label: 'Futuro' },
    };
    const dayOfWeek = ['Dom','Seg','Ter','Qua','Qui','Sex','Sáb'];

    wrap.querySelectorAll('.mp-cal-cell').forEach(cell => {
        cell.addEventListener('mouseenter', () => {
            const raw = cell.getAttribute('data-tip');
            if (!raw) return;
            let data;
            try { data = JSON.parse(raw); } catch { return; }
            const s = sm[data.status] || sm.future;
            const dt = new Date(data.dateStr + 'T00:00:00');
            const dateFmt = `${dayOfWeek[dt.getDay()]}, ${String(dt.getDate()).padStart(2,'0')}/${String(dt.getMonth()+1).padStart(2,'0')}`;

            let barHtml = '';
            if (data.meta > 0) {
                barHtml = `<div class="mt-2">
                    <div class="flex justify-between text-[10px] mb-1">
                        <span class="text-slate-400">Progresso</span>
                        <span class="font-black" style="color:${s.color}">${data.pct}%</span>
                    </div>
                    <div class="h-2 rounded-full overflow-hidden" style="background:${s.accent}20">
                        <div class="h-full rounded-full transition-all" style="width:${data.pct}%;background:linear-gradient(90deg,${s.accent},${s.color})"></div>
                    </div>
                </div>`;
            }

            let rows = '';
            if (data.status !== 'future') {
                const mkRow = (label, val, color) => `<div class="flex justify-between items-center py-0.5"><span class="text-slate-500 text-[10px]">${label}</span><span class="font-bold text-[11px]" style="color:${color}">${val}</span></div>`;
                if (data.matCount > 0 || data.aceCount > 0) rows += mkRow('📋 Matrículas', data.matCount, '#e2e8f0');
                if (data.aceCount > 0) rows += mkRow('🤝 Aceites', data.aceCount, '#c084fc');
                if (data.meta > 0) rows += mkRow('🎯 Meta', data.meta, '#94a3b8');
                if (data.bonus > 0) rows += `<div class="flex justify-between items-center py-1 mt-1 border-t border-slate-700/40"><span class="text-slate-400 text-[10px]">💰 Bônus do dia</span><span class="font-black text-xs text-emerald-400">${_mpFmt(data.bonus)}</span></div>`;
            }

            tip.innerHTML = `
                <div class="rounded-2xl overflow-hidden shadow-2xl" style="border:1px solid ${s.accent}30">
                    <div class="px-4 py-2.5 flex items-center gap-2" style="background:linear-gradient(135deg,${s.accent}25,${s.accent}10)">
                        <span class="material-symbols-outlined text-lg" style="color:${s.color}">${s.icon}</span>
                        <div class="flex-1">
                            <p class="text-xs font-bold text-white">${dateFmt}</p>
                            <p class="text-[10px] font-semibold" style="color:${s.color}">${s.label}</p>
                        </div>
                        ${data.isToday ? '<span class="text-[9px] font-black text-cyan-400 bg-cyan-400/15 px-2 py-0.5 rounded-full tracking-wide">HOJE</span>' : ''}
                    </div>
                    <div class="px-4 py-3" style="background:#0c1222">
                        ${barHtml}
                        <div class="mt-1.5">${rows || '<p class="text-slate-600 text-[10px]">Sem atividade</p>'}</div>
                    </div>
                    <div class="h-[3px]" style="background:linear-gradient(90deg,${s.accent},${s.color})"></div>
                </div>`;

            const rect = cell.getBoundingClientRect();
            const tipW = 250;
            tip.style.width = tipW + 'px';

            let left = rect.left + rect.width / 2 - tipW / 2;
            if (left < 8) left = 8;
            if (left + tipW > window.innerWidth - 8) left = window.innerWidth - tipW - 8;

            const tipH = 180;
            let top = rect.top - tipH - 10;
            if (top < 8) top = rect.bottom + 10;

            tip.style.left = left + 'px';
            tip.style.top = top + 'px';
            tip.style.opacity = '1';
            tip.style.transform = 'translateY(0) scale(1)';
        });

        cell.addEventListener('mouseleave', () => {
            tip.style.opacity = '0';
            tip.style.transform = 'translateY(4px) scale(.96)';
        });
    });
}


/* ═══ Progresso por Faixa ═══ */
function _mpRenderTierProgress(d) {
    const wrap = document.getElementById('mp-tier-progress');
    if (!wrap) return;
    const progress = d.tier_progress || [];
    if (!progress.length) { wrap.innerHTML = ''; return; }

    const tierColors = {
        base:          { bar: '#64748b', text: 'text-slate-400' },
        intermediaria: { bar: '#f97316', text: 'text-orange-400' },
        meta:          { bar: '#3b82f6', text: 'text-blue-400' },
        supermeta:     { bar: '#f59e0b', text: 'text-amber-400' },
    };
    const tierLabels = { base: 'Base', intermediaria: 'Intermediária', meta: 'Meta', supermeta: 'Supermeta' };
    const totalMat = d.total_matriculas || 0;

    wrap.innerHTML = progress.filter(p => p.tier !== 'base' || p.valor_por_mat > 0).map(p => {
        const c = tierColors[p.tier] || tierColors.base;
        const label = tierLabels[p.tier] || p.tier;
        const pct = Math.min(p.pct || 0, 100);
        const falta = Math.max(0, (p.target || 0) - totalMat);
        const ganhoExtra = p.valor_por_mat > 0 ? _mpFmt(p.ganho) : '';

        return `<div class="flex items-center gap-3">
            <div class="w-24 flex-shrink-0">
                <span class="text-xs font-semibold ${c.text}">${label}</span>
                ${p.target > 0 ? `<span class="text-[10px] text-slate-600 ml-1">(${p.target})</span>` : ''}
            </div>
            <div class="flex-1">
                <div class="bg-slate-700/30 rounded-full h-4 overflow-hidden relative">
                    <div class="h-full rounded-full transition-all duration-1000 flex items-center justify-end pr-1.5" style="width:${pct}%;background:${c.bar};box-shadow:0 0 12px ${c.bar}33">
                        ${pct >= 18 ? `<span class="text-[9px] font-bold text-white/90">${totalMat}/${p.target||'∞'}</span>` : ''}
                    </div>
                    ${pct < 18 && p.target > 0 ? `<span class="absolute left-2 top-0 h-full flex items-center text-[9px] text-slate-400">${totalMat}/${p.target}</span>` : ''}
                </div>
            </div>
            <div class="w-28 text-right flex-shrink-0">
                ${p.atingido
                    ? `<span class="text-[10px] font-bold text-emerald-400">✅ ${ganhoExtra}</span>`
                    : (falta > 0 ? `<span class="text-[10px] text-slate-500">falta ${falta}${ganhoExtra ? ' · +'+ganhoExtra : ''}</span>` : '')}
            </div>
        </div>`;
    }).join('');
}


/* ═══ Resumo Financeiro ═══ */
function _mpRenderFinanceiro(d) {
    const wrap = document.getElementById('mp-financeiro');
    if (!wrap) return;
    const prem = d.premiacao || {};
    const uni = d.unificado;
    const items = [
        { label: 'Bônus Tier',    value: prem.tier_bonus || 0, color: '#f59e0b' },
        { label: 'PIX Diários',   value: prem.daily_bonus || 0, color: '#06b6d4' },
        { label: 'Recebimentos',  value: prem.receb_bonus || 0, color: '#8b5cf6' },
    ];
    const total = prem.total || 0;
    const maxVal = Math.max(...items.map(i => i.value), 1);

    let uniBadge = '';
    if (uni) {
        uniBadge = `
        <div class="mb-3 p-3 rounded-xl bg-pink-500/8 border border-pink-500/15">
            <div class="flex items-center gap-2 mb-1">
                <span class="material-symbols-outlined text-pink-400 text-sm">link</span>
                <span class="text-xs font-bold text-pink-400">Campanhas Unificadas</span>
            </div>
            <p class="text-[10px] text-slate-400">${(uni.campanhas||[]).join(' + ')}</p>
            <p class="text-[10px] text-emerald-400 mt-1">+${_mpFmt(uni.ganho_extra)} a mais vs. individual!</p>
        </div>`;
    }

    wrap.innerHTML = uniBadge + items.map(i => {
        const pct = Math.round((i.value / maxVal) * 100);
        return `<div class="flex items-center gap-3">
            <span class="text-xs text-slate-400 w-24 flex-shrink-0">${i.label}</span>
            <div class="flex-1 bg-slate-700/30 rounded-full h-3 overflow-hidden">
                <div class="h-full rounded-full transition-all duration-1000" style="width:${pct}%;background:${i.color};box-shadow:0 0 8px ${i.color}33"></div>
            </div>
            <span class="text-xs font-bold text-white w-24 text-right">${_mpFmt(i.value)}</span>
        </div>`;
    }).join('') + `
        <div class="flex items-center justify-between pt-3 border-t border-slate-700/20">
            <span class="text-sm font-bold text-emerald-400">TOTAL</span>
            <span class="text-2xl font-black text-emerald-400 mp-stat-value" id="mp-fin-total">${_mpFmt(total)}</span>
        </div>`;

    _mpCountUp('mp-fin-total', total, { prefix: 'R$ ', decimalPlaces: 2, duration: 2, formattedValue: _mpFmt(total) });
}


/* ═══ Timeline — ApexCharts ═══ */
function _mpRenderTimeline(d) {
    const container = document.getElementById('mp-timeline-chart');
    if (!container || typeof ApexCharts === 'undefined') return;

    const breakdown = d.premiacao?.daily_breakdown || [];
    if (!breakdown.length) { container.innerHTML = ''; return; }

    const labels = breakdown.map(b => _mpFmtDate(b.data));
    const matData = breakdown.map(b => b.realizadas);
    const metaData = breakdown.map(b => b.meta);
    const bonusData = breakdown.map(b => b.total);

    const chart = new ApexCharts(container, {
        chart: {
            type: 'bar',
            height: 280,
            background: 'transparent',
            toolbar: { show: false },
            animations: { enabled: true, speed: 800, dynamicAnimation: { enabled: true } },
            fontFamily: 'Inter, sans-serif',
        },
        theme: { mode: 'dark' },
        series: [
            { name: 'Matrículas', type: 'bar', data: matData },
            { name: 'Meta', type: 'line', data: metaData },
            { name: 'Bônus R$', type: 'line', data: bonusData },
        ],
        colors: ['#10b981', '#f59e0b', '#06b6d4'],
        plotOptions: {
            bar: { borderRadius: 4, columnWidth: '55%' }
        },
        stroke: {
            width: [0, 2, 2],
            curve: 'smooth',
            dashArray: [0, 6, 0],
        },
        fill: {
            type: ['solid', 'solid', 'gradient'],
            gradient: { type: 'vertical', shadeIntensity: .3, opacityFrom: .7, opacityTo: .2 }
        },
        labels,
        xaxis: {
            labels: { style: { colors: '#64748b', fontSize: '9px' }, rotate: -45, rotateAlways: labels.length > 10 },
        },
        yaxis: [
            { labels: { style: { colors: '#64748b', fontSize: '9px' } }, title: { text: undefined } },
            { show: false },
            { opposite: true, labels: { style: { colors: '#06b6d4', fontSize: '9px' }, formatter: v => 'R$' + Math.round(v) }, title: { text: undefined } },
        ],
        grid: { borderColor: 'rgba(148,163,184,.08)', xaxis: { lines: { show: false } } },
        dataLabels: { enabled: false },
        legend: { labels: { colors: '#94a3b8' }, fontSize: '10px' },
        tooltip: {
            theme: 'dark',
            y: { formatter: (v, { seriesIndex }) => seriesIndex === 2 ? _mpFmt(v) : v }
        },
    });
    chart.render();
    _mpCharts.timeline = chart;
}


/* ═══ Histórico ═══ */
function _mpRenderHistorico(hist) {
    const wrap = document.getElementById('mp-historico');
    const wrapOuter = document.getElementById('mp-historico-wrap');
    if (!wrap) return;
    if (!hist.length) { if (wrapOuter) wrapOuter.classList.add('hidden'); return; }
    if (wrapOuter) wrapOuter.classList.remove('hidden');

    const tierLabels = { intermediaria: 'Intermediária', meta: 'Meta', supermeta: 'Supermeta' };
    const tierBorders = {
        intermediaria: 'border-orange-500/25',
        meta: 'border-blue-500/25',
        supermeta: 'border-amber-500/25',
    };

    wrap.innerHTML = hist.filter(h => !h.ativa).map(h => {
        const border = tierBorders[h.tier] || 'border-slate-700/30';
        return `<div class="mp-card p-4 min-w-[210px] flex-shrink-0 ${border} snap-start">
            <p class="text-xs font-semibold text-white mb-1">${h.nome}</p>
            <p class="text-[10px] text-slate-500">${_mpFmtDate(h.dt_inicio)} — ${_mpFmtDate(h.dt_fim)}</p>
            <div class="flex items-baseline gap-2 mt-2">
                <span class="text-lg font-bold text-white">${h.total_matriculas}</span>
                <span class="text-[10px] text-slate-500">matrículas</span>
            </div>
            <p class="text-xs ${h.tier ? 'text-emerald-400 font-semibold' : 'text-slate-600'}">${h.tier ? tierLabels[h.tier] : 'Sem tier'}</p>
            <p class="text-sm font-bold text-emerald-400 mt-1">${_mpFmt(h.total_premiacao)}</p>
        </div>`;
    }).join('') || '<p class="text-xs text-slate-600">Nenhuma campanha anterior</p>';
}


/* ═══════════════════════════════════════════════════════════════════════
   Sub-abas: Performance / Minhas Matrículas
   ═══════════════════════════════════════════════════════════════════════ */

let _mpCurrentTab = 'performance';
let _mpMatLoaded = false;

function _mpSwitchTab(tab) {
    _mpCurrentTab = tab;
    const perf = document.getElementById('mp-content');
    const mat  = document.getElementById('mp-mat-content');
    const tabP = document.getElementById('mp-tab-performance');
    const tabM = document.getElementById('mp-tab-matriculas');
    if (!perf || !mat) return;

    const activeClasses   = 'border-emerald-500 text-emerald-400';
    const inactiveClasses = 'border-transparent text-slate-500 hover:text-slate-300';

    if (tab === 'performance') {
        perf.classList.remove('hidden');
        mat.classList.add('hidden');
        tabP.className = tabP.className.replace(inactiveClasses, '').replace(activeClasses, '') ;
        tabP.classList.add(...activeClasses.split(' '));
        tabM.className = tabM.className.replace(activeClasses, '').replace(inactiveClasses, '');
        tabM.classList.add(...inactiveClasses.split(' '));
    } else {
        perf.classList.add('hidden');
        mat.classList.remove('hidden');
        tabM.className = tabM.className.replace(inactiveClasses, '').replace(activeClasses, '');
        tabM.classList.add(...activeClasses.split(' '));
        tabP.className = tabP.className.replace(activeClasses, '').replace(inactiveClasses, '');
        tabP.classList.add(...inactiveClasses.split(' '));
        if (!_mpMatLoaded) {
            _mpMatLoaded = true;
            const now = new Date();
            const y = now.getFullYear();
            const m = String(now.getMonth() + 1).padStart(2, '0');
            const iniEl = document.getElementById('mp-mat-dt-ini');
            const fimEl = document.getElementById('mp-mat-dt-fim');
            if (iniEl && !iniEl.value) iniEl.value = `${y}-${m}-01`;
            if (fimEl && !fimEl.value) {
                const last = new Date(y, now.getMonth() + 1, 0).getDate();
                fimEl.value = `${y}-${m}-${String(last).padStart(2, '0')}`;
            }
            _mpLoadMatriculas();
            _mpLoadMinhasMatriculas();
            _mpLoadAjustes();
        }
    }
}


/* ═══ Matrículas Oficiais ═══ */

let _mpOficialData = [];

async function _mpLoadMatriculas() {
    const uid = (_mpIsAdmin && _mpSelectedUid) ? _mpSelectedUid : _mpMyUid;
    if (!uid) return;
    const tbody = document.getElementById('mp-mat-oficial-tbody');
    const countEl = document.getElementById('mp-mat-oficial-count');
    if (tbody) tbody.innerHTML = '<tr><td colspan="6" class="py-6 text-center text-slate-600 text-xs">Carregando...</td></tr>';
    try {
        let qs = `kommo_uid=${uid}`;
        const dtIni = document.getElementById('mp-mat-dt-ini')?.value;
        const dtFim = document.getElementById('mp-mat-dt-fim')?.value;
        if (dtIni) qs += `&dt_ini=${dtIni}`;
        if (dtFim) qs += `&dt_fim=${dtFim}`;
        const res = await api(`/api/minha-performance/matriculas?${qs}`);
        const d = await res.json();
        _mpOficialData = d.matriculas || [];
        const emCurso = _mpOficialData.filter(m => (m.situacao||'').toUpperCase() === 'EM CURSO').length;
        const cancel = _mpOficialData.filter(m => {
            const s = (m.situacao||'').toUpperCase();
            return s.includes('CANCEL') || s.includes('EVAD') || s.includes('DESIST');
        }).length;
        const outros = _mpOficialData.length - emCurso - cancel;
        let summary = `${_mpOficialData.length} total`;
        if (emCurso) summary += ` · <span class="text-emerald-400">${emCurso} em curso</span>`;
        if (cancel) summary += ` · <span class="text-red-400">${cancel} cancelado${cancel > 1 ? 's' : ''}</span>`;
        if (outros) summary += ` · <span class="text-amber-400">${outros} outro${outros > 1 ? 's' : ''}</span>`;
        if (countEl) countEl.innerHTML = summary;
        _mpRenderOficialTable(_mpOficialData);
    } catch(e) {
        if (tbody) tbody.innerHTML = '<tr><td colspan="6" class="py-6 text-center text-red-400 text-xs">Erro ao carregar</td></tr>';
    }
}

function _mpRenderOficialTable(mats) {
    const tbody = document.getElementById('mp-mat-oficial-tbody');
    if (!tbody) return;
    if (!mats.length) {
        tbody.innerHTML = '<tr><td colspan="6" class="py-6 text-center text-slate-600 text-xs">Nenhuma matrícula encontrada</td></tr>';
        return;
    }
    tbody.innerHTML = mats.map(m => {
        const sit = (m.situacao || '').toUpperCase();
        const isEvadido = sit.includes('EVAD') || sit.includes('CANCEL') || sit.includes('DESIST');
        const badge = sit
            ? (isEvadido
                ? `<span class="px-2 py-0.5 rounded-full text-[10px] font-bold bg-red-500/15 text-red-400 border border-red-500/20">${sit}</span>`
                : `<span class="px-2 py-0.5 rounded-full text-[10px] font-bold bg-emerald-500/15 text-emerald-400 border border-emerald-500/20">${sit}</span>`)
            : '<span class="text-slate-600">—</span>';
        return `<tr class="border-b border-slate-800/50 mp-oficial-row hover:bg-slate-800/30 transition-colors"
                    data-search="${(m.nome||'').toLowerCase()} ${(m.rgm||'').toLowerCase()} ${(m.curso||'').toLowerCase()}">
            <td class="py-1.5 px-2 text-slate-300">${m.nome||'—'}</td>
            <td class="py-1.5 px-2 text-slate-400 font-mono">${m.rgm||'—'}</td>
            <td class="py-1.5 px-2 text-slate-400">${m.curso || m.nivel || '—'}</td>
            <td class="py-1.5 px-2 text-slate-400">${m.polo||'—'}</td>
            <td class="py-1.5 px-2 text-slate-400">${_mpFmtDate(m.data_matricula)}</td>
            <td class="py-1.5 px-2">${badge}</td>
        </tr>`;
    }).join('');
}

function _mpFilterOficial() {
    const q = (document.getElementById('mp-mat-oficial-search')?.value || '').toLowerCase();
    document.querySelectorAll('.mp-oficial-row').forEach(row => {
        row.style.display = !q || row.dataset.search.includes(q) ? '' : 'none';
    });
}


/* ═══ Minha Lista (CRUD) ═══ */

let _mpMinhasData = [];

async function _mpLoadMinhasMatriculas() {
    const uid = (_mpIsAdmin && _mpSelectedUid) ? _mpSelectedUid : _mpMyUid;
    const tbody = document.getElementById('mp-minha-lista-tbody');
    if (tbody) tbody.innerHTML = '<tr><td colspan="7" class="py-6 text-center text-slate-600 text-xs">Carregando...</td></tr>';
    try {
        const qs = uid ? `?kommo_uid=${uid}` : '';
        const res = await api(`/api/minha-performance/minhas-matriculas${qs}`);
        const d = await res.json();
        _mpMinhasData = d.matriculas || [];
        _mpRenderMinhaLista();
    } catch(e) {
        if (tbody) tbody.innerHTML = '<tr><td colspan="7" class="py-6 text-center text-red-400 text-xs">Erro ao carregar</td></tr>';
    }
}

function _mpRenderMinhaLista() {
    const tbody = document.getElementById('mp-minha-lista-tbody');
    if (!tbody) return;
    if (!_mpMinhasData.length) {
        tbody.innerHTML = '<tr><td colspan="7" class="py-6 text-center text-slate-600 text-xs">Nenhuma matrícula cadastrada. Clique em "Adicionar" acima.</td></tr>';
        return;
    }
    tbody.innerHTML = _mpMinhasData.map(m => `<tr class="border-b border-slate-800/50 hover:bg-slate-800/30 transition-colors">
        <td class="py-1.5 px-2 text-slate-300">${m.nome||'—'}</td>
        <td class="py-1.5 px-2 text-slate-400 font-mono">${m.rgm||'—'}</td>
        <td class="py-1.5 px-2 text-slate-400">${m.curso||'—'}</td>
        <td class="py-1.5 px-2 text-slate-400">${m.polo||'—'}</td>
        <td class="py-1.5 px-2 text-slate-400">${_mpFmtDate(m.data_matricula)}</td>
        <td class="py-1.5 px-2 text-slate-400 font-mono">${m.kommo_lead_id||'—'}</td>
        <td class="py-1.5 px-2">
            <div class="flex items-center gap-1">
                <button onclick="_mpEditMinhaMatricula(${m.id})" class="text-blue-400 hover:text-blue-300 transition-colors" title="Editar">
                    <span class="material-symbols-outlined text-sm">edit</span>
                </button>
                <button onclick="_mpDeleteMinhaMatricula(${m.id})" class="text-red-400 hover:text-red-300 transition-colors" title="Excluir">
                    <span class="material-symbols-outlined text-sm">delete</span>
                </button>
            </div>
        </td>
    </tr>`).join('');
}

function _mpOpenMinhaMatModal(data = null) {
    const modal = document.getElementById('mp-modal-minha-mat');
    const title = document.getElementById('mp-minha-mat-title');
    if (!modal) return;
    document.getElementById('mp-minha-mat-id').value = data ? data.id : '';
    document.getElementById('mp-mm-nome').value = data?.nome || '';
    document.getElementById('mp-mm-rgm').value = data?.rgm || '';
    document.getElementById('mp-mm-curso').value = data?.curso || '';
    document.getElementById('mp-mm-polo').value = data?.polo || '';
    document.getElementById('mp-mm-data').value = data?.data_matricula ? String(data.data_matricula).substring(0,10) : '';
    document.getElementById('mp-mm-ciclo').value = data?.ciclo || '';
    document.getElementById('mp-mm-nivel').value = data?.nivel || '';
    document.getElementById('mp-mm-kommo').value = data?.kommo_lead_id || '';
    document.getElementById('mp-mm-obs').value = data?.observacao || '';
    if (title) title.textContent = data ? 'Editar Matrícula' : 'Adicionar Matrícula';
    modal.classList.remove('hidden');
    modal.classList.add('flex');
}

function _mpEditMinhaMatricula(id) {
    const m = _mpMinhasData.find(x => x.id === id);
    if (m) _mpOpenMinhaMatModal(m);
}

async function _mpSaveMinhaMatricula() {
    const id = document.getElementById('mp-minha-mat-id').value;
    const body = {
        nome: document.getElementById('mp-mm-nome').value,
        rgm: document.getElementById('mp-mm-rgm').value,
        curso: document.getElementById('mp-mm-curso').value,
        polo: document.getElementById('mp-mm-polo').value,
        data_matricula: document.getElementById('mp-mm-data').value || null,
        ciclo: document.getElementById('mp-mm-ciclo').value,
        nivel: document.getElementById('mp-mm-nivel').value,
        kommo_lead_id: document.getElementById('mp-mm-kommo').value,
        observacao: document.getElementById('mp-mm-obs').value,
    };
    try {
        const method = id ? 'PUT' : 'POST';
        const url = id ? `/api/minha-performance/minhas-matriculas/${id}` : '/api/minha-performance/minhas-matriculas';
        await api(url, { method, headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body) });
        document.getElementById('mp-modal-minha-mat').classList.add('hidden');
        document.getElementById('mp-modal-minha-mat').classList.remove('flex');
        _mpLoadMinhasMatriculas();
    } catch(e) {
        alert('Erro ao salvar: ' + e.message);
    }
}

async function _mpDeleteMinhaMatricula(id) {
    if (!confirm('Excluir esta matrícula da sua lista?')) return;
    try {
        await api(`/api/minha-performance/minhas-matriculas/${id}`, { method: 'DELETE' });
        _mpLoadMinhasMatriculas();
    } catch(e) {
        alert('Erro ao excluir: ' + e.message);
    }
}


/* ═══ Solicitações de Ajuste (Agente) ═══ */

let _mpAjustesData = [];

async function _mpLoadAjustes() {
    const uid = (_mpIsAdmin && _mpSelectedUid) ? _mpSelectedUid : _mpMyUid;
    const list = document.getElementById('mp-ajustes-list');
    if (list) list.innerHTML = '<p class="text-xs text-slate-600 py-4 text-center">Carregando...</p>';
    try {
        const qs = uid ? `?kommo_uid=${uid}` : '';
        const res = await api(`/api/minha-performance/ajustes${qs}`);
        const d = await res.json();
        _mpAjustesData = d.ajustes || [];
        _mpRenderAjustesList();
    } catch(e) {
        if (list) list.innerHTML = '<p class="text-xs text-red-400 py-4 text-center">Erro ao carregar</p>';
    }
}

const _mpAjTipoLabel = { matricula_nao_computada: 'Matrícula não computada', dados_incorretos: 'Dados incorretos', evasao_indevida: 'Evasão indevida' };
const _mpAjStatusColor = {
    pendente: 'bg-amber-500/15 text-amber-400 border-amber-500/20',
    em_analise: 'bg-blue-500/15 text-blue-400 border-blue-500/20',
    aprovado: 'bg-emerald-500/15 text-emerald-400 border-emerald-500/20',
    rejeitado: 'bg-red-500/15 text-red-400 border-red-500/20',
};
const _mpAjStatusLabel = { pendente: 'Pendente', em_analise: 'Em análise', aprovado: 'Aprovado', rejeitado: 'Rejeitado' };

function _mpRenderAjustesList() {
    const list = document.getElementById('mp-ajustes-list');
    if (!list) return;
    if (!_mpAjustesData.length) {
        list.innerHTML = '<p class="text-xs text-slate-600 py-4 text-center">Nenhuma solicitação enviada.</p>';
        return;
    }
    list.innerHTML = _mpAjustesData.map(a => {
        const sc = _mpAjStatusColor[a.status] || _mpAjStatusColor.pendente;
        return `<div class="border border-slate-700/30 rounded-lg p-3 mb-2 hover:bg-slate-800/20 transition-colors">
            <div class="flex flex-wrap items-center gap-2 mb-1">
                <span class="px-2 py-0.5 rounded-full text-[10px] font-bold border ${sc}">${_mpAjStatusLabel[a.status] || a.status}</span>
                <span class="text-[10px] text-slate-500">${_mpAjTipoLabel[a.tipo] || a.tipo}</span>
                <span class="text-[10px] text-slate-600 ml-auto">${_mpFmtDate(String(a.created_at).substring(0,10))}</span>
            </div>
            <p class="text-xs text-slate-300"><strong>${a.nome_aluno || '—'}</strong> — RGM: ${a.rgm || '—'} — Lead: ${a.kommo_lead_id || '—'}</p>
            <p class="text-[10px] text-slate-500 mt-1">${a.descricao || ''}</p>
            ${a.resposta_admin ? `<div class="mt-2 px-3 py-2 rounded-lg bg-slate-800/50 border border-slate-700/30">
                <p class="text-[10px] text-slate-400"><span class="font-semibold text-slate-300">Resposta:</span> ${a.resposta_admin}</p>
            </div>` : ''}
        </div>`;
    }).join('');
}

function _mpOpenAjusteModal() {
    const modal = document.getElementById('mp-modal-ajuste');
    if (!modal) return;
    ['mp-aj-nome','mp-aj-rgm','mp-aj-curso','mp-aj-polo','mp-aj-data','mp-aj-kommo','mp-aj-desc'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.value = '';
    });
    const tipo = document.getElementById('mp-aj-tipo');
    if (tipo) tipo.value = 'matricula_nao_computada';
    modal.classList.remove('hidden');
    modal.classList.add('flex');
}

async function _mpSaveAjuste() {
    const kommoId = document.getElementById('mp-aj-kommo')?.value?.trim();
    const desc = document.getElementById('mp-aj-desc')?.value?.trim();
    if (!kommoId) { alert('Lead Kommo ID é obrigatório'); return; }
    if (!desc) { alert('Justificativa é obrigatória'); return; }
    const body = {
        tipo: document.getElementById('mp-aj-tipo')?.value,
        nome_aluno: document.getElementById('mp-aj-nome')?.value,
        rgm: document.getElementById('mp-aj-rgm')?.value,
        curso: document.getElementById('mp-aj-curso')?.value,
        polo: document.getElementById('mp-aj-polo')?.value,
        data_matricula: document.getElementById('mp-aj-data')?.value || null,
        kommo_lead_id: kommoId,
        descricao: desc,
    };
    try {
        await api('/api/minha-performance/ajustes', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(body) });
        document.getElementById('mp-modal-ajuste').classList.add('hidden');
        document.getElementById('mp-modal-ajuste').classList.remove('flex');
        _mpLoadAjustes();
    } catch(e) {
        alert('Erro ao enviar: ' + e.message);
    }
}


/* ═══ Filtro rápido por período de Meta ═══ */

let _mpMetaPeriodosCache = null;
let _mpMetaDropdownOpen = false;

async function _mpToggleMetaPeriodos() {
    const dropdown = document.getElementById('mp-meta-periodo-dropdown');
    const chevron = document.getElementById('mp-meta-periodo-chevron');
    if (!dropdown) return;

    _mpMetaDropdownOpen = !_mpMetaDropdownOpen;
    if (!_mpMetaDropdownOpen) {
        dropdown.classList.add('hidden');
        if (chevron) chevron.textContent = 'expand_more';
        return;
    }

    dropdown.classList.remove('hidden');
    if (chevron) chevron.textContent = 'expand_less';

    if (_mpMetaPeriodosCache && _mpMetaPeriodosCache.length) {
        _mpRenderMetaPeriodos(_mpMetaPeriodosCache);
        return;
    }

    const listEl = document.getElementById('mp-meta-periodo-list');
    if (listEl) listEl.innerHTML = '<div class="py-2">Carregando...</div>';

    try {
        const res = await api('/api/premiacao/campanhas-periodos');
        const d = await res.json();
        if (!d.ok) throw new Error(d.error || 'Erro');
        _mpMetaPeriodosCache = d.campanhas || [];
        _mpRenderMetaPeriodos(_mpMetaPeriodosCache);
    } catch(e) {
        if (listEl) listEl.innerHTML = `<div class="py-2 text-red-400">Erro: ${e.message}</div>`;
    }
}

function _mpRenderMetaPeriodos(campanhas) {
    const listEl = document.getElementById('mp-meta-periodo-list');
    if (!listEl) return;
    if (!campanhas.length) {
        listEl.innerHTML = '<div class="py-2 text-slate-500">Nenhuma meta cadastrada</div>';
        return;
    }
    listEl.innerHTML = campanhas.map(c => {
        const ini = c.dt_inicio;
        const fim = c.dt_fim;
        const label = c.nome || `${_mpFmtDate(ini)} → ${_mpFmtDate(fim)}`;
        const sub = `${_mpFmtDate(ini)} → ${_mpFmtDate(fim)}`;
        return `<button onclick="_mpAplicarMetaPeriodo('${ini}','${fim}')"
            class="w-full text-left px-3 py-2 hover:bg-slate-800 transition-colors border-b border-slate-800/50 last:border-0">
            <div class="font-semibold text-slate-200">${label}</div>
            <div class="text-[10px] text-slate-500 mt-0.5">${sub}</div>
        </button>`;
    }).join('');
}

function _mpAplicarMetaPeriodo(dtIni, dtFim) {
    const iniEl = document.getElementById('mp-mat-dt-ini');
    const fimEl = document.getElementById('mp-mat-dt-fim');
    if (iniEl) iniEl.value = dtIni;
    if (fimEl) fimEl.value = dtFim;

    const dropdown = document.getElementById('mp-meta-periodo-dropdown');
    const chevron = document.getElementById('mp-meta-periodo-chevron');
    if (dropdown) dropdown.classList.add('hidden');
    if (chevron) chevron.textContent = 'expand_more';
    _mpMetaDropdownOpen = false;

    _mpLoadMatriculas();
}

// Fecha dropdown ao clicar fora
document.addEventListener('click', function(e) {
    const wrap = document.getElementById('mp-meta-periodo-wrap');
    if (wrap && !wrap.contains(e.target) && _mpMetaDropdownOpen) {
        document.getElementById('mp-meta-periodo-dropdown')?.classList.add('hidden');
        const chevron = document.getElementById('mp-meta-periodo-chevron');
        if (chevron) chevron.textContent = 'expand_more';
        _mpMetaDropdownOpen = false;
    }
});
