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
        _mpRenderRanking(insights);
        _mpRenderConquistas(insights);
        _mpRenderDesbloqueie(insights);
        _mpRenderPixDia(insights);
        _mpRenderMomentum(insights);
        _mpRenderStreak(insights);
        _mpRenderTierProgress(insights);
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
        else hero.classList.add('mp-tier-base');
    }

    const el = id => document.getElementById(id);
    el('mp-hero-campanha').textContent = d.campanha?.nome || '';
    el('mp-hero-saldo').textContent = _mpFmt(total);

    const maxPotencial = _mpCalcMaxPotencial(d);
    el('mp-hero-potencial').textContent = maxPotencial > total ? _mpFmt(maxPotencial) : '';

    const tierLabels = { base: 'Base', intermediaria: 'Intermediária', meta: 'Meta', supermeta: 'Supermeta' };
    const badge = el('mp-hero-tier-badge');
    badge.textContent = tierLabels[tier] || tier || 'Base';
    badge.className = 'px-3 py-1 text-xs font-bold rounded-full ';
    if (tier === 'supermeta') badge.className += 'bg-amber-500/30 text-amber-300';
    else if (tier === 'meta') badge.className += 'bg-slate-400/20 text-slate-300';
    else if (tier === 'intermediaria') badge.className += 'bg-orange-500/20 text-orange-300';
    else badge.className += 'bg-slate-600/30 text-slate-400';

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
    const top = rk.top || [];

    const medalCfg = {
        1: { icon: 'trophy', color: 'text-amber-400', bg: 'bg-amber-500/15 border-amber-500/30', ring: 'ring-amber-500/40' },
        2: { icon: 'workspace_premium', color: 'text-slate-300', bg: 'bg-slate-400/10 border-slate-400/20', ring: 'ring-slate-400/30' },
        3: { icon: 'workspace_premium', color: 'text-orange-400', bg: 'bg-orange-500/10 border-orange-500/20', ring: 'ring-orange-500/30' },
    };

    const myMedal = medalCfg[pos];
    const heroHtml = `
        <div class="flex items-center gap-4 mb-4 pb-4 border-b border-slate-700/30">
            <div class="w-16 h-16 rounded-2xl flex items-center justify-center border-2 ${myMedal ? myMedal.bg + ' ' + myMedal.ring : 'bg-slate-700/50 border-slate-600/30'} ring-2 ${myMedal ? myMedal.ring : 'ring-slate-700/30'}">
                ${myMedal
                    ? `<span class="material-symbols-outlined text-3xl ${myMedal.color}">${myMedal.icon}</span>`
                    : `<span class="text-2xl font-black text-slate-400">${pos}°</span>`}
            </div>
            <div>
                <p class="text-2xl font-black text-white">${pos}° <span class="text-sm font-normal text-slate-500">de ${total}</span></p>
                ${pos === 1
                    ? '<p class="text-xs text-amber-400 font-bold">Você lidera o ranking!</p>'
                    : `<p class="text-xs text-slate-400">${diff} atrás do 1° lugar</p>`}
            </div>
        </div>`;

    const listHtml = top.map((t, i) => {
        const p = i + 1;
        const mc = medalCfg[p];
        const isMe = t.uid === (_mpSelectedUid || _mpMyUid);
        const nameParts = (t.nome || '').split(' ');
        const shortName = nameParts[0] || t.nome;
        return `<div class="flex items-center gap-2.5 py-1.5 ${isMe ? 'bg-cyan-500/5 -mx-2 px-2 rounded-lg border border-cyan-500/20' : ''}">
            <div class="w-7 h-7 rounded-lg flex items-center justify-center flex-shrink-0 ${mc ? mc.bg + ' border' : 'bg-slate-800/50'}">
                ${mc ? `<span class="material-symbols-outlined text-sm ${mc.color}">${mc.icon}</span>` : `<span class="text-[10px] font-bold text-slate-500">${p}°</span>`}
            </div>
            <span class="text-xs ${isMe ? 'text-cyan-300 font-bold' : 'text-slate-300'} flex-1 truncate">${isMe ? 'Você' : shortName}</span>
            <span class="text-xs font-bold ${isMe ? 'text-cyan-400' : 'text-white'}">${t.total}</span>
            ${t.aceites > 0 ? `<span class="text-[9px] px-1.5 py-0.5 rounded-full bg-purple-500/20 text-purple-400 font-medium">${t.aceites} ac.</span>` : ''}
        </div>`;
    }).join('');

    content.innerHTML = heroHtml + `<div class="space-y-0.5">${listHtml}</div>`;
}

/* ═══ Conquistas ═══ */
function _mpRenderConquistas(d) {
    const card = document.getElementById('mp-conquistas-card');
    const grid = document.getElementById('mp-conquistas-grid');
    if (!card || !grid) return;

    const achieved = d.conquistas || [];
    const allPossible = [
        { id: 'primeira_mat', nome: 'Primeira Matrícula', icone: 'school' },
        { id: 'streak_3', nome: '3 Dias Seguidos', icone: 'local_fire_department' },
        { id: 'streak_5', nome: '5 Dias Seguidos', icone: 'whatshot' },
        { id: 'streak_7', nome: 'Imparável', icone: 'bolt' },
        { id: 'meta_batida', nome: 'Meta Batida', icone: 'emoji_events' },
        { id: 'supermeta', nome: 'Supermeta', icone: 'military_tech' },
        { id: 'meta_antecipada', nome: 'Meta Antecipada', icone: 'schedule' },
        { id: 'melhor_dia', nome: 'Super Dia', icone: 'star' },
        { id: 'top_1', nome: 'Top 1 (Ouro)', icone: 'workspace_premium' },
        { id: 'top_2', nome: 'Top 2 (Prata)', icone: 'workspace_premium' },
        { id: 'top_3', nome: 'Top 3 (Bronze)', icone: 'workspace_premium' },
    ];
    const achievedIds = new Set(achieved.map(a => a.id));
    if (!achieved.length && !allPossible.length) { card.classList.add('hidden'); return; }
    card.classList.remove('hidden');

    grid.innerHTML = allPossible.map(a => {
        const unlocked = achievedIds.has(a.id);
        const real = achieved.find(x => x.id === a.id);
        const desc = real?.desc || a.nome;
        if (unlocked) {
            return `<div class="flex flex-col items-center gap-1.5 p-3 rounded-xl w-20 bg-gradient-to-b from-purple-500/20 to-purple-900/10 border border-purple-500/30 shadow-lg shadow-purple-500/5" title="${desc}">
                <span class="material-symbols-outlined text-2xl text-purple-300 drop-shadow-[0_0_6px_rgba(168,85,247,0.4)]">${real?.icone || a.icone}</span>
                <span class="text-[9px] text-center leading-tight text-purple-200 font-semibold">${real?.nome || a.nome}</span>
            </div>`;
        }
        return `<div class="flex flex-col items-center gap-1 p-2.5 rounded-xl w-20 bg-slate-800/20 opacity-30" title="${desc}">
            <span class="material-symbols-outlined text-xl text-slate-600">${a.icone}</span>
            <span class="text-[9px] text-center leading-tight text-slate-600">${a.nome}</span>
        </div>`;
    }).join('');
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
    const aceitesFila = hoje.aceites_fila || 0;
    const aceitesHoje = hoje.aceites_hoje || 0;
    const fixo = hoje.bonus_fixo || 0;
    const extra = hoje.bonus_extra || 0;

    const efetivo = feitas + aceitesHoje;
    const pct = meta > 0 ? Math.min(efetivo / meta, 1) : 0;
    const circum = 314.16;
    const offset = circum * (1 - pct);

    const ringFill = document.getElementById('mp-ring-fill');
    const ringValue = document.getElementById('mp-ring-value');
    const ringLabel = document.getElementById('mp-ring-label');
    if (ringFill) {
        ringFill.setAttribute('stroke-dashoffset', offset);
        ringFill.setAttribute('stroke', efetivo >= meta && meta > 0 ? '#10b981' : '#3b82f6');
    }
    if (ringValue) ringValue.textContent = efetivo;
    if (ringLabel) ringLabel.textContent = meta > 0 ? `de ${meta}` : 'sem meta';

    const status = document.getElementById('mp-pix-status');
    const detail = document.getElementById('mp-pix-detail');
    const valor = document.getElementById('mp-pix-valor');
    const aceitesBadge = document.getElementById('mp-aceites-badge');

    if (aceitesBadge) {
        if (aceitesFila > 0 || aceitesHoje > 0) {
            aceitesBadge.classList.remove('hidden');
            aceitesBadge.innerHTML = `
                <span class="material-symbols-outlined text-sm text-purple-400">pending</span>
                <span class="text-[10px] text-purple-300 font-medium">${aceitesHoje > 0 ? aceitesHoje + ' aceite' + (aceitesHoje > 1 ? 's' : '') + ' hoje' : ''}${aceitesHoje > 0 && aceitesFila > aceitesHoje ? ' · ' : ''}${aceitesFila > aceitesHoje ? aceitesFila + ' na fila total' : ''}</span>
            `;
        } else {
            aceitesBadge.classList.add('hidden');
        }
    }

    if (meta <= 0) {
        if (status) status.textContent = 'Sem meta diária hoje';
        if (detail) detail.textContent = aceitesFila > 0
            ? `Nenhuma meta PIX configurada, mas você tem ${aceitesFila} aceite${aceitesFila > 1 ? 's' : ''} na fila!`
            : 'Nenhuma meta PIX configurada para hoje.';
        if (valor) valor.textContent = '';
        return;
    }

    if (efetivo >= meta) {
        const ganho = fixo + extra * Math.max(0, efetivo - meta);
        if (status) { status.textContent = 'PIX Garantido!'; status.className = 'text-base font-bold text-emerald-400 mb-1'; }
        const parts = [];
        if (feitas > 0) parts.push(`${feitas} matrícula${feitas > 1 ? 's' : ''}`);
        if (aceitesHoje > 0) parts.push(`${aceitesHoje} aceite${aceitesHoje > 1 ? 's' : ''}`);
        if (detail) detail.textContent = efetivo > meta
            ? `Meta batida! ${parts.join(' + ')} · +${efetivo - meta} extra × ${_mpFmt(extra)} cada`
            : `Parabéns! ${parts.join(' + ')} = meta batida!`;
        if (valor) valor.textContent = _mpFmt(ganho);
    } else {
        const falta = meta - efetivo;
        if (status) { status.textContent = `Faltam ${falta} para o PIX!`; status.className = 'text-base font-bold text-cyan-400 mb-1'; }
        const aceiteTip = aceitesFila > aceitesHoje
            ? ` (${aceitesFila - aceitesHoje} aceite${aceitesFila - aceitesHoje > 1 ? 's' : ''} pendente${aceitesFila - aceitesHoje > 1 ? 's' : ''} podem virar matrícula!)` : '';
        if (detail) detail.textContent = `${feitas} matrícula${feitas !== 1 ? 's' : ''} + ${aceitesHoje} aceite${aceitesHoje !== 1 ? 's' : ''} = ${efetivo}/${meta}${aceiteTip}`;
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
    const tierLabels = { base: 'Base', intermediaria: 'Intermediária', meta: 'Meta', supermeta: 'Supermeta' };
    if (d.projecao_tier && d.projecao_tier !== 'base') {
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
    const seq = d.sequencia || 0;
    const nivel = d.streak_nivel;
    const streakNum = el('mp-streak-num');
    const streakLabel = el('mp-streak-label');

    if (streakNum) {
        streakNum.textContent = seq;
        if (nivel === 'imparavel') streakNum.className = 'text-3xl font-black text-transparent bg-clip-text bg-gradient-to-r from-purple-400 to-pink-400 mp-glow';
        else if (nivel === 'em_chamas') streakNum.className = 'text-3xl font-black text-transparent bg-clip-text bg-gradient-to-r from-orange-400 to-red-500';
        else if (nivel === 'aquecendo') streakNum.className = 'text-3xl font-black text-amber-400';
        else streakNum.className = 'text-3xl font-black text-slate-400';
    }

    const nivelLabels = { aquecendo: 'Aquecendo!', em_chamas: 'Em Chamas!', imparavel: 'IMPARÁVEL!' };
    const nivelIcons = { aquecendo: 'local_fire_department', em_chamas: 'whatshot', imparavel: 'bolt' };

    let labelText = seq > 0
        ? `${seq} dia${seq > 1 ? 's' : ''} consecutivo${seq > 1 ? 's' : ''}`
        : 'Inicie sua sequência hoje!';
    if (nivel) labelText += ` — ${nivelLabels[nivel]}`;
    if (streakLabel) streakLabel.textContent = labelText;

    const nivelWrap = document.getElementById('mp-streak-nivel');
    if (nivelWrap) {
        if (nivel) {
            nivelWrap.classList.remove('hidden');
            nivelWrap.innerHTML = `
                <span class="material-symbols-outlined text-sm ${nivel === 'imparavel' ? 'text-purple-400' : nivel === 'em_chamas' ? 'text-orange-400' : 'text-amber-400'}">${nivelIcons[nivel]}</span>
                <span class="text-[10px] font-bold ${nivel === 'imparavel' ? 'text-purple-400' : nivel === 'em_chamas' ? 'text-orange-400' : 'text-amber-400'}">${nivelLabels[nivel]}</span>
            `;
        } else {
            nivelWrap.classList.add('hidden');
        }
    }

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

/* ═══ Progresso por Faixa ═══ */
function _mpRenderTierProgress(d) {
    const wrap = document.getElementById('mp-tier-progress');
    if (!wrap) return;
    const progress = d.tier_progress || [];
    if (!progress.length) { wrap.innerHTML = ''; return; }

    const tierColors = {
        base: { bg: 'bg-slate-500', text: 'text-slate-400', border: 'border-slate-500/20' },
        intermediaria: { bg: 'bg-orange-500', text: 'text-orange-400', border: 'border-orange-500/20' },
        meta: { bg: 'bg-blue-500', text: 'text-blue-400', border: 'border-blue-500/20' },
        supermeta: { bg: 'bg-amber-500', text: 'text-amber-400', border: 'border-amber-500/20' },
    };
    const tierLabels = { base: 'Base', intermediaria: 'Intermediária', meta: 'Meta', supermeta: 'Supermeta' };
    const totalMat = d.total_matriculas || 0;

    wrap.innerHTML = progress.filter(p => p.tier !== 'base' || p.valor_por_mat > 0).map(p => {
        const c = tierColors[p.tier] || tierColors.base;
        const label = tierLabels[p.tier] || p.tier;
        const pct = p.pct || 0;
        const falta = Math.max(0, (p.target || 0) - totalMat);
        const ganhoExtra = p.valor_por_mat > 0 ? _mpFmt(p.ganho) : '';

        return `<div class="flex items-center gap-3">
            <div class="w-24 flex-shrink-0">
                <span class="text-xs font-semibold ${c.text}">${label}</span>
                ${p.target > 0 ? `<span class="text-[10px] text-slate-600 ml-1">(${p.target})</span>` : ''}
            </div>
            <div class="flex-1">
                <div class="bg-slate-700/30 rounded-full h-4 overflow-hidden relative">
                    <div class="${c.bg} h-full rounded-full transition-all duration-700 flex items-center justify-end pr-1" style="width:${pct}%">
                        ${pct >= 15 ? `<span class="text-[9px] font-bold text-white/80">${totalMat}/${p.target || '∞'}</span>` : ''}
                    </div>
                    ${pct < 15 && p.target > 0 ? `<span class="absolute left-1 top-0 h-full flex items-center text-[9px] text-slate-400">${totalMat}/${p.target}</span>` : ''}
                </div>
            </div>
            <div class="w-28 text-right flex-shrink-0">
                ${p.atingido
                    ? `<span class="text-[10px] font-bold text-emerald-400">${ganhoExtra}</span>`
                    : (falta > 0
                        ? `<span class="text-[10px] text-slate-500">falta ${falta} · ${ganhoExtra ? `+${ganhoExtra}` : ''}</span>`
                        : '')}
            </div>
        </div>`;
    }).join('');
}

/* ═══ S6: Resumo Financeiro ═══ */
function _mpRenderFinanceiro(d) {
    const wrap = document.getElementById('mp-financeiro');
    if (!wrap) return;
    const prem = d.premiacao || {};
    const uni = d.unificado;
    const items = [
        { label: 'Bônus Tier', value: prem.tier_bonus || 0, color: 'bg-amber-500' },
        { label: 'PIX Diários', value: prem.daily_bonus || 0, color: 'bg-cyan-500' },
        { label: 'Recebimentos', value: prem.receb_bonus || 0, color: 'bg-violet-500' },
    ];
    const total = prem.total || 0;
    const maxVal = Math.max(...items.map(i => i.value), 1);

    let uniBadge = '';
    if (uni) {
        uniBadge = `
        <div class="mb-3 p-2.5 rounded-lg bg-pink-500/10 border border-pink-500/20">
            <div class="flex items-center gap-2 mb-1">
                <span class="material-symbols-outlined text-pink-400 text-base">link</span>
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
