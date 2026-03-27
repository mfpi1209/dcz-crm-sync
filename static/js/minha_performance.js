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
    const metas = d.metas || {};
    const totalMat = d.total_matriculas || 0;

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
    if (tier === 'supermeta') badge.className += 'bg-amber-500/30 text-amber-200';
    else if (tier === 'meta') badge.className += 'bg-blue-500/30 text-blue-200';
    else if (tier === 'intermediaria') badge.className += 'bg-orange-500/30 text-orange-200';
    else badge.className += 'bg-emerald-500/20 text-emerald-300';

    el('mp-hero-mat').textContent = `${totalMat} matrículas`;
    el('mp-hero-dias').textContent = `${d.dias_restantes} dias restantes`;

    // Mensagem em HTML (suporta quebras de linha)
    const msgEl = el('mp-hero-msg');
    if (msgEl) msgEl.innerHTML = d.mensagem || '';

    // Termômetro segmentado por tier — SEMPRE vibrante
    const thermo = document.getElementById('mp-hero-thermo');
    if (thermo) {
        const inter = metas.intermediaria || 0;
        const metaVal = metas.meta || 0;
        const sup = metas.supermeta || 0;

        const segments = [];
        let prev = 0;
        if (inter > 0) { segments.push({ label: 'Inter', from: prev, to: inter, color: '#f97316', colorBg: '#431407', emoji: '🔥' }); prev = inter; }
        if (metaVal > 0) { segments.push({ label: 'Meta', from: prev, to: metaVal, color: '#3b82f6', colorBg: '#172554', emoji: '🎯' }); prev = metaVal; }
        if (sup > 0) { segments.push({ label: 'Super', from: prev, to: sup, color: '#f59e0b', colorBg: '#451a03', emoji: '🏆' }); prev = sup; }

        if (!segments.length) { thermo.innerHTML = ''; return; }

        const maxVal = segments[segments.length - 1].to;
        const scale = Math.max(maxVal, totalMat) * 1.02;

        const segsHtml = segments.map(seg => {
            const widthPct = ((seg.to - seg.from) / scale) * 100;
            const filled = totalMat >= seg.to;
            const partial = !filled && totalMat > seg.from;
            const partialPct = partial ? ((totalMat - seg.from) / (seg.to - seg.from)) * 100 : 0;

            const barBg = filled
                ? `background:${seg.color};box-shadow:0 0 14px ${seg.color}55`
                : `background:${seg.colorBg};border:1px solid ${seg.color}33`;

            const innerBar = partial
                ? `<div class="h-full rounded" style="width:${partialPct}%;background:${seg.color};box-shadow:0 0 10px ${seg.color}66"></div>`
                : '';

            const check = filled ? '✅' : '';

            return `<div class="flex flex-col items-center" style="width:${widthPct}%">
                <div class="w-full h-5 rounded overflow-hidden" style="${barBg}">${innerBar}</div>
                <span class="text-[11px] font-extrabold mt-1.5 whitespace-nowrap" style="color:${seg.color}">${seg.emoji} ${seg.label} ${check}</span>
                <span class="text-[10px] font-bold" style="color:${seg.color}cc">${seg.to} mat</span>
            </div>`;
        }).join('');

        const agentPct = Math.min(99, (totalMat / scale) * 100);
        const pinColor = '#10b981';

        thermo.innerHTML = `
            <div class="relative mt-2 mb-2">
                <div class="flex gap-1.5">${segsHtml}</div>
                <div class="absolute top-0 h-5 pointer-events-none" style="left:${agentPct}%">
                    <div class="w-1 h-7 -mt-1 rounded-full" style="background:#fff;box-shadow:0 0 8px ${pinColor}"></div>
                    <div class="absolute -top-6 -translate-x-1/2 px-2 py-0.5 rounded-md text-[10px] font-black text-white whitespace-nowrap" style="background:${pinColor};box-shadow:0 0 10px ${pinColor}55">📍 ${totalMat}</div>
                </div>
            </div>`;
    }
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
    const myMat = rk.minhas_mat || 0;
    const myAce = rk.meus_aceites || 0;

    const medalCfg = {
        1: { icon: 'trophy', color: 'text-amber-400', bg: 'bg-gradient-to-br from-amber-500/30 to-amber-900/15', border: 'border-amber-400/50', glow: 'shadow-amber-500/20', label: '🏆 Você lidera o ranking!', labelColor: 'text-amber-400' },
        2: { icon: 'workspace_premium', color: 'text-slate-200', bg: 'bg-gradient-to-br from-slate-300/20 to-slate-700/10', border: 'border-slate-300/40', glow: 'shadow-slate-400/15', label: '🥈 Vice-líder!', labelColor: 'text-slate-300' },
        3: { icon: 'workspace_premium', color: 'text-orange-400', bg: 'bg-gradient-to-br from-orange-500/25 to-orange-900/10', border: 'border-orange-400/40', glow: 'shadow-orange-500/15', label: '🥉 Top 3! Pódio!', labelColor: 'text-orange-400' },
    };
    const m = medalCfg[pos];

    let motivacao = '';
    if (pos === 1) {
        motivacao = '<p class="text-xs text-amber-300/80 mt-2">Ninguém te alcançou! Continue dominando! 🔥</p>';
    } else if (diff > 0 && diff <= 3) {
        motivacao = `<p class="text-xs text-cyan-400 font-semibold mt-2">🔥 Quase lá! Só <strong>${diff}</strong> para o topo!</p>`;
    } else if (diff > 0 && diff <= 10) {
        motivacao = `<p class="text-xs text-slate-400 mt-2">Faltam <strong>${diff}</strong> para o 1°. Cada matrícula conta! 💪</p>`;
    } else if (diff > 0) {
        motivacao = `<p class="text-xs text-slate-500 mt-2">${diff} atrás do líder — foco e consistência! 🚀</p>`;
    }

    const scoreDetail = `<p class="text-[10px] text-slate-500 mt-1">${myMat} mat${myAce > 0 ? ' + ' + myAce + ' aceite' + (myAce > 1 ? 's' : '') : ''}</p>`;

    content.innerHTML = `
        <div class="flex items-center gap-5">
            <div class="w-20 h-20 rounded-2xl flex items-center justify-center border-2 ${m ? m.bg + ' ' + m.border + ' ' + m.glow : 'bg-slate-700/40 border-slate-600/30'} shadow-lg">
                ${m
                    ? `<span class="material-symbols-outlined text-4xl ${m.color}">${m.icon}</span>`
                    : `<span class="text-3xl font-black text-slate-300">${pos}°</span>`}
            </div>
            <div class="flex-1">
                <p class="text-3xl font-black text-white">${pos}°</p>
                <p class="text-sm text-slate-500">de ${total}</p>
                ${scoreDetail}
                ${m ? `<p class="text-xs font-bold ${m.labelColor} mt-1">${m.label}</p>` : ''}
                ${motivacao}
            </div>
        </div>`;
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
        primeira_mat: { bg: '#065f46', border: '#10b981', icon: '#6ee7b7', glow: '#10b981' },
        streak_3: { bg: '#78350f', border: '#f59e0b', icon: '#fcd34d', glow: '#f59e0b' },
        streak_5: { bg: '#7c2d12', border: '#f97316', icon: '#fdba74', glow: '#f97316' },
        streak_7: { bg: '#7f1d1d', border: '#ef4444', icon: '#fca5a5', glow: '#ef4444' },
        meta_batida: { bg: '#1e3a5f', border: '#3b82f6', icon: '#93c5fd', glow: '#3b82f6' },
        supermeta: { bg: '#713f12', border: '#eab308', icon: '#fef08a', glow: '#eab308' },
        meta_antecipada: { bg: '#164e63', border: '#06b6d4', icon: '#67e8f9', glow: '#06b6d4' },
        melhor_dia: { bg: '#831843', border: '#ec4899', icon: '#f9a8d4', glow: '#ec4899' },
        top_3: { bg: '#451a03', border: '#d97706', icon: '#fbbf24', glow: '#d97706' },
    };
    const defaultColor = { bg: '#4a1d96', border: '#a855f7', icon: '#d8b4fe', glow: '#a855f7' };

    grid.innerHTML = achieved.map(a => {
        const c = colorMap[a.id] || defaultColor;
        return `<div class="flex flex-col items-center gap-2 p-3 rounded-xl w-[88px] border-2 shadow-lg transition-transform hover:scale-110 cursor-default" style="background:${c.bg};border-color:${c.border};box-shadow:0 0 16px ${c.glow}55, 0 4px 12px rgba(0,0,0,.3)" title="${a.desc || a.nome}">
            <span class="material-symbols-outlined text-3xl" style="color:${c.icon};filter:drop-shadow(0 0 8px ${c.glow})">${a.icone}</span>
            <span class="text-[9px] text-center leading-tight font-bold" style="color:${c.icon}">${a.nome}</span>
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

    const efetivo = feitas + aceitesFila;
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
        if (aceitesFila > 0) {
            aceitesBadge.classList.remove('hidden');
            aceitesBadge.innerHTML = `
                <span class="material-symbols-outlined text-sm text-purple-400">pending</span>
                <span class="text-[10px] text-purple-300 font-medium">${aceitesFila} aceite${aceitesFila > 1 ? 's' : ''} na fila${aceitesHoje > 0 ? ' (' + aceitesHoje + ' novo' + (aceitesHoje > 1 ? 's' : '') + ' hoje)' : ''}</span>
            `;
        } else {
            aceitesBadge.classList.add('hidden');
        }
    }

    const ontemRealizadas = hoje.ontem_realizadas || 0;

    // Comparação com ontem
    let yesterdayHtml = '';
    if (feitas > ontemRealizadas && ontemRealizadas >= 0) {
        const diff = feitas - ontemRealizadas;
        yesterdayHtml = `<p class="text-[10px] text-emerald-400 font-semibold mt-2">📈 +${diff} a mais que ontem — continue assim!</p>`;
    } else if (feitas === ontemRealizadas && feitas > 0) {
        yesterdayHtml = `<p class="text-[10px] text-amber-400 font-semibold mt-2">⚡ Mesmo ritmo de ontem — hora de ultrapassar!</p>`;
    } else if (feitas < ontemRealizadas && ontemRealizadas > 0) {
        yesterdayHtml = `<p class="text-[10px] text-orange-400 font-semibold mt-2">🔥 Ontem você fez ${ontemRealizadas} — bora superar!</p>`;
    }

    if (meta <= 0) {
        if (status) status.textContent = 'Sem meta diária hoje';
        if (detail) {
            detail.innerHTML = (aceitesFila > 0
                ? `Nenhuma meta PIX configurada, mas você tem ${aceitesFila} aceite${aceitesFila > 1 ? 's' : ''} na fila!`
                : 'Nenhuma meta PIX configurada para hoje.') + yesterdayHtml;
        }
        if (valor) valor.textContent = '';
        return;
    }

    if (efetivo >= meta) {
        const ganho = fixo + extra * Math.max(0, efetivo - meta);
        if (status) { status.textContent = '🎉 PIX Garantido!'; status.className = 'text-base font-bold text-emerald-400 mb-1'; }
        const parts = [];
        if (feitas > 0) parts.push(`${feitas} mat`);
        if (aceitesFila > 0) parts.push(`${aceitesFila} aceite${aceitesFila > 1 ? 's' : ''}`);
        if (detail) detail.innerHTML = (efetivo > meta
            ? `Meta batida! ${parts.join(' + ')} = ${efetivo} · +${efetivo - meta} extra × ${_mpFmt(extra)} cada`
            : `Parabéns! ${parts.join(' + ')} = meta batida!`) + yesterdayHtml;
        if (valor) valor.textContent = _mpFmt(ganho);
    } else {
        const falta = meta - efetivo;
        if (status) { status.textContent = `Faltam ${falta} para o PIX!`; status.className = 'text-base font-bold text-cyan-400 mb-1'; }
        if (detail) detail.innerHTML = `${feitas} mat + ${aceitesFila} aceite${aceitesFila !== 1 ? 's' : ''} = ${efetivo}/${meta}` + yesterdayHtml;
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
