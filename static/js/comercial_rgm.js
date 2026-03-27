// ---------------------------------------------------------------------------
// Dashboard Comercial
// ---------------------------------------------------------------------------

let _crgmChartEvolucao = null;
let _crgmChartAgentes = null;

async function loadComercialRgm() {
    await _crgmLoadCiclos();
    await _crgmLoadTurmas();
    const filtersData = await _crgmLoadFilters();
    await _crgmLoadSnapshotInfo();
    if (filtersData && (!filtersData.agentes || filtersData.agentes.length === 0)) {
        await _crgmAutoSyncUsers();
    }
    const hoje = new Date();
    const ini = new Date(hoje.getFullYear(), hoje.getMonth(), 1);
    const elIni = document.getElementById('crgm-dt-ini');
    const elFim = document.getElementById('crgm-dt-fim');
    if (!elIni.value) elIni.value = ini.toISOString().substring(0, 10);
    if (!elFim.value) elFim.value = hoje.toISOString().substring(0, 10);
    crgmAtualizar();
}

async function _crgmAutoSyncUsers() {
    try {
        const res = await api('/api/comercial-rgm/sync-users', { method: 'POST' });
        const d = await res.json();
        if (d.ok && d.synced > 0) await _crgmLoadFilters();
    } catch (e) { console.error('auto-sync users', e); }
}

async function _crgmLoadFilters() {
    try {
        const res = await api('/api/comercial-rgm/filters');
        const d = await res.json();
        if (!d.ok) return null;
        const selPolo = document.getElementById('crgm-polo');
        const selNivel = document.getElementById('crgm-nivel');
        const selAgente = document.getElementById('crgm-agente');
        const curPolo = selPolo.value, curNivel = selNivel.value, curAgente = selAgente ? selAgente.value : '';
        selPolo.innerHTML = '<option value="">Todos</option>' + d.polos.map(p => `<option value="${esc(p)}">${esc(p)}</option>`).join('');
        selNivel.innerHTML = '<option value="">Todos</option>' + d.niveis.map(n => `<option value="${esc(n)}">${esc(n)}</option>`).join('');
        if (selAgente && d.agentes) selAgente.innerHTML = '<option value="">Todos</option>' + d.agentes.map(a => `<option value="${a.id}">${esc(a.name)}</option>`).join('');
        if (curPolo) selPolo.value = curPolo;
        if (curNivel) selNivel.value = curNivel;
        if (curAgente && selAgente) selAgente.value = curAgente;
        return d;
    } catch (e) { console.error('crgm filters', e); return null; }
}

async function _crgmLoadSnapshotInfo() {
    try {
        const res = await api('/api/comercial-rgm/snapshot-info');
        const d = await res.json();
        if (!d.ok || !d.total) return;
        const dt = d.uploaded_at ? new Date(d.uploaded_at).toLocaleString('pt-BR') : '';
        let info = `${d.total.toLocaleString('pt-BR')} registros CSV`;
        if (d.min_date && d.max_date) info += ` | ${d.min_date} a ${d.max_date}`;
        if (d.mm_inscritos > 0 || d.mm_matriculados > 0) info += ` | M&M: ${(d.mm_inscritos||0).toLocaleString('pt-BR')} insc. / ${(d.mm_matriculados||0).toLocaleString('pt-BR')} matr.`;
        if (dt) info += ` | ${dt}`;
        document.getElementById('crgm-snapshot-info').textContent = info;
    } catch (e) { console.error('crgm snapshot-info', e); }
}

async function crgmAtualizar() {
    const polo = document.getElementById('crgm-polo').value;
    const nivel = document.getElementById('crgm-nivel').value;
    const dtIni = document.getElementById('crgm-dt-ini').value;
    const dtFim = document.getElementById('crgm-dt-fim').value;
    const cicloSel = document.getElementById('crgm-ciclo');
    const cicloId = cicloSel ? cicloSel.value : '';
    const ciclo = _crgmCiclosData.find(c => c.id === parseInt(cicloId));
    _crgmLoading(true); _crgmErro('');
    const qs = new URLSearchParams();
    if (polo) qs.set('polo', polo);
    if (nivel) qs.set('nivel', nivel);
    if (dtIni) qs.set('dt_ini', dtIni);
    if (dtFim) qs.set('dt_fim', dtFim);
    if (ciclo) qs.set('ciclo', ciclo.nome);
    const turmaSelEl = document.getElementById('crgm-turma');
    const turmaId = turmaSelEl ? turmaSelEl.value : '';
    const turmaObj = _crgmTurmasData.find(t => t.id === parseInt(turmaId));
    if (turmaObj) qs.set('turma', turmaObj.nome);
    try {
        const res = await api(`/api/comercial-rgm/data?${qs}`);
        const d = await res.json();
        if (!d.ok) { _crgmErro(d.error || 'Erro'); return; }
        _crgmRenderKPIs(d.kpis);
        _crgmRenderEvasao(d.evasao);
        _crgmRenderEvolucao(d.evolucao, d.evolucao_prev || []);
        _crgmRenderPoloTable(d.ranking_polo);
        _crgmRenderCicloTable(d.ranking_ciclo);
        _crgmRenderAgentes(d.ranking_agentes || []);
        _crgmRenderAgentesChart(d.ranking_agentes || []);
        _crgmRenderTransferencia(d.transferencia_regresso);
    } catch (e) { _crgmErro('Erro: ' + e.message); }
    finally { _crgmLoading(false); }
}

// ── KPIs ────────────────────────────────────────────────
function _crgmRenderKPIs(k) {
    document.getElementById('crgm-vendas').textContent = k.vendas.toLocaleString('pt-BR');
    // Mostra líquido se diferente do bruto
    const liqEl = document.getElementById('crgm-vendas-liquidas');
    const liqLabel = document.getElementById('crgm-vendas-liquidas-label');
    if (liqEl && liqLabel && k.vendas_liquidas != null && k.vendas_liquidas !== k.vendas) {
        liqEl.textContent = k.vendas_liquidas.toLocaleString('pt-BR');
        liqLabel.classList.remove('hidden');
    } else if (liqLabel) {
        liqLabel.classList.add('hidden');
    }
    document.getElementById('crgm-ytd').textContent = k.vendas_ytd.toLocaleString('pt-BR');
    document.getElementById('crgm-media').textContent = k.media_diaria.toLocaleString('pt-BR');
    document.getElementById('crgm-ticket').textContent = 'R$ ' + k.ticket_medio.toLocaleString('pt-BR', {minimumFractionDigits:2, maximumFractionDigits:2});
    document.getElementById('crgm-dias').textContent = k.dias;
    const mmEl = document.getElementById('crgm-mm-inscritos');
    if (mmEl) mmEl.textContent = (k.mm_inscritos || 0).toLocaleString('pt-BR');
    document.getElementById('crgm-1a-val').textContent = k.vendas_1a.toLocaleString('pt-BR');
    _crgmBadge('crgm-1a-badge', k.pct_1a);
    document.getElementById('crgm-6m-val').textContent = k.vendas_6m.toLocaleString('pt-BR');
    _crgmBadge('crgm-6m-badge', k.pct_6m);
    document.getElementById('crgm-ytd-prev').textContent = (k.vendas_prev_ytd||0).toLocaleString('pt-BR');
    _crgmBadge('crgm-ytd-badge', k.pct_ytd);
}

// ── Evasão ──────────────────────────────────────────────
const _EVASAO_COLORS = {
    'CANCELADO':   'bg-rose-500/20 text-rose-300',
    'TRANCADO':    'bg-amber-500/20 text-amber-300',
    'TRANSFERIDO': 'bg-purple-500/20 text-purple-300',
};
function _crgmRenderEvasao(evasao) {
    const panel = document.getElementById('crgm-evasao-panel');
    if (!panel) return;
    if (!evasao || evasao.total === 0) { panel.classList.add('hidden'); return; }
    panel.classList.remove('hidden');
    document.getElementById('crgm-evasao-total').textContent = evasao.total.toLocaleString('pt-BR');

    // Tags por tipo
    const tiposEl = document.getElementById('crgm-evasao-tipos');
    tiposEl.innerHTML = '';
    for (const [tipo, qtd] of Object.entries(evasao.por_tipo)) {
        const cls = _EVASAO_COLORS[tipo] || 'bg-slate-500/20 text-slate-300';
        tiposEl.insertAdjacentHTML('beforeend',
            `<span class="text-[10px] font-bold px-2 py-0.5 rounded-full ${cls}">${tipo.charAt(0)+tipo.slice(1).toLowerCase()}: ${qtd}</span>`);
    }

    // Lista por agente
    const agEl = document.getElementById('crgm-evasao-agentes');
    agEl.innerHTML = '';
    (evasao.por_agente || []).forEach(ag => {
        agEl.insertAdjacentHTML('beforeend', `
            <details class="group">
                <summary class="flex items-center justify-between px-4 py-2.5 cursor-pointer hover:bg-rose-500/5 list-none">
                    <span class="text-sm text-slate-300 font-medium">${ag.agente}</span>
                    <div class="flex items-center gap-2">
                        <span class="text-xs font-bold text-rose-300">${ag.total}</span>
                        <svg class="w-3.5 h-3.5 text-slate-500 group-open:rotate-180 transition-transform" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/></svg>
                    </div>
                </summary>
                <div class="px-4 pb-3 space-y-1">
                    ${(ag.itens || []).map(it => `
                        <div class="flex items-center gap-2 text-xs text-slate-400">
                            <span class="font-mono text-slate-500">${it.rgm}</span>
                            <span class="flex-1 truncate">${it.nome}</span>
                            <span class="${(_EVASAO_COLORS[it.situacao]||'bg-slate-500/20 text-slate-400')} text-[10px] px-1.5 py-0.5 rounded font-bold">${it.situacao.charAt(0)+it.situacao.slice(1).toLowerCase()}</span>
                            <span class="text-slate-600">${it.data_matricula||''}</span>
                        </div>`).join('')}
                </div>
            </details>`);
    });
}

function crgmToggleEvasaoDetalhes() {
    const el = document.getElementById('crgm-evasao-detalhes');
    const ch = document.getElementById('crgm-evasao-chevron');
    if (!el) return;
    el.classList.toggle('hidden');
    if (ch) ch.style.transform = el.classList.contains('hidden') ? '' : 'rotate(180deg)';
}
function _crgmBadge(id, pct) {
    const el = document.getElementById(id); if (!el) return;
    el.textContent = `${pct >= 0 ? '\u2191' : '\u2193'} ${Math.abs(pct)}%`;
    el.className = pct >= 0 ? 'font-bold px-1.5 py-0.5 rounded text-[10px] bg-emerald-500/20 text-emerald-400' : 'font-bold px-1.5 py-0.5 rounded text-[10px] bg-red-500/20 text-red-400';
}

// ── Evolução ────────────────────────────────────────────
function _crgmRenderEvolucao(evolucao, evolucaoPrev) {
    const ctx = document.getElementById('crgm-chart-evolucao');
    if (_crgmChartEvolucao) _crgmChartEvolucao.destroy();
    const labels = evolucao.map(e => { const d = new Date(e.data+'T00:00:00'); return d.toLocaleDateString('pt-BR',{day:'2-digit',month:'short'}); });
    const values = evolucao.map(e => e.count);
    const datasets = [{
        label: 'Matrículas', data: values,
        borderColor: '#3b82f6', backgroundColor: 'rgba(59,130,246,0.06)',
        borderWidth: 2.5, fill: true, tension: 0.3,
        pointRadius: evolucao.length > 60 ? 0 : 4, pointBackgroundColor: '#3b82f6', pointHoverRadius: 6,
    }];
    if (evolucaoPrev && evolucaoPrev.length > 0) {
        const prevMap = {};
        evolucaoPrev.forEach(e => { const d = new Date(e.data+'T00:00:00'); prevMap[`${String(d.getDate()).padStart(2,'0')}/${String(d.getMonth()+1).padStart(2,'0')}`] = (prevMap[`${String(d.getDate()).padStart(2,'0')}/${String(d.getMonth()+1).padStart(2,'0')}`]||0) + e.count; });
        datasets.push({
            label: 'Ano Anterior', data: evolucao.map(e => { const d=new Date(e.data+'T00:00:00'); return prevMap[`${String(d.getDate()).padStart(2,'0')}/${String(d.getMonth()+1).padStart(2,'0')}`]||0; }),
            borderColor: '#475569', backgroundColor: 'transparent',
            borderWidth: 1.5, borderDash: [6,4], fill: false, tension: 0.3, pointRadius: 0, pointHoverRadius: 4,
        });
    }
    _crgmChartEvolucao = new Chart(ctx, { type:'line', data:{labels,datasets}, options:{
        responsive:true, maintainAspectRatio:false,
        interaction:{mode:'index',intersect:false},
        plugins:{ legend:{display:evolucaoPrev&&evolucaoPrev.length>0, position:'top',align:'end', labels:{color:'#94a3b8',font:{size:10},boxWidth:12,padding:12}} },
        scales:{ x:{ticks:{color:'#64748b',maxTicksLimit:15,font:{size:10}},grid:{color:'rgba(100,116,139,0.08)'}}, y:{beginAtZero:true,ticks:{color:'#64748b',font:{size:10}},grid:{color:'rgba(100,116,139,0.08)'}} }
    }});
}

// ── Polo (tabela com barras) ────────────────────────────
function _crgmRenderPoloTable(ranking) {
    const tbody = document.getElementById('crgm-polo-body');
    if (!ranking || !ranking.length) { tbody.innerHTML = '<tr><td colspan="4" class="px-5 py-6 text-center text-slate-600">Sem dados</td></tr>'; return; }
    const max = Math.max(...ranking.map(r => r.total));
    tbody.innerHTML = ranking.map((r, i) =>
        `<tr class="hover:bg-white/[0.02] transition-colors">
            <td class="text-center px-3 py-2 text-slate-500 font-medium text-xs">${i+1}</td>
            <td class="px-4 py-2 text-slate-300 text-xs truncate max-w-[200px]">${esc(r.nome)}</td>
            <td class="px-4 py-2 text-right font-mono text-white font-semibold text-xs">${r.total.toLocaleString('pt-BR')}</td>
            <td class="px-4 py-2"><div class="h-3 rounded-full bg-slate-800 overflow-hidden"><div class="h-full rounded-full bg-gradient-to-r from-cyan-500 to-blue-500" style="width:${Math.round(r.total/max*100)}%"></div></div></td>
        </tr>`
    ).join('');
}

// ── Ciclo ───────────────────────────────────────────────
function _crgmRenderCicloTable(ciclos) {
    const tbody = document.getElementById('crgm-ciclo-body');
    if (!ciclos || !ciclos.length) { tbody.innerHTML = '<tr><td colspan="2" class="px-5 py-6 text-center text-slate-600">Sem dados</td></tr>'; return; }
    tbody.innerHTML = ciclos.map(c => `<tr class="hover:bg-white/[0.02] transition-colors"><td class="px-5 py-2.5 text-slate-300">${esc(c.nome)}</td><td class="px-5 py-2.5 text-right text-white font-semibold">${c.total.toLocaleString('pt-BR')}</td></tr>`).join('');
}

// ── Agentes (tabela) ────────────────────────────────────
function _crgmTierLabel(mp, meta, intermediaria, supermeta) {
    if (supermeta > 0 && mp >= supermeta) return { label: 'SUPERMETA', cls: 'text-emerald-400 font-bold', icon: '\u2B50' };
    if (intermediaria > 0 && mp >= intermediaria) return { label: 'INTERMED.', cls: 'text-amber-400 font-semibold', icon: '\u26A1' };
    if (meta > 0 && mp >= meta) return { label: 'META', cls: 'text-blue-400 font-semibold', icon: '\u2705' };
    if (meta > 0) return { label: `${Math.round(mp/meta*100)}%`, cls: 'text-red-400', icon: '' };
    return { label: '\u2014', cls: 'text-slate-600', icon: '' };
}

function _crgmRenderAgentes(agentes) {
    const tbody = document.getElementById('crgm-agentes-body');
    const countEl = document.getElementById('crgm-agentes-count');
    if (!agentes || !agentes.length) {
        tbody.innerHTML = '<tr><td colspan="12" class="px-5 py-8 text-center text-slate-600">Nenhum agente encontrado</td></tr>';
        if (countEl) countEl.textContent = ''; return;
    }
    const agenteFilter = document.getElementById('crgm-agente').value;
    let filtered = agenteFilter ? agentes.filter(a => String(a.user_id) === agenteFilter) : [...agentes];
    filtered.sort((a,b) => (b.matriculas_periodo||0) - (a.matriculas_periodo||0));
    filtered = filtered.filter(a => (a.matriculas_periodo||0)>0 || (a.ganhos||0)>0);
    if (countEl) countEl.textContent = `${filtered.length} agentes`;
    const medals = ['\uD83E\uDD47','\uD83E\uDD48','\uD83E\uDD49'];
    tbody.innerHTML = filtered.map((a,i) => {
        const mp = a.matriculas_periodo||0, pp = a.perdidos_periodo||0, np = a.novos_periodo||0;
        const meta = a.meta||0;
        const intermediaria = a.meta_intermediaria||0;
        const supermeta = a.supermeta||0;
        const tier = _crgmTierLabel(mp, meta, intermediaria, supermeta);
        const taxaClass = a.taxa_conversao>=20 ? 'text-emerald-400' : a.taxa_conversao>=8 ? 'text-amber-400' : 'text-red-400';
        const rank = i<3 ? medals[i] : (i+1);
        const rowBg = a.is_transferencia ? 'bg-amber-500/[0.06]' : (i<3 ? 'bg-blue-500/[0.03]' : '');

        // Tooltip with all category metas
        const mc = a.metas_cat || {};
        let tooltip = Object.entries(mc).length > 0
            ? Object.entries(mc).map(([cat, v]) =>
                `${_crgmCatLabel(cat)}: M=${v.meta} I=${v.intermediaria} S=${v.supermeta}`
            ).join(' | ')
            : 'Sem meta definida';

        return `<tr class="hover:bg-white/[0.03] transition-colors cursor-pointer ${rowBg}" title="${tooltip}" onclick="crgmAgenteDetalhe(${a.user_id})">
            <td class="text-center px-3 py-2.5 font-bold text-slate-400">${rank}</td>
            <td class="px-4 py-2.5 font-medium ${a.nome&&a.nome.startsWith('User #')?'text-slate-500 italic':'text-white'}">
                <div class="flex items-center gap-2">
                    <span>${esc(a.nome)}</span>
                    <button onclick="event.stopPropagation();navigateToPerformance(${a.user_id})" title="Ver painel de performance" class="opacity-0 group-hover:opacity-100 hover:opacity-100 text-indigo-400 hover:text-indigo-300 transition-all p-0.5 rounded hover:bg-indigo-500/10 flex-shrink-0" style="opacity:.35">
                        <span class="material-symbols-outlined text-sm">monitoring</span>
                    </button>
                </div>
            </td>
            <td class="px-4 py-2.5 text-right font-mono text-blue-400 font-semibold">${mp.toLocaleString('pt-BR')}</td>
            <td class="px-4 py-2.5 text-right font-mono text-amber-300/70">${intermediaria>0?intermediaria:'\u2014'}</td>
            <td class="px-4 py-2.5 text-right font-mono text-blue-300/70">${meta>0?meta:'\u2014'}</td>
            <td class="px-4 py-2.5 text-right font-mono text-emerald-300/70">${supermeta>0?supermeta:'\u2014'}</td>
            <td class="px-4 py-2.5 text-right font-mono ${tier.cls}">${tier.icon} ${tier.label}</td>
            <td class="px-4 py-2.5 text-right font-mono text-cyan-400">${np.toLocaleString('pt-BR')}</td>
            <td class="px-4 py-2.5 text-right font-mono text-red-400">${pp.toLocaleString('pt-BR')}</td>
            <td class="px-4 py-2.5 text-right font-mono text-slate-300">${a.total.toLocaleString('pt-BR')}</td>
            <td class="px-4 py-2.5 text-right font-mono text-teal-400">${a.ativos.toLocaleString('pt-BR')}</td>
            <td class="px-4 py-2.5 text-right font-mono font-bold ${taxaClass}">${a.taxa_conversao}%</td>
        </tr>`;
    }).join('');
}

// ── Detalhe por agente ──────────────────────────────────────────────────
async function crgmAgenteDetalhe(userId) {
    const dtIni = document.getElementById('crgm-dt-ini').value;
    const dtFim = document.getElementById('crgm-dt-fim').value;
    const polo  = document.getElementById('crgm-polo').value;
    const nivel = document.getElementById('crgm-nivel').value;
    const cicloSel = document.getElementById('crgm-ciclo');
    const cicloId  = cicloSel ? cicloSel.value : '';
    const cicloObj = _crgmCiclosData.find(c => c.id === parseInt(cicloId));
    const turmaSelEl = document.getElementById('crgm-turma');
    const turmaId    = turmaSelEl ? turmaSelEl.value : '';
    const turmaObj   = _crgmTurmasData.find(t => t.id === parseInt(turmaId));

    const qs = new URLSearchParams({ user_id: userId });
    if (dtIni)  qs.set('dt_ini', dtIni);
    if (dtFim)  qs.set('dt_fim', dtFim);
    if (polo)   qs.set('polo', polo);
    if (nivel)  qs.set('nivel', nivel);
    if (cicloObj) qs.set('ciclo', cicloObj.nome);
    if (turmaObj) qs.set('turma', turmaObj.nome);

    // Mostrar modal de loading
    _crgmDetalheOpen(userId, dtIni, dtFim, qs.toString(), null);
    try {
        const res = await api(`/api/comercial-rgm/agente-detalhe?${qs}`);
        const d = await res.json();
        if (!d.ok) { _crgmDetalheOpen(userId, dtIni, dtFim, qs.toString(), null, d.error); return; }
        _crgmDetalheOpen(userId, dtIni, dtFim, qs.toString(), d);
    } catch(e) { _crgmDetalheOpen(userId, dtIni, dtFim, qs.toString(), null, e.message); }
}

function _crgmDetalheOpen(userId, dtIni, dtFim, qs, data, err) {
    let modal = document.getElementById('crgm-detalhe-modal');
    if (!modal) {
        modal = document.createElement('div');
        modal.id = 'crgm-detalhe-modal';
        modal.className = 'fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm p-4';
        document.body.appendChild(modal);
    }
    const titulo = data ? `${data.total} matrícula(s)` : (err ? 'Erro' : 'Carregando...');
    const csvUrl = `/api/comercial-rgm/agente-detalhe?${qs}&fmt=csv`;

    function tipoBadge(tipo) {
        if (!tipo) return '';
        const t = tipo.toUpperCase();
        if (t === 'NOVA MATRICULA') return `<span class="inline-block px-1.5 py-0.5 rounded text-[10px] font-semibold bg-emerald-500/20 text-emerald-300">NOVA</span>`;
        if (t === 'RETORNO')        return `<span class="inline-block px-1.5 py-0.5 rounded text-[10px] font-semibold bg-amber-500/20 text-amber-300">RETORNO</span>`;
        if (t === 'RECOMPRA')       return `<span class="inline-block px-1.5 py-0.5 rounded text-[10px] font-semibold bg-blue-500/20 text-blue-300">RECOMPRA</span>`;
        return `<span class="inline-block px-1.5 py-0.5 rounded text-[10px] font-semibold bg-slate-600/40 text-slate-400">${esc(tipo)}</span>`;
    }

    let corpo = '';
    if (err) {
        corpo = `<p class="text-red-400 text-sm">${esc(err)}</p>`;
    } else if (!data) {
        corpo = `<p class="text-slate-400 text-sm animate-pulse">Buscando...</p>`;
    } else {
        // Resumo: total vs outliers (por prefixo de RGM)
        let outliers = 0;
        (data.itens || []).forEach(r => { if (r.outlier) outliers++; });
        const outlierBadge = outliers > 0
            ? `<span class="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-semibold bg-orange-500/20 text-orange-300 border border-orange-500/30" title="RGMs com prefixo abaixo do padrão do ciclo atual">⚠ ${outliers} RGM${outliers>1?'s':''} fora do padrão</span>`
            : `<span class="text-[10px] text-slate-500">Todos os RGMs dentro do padrão do ciclo</span>`;

        corpo = `
        <div class="mb-3 flex items-center justify-between gap-3 flex-wrap">
            <div class="flex items-center gap-2 flex-wrap text-xs">
                <span class="text-slate-400">Total: <span class="text-white font-semibold">${data.total}</span></span>
                <span class="text-slate-600">·</span>
                ${outlierBadge}
            </div>
            <a href="${csvUrl}" download class="text-xs bg-emerald-600 hover:bg-emerald-500 text-white px-3 py-1.5 rounded-lg flex items-center gap-1.5 transition-colors shrink-0">
                ⬇ Baixar CSV
            </a>
        </div>
        <div class="overflow-auto max-h-[52vh]">
        <table class="w-full text-xs">
            <thead class="sticky top-0 bg-slate-800 text-slate-400 uppercase tracking-wider text-[10px]">
                <tr>
                    <th class="px-3 py-2 text-left">RGM</th>
                    <th class="px-3 py-2 text-left">Nome</th>
                    <th class="px-3 py-2 text-left">Tipo</th>
                    <th class="px-3 py-2 text-left">Polo</th>
                    <th class="px-3 py-2 text-left">Nível</th>
                    <th class="px-3 py-2 text-left">Data Matrícula</th>
                    <th class="px-3 py-2 text-left">Curso</th>
                </tr>
            </thead>
            <tbody class="divide-y divide-slate-700/30">
                ${data.itens.map(r => {
                    const rowCls = r.outlier ? 'hover:bg-white/[0.03] bg-orange-500/5' : 'hover:bg-white/[0.03]';
                    const rgmTag = r.outlier
                        ? `<span class="font-mono text-orange-300" title="RGM fora do padrão do ciclo">${esc(r.rgm)} ⚠</span>`
                        : `<span class="font-mono text-slate-300">${esc(r.rgm)}</span>`;
                    return `
                    <tr class="${rowCls}">
                        <td class="px-3 py-2">${rgmTag}</td>
                        <td class="px-3 py-2 text-white">${esc(r.nome)}</td>
                        <td class="px-3 py-2">${tipoBadge(r.tipo_matricula)}</td>
                        <td class="px-3 py-2 text-slate-300">${esc(r.polo)}</td>
                        <td class="px-3 py-2 text-slate-400">${esc(r.nivel)}</td>
                        <td class="px-3 py-2 font-mono text-blue-300">${esc(r.data_matricula)}</td>
                        <td class="px-3 py-2 text-slate-300 max-w-[200px] truncate" title="${esc(r.turma)}">${esc(r.turma)}</td>
                    </tr>`;
                }).join('')}
            </tbody>
        </table></div>`;
    }
    modal.innerHTML = `
    <div class="bg-slate-900 border border-slate-700/40 rounded-2xl shadow-2xl w-full max-w-5xl max-h-[90vh] flex flex-col overflow-hidden">
        <div class="px-5 py-4 border-b border-slate-700/30 flex items-center justify-between gap-3">
            <h3 class="text-sm font-bold text-white">${titulo} — período ${dtIni} a ${dtFim}</h3>
            <div class="flex items-center gap-2 flex-shrink-0">
                <button onclick="document.getElementById('crgm-detalhe-modal').remove();navigateToPerformance(${userId})"
                    class="text-xs bg-indigo-600 hover:bg-indigo-500 text-white px-3 py-1.5 rounded-lg flex items-center gap-1.5 transition-colors"
                    title="Ver painel motivacional completo deste agente">
                    <span class="material-symbols-outlined text-sm">monitoring</span>
                    Ver Performance
                </button>
                <button onclick="document.getElementById('crgm-detalhe-modal').remove()"
                    class="text-slate-500 hover:text-white transition-colors text-lg leading-none">&times;</button>
            </div>
        </div>
        <div class="p-5 overflow-auto flex-1">${corpo}</div>
    </div>`;
}

function _crgmRenderTransferencia(tr) {
    const wrap = document.getElementById('crgm-transferencia-wrap');
    const body = document.getElementById('crgm-transferencia-body');
    const totalEl = document.getElementById('crgm-transferencia-total');
    if (!wrap || !body) return;
    if (!tr || !tr.total) {
        wrap.classList.add('hidden');
        body.innerHTML = '';
        return;
    }
    wrap.classList.remove('hidden');
    if (totalEl) totalEl.textContent = `(${tr.total})`;
    body.innerHTML = (tr.itens || []).map(x =>
        `<div><span class="text-amber-300/80">${esc(String(x.rgm || ''))}</span>${x.nome ? ' — ' + esc(x.nome) : ''}</div>`
    ).join('');
}

// ── Agentes (chart) ─────────────────────────────────────
function _crgmRenderAgentesChart(agentes) {
    const ctx = document.getElementById('crgm-chart-agentes');
    if (_crgmChartAgentes) _crgmChartAgentes.destroy();
    if (!agentes || !agentes.length) { _crgmChartAgentes = null; return; }

    const agenteFilter = document.getElementById('crgm-agente').value;
    let data = agenteFilter ? agentes.filter(a=>String(a.user_id)===agenteFilter) : [...agentes];
    data = data.filter(a=>(a.matriculas_periodo||0)>0||(a.meta||0)>0);
    data.sort((a,b)=>(a.matriculas_periodo||0)-(b.matriculas_periodo||0));
    const top = data.slice(-15);
    const labels = top.map(a=>a.nome||`#${a.user_id}`);

    const hasMeta  = top.some(a=>(a.meta||0)>0);
    const hasInter = top.some(a=>(a.meta_intermediaria||0)>0);
    const hasSuper = top.some(a=>(a.supermeta||0)>0);

    // Cor de cada barra baseada no tier de performance
    const barColors = top.map(a => {
        const mp = a.matriculas_periodo||0, m = a.meta||0,
              mi = a.meta_intermediaria||0, s = a.supermeta||0;
        if (s>0 && mp>=s) return '#34d399';       // supermeta — verde
        if (mi>0 && mp>=mi) return '#fbbf24';     // intermediária — âmbar
        if (m>0 && mp>=m)  return '#60a5fa';      // meta — azul
        if (m>0)           return '#f87171';      // abaixo da meta — vermelho
        return '#3b82f6';                          // sem meta — azul padrão
    });

    const datasets = [{
        label: 'Matrículas',
        data: top.map(a=>a.matriculas_periodo||0),
        backgroundColor: barColors,
        borderColor: barColors.map(c=>c+'cc'),
        borderWidth: 0,
        borderRadius: 5,
        barPercentage: 0.55,
        categoryPercentage: 0.8,
    }];

    // Linhas de meta como marcadores (barras transparentes de 1px com borda)
    if (hasInter) datasets.push({
        label: 'Intermediária',
        data: top.map(a=>a.meta_intermediaria||0),
        backgroundColor: 'transparent',
        borderColor: '#fcd34d',
        borderWidth: 2,
        borderSkipped: false,
        borderRadius: 0,
        barPercentage: 0.9,
        categoryPercentage: 0.8,
        barThickness: 2,
    });
    if (hasMeta) datasets.push({
        label: 'Meta',
        data: top.map(a=>a.meta||0),
        backgroundColor: 'transparent',
        borderColor: '#93c5fd',
        borderWidth: 2,
        borderSkipped: false,
        borderRadius: 0,
        barPercentage: 0.9,
        categoryPercentage: 0.8,
        barThickness: 2,
    });
    if (hasSuper) datasets.push({
        label: 'Supermeta',
        data: top.map(a=>a.supermeta||0),
        backgroundColor: 'transparent',
        borderColor: '#6ee7b7',
        borderWidth: 2,
        borderSkipped: false,
        borderRadius: 0,
        barPercentage: 0.9,
        categoryPercentage: 0.8,
        barThickness: 2,
    });

    // Plugin para escrever o valor ao final de cada barra
    const valueLabelsPlugin = {
        id: 'crgmValueLabels',
        afterDatasetsDraw(chart) {
            const { ctx: c, scales: { x, y } } = chart;
            const ds0 = chart.getDatasetMeta(0);
            c.save();
            ds0.data.forEach((bar, i) => {
                const val = top[i].matriculas_periodo || 0;
                const m   = top[i].meta || 0;
                if (val === 0) return;
                const xPos = x.getPixelForValue(val) + 6;
                const yPos = bar.y;
                c.font = '500 11px Inter, sans-serif';
                c.fillStyle = '#e2e8f0';
                c.textBaseline = 'middle';
                c.fillText(val, xPos, yPos);
                if (m > 0) {
                    const pct = Math.round(val / m * 100);
                    c.font = '400 9px Inter, sans-serif';
                    c.fillStyle = pct >= 100 ? '#34d399' : '#94a3b8';
                    c.fillText(`${pct}%`, xPos + 22, yPos);
                }
            });
            c.restore();
        }
    };

    const maxVal = Math.max(...top.map(a=>Math.max(a.matriculas_periodo||0, a.supermeta||0, a.meta||0, a.meta_intermediaria||0))) * 1.18;

    _crgmChartAgentes = new Chart(ctx, {
        type: 'bar',
        data: { labels, datasets },
        plugins: [valueLabelsPlugin],
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'top', align: 'end',
                    labels: { color:'#64748b', font:{size:10}, boxWidth:8, boxHeight:8, padding:14,
                        generateLabels(chart) {
                            const orig = Chart.defaults.plugins.legend.labels.generateLabels(chart);
                            // só mostrar legenda do primeiro dataset + metas
                            return orig.filter(l => l.datasetIndex === 0 || hasMeta || hasInter || hasSuper);
                        }
                    }
                },
                tooltip: {
                    callbacks: {
                        label(item) {
                            if (item.datasetIndex === 0) return ` ${item.parsed.x} matrículas`;
                            return ` ${item.dataset.label}: ${item.parsed.x}`;
                        },
                        afterBody(items) {
                            const a = top[items[0].dataIndex];
                            const mp = a.matriculas_periodo||0, m = a.meta||0,
                                  mi = a.meta_intermediaria||0, s = a.supermeta||0;
                            const tier = _crgmTierLabel(mp, m, mi, s);
                            const lines = [];
                            if (m > 0) {
                                lines.push(`─────────────────`);
                                lines.push(`${Math.round(mp/m*100)}% da meta`);
                                lines.push(`${tier.icon} ${tier.label}`);
                            }
                            return lines;
                        }
                    }
                }
            },
            scales: {
                x: {
                    beginAtZero: true,
                    max: maxVal || undefined,
                    ticks: { color:'#475569', font:{size:10} },
                    grid: { color:'rgba(100,116,139,0.07)' }
                },
                y: {
                    ticks: { color:'#cbd5e1', font:{size:11, weight:'500'} },
                    grid: { display:false }
                }
            }
        }
    });
}

// ── Ciclos ──────────────────────────────────────────────
let _crgmCiclosData = [];

async function _crgmLoadCiclos() {
    try {
        const res = await api('/api/comercial-rgm/ciclos');
        const d = await res.json();
        if (!d.ok) return;
        _crgmCiclosData = d.ciclos || [];
        const sel = document.getElementById('crgm-ciclo');
        const cur = sel.value;
        sel.innerHTML = '<option value="">Todos</option>' +
            _crgmCiclosData.map(c => {
                const label = c.descricao || c.nome;
                const badge = c.ativo ? ' (ativo)' : '';
                return `<option value="${c.id}" ${c.ativo ? 'selected' : ''}>${esc(label)}${badge}</option>`;
            }).join('');
        if (cur) sel.value = cur;
    } catch (e) { console.error('load ciclos', e); }
}

function crgmCicloChanged() {
    const sel = document.getElementById('crgm-ciclo');
    const id = parseInt(sel.value);
    const ciclo = _crgmCiclosData.find(c => c.id === id);
    if (ciclo) {
        document.getElementById('crgm-dt-ini').value = ciclo.dt_inicio;
        document.getElementById('crgm-dt-fim').value = ciclo.dt_fim;
    }
    _crgmLoadTurmas(id || null);
    crgmAtualizar();
}

function crgmToggleNovoCiclo() {
    document.getElementById('crgm-novo-ciclo').classList.toggle('hidden');
}

async function crgmSalvarCiclo() {
    const nome = document.getElementById('crgm-ciclo-nome').value.trim();
    const nivel = document.getElementById('crgm-ciclo-nivel').value;
    const dt_inicio = document.getElementById('crgm-ciclo-ini').value;
    const dt_fim = document.getElementById('crgm-ciclo-fim').value;
    const ativo = document.getElementById('crgm-ciclo-ativo').checked;
    if (!nome || !dt_inicio || !dt_fim) { _crgmErro('Preencha nome, início e fim do ciclo'); return; }
    try {
        const res = await api('/api/comercial-rgm/ciclos', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ nome, nivel, dt_inicio, dt_fim, ativo }),
        });
        const d = await res.json();
        if (d.error) { _crgmErro(d.error); return; }
        document.getElementById('crgm-ciclo-nome').value = '';
        document.getElementById('crgm-ciclo-ini').value = '';
        document.getElementById('crgm-ciclo-fim').value = '';
        document.getElementById('crgm-ciclo-ativo').checked = false;
        document.getElementById('crgm-novo-ciclo').classList.add('hidden');
        await _crgmLoadCiclos();
        _crgmErro('');
    } catch (e) { _crgmErro('Erro: ' + e.message); }
}

// ── Turmas ──────────────────────────────────────────────
let _crgmTurmasData = [];

async function _crgmLoadTurmas(cicloId) {
    try {
        const params = new URLSearchParams();
        if (cicloId) params.set('ciclo_id', cicloId);
        const qs = params.toString() ? `?${params}` : '';
        const res = await api(`/api/comercial-rgm/turmas${qs}`);
        const d = await res.json();
        if (!d.ok) return;
        _crgmTurmasData = d.turmas || [];
        const sel = document.getElementById('crgm-turma');
        const cur = sel.value;

        const grouped = {};
        _crgmTurmasData.forEach(t => {
            const key = t.nivel || 'Outros';
            if (!grouped[key]) grouped[key] = [];
            grouped[key].push(t);
        });

        let html = '<option value="">Todas</option>';
        for (const [nivel, turmas] of Object.entries(grouped)) {
            html += `<optgroup label="${esc(nivel)}">`;
            turmas.forEach(t => {
                html += `<option value="${t.id}">${esc(t.nome)}</option>`;
            });
            html += '</optgroup>';
        }
        sel.innerHTML = html;
        if (cur) sel.value = cur;

        const turmaCicloSel = document.getElementById('crgm-turma-ciclo');
        if (turmaCicloSel) {
            turmaCicloSel.innerHTML = '<option value="">Nenhum</option>' +
                _crgmCiclosData.map(c => `<option value="${c.id}">${esc(c.descricao || c.nome)}</option>`).join('');
        }
    } catch (e) { console.error('load turmas', e); }
}

function crgmTurmaChanged() {
    const sel = document.getElementById('crgm-turma');
    const id = parseInt(sel.value);
    const turma = _crgmTurmasData.find(t => t.id === id);
    if (turma) {
        document.getElementById('crgm-dt-ini').value = turma.dt_inicio;
        document.getElementById('crgm-dt-fim').value = turma.dt_fim;
    }
    crgmAtualizar();
}

function crgmToggleNovaTurma() {
    const panel = document.getElementById('crgm-nova-turma');
    const isHidden = panel.classList.contains('hidden');
    panel.classList.toggle('hidden');
    if (isHidden) {
        const sel = document.getElementById('crgm-turma-ciclo');
        sel.innerHTML = '<option value="">Nenhum</option>' +
            _crgmCiclosData.map(c => `<option value="${c.id}">${esc(c.descricao || c.nome)}</option>`).join('');
    }
}

async function crgmSalvarTurma() {
    const nome = document.getElementById('crgm-turma-nome').value.trim();
    const nivel = document.getElementById('crgm-turma-nivel').value;
    const ciclo_id = document.getElementById('crgm-turma-ciclo').value || null;
    const dt_inicio = document.getElementById('crgm-turma-ini').value;
    const dt_fim = document.getElementById('crgm-turma-fim').value;
    if (!nome || !dt_inicio || !dt_fim) { _crgmErro('Preencha nome, início e fim da turma'); return; }
    try {
        const res = await api('/api/comercial-rgm/turmas', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ nome, nivel, ciclo_id: ciclo_id ? parseInt(ciclo_id) : null, dt_inicio, dt_fim }),
        });
        const d = await res.json();
        if (d.error) { _crgmErro(d.error); return; }
        document.getElementById('crgm-turma-nome').value = '';
        document.getElementById('crgm-turma-ini').value = '';
        document.getElementById('crgm-turma-fim').value = '';
        document.getElementById('crgm-nova-turma').classList.add('hidden');
        await _crgmLoadTurmas();
        _crgmErro('');
    } catch (e) { _crgmErro('Erro: ' + e.message); }
}

// ── Metas (painel) ──────────────────────────────────────
let _crgmMetasCategorias = [];
let _crgmMetasAll = [];

function _crgmCatLabel(catId) {
    const c = _crgmMetasCategorias.find(x => x.id === catId);
    return c ? c.label : catId;
}

const _CAT_COLORS = {
    matriculas:'text-blue-400', inscricoes:'text-cyan-400', valor:'text-emerald-400',
    novos_leads:'text-amber-400', conversao:'text-purple-400',
};

function crgmToggleMetas() {
    const panel = document.getElementById('crgm-metas-panel');
    const isHidden = panel.classList.contains('hidden');
    panel.classList.toggle('hidden');
    if (isHidden) _crgmLoadMetasPanel();
}

async function _crgmLoadMetasPanel() {
    const grid = document.getElementById('crgm-metas-grid');
    const hist = document.getElementById('crgm-metas-historico');
    grid.innerHTML = '<p class="text-slate-500 text-xs col-span-full">Carregando...</p>';
    hist.innerHTML = '';

    const ini = document.getElementById('crgm-dt-ini').value;
    const fim = document.getElementById('crgm-dt-fim').value;
    document.getElementById('crgm-meta-ini').value = ini;
    document.getElementById('crgm-meta-fim').value = fim;

    try {
        const [metasRes, filtersRes] = await Promise.all([
            api('/api/comercial-rgm/metas').then(r=>r.json()),
            api('/api/comercial-rgm/filters').then(r=>r.json()),
        ]);

        // Populate category dropdowns
        if (metasRes.categorias) {
            _crgmMetasCategorias = metasRes.categorias;
            const catSelect = document.getElementById('crgm-meta-cat');
            const histFilter = document.getElementById('crgm-metas-hist-filter');
            catSelect.innerHTML = metasRes.categorias.map(c =>
                `<option value="${c.id}">${esc(c.label)}</option>`
            ).join('');
            histFilter.innerHTML = '<option value="">Todas categorias</option>' +
                metasRes.categorias.map(c => `<option value="${c.id}">${esc(c.label)}</option>`).join('');
        }

        const agentes = filtersRes.ok ? (filtersRes.agentes||[]).filter(a=>!['Admin','T.I','Suporte'].includes(a.name)) : [];
        if (!agentes.length) { grid.innerHTML='<p class="text-slate-500 text-xs col-span-full">Sync agentes primeiro.</p>'; return; }

        const _inp = (uid, uname, cls, ph, color) =>
            `<input type="number" min="0" step="any" data-uid="${uid}" data-uname="${esc(uname)}" placeholder="${ph}"
                class="w-20 text-right text-xs font-mono bg-slate-900/50 border border-slate-700 rounded px-2 py-1 ${color} focus:outline-none ${cls}">`;
        grid.innerHTML = agentes.map(a =>
            `<div class="grid grid-cols-[1fr_auto_auto_auto] items-center gap-2 bg-slate-800/30 rounded-lg px-3 py-1.5">
                <span class="text-xs text-slate-300 truncate">${esc(a.name)}</span>
                ${_inp(a.id, a.name, 'crgm-meta-int', '0', 'text-amber-300 focus:border-amber-500')}
                ${_inp(a.id, a.name, 'crgm-meta-val', '0', 'text-white focus:border-blue-500')}
                ${_inp(a.id, a.name, 'crgm-meta-sup', '0', 'text-emerald-300 focus:border-emerald-500')}
            </div>`
        ).join('');

        _crgmMetasAll = (metasRes.ok && metasRes.metas) ? metasRes.metas : [];
        _crgmRenderHistorico();
    } catch (e) { grid.innerHTML=`<p class="text-red-400 text-xs col-span-full">Erro: ${e.message}</p>`; }
}

function _crgmRenderHistorico(filterCat) {
    const hist = document.getElementById('crgm-metas-historico');
    let metas = _crgmMetasAll;
    if (filterCat) metas = metas.filter(m => m.categoria === filterCat);

    if (!metas.length) {
        hist.innerHTML = '<p class="text-slate-600 text-xs">Nenhuma meta cadastrada.</p>';
        return;
    }

    // Group by (categoria + dt_inicio + dt_fim + descricao)
    const groups = {};
    metas.forEach(m => {
        const key = `${m.categoria}|${m.dt_inicio}|${m.dt_fim}|${m.descricao||''}`;
        if (!groups[key]) groups[key] = { ...m, items: [] };
        groups[key].items.push(m);
    });

    hist.innerHTML = Object.values(groups).map(g => {
        const catColor = _CAT_COLORS[g.categoria] || 'text-slate-400';
        const catLabel = _crgmCatLabel(g.categoria);
        const agentsList = g.items.map(m => {
            const parts = [`M:${m.meta}`];
            if (m.meta_intermediaria > 0) parts.push(`I:${m.meta_intermediaria}`);
            if (m.supermeta > 0) parts.push(`S:${m.supermeta}`);
            return `<span class="inline-flex items-center gap-1 bg-slate-700/40 rounded px-2 py-0.5">
                <b class="text-white">${esc(m.user_name||'?')}</b>
                <span class="text-blue-300">${parts[0]}</span>
                ${parts[1]?`<span class="text-amber-300">${parts[1]}</span>`:''}
                ${parts[2]?`<span class="text-emerald-300">${parts[2]}</span>`:''}
                <button onclick="crgmDeleteMeta(${m.id})" class="text-red-500/60 hover:text-red-400 ml-0.5 text-[10px]" title="Excluir">&times;</button>
            </span>`;
        }).join(' ');
        return `<div class="bg-slate-800/30 rounded-lg px-4 py-3 text-xs border-l-2 ${catColor.replace('text-','border-')}">
            <div class="flex items-center gap-2 mb-1.5">
                <span class="font-semibold ${catColor}">${esc(catLabel)}</span>
                <span class="text-slate-500">${g.dt_inicio} a ${g.dt_fim}</span>
                ${g.descricao ? `<span class="text-slate-600 italic">${esc(g.descricao)}</span>` : ''}
            </div>
            <div class="flex flex-wrap gap-1.5">${agentsList}</div>
        </div>`;
    }).join('');
}

function _crgmFilterHistorico() {
    const cat = document.getElementById('crgm-metas-hist-filter').value;
    _crgmRenderHistorico(cat || null);
}

// ── Histórico de Metas (painel retrátil) ────────────────────────────────
let _crgmHistoricoMetasData = [];
let _crgmHistoricoGrupoAtivo = null;

function crgmToggleHistoricoMetas() {
    const panel = document.getElementById('crgm-historico-metas-panel');
    const btn   = document.getElementById('crgm-btn-historico-metas');
    const isHidden = panel.classList.contains('hidden');
    panel.classList.toggle('hidden');
    if (btn) btn.classList.toggle('border-violet-500', isHidden);
    if (isHidden) _crgmCarregarHistoricoMetas();
}

async function _crgmCarregarHistoricoMetas() {
    const body = document.getElementById('crgm-historico-metas-body');
    body.innerHTML = '<p class="text-slate-500 text-xs animate-pulse">Carregando histórico...</p>';
    _crgmHistoricoGrupoAtivo = null;
    try {
        const res = await api('/api/comercial-rgm/metas');
        const d   = await res.json();
        if (!d.ok) { body.innerHTML = `<p class="text-red-400 text-xs">Erro: ${esc(d.error||'')}</p>`; return; }

        _crgmHistoricoMetasData = d.metas || [];

        const catSel = document.getElementById('crgm-hist-cat-filter');
        if (catSel && d.categorias) {
            catSel.innerHTML = '<option value="">Todas as categorias</option>' +
                d.categorias.map(c => `<option value="${c.id}">${esc(c.label)}</option>`).join('');
        }

        _crgmRenderHistoricoMetas(null);
    } catch(e) {
        body.innerHTML = `<p class="text-red-400 text-xs">Erro: ${esc(e.message)}</p>`;
    }
}

function crgmFiltrarHistoricoMetas() {
    const cat = document.getElementById('crgm-hist-cat-filter').value || null;
    _crgmHistoricoGrupoAtivo = null;
    _crgmRenderHistoricoMetas(cat);
}

function _crgmAplicarDatasMeta(dtIni, dtFim) {
    document.getElementById('crgm-dt-ini').value = dtIni;
    document.getElementById('crgm-dt-fim').value = dtFim;
    crgmAtualizar();
    // Scroll suave de volta ao topo do dashboard
    document.getElementById('crgm-historico-metas-panel').scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function _crgmAbrirGrupo(key) {
    _crgmHistoricoGrupoAtivo = (_crgmHistoricoGrupoAtivo === key) ? null : key;
    _crgmRenderHistoricoMetas(document.getElementById('crgm-hist-cat-filter').value || null);
}

function _crgmRenderHistoricoMetas(filterCat) {
    const body = document.getElementById('crgm-historico-metas-body');
    let metas = _crgmHistoricoMetasData;
    if (filterCat) metas = metas.filter(m => m.categoria === filterCat);

    if (!metas.length) {
        body.innerHTML = '<p class="text-slate-600 text-xs">Nenhuma meta cadastrada.</p>';
        return;
    }

    // Agrupar por (dt_inicio + dt_fim + descricao) — ignora categoria para juntar períodos
    const groups = {};
    metas.forEach(m => {
        const key = `${m.dt_inicio}|${m.dt_fim}|${m.descricao||''}`;
        if (!groups[key]) groups[key] = {
            key, dt_inicio: m.dt_inicio, dt_fim: m.dt_fim,
            descricao: m.descricao || '', categorias: {}, agentes: []
        };
        if (!groups[key].categorias[m.categoria]) groups[key].categorias[m.categoria] = [];
        groups[key].categorias[m.categoria].push(m);
        groups[key].agentes.push(m);
    });

    const sorted = Object.values(groups).sort((a, b) => b.dt_inicio.localeCompare(a.dt_inicio));

    const catColors = {
        matriculas: 'bg-blue-500/20 text-blue-300',
        inscricoes:  'bg-cyan-500/20 text-cyan-300',
        valor:       'bg-emerald-500/20 text-emerald-300',
        novos_leads: 'bg-amber-500/20 text-amber-300',
        conversao:   'bg-purple-500/20 text-purple-300',
    };
    const catLabels = { matriculas:'Matrículas', inscricoes:'Inscrições', valor:'Valor', novos_leads:'Novos Leads', conversao:'Conversão' };
    const fmt = d => d ? d.split('-').reverse().join('/') : '?';

    body.innerHTML = sorted.map(g => {
        const isOpen = _crgmHistoricoGrupoAtivo === g.key;
        const agentesTotal = [...new Map(g.agentes.map(a => [a.user_id, a])).values()];
        const totalMeta = g.agentes.filter(a => a.categoria === 'matriculas').reduce((s, a) => s + (a.meta||0), 0);
        const catBadges = Object.keys(g.categorias).map(cat =>
            `<span class="px-1.5 py-0.5 rounded text-[10px] font-semibold ${catColors[cat]||'bg-slate-700/40 text-slate-400'}">${catLabels[cat]||cat}</span>`
        ).join('');

        // Detalhe expandido por categoria
        let detalhe = '';
        if (isOpen) {
            detalhe = Object.entries(g.categorias).map(([cat, itens]) => {
                const cc = catColors[cat] || 'bg-slate-700/40 text-slate-400';
                const cl = catLabels[cat] || cat;
                const ordenados = [...itens].sort((a,b) => (b.meta||0)-(a.meta||0));
                const linhas = ordenados.map(a => `
                    <tr class="border-b border-slate-700/20 hover:bg-white/[0.02]">
                        <td class="px-4 py-2 text-sm text-white">${esc(a.user_name||'?')}</td>
                        <td class="px-4 py-2 text-right font-mono text-blue-300">${(a.meta||0)>0 ? a.meta : '—'}</td>
                        <td class="px-4 py-2 text-right font-mono text-amber-300">${(a.meta_intermediaria||0)>0 ? a.meta_intermediaria : '—'}</td>
                        <td class="px-4 py-2 text-right font-mono text-emerald-300">${(a.supermeta||0)>0 ? a.supermeta : '—'}</td>
                        <td class="px-4 py-2 text-right">
                            <button onclick="crgmDeleteMeta(${a.id},'historico')" class="text-red-500/40 hover:text-red-400 text-[10px]" title="Excluir">✕</button>
                        </td>
                    </tr>`).join('');
                const totM = ordenados.reduce((s,a)=>s+(a.meta||0),0);
                const totI = ordenados.reduce((s,a)=>s+(a.meta_intermediaria||0),0);
                const totS = ordenados.reduce((s,a)=>s+(a.supermeta||0),0);
                return `<div class="border-t border-slate-700/20">
                    <div class="px-4 py-2 flex items-center gap-2 bg-slate-800/20">
                        <span class="text-[10px] font-semibold px-2 py-0.5 rounded ${cc}">${cl}</span>
                        <span class="text-[10px] text-slate-500">${ordenados.length} agente${ordenados.length!==1?'s':''}</span>
                    </div>
                    <table class="w-full text-xs">
                        <thead><tr class="text-[10px] text-slate-500 uppercase tracking-wider">
                            <th class="px-4 py-1.5 text-left">Agente</th>
                            <th class="px-4 py-1.5 text-right text-blue-400">Meta</th>
                            <th class="px-4 py-1.5 text-right text-amber-400">Interm.</th>
                            <th class="px-4 py-1.5 text-right text-emerald-400">Super</th>
                            <th class="px-4 py-1.5"></th>
                        </tr></thead>
                        <tbody>${linhas}</tbody>
                        <tfoot><tr class="bg-slate-800/30 text-[11px] font-semibold">
                            <td class="px-4 py-1.5 text-slate-500">Total</td>
                            <td class="px-4 py-1.5 text-right font-mono text-blue-300/70">${totM||'—'}</td>
                            <td class="px-4 py-1.5 text-right font-mono text-amber-300/70">${totI||'—'}</td>
                            <td class="px-4 py-1.5 text-right font-mono text-emerald-300/70">${totS||'—'}</td>
                            <td></td>
                        </tr></tfoot>
                    </table>
                </div>`;
            }).join('');

            detalhe = `<div class="border-t border-slate-700/20">${detalhe}
                <div class="px-4 py-3 flex justify-end border-t border-slate-700/20 bg-slate-800/20">
                    <button onclick="_crgmAplicarDatasMeta('${g.dt_inicio}','${g.dt_fim}')"
                        class="flex items-center gap-2 px-4 py-2 rounded-lg bg-violet-600 hover:bg-violet-500 text-white text-xs font-semibold transition-colors">
                        <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z"/></svg>
                        Aplicar período no dashboard
                    </button>
                </div>
            </div>`;
        }

        return `<div class="rounded-xl border ${isOpen ? 'border-violet-500/40' : 'border-slate-700/30'} overflow-hidden transition-all">
            <button onclick="_crgmAbrirGrupo('${g.key}')"
                class="w-full flex items-center justify-between px-4 py-3 bg-slate-800/40 hover:bg-slate-800/60 transition-colors text-left">
                <div class="flex items-center gap-3 flex-wrap">
                    <div class="flex flex-col items-start">
                        <span class="text-white font-semibold text-sm">${fmt(g.dt_inicio)} → ${fmt(g.dt_fim)}</span>
                        ${g.descricao ? `<span class="text-slate-500 text-[11px] italic">${esc(g.descricao)}</span>` : ''}
                    </div>
                    <div class="flex items-center gap-1.5 flex-wrap">${catBadges}</div>
                </div>
                <div class="flex items-center gap-4 shrink-0">
                    <div class="text-right">
                        <p class="text-[10px] text-slate-500 uppercase tracking-wider">Agentes</p>
                        <p class="text-white font-mono font-bold text-sm">${agentesTotal.length}</p>
                    </div>
                    ${totalMeta > 0 ? `<div class="text-right">
                        <p class="text-[10px] text-slate-500 uppercase tracking-wider">Meta mat.</p>
                        <p class="text-blue-300 font-mono font-bold text-sm">${totalMeta}</p>
                    </div>` : ''}
                    <svg class="w-4 h-4 text-slate-500 transition-transform ${isOpen ? 'rotate-180' : ''}" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/>
                    </svg>
                </div>
            </button>
            ${detalhe}
        </div>`;
    }).join('');
}

async function crgmSaveMetas() {
    const dtIni = document.getElementById('crgm-meta-ini').value;
    const dtFim = document.getElementById('crgm-meta-fim').value;
    const desc = document.getElementById('crgm-meta-desc').value || '';
    const cat = document.getElementById('crgm-meta-cat').value || 'matriculas';
    if (!dtIni || !dtFim) { _crgmErro('Defina o período da meta'); return; }

    // Collect values per agent (3 inputs each)
    const metaInputs = document.querySelectorAll('.crgm-meta-val');
    const intInputs = document.querySelectorAll('.crgm-meta-int');
    const supInputs = document.querySelectorAll('.crgm-meta-sup');
    const metas = [];
    metaInputs.forEach((inp, i) => {
        const m = parseFloat(inp.value) || 0;
        const mi = parseFloat(intInputs[i]?.value) || 0;
        const s = parseFloat(supInputs[i]?.value) || 0;
        if (m > 0 || mi > 0 || s > 0) {
            metas.push({
                user_id: parseInt(inp.dataset.uid),
                user_name: inp.dataset.uname || '',
                meta: m, meta_intermediaria: mi, supermeta: s,
                dt_inicio: dtIni, dt_fim: dtFim,
                descricao: desc, categoria: cat,
            });
        }
    });
    if (!metas.length) { _crgmErro('Defina ao menos uma meta > 0'); return; }
    try {
        const res = await api('/api/comercial-rgm/metas', {
            method:'POST', headers:{'Content-Type':'application/json'},
            body: JSON.stringify({metas}),
        });
        const d = await res.json();
        if (d.ok) { document.getElementById('crgm-metas-panel').classList.add('hidden'); crgmAtualizar(); }
        else _crgmErro(d.error||'Erro ao salvar');
    } catch (e) { _crgmErro('Erro: '+e.message); }
}

async function crgmDeleteMeta(id, origem) {
    if (!confirm('Excluir esta meta?')) return;
    try {
        await api(`/api/comercial-rgm/metas/${id}`, {method:'DELETE'});
        if (origem === 'historico') {
            _crgmCarregarHistoricoMetas();
        } else {
            _crgmLoadMetasPanel();
        }
        crgmAtualizar();
    } catch (e) { _crgmErro('Erro: '+e.message); }
}

// ── Congelar ────────────────────────────────────────────
async function crgmToggleCongelar() {
    const panel = document.getElementById('crgm-congelar-panel');
    const wasHidden = panel.classList.contains('hidden');
    panel.classList.toggle('hidden');
    document.getElementById('crgm-congelar-msg').classList.add('hidden');
    if (wasHidden) {
        try {
            const res = await api('/api/comercial-rgm/ciclo-atual');
            const d = await res.json();
            if (d.ok) {
                const info = document.getElementById('crgm-congelar-info');
                const grad = d.ciclos['Graduação'] || '?';
                const pos = d.ciclos['Pós-Graduação'] || '?';
                info.innerHTML = `Ciclo ativo: <b class="text-blue-400">Graduação → ${grad}</b> | <b class="text-purple-400">Pós-Graduação → ${pos}</b>`;
            }
        } catch (e) { console.error('ciclo-atual', e); }
    }
}

async function crgmCongelar() {
    const radios = document.querySelectorAll('input[name="crgm-congelar-nivel"]');
    let nivel = '';
    radios.forEach(r => { if (r.checked) nivel = r.value; });
    if (!nivel) { _crgmErro('Selecione Graduação ou Pós-Graduação'); return; }

    const msgEl = document.getElementById('crgm-congelar-msg');
    msgEl.classList.remove('hidden');
    msgEl.style.color = '#94a3b8';
    msgEl.textContent = `Congelando ${nivel}...`;

    try {
        const res = await api('/api/comercial-rgm/congelar', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ nivel }),
        });
        const d = await res.json();
        if (d.error) {
            msgEl.style.color = '#f87171';
            msgEl.textContent = d.error;
            return;
        }
        msgEl.style.color = '#34d399';
        let msg = `Congelamento OK: ${d.congelados} novos registros de ${d.nivel} congelados (ciclo ${d.ciclo_congelado}).`;
        if (d.proximo_ciclo) msg += ` Próximo ciclo: ${d.proximo_ciclo}`;
        else msg += ` Nenhum próximo ciclo encontrado.`;
        msgEl.textContent = msg;
        await crgmAtualizar();
    } catch (e) {
        msgEl.style.color = '#f87171';
        msgEl.textContent = 'Erro: ' + e.message;
    }
}

// ── Sync Kommo ──────────────────────────────────────────
let _crgmSyncKommoTaskId = null;
let _crgmSyncKommoPollTimer = null;

function crgmToggleSyncKommo() {
    const panel = document.getElementById('crgm-sync-kommo-panel');
    panel.classList.toggle('hidden');
}

async function crgmRunSyncKommo() {
    const btn = document.getElementById('crgm-sync-kommo-btn');
    const icon = document.getElementById('crgm-sync-kommo-icon');
    const progressWrap = document.getElementById('crgm-sync-kommo-progress-wrap');
    const bar = document.getElementById('crgm-sync-kommo-bar');
    const pctEl = document.getElementById('crgm-sync-kommo-pct');
    const logEl = document.getElementById('crgm-sync-kommo-log');

    let mode = 'delta';
    document.querySelectorAll('input[name="crgm-sync-mode"]').forEach(r => { if (r.checked) mode = r.value; });

    btn.disabled = true;
    btn.classList.add('opacity-60');
    icon.classList.add('animate-spin');
    progressWrap.classList.remove('hidden');
    bar.style.width = '0%';
    pctEl.textContent = '0%';
    logEl.innerHTML = '';

    try {
        const res = await api('/api/kommo/sync', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ mode }),
        });
        const d = await res.json();
        if (!d.ok) {
            _crgmSyncKommoErro(d.error || 'Falha ao iniciar sync');
            return;
        }
        _crgmSyncKommoTaskId = d.task_id;
        _crgmSyncKommoPoll();
    } catch (e) {
        _crgmSyncKommoErro('Erro: ' + e.message);
    }
}

function _crgmSyncKommoAppendLog(line, color) {
    const logEl = document.getElementById('crgm-sync-kommo-log');
    const span = document.createElement('div');
    span.style.color = color || '#94a3b8';
    span.textContent = line;
    logEl.appendChild(span);
    logEl.scrollTop = logEl.scrollHeight;
}

async function _crgmSyncKommoPoll() {
    if (!_crgmSyncKommoTaskId) return;
    try {
        const res = await api(`/api/kommo/task/${_crgmSyncKommoTaskId}`);
        const d = await res.json();
        if (!d.ok) { _crgmSyncKommoErro('Tarefa não encontrada'); return; }

        const t = d.data || d;   // suporte a {ok,data:{...}} e {ok,...} direto
        const bar = document.getElementById('crgm-sync-kommo-bar');
        const pctEl = document.getElementById('crgm-sync-kommo-pct');
        const logEl = document.getElementById('crgm-sync-kommo-log');

        bar.style.width = (t.progress || 0) + '%';
        pctEl.textContent = (t.progress || 0) + '%';

        // Mostra novas linhas de log
        const shown = logEl.children.length;
        const allLogs = t.log || [];
        for (let i = shown; i < allLogs.length; i++) {
            _crgmSyncKommoAppendLog(`[${allLogs[i].time}] ${allLogs[i].msg}`);
        }

        if (t.status === 'running') {
            _crgmSyncKommoPollTimer = setTimeout(_crgmSyncKommoPoll, 1500);
        } else if (t.status === 'completed') {
            _crgmSyncKommoFinalizar(true);
        } else {
            _crgmSyncKommoFinalizar(false, t.message);
        }
    } catch (e) {
        _crgmSyncKommoErro('Erro no polling: ' + e.message);
    }
}

function _crgmSyncKommoFinalizar(ok, msg) {
    const btn = document.getElementById('crgm-sync-kommo-btn');
    const icon = document.getElementById('crgm-sync-kommo-icon');
    const bar = document.getElementById('crgm-sync-kommo-bar');
    btn.disabled = false;
    btn.classList.remove('opacity-60');
    icon.classList.remove('animate-spin');
    if (ok) {
        bar.classList.remove('bg-green-500');
        bar.classList.add('bg-emerald-400');
        _crgmSyncKommoAppendLog('✓ Sincronização concluída!', '#34d399');
        setTimeout(() => crgmAtualizar(), 1000);
    } else {
        bar.classList.remove('bg-green-500');
        bar.classList.add('bg-red-500');
        _crgmSyncKommoAppendLog('✗ ' + (msg || 'Falha'), '#f87171');
    }
    _crgmSyncKommoTaskId = null;
}

function _crgmSyncKommoErro(msg) {
    const btn = document.getElementById('crgm-sync-kommo-btn');
    const icon = document.getElementById('crgm-sync-kommo-icon');
    btn.disabled = false;
    btn.classList.remove('opacity-60');
    icon.classList.remove('animate-spin');
    _crgmSyncKommoAppendLog('✗ ' + msg, '#f87171');
}

// ── Sync / Upload ───────────────────────────────────────
async function crgmSyncUsers() {
    const btn = document.getElementById('crgm-btn-sync');
    btn.disabled = true; btn.classList.add('opacity-50');
    try {
        const res = await api('/api/comercial-rgm/sync-users',{method:'POST'});
        const d = await res.json();
        if (d.error) _crgmErro(d.error);
        else { _crgmErro(''); await _crgmLoadFilters(); await crgmAtualizar(); }
    } catch (e) { _crgmErro('Erro: '+e.message); }
    finally { btn.disabled=false; btn.classList.remove('opacity-50'); }
}

async function crgmUpload(input) {
    const file = input.files[0]; if (!file) return; input.value='';
    _crgmLoading(true); _crgmErro('');
    const fd = new FormData(); fd.append('file', file);
    try {
        const res = await api('/api/comercial-rgm/upload',{method:'POST',body:fd});
        const d = await res.json();
        if (d.error) { _crgmErro(d.error); return; }
        if (d.comercial_added !== undefined) {
            _crgmErro(`Upload OK: ${d.snapshot_rows} matriculados processados, ${d.comercial_added} novos registros comerciais adicionados.`);
            const el = document.getElementById('crgm-erro');
            el.classList.remove('hidden'); el.style.color = '#34d399';
        }
        await _crgmLoadFilters(); await _crgmLoadSnapshotInfo(); await crgmAtualizar();
    } catch (e) { _crgmErro('Erro: '+e.message); }
    finally { _crgmLoading(false); }
}

function _crgmLoading(show) { document.getElementById('crgm-loading').classList.toggle('hidden', !show); }
function _crgmErro(msg) { const el=document.getElementById('crgm-erro'); el.textContent=msg; el.classList.toggle('hidden',!msg); }

// ── Atualizar 1 lead Kommo ────────────────────────────────
function crgmToggleKommoLead() {
    const p = document.getElementById('crgm-kommo-lead-panel');
    if (!p) return;
    p.classList.toggle('hidden');
    document.getElementById('crgm-kommo-lead-msg')?.classList.add('hidden');
    document.getElementById('crgm-kommo-lead-pick')?.classList.add('hidden');
}

async function crgmKommoSyncLead(forcedLeadId) {
    const msgEl = document.getElementById('crgm-kommo-lead-msg');
    const pickEl = document.getElementById('crgm-kommo-lead-pick');
    const btn = document.getElementById('crgm-kommo-sync-btn');
    let leadId = forcedLeadId;
    const rgmEl = document.getElementById('crgm-kommo-rgm');
    const idEl = document.getElementById('crgm-kommo-lead-id');
    if (leadId == null) {
        const idVal = (idEl && idEl.value || '').trim();
        const rgmVal = (rgmEl && rgmEl.value || '').trim().replace(/\D/g, '');
        if (idVal) leadId = parseInt(idVal, 10);
        if (pickEl) { pickEl.classList.add('hidden'); pickEl.innerHTML = ''; }
        const body = {};
        if (leadId && !isNaN(leadId)) body.lead_id = leadId;
        else if (rgmVal.length === 8) body.rgm = rgmVal;
        else {
            if (msgEl) {
                msgEl.classList.remove('hidden');
                msgEl.style.color = '#f87171';
                msgEl.textContent = 'Informe o ID do lead ou um RGM com 8 dígitos.';
            }
            return;
        }
        if (!btn) return;
        btn.disabled = true;
        try {
            const res = await api('/api/comercial-rgm/kommo-sync-lead', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
            const d = await res.json();
            if (res.status === 409 && d.lead_ids && d.lead_ids.length) {
                if (msgEl) {
                    msgEl.classList.remove('hidden');
                    msgEl.style.color = '#fbbf24';
                    msgEl.textContent = d.error || 'Vários leads com esse RGM. Clique no ID correto:';
                }
                if (pickEl) {
                    pickEl.classList.remove('hidden');
                    pickEl.innerHTML = d.lead_ids.map(id => `<button type="button" onclick="crgmKommoSyncLead(${id})" class="text-xs px-3 py-1.5 rounded-lg bg-slate-700 hover:bg-emerald-600 text-white">Lead #${id}</button>`).join('');
                }
                btn.disabled = false;
                return;
            }
            if (!d.ok) {
                if (msgEl) {
                    msgEl.classList.remove('hidden');
                    msgEl.style.color = '#f87171';
                    msgEl.textContent = d.error || 'Erro';
                }
                btn.disabled = false;
                return;
            }
            if (idEl) idEl.value = String(d.lead_id);
            if (rgmEl && d.rgm) rgmEl.value = String(d.rgm).replace(/\D/g, '').slice(0, 8);
            if (msgEl) {
                msgEl.classList.remove('hidden');
                msgEl.style.color = '#34d399';
                msgEl.innerHTML = `<strong>OK</strong> — Lead <strong>#${d.lead_id}</strong> (${d.nome_card || '—'}) · RGM: <strong>${d.rgm || '—'}</strong> · ${d.pipeline || 'Pipeline ?'} · ${d.status || ''}. ${d.msg || ''}`;
            }
            await crgmAtualizar();
        } catch (e) {
            if (msgEl) {
                msgEl.classList.remove('hidden');
                msgEl.style.color = '#f87171';
                msgEl.textContent = 'Erro: ' + e.message;
            }
        } finally {
            btn.disabled = false;
        }
        return;
    }
    /* forcedLeadId from duplicate RGM pick */
    if (idEl) idEl.value = String(leadId);
    if (rgmEl) rgmEl.value = '';
    btn.disabled = true;
    try {
        const res = await api('/api/comercial-rgm/kommo-sync-lead', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ lead_id: leadId }) });
        const d = await res.json();
        const msgEl2 = document.getElementById('crgm-kommo-lead-msg');
        const pickEl2 = document.getElementById('crgm-kommo-lead-pick');
        if (pickEl2) { pickEl2.classList.add('hidden'); pickEl2.innerHTML = ''; }
        if (!d.ok) {
            if (msgEl2) { msgEl2.classList.remove('hidden'); msgEl2.style.color = '#f87171'; msgEl2.textContent = d.error || 'Erro'; }
            return;
        }
        if (msgEl2) {
            msgEl2.classList.remove('hidden');
            msgEl2.style.color = '#34d399';
            msgEl2.innerHTML = `<strong>OK</strong> — Lead <strong>#${d.lead_id}</strong> · RGM: <strong>${d.rgm || '—'}</strong> · ${d.pipeline || ''} · ${d.status || ''}.`;
        }
        await crgmAtualizar();
    } catch (e) {
        const msgEl2 = document.getElementById('crgm-kommo-lead-msg');
        if (msgEl2) { msgEl2.classList.remove('hidden'); msgEl2.style.color = '#f87171'; msgEl2.textContent = 'Erro: ' + e.message; }
    } finally {
        btn.disabled = false;
    }
}

// ── Duplicatas ───────────────────────────────────────────
async function _crgmLoadDuplicatas() {
    const btn = document.getElementById('crgm-btn-dup');
    if (btn) { btn.disabled = true; btn.classList.add('opacity-50'); btn.innerHTML = '<svg class="animate-spin w-3.5 h-3.5" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"/><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"/></svg> Carregando...'; }
    try {
        const res = await api('/api/comercial-rgm/duplicatas');
        const d = await res.json();
        if (!d.ok || !d.duplicatas || d.duplicatas.length === 0) {
            document.getElementById('crgm-duplicatas-panel').classList.add('hidden');
            _crgmErro(d.ok ? 'Nenhuma duplicata encontrada!' : (d.error || 'Erro'));
            if (d.ok) { const el = document.getElementById('crgm-erro'); el.style.color = '#34d399'; }
            return;
        }
        _crgmErro('');
        const panel = document.getElementById('crgm-duplicatas-panel');
        panel.classList.remove('hidden');
        document.getElementById('crgm-dup-count').textContent = d.total;
        document.getElementById('crgm-duplicatas-body').classList.remove('hidden');
        document.getElementById('crgm-dup-toggle-text').textContent = 'Recolher';

        const tbody = document.getElementById('crgm-dup-tbody');
        tbody.innerHTML = d.duplicatas.map((dup, i) => {
            const details = dup.leads.map(l => {
                const statusColor = l.status === 'Ganho' ? 'text-emerald-400' :
                                    l.status === 'Perdido' ? 'text-red-400' : 'text-blue-400';
                return `<div class="flex items-center gap-2 text-xs py-0.5">
                    <a href="https://eduitbr.kommo.com/leads/detail/${l.lead_id}" target="_blank"
                       class="text-blue-400 hover:text-blue-300 underline font-mono">#${l.lead_id}</a>
                    <span class="text-slate-400">${l.consultora}</span>
                    <span class="${statusColor} text-[10px] font-semibold px-1.5 py-0.5 rounded bg-slate-800">${l.status}</span>
                    <span class="text-slate-500">${l.pipeline}</span>
                    ${l.preco ? `<span class="text-emerald-400">R$ ${(l.preco/100).toLocaleString('pt-BR')}</span>` : ''}
                </div>`;
            }).join('');
            return `<tr class="hover:bg-slate-800/30 transition-colors">
                <td class="text-center px-3 py-3 text-slate-600 text-xs">${i + 1}</td>
                <td class="px-4 py-3 font-mono text-amber-300 text-xs font-bold">${dup.rgm}</td>
                <td class="text-center px-4 py-3">
                    <span class="bg-amber-500/20 text-amber-300 font-bold text-xs px-2 py-0.5 rounded-full">${dup.count}</span>
                </td>
                <td class="px-4 py-3">${details}</td>
            </tr>`;
        }).join('');
    } catch (e) {
        _crgmErro('Erro ao carregar duplicatas: ' + e.message);
    } finally {
        if (btn) {
            btn.disabled = false; btn.classList.remove('opacity-50');
            btn.innerHTML = '<svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4.5c-.77-.833-2.694-.833-3.464 0L3.34 16.5c-.77.833.192 2.5 1.732 2.5z"/></svg> Duplicatas';
        }
    }
}

function crgmToggleDuplicatas() {
    const body = document.getElementById('crgm-duplicatas-body');
    const text = document.getElementById('crgm-dup-toggle-text');
    const hidden = body.classList.toggle('hidden');
    text.textContent = hidden ? 'Expandir' : 'Recolher';
}
