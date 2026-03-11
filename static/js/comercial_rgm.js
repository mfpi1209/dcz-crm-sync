// ---------------------------------------------------------------------------
// Dashboard Comercial
// ---------------------------------------------------------------------------

let _crgmChartEvolucao = null;
let _crgmChartAgentes = null;

async function loadComercialRgm() {
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
    _crgmLoading(true); _crgmErro('');
    const qs = new URLSearchParams();
    if (polo) qs.set('polo', polo);
    if (nivel) qs.set('nivel', nivel);
    if (dtIni) qs.set('dt_ini', dtIni);
    if (dtFim) qs.set('dt_fim', dtFim);
    try {
        const res = await api(`/api/comercial-rgm/data?${qs}`);
        const d = await res.json();
        if (!d.ok) { _crgmErro(d.error || 'Erro'); return; }
        _crgmRenderKPIs(d.kpis);
        _crgmRenderEvolucao(d.evolucao, d.evolucao_prev || []);
        _crgmRenderPoloTable(d.ranking_polo);
        _crgmRenderCicloTable(d.ranking_ciclo);
        _crgmRenderAgentes(d.ranking_agentes || []);
        _crgmRenderAgentesChart(d.ranking_agentes || []);
    } catch (e) { _crgmErro('Erro: ' + e.message); }
    finally { _crgmLoading(false); }
}

// ── KPIs ────────────────────────────────────────────────
function _crgmRenderKPIs(k) {
    document.getElementById('crgm-vendas').textContent = k.vendas.toLocaleString('pt-BR');
    document.getElementById('crgm-ytd').textContent = k.vendas_ytd.toLocaleString('pt-BR');
    document.getElementById('crgm-media').textContent = k.media_diaria.toLocaleString('pt-BR');
    document.getElementById('crgm-ticket').textContent = 'R$ ' + k.ticket_medio.toLocaleString('pt-BR', {minimumFractionDigits:0, maximumFractionDigits:0});
    document.getElementById('crgm-valor-total').textContent = k.valor_total >= 1000 ? 'R$ ' + (k.valor_total/1000).toFixed(0) + 'k' : 'R$ ' + k.valor_total.toLocaleString('pt-BR');
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
        const rowBg = i<3 ? 'bg-blue-500/[0.03]' : '';

        // Tooltip with all category metas
        const mc = a.metas_cat || {};
        let tooltip = Object.entries(mc).length > 0
            ? Object.entries(mc).map(([cat, v]) =>
                `${_crgmCatLabel(cat)}: M=${v.meta} I=${v.intermediaria} S=${v.supermeta}`
            ).join(' | ')
            : 'Sem meta definida';

        return `<tr class="hover:bg-white/[0.03] transition-colors ${rowBg}" title="${tooltip}">
            <td class="text-center px-3 py-2.5 font-bold text-slate-400">${rank}</td>
            <td class="px-4 py-2.5 font-medium ${a.nome&&a.nome.startsWith('User #')?'text-slate-500 italic':'text-white'}">${esc(a.nome)}</td>
            <td class="px-4 py-2.5 text-right font-mono text-blue-400 font-semibold">${mp.toLocaleString('pt-BR')}</td>
            <td class="px-4 py-2.5 text-right font-mono text-blue-300/70">${meta>0?meta:'\u2014'}</td>
            <td class="px-4 py-2.5 text-right font-mono text-amber-300/70">${intermediaria>0?intermediaria:'\u2014'}</td>
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

// ── Agentes (chart) ─────────────────────────────────────
function _crgmRenderAgentesChart(agentes) {
    const ctx = document.getElementById('crgm-chart-agentes');
    if (_crgmChartAgentes) _crgmChartAgentes.destroy();
    if (!agentes || !agentes.length) { _crgmChartAgentes = null; return; }
    const agenteFilter = document.getElementById('crgm-agente').value;
    let data = agenteFilter ? agentes.filter(a=>String(a.user_id)===agenteFilter) : [...agentes];
    data = data.filter(a=>(a.matriculas_periodo||0)>0||(a.meta||0)>0);
    data.sort((a,b)=>(a.matriculas_periodo||0)-(b.matriculas_periodo||0));
    const top = data.slice(-12);
    const labels = top.map(a=>a.nome||`#${a.user_id}`);
    const ds = [{
        label:'Matrículas', data:top.map(a=>a.matriculas_periodo||0),
        backgroundColor:'#3b82f6', borderColor:'#2563eb', borderWidth:1, borderRadius:4, barPercentage:0.5, categoryPercentage:0.85,
    }];
    const hasMeta = top.some(a=>(a.meta||0)>0);
    const hasInter = top.some(a=>(a.meta_intermediaria||0)>0);
    const hasSuper = top.some(a=>(a.supermeta||0)>0);
    if (hasMeta) ds.push({
        label:'Meta', data:top.map(a=>a.meta||0),
        backgroundColor:'transparent', borderColor:'#60a5fa', borderWidth:2, borderDash:[4,3], borderRadius:4,
        barPercentage:0.5, categoryPercentage:0.85, type:'bar',
    });
    if (hasInter) ds.push({
        label:'Intermediária', data:top.map(a=>a.meta_intermediaria||0),
        backgroundColor:'transparent', borderColor:'#fbbf24', borderWidth:2, borderDash:[6,3], borderRadius:4,
        barPercentage:0.5, categoryPercentage:0.85, type:'bar',
    });
    if (hasSuper) ds.push({
        label:'Supermeta', data:top.map(a=>a.supermeta||0),
        backgroundColor:'transparent', borderColor:'#34d399', borderWidth:2, borderDash:[8,4], borderRadius:4,
        barPercentage:0.5, categoryPercentage:0.85, type:'bar',
    });
    _crgmChartAgentes = new Chart(ctx, {type:'bar', data:{labels, datasets:ds}, options:{
        indexAxis:'y', responsive:true, maintainAspectRatio:false,
        plugins:{
            legend:{position:'top',align:'end',labels:{color:'#94a3b8',font:{size:10},boxWidth:10,padding:12}},
            tooltip:{callbacks:{afterBody:function(items){
                const a=top[items[0].dataIndex];
                const mp=a.matriculas_periodo||0, m=a.meta||0, mi=a.meta_intermediaria||0, s=a.supermeta||0;
                const tier = _crgmTierLabel(mp, m, mi, s);
                const lines = [`Conv.: ${a.taxa_conversao}%`];
                if(m>0) lines.push(`Meta: ${m} | Interm.: ${mi||'\u2014'} | Super: ${s||'\u2014'}`);
                lines.push(`N\u00edvel: ${tier.icon} ${tier.label}`);
                return lines;
            }}}
        },
        scales:{ x:{beginAtZero:true,ticks:{color:'#64748b',font:{size:10}},grid:{color:'rgba(100,116,139,0.08)'}}, y:{ticks:{color:'#e2e8f0',font:{size:11,weight:500}},grid:{display:false}} }
    }});
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
                ${_inp(a.id, a.name, 'crgm-meta-val', '0', 'text-white focus:border-blue-500')}
                ${_inp(a.id, a.name, 'crgm-meta-int', '0', 'text-amber-300 focus:border-amber-500')}
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

async function crgmDeleteMeta(id) {
    if (!confirm('Excluir esta meta?')) return;
    try {
        await api(`/api/comercial-rgm/metas/${id}`, {method:'DELETE'});
        _crgmLoadMetasPanel();
        crgmAtualizar();
    } catch (e) { _crgmErro('Erro: '+e.message); }
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
        _crgmErro(''); await _crgmLoadFilters(); await _crgmLoadSnapshotInfo(); await crgmAtualizar();
    } catch (e) { _crgmErro('Erro: '+e.message); }
    finally { _crgmLoading(false); }
}

function _crgmLoading(show) { document.getElementById('crgm-loading').classList.toggle('hidden', !show); }
function _crgmErro(msg) { const el=document.getElementById('crgm-erro'); el.textContent=msg; el.classList.toggle('hidden',!msg); }
