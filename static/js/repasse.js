/* ═══════════════════════════════════════════════════════
   Repasse de Recebimentos
   ═══════════════════════════════════════════════════════ */

let _repAgentesData       = [];
let _repDetalheData       = [];
let _repFiltrosCarregados = false;
let _repSelectedUid       = null;
let _repTurmasPorCiclo    = {};
let _repIsAdmin           = false;
let _repMyUid             = null;
let _repMyNome            = null;

// Paleta de cores para os avatares
const _repColors = [
    ['bg-violet-500','text-white'], ['bg-blue-500','text-white'],
    ['bg-emerald-500','text-white'], ['bg-amber-500','text-white'],
    ['bg-rose-500','text-white'],   ['bg-cyan-500','text-white'],
    ['bg-indigo-500','text-white'], ['bg-pink-500','text-white'],
    ['bg-teal-500','text-white'],   ['bg-orange-500','text-white'],
];

function _repGetTaxa() {
    const v = parseFloat(document.getElementById('rep-taxa')?.value || '30');
    return isNaN(v) ? 0.30 : v / 100;
}

function _repInitials(nome) {
    const parts = (nome || '').trim().split(/\s+/);
    if (parts.length >= 2) return (parts[0][0] + parts[1][0]).toUpperCase();
    return (nome || '?').substring(0, 2).toUpperCase();
}

function _repColorFor(index) {
    return _repColors[index % _repColors.length];
}

// ---------------------------------------------------------------------------
// Inicialização
// ---------------------------------------------------------------------------
async function repInit() {
    if (!_repFiltrosCarregados) {
        // Carrega papel do usuário
        try {
            const meRes = await api('/api/me');
            const me = await meRes.json();
            _repIsAdmin = me?.role === 'admin';
            _repMyUid   = me?.kommo_user_id || null;
            _repMyNome  = me?.name || me?.username || null;
        } catch(e) { _repIsAdmin = false; }

    await _repCarregarFiltros();
    await _repCarregarTaxa();
    _repFiltrosCarregados = true;
    }

    // Taxa de repasse: apenas admin pode editar
    const taxaInput = document.getElementById('rep-taxa');
    const taxaSalvarBtn = document.getElementById('rep-taxa-salvar-btn');
    if (taxaInput) {
        if (_repIsAdmin) {
            taxaInput.removeAttribute('readonly');
            taxaInput.classList.remove('opacity-50', 'cursor-not-allowed');
            if (taxaSalvarBtn) taxaSalvarBtn.classList.remove('hidden');
        } else {
            taxaInput.setAttribute('readonly', true);
            taxaInput.classList.add('opacity-50', 'cursor-not-allowed');
            if (taxaSalvarBtn) taxaSalvarBtn.classList.add('hidden');
        }
    }

    // Viewer: oculta buscar agente e mostra banner com seu nome
    const searchWrap = document.getElementById('rep-search-wrap');
    if (searchWrap) searchWrap.classList.toggle('hidden', !_repIsAdmin);
    const viewerBanner = document.getElementById('rep-viewer-banner');
    if (viewerBanner) {
        viewerBanner.classList.toggle('hidden', _repIsAdmin);
        if (!_repIsAdmin && _repMyNome) {
            const nomeEl = document.getElementById('rep-viewer-nome');
            if (nomeEl) nomeEl.textContent = _repMyNome;
            else viewerBanner.childNodes[viewerBanner.childNodes.length - 1].textContent = _repMyNome;
        }
    }
}

async function _repCarregarTaxa() {
    try {
        const res = await api('/api/repasse/taxa');
        const d = await res.json();
        const input = document.getElementById('rep-taxa');
        if (input && d.taxa != null) input.value = d.taxa;
    } catch(e) { /* mantém padrão 30 */ }
}

async function repSalvarTaxa() {
    const input = document.getElementById('rep-taxa');
    const taxa = parseFloat(input?.value || '30');
    if (isNaN(taxa) || taxa < 0 || taxa > 100) {
        alert('Taxa inválida. Use um valor entre 0 e 100.');
        return;
    }
    try {
        const res = await api('/api/repasse/taxa', { method: 'PUT', headers: {'Content-Type':'application/json'}, body: JSON.stringify({taxa}) });
        const d = await res.json();
        if (d.ok) {
            repAtualizarTaxa();
        }
    } catch(e) { console.error('Erro ao salvar taxa', e); }
}

async function _repCarregarFiltros() {
    try {
        const res = await api('/api/repasse/filtros');
        const d = await res.json();
        if (!d.ok) return;

        const selCiclo = document.getElementById('rep-ciclo');
        const selTipo  = document.getElementById('rep-tipo');

        while (selCiclo.options.length > 1) selCiclo.remove(1);
        while (selTipo.options.length > 1)  selTipo.remove(1);

        d.ciclos.forEach(c => { const o = document.createElement('option'); o.value = o.textContent = c; selCiclo.appendChild(o); });
        d.tipos.forEach(t => { const o = document.createElement('option'); o.value = o.textContent = t; selTipo.appendChild(o); });
        // Padrão: só mensalidade (filtro único oferecido pela API)
        if (selTipo && d.tipos && d.tipos.includes('Mensalidade')) selTipo.value = 'Mensalidade';

        _repTurmasPorCiclo = d.turmas_por_ciclo || {};
        repAtualizarTurmas();
    } catch(e) {
        console.error('repasse filtros error', e);
    }
}

// ---------------------------------------------------------------------------
// Buscar e montar carrossel
// ---------------------------------------------------------------------------
async function repLoad() {
    const ciclo = document.getElementById('rep-ciclo')?.value || '';
    const tipo  = document.getElementById('rep-tipo')?.value  || '';
    const turma = document.getElementById('rep-turma')?.value || '';

    const errEl = document.getElementById('rep-empty-err');
    if (errEl) { errEl.classList.add('hidden'); errEl.textContent = ''; }

    _repSetState('loading');
    repFecharDetalhe();
    _repSelectedUid = null;

    try {
        let qs = '';
        if (ciclo) qs += `ciclo=${encodeURIComponent(ciclo)}&`;
        if (tipo)  qs += `tipo=${encodeURIComponent(tipo)}&`;
        if (turma) qs += `turma=${encodeURIComponent(turma)}&`;

        const res = await api(`/api/repasse/agentes?${qs}`);
        let d;
        try {
            d = await res.json();
        } catch (parseErr) {
            throw new Error(
                res.ok
                    ? 'Resposta inválida do servidor (não é JSON). Verifique proxy/logs.'
                    : `Erro HTTP ${res.status}. Verifique o servidor ou o login.`
            );
        }
        if (!res.ok) throw new Error(d.error || d.message || `Erro HTTP ${res.status}`);
        if (!d.ok) throw new Error(d.error || 'Erro');

        _repAgentesData = d.agentes || [];
        _repAtualizarKpis(d.totais || {});

        if (!_repIsAdmin && _repAgentesData.length === 1) {
            // Viewer: vai direto para o detalhe do único agente retornado
            _repSetState('carrossel');
            _repRenderCarrossel(_repAgentesData);
            const a = _repAgentesData[0];
            repSelecionarAgente(String(a.id), a.nome, 0);
        } else {
            _repRenderCarrossel(_repAgentesData);
            _repSetState(_repAgentesData.length ? 'carrossel' : 'empty', null);
        }
    } catch(e) {
        const msg = e && e.message ? String(e.message) : 'Falha ao carregar';
        _repSetState('empty', msg);
        console.error('repasse load error', e);
    }
}

function _repAtualizarKpis(totais) {
    const taxa = _repGetTaxa();
    const totalValor   = totais.valor   || 0;
    const totalRepasse = totalValor * taxa;

    document.getElementById('rep-kpi-valor').textContent    = _repFmtMoeda(totalValor);
    document.getElementById('rep-kpi-repasse').textContent  = _repFmtMoeda(totalRepasse);
    document.getElementById('rep-kpi-alunos').textContent   = (totais.alunos  || 0).toLocaleString('pt-BR');
    document.getElementById('rep-kpi-agentes').textContent  = (totais.agentes || 0).toLocaleString('pt-BR');
    document.getElementById('rep-kpis').classList.remove('hidden');
    // Viewer não vê o KPI de total recebido nem de agentes (só vê o próprio)
    document.getElementById('rep-kpi-valor-wrap')?.classList.toggle('hidden', !_repIsAdmin);
    document.getElementById('rep-kpi-agentes-wrap')?.classList.toggle('hidden', !_repIsAdmin);

    // Labels de taxa (badges menores, não o KPI title)
    const taxaPct = (taxa * 100 % 1 === 0) ? (taxa * 100).toFixed(0) : (taxa * 100).toFixed(1);
    document.querySelectorAll('.rep-taxa-col').forEach(el => el.textContent = taxaPct);
}

function repAtualizarTurmas() {
    const ciclo   = document.getElementById('rep-ciclo')?.value || '';
    const selTurma = document.getElementById('rep-turma');
    if (!selTurma) return;

    // Limpa turmas atuais
    while (selTurma.options.length > 1) selTurma.remove(1);

    const turmas = ciclo
        ? (_repTurmasPorCiclo[ciclo] || [])
        : Object.values(_repTurmasPorCiclo).flat().filter((v, i, a) => a.indexOf(v) === i).sort();

    turmas.forEach(t => {
        const o = document.createElement('option');
        o.value = o.textContent = t;
        selTurma.appendChild(o);
    });
}

function repAtualizarTaxa() {
    if (!_repAgentesData.length) return;
    const totalValor   = _repAgentesData.reduce((s, a) => s + (a.total_valor  || 0), 0);
    const totalAlunos  = _repAgentesData.reduce((s, a) => s + (a.qtd_alunos   || 0), 0);
    const totalAgentes = _repAgentesData.filter(a => a.id).length;
    _repAtualizarKpis({ valor: totalValor, alunos: totalAlunos, agentes: totalAgentes });
    _repRenderCarrossel(_repAgentesData); // re-render com nova taxa
    // Se tem detalhe aberto, atualiza também
    if (_repSelectedUid !== null && _repDetalheData.length) {
        _repRenderDetalhe(_repDetalheData);
    }
}

function repReset() {
    document.getElementById('rep-ciclo').value  = '';
    const st = document.getElementById('rep-tipo');
    if (st && [...st.options].some(o => o.value === 'Mensalidade')) st.value = 'Mensalidade';
    else if (st) st.value = '';
    document.getElementById('rep-turma').value  = '';
    document.getElementById('rep-search').value = '';
    repAtualizarTurmas();
    _repAgentesData  = [];
    _repSelectedUid  = null;
    document.getElementById('rep-kpis').classList.add('hidden');
    repFecharDetalhe();
    _repSetState('empty', null);
}

// ---------------------------------------------------------------------------
// Carrossel
// ---------------------------------------------------------------------------
function _repRenderCarrossel(agentes) {
    const wrap = document.getElementById('rep-carrossel');
    if (!wrap) return;

    const taxa = _repGetTaxa();

    if (!agentes.length) {
        wrap.innerHTML = `<div class="text-xs text-slate-500 py-4 px-2">Nenhum consultor encontrado.</div>`;
        return;
    }

    // Calcula o maior valor para barra de progresso relativa
    const maxRepasse = Math.max(...agentes.map(a => (a.total_valor || 0) * taxa), 1);

    wrap.innerHTML = agentes.map((a, i) => {
        const [bgClass, textClass] = _repColorFor(i);
        const initials = _repInitials(a.nome);
        const repasse  = (a.total_valor || 0) * taxa;
        const isSelected = String(a.id) === String(_repSelectedUid);
        const baraPct = Math.round((repasse / maxRepasse) * 100);
        const borderClass = isSelected
            ? 'border-2 border-purple-400 shadow-lg shadow-purple-500/20'
            : 'border border-slate-700/50 hover:border-purple-500/40 hover:shadow-md hover:shadow-purple-500/10';
        const bgCard = isSelected ? 'bg-gradient-to-b from-purple-900/30 to-slate-800/80' : 'bg-slate-800/60 hover:bg-slate-800/80';

        return `<div class="rep-card flex-shrink-0 w-48 rounded-2xl p-4 cursor-pointer transition-all duration-200 ${bgCard} ${borderClass} snap-start group"
                     data-uid="${a.id || ''}"
                     data-nome="${(a.nome||'').toLowerCase()}"
                     onclick="repSelecionarAgente('${a.id || ''}', '${(a.nome||'').replace(/'/g,"\\'")}', ${i})">
            <!-- Avatar com rank -->
            <div class="relative flex justify-center mb-3">
                <div class="w-14 h-14 rounded-2xl ${bgClass} ${textClass} flex items-center justify-center text-xl font-bold select-none shadow-lg ring-2 ${isSelected ? 'ring-purple-400/60' : 'ring-white/5 group-hover:ring-white/10'} transition-all">
                    ${initials}
                </div>
                ${i === 0 ? '<span class="absolute -top-1 -right-1 w-5 h-5 bg-amber-400 rounded-full flex items-center justify-center text-[9px] font-bold text-amber-900">1</span>' : ''}
            </div>
            <!-- Nome -->
            <p class="text-xs font-bold text-slate-200 text-center truncate mb-1" title="${a.nome || '—'}">${a.nome || '—'}</p>
            <p class="text-[9px] text-slate-500 text-center mb-3">${(a.qtd_alunos||0).toLocaleString('pt-BR')} alunos</p>
            <!-- Barra de progresso -->
            <div class="h-1 rounded-full bg-slate-700/60 mb-2.5 overflow-hidden">
                <div class="h-full rounded-full bg-gradient-to-r from-amber-500 to-amber-400 transition-all duration-500" style="width:${baraPct}%"></div>
            </div>
            <!-- Stats -->
            <div class="space-y-1.5">
                ${_repIsAdmin ? `
                <div class="flex justify-between items-center text-[10px]">
                    <span class="text-slate-500 flex items-center gap-0.5"><span class="material-symbols-outlined text-[10px]">account_balance_wallet</span> Recebido</span>
                    <span class="text-emerald-400 font-mono text-[10px]">${_repFmtMoeda(a.total_valor)}</span>
                </div>` : ''}
                <div class="flex justify-between items-center text-[10px]">
                    <span class="text-amber-400/80 flex items-center gap-0.5"><span class="material-symbols-outlined text-[10px]">savings</span> Repasse</span>
                    <span class="text-amber-400 font-mono font-bold">${_repFmtMoeda(repasse)}</span>
                </div>
            </div>
            ${isSelected ? `
            <div class="mt-3 flex items-center justify-center gap-1 text-[9px] text-purple-400 bg-purple-500/10 border border-purple-500/20 rounded-full py-0.5">
                <span class="material-symbols-outlined text-[10px]">check_circle</span>
                <span class="font-semibold uppercase tracking-wider">Selecionado</span>
            </div>` : `
            <div class="mt-3 flex items-center justify-center gap-1 text-[9px] text-slate-600 group-hover:text-slate-400 transition-colors">
                <span class="material-symbols-outlined text-[10px]">open_in_new</span>
                <span>Ver detalhe</span>
            </div>`}
        </div>`;
    }).join('');
}

function repFilter() {
    const q = (document.getElementById('rep-search')?.value || '').toLowerCase();
    document.querySelectorAll('.rep-card').forEach(card => {
        card.style.display = !q || card.dataset.nome.includes(q) ? '' : 'none';
    });
}

// Navegação com setas
function repCarrosselNav(dir) {
    const wrap = document.getElementById('rep-carrossel');
    if (!wrap) return;
    wrap.scrollBy({ left: dir * 220, behavior: 'smooth' });
}

// ---------------------------------------------------------------------------
// Selecionar consultor (clique no card)
// ---------------------------------------------------------------------------
function repSelecionarAgente(uid, nome, colorIndex) {
    if (!uid) return;

    _repSelectedUid = uid;

    // Atualiza visual dos cards
    _repRenderCarrossel(_repAgentesData);

    // Abre detalhe
    const agente = _repAgentesData.find(a => String(a.id) === String(uid));
    const repasseTotal = agente ? (agente.total_valor || 0) * _repGetTaxa() : 0;
    repVerDetalhe(uid, nome, colorIndex, repasseTotal);
}

// ---------------------------------------------------------------------------
// Detalhe do consultor
// ---------------------------------------------------------------------------
async function repVerDetalhe(kommoUid, nome, colorIndex, repasseTotal) {
    const panel  = document.getElementById('rep-detalhe-panel');
    const loadEl = document.getElementById('rep-detalhe-loading');
    const tbody  = document.getElementById('rep-detalhe-tbody');

    // Monta header do consultor
    const [bgClass, textClass] = _repColorFor(colorIndex || 0);
    const avatarEl = document.getElementById('rep-detalhe-avatar');
    if (avatarEl) {
        avatarEl.className = `w-12 h-12 rounded-xl flex items-center justify-center text-lg font-bold text-white flex-shrink-0 ${bgClass} ${textClass}`;
        avatarEl.textContent = _repInitials(nome);
    }
    const nomeEl = document.getElementById('rep-detalhe-nome');
    if (nomeEl) nomeEl.textContent = nome;

    if (panel) panel.classList.remove('hidden');
    if (loadEl) loadEl.classList.remove('hidden');
    if (tbody) tbody.innerHTML = '';
    document.getElementById('rep-detalhe-total').textContent   = '';
    document.getElementById('rep-detalhe-repasse').textContent = _repFmtMoeda(repasseTotal || 0);
    document.getElementById('rep-detalhe-qtd').textContent     = '...';
    document.getElementById('rep-detalhe-search').value = '';

    const ciclo = document.getElementById('rep-ciclo')?.value || '';
    const tipo  = document.getElementById('rep-tipo')?.value  || '';
    const turma = document.getElementById('rep-turma')?.value || '';

    try {
        let qs = `kommo_uid=${kommoUid}`;
        if (ciclo) qs += `&ciclo=${encodeURIComponent(ciclo)}`;
        if (tipo)  qs += `&tipo=${encodeURIComponent(tipo)}`;
        if (turma) qs += `&turma=${encodeURIComponent(turma)}`;

        const res = await api(`/api/repasse/detalhe?${qs}`);
        const d   = await res.json();
        if (!d.ok) throw new Error(d.error || 'Erro');

        _repDetalheData = d.alunos || [];
        if (loadEl) loadEl.classList.add('hidden');
        _repRenderDetalhe(_repDetalheData);

        const totalRecebido = d.total || 0;
        const totalRepasse  = totalRecebido * _repGetTaxa();
        // Viewers não veem o total recebido
        const totalEl = document.getElementById('rep-detalhe-total');
        const totalWrap = document.getElementById('rep-detalhe-total-wrap');
        if (totalEl) totalEl.textContent = _repFmtMoeda(totalRecebido);
        if (totalWrap) totalWrap.classList.toggle('hidden', !_repIsAdmin);
        document.getElementById('rep-detalhe-repasse').textContent = _repFmtMoeda(totalRepasse);
        document.getElementById('rep-detalhe-qtd').textContent     = _repDetalheData.length.toLocaleString('pt-BR');

        panel.scrollIntoView({ behavior: 'smooth', block: 'start' });
    } catch(e) {
        if (loadEl) loadEl.classList.add('hidden');
        if (tbody) tbody.innerHTML = `<tr><td colspan="6" class="py-4 text-center text-red-400 text-xs">Erro ao carregar: ${e.message}</td></tr>`;
    }
}

function _repRenderDetalhe(alunos) {
    const tbody = document.getElementById('rep-detalhe-tbody');
    if (!tbody) return;
    if (!alunos.length) {
        tbody.innerHTML = `<tr><td colspan="6" class="py-6 text-center text-slate-500 text-xs">Nenhum aluno encontrado</td></tr>`;
        return;
    }
    const taxa = _repGetTaxa();
    // Atualiza visibilidade da coluna "Valor Recebido" no thead
    document.querySelectorAll('.rep-col-recebido').forEach(el => {
        el.classList.toggle('hidden', !_repIsAdmin);
    });
    tbody.innerHTML = alunos.map(a => {
        const repasse = (a.valor || 0) * taxa;
        return `<tr class="border-b border-slate-800/50 hover:bg-slate-800/20 transition-colors rep-detalhe-row"
            data-search="${(a.rgm||'').toLowerCase()} ${(a.tipo_pagamento||'').toLowerCase()} ${(a.turma||'').toLowerCase()}">
            <td class="py-1.5 px-4 font-mono text-slate-400">${a.rgm || '—'}</td>
            <td class="py-1.5 px-4 text-slate-300">${a.tipo_pagamento || '—'}</td>
            <td class="py-1.5 px-4 text-slate-400">${a.turma || '—'}</td>
            <td class="py-1.5 px-4 text-slate-400">${a.ciclo || '—'}</td>
            ${_repIsAdmin ? `<td class="py-1.5 px-4 text-right text-emerald-400 font-mono rep-col-recebido">${_repFmtMoeda(a.valor)}</td>` : ''}
            <td class="py-1.5 px-4 text-right text-amber-400 font-mono font-semibold">${_repFmtMoeda(repasse)}</td>
        </tr>`;
    }).join('');
}

function repFilterDetalhe() {
    const q = (document.getElementById('rep-detalhe-search')?.value || '').toLowerCase();
    document.querySelectorAll('.rep-detalhe-row').forEach(row => {
        row.style.display = !q || row.dataset.search.includes(q) ? '' : 'none';
    });
}

function repFecharDetalhe() {
    document.getElementById('rep-detalhe-panel')?.classList.add('hidden');
    _repDetalheData = [];
    _repSelectedUid = null;
}

// ---------------------------------------------------------------------------
// Estado da UI
// ---------------------------------------------------------------------------
function _repSetState(state, errMsg) {
    document.getElementById('rep-loading').classList.toggle('hidden',       state !== 'loading');
    document.getElementById('rep-empty').classList.toggle('hidden',         state !== 'empty');
    document.getElementById('rep-carrossel-wrap').classList.toggle('hidden', state !== 'carrossel');

    const titleEl = document.getElementById('rep-empty-title');
    const subEl   = document.getElementById('rep-empty-sub');
    const errEl   = document.getElementById('rep-empty-err');
    if (state === 'empty' && errMsg && titleEl && subEl && errEl) {
        titleEl.textContent = 'Não foi possível carregar';
        subEl.classList.add('hidden');
        errEl.textContent = errMsg;
        errEl.classList.remove('hidden');
    } else if (titleEl && subEl && errEl) {
        titleEl.textContent = 'Nenhum dado carregado';
        subEl.classList.remove('hidden');
        errEl.classList.add('hidden');
        errEl.textContent = '';
    }
}

function _repFmtMoeda(v) {
    return new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(v || 0);
}
