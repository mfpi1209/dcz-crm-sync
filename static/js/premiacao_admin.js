/* ═══════════════  Premiação Admin  ═══════════════ */

const _paFmt = v => Number(v||0).toLocaleString('pt-BR',{style:'currency',currency:'BRL'});
const _paDias = ['Seg','Ter','Qua','Qui','Sex','Sáb','Dom'];
let _paCampanhasData = [];
let _paAgentes = [];
let _paGruposData = [];

function _paFmtDateBR(d) {
    if (!d) return '';
    const p = d.split('-');
    return p.length === 3 ? `${p[2]}/${p[1]}/${p[0]}` : d;
}

/* ── Entry ── */
async function loadPremiacaoAdmin() {
    try {
        const [,agRaw] = await Promise.all([
            _paLoadCampanhas(),
            api('/api/minha-performance/agentes'),
        ]);
        const agRes = await agRaw.json();
        _paAgentes = agRes?.agentes || [];
    } catch(e) { console.error('loadPremiacaoAdmin', e); }
}

/* ═══ A) Campanhas ═══ */

async function _paLoadCampanhas() {
    const raw = await api('/api/premiacao/campanhas');
    const res = await raw.json();
    _paCampanhasData = res?.campanhas || [];
    _paRenderCampanhasList();
    _paFillCampanhaSelects();
}

function _paRenderCampanhasList() {
    const wrap = document.getElementById('pa-campanhas-list');
    if (!wrap) return;
    if (!_paCampanhasData.length) { wrap.innerHTML = '<p class="text-xs text-slate-600">Nenhuma campanha criada</p>'; return; }
    wrap.innerHTML = _paCampanhasData.map(c => {
        const tiers = c.tiers || {};
        const badge = c.ativa
            ? '<span class="px-2 py-0.5 text-[10px] rounded-full bg-emerald-500/20 text-emerald-400">Ativa</span>'
            : '<span class="px-2 py-0.5 text-[10px] rounded-full bg-slate-500/20 text-slate-400">Inativa</span>';
        return `<div class="bg-slate-800/40 rounded-xl p-4 border border-slate-700/30 flex flex-col sm:flex-row sm:items-center justify-between gap-3">
            <div class="flex-1 min-w-0">
                <div class="flex items-center gap-2 mb-1">
                    <span class="text-sm font-semibold text-white truncate">${c.nome}</span>
                    ${badge}
                </div>
                <p class="text-[10px] text-slate-500">${_paFmtDateBR(c.dt_inicio)} — ${_paFmtDateBR(c.dt_fim)}</p>
                <p class="text-[10px] text-slate-500 mt-0.5">Inter: ${_paFmt(tiers.intermediaria||0)} · Meta: ${_paFmt(tiers.meta||0)} · Super: ${_paFmt(tiers.supermeta||0)}</p>
            </div>
            <div class="flex items-center gap-1.5 flex-shrink-0">
                <button onclick="paEditCampanha(${c.id})" class="text-[10px] px-2.5 py-1 rounded-lg border border-slate-600/40 text-slate-400 hover:text-white hover:border-slate-500 transition-all">Editar</button>
                <button onclick="paToggleCampanha(${c.id},${c.ativa})" class="text-[10px] px-2.5 py-1 rounded-lg border border-slate-600/40 text-slate-400 hover:text-white hover:border-slate-500 transition-all">${c.ativa?'Desativar':'Ativar'}</button>
                <button onclick="paDeleteCampanha(${c.id})" class="text-[10px] px-2.5 py-1 rounded-lg border border-red-500/30 text-red-400 hover:bg-red-500/10 transition-all">Excluir</button>
            </div>
        </div>`;
    }).join('');
}

function _paFillCampanhaSelects() {
    const ids = ['pa-metas-camp', 'pa-grupo-camp', 'pa-daily-camp'];
    ids.forEach(id => {
        const el = document.getElementById(id);
        if (!el) return;
        const prev = el.value;
        el.innerHTML = '<option value="">Selecionar campanha...</option>' +
            _paCampanhasData.map(c => `<option value="${c.id}">${c.nome} (${_paFmtDateBR(c.dt_inicio)} – ${_paFmtDateBR(c.dt_fim)})</option>`).join('');
        if (prev) el.value = prev;
    });
}

async function paSaveCampanha() {
    const nome = document.getElementById('pa-camp-nome')?.value?.trim();
    const dt_inicio = document.getElementById('pa-camp-ini')?.value;
    const dt_fim = document.getElementById('pa-camp-fim')?.value;
    if (!nome || !dt_inicio || !dt_fim) { toast('Preencha nome e datas', 'error'); return; }

    const tiers = {};
    const iv = parseFloat(document.getElementById('pa-camp-tier-inter')?.value || 0);
    const mv = parseFloat(document.getElementById('pa-camp-tier-meta')?.value || 0);
    const sv = parseFloat(document.getElementById('pa-camp-tier-super')?.value || 0);
    if (iv > 0) tiers.intermediaria = iv;
    if (mv > 0) tiers.meta = mv;
    if (sv > 0) tiers.supermeta = sv;

    const receb_regras = [];
    const rModo = document.getElementById('pa-camp-receb-modo')?.value || 'percentual';
    const rVal = parseFloat(document.getElementById('pa-camp-receb-valor')?.value || 0);
    if (rVal > 0) receb_regras.push({ tier: 'qualquer', modo: rModo, valor: rVal });

    const raw = await api('/api/premiacao/campanhas', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({ nome, dt_inicio, dt_fim, tiers, receb_regras }) });
    const res = await raw.json();
    if (res?.ok) {
        toast('Campanha criada!');
        document.getElementById('pa-camp-nome').value = '';
        document.getElementById('pa-camp-ini').value = '';
        document.getElementById('pa-camp-fim').value = '';
        ['pa-camp-tier-inter','pa-camp-tier-meta','pa-camp-tier-super','pa-camp-receb-valor'].forEach(id => { const e = document.getElementById(id); if(e) e.value = ''; });
        await _paLoadCampanhas();
    } else { toast(res?.error || 'Erro ao criar', 'error'); }
}

async function paToggleCampanha(id, ativa) {
    await api(`/api/premiacao/campanhas/${id}`, { method:'PUT', headers:{'Content-Type':'application/json'}, body:JSON.stringify({ ativa: !ativa }) });
    await _paLoadCampanhas();
}

async function paDeleteCampanha(id) {
    if (!confirm('Excluir campanha e todos os dados associados?')) return;
    await api(`/api/premiacao/campanhas/${id}`, { method:'DELETE' });
    toast('Campanha excluída');
    await _paLoadCampanhas();
}

function paEditCampanha(id) {
    const c = _paCampanhasData.find(x => x.id === id);
    if (!c) return;
    document.getElementById('pa-edit-id').value = id;
    document.getElementById('pa-edit-nome').value = c.nome || '';
    document.getElementById('pa-edit-ini').value = c.dt_inicio || '';
    document.getElementById('pa-edit-fim').value = c.dt_fim || '';
    document.getElementById('pa-edit-tier-inter').value = c.tiers?.intermediaria || '';
    document.getElementById('pa-edit-tier-meta').value = c.tiers?.meta || '';
    document.getElementById('pa-edit-tier-super').value = c.tiers?.supermeta || '';
    const rr = (c.receb_regras || [])[0];
    document.getElementById('pa-edit-receb-modo').value = rr?.modo || 'percentual';
    document.getElementById('pa-edit-receb-valor').value = rr?.valor || '';
    document.getElementById('pa-edit-modal').classList.remove('hidden');
}

async function paSaveEditCampanha() {
    const id = document.getElementById('pa-edit-id').value;
    if (!id) return;
    const body = {
        nome: document.getElementById('pa-edit-nome').value.trim(),
        dt_inicio: document.getElementById('pa-edit-ini').value,
        dt_fim: document.getElementById('pa-edit-fim').value,
        tiers: {},
        receb_regras: [],
    };
    const iv = parseFloat(document.getElementById('pa-edit-tier-inter').value || 0);
    const mv = parseFloat(document.getElementById('pa-edit-tier-meta').value || 0);
    const sv = parseFloat(document.getElementById('pa-edit-tier-super').value || 0);
    if (iv > 0) body.tiers.intermediaria = iv;
    if (mv > 0) body.tiers.meta = mv;
    if (sv > 0) body.tiers.supermeta = sv;
    const rModo = document.getElementById('pa-edit-receb-modo').value || 'percentual';
    const rVal = parseFloat(document.getElementById('pa-edit-receb-valor').value || 0);
    if (rVal > 0) body.receb_regras.push({ tier: 'qualquer', modo: rModo, valor: rVal });
    const raw = await api(`/api/premiacao/campanhas/${id}`, { method:'PUT', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body) });
    const res = await raw.json();
    if (res?.ok) {
        toast('Campanha atualizada!');
        document.getElementById('pa-edit-modal').classList.add('hidden');
        await _paLoadCampanhas();
    } else { toast(res?.error || 'Erro', 'error'); }
}

/* ═══ B) Metas por Agente ═══ */

async function paLoadMetasAgente() {
    const cid = document.getElementById('pa-metas-camp')?.value;
    const wrap = document.getElementById('pa-metas-grid-wrap');
    if (!cid || !wrap) { if (wrap) wrap.innerHTML = '<p class="text-xs text-slate-600">Selecione uma campanha</p>'; return; }

    try {
        const [metasRaw] = await Promise.all([
            api(`/api/premiacao/campanhas/${cid}/metas`),
        ]);
        const metasRes = await metasRaw.json();
        const existingMetas = metasRes?.metas || [];
        const lookup = {};
        existingMetas.forEach(m => { lookup[m.kommo_user_id] = m; });

        if (!_paAgentes.length) {
            const agRaw = await api('/api/minha-performance/agentes');
            const agRes = await agRaw.json();
            _paAgentes = agRes?.agentes || [];
        }

        let html = `<table class="w-full text-xs">
            <thead><tr class="text-slate-500 border-b border-slate-700/30">
                <th class="text-left py-2 pr-3 min-w-[140px]">Agente</th>
                <th class="text-center px-2 py-2 w-24">Intermediária</th>
                <th class="text-center px-2 py-2 w-24">Meta</th>
                <th class="text-center px-2 py-2 w-24">Supermeta</th>
            </tr></thead><tbody>`;

        _paAgentes.forEach(a => {
            const m = lookup[a.kommo_uid] || {};
            html += `<tr class="border-b border-slate-800/30">
                <td class="py-1.5 pr-3 text-slate-300 font-medium">${a.name}</td>
                <td class="px-2 py-1.5"><input type="number" min="0" step="1" value="${m.meta_intermediaria || ''}" data-uid="${a.kommo_uid}" data-field="inter" class="pa-meta-input input-glass px-1.5 py-1 text-center text-xs text-slate-300 w-full"></td>
                <td class="px-2 py-1.5"><input type="number" min="0" step="1" value="${m.meta || ''}" data-uid="${a.kommo_uid}" data-field="meta" class="pa-meta-input input-glass px-1.5 py-1 text-center text-xs text-slate-300 w-full"></td>
                <td class="px-2 py-1.5"><input type="number" min="0" step="1" value="${m.supermeta || ''}" data-uid="${a.kommo_uid}" data-field="super" class="pa-meta-input input-glass px-1.5 py-1 text-center text-xs text-slate-300 w-full"></td>
            </tr>`;
        });

        html += '</tbody></table>';
        wrap.innerHTML = html;
    } catch(e) {
        console.error('paLoadMetasAgente', e);
        if (wrap) wrap.innerHTML = '<p class="text-xs text-red-400">Erro ao carregar</p>';
    }
}

async function paSaveMetasAgente() {
    const cid = document.getElementById('pa-metas-camp')?.value;
    if (!cid) { toast('Selecione uma campanha', 'error'); return; }

    const inputs = document.querySelectorAll('.pa-meta-input');
    const byUid = {};
    inputs.forEach(inp => {
        const uid = inp.dataset.uid;
        const field = inp.dataset.field;
        if (!byUid[uid]) byUid[uid] = { kommo_user_id: parseInt(uid), meta: 0, meta_intermediaria: 0, supermeta: 0 };
        const val = parseFloat(inp.value || 0);
        if (field === 'inter') byUid[uid].meta_intermediaria = val;
        if (field === 'meta') byUid[uid].meta = val;
        if (field === 'super') byUid[uid].supermeta = val;
    });

    const metas = Object.values(byUid).filter(m => m.meta > 0 || m.meta_intermediaria > 0 || m.supermeta > 0);
    if (!metas.length) { toast('Nenhuma meta preenchida', 'error'); return; }

    const raw = await api(`/api/premiacao/campanhas/${cid}/metas`, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({ metas }) });
    const res = await raw.json();
    if (res?.ok) { toast(`${res.saved} metas salvas!`); } else { toast(res?.error || 'Erro', 'error'); }
}

/* ═══ C) Grupos ═══ */

async function paLoadGrupos() {
    const cid = document.getElementById('pa-grupo-camp')?.value;
    const wrap = document.getElementById('pa-grupos-list');
    const warn = document.getElementById('pa-sem-grupo-warn');
    if (!cid) {
        if (wrap) wrap.innerHTML = '<p class="text-xs text-slate-600">Selecione uma campanha</p>';
        if (warn) warn.classList.add('hidden');
        return;
    }
    try {
        const raw = await api(`/api/premiacao/campanhas/${cid}/grupos`);
        const res = await raw.json();
        _paGruposData = res?.grupos || [];
        _paRenderGrupos();
    } catch(e) {
        if (wrap) wrap.innerHTML = '<p class="text-xs text-red-400">Erro ao carregar grupos</p>';
    }
}

function _paRenderGrupos() {
    const wrap = document.getElementById('pa-grupos-list');
    const warn = document.getElementById('pa-sem-grupo-warn');
    if (!wrap) return;

    if (!_paGruposData.length) {
        wrap.innerHTML = '<p class="text-xs text-slate-600">Nenhum grupo criado. Crie um grupo e adicione agentes.</p>';
        if (warn) warn.classList.add('hidden');
        return;
    }

    const allMembers = new Set();
    _paGruposData.forEach(g => g.membros.forEach(uid => allMembers.add(uid)));

    const agentName = uid => {
        const a = _paAgentes.find(x => x.kommo_uid === uid);
        return a ? a.name : `#${uid}`;
    };

    wrap.innerHTML = _paGruposData.map(g => {
        const chips = g.membros.map(uid =>
            `<span class="inline-flex items-center px-2 py-0.5 text-[10px] rounded-full bg-emerald-500/15 text-emerald-400 border border-emerald-500/20">${agentName(uid)}</span>`
        ).join('');
        return `<div class="bg-slate-800/40 rounded-xl p-4 border border-slate-700/30">
            <div class="flex items-center justify-between mb-2">
                <span class="text-sm font-semibold text-white">${g.nome}</span>
                <div class="flex items-center gap-1.5">
                    <button onclick="paEditGrupo(${g.id})" class="text-[10px] px-2.5 py-1 rounded-lg border border-slate-600/40 text-slate-400 hover:text-white hover:border-slate-500 transition-all">Editar</button>
                    <button onclick="paDeleteGrupo(${g.id})" class="text-[10px] px-2.5 py-1 rounded-lg border border-red-500/30 text-red-400 hover:bg-red-500/10 transition-all">Excluir</button>
                </div>
            </div>
            <div class="flex flex-wrap gap-1">${chips || '<span class="text-[10px] text-slate-600">Sem membros</span>'}</div>
        </div>`;
    }).join('');

    const semGrupo = _paAgentes.filter(a => !allMembers.has(a.kommo_uid));
    if (warn) {
        if (semGrupo.length > 0) {
            warn.classList.remove('hidden');
            warn.innerHTML = `<strong>Agentes sem grupo:</strong> ${semGrupo.map(a => a.name).join(', ')}`;
        } else {
            warn.classList.add('hidden');
        }
    }
}

function paNovoGrupo() {
    const cid = document.getElementById('pa-grupo-camp')?.value;
    if (!cid) { toast('Selecione uma campanha primeiro', 'error'); return; }
    document.getElementById('pa-grupo-modal-id').value = '';
    document.getElementById('pa-grupo-modal-nome').value = '';
    document.getElementById('pa-grupo-modal-title').textContent = 'Novo Grupo';
    _paRenderGrupoMembrosModal([]);
    document.getElementById('pa-grupo-modal').classList.remove('hidden');
}

function paEditGrupo(gid) {
    const g = _paGruposData.find(x => x.id === gid);
    if (!g) return;
    document.getElementById('pa-grupo-modal-id').value = gid;
    document.getElementById('pa-grupo-modal-nome').value = g.nome;
    document.getElementById('pa-grupo-modal-title').textContent = 'Editar Grupo';
    _paRenderGrupoMembrosModal(g.membros);
    document.getElementById('pa-grupo-modal').classList.remove('hidden');
}

function _paRenderGrupoMembrosModal(selectedUids) {
    const wrap = document.getElementById('pa-grupo-modal-membros');
    if (!wrap) return;
    const sel = new Set(selectedUids.map(Number));
    wrap.innerHTML = _paAgentes.map(a => `
        <label class="flex items-center gap-2 px-2 py-1.5 rounded-lg hover:bg-slate-700/30 cursor-pointer transition-colors">
            <input type="checkbox" value="${a.kommo_uid}" class="pa-grupo-chk rounded border-slate-600 text-emerald-500 focus:ring-emerald-500/30" ${sel.has(a.kommo_uid)?'checked':''}>
            <span class="text-xs text-slate-300">${a.name}</span>
        </label>
    `).join('');
}

async function paSaveGrupo() {
    const cid = document.getElementById('pa-grupo-camp')?.value;
    if (!cid) return;
    const gid = document.getElementById('pa-grupo-modal-id').value;
    const nome = document.getElementById('pa-grupo-modal-nome').value.trim();
    if (!nome) { toast('Nome do grupo é obrigatório', 'error'); return; }
    const membros = Array.from(document.querySelectorAll('.pa-grupo-chk:checked')).map(cb => parseInt(cb.value));

    let raw;
    if (gid) {
        raw = await api(`/api/premiacao/grupos/${gid}`, { method:'PUT', headers:{'Content-Type':'application/json'}, body:JSON.stringify({ nome, membros }) });
    } else {
        raw = await api(`/api/premiacao/campanhas/${cid}/grupos`, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({ nome, membros }) });
    }
    const res = await raw.json();
    if (res?.ok) {
        toast(gid ? 'Grupo atualizado!' : 'Grupo criado!');
        document.getElementById('pa-grupo-modal').classList.add('hidden');
        await paLoadGrupos();
        const dailyCid = document.getElementById('pa-daily-camp')?.value;
        if (dailyCid === cid) paLoadDailyGrid();
    } else { toast(res?.error || 'Erro', 'error'); }
}

async function paDeleteGrupo(gid) {
    if (!confirm('Excluir grupo e suas metas diárias?')) return;
    await api(`/api/premiacao/grupos/${gid}`, { method:'DELETE' });
    toast('Grupo excluído');
    await paLoadGrupos();
}

/* ═══ C) PIX Diário por Grupo ═══ */

async function paLoadDailyGrid() {
    const cid = document.getElementById('pa-daily-camp')?.value;
    const wrap = document.getElementById('pa-daily-grid-wrap');
    if (!cid || !wrap) { if (wrap) wrap.innerHTML = '<p class="text-xs text-slate-600">Selecione uma campanha</p>'; return; }

    try {
        const [gruposRaw, diariasRaw] = await Promise.all([
            api(`/api/premiacao/campanhas/${cid}/grupos`),
            api(`/api/premiacao/campanhas/${cid}/diarias-grupo`),
        ]);
        const gruposRes = await gruposRaw.json();
        const diariasRes = await diariasRaw.json();
        const grupos = gruposRes?.grupos || [];
        const diarias = diariasRes?.diarias || [];

        if (!grupos.length) {
            wrap.innerHTML = '<p class="text-xs text-amber-400">Crie grupos de agentes primeiro na seção acima</p>';
            return;
        }

        const lookup = {};
        diarias.forEach(d => { lookup[`${d.grupo_id}_${d.dia_semana}`] = d; });

        const agentName = uid => {
            const a = _paAgentes.find(x => x.kommo_uid === uid);
            return a ? a.name : `#${uid}`;
        };

        let html = '';
        grupos.forEach(g => {
            const membrosStr = g.membros.map(uid => agentName(uid)).join(', ') || 'Sem membros';
            html += `<div class="bg-slate-800/40 rounded-xl p-4 border border-slate-700/30">
                <div class="flex items-center justify-between mb-3">
                    <div>
                        <span class="text-sm font-semibold text-white">${g.nome}</span>
                        <p class="text-[10px] text-slate-500">${membrosStr}</p>
                    </div>
                </div>
                <div class="overflow-x-auto">
                    <table class="w-full text-[10px]">
                        <thead>
                            <tr class="text-slate-500">
                                <th class="text-left pr-3 pb-1 w-16"></th>
                                ${_paDias.map(d => `<th class="text-center px-1 pb-1 min-w-[52px]">${d}</th>`).join('')}
                            </tr>
                        </thead>
                        <tbody>
                            <tr>
                                <td class="text-slate-400 pr-3 py-1 font-medium">Meta</td>
                                ${_paDias.map((_, dow) => {
                                    const v = lookup[`${g.id}_${dow}`]?.meta_diaria || '';
                                    return `<td class="px-1 py-1"><input type="number" min="0" value="${v}" data-gid="${g.id}" data-dow="${dow}" data-field="meta" class="pa-daily-input input-glass px-1.5 py-1 text-center text-xs text-slate-300 w-full"></td>`;
                                }).join('')}
                            </tr>
                            <tr>
                                <td class="text-slate-400 pr-3 py-1 font-medium">Fixo R$</td>
                                ${_paDias.map((_, dow) => {
                                    const v = lookup[`${g.id}_${dow}`]?.bonus_fixo || '';
                                    return `<td class="px-1 py-1"><input type="number" min="0" step="0.01" value="${v}" data-gid="${g.id}" data-dow="${dow}" data-field="fixo" class="pa-daily-input input-glass px-1.5 py-1 text-center text-xs text-slate-300 w-full"></td>`;
                                }).join('')}
                            </tr>
                            <tr>
                                <td class="text-slate-400 pr-3 py-1 font-medium">Extra R$</td>
                                ${_paDias.map((_, dow) => {
                                    const v = lookup[`${g.id}_${dow}`]?.bonus_extra || '';
                                    return `<td class="px-1 py-1"><input type="number" min="0" step="0.01" value="${v}" data-gid="${g.id}" data-dow="${dow}" data-field="extra" class="pa-daily-input input-glass px-1.5 py-1 text-center text-xs text-slate-300 w-full"></td>`;
                                }).join('')}
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>`;
        });

        wrap.innerHTML = html;
    } catch(e) {
        console.error('paLoadDailyGrid', e);
        wrap.innerHTML = '<p class="text-xs text-red-400">Erro ao carregar</p>';
    }
}

async function paSaveDailyGrupo() {
    const cid = document.getElementById('pa-daily-camp')?.value;
    if (!cid) { toast('Selecione uma campanha', 'error'); return; }

    const inputs = document.querySelectorAll('.pa-daily-input');
    const byKey = {};
    inputs.forEach(inp => {
        const gid = inp.dataset.gid;
        const dow = inp.dataset.dow;
        const field = inp.dataset.field;
        const key = `${gid}_${dow}`;
        if (!byKey[key]) byKey[key] = { grupo_id: parseInt(gid), dia_semana: parseInt(dow), meta_diaria: 0, bonus_fixo: 0, bonus_extra: 0 };
        if (field === 'meta') byKey[key].meta_diaria = parseInt(inp.value || 0);
        if (field === 'fixo') byKey[key].bonus_fixo = parseFloat(inp.value || 0);
        if (field === 'extra') byKey[key].bonus_extra = parseFloat(inp.value || 0);
    });

    const items = Object.values(byKey).filter(i => i.meta_diaria > 0 || i.bonus_fixo > 0 || i.bonus_extra > 0);
    if (!items.length) { toast('Nenhuma meta preenchida', 'error'); return; }

    const raw = await api(`/api/premiacao/campanhas/${cid}/diarias-grupo`, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({ items }) });
    const res = await raw.json();
    if (res?.ok) { toast('PIX diário salvo!'); } else { toast(res?.error || 'Erro', 'error'); }
}

/* ═══ D) Upload Recebimentos ═══ */

async function paUploadRecebimentos(input) {
    const file = input.files?.[0];
    if (!file) return;
    const mesRef = document.getElementById('pa-receb-mes')?.value || '';
    const fd = new FormData();
    fd.append('file', file);
    fd.append('mes_ref', mesRef);
    const msg = document.getElementById('pa-receb-msg');
    try {
        const res = await fetch('/api/recebimentos/upload', { method:'POST', body:fd });
        const data = await res.json();
        if (msg) {
            msg.classList.remove('hidden');
            if (data.ok) {
                msg.className = 'mt-3 text-xs p-3 rounded-lg bg-emerald-500/10 text-emerald-400 border border-emerald-500/20';
                msg.textContent = `Upload concluído: ${data.rows} linhas importadas`;
            } else {
                msg.className = 'mt-3 text-xs p-3 rounded-lg bg-red-500/10 text-red-400 border border-red-500/20';
                msg.textContent = data.error || 'Erro no upload';
            }
        }
    } catch(e) {
        if (msg) {
            msg.classList.remove('hidden');
            msg.className = 'mt-3 text-xs p-3 rounded-lg bg-red-500/10 text-red-400 border border-red-500/20';
            msg.textContent = 'Erro de conexão';
        }
    }
    input.value = '';
}
