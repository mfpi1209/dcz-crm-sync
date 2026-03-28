/* ═══════════════  Ajustes de Matrícula — Painel Admin  ═══════════════ */

let _amData = [];

const _amTipoLabel = {
    matricula_nao_computada: 'Matrícula não computada',
    dados_incorretos: 'Dados incorretos',
    evasao_indevida: 'Evasão indevida',
};

const _amStatusColor = {
    pendente:   'bg-amber-500/15 text-amber-400 border-amber-500/20',
    em_analise: 'bg-blue-500/15 text-blue-400 border-blue-500/20',
    aprovado:   'bg-emerald-500/15 text-emerald-400 border-emerald-500/20',
    rejeitado:  'bg-red-500/15 text-red-400 border-red-500/20',
};
const _amStatusLabel = {
    pendente: 'Pendente', em_analise: 'Em análise', aprovado: 'Aprovado', rejeitado: 'Rejeitado',
};

function _amFmtDate(d) {
    if (!d) return '';
    const p = String(d).substring(0, 10).split('-');
    return p.length === 3 ? `${p[2]}/${p[1]}/${p[0]}` : d;
}

async function loadAjustesMatricula() {
    await amLoadAjustes();
}

async function amLoadAjustes() {
    const status = document.getElementById('am-filter-status')?.value || '';
    const list = document.getElementById('am-list');
    if (list) list.innerHTML = '<p class="text-xs text-slate-600 text-center py-8">Carregando...</p>';

    try {
        const qs = status ? `?status=${status}` : '';
        const res = await api(`/api/ajustes-matricula${qs}`);
        const d = await res.json();
        _amData = d.ajustes || [];
        _amRenderStats();
        _amRenderList();
    } catch(e) {
        if (list) list.innerHTML = '<p class="text-xs text-red-400 text-center py-8">Erro ao carregar solicitações</p>';
    }
}

function _amRenderStats() {
    const el = document.getElementById('am-stats');
    if (!el) return;
    const counts = { pendente: 0, em_analise: 0, aprovado: 0, rejeitado: 0 };
    _amData.forEach(a => { if (counts[a.status] !== undefined) counts[a.status]++; });

    const items = [
        { key: 'pendente', icon: 'schedule', color: 'text-amber-400', bg: 'border-amber-500/20' },
        { key: 'em_analise', icon: 'pending', color: 'text-blue-400', bg: 'border-blue-500/20' },
        { key: 'aprovado', icon: 'check_circle', color: 'text-emerald-400', bg: 'border-emerald-500/20' },
        { key: 'rejeitado', icon: 'cancel', color: 'text-red-400', bg: 'border-red-500/20' },
    ];

    el.innerHTML = items.map(i => `
        <div class="am-card p-4 ${i.bg}">
            <div class="flex items-center gap-2 mb-1">
                <span class="material-symbols-outlined text-sm ${i.color}">${i.icon}</span>
                <span class="text-[10px] text-slate-500 uppercase tracking-wider">${_amStatusLabel[i.key]}</span>
            </div>
            <p class="text-2xl font-black text-white">${counts[i.key]}</p>
        </div>
    `).join('');
}

function _amRenderList() {
    const list = document.getElementById('am-list');
    if (!list) return;
    if (!_amData.length) {
        list.innerHTML = '<p class="text-xs text-slate-600 text-center py-8">Nenhuma solicitação encontrada.</p>';
        return;
    }
    list.innerHTML = _amData.map(a => {
        const sc = _amStatusColor[a.status] || _amStatusColor.pendente;
        return `<div class="am-card p-4 cursor-pointer" onclick="amOpenDetail(${a.id})">
            <div class="flex flex-wrap items-center gap-2 mb-2">
                <span class="px-2 py-0.5 rounded-full text-[10px] font-bold border ${sc}">${_amStatusLabel[a.status] || a.status}</span>
                <span class="text-[10px] text-slate-500">${_amTipoLabel[a.tipo] || a.tipo}</span>
                <span class="text-[10px] text-slate-600 ml-auto">${_amFmtDate(a.created_at)}</span>
            </div>
            <div class="flex flex-wrap gap-x-6 gap-y-1 mb-1">
                <p class="text-xs text-white font-semibold">${a.agent_name || 'Agente #' + a.user_id}</p>
                <p class="text-xs text-slate-400">Aluno: <strong class="text-slate-300">${a.nome_aluno || '—'}</strong></p>
                <p class="text-xs text-slate-400">RGM: <span class="font-mono">${a.rgm || '—'}</span></p>
                <p class="text-xs text-slate-400">Lead: <span class="font-mono">${a.kommo_lead_id || '—'}</span></p>
            </div>
            <p class="text-[10px] text-slate-500 line-clamp-2">${a.descricao || ''}</p>
            ${a.resposta_admin ? `<p class="text-[10px] text-blue-400 mt-1"><strong>Resposta:</strong> ${a.resposta_admin}</p>` : ''}
        </div>`;
    }).join('');
}

function amOpenDetail(id) {
    const a = _amData.find(x => x.id === id);
    if (!a) return;
    const modal = document.getElementById('am-modal-detail');
    const body = document.getElementById('am-detail-body');
    document.getElementById('am-detail-id').value = id;
    document.getElementById('am-detail-status').value = a.status;
    document.getElementById('am-detail-resposta').value = a.resposta_admin || '';

    if (body) body.innerHTML = `
        <div class="grid grid-cols-2 gap-3 text-xs">
            <div><span class="text-slate-500">Agente:</span><p class="text-white font-medium">${a.agent_name || 'Agente #' + a.user_id}</p></div>
            <div><span class="text-slate-500">Tipo:</span><p class="text-white">${_amTipoLabel[a.tipo] || a.tipo}</p></div>
            <div><span class="text-slate-500">Nome do Aluno:</span><p class="text-white">${a.nome_aluno || '—'}</p></div>
            <div><span class="text-slate-500">RGM:</span><p class="text-white font-mono">${a.rgm || '—'}</p></div>
            <div><span class="text-slate-500">Curso:</span><p class="text-white">${a.curso || '—'}</p></div>
            <div><span class="text-slate-500">Polo:</span><p class="text-white">${a.polo || '—'}</p></div>
            <div><span class="text-slate-500">Data Matrícula:</span><p class="text-white">${_amFmtDate(a.data_matricula)}</p></div>
            <div><span class="text-slate-500">Lead Kommo:</span><p class="text-white font-mono">${a.kommo_lead_id || '—'}</p></div>
            <div class="col-span-2"><span class="text-slate-500">Justificativa:</span><p class="text-white mt-1">${a.descricao || '—'}</p></div>
            <div class="col-span-2"><span class="text-slate-500">Enviada em:</span><p class="text-slate-400">${_amFmtDate(a.created_at)}</p></div>
            ${a.resolved_at ? `<div class="col-span-2"><span class="text-slate-500">Resolvida em:</span><p class="text-slate-400">${_amFmtDate(a.resolved_at)}</p></div>` : ''}
        </div>`;

    modal.classList.remove('hidden');
    modal.classList.add('flex');
}

async function amSaveReview() {
    const id = document.getElementById('am-detail-id').value;
    const status = document.getElementById('am-detail-status').value;
    const resposta = document.getElementById('am-detail-resposta').value;
    try {
        await api(`/api/ajustes-matricula/${id}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ status, resposta_admin: resposta }),
        });
        document.getElementById('am-modal-detail').classList.add('hidden');
        document.getElementById('am-modal-detail').classList.remove('flex');
        amLoadAjustes();
    } catch(e) {
        alert('Erro ao salvar: ' + e.message);
    }
}
