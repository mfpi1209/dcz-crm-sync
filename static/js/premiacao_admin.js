// ---------------------------------------------------------------------------
// Premiação Admin — JS
// ---------------------------------------------------------------------------
const _paFmt = v => (v || 0).toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
const _paDias = ['Seg', 'Ter', 'Qua', 'Qui', 'Sex', 'Sáb', 'Dom'];

function _paFmtDateBR(s) {
    if (!s) return '';
    const parts = s.split('-');
    if (parts.length === 3) return `${parts[2]}/${parts[1]}/${parts[0]}`;
    return s;
}

async function loadPremiacaoAdmin() {
    try {
        await Promise.all([
            _paLoadCampanhas(),
            _paLoadAgentSelect(),
        ]);
    } catch (e) {
        console.error('loadPremiacaoAdmin', e);
    }
}

// ---------------------------------------------------------------------------
// Seletor de agente (para visualizar dados)
// ---------------------------------------------------------------------------
async function _paLoadAgentSelect() {
    try {
        const res = await api('/api/minha-performance/agentes');
        const d = await res.json();
        if (!d.ok) return;
        const sel = document.getElementById('pa-agent-select');
        const current = sel.value;
        sel.innerHTML = '<option value="">Ver dados de um agente...</option>' +
            d.agentes.map(a => `<option value="${a.kommo_uid}" ${String(a.kommo_uid) === current ? 'selected' : ''}>${a.name}</option>`).join('');
    } catch (e) { console.error(e); }
}

function paAgentChanged() {
    // placeholder: could open performance data for selected agent
}

// ---------------------------------------------------------------------------
// Campanhas CRUD
// ---------------------------------------------------------------------------
async function _paLoadCampanhas() {
    try {
        const res = await api('/api/premiacao/campanhas');
        const d = await res.json();
        if (!d.ok) return;
        const wrap = document.getElementById('pa-campanhas-list');
        const sel = document.getElementById('pa-daily-camp');
        sel.innerHTML = '<option value="">Selecionar...</option>';

        if (!d.campanhas.length) {
            wrap.innerHTML = '<p class="text-xs text-slate-600">Nenhuma campanha criada</p>';
            return;
        }
        wrap.innerHTML = d.campanhas.map(c => {
            const tiers = c.tiers || {};
            const ativaLabel = c.ativa
                ? '<span class="text-[9px] bg-emerald-500/20 text-emerald-400 px-1.5 py-0.5 rounded-full font-bold">Ativa</span>'
                : '<span class="text-[9px] bg-slate-700 text-slate-400 px-1.5 py-0.5 rounded-full">Inativa</span>';
            return `<div class="bg-slate-800/30 rounded-lg p-3 border border-slate-700/30 flex items-center justify-between">
                <div>
                    <div class="flex items-center gap-2">
                        <span class="text-sm font-bold text-white">${c.nome}</span>
                        ${ativaLabel}
                    </div>
                    <p class="text-[10px] text-slate-500 mt-0.5">${_paFmtDateBR(c.dt_inicio)} — ${_paFmtDateBR(c.dt_fim)}</p>
                    <p class="text-[10px] text-slate-400 mt-1">
                        Inter: ${_paFmt(tiers.intermediaria || 0)}/mat
                        | Meta: ${_paFmt(tiers.meta || 0)}/mat
                        | Super: ${_paFmt(tiers.supermeta || 0)}/mat
                    </p>
                </div>
                <div class="flex gap-2">
                    <button onclick="paToggleCampanha(${c.id}, ${c.ativa})" class="text-[10px] text-slate-400 hover:text-white border border-slate-700 px-2 py-1 rounded transition-all">
                        ${c.ativa ? 'Desativar' : 'Ativar'}
                    </button>
                    <button onclick="paDeleteCampanha(${c.id})" class="text-[10px] text-red-400 hover:text-red-300 border border-red-900/30 px-2 py-1 rounded transition-all">Excluir</button>
                </div>
            </div>`;
        }).join('');

        d.campanhas.forEach(c => {
            sel.innerHTML += `<option value="${c.id}">${c.nome} (${_paFmtDateBR(c.dt_inicio)} — ${_paFmtDateBR(c.dt_fim)})</option>`;
        });
    } catch (e) { console.error(e); }
}

async function paSaveCampanha() {
    const nome = document.getElementById('pa-camp-nome').value.trim();
    const dt_inicio = document.getElementById('pa-camp-ini').value;
    const dt_fim = document.getElementById('pa-camp-fim').value;
    if (!nome || !dt_inicio || !dt_fim) { toast('Preencha nome e datas', 'warning'); return; }
    const tiers = {};
    const inter = parseFloat(document.getElementById('pa-camp-tier-inter').value) || 0;
    const meta = parseFloat(document.getElementById('pa-camp-tier-meta').value) || 0;
    const sup = parseFloat(document.getElementById('pa-camp-tier-super').value) || 0;
    if (inter > 0) tiers.intermediaria = inter;
    if (meta > 0) tiers.meta = meta;
    if (sup > 0) tiers.supermeta = sup;

    const receb_regras = [];
    const recebValor = parseFloat(document.getElementById('pa-camp-receb-valor').value) || 0;
    if (recebValor > 0) {
        receb_regras.push({
            tier: 'qualquer',
            modo: document.getElementById('pa-camp-receb-modo').value,
            valor: recebValor,
        });
    }

    try {
        const res = await api('/api/premiacao/campanhas', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ nome, dt_inicio, dt_fim, tiers, receb_regras }),
        });
        const d = await res.json();
        if (d.error) { toast(d.error, 'error'); return; }
        toast('Campanha criada', 'success');
        document.getElementById('pa-camp-nome').value = '';
        _paLoadCampanhas();
    } catch (e) { toast('Erro: ' + e.message, 'error'); }
}

async function paToggleCampanha(cid, currentActive) {
    try {
        await api(`/api/premiacao/campanhas/${cid}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ ativa: !currentActive }),
        });
        _paLoadCampanhas();
    } catch (e) { toast('Erro: ' + e.message, 'error'); }
}

async function paDeleteCampanha(cid) {
    if (!confirm('Excluir esta campanha? Todas as metas diárias e regras serão removidas.')) return;
    try {
        await api(`/api/premiacao/campanhas/${cid}`, { method: 'DELETE' });
        _paLoadCampanhas();
        toast('Campanha excluída', 'success');
    } catch (e) { toast('Erro: ' + e.message, 'error'); }
}

// ---------------------------------------------------------------------------
// Metas diárias grid
// ---------------------------------------------------------------------------
let _paDailyAgents = [];

async function paLoadDailyGrid() {
    const cid = document.getElementById('pa-daily-camp').value;
    const wrap = document.getElementById('pa-daily-grid-wrap');
    if (!cid) { wrap.innerHTML = '<p class="text-xs text-slate-600">Selecione uma campanha acima</p>'; return; }

    try {
        const [agRes, diRes] = await Promise.all([
            api('/api/minha-performance/agentes'),
            api(`/api/premiacao/campanhas/${cid}/diarias`),
        ]);
        const agents = (await agRes.json()).agentes || [];
        const diarias = (await diRes.json()).diarias || [];
        _paDailyAgents = agents;

        const lookup = {};
        diarias.forEach(d => {
            const key = `${d.kommo_user_id}_${d.dia_semana}`;
            lookup[key] = d;
        });

        let html = `<table class="w-full text-[11px]">
            <thead>
                <tr class="text-[9px] uppercase tracking-wider text-slate-500 border-b border-slate-700/20">
                    <th class="text-left px-2 py-2 sticky left-0 bg-slate-900/95 z-10">Agente</th>`;
        _paDias.forEach(d => {
            html += `<th class="text-center px-1 py-2" colspan="3">${d}</th>`;
        });
        html += `</tr><tr class="text-[8px] text-slate-600 border-b border-slate-700/10">
            <th class="sticky left-0 bg-slate-900/95 z-10"></th>`;
        _paDias.forEach(() => {
            html += '<th class="px-1">Meta</th><th class="px-1">Fixo</th><th class="px-1">Extra</th>';
        });
        html += '</tr></thead><tbody>';

        agents.forEach(a => {
            html += `<tr class="border-b border-slate-700/10"><td class="px-2 py-1.5 text-slate-300 font-medium sticky left-0 bg-slate-900/95 z-10 whitespace-nowrap">${a.name}</td>`;
            for (let dow = 0; dow < 7; dow++) {
                const key = `${a.kommo_uid}_${dow}`;
                const val = lookup[key] || {};
                html += `<td class="px-0.5"><input type="number" min="0" data-uid="${a.kommo_uid}" data-dow="${dow}" data-field="meta" value="${val.meta_diaria || 0}" class="pa-daily-input w-10 input-glass px-1 py-0.5 text-center text-[10px] text-slate-300"></td>`;
                html += `<td class="px-0.5"><input type="number" min="0" step="0.01" data-uid="${a.kommo_uid}" data-dow="${dow}" data-field="fixo" value="${val.bonus_fixo || 0}" class="pa-daily-input w-12 input-glass px-1 py-0.5 text-center text-[10px] text-slate-300"></td>`;
                html += `<td class="px-0.5"><input type="number" min="0" step="0.01" data-uid="${a.kommo_uid}" data-dow="${dow}" data-field="extra" value="${val.bonus_extra || 0}" class="pa-daily-input w-12 input-glass px-1 py-0.5 text-center text-[10px] text-slate-300"></td>`;
            }
            html += '</tr>';
        });
        html += '</tbody></table>';
        wrap.innerHTML = html;
    } catch (e) {
        wrap.innerHTML = `<p class="text-xs text-red-400">${e.message}</p>`;
    }
}

async function paSaveDailyTargets() {
    const cid = document.getElementById('pa-daily-camp').value;
    if (!cid) { toast('Selecione uma campanha', 'warning'); return; }

    const inputs = document.querySelectorAll('.pa-daily-input');
    const dataMap = {};
    inputs.forEach(inp => {
        const uid = inp.dataset.uid;
        const dow = inp.dataset.dow;
        const field = inp.dataset.field;
        const key = `${uid}_${dow}`;
        if (!dataMap[key]) dataMap[key] = { kommo_user_id: parseInt(uid), dia_semana: parseInt(dow) };
        if (field === 'meta') dataMap[key].meta_diaria = parseInt(inp.value) || 0;
        if (field === 'fixo') dataMap[key].bonus_fixo = parseFloat(inp.value) || 0;
        if (field === 'extra') dataMap[key].bonus_extra = parseFloat(inp.value) || 0;
    });

    const items = Object.values(dataMap).filter(d => d.meta_diaria > 0 || d.bonus_fixo > 0 || d.bonus_extra > 0);
    try {
        const res = await api(`/api/premiacao/campanhas/${cid}/diarias`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ items }),
        });
        const d = await res.json();
        if (d.ok) toast('Metas diárias salvas', 'success');
        else toast(d.error || 'Erro', 'error');
    } catch (e) { toast('Erro: ' + e.message, 'error'); }
}

// ---------------------------------------------------------------------------
// Upload recebimentos
// ---------------------------------------------------------------------------
async function paUploadRecebimentos(input) {
    const file = input.files[0];
    if (!file) return;
    const mesRef = document.getElementById('pa-receb-mes').value.trim();
    const fd = new FormData();
    fd.append('file', file);
    fd.append('mes_ref', mesRef);
    const msg = document.getElementById('pa-receb-msg');
    msg.className = 'mt-3 text-xs p-3 rounded-lg bg-blue-500/10 text-blue-400 border border-blue-500/20';
    msg.textContent = 'Enviando...';
    msg.classList.remove('hidden');
    try {
        const res = await api('/api/recebimentos/upload', { method: 'POST', body: fd });
        const d = await res.json();
        if (d.ok) {
            msg.className = 'mt-3 text-xs p-3 rounded-lg bg-emerald-500/10 text-emerald-400 border border-emerald-500/20';
            msg.textContent = `Upload concluído: ${d.rows} linhas importadas.`;
        } else {
            msg.className = 'mt-3 text-xs p-3 rounded-lg bg-red-500/10 text-red-400 border border-red-500/20';
            msg.textContent = d.error || 'Erro no upload';
        }
    } catch (e) {
        msg.className = 'mt-3 text-xs p-3 rounded-lg bg-red-500/10 text-red-400 border border-red-500/20';
        msg.textContent = 'Erro: ' + e.message;
    }
    input.value = '';
}
