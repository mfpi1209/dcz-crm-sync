let _cmCampanhas = [];
let _cmAgentes = [];
let _cmGrupos = [];

const _cmMoney = (v) => Number(v || 0).toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });

function loadComissao() {
    cmBoot();
}

async function cmBoot() {
    try {
        const [campRaw, agRaw] = await Promise.all([
            api('/api/premiacao/campanhas'),
            api('/api/minha-performance/agentes'),
        ]);
        const camp = await campRaw.json();
        const ag = await agRaw.json();
        _cmCampanhas = camp.campanhas || [];
        _cmAgentes = ag.agentes || [];
        const sel = document.getElementById('cm-camp');
        const hoje = new Date().toISOString().slice(0, 10);
        let escolhida = _cmCampanhas.find(c => c.dt_inicio <= hoje && c.dt_fim >= hoje) || _cmCampanhas[0];
        sel.innerHTML = _cmCampanhas.map(c =>
            `<option value="${c.id}">${c.nome} (${c.dt_inicio.split('-').reverse().join('/')} – ${c.dt_fim.split('-').reverse().join('/')})</option>`
        ).join('') || '<option value="">Nenhuma campanha</option>';
        if (escolhida) sel.value = String(escolhida.id);
        cmCampanha();
        cmTabelas();
    } catch (e) {
        toast('Não foi possível abrir a Comissão', 'error');
    }
}

function cmCampanha() {
    const c = _cmCampanhas.find(x => String(x.id) === document.getElementById('cm-camp').value);
    if (c) {
        document.getElementById('cm-de').value = c.dt_inicio;
        document.getElementById('cm-ate').value = c.dt_fim;
    }
    cmLoadGrupos();
}

async function cmLoadGrupos() {
    const cid = document.getElementById('cm-camp').value;
    const wrap = document.getElementById('cm-grupos');
    if (!cid) {
        wrap.innerHTML = '<p class="text-xs text-slate-600">Nenhuma campanha</p>';
        return;
    }
    const raw = await api(`/api/premiacao/campanhas/${cid}/grupos`);
    const res = await raw.json();
    _cmGrupos = res.grupos || [];
    if (!_cmGrupos.length) {
        wrap.innerHTML = '<p class="text-xs text-slate-600">Nenhum grupo criado. Crie um grupo e adicione os consultores. O nome precisa ser Alta Performance ou Impulso.</p>';
        return;
    }
    const nome = (uid) => (_cmAgentes.find(a => a.kommo_uid === uid) || {}).name || `#${uid}`;
    wrap.innerHTML = _cmGrupos.map(g => `
        <div class="bg-slate-100/90 dark:bg-slate-800/40 rounded-xl p-4 border border-[var(--border)]">
            <div class="flex items-center justify-between mb-2">
                <span class="text-sm font-semibold text-[var(--text-primary)]">${g.nome}</span>
                <span class="flex gap-1.5">
                    <button type="button" onclick="cmEditGrupo(${g.id})" class="text-[10px] px-2.5 py-1 rounded-lg border border-slate-500/40 text-slate-400">Editar</button>
                    <button type="button" onclick="cmExcluirGrupo(${g.id})" class="text-[10px] px-2.5 py-1 rounded-lg border border-red-500/30 text-red-400">Excluir</button>
                </span>
            </div>
            <div class="flex flex-wrap gap-1">${(g.membros || []).map(uid => `<span class="px-2 py-0.5 text-[10px] rounded-full bg-emerald-500/15 text-emerald-400 border border-emerald-500/20">${nome(uid)}</span>`).join('') || '<span class="text-[10px] text-slate-600">Sem membros</span>'}</div>
        </div>
    `).join('');
}

function cmTabelas() {
    const box = document.getElementById('cm-tabelas');
    const card = (titulo, linhas) => `
        <div class="rounded-lg border border-[var(--border)] overflow-hidden">
            <div class="px-3 py-2 font-semibold text-[var(--text-primary)]">${titulo}</div>
            <table class="w-full">
                <tr class="text-slate-500"><th class="px-3 py-1"></th><th class="px-3 py-1 text-right">Graduação</th><th class="px-3 py-1 text-right">Pós-Graduação</th></tr>
                ${linhas.map(([nome, g, p]) => `<tr class="border-t border-[var(--border)]"><td class="px-3 py-1">${nome}</td><td class="px-3 py-1 text-right">${g}</td><td class="px-3 py-1 text-right">${p}</td></tr>`).join('')}
            </table>
        </div>`;
    box.innerHTML = card('Premiação Alta Performance', [['Inter', 10, 10], ['Meta', 15, 10], ['Super', 20, 20]])
        + card('Premiação Impulso', [['Inter', 8, 8], ['Meta', 12, 10], ['Super', 16, 16]]);
}

function _cmMembros(selecionados) {
    const set = new Set(selecionados || []);
    document.getElementById('cm-modal-membros').innerHTML = _cmAgentes.map(a => `
        <label class="flex items-center gap-2 text-xs text-[var(--text-primary)]">
            <input type="checkbox" value="${a.kommo_uid}" ${set.has(a.kommo_uid) ? 'checked' : ''}>
            ${a.name}
        </label>
    `).join('');
}

function cmNovoGrupo() {
    if (!document.getElementById('cm-camp').value) return;
    const modal = document.getElementById('cm-modal');
    if (modal.parentElement !== document.body) document.body.appendChild(modal);
    document.getElementById('cm-modal-id').value = '';
    document.getElementById('cm-modal-nome').value = '';
    document.getElementById('cm-modal-title').textContent = 'Novo Grupo';
    _cmMembros([]);
    document.getElementById('cm-modal').classList.remove('hidden');
}

function cmEditGrupo(id) {
    const g = _cmGrupos.find(x => x.id === id);
    if (!g) return;
    const modal = document.getElementById('cm-modal');
    if (modal.parentElement !== document.body) document.body.appendChild(modal);
    document.getElementById('cm-modal-id').value = String(g.id);
    document.getElementById('cm-modal-nome').value = g.nome;
    document.getElementById('cm-modal-title').textContent = 'Editar Grupo';
    _cmMembros(g.membros || []);
    document.getElementById('cm-modal').classList.remove('hidden');
}

async function cmSalvarGrupo() {
    const cid = document.getElementById('cm-camp').value;
    const gid = document.getElementById('cm-modal-id').value;
    const nome = document.getElementById('cm-modal-nome').value.trim();
    const membros = Array.from(document.querySelectorAll('#cm-modal-membros input:checked')).map(el => Number(el.value));
    if (!nome) { toast('Informe o nome do grupo', 'error'); return; }
    const body = JSON.stringify({ nome, membros });
    const raw = gid
        ? await api(`/api/premiacao/grupos/${gid}`, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body })
        : await api(`/api/premiacao/campanhas/${cid}/grupos`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body });
    const res = await raw.json();
    if (!res.ok) { toast(res.error || 'Erro ao salvar', 'error'); return; }
    document.getElementById('cm-modal').classList.add('hidden');
    await cmLoadGrupos();
    cmCalcular();
}

async function cmExcluirGrupo(id) {
    if (!confirm('Excluir este grupo?')) return;
    await api(`/api/premiacao/grupos/${id}`, { method: 'DELETE' });
    await cmLoadGrupos();
    cmCalcular();
}

async function cmCalcular() {
    const cid = document.getElementById('cm-camp').value;
    const dtIni = document.getElementById('cm-de').value;
    const dtFim = document.getElementById('cm-ate').value;
    const body = document.getElementById('cm-body');
    const curso = document.getElementById('cm-curso');
    if (!cid || !dtIni || !dtFim) { toast('Escolha a campanha e o período', 'error'); return; }
    body.innerHTML = '<tr><td colspan="9" class="py-6 text-xs text-slate-500">Calculando as matrículas do dashboard…</td></tr>';
    curso.textContent = '';
    try {
        const raw = await api(`/api/comissao?dt_ini=${dtIni}&dt_fim=${dtFim}&campanha_id=${cid}`);
        const res = await raw.json();
        if (!res.ok) { toast(res.error || 'Erro ao calcular', 'error'); return; }
        curso.textContent = `${Number(res.em_curso || 0).toLocaleString('pt-BR')} em curso no dashboard neste período`;
        if (!res.linhas.length) {
            body.innerHTML = '<tr><td colspan="9" class="py-6 text-xs text-slate-500">Nenhum consultor nos grupos desta campanha.</td></tr>';
        } else {
            const rows = res.linhas.map(r => `<tr class="border-b border-[var(--border)]">
                <td class="py-2 pr-3">${r.agente}</td>
                <td class="py-2 pr-3 text-slate-400">${r.grupo}${r.tabela ? '' : ' · sem tabela'}</td>
                <td class="py-2 pr-3 text-right">${r.matriculas}</td>
                <td class="py-2 pr-3 text-right">${r.grad}</td>
                <td class="py-2 pr-3 text-right">${r.pos}</td>
                <td class="py-2 pr-3">${r.faixa_label}</td>
                <td class="py-2 pr-3 text-right">${r.valor_grad ? _cmMoney(r.valor_grad) : '—'}</td>
                <td class="py-2 pr-3 text-right">${r.valor_pos ? _cmMoney(r.valor_pos) : '—'}</td>
                <td class="py-2 text-right font-semibold">${_cmMoney(r.comissao)}</td>
            </tr>`);
            rows.push(`<tr><td colspan="8" class="py-2 text-right text-xs text-slate-500">Total</td><td class="py-2 text-right font-bold">${_cmMoney(res.total)}</td></tr>`);
            body.innerHTML = rows.join('');
        }
        const sem = document.getElementById('cm-sem');
        sem.textContent = res.sem_grupo && res.sem_grupo.length
            ? 'Com matrícula no período e fora dos grupos: ' + res.sem_grupo.map(a => `${a.agente} (${a.matriculas})`).join(', ')
            : '';
    } catch (e) {
        body.innerHTML = '<tr><td colspan="9" class="py-6 text-xs text-red-400">Falha ao calcular.</td></tr>';
    }
}
