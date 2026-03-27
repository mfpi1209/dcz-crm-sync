// ---------------------------------------------------------------------------
// Marco Email — Classificação & Distribuição de E-mails
// ---------------------------------------------------------------------------
const ME_API = 'https://banco-dev-n8n-eduit.6tqx2r.easypanel.host/webhook/api/marco_email';

let meCache = {};
let meLogsPage = 0;
const ME_LOGS_LIMIT = 30;

async function meApi(action, params = {}) {
    const res = await fetch(ME_API, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action, params })
    });
    const data = await res.json();
    if (!data.success) throw new Error(data.error || 'Erro na API');
    return data.data;
}

// ---------------------------------------------------------------------------
// Tab navigation
// ---------------------------------------------------------------------------
const ME_TABS = ['dash', 'agentes', 'categorias', 'roteamento', 'logs', 'config'];

function meTab(tab) {
    ME_TABS.forEach(t => {
        const sec = document.getElementById('me-sec-' + t);
        const btn = document.getElementById('me-tab-' + t);
        if (t === tab) {
            sec.classList.remove('hidden');
            btn.className = 'text-sm font-semibold px-5 py-2 rounded-lg transition bg-blue-500/15 text-blue-400 border border-blue-500/30 flex items-center gap-2 whitespace-nowrap';
        } else {
            sec.classList.add('hidden');
            btn.className = 'text-sm font-semibold px-5 py-2 rounded-lg transition text-gray-500 hover:text-gray-300 flex items-center gap-2 whitespace-nowrap';
        }
    });
    if (tab === 'dash') meLoadDash();
    if (tab === 'agentes') meLoadAgentes();
    if (tab === 'categorias') meLoadCategorias();
    if (tab === 'roteamento') meLoadRoteamento();
    if (tab === 'logs') meLoadLogs();
    if (tab === 'config') meLoadConfig();
}

// ---------------------------------------------------------------------------
// Dashboard
// ---------------------------------------------------------------------------
async function meLoadDash() {
    try {
        const s = await meApi('get_stats');
        const cards = document.getElementById('me-dash-cards');
        cards.innerHTML = [
            meDashCard('Total', s.total_emails || 0, 'mail', 'blue'),
            meDashCard('Hoje', s.emails_hoje || 0, 'today', 'cyan'),
            meDashCard('7 dias', s.emails_7dias || 0, 'date_range', 'teal'),
            meDashCard('Agentes', s.total_agentes || 0, 'support_agent', 'emerald'),
            meDashCard('Categorias', s.total_categorias || 0, 'category', 'violet'),
            meDashCard('Importados', s.emails_importados || 0, 'upload', 'amber'),
        ].join('');

        const catEl = document.getElementById('me-dash-cat');
        if (s.por_categoria && s.por_categoria.length) {
            const max = Math.max(...s.por_categoria.map(c => c.total), 1);
            catEl.innerHTML = s.por_categoria.map(c => `
                <div class="flex items-center gap-3">
                    <span class="w-24 text-xs text-gray-400 truncate">${esc(c.nome)}</span>
                    <div class="flex-1 bg-gray-800/50 rounded-full h-5 overflow-hidden">
                        <div class="h-full bg-blue-500/40 rounded-full flex items-center pl-2 text-[10px] text-blue-300 font-semibold" style="width:${Math.max(c.total / max * 100, 8)}%">${c.total}</div>
                    </div>
                </div>`).join('');
        } else {
            catEl.innerHTML = '<p class="text-gray-600 text-sm">Nenhum dado</p>';
        }

        const recEl = document.getElementById('me-dash-recent');
        if (s.ultimos_emails && s.ultimos_emails.length) {
            recEl.innerHTML = '<table class="w-full text-left text-sm"><thead><tr class="text-xs text-gray-500 border-b border-gray-700/30"><th class="pb-2">De</th><th class="pb-2">Assunto</th><th class="pb-2">Status</th></tr></thead><tbody class="text-gray-300">' +
                s.ultimos_emails.map(e => `<tr class="border-b border-gray-800/30"><td class="py-1.5">${esc(e.de_nome || e.de_email || '-')}</td><td class="py-1.5 truncate max-w-[200px]">${esc((e.assunto || '').substring(0, 50))}</td><td class="py-1.5">${meBadge(e.status, 'status')}</td></tr>`).join('') +
                '</tbody></table>';
        } else {
            recEl.innerHTML = '<p class="text-gray-600 text-sm py-4 text-center">Nenhum e-mail processado</p>';
        }
    } catch (e) {
        toast(e.message, 'error');
    }
}

function meDashCard(label, value, icon, color) {
    return `<div class="glass-card p-4 text-center">
        <span class="material-symbols-outlined text-${color}-400 text-xl mb-1">${icon}</span>
        <p class="text-2xl font-extrabold text-${color}-400">${value}</p>
        <p class="text-[11px] text-gray-500 mt-0.5">${label}</p>
    </div>`;
}

// ---------------------------------------------------------------------------
// Badges
// ---------------------------------------------------------------------------
function meBadge(val, type) {
    if (!val) return '<span class="text-gray-600">—</span>';
    const maps = {
        status: { recebido: 'blue', classificado: 'amber', encaminhado: 'emerald', respondido: 'emerald', importado: 'gray' },
        urgencia: { baixa: 'gray', normal: 'blue', alta: 'amber', critica: 'red' },
        ativo: { true: 'emerald', false: 'red' }
    };
    const m = maps[type] || {};
    const c = m[String(val)] || 'gray';
    const label = type === 'ativo' ? (val ? 'Ativo' : 'Inativo') : val;
    return `<span class="inline-flex items-center px-2 py-0.5 rounded-full text-[10px] font-semibold bg-${c}-500/15 text-${c}-400">${label}</span>`;
}

// ---------------------------------------------------------------------------
// Agentes
// ---------------------------------------------------------------------------
async function meLoadAgentes() {
    try {
        const agentes = await meApi('list_agentes');
        meCache.agentes = agentes;
        document.getElementById('me-agentes-title').textContent = `Agentes (${agentes.length})`;
        const tbody = document.getElementById('me-agentes-tbody');
        if (!agentes.length) {
            tbody.innerHTML = '<tr><td colspan="6" class="py-8 text-center text-gray-600">Nenhum agente cadastrado</td></tr>';
            return;
        }
        tbody.innerHTML = agentes.map(a => {
            const cats = (a.categorias && a.categorias.length && a.categorias[0]) ? a.categorias.map(c => c.nome).join(', ') : '—';
            return `<tr class="border-b border-gray-800/30 hover:bg-gray-800/20 transition">
                <td class="py-2.5 font-medium text-slate-200">${esc(a.nome)}</td>
                <td class="py-2.5">${esc(a.email)}</td>
                <td class="py-2.5">${esc(a.departamento || '—')}</td>
                <td class="py-2.5 text-xs">${esc(cats)}</td>
                <td class="py-2.5">${meBadge(a.ativo, 'ativo')}</td>
                <td class="py-2.5 flex gap-2">
                    <button onclick="meModalAgente(${a.id})" class="text-xs text-blue-400 hover:text-blue-300">Editar</button>
                    <button onclick="meToggleAgente(${a.id})" class="text-xs ${a.ativo ? 'text-red-400 hover:text-red-300' : 'text-emerald-400 hover:text-emerald-300'}">${a.ativo ? 'Desativar' : 'Ativar'}</button>
                </td>
            </tr>`;
        }).join('');
    } catch (e) { toast(e.message, 'error'); }
}

function meModalAgente(id) {
    const ag = id ? (meCache.agentes || []).find(a => a.id === id) : null;
    meOpenModal(ag ? 'Editar Agente' : 'Novo Agente',
        meFormGroup('Nome', 'me-f-nome', 'text', ag?.nome || '') +
        meFormGroup('E-mail', 'me-f-email', 'email', ag?.email || '') +
        meFormGroup('Departamento', 'me-f-dept', 'text', ag?.departamento || ''),
        `<button onclick="meCloseModal()" class="text-sm text-gray-500 hover:text-gray-300 px-4 py-2">Cancelar</button>
         <button onclick="meSalvarAgente(${id || 'null'})" class="btn-primary text-sm px-5 py-2 rounded-xl">Salvar</button>`
    );
}

async function meSalvarAgente(id) {
    const p = {
        nome: document.getElementById('me-f-nome').value.trim(),
        email: document.getElementById('me-f-email').value.trim(),
        departamento: document.getElementById('me-f-dept').value.trim()
    };
    if (!p.nome || !p.email) { toast('Nome e e-mail são obrigatórios', 'error'); return; }
    try {
        if (id) {
            const ag = (meCache.agentes || []).find(a => a.id === id);
            p.id = id; p.ativo = ag ? ag.ativo : true;
            await meApi('update_agente', p);
            toast('Agente atualizado', 'success');
        } else {
            await meApi('create_agente', p);
            toast('Agente criado', 'success');
        }
        meCloseModal(); meLoadAgentes();
    } catch (e) { toast(e.message, 'error'); }
}

async function meToggleAgente(id) {
    try {
        await meApi('toggle_agente', { id });
        toast('Status alterado', 'success');
        meLoadAgentes();
    } catch (e) { toast(e.message, 'error'); }
}

// ---------------------------------------------------------------------------
// Categorias
// ---------------------------------------------------------------------------
async function meLoadCategorias() {
    try {
        const cats = await meApi('list_categorias');
        meCache.categorias = cats;
        document.getElementById('me-cat-title').textContent = `Categorias (${cats.length})`;
        const tbody = document.getElementById('me-cat-tbody');
        if (!cats.length) {
            tbody.innerHTML = '<tr><td colspan="6" class="py-8 text-center text-gray-600">Nenhuma categoria</td></tr>';
            return;
        }
        tbody.innerHTML = cats.map(c => `<tr class="border-b border-gray-800/30 hover:bg-gray-800/20 transition">
            <td class="py-2.5 font-medium text-slate-200">${esc(c.nome)}</td>
            <td class="py-2.5 text-xs truncate max-w-[200px]">${esc((c.descricao || '').substring(0, 60))}</td>
            <td class="py-2.5 text-center">${c.total_agentes}</td>
            <td class="py-2.5 text-center">${c.total_emails}</td>
            <td class="py-2.5">${meBadge(c.ativo, 'ativo')}</td>
            <td class="py-2.5">
                <button onclick="meModalCategoria(${c.id})" class="text-xs text-blue-400 hover:text-blue-300">Editar</button>
            </td>
        </tr>`).join('');
    } catch (e) { toast(e.message, 'error'); }
}

function meModalCategoria(id) {
    const cat = id ? (meCache.categorias || []).find(c => c.id === id) : null;
    meOpenModal(cat ? 'Editar Categoria' : 'Nova Categoria',
        meFormGroup('Nome', 'me-f-cat-nome', 'text', cat?.nome || '') +
        `<div class="mb-4">
            <label class="block text-xs text-gray-500 mb-1.5 font-medium">Descrição</label>
            <textarea id="me-f-cat-desc" class="input-glass px-3 py-2 text-sm text-gray-200 w-full" rows="3">${esc(cat?.descricao || '')}</textarea>
        </div>` +
        (cat ? `<div class="mb-4 flex items-center gap-3">
            <label class="text-xs text-gray-500 font-medium">Ativo</label>
            <input type="checkbox" id="me-f-cat-ativo" ${cat.ativo ? 'checked' : ''} class="w-4 h-4 accent-blue-500">
        </div>` : ''),
        `<button onclick="meCloseModal()" class="text-sm text-gray-500 hover:text-gray-300 px-4 py-2">Cancelar</button>
         <button onclick="meSalvarCategoria(${id || 'null'})" class="btn-primary text-sm px-5 py-2 rounded-xl">Salvar</button>`
    );
}

async function meSalvarCategoria(id) {
    const p = {
        nome: document.getElementById('me-f-cat-nome').value.trim(),
        descricao: document.getElementById('me-f-cat-desc').value.trim()
    };
    if (!p.nome) { toast('Nome é obrigatório', 'error'); return; }
    try {
        if (id) {
            p.id = id;
            p.ativo = document.getElementById('me-f-cat-ativo')?.checked ?? true;
            await meApi('update_categoria', p);
            toast('Categoria atualizada', 'success');
        } else {
            await meApi('create_categoria', p);
            toast('Categoria criada', 'success');
        }
        meCloseModal(); meLoadCategorias();
    } catch (e) { toast(e.message, 'error'); }
}

// ---------------------------------------------------------------------------
// Roteamento
// ---------------------------------------------------------------------------
async function meLoadRoteamento() {
    try {
        const [rotas, agentes, categorias] = await Promise.all([
            meApi('list_roteamento'), meApi('list_agentes'), meApi('list_categorias')
        ]);
        meCache.agentes = agentes;
        meCache.categorias = categorias;
        meCache.rotas = rotas;
        document.getElementById('me-rota-title').textContent = `Roteamento (${rotas.length} vínculos)`;
        const tbody = document.getElementById('me-rota-tbody');
        if (!rotas.length) {
            tbody.innerHTML = '<tr><td colspan="6" class="py-8 text-center text-gray-600">Nenhum vínculo</td></tr>';
        } else {
            tbody.innerHTML = rotas.map(r => `<tr class="border-b border-gray-800/30 hover:bg-gray-800/20 transition">
                <td class="py-2.5 font-medium text-slate-200">${esc(r.categoria_nome)}</td>
                <td class="py-2.5">${esc(r.agente_nome)}</td>
                <td class="py-2.5 text-xs">${esc(r.agente_email)}</td>
                <td class="py-2.5 text-center">${r.prioridade}</td>
                <td class="py-2.5">${meBadge(r.agente_ativo, 'ativo')}</td>
                <td class="py-2.5">
                    <button onclick="meRemoveRota(${r.id})" class="text-xs text-red-400 hover:text-red-300">Remover</button>
                </td>
            </tr>`).join('');
        }

        const mapEl = document.getElementById('me-rota-map');
        const catMap = {};
        rotas.forEach(r => {
            if (!catMap[r.categoria_nome]) catMap[r.categoria_nome] = [];
            catMap[r.categoria_nome].push(r.agente_nome + ' (' + r.agente_email + ')');
        });
        if (Object.keys(catMap).length) {
            mapEl.innerHTML = Object.entries(catMap).map(([cat, ags]) =>
                `<div class="flex items-start gap-2">
                    <span class="text-blue-400 font-semibold min-w-[100px]">${esc(cat)}</span>
                    <span class="material-symbols-outlined text-sm text-gray-600">arrow_forward</span>
                    <span class="text-gray-300">${ags.map(a => esc(a)).join(', ')}</span>
                </div>`
            ).join('');
        } else {
            mapEl.innerHTML = '<p class="text-gray-600 text-sm">Configure vínculos acima</p>';
        }
    } catch (e) { toast(e.message, 'error'); }
}

function meModalRota() {
    const agentes = (meCache.agentes || []).filter(a => a.ativo);
    const categorias = (meCache.categorias || []).filter(c => c.ativo);
    const agOpts = agentes.map(a => `<option value="${a.id}">${esc(a.nome)} (${esc(a.email)})</option>`).join('');
    const catOpts = categorias.map(c => `<option value="${c.id}">${esc(c.nome)}</option>`).join('');
    meOpenModal('Novo Vínculo',
        `<div class="mb-4"><label class="block text-xs text-gray-500 mb-1.5 font-medium">Categoria</label><select id="me-f-rota-cat" class="input-glass px-3 py-2 text-sm text-gray-200 w-full">${catOpts}</select></div>` +
        `<div class="mb-4"><label class="block text-xs text-gray-500 mb-1.5 font-medium">Agente</label><select id="me-f-rota-ag" class="input-glass px-3 py-2 text-sm text-gray-200 w-full">${agOpts}</select></div>` +
        meFormGroup('Prioridade (0 = mais alta)', 'me-f-rota-pri', 'number', '0'),
        `<button onclick="meCloseModal()" class="text-sm text-gray-500 hover:text-gray-300 px-4 py-2">Cancelar</button>
         <button onclick="meSalvarRota()" class="btn-primary text-sm px-5 py-2 rounded-xl">Salvar</button>`
    );
}

async function meSalvarRota() {
    try {
        await meApi('add_roteamento', {
            agente_id: parseInt(document.getElementById('me-f-rota-ag').value),
            categoria_id: parseInt(document.getElementById('me-f-rota-cat').value),
            prioridade: parseInt(document.getElementById('me-f-rota-pri').value) || 0
        });
        toast('Vínculo criado', 'success');
        meCloseModal(); meLoadRoteamento();
    } catch (e) { toast(e.message, 'error'); }
}

async function meRemoveRota(id) {
    if (!confirm('Remover este vínculo?')) return;
    try {
        await meApi('remove_roteamento', { id });
        toast('Vínculo removido', 'success');
        meLoadRoteamento();
    } catch (e) { toast(e.message, 'error'); }
}

// ---------------------------------------------------------------------------
// Logs
// ---------------------------------------------------------------------------
async function meLoadLogs() {
    const params = { limit: ME_LOGS_LIMIT, offset: meLogsPage * ME_LOGS_LIMIT };
    const di = document.getElementById('me-log-di')?.value;
    const df = document.getElementById('me-log-df')?.value;
    const cat = document.getElementById('me-log-cat')?.value;
    const st = document.getElementById('me-log-st')?.value;
    const busca = document.getElementById('me-log-busca')?.value;
    if (di) params.data_inicio = di;
    if (df) params.data_fim = df;
    if (cat) params.categoria_id = parseInt(cat);
    if (st) params.status = st;
    if (busca) params.busca = busca;

    try {
        if (!meCache.categorias) meCache.categorias = await meApi('list_categorias');
        const sel = document.getElementById('me-log-cat');
        if (sel.options.length <= 1) {
            meCache.categorias.forEach(c => {
                const o = document.createElement('option');
                o.value = c.id; o.textContent = c.nome;
                sel.appendChild(o);
            });
        }

        const logs = await meApi('list_logs', params);
        meCache.logs = logs;
        const tbody = document.getElementById('me-logs-tbody');
        if (!logs.length) {
            tbody.innerHTML = '<tr><td colspan="8" class="py-8 text-center text-gray-600">Nenhum log encontrado</td></tr>';
        } else {
            tbody.innerHTML = logs.map(l => `<tr class="border-b border-gray-800/30 hover:bg-gray-800/20 transition cursor-pointer" onclick="meModalLog(${l.id})">
                <td class="py-2">${l.id}</td>
                <td class="py-2 whitespace-nowrap text-xs">${fmtDate(l.criado_em) || '—'}</td>
                <td class="py-2 text-xs">${esc(l.de_nome || l.de_email || '—')}</td>
                <td class="py-2 text-xs truncate max-w-[180px]">${esc((l.assunto || '').substring(0, 50))}</td>
                <td class="py-2">${esc(l.categoria_nome || l.categoria_nome_ref || '—')}</td>
                <td class="py-2 text-xs">${esc(l.agente_nome_ref || '—')}</td>
                <td class="py-2">${meBadge(l.status, 'status')}</td>
                <td class="py-2">${meBadge(l.urgencia, 'urgencia')}</td>
            </tr>`).join('');
        }

        const pag = document.getElementById('me-logs-pag');
        pag.innerHTML = `
            <button onclick="meLogsPage--;meLoadLogs()" ${meLogsPage === 0 ? 'disabled' : ''} class="text-xs px-3 py-1.5 rounded-lg border border-gray-700/30 text-gray-400 hover:text-white disabled:opacity-30 disabled:cursor-default transition">Anterior</button>
            <span class="text-xs text-gray-500">Página ${meLogsPage + 1}</span>
            <button onclick="meLogsPage++;meLoadLogs()" ${logs.length < ME_LOGS_LIMIT ? 'disabled' : ''} class="text-xs px-3 py-1.5 rounded-lg border border-gray-700/30 text-gray-400 hover:text-white disabled:opacity-30 disabled:cursor-default transition">Próximo</button>`;
    } catch (e) { toast(e.message, 'error'); }
}

function meModalLog(id) {
    const l = (meCache.logs || []).find(x => x.id === id);
    if (!l) return;
    let body = '<div class="space-y-3 text-sm">';
    body += `<div class="flex gap-4">${meField('Protocolo', '#' + l.id)}${meField('Data', fmtDate(l.criado_em))}</div>`;
    body += `<div class="flex gap-4">${meField('De', (l.de_nome || '') + ' (' + (l.de_email || '') + ')')}${meField('Para', l.para_email)}</div>`;
    body += meField('Assunto', l.assunto);
    body += '<hr class="border-gray-700/30">';
    body += `<div class="flex gap-4">${meField('Categoria', l.categoria_nome || l.categoria_nome_ref)}${meField('Agente', l.agente_nome_ref || l.agente_email)}</div>`;
    body += `<div class="flex gap-4"><div>${meBadge(l.status, 'status')}</div><div>${meBadge(l.urgencia, 'urgencia')}</div></div>`;
    if (l.cpf) body += meField('CPF', l.cpf);
    if (l.resumo_ia) body += `<div><span class="text-gray-500 text-xs">Resumo IA</span><p class="text-gray-300 mt-1">${esc(l.resumo_ia)}</p></div>`;
    if (l.corpo_resumo) body += `<div><span class="text-gray-500 text-xs">Corpo</span><pre class="text-gray-400 mt-1 text-xs bg-gray-800/40 rounded-lg p-3 max-h-[200px] overflow-y-auto whitespace-pre-wrap">${esc(l.corpo_resumo)}</pre></div>`;
    body += '</div>';
    meOpenModal('E-mail #' + l.id, body,
        '<button onclick="meCloseModal()" class="text-sm text-gray-500 hover:text-gray-300 px-4 py-2">Fechar</button>');
}

// ---------------------------------------------------------------------------
// Configurações
// ---------------------------------------------------------------------------
async function meLoadConfig() {
    try {
        const configs = await meApi('get_config');
        meCache.configs = configs;
        const el = document.getElementById('me-config-list');
        el.innerHTML = configs.map(c => {
            const isLong = (c.valor || '').length > 100;
            return `<div class="glass-card p-5">
                <div class="flex items-start justify-between gap-4">
                    <div class="flex-1">
                        <code class="text-sm text-indigo-400 font-mono font-semibold">${esc(c.chave)}</code>
                        <p class="text-xs text-gray-500 mt-0.5">${esc(c.descricao || '')}</p>
                        ${isLong
                            ? `<textarea id="me-cfg-${esc(c.chave)}" class="input-glass px-3 py-2 text-sm text-gray-200 w-full mt-2 font-mono" rows="3">${esc(c.valor || '')}</textarea>`
                            : `<input id="me-cfg-${esc(c.chave)}" value="${esc(c.valor || '')}" class="input-glass px-3 py-2 text-sm text-gray-200 w-full mt-2">`
                        }
                    </div>
                    <button onclick="meSalvarConfig('${esc(c.chave)}')" class="btn-primary text-xs px-4 py-2 rounded-lg mt-6 whitespace-nowrap">Salvar</button>
                </div>
                <p class="text-[10px] text-gray-600 mt-2">Atualizado: ${fmtDate(c.atualizado_em) || '—'}</p>
            </div>`;
        }).join('');
    } catch (e) { toast(e.message, 'error'); }
}

async function meSalvarConfig(chave) {
    const el = document.getElementById('me-cfg-' + chave);
    if (!el) return;
    try {
        await meApi('set_config', { chave, valor: el.value });
        toast('Configuração salva', 'success');
    } catch (e) { toast(e.message, 'error'); }
}

// ---------------------------------------------------------------------------
// Modal & helpers
// ---------------------------------------------------------------------------
function meOpenModal(title, bodyHtml, footHtml) {
    document.getElementById('me-modal-title').textContent = title;
    document.getElementById('me-modal-body').innerHTML = bodyHtml;
    document.getElementById('me-modal-foot').innerHTML = footHtml;
    document.getElementById('me-modal-overlay').classList.remove('hidden');
    document.getElementById('me-modal-overlay').classList.add('flex');
}

function meCloseModal() {
    document.getElementById('me-modal-overlay').classList.add('hidden');
    document.getElementById('me-modal-overlay').classList.remove('flex');
}

function meFormGroup(label, id, type, value) {
    return `<div class="mb-4">
        <label class="block text-xs text-gray-500 mb-1.5 font-medium">${label}</label>
        <input type="${type}" id="${id}" value="${esc(value || '')}" class="input-glass px-3 py-2 text-sm text-gray-200 w-full">
    </div>`;
}

function meField(label, value) {
    return `<div class="flex-1"><span class="text-gray-500 text-xs">${label}</span><p class="text-gray-300">${value ? esc(String(value)) : '<span class="text-gray-600">—</span>'}</p></div>`;
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------
function loadMarcoEmail() {
    meTab('dash');
}
