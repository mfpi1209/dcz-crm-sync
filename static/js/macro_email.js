// ---------------------------------------------------------------------------
// Macro Email — Classificação & Distribuição de E-mails
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

async function meDistApi() {
    const res = await fetch('/api/distribuicao');
    return await res.json();
}

// ---------------------------------------------------------------------------
// Tab navigation
// ---------------------------------------------------------------------------
const ME_TABS = ['dash', 'agentes', 'categorias', 'roteamento', 'logs', 'kb', 'config'];

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
    if (tab === 'roteamento') meLoadDistribuicao();
    if (tab === 'logs') meLoadLogs();
    if (tab === 'kb') meLoadKB();
    if (tab === 'config') meLoadConfig();
}

// ---------------------------------------------------------------------------
// Dashboard
// ---------------------------------------------------------------------------
async function meLoadDash() {
    try {
        const [s, distData] = await Promise.all([
            meApi('get_stats'),
            meDistApi().catch(() => ({ distribuicao: [] }))
        ]);
        const distAgentes = (distData.distribuicao || []).filter(a => a.status === 'Ativo');

        const cards = document.getElementById('me-dash-cards');
        cards.innerHTML = [
            meDashCard('Total', s.total_emails || 0, 'mail', 'blue'),
            meDashCard('Hoje', s.emails_hoje || 0, 'today', 'cyan'),
            meDashCard('7 dias', s.emails_7dias || 0, 'date_range', 'teal'),
            meDashCard('Consultores Ativos', distAgentes.length, 'support_agent', 'emerald'),
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
        ativo: { true: 'emerald', false: 'red', Ativo: 'emerald', Inativo: 'red' }
    };
    const m = maps[type] || {};
    const c = m[String(val)] || 'gray';
    const label = type === 'ativo' ? (val === true || val === 'Ativo' ? 'Ativo' : 'Inativo') : val;
    return `<span class="inline-flex items-center px-2 py-0.5 rounded-full text-[10px] font-semibold bg-${c}-500/15 text-${c}-400">${label}</span>`;
}

// ---------------------------------------------------------------------------
// Consultores (Distribuição Acadêmica)
// ---------------------------------------------------------------------------
async function meLoadAgentes() {
    const refreshIcon = document.getElementById('me-agentes-refresh');
    if (refreshIcon) refreshIcon.classList.add('animate-spin');

    try {
        const [distData, agentStats, emailCfg] = await Promise.all([
            meDistApi(),
            meApi('agent_stats'),
            meApi('get_dist_email').catch(() => [])
        ]);

        const emailMap = {};
        (emailCfg || []).forEach(c => { emailMap[String(c.dist_id)] = c.distribuir_email; });

        const consultores = distData.distribuicao || [];
        consultores.forEach(c => { c.distribuir_email = !!emailMap[String(c.id)]; });
        const statsMap = {};
        (agentStats || []).forEach(s => {
            const key = (s.agente_nome || '').toLowerCase().trim();
            statsMap[key] = s;
        });

        document.getElementById('me-agentes-title').textContent = `Consultores (${consultores.length})`;
        const tbody = document.getElementById('me-agentes-tbody');

        if (!consultores.length) {
            tbody.innerHTML = '<tr><td colspan="8" class="py-8 text-center text-gray-600">Nenhum consultor na distribuição</td></tr>';
            return;
        }

        const avatarColors = ['from-indigo-500 to-purple-500', 'from-emerald-500 to-teal-500', 'from-amber-500 to-orange-500', 'from-rose-500 to-pink-500', 'from-cyan-500 to-blue-500'];

        tbody.innerHTML = consultores.map((c, idx) => {
            const key = (c.responsavel || '').toLowerCase().trim();
            const stats = statsMap[key] || {};
            const initials = c.responsavel ? c.responsavel.split(' ').map(n => n[0]).join('').slice(0, 2).toUpperCase() : '??';
            const avatarColor = avatarColors[idx % avatarColors.length];
            const filaNum = parseInt(c.fila) || 0;
            const filaBg = filaNum > 5 ? 'bg-rose-500/15 text-rose-400 border-rose-500/30' :
                           filaNum > 0 ? 'bg-amber-500/15 text-amber-400 border-amber-500/30' :
                           'bg-emerald-500/10 text-emerald-400 border-emerald-500/20';
            const tipoBadge = c.tipo_atendimento === 'Atendimento'
                ? 'bg-indigo-500/15 text-indigo-400' : 'bg-emerald-500/15 text-emerald-400';

            return `<tr class="border-b border-gray-800/30 hover:bg-gray-800/20 transition">
                <td class="py-2.5">
                    <div class="flex items-center gap-3">
                        <div class="w-8 h-8 rounded-xl bg-gradient-to-br ${avatarColor} flex items-center justify-center text-white text-xs font-bold shadow-lg">${initials}</div>
                        <span class="font-medium text-slate-200">${esc(c.responsavel)}</span>
                    </div>
                </td>
                <td class="py-2.5 text-center">${meBadge(c.status, 'ativo')}</td>
                <td class="py-2.5 text-center">
                    <span class="inline-flex items-center justify-center min-w-[36px] px-2 py-0.5 rounded-lg text-xs font-bold border ${filaBg}">${filaNum}</span>
                </td>
                <td class="py-2.5 text-center">
                    <span class="inline-flex items-center px-2 py-0.5 rounded-full text-[10px] font-semibold ${tipoBadge}">${esc(c.tipo_atendimento || '—')}</span>
                </td>
                <td class="py-2.5 text-center">
                    ${c.distribuir_email
                        ? '<span class="inline-flex items-center px-2 py-0.5 rounded-full text-[10px] font-semibold bg-cyan-500/15 text-cyan-400">Sim</span>'
                        : '<span class="inline-flex items-center px-2 py-0.5 rounded-full text-[10px] font-semibold bg-gray-500/15 text-gray-500">Não</span>'}
                </td>
                <td class="py-2.5 text-center font-semibold text-slate-300">${stats.total_emails || 0}</td>
                <td class="py-2.5 text-center text-cyan-400 font-semibold">${stats.emails_hoje || 0}</td>
                <td class="py-2.5 text-xs text-gray-400">${esc(c.ultima_execucao || '—')}</td>
            </tr>`;
        }).join('');
    } catch (e) {
        toast(e.message, 'error');
    } finally {
        if (refreshIcon) refreshIcon.classList.remove('animate-spin');
    }
}

// ---------------------------------------------------------------------------
// Distribuição de Emails (antes Roteamento)
// ---------------------------------------------------------------------------
async function meLoadDistribuicao() {
    try {
        const [agentStats, recentEmails] = await Promise.all([
            meApi('agent_stats'),
            meApi('recent_distributed')
        ]);

        const rankingEl = document.getElementById('me-dist-ranking');
        if (agentStats && agentStats.length) {
            const max = Math.max(...agentStats.map(a => parseInt(a.total_emails) || 0), 1);
            rankingEl.innerHTML = agentStats.map(a => {
                const total = parseInt(a.total_emails) || 0;
                const hoje = parseInt(a.emails_hoje) || 0;
                const w = Math.max(total / max * 100, 8);
                return `<div class="flex items-center gap-3">
                    <span class="w-32 text-xs text-gray-300 truncate font-medium">${esc(a.agente_nome)}</span>
                    <div class="flex-1 bg-gray-800/50 rounded-full h-6 overflow-hidden">
                        <div class="h-full bg-blue-500/40 rounded-full flex items-center justify-between px-2 text-[10px] font-semibold" style="width:${w}%">
                            <span class="text-blue-300">${total}</span>
                            ${hoje > 0 ? `<span class="text-cyan-300">+${hoje} hoje</span>` : ''}
                        </div>
                    </div>
                </div>`;
            }).join('');
        } else {
            rankingEl.innerHTML = '<p class="text-gray-600 text-sm py-4">Nenhum email distribuído ainda</p>';
        }

        const summaryEl = document.getElementById('me-dist-summary');
        if (agentStats && agentStats.length) {
            const totalEmails = agentStats.reduce((s, a) => s + (parseInt(a.total_emails) || 0), 0);
            const totalHoje = agentStats.reduce((s, a) => s + (parseInt(a.emails_hoje) || 0), 0);
            const total7d = agentStats.reduce((s, a) => s + (parseInt(a.emails_7dias) || 0), 0);
            summaryEl.innerHTML = `
                <div class="flex items-center justify-between bg-gray-800/30 rounded-lg p-3">
                    <span class="text-gray-400">Total distribuído</span>
                    <span class="text-lg font-bold text-blue-400">${totalEmails}</span>
                </div>
                <div class="flex items-center justify-between bg-gray-800/30 rounded-lg p-3">
                    <span class="text-gray-400">Distribuído hoje</span>
                    <span class="text-lg font-bold text-cyan-400">${totalHoje}</span>
                </div>
                <div class="flex items-center justify-between bg-gray-800/30 rounded-lg p-3">
                    <span class="text-gray-400">Últimos 7 dias</span>
                    <span class="text-lg font-bold text-teal-400">${total7d}</span>
                </div>
                <div class="flex items-center justify-between bg-gray-800/30 rounded-lg p-3">
                    <span class="text-gray-400">Consultores ativos</span>
                    <span class="text-lg font-bold text-emerald-400">${agentStats.length}</span>
                </div>`;
        } else {
            summaryEl.innerHTML = '<p class="text-gray-600 text-sm">Sem dados</p>';
        }

        const tbody = document.getElementById('me-dist-tbody');
        if (recentEmails && recentEmails.length) {
            tbody.innerHTML = recentEmails.map(e => `<tr class="border-b border-gray-800/30 hover:bg-gray-800/20 transition">
                <td class="py-2 whitespace-nowrap text-xs">${fmtDate(e.criado_em) || '—'}</td>
                <td class="py-2 text-xs">${esc(e.de_nome || e.de_email || '—')}</td>
                <td class="py-2 text-xs truncate max-w-[200px]">${esc((e.assunto || '').substring(0, 50))}</td>
                <td class="py-2">${esc(e.categoria_nome || '—')}</td>
                <td class="py-2 font-medium text-slate-200 text-xs">${esc(e.agente_nome || '—')}</td>
                <td class="py-2">${meBadge(e.status, 'status')}</td>
            </tr>`).join('');
        } else {
            tbody.innerHTML = '<tr><td colspan="6" class="py-8 text-center text-gray-600">Nenhum email distribuído</td></tr>';
        }
    } catch (e) {
        toast(e.message, 'error');
    }
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
                <td class="py-2 text-xs">${esc(l.agente_nome_ref || l.agente_nome || '—')}</td>
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
    body += `<div class="flex gap-4">${meField('Categoria', l.categoria_nome || l.categoria_nome_ref)}${meField('Consultor', l.agente_nome_ref || l.agente_nome || l.agente_email)}</div>`;
    body += `<div class="flex gap-4"><div>${meBadge(l.status, 'status')}</div><div>${meBadge(l.urgencia, 'urgencia')}</div></div>`;
    if (l.cpf) body += meField('CPF', l.cpf);
    if (l.resumo_ia) body += `<div><span class="text-gray-500 text-xs">Resumo IA</span><p class="text-gray-300 mt-1">${esc(l.resumo_ia)}</p></div>`;
    if (l.corpo_resumo) body += `<div><span class="text-gray-500 text-xs">Corpo</span><pre class="text-gray-400 mt-1 text-xs bg-gray-800/40 rounded-lg p-3 max-h-[200px] overflow-y-auto whitespace-pre-wrap">${esc(l.corpo_resumo)}</pre></div>`;
    body += '</div>';
    meOpenModal('E-mail #' + l.id, body,
        '<button onclick="meCloseModal()" class="text-sm text-gray-500 hover:text-gray-300 px-4 py-2">Fechar</button>');
}

// ---------------------------------------------------------------------------
// Base de Conhecimento
// ---------------------------------------------------------------------------
async function meLoadKB() {
    try {
        const items = await meApi('list_kb');
        meCache.kb = items;
        meRenderKB();
    } catch (e) { toast(e.message, 'error'); }
}

function meRenderKB() {
    const items = meCache.kb || [];
    const filter = document.getElementById('me-kb-filter-cat')?.value || '';
    const filtered = filter ? items.filter(i => i.categoria === filter) : items;
    const grid = document.getElementById('me-kb-grid');

    if (!filtered.length) {
        grid.innerHTML = '<div class="text-gray-500 text-sm text-center py-8 col-span-full">Nenhuma resposta cadastrada' + (filter ? ' nesta categoria' : '') + '</div>';
        return;
    }

    const catColors = {
        financeiro: 'amber', academico: 'blue', documentos: 'violet',
        estagio: 'teal', juridico: 'red', tecnico: 'cyan', geral: 'gray'
    };

    grid.innerHTML = filtered.map(item => {
        const color = catColors[item.categoria] || 'gray';
        return `<div class="bg-gray-800/30 border border-gray-700/20 rounded-xl p-4 hover:border-gray-600/30 transition">
            <div class="flex items-start justify-between gap-2 mb-3">
                <div class="flex items-center gap-2 flex-wrap">
                    <span class="inline-flex items-center px-2 py-0.5 rounded-full text-[10px] font-semibold bg-${color}-500/15 text-${color}-400">${esc(item.categoria)}</span>
                    <span class="text-[10px] text-gray-500">${esc(item.subcategoria)}</span>
                </div>
                <div class="flex items-center gap-1.5 shrink-0">
                    ${item.ativo
                        ? '<span class="w-2 h-2 rounded-full bg-emerald-500"></span>'
                        : '<span class="w-2 h-2 rounded-full bg-red-500"></span>'}
                    <button onclick="meModalKB(${item.id})" class="text-xs text-blue-400 hover:text-blue-300">Editar</button>
                    <button onclick="meDeleteKB(${item.id})" class="text-xs text-red-400 hover:text-red-300">Excluir</button>
                </div>
            </div>
            <p class="text-sm text-slate-200 font-medium mb-2">${esc(item.pergunta_exemplo)}</p>
            <p class="text-xs text-gray-400 line-clamp-3">${esc(item.resposta_modelo)}</p>
            ${item.palavras_chave ? `<div class="mt-2 flex flex-wrap gap-1">${item.palavras_chave.split(',').map(w => `<span class="text-[9px] bg-gray-700/50 text-gray-400 px-1.5 py-0.5 rounded">${esc(w.trim())}</span>`).join('')}</div>` : ''}
        </div>`;
    }).join('');
}

const ME_CATS = ['financeiro', 'academico', 'documentos', 'estagio', 'juridico', 'tecnico', 'geral'];

function meModalKB(id) {
    const item = id ? (meCache.kb || []).find(k => k.id === id) : null;
    const catOpts = ME_CATS.map(c => `<option value="${c}" ${item?.categoria === c ? 'selected' : ''}>${c}</option>`).join('');

    meOpenModal(item ? 'Editar Resposta' : 'Nova Resposta',
        `<div class="mb-4">
            <label class="block text-xs text-gray-500 mb-1.5 font-medium">Categoria</label>
            <select id="me-f-kb-cat" class="input-glass px-3 py-2 text-sm text-gray-200 w-full">${catOpts}</select>
        </div>` +
        meFormGroup('Subcategoria', 'me-f-kb-sub', 'text', item?.subcategoria || '') +
        `<div class="mb-4">
            <label class="block text-xs text-gray-500 mb-1.5 font-medium">Pergunta Exemplo</label>
            <textarea id="me-f-kb-pergunta" class="input-glass px-3 py-2 text-sm text-gray-200 w-full" rows="2">${esc(item?.pergunta_exemplo || '')}</textarea>
        </div>` +
        `<div class="mb-4">
            <label class="block text-xs text-gray-500 mb-1.5 font-medium">Resposta Modelo</label>
            <textarea id="me-f-kb-resposta" class="input-glass px-3 py-2 text-sm text-gray-200 w-full" rows="5">${esc(item?.resposta_modelo || '')}</textarea>
        </div>` +
        meFormGroup('Palavras-chave (separadas por vírgula)', 'me-f-kb-palavras', 'text', item?.palavras_chave || '') +
        (item ? `<div class="mb-4 flex items-center gap-3">
            <label class="text-xs text-gray-500 font-medium">Ativo</label>
            <input type="checkbox" id="me-f-kb-ativo" ${item.ativo ? 'checked' : ''} class="w-4 h-4 accent-blue-500">
        </div>` : ''),
        `<button onclick="meCloseModal()" class="text-sm text-gray-500 hover:text-gray-300 px-4 py-2">Cancelar</button>
         <button onclick="meSalvarKB(${id || 'null'})" class="btn-primary text-sm px-5 py-2 rounded-xl">Salvar</button>`
    );
}

async function meSalvarKB(id) {
    const p = {
        categoria: document.getElementById('me-f-kb-cat').value,
        subcategoria: document.getElementById('me-f-kb-sub').value.trim(),
        pergunta_exemplo: document.getElementById('me-f-kb-pergunta').value.trim(),
        resposta_modelo: document.getElementById('me-f-kb-resposta').value.trim(),
        palavras_chave: document.getElementById('me-f-kb-palavras').value.trim()
    };
    if (!p.subcategoria || !p.pergunta_exemplo || !p.resposta_modelo) {
        toast('Subcategoria, pergunta e resposta são obrigatórios', 'error');
        return;
    }
    try {
        if (id) {
            p.id = id;
            p.ativo = document.getElementById('me-f-kb-ativo')?.checked ?? true;
            await meApi('update_kb', p);
            toast('Resposta atualizada', 'success');
        } else {
            await meApi('create_kb', p);
            toast('Resposta criada', 'success');
        }
        meCloseModal();
        meLoadKB();
    } catch (e) { toast(e.message, 'error'); }
}

async function meDeleteKB(id) {
    if (!confirm('Excluir esta resposta da base de conhecimento?')) return;
    try {
        await meApi('delete_kb', { id });
        toast('Resposta excluída', 'success');
        meLoadKB();
    } catch (e) { toast(e.message, 'error'); }
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
function loadMacroEmail() {
    meTab('dash');
}
