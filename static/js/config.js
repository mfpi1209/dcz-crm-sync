// ---------------------------------------------------------------------------
// Logs / Relatórios
// ---------------------------------------------------------------------------
let _currentLogFile = null;

async function loadLogFiles() {
    try {
        const res = await api('/api/logs');
        const data = await res.json();
        const list = document.getElementById('log-file-list');

        if (!data.files || !data.files.length) {
            list.innerHTML = '<p class="text-gray-500 text-xs">Nenhum arquivo encontrado.</p>';
            return;
        }

        list.innerHTML = data.files.map(f => {
            const size = f.size > 1048576 ? (f.size / 1048576).toFixed(1) + ' MB' : (f.size / 1024).toFixed(0) + ' KB';
            const isActive = _currentLogFile === f.path;
            return `<button onclick="viewLogFile('${esc(f.path)}')"
                class="w-full text-left px-3 py-2 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-800/40 transition ${isActive ? 'bg-blue-500/10 text-blue-400 border border-blue-500/20' : 'text-gray-300 border border-transparent'}">
                <div class="font-medium text-xs truncate">${esc(f.name)}</div>
                <div class="text-[10px] text-gray-600">${esc(f.dir)} &middot; ${size} &middot; ${fmtDate(f.modified) || '—'}</div>
            </button>`;
        }).join('');
    } catch (err) {
        console.error(err);
    }
}

async function viewLogFile(filepath) {
    _currentLogFile = filepath;
    const tail = document.getElementById('log-tail-select').value;
    const title = document.getElementById('log-viewer-title');
    const content = document.getElementById('log-viewer-content');
    const btnDl = document.getElementById('btn-download-log');

    title.textContent = filepath;
    content.textContent = 'Carregando...';
    btnDl.classList.remove('hidden');

    try {
        const res = await api(`/api/logs/view/${filepath}?tail=${tail}`);
        const data = await res.json();
        if (data.error) {
            content.textContent = 'Erro: ' + data.error;
            return;
        }
        title.textContent = `${data.name} (${data.showing}/${data.total_lines} linhas)`;
        content.textContent = data.lines.join('\n');
        content.scrollTop = content.scrollHeight;
    } catch (err) {
        content.textContent = 'Erro: ' + err.message;
    }

    loadLogFiles();
}

function reloadLogView() {
    if (_currentLogFile) viewLogFile(_currentLogFile);
}

function downloadCurrentLog() {
    if (!_currentLogFile) return;
    window.open(`/api/logs/download/${_currentLogFile}`, '_blank');
}

// ---------------------------------------------------------------------------
// Configurações (Turmas + Ciclos)
// ---------------------------------------------------------------------------
let _configTab = 'ciclos';

function switchConfigTab(tab) {
    _configTab = tab;
    const tabs = ['ciclos', 'turmas', 'usuarios', 'regua'];
    const activeClass = 'text-sm font-semibold px-5 py-2 rounded-lg transition bg-blue-500/15 text-blue-400 border border-blue-500/30 flex items-center gap-2';
    const inactiveClass = 'text-sm font-semibold px-5 py-2 rounded-lg transition text-gray-500 hover:text-gray-300 flex items-center gap-2';
    tabs.forEach(t => {
        const tabEl = document.getElementById('cfg-tab-' + t);
        const secEl = document.getElementById('cfg-section-' + t);
        if (tabEl) tabEl.className = t === tab ? activeClass : inactiveClass;
        if (secEl) secEl.classList.toggle('hidden', t !== tab);
    });
    if (tab === 'ciclos') loadCiclos();
    if (tab === 'turmas') loadTurmas();
    if (tab === 'usuarios') loadUsers();
    if (tab === 'regua') _loadCommRules();
}

// Turmas
let _turmaTab = 'grad';
let _turmaData = [];

async function loadTurmas() {
    try {
        const res = await api('/api/turmas');
        _turmaData = await res.json();
        renderTurmas();
    } catch (e) { console.error(e); }
}

function switchTurmaTab(tab) {
    _turmaTab = tab;
    document.getElementById('tab-turma-grad').className = tab === 'grad'
        ? 'text-sm font-semibold px-4 py-1.5 rounded-lg transition bg-blue-500/15 text-blue-400 border border-blue-500/30'
        : 'text-sm font-semibold px-4 py-1.5 rounded-lg transition text-gray-500 hover:text-gray-300';
    document.getElementById('tab-turma-pos').className = tab === 'pos'
        ? 'text-sm font-semibold px-4 py-1.5 rounded-lg transition bg-blue-500/15 text-blue-400 border border-blue-500/30'
        : 'text-sm font-semibold px-4 py-1.5 rounded-lg transition text-gray-500 hover:text-gray-300';
    renderTurmas();
}

function renderTurmas() {
    const nivel = _turmaTab === 'grad' ? 'Graduação' : 'Pós-Graduação';
    const filtered = _turmaData.filter(t => t.nivel === nivel).sort((a,b) => a.dt_inicio.localeCompare(b.dt_inicio));
    const tbody = document.getElementById('turma-tbody');
    if (!filtered.length) {
        tbody.innerHTML = '<tr><td colspan="5" class="py-4 text-center text-gray-500">Nenhuma turma encontrada</td></tr>';
        return;
    }
    tbody.innerHTML = filtered.map(t => `
        <tr class="border-b border-[var(--border)] hover:bg-white/[0.02]" data-turma-id="${t.id}">
            <td class="py-2.5 pr-4">
                <input type="text" value="${t.nome}" class="bg-transparent border-b border-transparent hover:border-gray-600 focus:border-indigo-500 outline-none text-gray-200 w-full turma-nome" />
            </td>
            <td class="py-2.5 pr-4">
                <input type="date" value="${t.dt_inicio}" class="input-glass px-2 py-1 text-sm text-gray-200 turma-inicio" />
            </td>
            <td class="py-2.5 pr-4">
                <input type="date" value="${t.dt_fim}" class="input-glass px-2 py-1 text-sm text-gray-200 turma-fim" />
            </td>
            <td class="py-2.5 pr-4 text-gray-400">${t.ano}</td>
            <td class="py-2.5 flex gap-2">
                <button onclick="saveTurma(${t.id})" title="Salvar" class="text-emerald-400 hover:text-emerald-300 transition">
                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/></svg>
                </button>
                <button onclick="deleteTurma(${t.id})" title="Excluir" class="text-rose-400 hover:text-rose-300 transition">
                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"/></svg>
                </button>
            </td>
        </tr>
    `).join('');
}

async function saveTurma(id) {
    const row = document.querySelector(`tr[data-turma-id="${id}"]`);
    if (!row) return;
    const nome = row.querySelector('.turma-nome').value.trim();
    const dt_inicio = row.querySelector('.turma-inicio').value;
    const dt_fim = row.querySelector('.turma-fim').value;
    const nivel = _turmaTab === 'grad' ? 'Graduação' : 'Pós-Graduação';
    const ano = parseInt(dt_inicio.substring(0, 4)) || 2026;
    try {
        await api(`/api/turmas/${id}`, { method: 'PUT', headers: {'Content-Type':'application/json'}, body: JSON.stringify({ nivel, nome, dt_inicio, dt_fim, ano }) });
        loadTurmas();
    } catch (e) { toast('Erro ao salvar: ' + e.message, 'error'); }
}

async function deleteTurma(id) {
    if (!confirm('Excluir esta turma?')) return;
    try {
        await api(`/api/turmas/${id}`, { method: 'DELETE' });
        loadTurmas();
    } catch (e) { toast('Erro ao excluir: ' + e.message, 'error'); }
}

async function seedTurmas() {
    const ano = parseInt(document.getElementById('turma-ano').value) || 2026;
    try {
        const res = await api('/api/turmas/seed', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({ ano }) });
        const d = await res.json();
        toast(`Turmas geradas: ${d.created} novas para ${d.ano}`, 'success');
        loadTurmas();
    } catch (e) { toast('Erro: ' + e.message, 'error'); }
}

async function createTurma() {
    const nivel = document.getElementById('turma-new-nivel').value;
    const nome = document.getElementById('turma-new-nome').value.trim();
    const dt_inicio = document.getElementById('turma-new-inicio').value;
    const dt_fim = document.getElementById('turma-new-fim').value;
    const ano = parseInt(document.getElementById('turma-new-ano').value) || 2026;
    if (!nome || !dt_inicio || !dt_fim) { toast('Preencha todos os campos', 'warning'); return; }
    try {
        await api('/api/turmas', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({ nivel, nome, dt_inicio, dt_fim, ano }) });
        document.getElementById('turma-new-nome').value = '';
        document.getElementById('turma-new-inicio').value = '';
        document.getElementById('turma-new-fim').value = '';
        loadTurmas();
    } catch (e) { toast('Erro: ' + e.message, 'error'); }
}

// ---------------------------------------------------------------------------
// Ciclos
// ---------------------------------------------------------------------------
let _cicloTab = 'grad';
let _cicloData = [];

async function loadCiclos() {
    try {
        const res = await api('/api/ciclos');
        _cicloData = await res.json();
        renderCiclos();
    } catch (e) { console.error(e); }
}

function switchCicloTab(tab) {
    _cicloTab = tab;
    document.getElementById('tab-ciclo-grad').className = tab === 'grad'
        ? 'text-sm font-semibold px-4 py-1.5 rounded-lg transition bg-blue-500/15 text-blue-400 border border-blue-500/30'
        : 'text-sm font-semibold px-4 py-1.5 rounded-lg transition text-gray-500 hover:text-gray-300';
    document.getElementById('tab-ciclo-pos').className = tab === 'pos'
        ? 'text-sm font-semibold px-4 py-1.5 rounded-lg transition bg-blue-500/15 text-blue-400 border border-blue-500/30'
        : 'text-sm font-semibold px-4 py-1.5 rounded-lg transition text-gray-500 hover:text-gray-300';
    renderCiclos();
}

function renderCiclos() {
    const nivel = _cicloTab === 'grad' ? 'Graduação' : 'Pós-Graduação';
    const filtered = _cicloData.filter(c => c.nivel === nivel).sort((a,b) => a.dt_inicio.localeCompare(b.dt_inicio));
    const tbody = document.getElementById('ciclo-tbody');
    if (!filtered.length) {
        tbody.innerHTML = '<tr><td colspan="4" class="py-4 text-center text-gray-500">Nenhum ciclo encontrado</td></tr>';
        return;
    }
    tbody.innerHTML = filtered.map(c => `
        <tr class="border-b border-[var(--border)] hover:bg-white/[0.02]" data-ciclo-id="${c.id}">
            <td class="py-2.5 pr-4">
                <input type="text" value="${c.nome}" class="bg-transparent border-b border-transparent hover:border-gray-600 focus:border-indigo-500 outline-none text-gray-200 w-full ciclo-nome" />
            </td>
            <td class="py-2.5 pr-4">
                <input type="date" value="${c.dt_inicio}" class="input-glass px-2 py-1 text-sm text-gray-200 ciclo-inicio" />
            </td>
            <td class="py-2.5 pr-4">
                <input type="date" value="${c.dt_fim}" class="input-glass px-2 py-1 text-sm text-gray-200 ciclo-fim" />
            </td>
            <td class="py-2.5 flex gap-2">
                <button onclick="saveCiclo(${c.id})" title="Salvar" class="text-emerald-400 hover:text-emerald-300 transition">
                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/></svg>
                </button>
                <button onclick="deleteCiclo(${c.id})" title="Excluir" class="text-rose-400 hover:text-rose-300 transition">
                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"/></svg>
                </button>
            </td>
        </tr>
    `).join('');
}

async function saveCiclo(id) {
    const row = document.querySelector(`tr[data-ciclo-id="${id}"]`);
    if (!row) return;
    const nome = row.querySelector('.ciclo-nome').value.trim();
    const dt_inicio = row.querySelector('.ciclo-inicio').value;
    const dt_fim = row.querySelector('.ciclo-fim').value;
    const nivel = _cicloTab === 'grad' ? 'Graduação' : 'Pós-Graduação';
    try {
        await api(`/api/ciclos/${id}`, { method: 'PUT', headers: {'Content-Type':'application/json'}, body: JSON.stringify({ nivel, nome, dt_inicio, dt_fim }) });
        loadCiclos();
    } catch (e) { toast('Erro ao salvar: ' + e.message, 'error'); }
}

async function deleteCiclo(id) {
    if (!confirm('Excluir este ciclo?')) return;
    try {
        await api(`/api/ciclos/${id}`, { method: 'DELETE' });
        loadCiclos();
    } catch (e) { toast('Erro ao excluir: ' + e.message, 'error'); }
}

async function seedCiclos() {
    const ano = parseInt(document.getElementById('ciclo-ano').value) || 2026;
    try {
        const res = await api('/api/ciclos/seed', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({ ano }) });
        const d = await res.json();
        toast(`Ciclos gerados: ${d.created} novos para ${d.ano}`, 'success');
        loadCiclos();
    } catch (e) { toast('Erro: ' + e.message, 'error'); }
}

async function createCiclo() {
    const nivel = document.getElementById('ciclo-new-nivel').value;
    const nome = document.getElementById('ciclo-new-nome').value.trim();
    const dt_inicio = document.getElementById('ciclo-new-inicio').value;
    const dt_fim = document.getElementById('ciclo-new-fim').value;
    if (!nome || !dt_inicio || !dt_fim) { toast('Preencha todos os campos', 'warning'); return; }
    try {
        await api('/api/ciclos', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({ nivel, nome, dt_inicio, dt_fim }) });
        document.getElementById('ciclo-new-nome').value = '';
        document.getElementById('ciclo-new-inicio').value = '';
        document.getElementById('ciclo-new-fim').value = '';
        loadCiclos();
    } catch (e) { toast('Erro: ' + e.message, 'error'); }
}

// ---------------------------------------------------------------------------
// Schedule
// ---------------------------------------------------------------------------
const DAY_LABELS = { '*': 'Todos os dias', '0,1,2,3,4': 'Seg — Sex', '0': 'Seg', '1': 'Ter', '2': 'Qua', '3': 'Qui', '4': 'Sex', '5': 'Sáb', '6': 'Dom' };
const TYPE_LABELS = { sync_delta: 'Sync Delta', sync_full: 'Sync Full' };

async function loadSchedules() {
    try {
        const res = await api('/api/schedules');
        const data = await res.json();
        const container = document.getElementById('schedule-list');

        if (!data.schedules || !data.schedules.length) {
            container.innerHTML = '<p class="text-sm text-gray-500">Nenhum agendamento configurado.</p>';
            return;
        }

        container.innerHTML = `<div class="space-y-3">${data.schedules.map(s => {
            const typeColor = s.job_type === 'sync_full' ? 'bg-purple-500/15 text-purple-400 border border-purple-500/30' : 'bg-blue-500/15 text-blue-400 border border-blue-500/30';
            const enabledColor = s.enabled ? 'bg-emerald-500/15 text-emerald-400 border border-emerald-500/30' : 'bg-[var(--bg-card)] text-gray-500 border border-[var(--border)]';
            const days = DAY_LABELS[s.cron_days] || s.cron_days;
            const time = String(s.cron_hour).padStart(2, '0') + ':' + String(s.cron_minute).padStart(2, '0');

            return `<div class="flex items-center justify-between p-4 glass-card">
                <div class="flex items-center gap-4">
                    <div>
                        <span class="tag-pill ${typeColor}">${TYPE_LABELS[s.job_type] || s.job_type}</span>
                    </div>
                    <div>
                        <p class="text-sm text-gray-200 font-semibold">${days} às ${time}</p>
                        <p class="text-xs text-gray-500">
                            ${s.last_run_at ? 'Última execução: ' + fmtDate(s.last_run_at) : 'Nunca executado'}
                            ${s.next_run ? ' &middot; Próxima: ' + fmtDate(s.next_run) : ''}
                        </p>
                    </div>
                </div>
                <div class="flex items-center gap-2">
                    <span class="tag-pill ${enabledColor}">${s.enabled ? 'Ativo' : 'Inativo'}</span>
                    <button onclick="toggleSchedule('${esc(s.id)}')" class="text-xs px-3 py-1.5 rounded-lg btn-secondary text-gray-300">
                        ${s.enabled ? 'Desativar' : 'Ativar'}
                    </button>
                    <button onclick="deleteSchedule('${esc(s.id)}')" class="text-xs px-3 py-1.5 rounded-lg bg-red-500/10 hover:bg-red-500/20 text-red-400 border border-red-500/20 transition">
                        Excluir
                    </button>
                </div>
            </div>`;
        }).join('')}</div>`;
    } catch (err) {
        console.error(err);
    }
}

async function saveSchedule() {
    const payload = {
        job_type: document.getElementById('sched-type').value,
        cron_days: document.getElementById('sched-days').value,
        cron_hour: parseInt(document.getElementById('sched-hour').value),
        cron_minute: parseInt(document.getElementById('sched-minute').value),
        enabled: true,
    };

    try {
        const res = await api('/api/schedules', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
        });
        const data = await res.json();
        if (data.error) { toast(data.error, 'error'); return; }
        loadSchedules();
    } catch (err) {
        toast('Erro: ' + err.message, 'error');
    }
}

async function toggleSchedule(id) {
    try {
        await api(`/api/schedules/${id}/toggle`, { method: 'POST' });
        loadSchedules();
    } catch (err) {
        toast('Erro: ' + err.message, 'error');
    }
}

async function deleteSchedule(id) {
    if (!confirm('Excluir este agendamento?')) return;
    try {
        await api(`/api/schedules/${id}`, { method: 'DELETE' });
        loadSchedules();
    } catch (err) {
        toast('Erro: ' + err.message, 'error');
    }
}

// ---------------------------------------------------------------------------
// Usuários — CRUD
// ---------------------------------------------------------------------------
const PAGE_LABELS = {
    dashboard: 'Dashboard', search: 'Buscar', sync: 'Sync/Delta CRM Acadêmico',
    kommo_sync: 'Sync/Delta CRM Comercial',
    update: 'Upload Acadêmico', pipeline: 'Atualização CRM Acadêmico',
    match_merge: 'Upload Comercial', comercial_rgm: 'Dashboard Comercial',
    logs: 'Logs / Relatórios', distribuicao: 'Distribuição', ativacoes: 'Ativações Acadêmicas',
    intelligence: 'Inteligência', inadimplencia: 'Inadimplência',
    feedback: 'Feedback', config: 'Configurações', schedule: 'Agendamento',
    inscricao: 'Inscrição Automática',
    comparar_cursos: 'Comparar Cursos',
    recomendacao_cursos: 'Recomendação',
    localizacao_polos: 'Localização',
    info_cursos: 'Informações de Cursos',
    avisos: 'Avisos',
    kommo_dispatcher: 'Monitor de Conversas',
    minha_performance: 'Minha Performance',
    leads_parados: 'Leads Parados',
    premiacao_admin: 'Premiação (Admin)',
};

const PAGE_GROUPS_CONFIG = [
    { label: 'Geral', pages: ['dashboard', 'search', 'avisos'] },
    { label: 'Operação — Acadêmico', pages: ['ativacoes', 'distribuicao', 'intelligence', 'inadimplencia', 'feedback'] },
    { label: 'Ferramentas', pages: ['comparar_cursos', 'recomendacao_cursos', 'localizacao_polos', 'info_cursos'] },
    { label: 'Operação — Comercial', pages: ['comercial_rgm', 'inscricao', 'minha_performance', 'leads_parados'] },
    { label: 'Sistema', pages: ['kommo_dispatcher', 'logs', 'config', 'schedule', 'premiacao_admin'] },
    { label: 'Sistema — CRM', pages: ['pipeline', 'sync', 'kommo_sync', 'update', 'match_merge'] },
];
let _allPages = [];
let _usersData = [];

async function loadUsers() {
    try {
        const res = await api('/api/users');
        const d = await res.json();
        _allPages = d.all_pages || [];
        _usersData = d.users || [];
        renderUsers();
        renderNewUserPermsGrid();
    } catch (e) { console.error(e); }
}

function renderUsers() {
    const tbody = document.getElementById('users-tbody');
    if (!_usersData.length) {
        tbody.innerHTML = '<tr><td colspan="5" class="py-4 text-center text-gray-500">Nenhum usuário</td></tr>';
        return;
    }
    tbody.innerHTML = _usersData.map(u => {
        const roleLabel = u.role === 'admin'
            ? '<span class="tag-pill bg-indigo-500/20 text-indigo-400 border border-indigo-500/30">Admin</span>'
            : '<span class="tag-pill bg-gray-700/50 text-gray-400 border border-gray-600/30">Viewer</span>';
        const permsHtml = u.role === 'admin'
            ? '<span class="text-xs text-emerald-400">Acesso total</span>'
            : (u.pages || []).map(p => `<span class="inline-block text-[10px] bg-gray-100 dark:bg-gray-800/50 text-gray-400 px-1.5 py-0.5 rounded mr-1 mb-1">${PAGE_LABELS[p] || p}</span>`).join('');
        return `<tr class="border-b border-[var(--border)]">
            <td class="py-3 font-medium">${u.username}</td>
            <td class="py-3 text-xs">${u.email_cruzeiro || '<span class="text-gray-600">—</span>'}</td>
            <td class="py-3">${roleLabel}</td>
            <td class="py-3 max-w-xs">${permsHtml}</td>
            <td class="py-3 text-xs text-gray-500">${u.created_at || ''}</td>
            <td class="py-3">
                <div class="flex gap-2">
                    <button onclick="editUser(${u.id})" class="text-xs text-indigo-400 hover:text-indigo-300">Editar</button>
                    <button onclick="deleteUser(${u.id}, '${u.username}')" class="text-xs text-red-400 hover:text-red-300">Excluir</button>
                </div>
            </td>
        </tr>`;
    }).join('');
}

function _renderPermsGrouped(cbClass, checkedPages, disabled) {
    const groups = PAGE_GROUPS_CONFIG.map(g => {
        const groupPages = g.pages.filter(p => _allPages.includes(p));
        if (!groupPages.length) return '';
        const items = groupPages.map(p => {
            const ck = checkedPages.includes(p) ? 'checked' : '';
            const dis = disabled ? 'disabled' : '';
            return `<label class="flex items-center gap-2.5 py-1 px-2 rounded-lg hover:bg-white/5 cursor-pointer transition-colors text-[13px] text-gray-300 select-none">
                <input type="checkbox" value="${p}" class="${cbClass} accent-indigo-500 w-3.5 h-3.5 rounded flex-shrink-0" ${ck} ${dis}>
                <span class="truncate">${PAGE_LABELS[p] || p}</span>
            </label>`;
        }).join('');
        return `<div class="bg-slate-800/30 rounded-xl p-3 border border-slate-700/20">
            <p class="text-[10px] font-bold text-indigo-400/70 uppercase tracking-wider mb-2 px-1">${g.label}</p>
            <div class="space-y-0.5">${items}</div>
        </div>`;
    }).filter(Boolean);
    return groups.join('');
}

function renderNewUserPermsGrid() {
    const grid = document.getElementById('user-new-perms-grid');
    grid.innerHTML = _renderPermsGrouped('user-new-page-cb', _allPages, false);
}

function toggleNewUserPerms() {
    const role = document.getElementById('user-new-role').value;
    const permsDiv = document.getElementById('user-new-perms');
    permsDiv.style.display = role === 'admin' ? 'none' : '';
}

async function createUser() {
    const username = document.getElementById('user-new-username').value.trim();
    const password = document.getElementById('user-new-password').value;
    const role = document.getElementById('user-new-role').value;
    const kommoRaw = document.getElementById('user-new-kommo-uid').value.trim();
    const kommo_user_id = kommoRaw ? parseInt(kommoRaw) : null;
    const email_cruzeiro = (document.getElementById('user-new-email-cruzeiro').value || '').trim() || null;
    if (!username || !password) { toast('Usuário e senha são obrigatórios', 'warning'); return; }
    const cbs = document.querySelectorAll('.user-new-page-cb:checked');
    const pages = Array.from(cbs).map(cb => cb.value);
    try {
        const res = await api('/api/users', {
            method: 'POST', headers: {'Content-Type':'application/json'},
            body: JSON.stringify({ username, password, role, pages, kommo_user_id, email_cruzeiro }),
        });
        const d = await res.json();
        if (d.error) { toast(d.error, 'error'); return; }
        document.getElementById('user-new-username').value = '';
        document.getElementById('user-new-password').value = '';
        document.getElementById('user-new-email-cruzeiro').value = '';
        loadUsers();
    } catch (e) { toast('Erro: ' + e.message, 'error'); }
}

async function deleteUser(uid, name) {
    if (!confirm(`Excluir o usuário "${name}"?`)) return;
    try {
        const res = await api('/api/users/' + uid, { method: 'DELETE' });
        const d = await res.json();
        if (d.error) { toast(d.error, 'error'); return; }
        loadUsers();
    } catch (e) { toast('Erro: ' + e.message, 'error'); }
}

async function editUser(uid) {
    const u = _usersData.find(x => x.id === uid);
    if (!u) return;
    const userPages = u.role === 'admin' ? _allPages : (u.pages || []);
    const permsHtml = _renderPermsGrouped('edit-perm-cb', userPages, u.role === 'admin');

    const modal = document.createElement('div');
    modal.id = 'user-edit-modal';
    modal.className = 'fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm p-4';
    modal.onclick = e => { if (e.target === modal) modal.remove(); };
    modal.innerHTML = `
        <div class="glass-card w-full max-w-2xl max-h-[90vh] overflow-y-auto" style="background:rgba(15,23,42,0.97)" onclick="event.stopPropagation()">
            <div class="sticky top-0 z-10 px-6 py-4 border-b border-slate-700/30 bg-slate-900/95 backdrop-blur flex items-center justify-between">
                <h3 class="text-lg font-bold text-white font-display">Editar: ${u.username}</h3>
                <button onclick="document.getElementById('user-edit-modal').remove()" class="text-slate-500 hover:text-white transition-colors">
                    <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>
                </button>
            </div>
            <div class="p-6 space-y-5">
                <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
                    <div>
                        <label class="block text-xs text-gray-500 mb-1.5 font-medium">Nova Senha</label>
                        <input type="password" id="edit-user-pw" class="input-glass px-3 py-2 text-sm text-gray-200 w-full" autocomplete="new-password" placeholder="Vazio = manter">
                    </div>
                    <div>
                        <label class="block text-xs text-gray-500 mb-1.5 font-medium">Kommo User ID</label>
                        <input type="number" id="edit-user-kommo-uid" value="${u.kommo_user_id||''}" class="input-glass px-3 py-2 text-sm text-gray-200 w-full" placeholder="ID do Kommo">
                    </div>
                    <div>
                        <label class="block text-xs text-gray-500 mb-1.5 font-medium">E-mail Cruzeiro</label>
                        <input type="email" id="edit-user-email-cruzeiro" value="${u.email_cruzeiro||''}" class="input-glass px-3 py-2 text-sm text-gray-200 w-full" placeholder="nome@cruzeirodosul.edu.br">
                    </div>
                    <div>
                        <label class="block text-xs text-gray-500 mb-1.5 font-medium">Nível</label>
                        <select id="edit-user-role" class="input-glass px-3 py-2 text-sm text-gray-200 w-full"
                            onchange="document.querySelectorAll('.edit-perm-cb').forEach(cb=>{cb.disabled=this.value==='admin';if(this.value==='admin')cb.checked=true});document.getElementById('edit-perms-section').style.display=this.value==='admin'?'none':''">
                            <option value="viewer" ${u.role==='viewer'?'selected':''}>Visualizador</option>
                            <option value="admin" ${u.role==='admin'?'selected':''}>Administrador</option>
                        </select>
                    </div>
                </div>
                <div id="edit-perms-section" ${u.role==='admin'?'style="display:none"':''}>
                    <label class="block text-xs text-gray-500 mb-3 font-medium">Permissões por página</label>
                    <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">${permsHtml}</div>
                </div>
                <div class="flex gap-3 pt-2 border-t border-slate-700/20">
                    <button onclick="saveUserEdit(${uid})" class="btn-primary text-white text-sm px-6 py-2.5 rounded-xl font-medium">Salvar Alterações</button>
                    <button onclick="document.getElementById('user-edit-modal').remove()" class="btn-secondary text-sm px-5 py-2.5 rounded-xl">Cancelar</button>
                </div>
            </div>
        </div>`;
    document.body.appendChild(modal);
}

async function saveUserEdit(uid) {
    const pw = document.getElementById('edit-user-pw').value;
    const role = document.getElementById('edit-user-role').value;
    const kommoRaw = document.getElementById('edit-user-kommo-uid').value.trim();
    const emailCruzeiro = (document.getElementById('edit-user-email-cruzeiro').value || '').trim();
    const cbs = document.querySelectorAll('.edit-perm-cb:checked');
    const pages = Array.from(cbs).map(cb => cb.value);
    const body = { role, pages, kommo_user_id: kommoRaw ? parseInt(kommoRaw) : null, email_cruzeiro: emailCruzeiro || null };
    if (pw) body.password = pw;
    try {
        const res = await api('/api/users/' + uid, {
            method: 'PUT', headers: {'Content-Type':'application/json'},
            body: JSON.stringify(body),
        });
        const d = await res.json();
        if (d.error) { toast(d.error, 'error'); return; }
        document.getElementById('user-edit-modal').remove();
        loadUsers();
    } catch (e) { toast('Erro: ' + e.message, 'error'); }
}

async function importKommoUsers() {
    const msg = document.getElementById('import-kommo-msg');
    if (msg) msg.textContent = 'Importando...';
    try {
        const res = await api('/api/users/import-kommo', {
            method: 'POST', headers: {'Content-Type':'application/json'},
        });
        const d = await res.json();
        if (d.ok) {
            toast(d.summary);
            if (msg) msg.textContent = d.summary;
            loadUsers();
        } else {
            toast(d.error || 'Erro', 'error');
            if (msg) msg.textContent = d.error || 'Erro';
        }
    } catch (e) {
        toast('Erro: ' + e.message, 'error');
        if (msg) msg.textContent = 'Erro de conexão';
    }
}
