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
    const activeClass = dsSegActive('flex items-center gap-2');
    const inactiveClass = dsSegInactive('flex items-center gap-2');
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
        ? dsSegActive()
        : dsSegInactive();
    document.getElementById('tab-turma-pos').className = tab === 'pos'
        ? dsSegActive()
        : dsSegInactive();
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
        ? dsSegActive()
        : dsSegInactive();
    document.getElementById('tab-ciclo-pos').className = tab === 'pos'
        ? dsSegActive()
        : dsSegInactive();
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
    match_merge: 'Upload Comercial', comercial_rgm: 'Comercial RGM',
    logs: 'Logs / Relatórios', distribuicao: 'Distribuição',
    ativacoes: 'Ativações Acadêmicas',
    intelligence: 'Inteligência', inadimplencia: 'Inadimplência',
    feedback: 'Feedback', config: 'Configurações', schedule: 'Agendamento',
    inscricao: 'Inscrições',
    comparar_cursos: 'Comparar Cursos',
    recomendacao_cursos: 'Recomendação',
    localizacao_polos: 'Localização',
    info_cursos: 'Informações de Cursos',
    leads_inscricao: 'Leads em Inscrição Automática',
    avisos: 'Avisos',
    kommo_dispatcher: 'Monitor de Conversas',
    minha_performance: 'Minha Performance',
    leads_parados: 'Leads Parados',
    premiacao_admin: 'Premiação (Admin)',
    ajustes_matricula: 'Ajustes de Matrícula',
    macro_email: 'Macro Email',
    repasse: 'Repasse',
    dist_consultor: 'Distribuição Consultor',
    // Acrescentadas
    recadastros: 'Recadastros',
    vocacional: 'Dashboard Vocacional',
    comercial_dashboard: 'Dashboard Atendimentos',
    auditoria_comercial: 'Feedback Comercial',
    'meta-campaigns': 'Campaign Performance',
    dist_comercial: 'Distribuição Comercial',
    atualizar_preco: 'Atualizar Preço',
    captacao: 'Captação Externa',
    clicks: 'QR Codes',
    leads_promotores: 'Leads · Promotores',
    meus_atendimentos: 'Meus Atendimentos',
};

// ---------------------------------------------------------------------------
// Presets de permissão por categoria
// Aplicados quando o admin clica em "Aplicar preset" ao lado do select de
// Categoria no formulário de Novo Usuário ou no modal de Editar Usuário.
// Cada preset substitui completamente as permissões marcadas.
// ---------------------------------------------------------------------------
const _PRESET_FERRAMENTAS_FULL  = ['comparar_cursos', 'recomendacao_cursos', 'localizacao_polos', 'info_cursos', 'leads_inscricao'];
const _PRESET_FERRAMENTAS_BASIC = ['comparar_cursos', 'recomendacao_cursos', 'localizacao_polos', 'info_cursos']; // sem Leads em Inscrição

const CATEGORY_PRESETS = {
    'Comercial': [
        ..._PRESET_FERRAMENTAS_FULL,
        'minha_performance', 'repasse', 'dist_consultor', 'search', 'avisos',
    ],
    'Acadêmico': [
        ..._PRESET_FERRAMENTAS_BASIC,
        'meus_atendimentos',
        'search', 'avisos',
    ],
    'Suporte Comercial': [
        ..._PRESET_FERRAMENTAS_BASIC,
        'minha_performance', 'premiacao_admin',
        'search', 'avisos',
    ],
    // Geral (Dashboard, Buscar, Avisos) + Ferramentas completas + Comercial
    // (todas exceto Atualizar Preço). Sem Acadêmico, sem Sistema.
    'Supervisor Comercial': [
        'dashboard', 'search', 'avisos',
        ..._PRESET_FERRAMENTAS_FULL,
        'dist_consultor', 'comercial_rgm', 'dist_comercial', 'inscricao',
        'recadastros', 'comercial_dashboard', 'auditoria_comercial',
        'leads_parados', 'minha_performance', 'repasse',
    ],
    // Geral (Dashboard, Buscar, Avisos) + Acadêmico completo + Ferramentas
    // exceto Leads em Inscrição. Sem Comercial, sem Sistema.
    'Supervisor Acadêmico': [
        'dashboard', 'search', 'avisos',
        'ativacoes', 'distribuicao', 'intelligence', 'inadimplencia',
        'feedback', 'macro_email', 'meus_atendimentos',
        ..._PRESET_FERRAMENTAS_BASIC,
    ],
};

function applyCategoryPreset(cbClass, categoria) {
    const pages = CATEGORY_PRESETS[categoria];
    if (!pages) {
        toast('Sem preset definido para "' + (categoria || '—') + '"', 'warning');
        return;
    }
    const set = new Set(pages);
    let touched = 0;
    document.querySelectorAll('.' + cbClass + ':not(:disabled)').forEach(cb => {
        const before = cb.checked;
        cb.checked = set.has(cb.value);
        if (before !== cb.checked) touched++;
    });
    toast(`Preset "${categoria}" aplicado (${pages.length} páginas marcadas).`, 'success');
}

// Lê a categoria atualmente selecionada (Novo Usuário ou Editar Usuário) e
// aplica o preset correspondente nas checkboxes do `cbClass`.
function applyCategoryPresetFromSelect(selectId, cbClass) {
    const sel = document.getElementById(selectId);
    if (!sel) return;
    const categoria = sel.value || '';
    if (!categoria) {
        toast('Selecione uma categoria antes de aplicar o preset.', 'warning');
        return;
    }
    applyCategoryPreset(cbClass, categoria);
}

// Espelho dos grupos do sidebar (templates/partials/_sidebar.html).
// Mantenha em sincronia com o sidebar caso reorganize a navegação.
const PAGE_GROUPS_CONFIG = [
    {
        label: 'Geral',
        icon: 'dashboard',
        color: 'var(--primary)',
        pages: ['dashboard', 'search', 'avisos'],
    },
    {
        label: 'Acadêmico',
        section: 'Operação',
        icon: 'school',
        color: 'var(--primary)',
        pages: [
            'meus_atendimentos', 'ativacoes', 'distribuicao', 'intelligence',
            'inadimplencia', 'feedback', 'macro_email',
        ],
    },
    {
        label: 'Ferramentas',
        section: 'Operação',
        icon: 'lightbulb',
        color: 'var(--primary)',
        pages: ['comparar_cursos', 'recomendacao_cursos', 'localizacao_polos', 'info_cursos', 'leads_inscricao'],
    },
    {
        label: 'Comercial',
        section: 'Operação',
        icon: 'trending_up',
        color: 'var(--secondary)',
        pages: [
            'dist_consultor', 'comercial_rgm', 'premiacao_admin', 'dist_comercial', 'inscricao',
            'recadastros', 'comercial_dashboard', 'auditoria_comercial',
            'atualizar_preco', 'leads_parados', 'minha_performance', 'repasse',
            'captacao', 'clicks', 'leads_promotores',
        ],
    },
    {
        label: 'Vocacional',
        section: 'Operação',
        icon: 'psychology',
        color: 'var(--primary-medium)',
        pages: ['vocacional'],
        // Sub-abas dentro da pagina Vocacional — compartilham a permissao "vocacional"
        // (sem checkbox individual; sao apenas informativas).
        subItems: [
            { icon: 'filter_alt', label: 'Visão Geral do Funil' },
            { icon: 'ads_click', label: 'Análise de Tráfego' },
            { icon: 'quiz', label: 'Comportamento Quiz' },
            { icon: 'person_add', label: 'Conversão de Leads' },
            { icon: 'school', label: 'Interesse em Cursos' },
            { icon: 'swap_horiz', label: 'Funil por Canal' },
            { icon: 'phone_in_talk', label: 'Status Comercial' },
        ],
    },
    {
        label: 'Meta · Campanhas',
        section: 'Operação',
        icon: 'pie_chart',
        color: 'var(--primary)',
        pages: ['meta-campaigns'],
    },
    {
        label: 'Sistema',
        section: 'Sistema',
        icon: 'settings',
        color: 'var(--outline)',
        pages: [
            'config', 'schedule', 'kommo_dispatcher', 'logs',
            'ajustes_matricula',
        ],
    },
    {
        label: 'Sistema — CRM',
        section: 'Sistema',
        icon: 'sync_alt',
        color: 'var(--outline)',
        pages: ['pipeline', 'update', 'sync', 'kommo_sync', 'match_merge'],
    },
];
let _allPages = [];
let _usersData = [];

function filterUsersTable() {
    const q = (document.getElementById('cfg-users-search').value || '').toLowerCase().trim();
    const cat = document.getElementById('cfg-users-cat-filter').value;
    const rows = document.querySelectorAll('#users-tbody tr[data-user-row]');
    rows.forEach(row => {
        const name = (row.dataset.userName || '').toLowerCase();
        const rowCat = row.dataset.userCat || '';
        let show = true;
        if (q && !name.includes(q)) show = false;
        if (cat === '__none__' && rowCat !== '') show = false;
        else if (cat && cat !== '__none__' && rowCat !== cat) show = false;
        row.style.display = show ? '' : 'none';
    });
}

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
        tbody.innerHTML = '<tr><td colspan="6" class="py-4 text-center text-[var(--text-muted)]">Nenhum usuário</td></tr>';
        return;
    }
    const _catClass = {
        'Comercial': 'tag-cat-comercial',
        'Suporte Comercial': 'tag-cat-suporte',
        'Acadêmico': 'tag-cat-academico',
        'Supervisor Comercial': 'tag-cat-supervisor-comercial',
        'Supervisor Acadêmico': 'tag-cat-supervisor-academico',
    };
    const _roleTag = {
        admin:  '<span class="tag-pill tag-role-admin">Admin</span>',
        editor: '<span class="tag-pill tag-role-editor">Editor</span>',
        viewer: '<span class="tag-pill tag-role-viewer">Viewer</span>',
    };
    tbody.innerHTML = _usersData.map(u => {
        const roleLabel = _roleTag[u.role] || _roleTag.viewer;
        const catLabel = u.categoria
            ? `<span class="tag-pill text-[10px] ${_catClass[u.categoria] || 'tag-cat-fallback'}">${u.categoria}</span>`
            : '<span class="text-xs" style="color: var(--text-muted)">—</span>';
        const permsHtml = u.role === 'admin'
            ? '<span class="tag-page-all">Acesso total</span>'
            : `<div class="flex flex-wrap gap-1">${(u.pages || []).map(p => `<span class="tag-page">${PAGE_LABELS[p] || p}</span>`).join('')}</div>`;
        return `<tr class="border-b border-[var(--border)] align-top" data-user-row data-user-name="${(u.username||'').toLowerCase()}" data-user-cat="${u.categoria||''}">
            <td class="py-2.5 pr-3 font-medium text-[13px] whitespace-nowrap" style="color: var(--text-primary)">${u.username}</td>
            <td class="py-2.5 pr-3 whitespace-nowrap">${catLabel}</td>
            <td class="py-2.5 pr-3 whitespace-nowrap">${roleLabel}</td>
            <td class="py-2.5 pr-3" style="max-width:320px">${permsHtml}</td>
            <td class="py-2.5 pr-3 text-xs whitespace-nowrap" style="color: var(--text-muted)">${u.created_at || ''}</td>
            <td class="py-2.5">
                <div class="flex gap-2 whitespace-nowrap">
                    <button onclick="editUser(${u.id})" class="text-xs font-semibold transition-colors" style="color: var(--primary)">Editar</button>
                    <button onclick="deleteUser(${u.id}, '${u.username}')" class="text-xs font-semibold text-red-600 hover:text-red-500 dark:text-red-400 dark:hover:text-red-300 transition-colors">Excluir</button>
                </div>
            </td>
        </tr>`;
    }).join('');
}

// ---------------------------------------------------------------------------
// Helpers de seleção de permissões (toolbar global + por grupo)
// ---------------------------------------------------------------------------
function permSelectAll(cbClass) {
    document.querySelectorAll('.' + cbClass + ':not(:disabled)').forEach(cb => { cb.checked = true; });
}
function permClearAll(cbClass) {
    document.querySelectorAll('.' + cbClass + ':not(:disabled)').forEach(cb => { cb.checked = false; });
}
function togglePermGroup(cbClass, groupSlug) {
    const cbs = document.querySelectorAll(
        `.${cbClass}[data-perm-group="${groupSlug}"]:not(:disabled)`
    );
    if (!cbs.length) return;
    const allChecked = Array.from(cbs).every(cb => cb.checked);
    cbs.forEach(cb => { cb.checked = !allChecked; });
}
function _renderPermsToolbar(cbClass) {
    return `<div class="perm-toolbar">
        <button type="button" class="perm-toolbar-btn primary"
                onclick="permSelectAll('${cbClass}')">
            <span class="material-symbols-outlined text-[15px]">done_all</span>
            Marcar tudo
        </button>
        <button type="button" class="perm-toolbar-btn"
                onclick="permClearAll('${cbClass}')">
            <span class="material-symbols-outlined text-[15px]">remove_done</span>
            Limpar seleção
        </button>
    </div>`;
}

function _renderPermsGrouped(cbClass, checkedPages, disabled) {
    const slugify = (s) => String(s)
        .toLowerCase()
        .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
        .replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '') || 'grp';
    const renderGroup = (g) => {
        const groupPages = g.pages.filter(p => _allPages.includes(p));
        if (!groupPages.length) return '';
        const groupSlug = slugify(g.label);
        const items = groupPages.map(p => {
            const ck = checkedPages.includes(p) ? 'checked' : '';
            const dis = disabled ? 'disabled' : '';
            return `<label class="perm-item flex items-center gap-2.5 py-1 px-2 rounded-lg cursor-pointer transition-colors text-[13px] select-none">
                <input type="checkbox" value="${p}" data-perm-group="${groupSlug}" class="${cbClass} accent-indigo-500 w-3.5 h-3.5 rounded flex-shrink-0" ${ck} ${dis}>
                <span class="truncate">${PAGE_LABELS[p] || p}</span>
            </label>`;
        }).join('');
        // Sub-abas decorativas (compartilham a permissao do checkbox acima).
        const subItemsHtml = (g.subItems && g.subItems.length)
            ? `<div class="perm-subitems">
                <p class="perm-subitems-label">Inclui</p>
                ${g.subItems.map(s => `<div class="perm-subitem">
                    <span class="material-symbols-outlined text-[13px]">${s.icon || 'subdirectory_arrow_right'}</span>
                    <span class="truncate">${s.label}</span>
                </div>`).join('')}
            </div>`
            : '';
        const iconHtml = g.icon
            ? `<span class="material-symbols-outlined text-[14px] perm-group-icon" style="color: ${g.color || 'var(--primary)'}">${g.icon}</span>`
            : '';
        const toggleBtn = disabled
            ? ''
            : `<button type="button" class="perm-group-toggle"
                       title="Marcar/Desmarcar todos deste grupo"
                       onclick="togglePermGroup('${cbClass}', '${groupSlug}')">
                  <span class="material-symbols-outlined text-[15px]">done_all</span>
               </button>`;
        return `<div class="perm-group rounded-xl p-3 border" data-perm-group-card="${groupSlug}">
            <div class="flex items-center gap-1.5 mb-2 px-1">
                ${iconHtml}
                <p class="perm-group-title text-[10px] font-bold uppercase tracking-wider flex-1">${g.label}</p>
                ${toggleBtn}
            </div>
            <div class="space-y-0.5">${items}</div>
            ${subItemsHtml}
        </div>`;
    };

    // Agrupa por section ("Operação", "Sistema") preservando a ordem do array.
    // Itens sem section (ex.: Geral) viram um bloco solto.
    const sections = [];
    const sectionMap = new Map();
    for (const g of PAGE_GROUPS_CONFIG) {
        const key = g.section || '__top';
        if (!sectionMap.has(key)) {
            sectionMap.set(key, []);
            sections.push({ key, label: g.section || null });
        }
        sectionMap.get(key).push(g);
    }

    const blocks = sections.map(s => {
        const groupsHtml = sectionMap.get(s.key).map(renderGroup).filter(Boolean).join('');
        if (!groupsHtml) return '';
        const header = s.label
            ? `<p class="perm-section-label">— ${s.label} —</p>`
            : '';
        return `<div class="perm-section">${header}
            <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">${groupsHtml}</div>
        </div>`;
    }).filter(Boolean);

    return blocks.join('');
}

function renderNewUserPermsGrid() {
    const grid = document.getElementById('user-new-perms-grid');
    const toolbar = _renderPermsToolbar('user-new-page-cb');
    grid.innerHTML = toolbar + _renderPermsGrouped('user-new-page-cb', _allPages, false);
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
    const categoria = document.getElementById('user-new-categoria').value || null;
    if (!username || !password) { toast('Usuário e senha são obrigatórios', 'warning'); return; }
    const cbs = document.querySelectorAll('.user-new-page-cb:checked');
    const pages = Array.from(cbs).map(cb => cb.value);
    try {
        const res = await api('/api/users', {
            method: 'POST', headers: {'Content-Type':'application/json'},
            body: JSON.stringify({ username, password, role, pages, kommo_user_id, email_cruzeiro, categoria }),
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
    const isAdmin = u.role === 'admin';
    const permsToolbar = isAdmin ? '' : _renderPermsToolbar('edit-perm-cb');
    const permsHtml = _renderPermsGrouped('edit-perm-cb', userPages, isAdmin);

    const modal = document.createElement('div');
    modal.id = 'user-edit-modal';
    modal.className = 'fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm p-4';
    modal.onclick = e => { if (e.target === modal) modal.remove(); };
    modal.innerHTML = `
        <div class="glass-card w-full max-w-2xl max-h-[90vh] overflow-y-auto p-0" onclick="event.stopPropagation()">
            <div class="sticky top-0 z-10 px-6 py-4 border-b backdrop-blur flex items-center justify-between"
                 style="border-color: var(--border); background: var(--bg-overlay);">
                <h3 class="text-lg font-bold font-display" style="color: var(--text-primary);">Editar: ${u.username}</h3>
                <button onclick="document.getElementById('user-edit-modal').remove()"
                        class="transition-colors"
                        style="color: var(--text-muted);"
                        onmouseover="this.style.color='var(--text-primary)'"
                        onmouseout="this.style.color='var(--text-muted)'">
                    <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>
                </button>
            </div>
            <div class="p-6 space-y-5">
                <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
                    <div>
                        <label class="block text-xs mb-1.5 font-medium" style="color: var(--text-secondary);">Nova Senha</label>
                        <input type="password" id="edit-user-pw" class="input-glass px-3 py-2 text-sm w-full" autocomplete="new-password" placeholder="Vazio = manter">
                    </div>
                    <div>
                        <label class="block text-xs mb-1.5 font-medium" style="color: var(--text-secondary);">Kommo User ID</label>
                        <input type="number" id="edit-user-kommo-uid" value="${u.kommo_user_id||''}" class="input-glass px-3 py-2 text-sm w-full" placeholder="ID do Kommo">
                    </div>
                    <div>
                        <label class="block text-xs mb-1.5 font-medium" style="color: var(--text-secondary);">E-mail Cruzeiro</label>
                        <input type="email" id="edit-user-email-cruzeiro" value="${u.email_cruzeiro||''}" class="input-glass px-3 py-2 text-sm w-full" placeholder="nome@cruzeirodosul.edu.br">
                    </div>
                    <div>
                        <label class="block text-xs mb-1.5 font-medium" style="color: var(--text-secondary);">Categoria</label>
                        <div class="flex items-stretch gap-2">
                            <select id="edit-user-categoria" class="input-glass px-3 py-2 text-sm flex-1 min-w-0">
                                <option value="">— Nenhuma —</option>
                                <option value="Comercial" ${u.categoria==='Comercial'?'selected':''}>Comercial</option>
                                <option value="Suporte Comercial" ${u.categoria==='Suporte Comercial'?'selected':''}>Suporte Comercial</option>
                                <option value="Supervisor Comercial" ${u.categoria==='Supervisor Comercial'?'selected':''}>Supervisor Comercial</option>
                                <option value="Acadêmico" ${u.categoria==='Acadêmico'?'selected':''}>Acadêmico</option>
                                <option value="Supervisor Acadêmico" ${u.categoria==='Supervisor Acadêmico'?'selected':''}>Supervisor Acadêmico</option>
                            </select>
                            <button type="button"
                                    onclick="applyCategoryPresetFromSelect('edit-user-categoria', 'edit-perm-cb')"
                                    class="btn-secondary text-[11px] font-bold uppercase tracking-wider px-3 rounded-xl flex items-center gap-1 whitespace-nowrap"
                                    title="Aplicar preset de permissões da categoria">
                                <span class="material-symbols-outlined text-[14px]">auto_fix_high</span>
                                Preset
                            </button>
                        </div>
                    </div>
                    <div>
                        <label class="block text-xs mb-1.5 font-medium" style="color: var(--text-secondary);">Nível</label>
                        <select id="edit-user-role" class="input-glass px-3 py-2 text-sm w-full"
                            onchange="document.querySelectorAll('.edit-perm-cb').forEach(cb=>{cb.disabled=this.value==='admin';if(this.value==='admin')cb.checked=true});document.getElementById('edit-perms-section').style.display=this.value==='admin'?'none':''">
                            <option value="viewer" ${u.role==='viewer'?'selected':''}>Visualizador</option>
                            <option value="editor" ${u.role==='editor'?'selected':''}>Editor</option>
                            <option value="admin" ${u.role==='admin'?'selected':''}>Administrador</option>
                        </select>
                    </div>
                </div>
                <div id="edit-perms-section" ${isAdmin?'style="display:none"':''}>
                    <label class="block text-xs mb-3 font-medium" style="color: var(--text-secondary);">Permissões por página</label>
                    ${permsToolbar}
                    <div class="perm-grid">${permsHtml}</div>
                </div>
                <div class="flex gap-3 pt-4 border-t" style="border-color: var(--border);">
                    <button onclick="saveUserEdit(${uid})" class="btn-primary text-sm px-6 py-2.5 rounded-xl font-medium">Salvar Alterações</button>
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
    const categoria = document.getElementById('edit-user-categoria').value || null;
    const cbs = document.querySelectorAll('.edit-perm-cb:checked');
    const pages = Array.from(cbs).map(cb => cb.value);
    const body = { role, pages, kommo_user_id: kommoRaw ? parseInt(kommoRaw) : null, email_cruzeiro: emailCruzeiro || null, categoria };
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
    if (msg) msg.textContent = 'Importando do Kommo...';
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

async function importDataCrazyUsers() {
    const msg = document.getElementById('import-kommo-msg');
    if (msg) msg.textContent = 'Importando do DataCrazy...';
    try {
        const res = await api('/api/users/import-datacrazy', {
            method: 'POST', headers: {'Content-Type':'application/json'},
        });
        const d = await res.json();
        console.log('DataCrazy import response:', JSON.stringify(d, null, 2));
        if (d.ok) {
            let detail = d.summary;
            if (d.version) detail += ` [${d.version}]`;
            toast(detail);
            if (msg) msg.textContent = detail;
            if (d.skipped?.length) {
                console.table(d.skipped);
            }
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
