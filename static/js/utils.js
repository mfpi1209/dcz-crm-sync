// ---------------------------------------------------------------------------
// API helper
// ---------------------------------------------------------------------------
async function api(url, opts = {}) {
    const res = await fetch(url, opts);
    if (res.status === 401) {
        window.location.href = '/login';
        throw new Error('Sessão expirada');
    }
    return res;
}

// ---------------------------------------------------------------------------
// SPA Navigation
// ---------------------------------------------------------------------------
const PAGES = ['dashboard', 'search', 'sync', 'kommo_sync', 'update', 'pipeline', 'match_merge', 'distribuicao', 'ativacoes', 'intelligence', 'inadimplencia', 'feedback', 'comparar_cursos', 'recomendacao_cursos', 'localizacao_polos', 'info_cursos', 'logs', 'config', 'schedule', 'inscricao'];
const PAGE_TITLES = { dashboard: 'Dashboard', search: 'Buscar', sync: 'Sincronização', kommo_sync: 'Sync Comercial', update: 'Atualização CRM', pipeline: 'Saneamento / Pipeline', match_merge: 'Match & Merge', distribuicao: 'Distribuição', ativacoes: 'Ativações Acadêmicas', intelligence: 'Inteligência', inadimplencia: 'Inadimplência', feedback: 'Feedback', comparar_cursos: 'Comparar Cursos', recomendacao_cursos: 'Recomendação', localizacao_polos: 'Localização', info_cursos: 'Informações de Cursos', logs: 'Logs / Relatórios', config: 'Configurações', schedule: 'Agendamento', inscricao: 'Inscrição Automática' };

function navigate(page) {
    PAGES.forEach(p => {
        document.getElementById('page-' + p).classList.toggle('hidden', p !== page);
    });
    document.querySelectorAll('.sidebar-link').forEach(el => {
        el.classList.toggle('active', el.dataset.page === page);
    });
    document.getElementById('mobile-title').textContent = PAGE_TITLES[page] || page;

    // Close mobile sidebar
    document.getElementById('sidebar').classList.remove('open');
    document.getElementById('sidebar-overlay').classList.remove('open');

    // Load page data
    if (page === 'dashboard') loadDashboard();
    if (page === 'search') loadXlSnapshots();
    if (page === 'sync') loadSyncState();
    if (page === 'update') loadFileInfo();
    if (page === 'logs') { loadLogFiles(); loadDashboard(); }
    if (page === 'config') { loadCiclos(); loadTurmas(); }
    if (page === 'distribuicao') loadDistribuicao();
    if (page === 'ativacoes') loadAtivacoes();
    if (page === 'intelligence') loadIntelligence();
    if (page === 'inadimplencia') loadInadimplencia();
    if (page === 'kommo_sync') loadKommoSync();
    if (page === 'match_merge') loadMatchMerge();
    if (page === 'feedback') fbInit();
    if (page === 'inscricao') loadInscricao();
    if (page === 'schedule') loadSchedules();
    if (FERRAMENTA_MAP && FERRAMENTA_MAP[page]) loadFerramenta(page);

    history.replaceState(null, '', '#' + page);
}

window.addEventListener('hashchange', () => {
    const hash = location.hash.replace('#', '') || 'dashboard';
    if (PAGES.includes(hash)) navigate(hash);
});

function toggleSidebar() {
    document.getElementById('sidebar').classList.toggle('open');
    document.getElementById('sidebar-overlay').classList.toggle('open');
}

function toggleSidebarGroup(name) {
    const group = document.querySelector(`.sidebar-group[data-group="${name}"]`);
    if (group) group.classList.toggle('collapsed');
    localStorage.setItem('sb-' + name, group.classList.contains('collapsed') ? '0' : '1');
}

(function restoreSidebarGroups() {
    document.querySelectorAll('.sidebar-group[data-group]').forEach(g => {
        const key = 'sb-' + g.dataset.group;
        if (localStorage.getItem(key) === '0') g.classList.add('collapsed');
    });
})();

// ---------------------------------------------------------------------------
// Utils
// ---------------------------------------------------------------------------
function fmtDate(val) {
    if (!val) return null;
    if (/^\d{2}\/\d{2}\/\d{4}/.test(val)) return val;
    try {
        const d = new Date(val);
        if (isNaN(d)) return val;
        return d.toLocaleDateString('pt-BR') + ' ' + d.toLocaleTimeString('pt-BR', {hour:'2-digit', minute:'2-digit'});
    } catch { return val; }
}

function esc(s) {
    const el = document.createElement('span');
    el.textContent = s;
    return el.innerHTML;
}

function field(label, value) {
    return `<div><span class="text-slate-500">${esc(label)}</span><br><span class="text-slate-200">${value ? esc(String(value)) : '<span class=text-slate-600>—</span>'}</span></div>`;
}

// ---------------------------------------------------------------------------
// Global badge
// ---------------------------------------------------------------------------
let _syncRunningFlag = false;
let _updateRunningFlag = false;

function refreshBadge() {
    const badge = document.getElementById('global-badge');
    if (_syncRunningFlag) {
        badge.innerHTML = '<span class="inline-block w-2 h-2 rounded-full bg-indigo-400 animate-pulse"></span> Sincronizando...';
        badge.className = 'text-xs px-2.5 py-1 rounded-full bg-indigo-900/50 text-indigo-300 flex items-center gap-1.5 animate-pulse';
    } else if (_updateRunningFlag) {
        badge.innerHTML = '<span class="inline-block w-2 h-2 rounded-full bg-amber-400 animate-pulse"></span> Atualizando...';
        badge.className = 'text-xs px-2.5 py-1 rounded-full bg-amber-900/50 text-amber-300 flex items-center gap-1.5 animate-pulse';
    } else {
        badge.innerHTML = '<span class="green-dot"></span> Conectado';
        badge.className = 'text-xs px-2.5 py-1 rounded-full bg-emerald-900/40 text-emerald-400 flex items-center gap-1.5';
    }
}

// ---------------------------------------------------------------------------
// Sidebar — permissões dinâmicas
// ---------------------------------------------------------------------------
const SIDEBAR_GROUPS = {
    academico: ['ativacoes', 'distribuicao', 'intelligence', 'inadimplencia', 'feedback'],
    ferramentas: ['comparar_cursos', 'recomendacao_cursos', 'localizacao_polos', 'info_cursos'],
    comercial: ['pipeline', 'update', 'match_merge', 'inscricao'],
};

async function applySidebarPermissions() {
    try {
        const res = await api('/api/me');
        const d = await res.json();
        const pages = d.pages || [];
        const role = d.role || '';

        document.querySelectorAll('#sidebar .sidebar-link[data-page]').forEach(link => {
            const page = link.getAttribute('data-page');
            if (role === 'admin' || pages.includes(page)) {
                link.style.display = '';
            } else {
                link.style.display = 'none';
            }
        });

        Object.entries(SIDEBAR_GROUPS).forEach(([group, groupPages]) => {
            const el = document.querySelector(`.sidebar-group[data-group="${group}"]`);
            if (!el) return;
            const hasAny = role === 'admin' || groupPages.some(p => pages.includes(p));
            el.style.display = hasAny ? '' : 'none';
        });

        const operacaoLabel = document.getElementById('sidebar-section-operacao');
        if (operacaoLabel) {
            const anyOp = role === 'admin' || [...SIDEBAR_GROUPS.academico, ...SIDEBAR_GROUPS.comercial].some(p => pages.includes(p));
            operacaoLabel.style.display = anyOp ? '' : 'none';
        }
        const sistemaLabel = document.getElementById('sidebar-section-sistema');
        if (sistemaLabel) {
            const sysPages = ['sync', 'kommo_sync', 'logs', 'config', 'schedule'];
            const anySys = role === 'admin' || sysPages.some(p => pages.includes(p));
            sistemaLabel.style.display = anySys ? '' : 'none';
        }

        const cfgTab = document.getElementById('cfg-tab-usuarios');
        if (cfgTab) cfgTab.style.display = role === 'admin' ? '' : 'none';
    } catch (e) { console.error('sidebar permissions', e); }
}

applySidebarPermissions();

// ---------------------------------------------------------------------------
// Theme toggle
// ---------------------------------------------------------------------------
function toggleTheme() {
    const html = document.documentElement;
    const isDark = html.classList.contains('dark');
    const newTheme = isDark ? 'light' : 'dark';
    html.classList.remove('dark', 'light');
    html.classList.add(newTheme);
    localStorage.setItem('eduit-theme', newTheme);
    updateThemeUI(newTheme);
}

function updateThemeUI(theme) {
    const sunIcon = document.getElementById('theme-icon-sun');
    const moonIcon = document.getElementById('theme-icon-moon');
    const label = document.getElementById('theme-label');
    if (theme === 'dark') {
        sunIcon.classList.add('hidden');
        moonIcon.classList.remove('hidden');
        label.textContent = 'Modo claro';
    } else {
        sunIcon.classList.remove('hidden');
        moonIcon.classList.add('hidden');
        label.textContent = 'Modo escuro';
    }
}

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------
const currentTheme = localStorage.getItem('eduit-theme') || 'dark';
updateThemeUI(currentTheme);

document.addEventListener('DOMContentLoaded', () => {
    const hash = window.location.hash.replace('#', '') || 'dashboard';
    if (PAGES.includes(hash)) {
        navigate(hash);
    } else {
        navigate('dashboard');
    }
});
