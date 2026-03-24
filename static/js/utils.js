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
const PAGES = ['dashboard', 'search', 'sync', 'kommo_sync', 'update', 'pipeline', 'match_merge', 'comercial_rgm', 'dist_comercial', 'distribuicao', 'ativacoes', 'intelligence', 'inadimplencia', 'feedback', 'comparar_cursos', 'recomendacao_cursos', 'localizacao_polos', 'info_cursos', 'logs', 'config', 'schedule', 'inscricao', 'avisos', 'kommo_dispatcher', 'meta-campaigns', 'recadastros', 'comercial_dashboard', 'auditoria_comercial', 'vocacional'];
const PAGE_TITLES = { dashboard: 'Dashboard', search: 'Buscar', sync: 'Sincronização', kommo_sync: 'Sync Comercial', update: 'Atualização CRM', pipeline: 'Saneamento / Pipeline', match_merge: 'Match & Merge', comercial_rgm: 'Dashboard Comercial', dist_comercial: 'Distribuição Comercial', distribuicao: 'Distribuição', ativacoes: 'Ativações Acadêmicas', intelligence: 'Inteligência', inadimplencia: 'Inadimplência', feedback: 'Feedback', comparar_cursos: 'Comparar Cursos', recomendacao_cursos: 'Recomendação', localizacao_polos: 'Localização', info_cursos: 'Informações de Cursos', logs: 'Logs / Relatórios', config: 'Configurações', schedule: 'Agendamento', inscricao: 'Inscrição Automática', avisos: 'Avisos', kommo_dispatcher: 'Kommo Dispatcher', 'meta-campaigns': 'Campaign Performance', recadastros: 'Recadastros', comercial_dashboard: 'Dashboard Atendimentos', auditoria_comercial: 'Feedback Comercial', vocacional: 'Dashboard Vocacional' };

function navigate(page) {
    PAGES.forEach(p => {
        const el = document.getElementById('page-' + p);
        if (p === page) {
            el.classList.remove('hidden');
            el.classList.remove('page-enter');
            void el.offsetWidth;
            el.classList.add('page-enter');
        } else {
            el.classList.add('hidden');
            el.classList.remove('page-enter');
        }
    });
    document.querySelectorAll('.sidebar-link').forEach(el => {
        el.classList.toggle('active', el.dataset.page === page);
    });
    document.getElementById('mobile-title').textContent = PAGE_TITLES[page] || page;

    document.getElementById('sidebar').classList.remove('open');
    document.getElementById('sidebar-overlay').classList.remove('open');

    if (page === 'dashboard') loadDashboard();
    if (page === 'search') loadXlSnapshots();
    if (page === 'sync') loadSyncState();
    if (page === 'update') loadFileInfo();
    if (page === 'logs') { loadLogFiles(); loadDashboard(); }
    if (page === 'config') { loadCiclos(); loadTurmas(); }
    if (page === 'distribuicao') loadDistribuicao();
    if (page === 'dist_comercial') loadDistComercial();
    if (page === 'ativacoes') loadAtivacoes();
    if (page === 'intelligence') loadIntelligence();
    if (page === 'inadimplencia') loadInadimplencia();
    if (page === 'kommo_sync') loadKommoSync();
    if (page === 'match_merge') loadMatchMerge();
    if (page === 'comercial_rgm') loadComercialRgm();
    if (page === 'feedback') fbInit();
    if (page === 'inscricao') loadInscricao();
    if (page === 'comercial_dashboard') cdLoadPage();
    if (page === 'auditoria_comercial' && typeof acLoadPage === 'function') acLoadPage();
    if (page === 'vocacional') vocLoadPage();
    if (page === 'schedule') loadSchedules();
    if (page === 'avisos') loadAvisos();
    if (page === 'kommo_dispatcher') loadKommoDispatcher();
    if (FERRAMENTA_MAP && FERRAMENTA_MAP[page]) loadFerramenta(page);
    if (page === 'meta-campaigns') loadMetaCampaigns();
    if (page === 'recadastros') loadRecadastros();

    history.replaceState(null, '', '#' + page);
}

window.addEventListener('hashchange', () => {
    const hash = location.hash.replace('#', '') || 'dashboard';
    if (PAGES.includes(hash)) navigate(hash);
});

function navigateVoc(tab) {
    PAGES.forEach(p => {
        document.getElementById('page-' + p).classList.toggle('hidden', p !== 'vocacional');
    });
    document.querySelectorAll('.sidebar-link').forEach(el => {
        el.classList.toggle('active', el.dataset.page === 'voc_' + tab);
    });
    document.getElementById('mobile-title').textContent = PAGE_TITLES['vocacional'] || 'Vocacional';
    document.getElementById('sidebar').classList.remove('open');
    document.getElementById('sidebar-overlay').classList.remove('open');
    vocLoadPage();
    vocSwitchTab(tab);
    history.replaceState(null, '', '#vocacional');
}

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
        if (localStorage.getItem(key) !== '1') g.classList.add('collapsed');
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
    comercial: ['pipeline', 'update', 'match_merge', 'comercial_rgm', 'inscricao'],
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
            const sysPages = ['sync', 'kommo_sync', 'logs', 'config', 'schedule', 'kommo_dispatcher'];
            const anySys = role === 'admin' || sysPages.some(p => pages.includes(p));
            sistemaLabel.style.display = anySys ? '' : 'none';
        }

        const cfgTab = document.getElementById('cfg-tab-usuarios');
        if (cfgTab) cfgTab.style.display = role === 'admin' ? '' : 'none';

        document.body.dataset.role = role;
    } catch (e) { console.error('sidebar permissions', e); }
}

applySidebarPermissions().then(() => checkAvisosNaoLidos());

// ---------------------------------------------------------------------------
// Avisos — popup ao logar + badge sidebar
// ---------------------------------------------------------------------------
async function checkAvisosNaoLidos() {
    try {
        const res = await api('/api/avisos/nao-lidos');
        const data = await res.json();
        const count = data.count || 0;

        const badge = document.getElementById('av-sidebar-badge');
        if (badge) {
            if (count > 0) { badge.textContent = count; badge.classList.remove('hidden'); }
            else badge.classList.add('hidden');
        }

        if (count > 0 && !sessionStorage.getItem('avisos_popup_shown')) {
            _showAvisosPopup(data.avisos);
            sessionStorage.setItem('avisos_popup_shown', '1');
        }
    } catch (e) { console.error('checkAvisosNaoLidos', e); }
}

function _showAvisosPopup(avisos) {
    if (!avisos || !avisos.length) return;
    const existing = document.getElementById('avisos-popup-overlay');
    if (existing) existing.remove();

    const prioBadge = { urgente: 'bg-red-500/20 text-red-400', importante: 'bg-amber-500/20 text-amber-400', normal: 'bg-slate-500/20 text-slate-400' };

    const cards = avisos.slice(0, 10).map(a => {
        const pb = prioBadge[a.prioridade] || prioBadge.normal;
        const dt = a.created_at ? new Date(a.created_at).toLocaleDateString('pt-BR') : '';
        return `<div class="p-3 rounded-lg bg-slate-800/50 border border-slate-700/40 mb-2">
            <div class="flex items-center gap-2 mb-1">
                <span class="text-[10px] font-bold px-2 py-0.5 rounded-full ${pb}">${a.prioridade}</span>
                <span class="text-sm font-semibold text-white">${a.titulo}</span>
            </div>
            <p class="text-xs text-slate-300 whitespace-pre-line">${a.corpo}</p>
            <p class="text-[10px] text-slate-600 mt-1">${dt} — ${a.autor || 'Sistema'}</p>
        </div>`;
    }).join('');

    const overlay = document.createElement('div');
    overlay.id = 'avisos-popup-overlay';
    overlay.className = 'fixed inset-0 z-[9999] flex items-center justify-center bg-black/60 backdrop-blur-sm';
    overlay.innerHTML = `
        <div class="glass-card w-full max-w-lg mx-4 max-h-[80vh] flex flex-col rounded-2xl shadow-2xl border border-slate-700/50">
            <div class="flex items-center justify-between p-5 border-b border-slate-700/40">
                <div class="flex items-center gap-3">
                    <div class="w-8 h-8 rounded-lg bg-amber-500/20 flex items-center justify-center">
                        <svg class="w-4 h-4 text-amber-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 17h5l-1.405-1.405A2.032 2.032 0 0118 14.158V11a6.002 6.002 0 00-4-5.659V5a2 2 0 10-4 0v.341C7.67 6.165 6 8.388 6 11v3.159c0 .538-.214 1.055-.595 1.436L4 17h5m6 0v1a3 3 0 11-6 0v-1m6 0H9"/></svg>
                    </div>
                    <h3 class="text-base font-bold text-white">Avisos (${avisos.length} não lido${avisos.length > 1 ? 's' : ''})</h3>
                </div>
                <button onclick="document.getElementById('avisos-popup-overlay').remove()" class="text-slate-400 hover:text-white text-xl leading-none">&times;</button>
            </div>
            <div class="overflow-y-auto p-5 flex-1">${cards}</div>
            <div class="flex items-center justify-between p-4 border-t border-slate-700/40">
                <button onclick="_popupMarcarTodos()" class="text-xs text-violet-400 hover:text-violet-300 transition">Marcar todos como lidos</button>
                <button onclick="document.getElementById('avisos-popup-overlay').remove(); navigate('avisos');" class="btn-primary text-white font-medium text-xs px-4 py-2 rounded-lg">Ver todos</button>
            </div>
        </div>`;
    document.body.appendChild(overlay);
    overlay.addEventListener('click', e => { if (e.target === overlay) overlay.remove(); });
}

function _popupMarcarTodos() {
    api('/api/avisos/marcar-todos-lidos', { method: 'POST' }).then(() => {
        const overlay = document.getElementById('avisos-popup-overlay');
        if (overlay) overlay.remove();
        checkAvisosNaoLidos();
        if (typeof _loadNaoLidos === 'function') _loadNaoLidos();
    });
}

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
// Toast notifications
// ---------------------------------------------------------------------------
const TOAST_ICONS = {
    success: '<svg class="toast-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path stroke-linecap="round" stroke-linejoin="round" d="M5 13l4 4L19 7"/></svg>',
    error:   '<svg class="toast-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><circle cx="12" cy="12" r="10"/><path stroke-linecap="round" d="M15 9l-6 6M9 9l6 6"/></svg>',
    warning: '<svg class="toast-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path stroke-linecap="round" stroke-linejoin="round" d="M12 9v4m0 4h.01M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z"/></svg>',
    info:    '<svg class="toast-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><circle cx="12" cy="12" r="10"/><path stroke-linecap="round" d="M12 16v-4M12 8h.01"/></svg>',
};

function toast(message, type = 'info', duration = 4000) {
    const container = document.getElementById('toast-container');
    if (!container) return;

    const el = document.createElement('div');
    el.className = `toast toast-${type}`;
    el.style.position = 'relative';
    el.style.overflow = 'hidden';
    el.innerHTML = `
        ${TOAST_ICONS[type] || TOAST_ICONS.info}
        <span style="flex:1">${esc(message)}</span>
        <button class="toast-close" onclick="this.closest('.toast').remove()">&times;</button>
        <div class="toast-progress" style="width:100%;transition-duration:${duration}ms"></div>`;
    container.appendChild(el);

    requestAnimationFrame(() => {
        const bar = el.querySelector('.toast-progress');
        if (bar) bar.style.width = '0%';
    });

    const timer = setTimeout(() => {
        el.classList.add('removing');
        setTimeout(() => el.remove(), 260);
    }, duration);

    el.querySelector('.toast-close').addEventListener('click', () => clearTimeout(timer));
    return el;
}

// ---------------------------------------------------------------------------
// Count-up animation
// ---------------------------------------------------------------------------
function countUp(el, target, duration = 600) {
    if (!el || isNaN(target)) return;
    const start = parseInt(el.textContent.replace(/\D/g, '')) || 0;
    if (start === target) return;

    const startTime = performance.now();
    const fmt = n => Math.round(n).toLocaleString('pt-BR');

    function tick(now) {
        const elapsed = now - startTime;
        const progress = Math.min(elapsed / duration, 1);
        const ease = 1 - Math.pow(1 - progress, 3);
        el.textContent = fmt(start + (target - start) * ease);
        if (progress < 1) requestAnimationFrame(tick);
    }
    requestAnimationFrame(tick);
}

function countUpAll(container) {
    if (!container) return;
    container.querySelectorAll('[data-count]').forEach(el => {
        const target = parseInt(el.dataset.count);
        if (!isNaN(target)) countUp(el, target);
    });
}

// ---------------------------------------------------------------------------
// Skeleton helpers
// ---------------------------------------------------------------------------
function showSkeleton(containerId, count = 4) {
    const el = document.getElementById(containerId);
    if (!el) return;
    el.innerHTML = Array.from({ length: count }, () =>
        '<div class="skeleton skeleton-card p-5"></div>'
    ).join('');
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
