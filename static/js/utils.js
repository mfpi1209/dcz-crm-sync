// ---------------------------------------------------------------------------
// Abas segmentadas (design system — classe .ds-segment no HTML)
// ---------------------------------------------------------------------------
function dsSegActive(extraClasses = '') {
    return ('ds-segment__btn ds-segment__btn--active ' + extraClasses).trim();
}
function dsSegInactive(extraClasses = '') {
    return ('ds-segment__btn ds-segment__btn--inactive ' + extraClasses).trim();
}

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
const PAGES = ['dashboard', 'search', 'sync', 'kommo_sync', 'update', 'pipeline', 'match_merge', 'comercial_rgm', 'dist_comercial', 'distribuicao', 'ativacoes', 'intelligence', 'inadimplencia', 'feedback', 'comparar_cursos', 'recomendacao_cursos', 'localizacao_polos', 'info_cursos', 'leads_inscricao', 'cadastro_leads', 'logs', 'config', 'schedule', 'inscricao', 'avisos', 'kommo_dispatcher', 'meta-campaigns', 'recadastros', 'comercial_dashboard', 'auditoria_comercial', 'atualizar_preco', 'vocacional', 'leads_parados', 'minha_performance', 'premiacao_admin', 'macro_email', 'ajustes_matricula', 'repasse', 'dist_consultor', 'captacao', 'clicks', 'leads_promotores', 'profile', 'meus_atendimentos', 'premiacoes_internas', 'aprovacao_premiacoes', 'rematricula', 'disparador_whatsapp', 'ia_comercial', 'page_views', 'solicitacoes_ti', 'siaa_consulta', 'siaa_sessao', 'match_inadimplentes', 'materias_alunos', 'academico_interacoes'];
const PAGE_TITLES = { dashboard: 'Dashboard', search: 'Buscar', sync: 'Sincronização', kommo_sync: 'Sync Comercial', update: 'Upload Acadêmico', pipeline: 'Saneamento / Pipeline', match_merge: 'Match & Merge', comercial_rgm: 'Dashboard Comercial', dist_comercial: 'Distribuição Comercial', distribuicao: 'Distribuição', ativacoes: 'Ativações Acadêmicas', intelligence: 'Análises', inadimplencia: 'Inadimplência', feedback: 'Feedback', comparar_cursos: 'Comparar Cursos', recomendacao_cursos: 'Recomendação', localizacao_polos: 'Localização', info_cursos: 'Informações de Cursos', leads_inscricao: 'Leads em Inscrição Automática', cadastro_leads: 'Cadastro de Leads', logs: 'Logs / Relatórios', config: 'Configurações', schedule: 'Agendamento', inscricao: 'Inscrições', avisos: 'Avisos', kommo_dispatcher: 'Kommo Dispatcher', 'meta-campaigns': 'Campaign Performance', recadastros: 'Recadastros', comercial_dashboard: 'Dashboard Atendimentos', auditoria_comercial: 'Feedback Comercial', atualizar_preco: 'Atualizar Preço', vocacional: 'Dashboard Vocacional', leads_parados: 'Parados', minha_performance: 'Minha Performance', premiacao_admin: 'Premiação', macro_email: 'Macro Email', ajustes_matricula: 'Ajustes de Matrícula', repasse: 'Repasse', dist_consultor: 'Distribuição Consultor', captacao: 'Captação Externa', clicks: 'QR Codes', leads_promotores: 'Leads · Promotores', profile: 'Meu Perfil', meus_atendimentos: 'Meus Atendimentos', premiacoes_internas: 'Premiações Internas', aprovacao_premiacoes: 'Aprovação de Premiações', rematricula: 'Rematrícula', disparador_whatsapp: 'Disparador WhatsApp', ia_comercial: 'IA Comercial', page_views: 'Uso do Dashboard', solicitacoes_ti: 'Solicitações TI', siaa_consulta: 'Consulta SIAA', siaa_sessao: 'Atualizar sessão SIAA', match_inadimplentes: 'Match Inadimplentes', materias_alunos: 'Matérias dos Alunos', academico_interacoes: 'Interações Acadêmicas' };

// Páginas permitidas vêm do servidor (data-allowed-pages no <body>) — evita
// flash de UI carregando conteúdo proibido antes de o JS esconder.
let ALLOWED_PAGES = null;
function _initAllowedPages() {
    try {
        const raw = document.body && document.body.dataset.allowedPages;
        if (raw) ALLOWED_PAGES = new Set(JSON.parse(raw));
    } catch (e) { ALLOWED_PAGES = null; }
}
_initAllowedPages();

function isPageAllowed(page) {
    if (!ALLOWED_PAGES) return true;
    return ALLOWED_PAGES.has(page);
}

function _initialPageFromBody() {
    const ip = (document.body && document.body.dataset.initialPage) || '';
    return ip || 'dashboard';
}

let _bootSplashDismissed = false;
function _dismissBootSplash() {
    if (_bootSplashDismissed) return;
    _bootSplashDismissed = true;
    document.body.classList.remove('initial-bootstrap');
    document.body.classList.add('splash-exiting');
    setTimeout(() => {
        const sp = document.getElementById('boot-splash');
        if (sp && sp.parentNode) sp.parentNode.removeChild(sp);
        document.body.classList.remove('splash-exiting');
    }, 1350);
}

// Spotlight cyan que segue o cursor nos itens da sidebar.
// Atualiza CSS vars --mx/--my no elemento mais próximo (link ou cabeçalho de grupo).
(function _initSidebarSpotlight() {
    function attach() {
        const sb = document.getElementById('sidebar');
        if (!sb) return;
        sb.addEventListener('mousemove', function(e) {
            const el = e.target.closest('.sidebar-link, .sidebar-group-toggle');
            if (!el) return;
            const r = el.getBoundingClientRect();
            el.style.setProperty('--mx', (e.clientX - r.left) + 'px');
            el.style.setProperty('--my', (e.clientY - r.top) + 'px');
        }, { passive: true });
    }
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', attach);
    } else {
        attach();
    }
})();

function navigate(page, params) {
    if (!isPageAllowed(page)) {
        const fallback = _initialPageFromBody();
        if (page !== fallback && isPageAllowed(fallback)) {
            page = fallback;
        }
    }
    PAGES.forEach(p => {
        const el = document.getElementById('page-' + p);
        if (!el) return;
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
    _dismissBootSplash();
    document.querySelectorAll('.sidebar-link').forEach(el => {
        el.classList.toggle('active', el.dataset.page === page);
    });
    setPageTitle(PAGE_TITLES[page] || page);

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
    if (page === 'leads_inscricao') loadLeadsInscricao();
    if (page === 'cadastro_leads' && typeof loadCadastroLeads === 'function') loadCadastroLeads();
    if (page === 'meta-campaigns') loadMetaCampaigns();
    if (page === 'recadastros') loadRecadastros();
    if (page === 'leads_parados') loadLeadsParados();
    if (page === 'dist_consultor') loadDistConsultor();
    if (page === 'minha_performance') loadMinhaPerformance(params);
    if (page === 'premiacao_admin') loadPremiacaoAdmin();
    if (page === 'ajustes_matricula') loadAjustesMatricula();
    if (page === 'macro_email') loadMacroEmail();
    if (page === 'repasse') repInit();
    if (page === 'captacao') loadCaptacao();
    if (page === 'clicks') loadClicks();
    if (page === 'leads_promotores') loadLeadsPromotores();
    if (page === 'profile') loadProfile();
    if (page === 'meus_atendimentos' && typeof loadMeusAtendimentos === 'function') loadMeusAtendimentos();
    if (page === 'premiacoes_internas' && typeof loadPremiacoesInternas === 'function') loadPremiacoesInternas();
    if (page === 'aprovacao_premiacoes' && typeof loadAprovacaoPremiacoes === 'function') loadAprovacaoPremiacoes();
    if (page === 'disparador_whatsapp' && typeof loadDisparadorWhatsapp === 'function') loadDisparadorWhatsapp();
    if (page === 'rematricula' && typeof loadRematricula === 'function') loadRematricula();
    if (page === 'ia_comercial' && typeof loadIaComercial === 'function') loadIaComercial();
    if (page === 'page_views' && typeof loadPageViews === 'function') loadPageViews();
    if (page === 'solicitacoes_ti' && typeof loadSolicitacoesTi === 'function') loadSolicitacoesTi();
    if (page === 'siaa_consulta' && typeof siaaLoadPage === 'function') siaaLoadPage();
    if (page === 'siaa_sessao' && typeof siaaSessaoLoadPage === 'function') siaaSessaoLoadPage();
    if (page === 'match_inadimplentes' && typeof matchInadLoadPage === 'function') matchInadLoadPage();
    if (page === 'materias_alunos' && typeof materiasAlunosLoadPage === 'function') materiasAlunosLoadPage();
    if (page === 'academico_interacoes' && typeof acadInteracoesLoadPage === 'function') acadInteracoesLoadPage();

    history.replaceState(null, '', '#' + page);
    refreshTopbarForPage(page);
    trackPageView(page);
}

function trackPageView(page) {
    if (!page) return;
    try {
        const body = JSON.stringify({ page });
        if (navigator.sendBeacon) {
            const blob = new Blob([body], { type: 'application/json' });
            navigator.sendBeacon('/api/track-page-view', blob);
        } else {
            fetch('/api/track-page-view', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body, keepalive: true,
            }).catch(() => {});
        }
    } catch (e) { /* nunca atrapalha navegacao */ }
}

window.addEventListener('hashchange', () => {
    const fallback = _initialPageFromBody();
    const hash = location.hash.replace('#', '') || fallback;
    if (PAGES.includes(hash)) navigate(hash);
});

function navigateVoc(tab) {
    _dismissBootSplash();
    PAGES.forEach(p => {
        const el = document.getElementById('page-' + p);
        if (el) el.classList.toggle('hidden', p !== 'vocacional');
    });
    document.querySelectorAll('.sidebar-link').forEach(el => {
        el.classList.toggle('active', el.dataset.page === 'voc_' + tab);
    });
    setPageTitle(PAGE_TITLES['vocacional'] || 'Vocacional');
    document.getElementById('sidebar').classList.remove('open');
    document.getElementById('sidebar-overlay').classList.remove('open');
    vocLoadPage();
    vocSwitchTab(tab);
    history.replaceState(null, '', '#vocacional');
    refreshTopbarForPage('vocacional');
    trackPageView('vocacional');
}

function setPageTitle(text) {
    var title = text || '';
    var topbar = document.getElementById('page-title');
    if (topbar) {
        topbar.textContent = title;
        topbar.setAttribute('title', title);
    }
    var mobile = document.getElementById('mobile-title');
    if (mobile) mobile.textContent = title;
    if (title) document.title = title + ' · eduit.';
}

/** Intervalo de datas no pill da TopBar (ex.: Comercial RGM). */
function formatTopbarDateRange(isoFrom, isoTo) {
    function fmt(iso) {
        if (!iso) return '';
        var d = new Date(String(iso).substring(0, 10) + 'T12:00:00');
        if (isNaN(d.getTime())) return iso;
        return d.toLocaleDateString('pt-BR', { day: '2-digit', month: 'short', year: 'numeric' }).replace(/\./g, '');
    }
    return fmt(isoFrom) + ' — ' + fmt(isoTo);
}

function refreshTopbarForPage(page) {
    var metaText = document.getElementById('topbar-meta-text');
    if (!metaText) return;
    if (page === 'comercial_rgm') {
        var i = document.getElementById('crgm-dt-ini');
        var f = document.getElementById('crgm-dt-fim');
        if (i && f && i.value && f.value) {
            metaText.textContent = formatTopbarDateRange(i.value, f.value);
            return;
        }
    }
    metaText.textContent = 'eduit. · cockpit';
}

function initTopbarUser() {
    var raw = (document.body && document.body.getAttribute('data-username')) || '';
    var uname = (raw || '').trim();
    var wrap = document.getElementById('topbar-user');
    var elName = document.getElementById('topbar-user-name');
    var elIni = document.getElementById('topbar-user-initials');
    if (!wrap || !elName || !elIni) return;
    if (!uname) {
        wrap.classList.remove('has-name');
        wrap.removeAttribute('title');
        return;
    }
    elName.textContent = uname;
    wrap.setAttribute('title', uname);
    wrap.classList.add('has-name');
    var parts = uname.split(/\s+/).filter(Boolean);
    var a = (parts[0] && parts[0][0]) ? parts[0][0] : '';
    var b = (parts.length > 1 && parts[parts.length - 1][0]) ? parts[parts.length - 1][0] : '';
    elIni.textContent = (a + b).toUpperCase() || uname.slice(0, 2).toUpperCase();
}

function toggleSidebar() {
    document.getElementById('sidebar').classList.toggle('open');
    document.getElementById('sidebar-overlay').classList.toggle('open');
}

function toggleSidebarGroup(name) {
    const group = document.querySelector(`.sidebar-group[data-group="${name}"]`);
    if (group) group.classList.toggle('collapsed');
}

// Sidebar sempre comeca recolhida em cada page load. O usuario pode abrir os
// grupos durante a sessao via toggleSidebarGroup, mas a cada refresh o estado
// retorna ao padrao "tudo fechado" para um menu mais limpo no primeiro acesso.
(function collapseAllSidebarGroups() {
    document.querySelectorAll('.sidebar-group[data-group]').forEach(g => {
        g.classList.add('collapsed');
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
// Sidebar — permissões aplicadas no servidor (Jinja). Aqui apenas mantemos
// o role no body em sincronia com /api/me e disparamos o check de avisos.
// ---------------------------------------------------------------------------
async function applySidebarPermissions() {
    try {
        const res = await api('/api/me');
        const d = await res.json();
        const role = d.role || '';
        document.body.dataset.role = role;
        if (d.categoria) document.body.dataset.categoria = d.categoria;
    } catch (e) { console.error('sidebar permissions', e); }
}

window._sidebarPermsReady = applySidebarPermissions();
window._sidebarPermsReady.then(() => checkAvisosNaoLidos());

// ---------------------------------------------------------------------------
// Avisos — popup ao logar + badge sidebar
// ---------------------------------------------------------------------------
async function checkAvisosNaoLidos() {
    try {
        const res = await api('/api/avisos/nao-lidos');
        const data = await res.json();
        const count = data.count || 0;
        window._avisosCache = Array.isArray(data.avisos) ? data.avisos : [];
        window._avisosNaoLidos = count;

        const badge = document.getElementById('av-sidebar-badge');
        if (badge) {
            if (count > 0) { badge.textContent = count; badge.classList.remove('hidden'); }
            else badge.classList.add('hidden');
        }

        const tbDot = document.getElementById('tb-avisos-dot');
        const tbCount = document.getElementById('tb-avisos-count');
        if (tbDot && tbCount) {
            if (count > 0) {
                tbCount.textContent = count > 99 ? '99+' : count;
                tbCount.classList.remove('hidden');
                tbDot.classList.add('hidden');
            } else {
                tbCount.classList.add('hidden');
                tbDot.classList.add('hidden');
            }
        }
        renderNotifPanel(window._avisosCache);

        if (count > 0 && !sessionStorage.getItem('avisos_popup_shown_v2')) {
            _showAvisosPopup(data.avisos);
            sessionStorage.setItem('avisos_popup_shown_v2', '1');
        }
    } catch (e) { console.error('checkAvisosNaoLidos', e); }
}

// ---------------------------------------------------------------------------
// Topbar notification dropdown
// ---------------------------------------------------------------------------
function _initNotifPanel() {
    document.addEventListener('click', function(ev) {
        const wrap = document.getElementById('topbar-notif-wrap');
        const panel = document.getElementById('notif-panel');
        if (!wrap || !panel) return;
        if (!wrap.contains(ev.target)) closeNotifPanel();
    });
    document.addEventListener('keydown', function(ev) {
        if (ev.key === 'Escape') closeNotifPanel();
    });
}

function toggleNotifPanel(ev) {
    if (ev) ev.stopPropagation();
    const panel = document.getElementById('notif-panel');
    if (!panel) return;
    if (panel.classList.contains('hidden')) openNotifPanel();
    else closeNotifPanel();
}

function openNotifPanel() {
    const panel = document.getElementById('notif-panel');
    if (!panel) return;
    panel.classList.remove('hidden');
    renderNotifPanel(window._avisosCache || []);
    api('/api/avisos').then(r => r.json()).then(rows => {
        if (Array.isArray(rows)) renderNotifPanel(rows.slice(0, 12));
    }).catch(()=>{});
}

function closeNotifPanel() {
    const panel = document.getElementById('notif-panel');
    if (panel) panel.classList.add('hidden');
}

function renderNotifPanel(items) {
    const list = document.getElementById('notif-panel-list');
    const countLabel = document.getElementById('notif-panel-count');
    if (!list) return;
    const naoLidos = (window._avisosNaoLidos || 0);
    if (countLabel) countLabel.textContent = naoLidos > 0 ? (naoLidos + ' Nova' + (naoLidos > 1 ? 's' : '')) : 'Em dia';

    if (!items || !items.length) {
        list.innerHTML = '<div class="p-8 text-center"><div class="w-10 h-10 mx-auto mb-2 rounded-full flex items-center justify-center" style="background: var(--bg-elevated);"><span class="material-symbols-outlined text-[20px]" style="color: var(--text-muted);">inbox</span></div><p class="text-xs text-[var(--text-muted)]">Sem avisos no momento.</p></div>';
        return;
    }
    const ICONS = {
        urgente:    { icon: 'priority_high', cls: 'bg-rose-100 text-rose-700 dark:bg-rose-500/20 dark:text-rose-300' },
        importante: { icon: 'warning',       cls: 'bg-amber-100 text-amber-700 dark:bg-amber-500/20 dark:text-amber-300' },
        normal:     { icon: 'info',          cls: 'bg-blue-100 text-blue-700 dark:bg-blue-500/20 dark:text-blue-300' },
    };
    const fmtTime = (iso) => {
        if (!iso) return '';
        const d = new Date(iso);
        const diffMs = Date.now() - d.getTime();
        const m = Math.floor(diffMs / 60000);
        if (m < 1) return 'agora';
        if (m < 60) return m + ' min';
        const h = Math.floor(m / 60);
        if (h < 24) return h + 'h';
        const dd = Math.floor(h / 24);
        return dd + 'd';
    };
    list.innerHTML = items.map(a => {
        const ic = ICONS[a.prioridade] || ICONS.normal;
        const unreadBg = a.lido ? '' : 'background: rgba(0,52,111,0.04);';
        return `<div class="flex gap-3 px-4 py-3 border-b cursor-default transition-colors hover:bg-[var(--bg-elevated)]"
                     style="border-color: var(--border); ${unreadBg}">
            <div class="w-8 h-8 rounded-full flex items-center justify-center shrink-0 ${ic.cls}">
                <span class="material-symbols-outlined text-[16px]">${ic.icon}</span>
            </div>
            <div class="min-w-0 flex-1 space-y-0.5">
                <div class="flex items-center justify-between gap-2">
                    <p class="text-xs font-bold text-[var(--text-primary)] leading-tight truncate">${esc(a.titulo || '')}</p>
                    <span class="text-[10px] font-medium text-[var(--text-muted)] shrink-0">${fmtTime(a.created_at)}</span>
                </div>
                <p class="text-[11px] text-[var(--text-secondary)] leading-snug line-clamp-2">${esc(a.corpo || '')}</p>
                ${a.autor ? `<p class="text-[10px] text-[var(--text-muted)] mt-0.5">${esc(a.autor)}</p>` : ''}
            </div>
        </div>`;
    }).join('');
}

function _showAvisosPopup(avisos) {
    if (!avisos || !avisos.length) return;
    const existing = document.getElementById('avisos-popup-overlay');
    if (existing) existing.remove();

    const prioBadge = { urgente: 'bg-red-100 text-red-700 dark:bg-red-500/20 dark:text-red-400', importante: 'bg-amber-100 text-amber-700 dark:bg-amber-500/20 dark:text-amber-400', normal: 'bg-slate-200 text-slate-700 dark:bg-slate-500/20 dark:text-slate-400' };

    const cards = avisos.slice(0, 10).map(a => {
        const pb = prioBadge[a.prioridade] || prioBadge.normal;
        const dt = a.created_at ? new Date(a.created_at).toLocaleDateString('pt-BR') : '';
        return `<div class="p-3 rounded-lg bg-slate-50 dark:bg-slate-800/50 border border-[var(--border)] mb-2">
            <div class="flex items-center gap-2 mb-1">
                <span class="text-[10px] font-bold px-2 py-0.5 rounded-full ${pb}">${a.prioridade}</span>
                <span class="text-sm font-semibold text-[var(--text-primary)]">${a.titulo}</span>
            </div>
            <p class="text-xs text-slate-700 dark:text-slate-300 whitespace-pre-line">${a.corpo}</p>
            <p class="text-[10px] text-slate-500 dark:text-slate-600 mt-1">${dt} — ${a.autor || 'Sistema'}</p>
        </div>`;
    }).join('');

    const overlay = document.createElement('div');
    overlay.id = 'avisos-popup-overlay';
    overlay.className = 'fixed inset-0 z-[9999] flex items-center justify-center bg-black/40 backdrop-blur-sm';
    overlay.innerHTML = `
        <div class="glass-card w-full max-w-lg mx-4 max-h-[80vh] flex flex-col rounded-2xl shadow-2xl">
            <div class="flex items-center justify-between p-5 border-b border-[var(--border)]">
                <div class="flex items-center gap-3">
                    <div class="w-8 h-8 rounded-lg bg-amber-100 dark:bg-amber-500/20 flex items-center justify-center">
                        <svg class="w-4 h-4 text-amber-700 dark:text-amber-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 17h5l-1.405-1.405A2.032 2.032 0 0118 14.158V11a6.002 6.002 0 00-4-5.659V5a2 2 0 10-4 0v.341C7.67 6.165 6 8.388 6 11v3.159c0 .538-.214 1.055-.595 1.436L4 17h5m6 0v1a3 3 0 11-6 0v-1m6 0H9"/></svg>
                    </div>
                    <h3 class="text-base font-bold text-[var(--text-primary)]">Avisos (${avisos.length} não lido${avisos.length > 1 ? 's' : ''})</h3>
                </div>
                <button onclick="document.getElementById('avisos-popup-overlay').remove()" class="text-slate-500 hover:text-[var(--text-primary)] text-xl leading-none">&times;</button>
            </div>
            <div class="overflow-y-auto p-5 flex-1">${cards}</div>
            <div class="flex items-center justify-between p-4 border-t border-[var(--border)]">
                <button onclick="_popupMarcarTodos()" class="text-xs text-violet-700 dark:text-violet-400 hover:text-violet-900 dark:hover:text-violet-300 transition">Marcar todos como lidos</button>
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
    if (typeof profSyncThemeButtons === 'function') profSyncThemeButtons();
}

function updateThemeUI(theme) {
    var pairs = [
        { sun: 'theme-icon-sun',    moon: 'theme-icon-moon' },
        { sun: 'tb-theme-icon-sun', moon: 'tb-theme-icon-moon' },
    ];
    pairs.forEach(function(p) {
        var sun  = document.getElementById(p.sun);
        var moon = document.getElementById(p.moon);
        if (!sun || !moon) return;
        if (theme === 'dark') {
            sun.classList.add('hidden');
            moon.classList.remove('hidden');
        } else {
            sun.classList.remove('hidden');
            moon.classList.add('hidden');
        }
    });
    var label = document.getElementById('theme-label');
    if (label) label.textContent = theme === 'dark' ? 'Modo claro' : 'Modo escuro';

    applyChartTheme(theme);
}

// ---------------------------------------------------------------------------
// Chart.js global theming (light/dark)
// ---------------------------------------------------------------------------
function applyChartTheme(theme) {
    if (typeof Chart === 'undefined' || !Chart.defaults) return;
    var isDark = theme === 'dark';
    var textColor   = isDark ? 'rgba(226, 232, 240, 0.85)' : 'rgba(30, 41, 59, 0.9)';
    var mutedColor  = isDark ? 'rgba(148, 163, 184, 0.7)'  : 'rgba(71, 85, 105, 0.85)';
    var gridColor   = isDark ? 'rgba(148, 163, 184, 0.10)' : 'rgba(15, 23, 42, 0.08)';
    var borderColor = isDark ? 'rgba(148, 163, 184, 0.20)' : 'rgba(15, 23, 42, 0.15)';
    var tooltipBg   = isDark ? 'rgba(15, 23, 42, 0.95)'    : 'rgba(255, 255, 255, 0.97)';
    var tooltipText = isDark ? 'rgba(226, 232, 240, 1)'    : 'rgba(15, 23, 42, 1)';

    Chart.defaults.color = textColor;
    Chart.defaults.borderColor = borderColor;
    if (Chart.defaults.font) Chart.defaults.font.family = "'Inter', system-ui, sans-serif";

    if (Chart.defaults.plugins) {
        if (Chart.defaults.plugins.legend) {
            Chart.defaults.plugins.legend.labels = Chart.defaults.plugins.legend.labels || {};
            Chart.defaults.plugins.legend.labels.color = textColor;
        }
        if (Chart.defaults.plugins.tooltip) {
            Chart.defaults.plugins.tooltip.backgroundColor = tooltipBg;
            Chart.defaults.plugins.tooltip.titleColor = tooltipText;
            Chart.defaults.plugins.tooltip.bodyColor = tooltipText;
            Chart.defaults.plugins.tooltip.borderColor = borderColor;
            Chart.defaults.plugins.tooltip.borderWidth = 1;
        }
    }

    if (Chart.defaults.scale) {
        Chart.defaults.scale.grid = Chart.defaults.scale.grid || {};
        Chart.defaults.scale.grid.color = gridColor;
        Chart.defaults.scale.ticks = Chart.defaults.scale.ticks || {};
        Chart.defaults.scale.ticks.color = mutedColor;
    }
    ['scales', 'category', 'linear', 'logarithmic', 'time', 'radialLinear'].forEach(function(scale) {
        var s = Chart.defaults.scales && Chart.defaults.scales[scale];
        if (s) {
            s.grid = s.grid || {}; s.grid.color = gridColor;
            s.ticks = s.ticks || {}; s.ticks.color = mutedColor;
            if (s.angleLines) s.angleLines.color = gridColor;
        }
    });

    try {
        var instances = Chart.instances ? Object.values(Chart.instances) : [];
        instances.forEach(function(inst) {
            try { inst.update('none'); } catch (_) {}
        });
    } catch (_) {}

    if (typeof ApexCharts !== 'undefined') {
        try {
            var apexMode = isDark ? 'dark' : 'light';
            (window.ApexCharts._instances || []).forEach(function(){});
        } catch (_) {}
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
// KPI Card — componente reutilizável estilo Arquiteto Executivo.
// Uso:
//   kpiCard({ title: 'Receita', value: 'R$ 1,2M', subtitle: 'mês', trend: '+12%', isPositive: true })
//   kpiCard({ ..., variant: 'primary' })          // azul escuro
//   kpiCard({ ..., variant: 'secondary' })        // azul médio
//   kpiCard({ ..., id: 'kpi-receita-mes' })       // adiciona id
//   kpiCard({ ..., sparkId: 'spark-receita' })    // reserva canvas <canvas id> para sparkline
// ---------------------------------------------------------------------------
function kpiCard(opts) {
    opts = opts || {};
    const variant = opts.variant === 'primary' || opts.variant === 'secondary' ? opts.variant : null;
    const variantCls = variant === 'primary' ? 'is-primary' : variant === 'secondary' ? 'is-secondary' : '';
    const isPositive = !!opts.isPositive;
    const trendCls = isPositive ? 'up' : 'down';
    const trendIcon = isPositive ? 'trending_up' : 'trending_down';
    const trendHtml = (opts.trend === undefined || opts.trend === null || opts.trend === '')
        ? ''
        : `<span class="kpi-trend ${trendCls}">
            <span class="material-symbols-outlined ms-icon">${trendIcon}</span>
            ${esc(String(opts.trend))}
           </span>`;
    const sparkHtml = opts.sparkId
        ? `<div class="kpi-spark"><canvas id="${esc(opts.sparkId)}"></canvas></div>`
        : '';
    const idAttr = opts.id ? ` id="${esc(opts.id)}"` : '';
    const valueId = opts.valueId ? ` id="${esc(opts.valueId)}"` : '';
    const subtitleId = opts.subtitleId ? ` id="${esc(opts.subtitleId)}"` : '';
    const trendId = opts.trendId ? ` id="${esc(opts.trendId)}"` : '';

    return `<div class="kpi-card ${variantCls}"${idAttr}>
        <div class="kpi-head">
            <span class="kpi-title">${esc(opts.title || '')}</span>
            ${trendHtml ? `<span${trendId}>${trendHtml}</span>` : ''}
        </div>
        <div class="kpi-value-row">
            <span class="kpi-value"${valueId}>${esc(String(opts.value != null ? opts.value : '—'))}</span>
            ${opts.subtitle ? `<span class="kpi-subtitle"${subtitleId}>${esc(opts.subtitle)}</span>` : ''}
        </div>
        ${sparkHtml}
    </div>`;
}

// ---------------------------------------------------------------------------
// Scroll to top (works on window scroll AND inner <main> overflow scroll)
// ---------------------------------------------------------------------------
function _scrollContainer() {
    var main = document.querySelector('main.flex-1.overflow-auto');
    if (main && main.scrollHeight > main.clientHeight) return main;
    return window;
}

function scrollPageToTop() {
    var c = _scrollContainer();
    if (c === window) {
        window.scrollTo({ top: 0, behavior: 'smooth' });
    } else {
        try { c.scrollTo({ top: 0, behavior: 'smooth' }); }
        catch (_) { c.scrollTop = 0; }
    }
}

function _initScrollToTop() {
    var btn = document.getElementById('scroll-to-top');
    if (!btn) return;
    var threshold = 300;

    function update() {
        var c = _scrollContainer();
        var y = c === window ? (window.pageYOffset || document.documentElement.scrollTop) : c.scrollTop;
        if (y > threshold) {
            btn.classList.remove('opacity-0', 'translate-y-4', 'pointer-events-none');
            btn.classList.add('opacity-100', 'translate-y-0');
        } else {
            btn.classList.add('opacity-0', 'translate-y-4', 'pointer-events-none');
            btn.classList.remove('opacity-100', 'translate-y-0');
        }
    }

    var main = document.querySelector('main.flex-1.overflow-auto');
    if (main) main.addEventListener('scroll', update, { passive: true });
    window.addEventListener('scroll', update, { passive: true });
    update();
}

// ---------------------------------------------------------------------------
// Polos — nomes canônicos (espelha helpers.normalize_polo_display)
// ---------------------------------------------------------------------------
function _poloRawKey(polo) {
    if (!polo) return '';
    let p = String(polo).normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase();
    p = p.replace(/^\d+\s*[-–]\s*/, '');
    p = p.replace(/^ceb\s+/, '');
    p = p.replace(/^polo\s+sp_/, '');
    p = p.replace(/^polo\s+/, '');
    p = p.replace(/\([^)]*\)/g, '');
    p = p.replace(/\s+/g, ' ').trim();
    return p;
}

function normalizePoloDisplay(polo) {
    if (!polo || !String(polo).trim()) return '';
    const k = _poloRawKey(polo);
    if (k.includes('taboao') || k.includes('taboa')) {
        if (k.includes('mituzi') || k.includes('jardim')) return 'Taboão da Serra_Jardim Mituzi';
        return 'Taboão da Serra_Centro';
    }
    if (k.includes('barra funda')) return 'Barra Funda';
    if (k.includes('sapopemba')) return 'Sapopemba';
    if (k.includes('vila prudente')) return 'Vila Prudente';
    if (k.includes('santana')) return 'Santana 2';
    if (k.includes('ibirapuera')) return 'Ibirapuera';
    if (k.includes('morumbi')) return 'Morumbi';
    if (k.includes('campinas')) return 'Campinas';
    if (k.includes('capivari')) return 'Capivari';
    if (k.includes('itapira')) return 'Itapira';
    if (k.includes('freguesia')) return 'Freguesia do Ó';
    if (k.includes('vila mariana')) return 'Vila Mariana';
    const cleaned = _poloRawKey(polo).replace(/_/g, ' ');
    if (!cleaned) return String(polo).trim();
    return cleaned.split(' ').map((w, i) => {
        if (['da', 'de', 'do', 'dos', 'das', 'e'].includes(w) && i > 0) return w;
        return w.charAt(0).toUpperCase() + w.slice(1);
    }).join(' ');
}

function mergePoloBreakdown(byPolo) {
    const out = {};
    for (const [raw, cnt] of Object.entries(byPolo || {})) {
        const name = normalizePoloDisplay(raw) || raw;
        out[name] = (out[name] || 0) + (cnt || 0);
    }
    return Object.fromEntries(Object.entries(out).sort((a, b) => b[1] - a[1]));
}

// ---------------------------------------------------------------------------
// Modais — portar para <body> (position:fixed relativo à viewport)
// ---------------------------------------------------------------------------
const DCZ_MODAL_ROOT_IDS = [
    'dist-modal-add',
    'ac-modal-overlay',
    'fb-modal-overlay',
    'rule-modal-overlay',
    'me-modal-overlay',
    'dc-modal-overlay',
    'ap-modal',
    'pi-modal',
    'iac-exec-modal',
    'mp-modal-minha-mat',
    'mp-modal-ajuste',
    'pa-grupo-modal',
    'pa-edit-modal',
    'crgm-edit-meta-modal',
];

function dczPortalToBody(el) {
    if (!el || el.parentElement === document.body) {
        if (el) el.classList.add('dcz-modal-portal');
        return el;
    }
    document.body.appendChild(el);
    el.classList.add('dcz-modal-portal');
    return el;
}

function dczRelocateModalsToBody() {
    const main = document.querySelector('main');
    if (!main) return;
    DCZ_MODAL_ROOT_IDS.forEach((id) => {
        const el = document.getElementById(id);
        if (el && main.contains(el)) dczPortalToBody(el);
    });
    main.querySelectorAll('[data-dcz-modal-root]').forEach((el) => dczPortalToBody(el));
}

function dczLockBodyScroll(lock) {
    document.body.classList.toggle('dcz-modal-open', !!lock);
}

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------
const currentTheme = localStorage.getItem('eduit-theme') || 'dark';
updateThemeUI(currentTheme);

document.addEventListener('DOMContentLoaded', () => {
    dczRelocateModalsToBody();
    initTopbarUser();
    _initScrollToTop();
    _initNotifPanel();
    const initial = _initialPageFromBody();
    const hash = window.location.hash.replace('#', '') || initial;
    if (PAGES.includes(hash) && isPageAllowed(hash)) {
        navigate(hash);
    } else {
        navigate(initial);
    }
});
