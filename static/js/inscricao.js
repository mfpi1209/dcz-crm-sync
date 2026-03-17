// ---------------------------------------------------------------------------
// Inscrição Automática — Dashboard
// ---------------------------------------------------------------------------
const INSC_API_BASE = 'https://n8n-new-n8n.ca31ey.easypanel.host/webhook/dash_isncri';

const inscState = {
    view: 'home',
    from: '',
    to: '',
    limit: 200,
    offset: 0,
    data: null,
    loading: false,
    error: null,
    initialized: false
};

function inscSafe(val, fallback) {
    return (val === null || val === undefined || val === '') ? (fallback || '—') : val;
}

function inscBuildURL() {
    const params = new URLSearchParams();
    params.set('view', inscState.view);
    if (inscState.from) params.set('from', inscState.from);
    if (inscState.to) params.set('to', inscState.to);
    if (inscState.view === 'errors') {
        params.set('limit', inscState.limit);
        params.set('offset', inscState.offset);
    }
    return `${INSC_API_BASE}?${params.toString()}`;
}

function inscShowLoading(show) {
    inscState.loading = show;
    document.getElementById('insc-loading').classList.toggle('active', show);
}

function inscShowError(msg) {
    inscState.error = msg;
    const banner = document.getElementById('insc-errorBanner');
    document.getElementById('insc-errorBannerText').textContent = msg;
    banner.classList.add('active');
}

function inscCloseBanner() {
    document.getElementById('insc-errorBanner').classList.remove('active');
    inscState.error = null;
}

function inscFormatDate(isoStr) {
    if (!isoStr) return '—';
    try {
        const d = new Date(isoStr);
        return d.toLocaleDateString('pt-BR', { day: '2-digit', month: '2-digit', year: 'numeric' });
    } catch { return isoStr; }
}

function inscFormatDateTime(isoStr) {
    if (!isoStr) return null;
    try {
        const d = new Date(isoStr);
        if (isNaN(d.getTime())) return null;
        return d.toLocaleString('pt-BR', {
            day: '2-digit', month: '2-digit', year: 'numeric',
            hour: '2-digit', minute: '2-digit', second: '2-digit'
        });
    } catch { return null; }
}

function inscGetEndTime(row) {
    return row.finished_at || row.finishedAt || row.stoppedAt
        || row.ended_at   || row.endedAt    || row.completedAt
        || row.completed_at || row.data_fim  || row.updatedAt
        || row.updated_at  || null;
}

// ---------------------------------------------------------------------------
// Fetch
// ---------------------------------------------------------------------------
async function inscFetchData() {
    inscCloseBanner();
    inscShowLoading(true);

    const url = inscBuildURL();
    console.log('[Inscrição] Fetching:', url);

    try {
        const res = await fetch(url);
        if (!res.ok) throw new Error(`HTTP ${res.status}: ${res.statusText}`);
        const json = await res.json();
        inscState.data = json;
        inscRender();
    } catch (err) {
        console.error('[Inscrição] Fetch error:', err);
        inscShowError(`Erro de rede: ${err.message}. Verifique sua conexão e tente novamente.`);
        document.getElementById('insc-contentArea').innerHTML = inscRenderEmptyState(
            'Falha ao carregar',
            'Não foi possível conectar ao servidor. Tente novamente.'
        );
    } finally {
        inscShowLoading(false);
    }
}

// ---------------------------------------------------------------------------
// Actions
// ---------------------------------------------------------------------------
function inscSyncFilters() {
    inscState.from = document.getElementById('insc-inputFrom').value;
    inscState.to   = document.getElementById('insc-inputTo').value;
}

function inscApplyFilter() {
    inscSyncFilters();
    if (inscState.view === 'errors') inscState.offset = 0;
    inscFetchData();
}

function inscSwitchToErrors() {
    inscSyncFilters();
    inscState.view = 'errors';
    inscState.offset = 0;
    inscUpdateToolbarButtons();
    inscFetchData();
}

function inscSwitchToCpfInscrito() {
    inscSyncFilters();
    inscState.view = 'cpf_inscrito';
    inscState.offset = 0;
    inscUpdateToolbarButtons();
    inscFetchData();
}

function inscSwitchToHome() {
    inscSyncFilters();
    inscState.view = 'home';
    inscUpdateToolbarButtons();
    inscFetchData();
}

function inscNextPage() {
    inscState.offset += inscState.limit;
    inscFetchData();
}

function inscPrevPage() {
    inscState.offset = Math.max(0, inscState.offset - inscState.limit);
    inscFetchData();
}

function inscUpdateToolbarButtons() {
    const isHome = inscState.view === 'home';
    const isErrors = inscState.view === 'errors';
    const isCpfInscrito = inscState.view === 'cpf_inscrito';
    
    document.getElementById('insc-btnErrors').style.display = isHome ? '' : 'none';
    document.getElementById('insc-btnCpfInscrito').style.display = isHome ? '' : 'none';
    document.getElementById('insc-btnHome').style.display = isHome ? 'none' : '';
    
    document.getElementById('insc-btnErrors').classList.toggle('active', isErrors);
    document.getElementById('insc-btnCpfInscrito').classList.toggle('active', isCpfInscrito);
}

function inscToggleOutput(id) {
    const el = document.getElementById(id);
    if (el) el.classList.toggle('open');
}

// ---------------------------------------------------------------------------
// Render
// ---------------------------------------------------------------------------
function inscRender() {
    if (!inscState.data) return;
    inscRenderViewIndicator();
    if (inscState.view === 'home') {
        inscRenderHome();
    } else if (inscState.view === 'cpf_inscrito') {
        inscRenderCpfInscrito();
    } else {
        inscRenderErrors();
    }
}

function inscRenderViewIndicator() {
    const d = inscState.data;
    const view = inscState.view;
    const filters = d.filters || {};
    const fromStr = inscFormatDate(filters.from);
    const toStr   = inscFormatDate(filters.to);
    const filterText = (filters.from || filters.to)
        ? `Período: ${fromStr} a ${toStr}`
        : 'Sem filtro de data';

    let badgeClass = 'home';
    let badgeContent = '<svg viewBox="0 0 24 24" fill="currentColor"><path d="M10 20v-6h4v6h5v-8h3L12 3 2 12h3v8z"/></svg> Home';
    
    if (view === 'errors') {
        badgeClass = 'errors';
        badgeContent = '<svg viewBox="0 0 24 24" fill="currentColor"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 15h-2v-2h2v2zm0-4h-2V7h2v6z"/></svg> Erros';
    } else if (view === 'cpf_inscrito') {
        badgeClass = 'cpf-inscrito';
        badgeContent = '<svg viewBox="0 0 24 24" fill="currentColor"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 15l-5-5 1.41-1.41L10 14.17l7.59-7.59L19 8l-9 9z"/></svg> CPF\'s com Inscrição';
    }

    document.getElementById('insc-viewIndicator').innerHTML = `
        <span class="insc-view-badge ${badgeClass}">
            ${badgeContent}
        </span>
        <span class="insc-filter-display">${filterText}</span>
    `;
}

function inscRenderHome() {
    const d = inscState.data;
    const m = d.metrics || {};
    const tipos = d.tipo_inscricao || [];
    const maxTotal = tipos.length > 0 ? Math.max(...tipos.map(t => t.total || 0)) : 1;

    let tiposHTML = '';
    if (tipos.length === 0) {
        tiposHTML = '<div style="padding:12px;color:var(--insc-text-muted);font-size:14px;">Nenhum tipo encontrado.</div>';
    } else {
        tiposHTML = tipos.map(t => {
            const pct = maxTotal > 0 ? ((t.total || 0) / maxTotal) * 100 : 0;
            return `
            <div class="insc-tipo-item">
                <span class="insc-tipo-name">${inscSafe(t.tipo_inscricao)}</span>
                <div class="insc-tipo-bar-container">
                    <div class="insc-tipo-bar" style="width:${pct}%"></div>
                </div>
                <span class="insc-tipo-total">${inscSafe(t.total)}</span>
            </div>`;
        }).join('');
    }

    document.getElementById('insc-contentArea').innerHTML = `
        <div class="insc-cards-grid">
            <div class="insc-metric-card card-time insc-animate-in">
                <div class="insc-card-header">
                    <div class="insc-card-icon">
                        <svg viewBox="0 0 24 24"><path d="M11.99 2C6.47 2 2 6.48 2 12s4.47 10 9.99 10C17.52 22 22 17.52 22 12S17.52 2 11.99 2zM12 20c-4.42 0-8-3.58-8-8s3.58-8 8-8 8 3.58 8 8-3.58 8-8 8zm.5-13H11v6l5.25 3.15.75-1.23-4.5-2.67V7z"/></svg>
                    </div>
                    <span class="insc-card-label">Tempo Médio de Execução</span>
                </div>
                <div class="insc-card-value">${inscSafe(m.avg_execution_hhmmss)}</div>
                <div class="insc-card-sub">${m.avg_execution_seconds != null ? m.avg_execution_seconds + ' segundos' : ''}</div>
            </div>

            <div class="insc-metric-card card-exec insc-animate-in">
                <div class="insc-card-header">
                    <div class="insc-card-icon">
                        <svg viewBox="0 0 24 24"><path d="M19 3H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zm-7 14l-5-5 1.41-1.41L12 14.17l7.59-7.59L21 8l-9 9z"/></svg>
                    </div>
                    <span class="insc-card-label">Execuções Consideradas</span>
                </div>
                <div class="insc-card-value">${inscSafe(m.total_com_inicio_fim)}</div>
                <div class="insc-card-sub">com início e fim registrados</div>
            </div>

            <div class="insc-metric-card card-types insc-animate-in">
                <div class="insc-card-header">
                    <div class="insc-card-icon">
                        <svg viewBox="0 0 24 24"><path d="M3 13h8V3H3v10zm0 8h8v-6H3v6zm10 0h8V11h-8v10zm0-18v6h8V3h-8z"/></svg>
                    </div>
                    <span class="insc-card-label">Tipo de Inscrição</span>
                </div>
                <div class="insc-tipos-list">
                    ${tiposHTML}
                </div>
            </div>
        </div>
    `;
}

function inscIsCpfJaInscrito(row) {
    const msg = (row.erro_mensagem || '').toLowerCase();
    const output = (row.output || '').toLowerCase();
    const combined = msg + ' ' + output;
    
    const patterns = [
        'cpf já possui inscrição',
        'cpf ja possui inscricao',
        'já possui inscrição',
        'ja possui inscricao',
        'cpf já possui inscri',
        'possui inscrição',
        'cpf já cadastrado',
        'cpf ja cadastrado',
        'já está inscrito',
        'ja esta inscrito',
        'inscrição já existe',
        'inscricao ja existe',
        'cpf already',
        'already registered',
        'já inscrito',
        'ja inscrito'
    ];
    
    return patterns.some(p => combined.includes(p));
}

function inscRenderCpfInscrito() {
    const d = inscState.data;
    const allRows = d.rows || [];
    const rows = allRows.filter(r => inscIsCpfJaInscrito(r));
    const totalReturned = rows.length;
    const pagination = d.pagination || {};
    const limit  = pagination.limit  != null ? pagination.limit  : inscState.limit;
    const offset = pagination.offset != null ? pagination.offset : inscState.offset;

    let rowsHTML = '';
    if (rows.length === 0) {
        rowsHTML = inscRenderEmptyState('Nenhum CPF com inscrição', 'Não há registros de CPF que já possuem inscrição neste período.');
    } else {
        rowsHTML = rows.map((r, i) => {
            const uid = `insc-cpf-output-${offset}-${i}`;
            const endTimeRaw = inscGetEndTime(r);
            const endTimeFormatted = inscFormatDateTime(endTimeRaw);
            return `
            <div class="insc-error-card insc-cpf-card insc-animate-in">
                <div class="insc-error-card-header" style="background: linear-gradient(180deg, #f0fdf4 0%, white 100%);">
                    <span class="insc-error-id" title="${inscSafe(r.execution_id)}">${inscSafe(r.execution_id)}</span>
                    <div class="insc-error-card-header-right">
                        ${endTimeFormatted ? `
                        <span class="insc-error-timestamp" title="Fim da execução">
                            <svg viewBox="0 0 24 24"><path d="M11.99 2C6.47 2 2 6.48 2 12s4.47 10 9.99 10C17.52 22 22 17.52 22 12S17.52 2 11.99 2zM12 20c-4.42 0-8-3.58-8-8s3.58-8 8-8 8 3.58 8 8-3.58 8-8 8zm.5-13H11v6l5.25 3.15.75-1.23-4.5-2.67V7z"/></svg>
                            ${endTimeFormatted}
                        </span>` : ''}
                        <span class="insc-error-etapa" style="background: #dcfce7; color: #16a34a;">${inscSafe(r.etapa_erro, 'Etapa desconhecida')}</span>
                    </div>
                </div>
                <div class="insc-error-card-body">
                    <div class="insc-error-msg-label">Mensagem</div>
                    <div class="insc-error-msg" style="color: #16a34a;">${inscSafe(r.erro_mensagem, 'Sem mensagem')}</div>
                    ${r.output ? `
                        <button class="insc-error-output-toggle" onclick="inscToggleOutput('${uid}')">
                            <svg viewBox="0 0 24 24" fill="currentColor"><path d="M9.4 16.6L4.8 12l4.6-4.6L8 6l-6 6 6 6 1.4-1.4zm5.2 0l4.6-4.6-4.6-4.6L16 6l6 6-6 6-1.4-1.4z"/></svg>
                            Ver output completo
                        </button>
                        <div class="insc-error-output" id="${uid}">${inscEscapeHtml(r.output)}</div>
                    ` : ''}
                </div>
            </div>`;
        }).join('');
    }

    document.getElementById('insc-contentArea').innerHTML = `
        <div class="insc-errors-header">
            <div class="insc-errors-stats">
                <span class="insc-stat-chip" style="background: #f0fdf4; border-color: #bbf7d0; color: #16a34a;">CPF's com inscrição: <strong>&nbsp;${inscSafe(totalReturned)}</strong></span>
            </div>
        </div>
        <div class="insc-errors-list">
            ${rowsHTML}
        </div>
    `;
}

function inscRenderErrors() {
    const d = inscState.data;
    const allRows = d.rows || [];
    const rows = allRows.filter(r => !inscIsCpfJaInscrito(r));
    const totalReturned = rows.length;
    const pagination = d.pagination || {};
    const limit  = pagination.limit  != null ? pagination.limit  : inscState.limit;
    const offset = pagination.offset != null ? pagination.offset : inscState.offset;

    let rowsHTML = '';
    if (rows.length === 0) {
        rowsHTML = inscRenderEmptyState('Nenhum erro encontrado', 'Ótima notícia! Não há erros registrados neste período.');
    } else {
        rowsHTML = rows.map((r, i) => {
            const uid = `insc-output-${offset}-${i}`;
            const endTimeRaw = inscGetEndTime(r);
            const endTimeFormatted = inscFormatDateTime(endTimeRaw);
            return `
            <div class="insc-error-card insc-animate-in">
                <div class="insc-error-card-header">
                    <span class="insc-error-id" title="${inscSafe(r.execution_id)}">${inscSafe(r.execution_id)}</span>
                    <div class="insc-error-card-header-right">
                        ${endTimeFormatted ? `
                        <span class="insc-error-timestamp" title="Fim da execução">
                            <svg viewBox="0 0 24 24"><path d="M11.99 2C6.47 2 2 6.48 2 12s4.47 10 9.99 10C17.52 22 22 17.52 22 12S17.52 2 11.99 2zM12 20c-4.42 0-8-3.58-8-8s3.58-8 8-8 8 3.58 8 8-3.58 8-8 8zm.5-13H11v6l5.25 3.15.75-1.23-4.5-2.67V7z"/></svg>
                            ${endTimeFormatted}
                        </span>` : ''}
                        <span class="insc-error-etapa">${inscSafe(r.etapa_erro, 'Etapa desconhecida')}</span>
                    </div>
                </div>
                <div class="insc-error-card-body">
                    <div class="insc-error-msg-label">Mensagem de erro</div>
                    <div class="insc-error-msg">${inscSafe(r.erro_mensagem, 'Sem mensagem')}</div>
                    ${r.output ? `
                        <button class="insc-error-output-toggle" onclick="inscToggleOutput('${uid}')">
                            <svg viewBox="0 0 24 24" fill="currentColor"><path d="M9.4 16.6L4.8 12l4.6-4.6L8 6l-6 6 6 6 1.4-1.4zm5.2 0l4.6-4.6-4.6-4.6L16 6l6 6-6 6-1.4-1.4z"/></svg>
                            Ver output completo
                        </button>
                        <div class="insc-error-output" id="${uid}">${inscEscapeHtml(r.output)}</div>
                    ` : ''}
                </div>
            </div>`;
        }).join('');
    }

    const hasPrev = offset > 0;
    const hasNext = rows.length >= limit;

    document.getElementById('insc-contentArea').innerHTML = `
        <div class="insc-errors-header">
            <div class="insc-errors-stats">
                <span class="insc-stat-chip">Total retornado: <strong>&nbsp;${inscSafe(totalReturned)}</strong></span>
                <span class="insc-stat-chip">Limit: <strong>&nbsp;${inscSafe(limit)}</strong></span>
                <span class="insc-stat-chip">Offset: <strong>&nbsp;${inscSafe(offset)}</strong></span>
            </div>
        </div>
        <div class="insc-errors-list">
            ${rowsHTML}
        </div>
        <div class="insc-pagination">
            <button class="insc-btn insc-btn-outline" onclick="inscPrevPage()" ${hasPrev ? '' : 'disabled'}>
                <svg viewBox="0 0 24 24" fill="currentColor" style="width:16px;height:16px"><path d="M15.41 7.41L14 6l-6 6 6 6 1.41-1.41L10.83 12z"/></svg>
                Anterior
            </button>
            <span class="insc-pagination-info">Página ${Math.floor(offset / limit) + 1}</span>
            <button class="insc-btn insc-btn-outline" onclick="inscNextPage()" ${hasNext ? '' : 'disabled'}>
                Próximo
                <svg viewBox="0 0 24 24" fill="currentColor" style="width:16px;height:16px"><path d="M10 6L8.59 7.41 13.17 12l-4.58 4.59L10 18l6-6z"/></svg>
            </button>
        </div>
    `;
}

function inscRenderEmptyState(title, text) {
    return `
        <div class="insc-empty-state">
            <svg viewBox="0 0 24 24"><path d="M20 6h-8l-2-2H4c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V8c0-1.1-.9-2-2-2zm-1 12H5c-.55 0-1-.45-1-1V9c0-.55.45-1 1-1h14c.55 0 1 .45 1 1v8c0 .55-.45 1-1 1z"/></svg>
            <h3>${title}</h3>
            <p>${text}</p>
        </div>
    `;
}

function inscEscapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// ---------------------------------------------------------------------------
// Init (called by navigate)
// ---------------------------------------------------------------------------
function loadInscricao() {
    if (!inscState.initialized) {
        const now = new Date();
        const to = now.toISOString().split('T')[0];
        const from = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];

        document.getElementById('insc-inputFrom').value = from;
        document.getElementById('insc-inputTo').value = to;

        inscState.from = from;
        inscState.to = to;
        inscState.initialized = true;
    }

    inscUpdateToolbarButtons();
    inscFetchData();
}
