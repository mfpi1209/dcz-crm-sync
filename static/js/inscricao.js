// ---------------------------------------------------------------------------
// Inscrição Automática — Dashboard
// ---------------------------------------------------------------------------
const INSC_API_BASE = '/api/inscricao';

const inscState = {
    view: 'home',
    from: '',
    to: '',
    limit: 200,
    offset: 0,
    q: '',
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
    if (inscState.view === 'search' && inscState.q) {
        params.set('q', inscState.q);
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
        const json = await res.json();
        if (!res.ok || (json && json.error)) {
            throw new Error((json && json.error) || `HTTP ${res.status}`);
        }
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
    const el = document.getElementById('insc-inputSearch');
    inscState.q = (el && el.value || '').trim();
    const digits = inscState.q.replace(/\D/g, '');
    if (digits.length >= 8) {
        inscState.view = 'search';
    } else {
        if (inscState.q) {
            inscShowError('Informe um CPF ou telefone com pelo menos 8 dígitos, ou deixe o campo vazio para filtrar por data.');
            return;
        }
        inscState.view = 'home';
        inscState.q = '';
        inscState.offset = 0;
    }
    inscUpdateToolbarButtons();
    inscFetchData();
}

function inscSwitchToErrors() {
    inscSyncFilters();
    inscState.view = 'errors';
    inscState.offset = 0;
    inscUpdateToolbarButtons();
    inscFetchData();
}

function inscSwitchToHome() {
    inscSyncFilters();
    inscState.view = 'home';
    inscState.q = '';
    const el = document.getElementById('insc-inputSearch');
    if (el) el.value = '';
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
    document.getElementById('insc-btnErrors').style.display = isHome ? '' : 'none';
    document.getElementById('insc-btnHome').style.display   = isHome ? 'none' : '';
    document.getElementById('insc-btnErrors').classList.toggle('active', inscState.view === 'errors');
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
    } else if (inscState.view === 'search') {
        inscRenderSearch();
    } else {
        inscRenderErrors();
    }
}

function inscRenderViewIndicator() {
    const d = inscState.data;
    const isHome = inscState.view === 'home';
    const filters = d.filters || {};
    const fromStr = inscFormatDate(filters.from);
    const toStr   = inscFormatDate(filters.to);
    const filterText = inscState.view === 'search'
        ? `Busca: ${inscSafe(d.q || inscState.q)}`
        : ((filters.from || filters.to)
            ? `Período: ${fromStr} a ${toStr}`
            : 'Sem filtro de data');

    document.getElementById('insc-viewIndicator').innerHTML = `
        <span class="insc-view-badge ${isHome ? 'home' : 'errors'}">
            ${isHome
                ? '<svg viewBox="0 0 24 24" fill="currentColor"><path d="M10 20v-6h4v6h5v-8h3L12 3 2 12h3v8z"/></svg> Home'
                : (inscState.view === 'search'
                    ? '<svg viewBox="0 0 24 24" fill="currentColor"><path d="M15.5 14h-.79l-.28-.27A6.47 6.47 0 0016 9.5 6.5 6.5 0 109.5 16c1.61 0 3.09-.59 4.23-1.57l.27.28v.79l5 4.99L20.49 19l-4.99-5zm-6 0C7.01 14 5 11.99 5 9.5S7.01 5 9.5 5 14 7.01 14 9.5 11.99 14 9.5 14z"/></svg> Busca'
                    : '<svg viewBox="0 0 24 24" fill="currentColor"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 15h-2v-2h2v2zm0-4h-2V7h2v6z"/></svg> Erros')
            }
        </span>
        <span class="insc-filter-display">${filterText}</span>
    `;
}

function inscBars(items, nameKey) {
    const maxTotal = items.length > 0 ? Math.max(...items.map(t => t.total || 0)) : 1;
    if (!items.length) {
        return '<div style="padding:12px;color:var(--insc-text-muted);font-size:14px;">Nenhum item.</div>';
    }
    return items.map(t => {
        const pct = maxTotal > 0 ? ((t.total || 0) / maxTotal) * 100 : 0;
        return `
            <div class="insc-tipo-item">
                <span class="insc-tipo-name">${inscSafe(t[nameKey])}</span>
                <div class="insc-tipo-bar-container">
                    <div class="insc-tipo-bar" style="width:${pct}%"></div>
                </div>
                <span class="insc-tipo-total">${inscSafe(t.total)}</span>
            </div>`;
    }).join('');
}

function inscMetaGrid(r) {
    const pairs = [
        ['Curso', r.curso],
        ['Polo', r.polo],
        ['Km polo', r.polo_km],
        ['Forma', r.forma_ingresso || r.tipo_inscricao],
        ['Departamento', r.department],
        ['CPF', r.cpf],
        ['Telefone', r.telefone],
        ['E-mail', r.email],
        ['Lead', r.lead_id],
        ['Pedido', r.order_id],
        ['Inscrição SIAA', r.inscricao_siaa],
        ['Duração', r.duration_ms != null ? Math.round(r.duration_ms) + ' ms' : ''],
    ].filter(([, v]) => v !== null && v !== undefined && String(v).trim() !== '');
    if (!pairs.length) return '';
    return `<div class="insc-meta-grid">${pairs.map(([k, v]) =>
        `<div class="insc-meta-item"><span>${k}</span><strong>${inscSafe(v)}</strong></div>`
    ).join('')}</div>`;
}

function inscRenderHome() {
    const d = inscState.data;
    const m = d.metrics || {};
    const tipos = d.tipo_inscricao || [];
    const depts = d.department || [];
    const recent = d.recent || [];
    const avgSec = m.avg_execution_seconds;
    const avgLabel = avgSec != null ? (Math.round(avgSec * 10) / 10) + ' segundos' : '';

    const recentHTML = recent.length === 0
        ? '<div style="padding:12px;color:var(--insc-text-muted);font-size:14px;">Nenhum registro no período.</div>'
        : recent.map(r => `
            <div class="insc-recent-row insc-animate-in">
                <span class="insc-recent-cpf">${inscSafe(r.cpf, 'sem CPF')}</span>
                <span class="insc-recent-msg">${r.ok ? 'Inscrição concluída' : inscSafe(r.error_message || r.erro_mensagem, 'Erro')}</span>
                <span class="insc-error-etapa" style="${r.ok ? 'color:#047857;background:#d1fae5' : ''}">${r.ok ? 'OK' : inscSafe(r.error_code || r.etapa_erro, 'Erro')}</span>
            </div>`).join('');

    document.getElementById('insc-contentArea').innerHTML = `
        <div class="insc-cards-grid">
            <div class="insc-metric-card card-time insc-animate-in">
                <div class="insc-card-header">
                    <div class="insc-card-icon">
                        <svg viewBox="0 0 24 24"><path d="M11.99 2C6.47 2 2 6.48 2 12s4.47 10 9.99 10C17.52 22 22 17.52 22 12S17.52 2 11.99 2zM12 20c-4.42 0-8-3.58-8-8s3.58-8 8-8 8 3.58 8 8-3.58 8-8 8zm.5-13H11v6l5.25 3.15.75-1.23-4.5-2.67V7z"/></svg>
                    </div>
                    <span class="insc-card-label">Tempo Médio</span>
                </div>
                <div class="insc-card-value">${inscSafe(m.avg_execution_hhmmss)}</div>
                <div class="insc-card-sub">${avgLabel}</div>
            </div>
            <div class="insc-metric-card card-exec insc-animate-in">
                <div class="insc-card-header">
                    <div class="insc-card-icon">
                        <svg viewBox="0 0 24 24"><path d="M19 3H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zm-7 14l-5-5 1.41-1.41L12 14.17l7.59-7.59L21 8l-9 9z"/></svg>
                    </div>
                    <span class="insc-card-label">Registros</span>
                </div>
                <div class="insc-card-value">${inscSafe(m.total)}</div>
                <div class="insc-card-sub">${inscSafe(m.total_ok)} ok · ${inscSafe(m.total_erro)} erro</div>
            </div>
            <div class="insc-metric-card card-types insc-animate-in">
                <div class="insc-card-header">
                    <div class="insc-card-icon">
                        <svg viewBox="0 0 24 24"><path d="M3 13h8V3H3v10zm0 8h8v-6H3v6zm10 0h8V11h-8v10zm0-18v6h8V3h-8z"/></svg>
                    </div>
                    <span class="insc-card-label">Forma de ingresso</span>
                </div>
                <div class="insc-tipos-list">${inscBars(tipos, 'tipo_inscricao')}</div>
            </div>
            <div class="insc-metric-card card-types insc-animate-in">
                <div class="insc-card-header">
                    <div class="insc-card-icon">
                        <svg viewBox="0 0 24 24"><path d="M12 7V3H2v18h20V7H12zM6 19H4v-2h2v2zm0-4H4v-2h2v2zm0-4H4V9h2v2zm0-4H4V5h2v2zm4 12H8v-2h2v2zm0-4H8v-2h2v2zm0-4H8V9h2v2zm0-4H8V5h2v2zm10 12h-8v-2h2v-2h-2v-2h2v-2h-2V9h8v10z"/></svg>
                    </div>
                    <span class="insc-card-label">Departamento</span>
                </div>
                <div class="insc-tipos-list">${inscBars(depts, 'department')}</div>
            </div>
        </div>
        <div class="insc-errors-header" style="margin-top:28px">
            <div class="insc-errors-stats">
                <span class="insc-stat-chip">Últimos registros</span>
            </div>
        </div>
        <div class="insc-errors-list">${recentHTML}</div>
    `;
}

function inscRenderPersonCards(rows, emptyTitle, emptyText) {
    if (!rows.length) return inscRenderEmptyState(emptyTitle, emptyText);
    return rows.map(r => `
        <div class="insc-error-card insc-animate-in">
            <div class="insc-error-card-header" style="${r.ok ? 'background:linear-gradient(180deg,#ecfdf5 0%,white 100%)' : ''}">
                <span class="insc-error-id">${inscSafe(r.cpf, inscSafe(r.id))}</span>
                <div class="insc-error-card-header-right">
                    ${inscFormatDateTime(r.created_at) ? `<span class="insc-error-timestamp">${inscFormatDateTime(r.created_at)}</span>` : ''}
                    <span class="insc-error-etapa" style="${r.ok ? 'color:#047857;background:#d1fae5' : ''}">${r.ok ? 'OK' : inscSafe(r.error_code || r.etapa_erro, 'Erro')}</span>
                </div>
            </div>
            <div class="insc-error-card-body">
                ${r.ok ? '' : `<div class="insc-error-msg">${inscSafe(r.error_message || r.erro_mensagem, 'Sem mensagem')}</div>`}
                ${inscMetaGrid(r)}
            </div>
        </div>`).join('');
}

function inscRenderSearch() {
    const d = inscState.data;
    const rows = d.rows || [];
    document.getElementById('insc-contentArea').innerHTML = `
        <div class="insc-errors-header">
            <div class="insc-errors-stats">
                <span class="insc-stat-chip">Encontradas: <strong>&nbsp;${inscSafe(d.total_returned != null ? d.total_returned : rows.length)}</strong></span>
            </div>
        </div>
        <div class="insc-errors-list">${inscRenderPersonCards(rows, 'Nenhuma inscrição', 'Não achei CPF/telefone nesse valor.')}</div>
    `;
}

function inscRenderErrors() {
    const d = inscState.data;
    const rows = d.rows || [];
    const totalReturned = d.total_returned != null ? d.total_returned : rows.length;
    const pagination = d.pagination || {};
    const limit  = pagination.limit  != null ? pagination.limit  : inscState.limit;
    const offset = pagination.offset != null ? pagination.offset : inscState.offset;

    let rowsHTML = '';
    if (rows.length === 0) {
        rowsHTML = inscRenderEmptyState('Nenhum erro encontrado', 'Ótima notícia! Não há erros registrados neste período.');
    } else {
        rowsHTML = rows.map((r) => {
            const endTimeFormatted = inscFormatDateTime(inscGetEndTime(r) || r.created_at);
            return `
            <div class="insc-error-card insc-animate-in">
                <div class="insc-error-card-header">
                    <span class="insc-error-id" title="${inscSafe(r.execution_id || r.id)}">${inscSafe(r.execution_id || r.id)}</span>
                    <div class="insc-error-card-header-right">
                        ${endTimeFormatted ? `
                        <span class="insc-error-timestamp" title="Data">
                            <svg viewBox="0 0 24 24"><path d="M11.99 2C6.47 2 2 6.48 2 12s4.47 10 9.99 10C17.52 22 22 17.52 22 12S17.52 2 11.99 2zM12 20c-4.42 0-8-3.58-8-8s3.58-8 8-8 8 3.58 8 8-3.58 8-8 8zm.5-13H11v6l5.25 3.15.75-1.23-4.5-2.67V7z"/></svg>
                            ${endTimeFormatted}
                        </span>` : ''}
                        <span class="insc-error-etapa">${inscSafe(r.error_code || r.etapa_erro, 'Erro')}</span>
                    </div>
                </div>
                <div class="insc-error-card-body">
                    <div class="insc-error-msg-label">Mensagem de erro</div>
                    <div class="insc-error-msg">${inscSafe(r.error_message || r.erro_mensagem, 'Sem mensagem')}</div>
                    ${inscMetaGrid(r)}
                </div>
            </div>`;
        }).join('');
    }

    const hasPrev = offset > 0;
    const hasNext = rows.length >= limit;

    document.getElementById('insc-contentArea').innerHTML = `
        <div class="insc-errors-header">
            <div class="insc-errors-stats">
                <span class="insc-stat-chip">Erros: <strong>&nbsp;${inscSafe(d.total_erros != null ? d.total_erros : totalReturned)}</strong></span>
                <span class="insc-stat-chip">Nesta página: <strong>&nbsp;${inscSafe(totalReturned)}</strong></span>
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
