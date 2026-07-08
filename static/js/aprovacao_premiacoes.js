// ═══════════════════════════════════════════════════════════════════════════
// Central de Aprovacao de Premiacoes — Aprovador
// ═══════════════════════════════════════════════════════════════════════════

window.AP_STATE = window.AP_STATE || {
    currentLoteId: null,
    currentLote: null,
    currentColabs: [],
    currentEventos: [],
    pendingDecision: null,
    activeTab: 'colab',
    _reloadTimer: null,
};

function apFmtMoney(v) {
    const n = Number(v || 0);
    return n.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
}

function apFmtDate(iso) {
    if (!iso) return '';
    const d = new Date(iso);
    if (isNaN(d)) return iso;
    return d.toLocaleDateString('pt-BR') + ' ' + d.toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' });
}

function apFmtMes(mesRef) {
    if (!mesRef) return '';
    const meses = ['', 'Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez'];
    try {
        const [y, m] = mesRef.split('-');
        return `${meses[parseInt(m, 10)]}/${y}`;
    } catch { return mesRef; }
}

const AP_STATUS_LABEL = {
    aguardando_aprovacao: { label: 'Aguardando aprovação', cls: 'ap-badge-aguardando' },
    aprovado:             { label: 'Aprovado',             cls: 'ap-badge-aprovado' },
    reprovado:            { label: 'Reprovado',            cls: 'ap-badge-reprovado' },
    ajuste_solicitado:    { label: 'Ajuste solicitado',    cls: 'ap-badge-ajuste' },
};

function apStatusBadge(status) {
    const info = AP_STATUS_LABEL[status] || { label: status, cls: 'ap-badge-aguardando' };
    return `<span class="ap-badge ${info.cls}">${esc(info.label)}</span>`;
}

// ---------------------------------------------------------------------------
// Init / reload
// ---------------------------------------------------------------------------

function loadAprovacaoPremiacoes() {
    apReloadLotes();
}

function apDebouncedReload() {
    clearTimeout(window.AP_STATE._reloadTimer);
    window.AP_STATE._reloadTimer = setTimeout(apReloadLotes, 350);
}

async function apReloadLotes() {
    const mes = (document.getElementById('ap-filter-mes')?.value || '').trim();
    const setor = (document.getElementById('ap-filter-setor')?.value || '').trim();
    const q = (document.getElementById('ap-filter-q')?.value || '').trim();
    const incluir = document.getElementById('ap-filter-decididos')?.checked;

    const params = new URLSearchParams();
    if (mes) params.set('mes', mes);
    if (setor) params.set('setor', setor);
    if (q) params.set('q', q);
    if (incluir) params.set('incluir_decididos', '1');

    const tbody = document.getElementById('ap-tbody');
    const empty = document.getElementById('ap-empty');
    if (!tbody) return;
    tbody.innerHTML = `<tr><td colspan="7" class="text-center py-8" style="color: var(--text-secondary)">Carregando…</td></tr>`;
    if (empty) empty.classList.add('hidden');

    try {
        const res = await api('/api/premiacoes-internas/aprovacao/pendentes?' + params.toString());
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || 'Falha ao carregar');

        apRenderKpis(data.kpis || {});
        const lotes = data.lotes || [];
        if (!lotes.length) {
            tbody.innerHTML = '';
            if (empty) empty.classList.remove('hidden');
            return;
        }
        tbody.innerHTML = lotes.map(l => apRenderRow(l)).join('');
    } catch (e) {
        console.error('apReloadLotes', e);
        tbody.innerHTML = `<tr><td colspan="7" class="text-center py-8 text-red-500">${esc(e.message || 'Erro')}</td></tr>`;
    }
}

function apRenderKpis(k) {
    const set = (id, val) => {
        const el = document.getElementById(id);
        if (el) el.textContent = val;
    };
    set('ap-kpi-pendentes', String(k.pendentes ?? 0));
    set('ap-kpi-aprovadas', String(k.aprovadas_hoje ?? 0));
    set('ap-kpi-reprovadas', String(k.reprovadas_hoje ?? 0));
    set('ap-kpi-ajustes', String(k.ajustes_hoje ?? 0));
    set('ap-kpi-valor-pendente', `${apFmtMoney(k.valor_pendente || 0)} em análise`);
}

function apRenderRow(l) {
    return `<tr onclick="apOpenModal(${l.id})">
        <td>${esc(apFmtMes(l.mes_referencia))}</td>
        <td class="font-medium">${esc(l.gestor_nome || '')}</td>
        <td>${esc(l.setor || '')}</td>
        <td>—</td>
        <td class="font-semibold">${apFmtMoney(l.valor_total)}</td>
        <td>${esc(apFmtDate(l.enviado_em || l.updated_at))}</td>
        <td>${apStatusBadge(l.status)}</td>
    </tr>`;
}

// ---------------------------------------------------------------------------
// Modal
// ---------------------------------------------------------------------------

async function apOpenModal(loteId) {
    const modal = document.getElementById('ap-modal');
    if (!modal) return;
    if (modal.parentNode !== document.body) {
        document.body.appendChild(modal);
    }
    document.body.style.overflow = 'hidden';
    modal.classList.remove('hidden');

    window.AP_STATE.currentLoteId = loteId;
    window.AP_STATE.pendingDecision = null;
    window.AP_STATE.activeTab = 'colab';

    document.getElementById('ap-decision-box').classList.add('hidden');
    document.getElementById('ap-decision-justificativa').value = '';
    apSwitchTab('colab');

    document.getElementById('ap-tbody-loading');
    document.getElementById('ap-lote-header').innerHTML = '<div class="text-center py-6" style="color: var(--text-secondary)">Carregando…</div>';
    document.getElementById('ap-colab-list').innerHTML = '';
    document.getElementById('ap-hist-list').innerHTML = '';
    document.getElementById('ap-total-valor').textContent = '';
    document.getElementById('ap-decided-info').classList.add('hidden');

    try {
        const res = await api(`/api/premiacoes-internas/aprovacao/lotes/${loteId}`);
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || 'Falha ao carregar');

        window.AP_STATE.currentLote = data.lote;
        window.AP_STATE.currentColabs = data.colaboradores || [];
        window.AP_STATE.currentEventos = data.eventos || [];

        apRenderModal();
    } catch (e) {
        console.error('apOpenModal', e);
        toast(e.message || 'Erro ao abrir lote', 'error');
        apCloseModal();
    }
}

function apMaybeCloseModal(ev) {
    if (ev && ev.target && ev.target.classList.contains('ap-modal-backdrop')) {
        apCloseModal();
    }
}

function apCloseModal() {
    const modal = document.getElementById('ap-modal');
    if (modal) modal.classList.add('hidden');
    document.body.style.overflow = '';
}

function apRenderModal() {
    const lote = window.AP_STATE.currentLote || {};
    const colabs = window.AP_STATE.currentColabs || [];
    const eventos = window.AP_STATE.currentEventos || [];

    document.getElementById('ap-modal-title').textContent = 'Revisar premiação';
    document.getElementById('ap-modal-subtitle').innerHTML =
        `${apStatusBadge(lote.status)} <span style="color: var(--text-secondary)">· ${esc(apFmtMes(lote.mes_referencia))} · ${esc(lote.setor || '')}</span>`;

    // Header do lote
    document.getElementById('ap-lote-header').innerHTML = `<div class="grid grid-cols-1 sm:grid-cols-3 gap-3 mb-3">
        <div class="ap-colab-card">
            <div class="ap-label">Gestor</div>
            <div class="text-sm font-semibold">${esc(lote.gestor_nome || '')}</div>
        </div>
        <div class="ap-colab-card">
            <div class="ap-label">Setor</div>
            <div class="text-sm font-semibold">${esc(lote.setor || '')}</div>
        </div>
        <div class="ap-colab-card">
            <div class="ap-label">Mês</div>
            <div class="text-sm font-semibold">${esc(apFmtMes(lote.mes_referencia))}</div>
        </div>
    </div>
    ${lote.observacoes_gerais ? `<div class="ap-colab-card mb-3"><div class="ap-label">Observações do gestor</div><div class="text-sm whitespace-pre-wrap" style="color: var(--text-primary)">${esc(lote.observacoes_gerais)}</div></div>` : ''}`;

    // Colaboradores
    document.getElementById('ap-colab-list').innerHTML = colabs.map((c, i) => `<div class="ap-colab-card">
        <div class="flex items-start justify-between gap-3">
            <div class="min-w-0 flex-1">
                <div class="flex items-center gap-2 mb-0.5">
                    <div class="font-semibold" style="color: var(--text-primary)">${esc(c.nome || '')}</div>
                    ${c.is_auto_premiacao ? '<span class="ap-badge ap-badge-ajuste" style="font-size:.55rem">Auto-premiação</span>' : ''}
                </div>
                ${c.email ? `<div class="text-[11px] mb-0.5" style="color: var(--text-secondary)">${esc(c.email)}</div>` : ''}
                <div class="text-xs" style="color: var(--text-secondary)">${esc(c.cargo || '')} · ${esc(c.setor || '')}</div>
            </div>
            <div class="text-right whitespace-nowrap">
                <div class="text-lg font-bold" style="color: var(--primary)">${apFmtMoney(c.valor)}</div>
            </div>
        </div>
        ${c.justificativa ? `<div class="mt-2 text-xs" style="color: var(--text-primary)"><div class="ap-label" style="margin-bottom:.15rem">Justificativa</div><div class="whitespace-pre-wrap">${esc(c.justificativa)}</div></div>` : ''}
        ${c.observacoes ? `<div class="mt-2 text-xs" style="color: var(--text-secondary)"><div class="ap-label" style="margin-bottom:.15rem">Observações</div><div class="whitespace-pre-wrap">${esc(c.observacoes)}</div></div>` : ''}
    </div>`).join('');

    // Total
    const total = colabs.reduce((acc, c) => acc + Number(c.valor || 0), 0);
    document.getElementById('ap-total-valor').textContent = apFmtMoney(total);

    // Historico
    const TIPO_LABEL = {
        criado: 'Criado como rascunho',
        editado: 'Editado',
        enviado: 'Enviado para aprovação',
        reenviado: 'Reenviado para aprovação',
        aprovado: 'Aprovado',
        reprovado: 'Reprovado',
        ajuste_solicitado: 'Ajuste solicitado',
    };
    document.getElementById('ap-hist-list').innerHTML = eventos.map(ev => `<div class="ap-timeline-item">
        <div class="ap-timeline-dot"></div>
        <div class="text-sm font-semibold" style="color: var(--text-primary)">${esc(TIPO_LABEL[ev.tipo] || ev.tipo)}</div>
        <div class="text-xs" style="color: var(--text-secondary)">${esc(ev.autor_nome)} · ${esc(apFmtDate(ev.created_at))}</div>
        ${ev.justificativa ? `<div class="text-xs mt-1 p-2 rounded" style="background: var(--bg-elevated); color: var(--text-primary); white-space:pre-wrap">${esc(ev.justificativa)}</div>` : ''}
    </div>`).join('');

    // Acoes: mostra ou oculta baseado no status
    const isPending = lote.status === 'aguardando_aprovacao';
    const actions = document.getElementById('ap-modal-actions');
    if (actions) actions.style.display = isPending ? '' : 'none';
    const hint = document.getElementById('ap-modal-footer-hint');
    if (hint) {
        hint.textContent = isPending
            ? 'Escolha uma ação para o lote.'
            : `Este lote já foi decidido em ${apFmtDate(lote.decidido_em)}.`;
    }

    // Info de decisao (se ja decidido)
    if (!isPending && (lote.aprovador_nome || lote.aprovador_justificativa)) {
        const box = document.getElementById('ap-decided-info');
        box.classList.remove('hidden');
        box.innerHTML = `<div class="text-xs font-semibold" style="color: var(--text-secondary)">Decisão registrada</div>
            <div class="text-sm font-medium mt-0.5">${esc(lote.aprovador_nome || '—')} · ${apStatusBadge(lote.status)}</div>
            ${lote.aprovador_justificativa ? `<div class="text-xs mt-1 whitespace-pre-wrap" style="color: var(--text-primary)">${esc(lote.aprovador_justificativa)}</div>` : ''}`;
    }
}

function apSwitchTab(tab) {
    window.AP_STATE.activeTab = tab;
    document.querySelectorAll('#ap-modal .ap-tab').forEach(el => {
        el.classList.toggle('is-active', el.dataset.tab === tab);
    });
    document.getElementById('ap-tab-colab').classList.toggle('hidden', tab !== 'colab');
    document.getElementById('ap-tab-hist').classList.toggle('hidden', tab !== 'hist');
}

// ---------------------------------------------------------------------------
// Decisao
// ---------------------------------------------------------------------------

function apRequestDecision(decisao) {
    window.AP_STATE.pendingDecision = decisao;

    const box = document.getElementById('ap-decision-box');
    const title = document.getElementById('ap-decision-title');
    const btn = document.getElementById('ap-decision-confirm-btn');
    const ta = document.getElementById('ap-decision-justificativa');
    if (!box || !title || !btn || !ta) return;

    // Aprovar nao precisa de justificativa mas oferece campo opcional
    if (decisao === 'aprovado') {
        title.textContent = 'Confirmar aprovação';
        ta.placeholder = 'Comentário (opcional)';
        btn.className = 'ap-btn ap-btn-success';
        btn.textContent = 'Confirmar aprovação';
    } else if (decisao === 'reprovado') {
        title.textContent = 'Justificativa da reprovação *';
        ta.placeholder = 'Explique por que este lote está sendo reprovado (obrigatório)…';
        btn.className = 'ap-btn ap-btn-danger';
        btn.textContent = 'Confirmar reprovação';
    } else if (decisao === 'ajuste_solicitado') {
        title.textContent = 'Justificativa do ajuste *';
        ta.placeholder = 'Descreva os ajustes solicitados (obrigatório)…';
        btn.className = 'ap-btn ap-btn-warn';
        btn.textContent = 'Solicitar ajustes';
    }
    ta.value = '';
    box.classList.remove('hidden');
    setTimeout(() => ta.focus(), 60);
}

function apCancelDecision() {
    window.AP_STATE.pendingDecision = null;
    document.getElementById('ap-decision-box').classList.add('hidden');
}

async function apConfirmDecision() {
    const decisao = window.AP_STATE.pendingDecision;
    const loteId = window.AP_STATE.currentLoteId;
    if (!decisao || !loteId) return;

    const justificativa = (document.getElementById('ap-decision-justificativa')?.value || '').trim();
    if ((decisao === 'reprovado' || decisao === 'ajuste_solicitado') && !justificativa) {
        toast('Justificativa é obrigatória.', 'warning');
        return;
    }

    try {
        const res = await api(`/api/premiacoes-internas/aprovacao/lotes/${loteId}/decidir`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ decisao, justificativa }),
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || 'Falha ao registrar decisão');

        const msg = {
            aprovado: 'Premiação aprovada.',
            reprovado: 'Premiação reprovada.',
            ajuste_solicitado: 'Ajuste solicitado ao gestor.',
        }[decisao] || 'Decisão registrada.';
        toast(msg, 'success');

        apCloseModal();
        apReloadLotes();
    } catch (e) {
        console.error('apConfirmDecision', e);
        toast(e.message || 'Erro ao registrar decisão', 'error');
    }
}
