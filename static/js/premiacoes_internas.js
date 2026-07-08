// ═══════════════════════════════════════════════════════════════════════════
// Premiacoes Internas — Gestor
// ═══════════════════════════════════════════════════════════════════════════

// Setores validos — dropdown fechado. Manter em sincronia com SETORES_VALIDOS
// em routes/premiacoes_internas.py.
const PI_SETORES = ['Acadêmico', 'Comercial', 'TI', 'Marketing'];

// Estado atual do modal
window.PI_STATE = window.PI_STATE || {
    editingId: null,          // id do lote sendo editado (null = novo)
    editable: true,           // se o modal permite edicao
    loteCache: null,          // lote atual (para historico/aprovador)
    colaboradores: [],        // draft de colaboradores no modal
    _reloadTimer: null,       // debounce
    usuariosCache: null,      // [{id, username, email, categoria}] cache lookup
    _usuariosPromise: null,   // in-flight fetch dedupe
    meCache: null,            // { user_id, username } do gestor logado
    _mePromise: null,
};

async function piLoadMe(force) {
    if (!force && window.PI_STATE.meCache) return window.PI_STATE.meCache;
    if (window.PI_STATE._mePromise) return window.PI_STATE._mePromise;
    window.PI_STATE._mePromise = (async () => {
        try {
            const res = await api('/api/me');
            const d = await res.json();
            if (res.ok) {
                window.PI_STATE.meCache = d;
                return d;
            }
        } catch (e) { console.error('piLoadMe', e); }
        return null;
    })();
    try { return await window.PI_STATE._mePromise; }
    finally { window.PI_STATE._mePromise = null; }
}

async function piLoadUsuarios(force) {
    if (!force && Array.isArray(window.PI_STATE.usuariosCache)) {
        return window.PI_STATE.usuariosCache;
    }
    if (window.PI_STATE._usuariosPromise) return window.PI_STATE._usuariosPromise;
    window.PI_STATE._usuariosPromise = (async () => {
        try {
            const res = await api('/api/premiacoes-internas/usuarios-disponiveis');
            const data = await res.json();
            if (res.ok) {
                window.PI_STATE.usuariosCache = data.usuarios || [];
                piRenderUsuariosDatalist();
                return window.PI_STATE.usuariosCache;
            }
        } catch (e) {
            console.error('piLoadUsuarios', e);
        }
        return [];
    })();
    try {
        return await window.PI_STATE._usuariosPromise;
    } finally {
        window.PI_STATE._usuariosPromise = null;
    }
}

function piRenderUsuariosDatalist() {
    let dl = document.getElementById('pi-users-datalist');
    if (!dl) {
        dl = document.createElement('datalist');
        dl.id = 'pi-users-datalist';
        document.body.appendChild(dl);
    }
    const users = window.PI_STATE.usuariosCache || [];
    // <option value="username" label="email · categoria"> — Chrome/Edge exibem
    // o label ao lado. Firefox ignora label mas o valor (username) e unico.
    dl.innerHTML = users.map(u => {
        const parts = [];
        if (u.email) parts.push(u.email);
        if (u.categoria) parts.push(u.categoria);
        const label = parts.join(' · ');
        return `<option value="${esc(u.username)}"${label ? ` label="${esc(label)}"` : ''}></option>`;
    }).join('');
}

function piFindUsuarioByUsername(username) {
    if (!username) return null;
    const u = (username || '').trim().toLowerCase();
    if (!u) return null;
    const cache = window.PI_STATE.usuariosCache || [];
    return cache.find(x => (x.username || '').toLowerCase() === u) || null;
}

function piFindUsuarioById(id) {
    if (!id) return null;
    const cache = window.PI_STATE.usuariosCache || [];
    return cache.find(x => x.id === id) || null;
}

async function piAutofillGestorLogado(idx) {
    const c = window.PI_STATE.colaboradores[idx];
    if (!c) return;

    // Garante que o cache de /api/me e /usuarios-disponiveis esta pronto.
    const [me, users] = await Promise.all([piLoadMe(), piLoadUsuarios()]);
    if (!me || !me.user_id) {
        toast('Não foi possível identificar o gestor logado.', 'warning');
        return;
    }
    const match = piFindUsuarioById(me.user_id);
    if (!match) {
        // Fallback: usa dados do /api/me mesmo sem match na lista
        // (caso raro: user admin com id=0 ou lista filtrada).
        c.app_user_id = me.user_id || null;
        c.nome = me.username || '';
        c.email = '';
        if (!c.cargo && me.categoria) c.cargo = me.categoria;
        piRenderColabList();
        return;
    }
    c.app_user_id = match.id;
    c.nome = match.username || '';
    c.email = match.email || '';
    if (!c.cargo && match.categoria) c.cargo = match.categoria;
    // Re-renderiza o card para refletir os campos autofillados
    piRenderColabList();
}

function piSetorOptions(current) {
    const cur = current || '';
    const opts = PI_SETORES.map(s => `<option value="${esc(s)}" ${s === cur ? 'selected' : ''}>${esc(s)}</option>`).join('');
    return `<option value="" ${cur ? '' : 'selected'}>Selecione…</option>${opts}`;
}

function piOnSetorLoteChange() {
    // Propaga setor do lote para colaboradores que ainda nao tem setor definido.
    const loteSetor = document.getElementById('pi-lote-setor')?.value || '';
    if (!loteSetor) return;
    let changed = false;
    window.PI_STATE.colaboradores.forEach(c => {
        if (!c.setor) { c.setor = loteSetor; changed = true; }
    });
    if (changed) piRenderColabList();
}

function piFmtMoney(v) {
    const n = Number(v || 0);
    return n.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
}

function piFmtDate(iso) {
    if (!iso) return '';
    const d = new Date(iso);
    if (isNaN(d)) return iso;
    return d.toLocaleDateString('pt-BR') + ' ' + d.toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' });
}

function piFmtMes(mesRef) {
    if (!mesRef) return '';
    const meses = ['', 'Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez'];
    try {
        const [y, m] = mesRef.split('-');
        return `${meses[parseInt(m, 10)]}/${y}`;
    } catch { return mesRef; }
}

const PI_STATUS_LABEL = {
    rascunho:             { label: 'Rascunho',              cls: 'pi-badge-rascunho' },
    aguardando_aprovacao: { label: 'Aguardando aprovação',  cls: 'pi-badge-aguardando' },
    aprovado:             { label: 'Aprovado',              cls: 'pi-badge-aprovado' },
    reprovado:            { label: 'Reprovado',             cls: 'pi-badge-reprovado' },
    ajuste_solicitado:    { label: 'Ajuste solicitado',     cls: 'pi-badge-ajuste' },
};

function piStatusBadge(status) {
    const info = PI_STATUS_LABEL[status] || { label: status, cls: 'pi-badge-rascunho' };
    return `<span class="pi-badge ${info.cls}">${esc(info.label)}</span>`;
}

// ---------------------------------------------------------------------------
// Init / reload
// ---------------------------------------------------------------------------

function loadPremiacoesInternas() {
    // Default mes = mes atual
    const mesInput = document.getElementById('pi-filter-mes');
    if (mesInput && !mesInput.value) {
        const d = new Date();
        mesInput.value = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
    }
    piLoadUsuarios();
    piLoadMe();
    piReloadLotes();
}

function piDebouncedReload() {
    clearTimeout(window.PI_STATE._reloadTimer);
    window.PI_STATE._reloadTimer = setTimeout(piReloadLotes, 350);
}

async function piReloadLotes() {
    const mes = (document.getElementById('pi-filter-mes')?.value || '').trim();
    const setor = (document.getElementById('pi-filter-setor')?.value || '').trim();
    const status = (document.getElementById('pi-filter-status')?.value || '').trim();
    const q = (document.getElementById('pi-filter-q')?.value || '').trim();

    const params = new URLSearchParams();
    if (mes) params.set('mes', mes);
    if (setor) params.set('setor', setor);
    if (status) params.set('status', status);
    if (q) params.set('q', q);

    const tbody = document.getElementById('pi-tbody');
    const empty = document.getElementById('pi-empty');
    if (!tbody) return;
    tbody.innerHTML = `<tr><td colspan="7" class="text-center py-8" style="color: var(--text-secondary)">Carregando…</td></tr>`;
    if (empty) empty.classList.add('hidden');

    try {
        const res = await api('/api/premiacoes-internas/lotes?' + params.toString());
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || 'Falha ao carregar');

        const lotes = data.lotes || [];
        if (!lotes.length) {
            tbody.innerHTML = '';
            if (empty) empty.classList.remove('hidden');
            return;
        }

        tbody.innerHTML = lotes.map(l => piRenderLoteRow(l)).join('');
    } catch (e) {
        console.error('piReloadLotes', e);
        tbody.innerHTML = `<tr><td colspan="7" class="text-center py-8 text-red-500">${esc(e.message || 'Erro')}</td></tr>`;
    }
}

function piRenderLoteRow(l) {
    const canDelete = l.status === 'rascunho';
    const nColabsCell = `<td>${l._n_colabs != null ? l._n_colabs : '—'}</td>`; // placeholder; API atual so retorna a contagem via detalhe
    return `<tr>
        <td>${esc(piFmtMes(l.mes_referencia))}</td>
        <td>${esc(l.setor || '')}</td>
        <td>—</td>
        <td class="font-semibold">${piFmtMoney(l.valor_total)}</td>
        <td>${piStatusBadge(l.status)}</td>
        <td>${esc(piFmtDate(l.updated_at || l.created_at))}</td>
        <td class="text-right">
            <button class="pi-btn pi-btn-ghost" onclick="piOpenModal(${l.id})">Abrir</button>
            ${canDelete ? `<button class="pi-btn pi-btn-danger ml-1" onclick="piDeletarLote(${l.id})">Excluir</button>` : ''}
        </td>
    </tr>`;
}

// ---------------------------------------------------------------------------
// Modal
// ---------------------------------------------------------------------------

function piOpenModal(loteId) {
    const modal = document.getElementById('pi-modal');
    if (!modal) return;
    if (modal.parentNode !== document.body) {
        document.body.appendChild(modal);
    }
    document.body.style.overflow = 'hidden';
    modal.classList.remove('hidden');

    // Reset
    window.PI_STATE.editingId = loteId || null;
    window.PI_STATE.editable = true;
    window.PI_STATE.colaboradores = [];
    window.PI_STATE.loteCache = null;

    document.getElementById('pi-modal-title').textContent = loteId ? 'Editar premiação' : 'Nova premiação';
    document.getElementById('pi-modal-subtitle').textContent = '';
    document.getElementById('pi-aprovador-box').classList.add('hidden');
    document.getElementById('pi-readonly-alert').classList.add('hidden');
    document.getElementById('pi-historico-section').classList.add('hidden');

    // Reset form
    document.getElementById('pi-lote-mes').value = (function() {
        const d = new Date();
        return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
    })();
    document.getElementById('pi-lote-setor').value = '';
    document.getElementById('pi-lote-obs').value = '';

    if (loteId) {
        piCarregarLote(loteId);
    } else {
        piRenderColabList();
        piUpdateTotal();
        piUpdateModalActions();
    }
}

function piMaybeCloseModal(ev) {
    if (ev && ev.target && ev.target.classList.contains('pi-modal-backdrop')) {
        piCloseModal();
    }
}

function piCloseModal() {
    const modal = document.getElementById('pi-modal');
    if (modal) modal.classList.add('hidden');
    document.body.style.overflow = '';
}

async function piCarregarLote(loteId) {
    try {
        const res = await api(`/api/premiacoes-internas/lotes/${loteId}`);
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || 'Falha ao carregar lote');

        const lote = data.lote || {};
        const colabs = data.colaboradores || [];
        const eventos = data.eventos || [];

        window.PI_STATE.loteCache = lote;
        window.PI_STATE.colaboradores = colabs.map(c => ({...c}));

        // Editavel se rascunho ou ajuste_solicitado
        const editable = ['rascunho', 'ajuste_solicitado'].includes(lote.status);
        window.PI_STATE.editable = editable;

        // Preenche form
        document.getElementById('pi-lote-mes').value = lote.mes_referencia || '';
        document.getElementById('pi-lote-setor').value = lote.setor || '';
        document.getElementById('pi-lote-obs').value = lote.observacoes_gerais || '';

        document.getElementById('pi-modal-title').textContent = editable
            ? (lote.status === 'ajuste_solicitado' ? 'Ajustar premiação' : 'Editar rascunho')
            : 'Detalhes da premiação';
        document.getElementById('pi-modal-subtitle').innerHTML =
            `${piStatusBadge(lote.status)} <span style="color: var(--text-secondary)">· ${esc(piFmtMes(lote.mes_referencia))} · ${esc(lote.setor || '')}</span>`;

        // Feedback do aprovador (se houver)
        if (lote.aprovador_justificativa || lote.aprovador_nome) {
            document.getElementById('pi-aprovador-box').classList.remove('hidden');
            document.getElementById('pi-aprovador-nome').textContent =
                `${lote.aprovador_nome || '—'} · ${piFmtDate(lote.decidido_em)}`;
            document.getElementById('pi-aprovador-justificativa').textContent = lote.aprovador_justificativa || '(sem justificativa)';
        }

        // Alerta somente leitura
        if (!editable) {
            document.getElementById('pi-readonly-alert').classList.remove('hidden');
            document.getElementById('pi-readonly-msg').textContent =
                `Este lote está em ${PI_STATUS_LABEL[lote.status]?.label || lote.status} e não pode ser editado.`;
        }

        // Historico
        piRenderHistorico(eventos);

        // Colaboradores
        piRenderColabList();
        piUpdateTotal();
        piUpdateModalActions();

        // Disabled inputs no read-only
        piApplyEditableState();
    } catch (e) {
        console.error('piCarregarLote', e);
        toast(e.message || 'Erro ao carregar lote', 'error');
        piCloseModal();
    }
}

function piApplyEditableState() {
    const editable = window.PI_STATE.editable;
    ['pi-lote-mes', 'pi-lote-setor', 'pi-lote-obs'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.disabled = !editable;
    });
    const addBtn = document.getElementById('pi-add-colab-btn');
    if (addBtn) addBtn.style.display = editable ? '' : 'none';
}

// ---------------------------------------------------------------------------
// Colaboradores
// ---------------------------------------------------------------------------

function piAddColab() {
    window.PI_STATE.colaboradores.push({
        app_user_id: null,
        nome: '',
        email: '',
        cargo: '',
        setor: document.getElementById('pi-lote-setor').value || '',
        valor: 0,
        justificativa: '',
        observacoes: '',
        is_auto_premiacao: false,
    });
    piRenderColabList();
    piUpdateTotal();
    // Foca o ultimo card
    setTimeout(() => {
        const items = document.querySelectorAll('#pi-colab-list [data-colab-idx]');
        if (items.length) {
            const last = items[items.length - 1];
            const input = last.querySelector('input[data-field="nome"]');
            if (input) input.focus();
        }
    }, 60);
}

function piRemoveColab(idx) {
    window.PI_STATE.colaboradores.splice(idx, 1);
    piRenderColabList();
    piUpdateTotal();
}

function piRenderColabList() {
    const list = document.getElementById('pi-colab-list');
    if (!list) return;
    const editable = window.PI_STATE.editable;
    const colabs = window.PI_STATE.colaboradores;

    if (!colabs.length) {
        list.innerHTML = `<div class="text-center py-6 rounded-lg" style="background: var(--bg-elevated); color: var(--text-secondary); font-size:.85rem">
            Nenhum colaborador adicionado. ${editable ? 'Clique em "Adicionar" para começar.' : ''}
        </div>`;
        return;
    }

    list.innerHTML = colabs.map((c, idx) => piRenderColabRow(c, idx, editable)).join('');
    // Bind eventos
    list.querySelectorAll('[data-colab-idx]').forEach(row => {
        const idx = Number(row.dataset.colabIdx);
        row.querySelectorAll('[data-field]').forEach(el => {
            el.addEventListener('input', () => piUpdateColabField(idx, el.dataset.field, el));
            el.addEventListener('change', () => piUpdateColabField(idx, el.dataset.field, el));
        });
    });
}

function piRenderColabRow(c, idx, editable) {
    const disabled = editable ? '' : 'disabled';
    const hasUser = !!c.app_user_id;
    return `<div class="pi-colab-row" data-colab-idx="${idx}">
        <div class="flex items-center justify-between mb-2">
            <div class="text-xs font-semibold" style="color: var(--text-secondary)">Colaborador #${idx + 1}</div>
            ${editable ? `<button type="button" class="text-xs text-red-500 hover:underline" onclick="piRemoveColab(${idx})">Remover</button>` : ''}
        </div>
        <div class="grid grid-cols-1 sm:grid-cols-2 gap-2 mb-2">
            <div>
                <label class="pi-label">Usuário *</label>
                <input class="pi-input" data-field="nome" list="pi-users-datalist"
                       value="${esc(c.nome || '')}"
                       placeholder="Digite o nome do usuário…" ${disabled}
                       autocomplete="off">
                <div class="text-[11px] mt-1" data-role="colab-hint" style="color: var(--text-secondary); min-height:14px">
                    ${hasUser
                        ? `${c.email ? esc(c.email) : '<span style="opacity:.6">(sem email cadastrado)</span>'}`
                        : (c.nome ? '<span style="color:#d97706">Usuário não vinculado — selecione um da lista</span>' : '')}
                </div>
            </div>
            <div>
                <label class="pi-label">Cargo *</label>
                <input class="pi-input" data-field="cargo" value="${esc(c.cargo || '')}"
                       placeholder="Ex: Analista Comercial…" ${disabled}>
            </div>
            <div>
                <label class="pi-label">Setor</label>
                <select class="pi-select" data-field="setor" ${disabled}>
                    ${piSetorOptions(c.setor)}
                </select>
            </div>
            <div>
                <label class="pi-label">Valor (R$) *</label>
                <input class="pi-input" type="number" step="0.01" min="0" data-field="valor" value="${Number(c.valor || 0)}" ${disabled}>
            </div>
        </div>
        <div class="mb-2">
            <label class="pi-label">Justificativa *</label>
            <textarea class="pi-textarea" data-field="justificativa" ${disabled}>${esc(c.justificativa || '')}</textarea>
        </div>
        <div class="mb-2">
            <label class="pi-label">Observações</label>
            <textarea class="pi-textarea" data-field="observacoes" ${disabled}>${esc(c.observacoes || '')}</textarea>
        </div>
        <label class="flex items-center gap-2 text-xs" style="color: var(--text-secondary)">
            <input type="checkbox" data-field="is_auto_premiacao" ${c.is_auto_premiacao ? 'checked' : ''} ${disabled}>
            Auto-premiação (este colaborador é o próprio gestor)
        </label>
    </div>`;
}

function piUpdateColabField(idx, field, el) {
    const c = window.PI_STATE.colaboradores[idx];
    if (!c) return;
    if (el.type === 'checkbox') {
        c[field] = el.checked;
        if (field === 'is_auto_premiacao' && el.checked) {
            piAutofillGestorLogado(idx);
        }
        return;
    }
    if (field === 'valor') {
        c[field] = Number(el.value || 0);
        piUpdateTotal();
        return;
    }

    c[field] = el.value;

    // Autofill quando o campo Nome bate com um user do cache
    if (field === 'nome') {
        const match = piFindUsuarioByUsername(el.value);
        const row = el.closest('[data-colab-idx]');
        const hint = row ? row.querySelector('[data-role="colab-hint"]') : null;
        if (match) {
            c.app_user_id = match.id;
            c.email = match.email || '';
            // Cargo: pre-preenche com `categoria` do user se ainda vazio.
            if (!c.cargo && match.categoria) {
                c.cargo = match.categoria;
                const cargoInput = row ? row.querySelector('[data-field="cargo"]') : null;
                if (cargoInput) cargoInput.value = c.cargo;
            }
            if (hint) {
                hint.innerHTML = c.email ? esc(c.email) : '<span style="opacity:.6">(sem email cadastrado)</span>';
                hint.style.color = 'var(--text-secondary)';
            }
        } else {
            c.app_user_id = null;
            c.email = '';
            if (hint) {
                if (el.value) {
                    hint.innerHTML = 'Usuário não vinculado — selecione um da lista';
                    hint.style.color = '#d97706';
                } else {
                    hint.innerHTML = '';
                    hint.style.color = 'var(--text-secondary)';
                }
            }
        }
    }
}

function piUpdateTotal() {
    const total = window.PI_STATE.colaboradores.reduce((acc, c) => acc + Number(c.valor || 0), 0);
    const el = document.getElementById('pi-total-valor');
    if (el) el.textContent = piFmtMoney(total);
}

// ---------------------------------------------------------------------------
// Historico
// ---------------------------------------------------------------------------

function piRenderHistorico(eventos) {
    if (!eventos || !eventos.length) {
        document.getElementById('pi-historico-section').classList.add('hidden');
        return;
    }
    document.getElementById('pi-historico-section').classList.remove('hidden');
    const list = document.getElementById('pi-historico-list');
    const TIPO_LABEL = {
        criado: 'Criado como rascunho',
        editado: 'Editado',
        enviado: 'Enviado para aprovação',
        reenviado: 'Reenviado para aprovação',
        aprovado: 'Aprovado',
        reprovado: 'Reprovado',
        ajuste_solicitado: 'Ajuste solicitado',
    };
    list.innerHTML = eventos.map(ev => `<div class="pi-timeline-item">
        <div class="pi-timeline-dot"></div>
        <div class="text-sm font-semibold" style="color: var(--text-primary)">${esc(TIPO_LABEL[ev.tipo] || ev.tipo)}</div>
        <div class="text-xs" style="color: var(--text-secondary)">${esc(ev.autor_nome)} · ${esc(piFmtDate(ev.created_at))}</div>
        ${ev.justificativa ? `<div class="text-xs mt-1 p-2 rounded" style="background: var(--bg-elevated); color: var(--text-primary); white-space:pre-wrap">${esc(ev.justificativa)}</div>` : ''}
    </div>`).join('');
}

// ---------------------------------------------------------------------------
// Actions
// ---------------------------------------------------------------------------

function piUpdateModalActions() {
    const editable = window.PI_STATE.editable;
    const saveBtn = document.getElementById('pi-save-btn');
    const submitBtn = document.getElementById('pi-submit-btn');
    if (saveBtn) saveBtn.style.display = editable ? '' : 'none';
    if (submitBtn) submitBtn.style.display = editable ? '' : 'none';
}

function piBuildPayload() {
    return {
        mes_referencia: (document.getElementById('pi-lote-mes')?.value || '').trim(),
        setor: (document.getElementById('pi-lote-setor')?.value || '').trim(),
        observacoes_gerais: (document.getElementById('pi-lote-obs')?.value || '').trim(),
        colaboradores: window.PI_STATE.colaboradores.map(c => ({
            app_user_id: c.app_user_id || null,
            nome: (c.nome || '').trim(),
            email: (c.email || '').trim(),
            cargo: (c.cargo || '').trim(),
            setor: (c.setor || '').trim(),
            valor: Number(c.valor || 0),
            justificativa: (c.justificativa || '').trim(),
            observacoes: (c.observacoes || '').trim(),
            is_auto_premiacao: !!c.is_auto_premiacao,
        })),
    };
}

async function piSalvarRascunho() {
    const payload = piBuildPayload();
    const id = window.PI_STATE.editingId;
    const url = id ? `/api/premiacoes-internas/lotes/${id}` : `/api/premiacoes-internas/lotes`;
    const method = id ? 'PUT' : 'POST';

    try {
        const res = await api(url, {
            method,
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
        });
        const data = await res.json();
        if (!res.ok) {
            const detail = (data.detalhes || []).join(' ') || data.error;
            throw new Error(detail || 'Falha ao salvar');
        }
        toast(id ? 'Rascunho atualizado' : 'Rascunho criado', 'success');
        piCloseModal();
        piReloadLotes();
    } catch (e) {
        console.error('piSalvarRascunho', e);
        toast(e.message || 'Erro ao salvar', 'error');
    }
}

async function piEnviarAprovacao() {
    if (!confirm('Enviar esta premiação para aprovação? Você não poderá editá-la enquanto estiver aguardando decisão.')) return;
    const payload = piBuildPayload();
    payload.save_before_submit = true;

    let id = window.PI_STATE.editingId;
    try {
        // Se ainda nao criou, cria primeiro
        if (!id) {
            const resCreate = await api('/api/premiacoes-internas/lotes', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
            });
            const dc = await resCreate.json();
            if (!resCreate.ok) {
                const detail = (dc.detalhes || []).join(' ') || dc.error;
                throw new Error(detail || 'Falha ao criar');
            }
            id = dc.id;
            window.PI_STATE.editingId = id;
        }

        const res = await api(`/api/premiacoes-internas/lotes/${id}/enviar`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
        });
        const data = await res.json();
        if (!res.ok) {
            const detail = (data.detalhes || []).join(' ') || data.error;
            throw new Error(detail || 'Falha ao enviar');
        }
        toast('Premiação enviada para aprovação', 'success');
        piCloseModal();
        piReloadLotes();
    } catch (e) {
        console.error('piEnviarAprovacao', e);
        toast(e.message || 'Erro ao enviar', 'error');
    }
}

async function piDeletarLote(loteId) {
    if (!confirm('Excluir este rascunho? Esta ação não pode ser desfeita.')) return;
    try {
        const res = await api(`/api/premiacoes-internas/lotes/${loteId}`, { method: 'DELETE' });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || 'Falha ao excluir');
        toast('Rascunho excluído', 'success');
        piReloadLotes();
    } catch (e) {
        console.error('piDeletarLote', e);
        toast(e.message || 'Erro ao excluir', 'error');
    }
}
