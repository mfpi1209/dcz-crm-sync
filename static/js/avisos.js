/* ── Avisos ─────────────────────────────────────────────────── */

let _avCurrentTab = 'nao-lidos';
let _avSelectedUsers = [];
let _avUsersCache = [];

const _PRIO_BADGE = {
    urgente:    'bg-red-500/20 text-red-400 border-red-500/30',
    importante: 'bg-amber-500/20 text-amber-400 border-amber-500/30',
    normal:     'bg-gray-500/20 text-gray-400 border-gray-500/30',
};

const _PRIO_ACCENT = {
    urgente:    'border-l-red-500',
    importante: 'border-l-amber-500',
    normal:     'border-l-gray-600',
};

function _fmtDate(iso) {
    if (!iso) return '—';
    const d = new Date(iso);
    return d.toLocaleDateString('pt-BR', { day: '2-digit', month: '2-digit', year: 'numeric' });
}

function _fmtDatetime(iso) {
    if (!iso) return '—';
    const d = new Date(iso);
    return d.toLocaleDateString('pt-BR', { day: '2-digit', month: '2-digit', year: 'numeric' })
         + ' ' + d.toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' });
}

function _avisoCard(a, showReadBtn) {
    const prio = _PRIO_BADGE[a.prioridade] || _PRIO_BADGE.normal;
    const accent = _PRIO_ACCENT[a.prioridade] || _PRIO_ACCENT.normal;
    const readClass = a.lido ? 'opacity-60' : '';
    const readIcon = a.lido ? '<span class="text-[10px] text-emerald-400 ml-2">Lido</span>' : '';
    const readBtn = showReadBtn && !a.lido
        ? `<button onclick="avisosMarcarLido(${a.id})" class="text-xs text-violet-400 hover:text-violet-300 transition whitespace-nowrap">Marcar como lido</button>`
        : '';

    return `<div class="glass-card p-4 border-l-4 ${accent} ${readClass}" id="aviso-card-${a.id}">
        <div class="flex items-start justify-between gap-3">
            <div class="flex-1 min-w-0">
                <div class="flex items-center gap-2 mb-1 flex-wrap">
                    <span class="text-xs font-bold px-2 py-0.5 rounded-full border ${prio}">${a.prioridade}</span>
                    <h4 class="text-sm font-semibold text-[var(--text-primary)] truncate">${a.titulo}</h4>
                    ${readIcon}
                </div>
                <p class="text-xs text-gray-300 whitespace-pre-line mt-1">${a.corpo}</p>
                <p class="text-[10px] text-gray-600 mt-2">${_fmtDatetime(a.created_at)} — ${a.autor || 'Sistema'}</p>
            </div>
            <div class="flex-shrink-0">${readBtn}</div>
        </div>
    </div>`;
}

/* ── Tabs ───────────────────────────────────────────────────── */

function avisosTab(tab) {
    _avCurrentTab = tab;
    const tabs = ['nao-lidos', 'todos', 'admin'];
    tabs.forEach(t => {
        const btn = document.getElementById(`av-tab-${t}`);
        const sec = document.getElementById(`av-section-${t}`);
        if (!btn || !sec) return;
        if (t === tab) {
            btn.className = 'px-4 py-2 text-sm font-medium rounded-lg transition-all bg-violet-600/20 text-violet-400 border border-violet-500/30';
            sec.classList.remove('hidden');
        } else {
            btn.className = 'px-4 py-2 text-sm font-medium rounded-lg transition-all text-gray-400 hover:text-[var(--text-primary)]';
            sec.classList.add('hidden');
        }
    });
    if (tab === 'nao-lidos') _loadNaoLidos();
    if (tab === 'todos') _loadTodos();
    if (tab === 'admin') _loadAdmin();
}

/* ── Load lists ─────────────────────────────────────────────── */

function _loadNaoLidos() {
    api('/api/avisos/nao-lidos').then(r => r.json()).then(data => {
        const list = document.getElementById('av-list-nao-lidos');
        const empty = document.getElementById('av-empty-nao-lidos');
        const count = document.getElementById('av-count-nao-lidos');
        const badge = document.getElementById('av-badge-tab');
        count.textContent = data.count || 0;
        if (badge) badge.textContent = data.count || 0;
        _updateSidebarBadge(data.count || 0);

        if (!data.avisos || !data.avisos.length) {
            list.innerHTML = '';
            empty.classList.remove('hidden');
        } else {
            empty.classList.add('hidden');
            list.innerHTML = data.avisos.map(a => _avisoCard(a, true)).join('');
        }
    });
}

function _loadTodos() {
    api('/api/avisos').then(r => r.json()).then(rows => {
        const list = document.getElementById('av-list-todos');
        const empty = document.getElementById('av-empty-todos');
        if (!rows.length) {
            list.innerHTML = '';
            empty.classList.remove('hidden');
        } else {
            empty.classList.add('hidden');
            list.innerHTML = rows.map(a => _avisoCard(a, true)).join('');
        }
    });
}

function _loadAdmin() {
    api('/api/avisos/admin').then(r => r.json()).then(rows => {
        const tbody = document.getElementById('av-admin-tbody');
        if (!rows.length) {
            tbody.innerHTML = '<tr><td colspan="7" class="text-center text-gray-600 py-4">Nenhum aviso</td></tr>';
            return;
        }
        tbody.innerHTML = rows.map(a => {
            const prio = _PRIO_BADGE[a.prioridade] || _PRIO_BADGE.normal;
            const status = a.active ? '<span class="text-emerald-400">Ativo</span>' : '<span class="text-red-400">Inativo</span>';
            return `<tr class="hover:bg-gray-100 dark:hover:bg-gray-800/30">
                <td class="py-2 px-3 text-gray-300 max-w-[200px] truncate">${a.titulo}</td>
                <td class="py-2 px-3"><span class="text-[10px] font-bold px-2 py-0.5 rounded-full border ${prio}">${a.prioridade}</span></td>
                <td class="py-2 px-3 text-gray-400">${a.target_role}${a.target_user_ids && a.target_user_ids.length ? ' +' + a.target_user_ids.length : ''}</td>
                <td class="py-2 px-3 text-gray-500">${_fmtDate(a.created_at)}</td>
                <td class="py-2 px-3 text-gray-500">${_fmtDate(a.expires_at)}</td>
                <td class="py-2 px-3">${status}</td>
                <td class="py-2 px-3 flex gap-2">
                    <button onclick="avisosEditar(${a.id})" class="text-xs text-violet-400 hover:text-violet-300">Editar</button>
                    ${a.active ? `<button onclick="avisosDesativar(${a.id})" class="text-xs text-red-400 hover:text-red-300">Desativar</button>` : ''}
                </td>
            </tr>`;
        }).join('');
    });

    _loadUsersSelect();
}

/* ── User multi-select ──────────────────────────────────────── */

function _loadUsersSelect() {
    if (_avUsersCache.length) return;
    api('/api/avisos/usuarios').then(r => r.json()).then(users => {
        _avUsersCache = users;
        _renderUsersSelect();
    });
}

function _renderUsersSelect() {
    const sel = document.getElementById('av-users-select');
    if (!sel) return;
    const opts = _avUsersCache.filter(u => !_avSelectedUsers.includes(u.id));
    sel.innerHTML = '<option value="">+ Adicionar usuário...</option>' +
        opts.map(u => `<option value="${u.id}">${u.username} (${u.role})</option>`).join('');
}

function avAddUser(sel) {
    const uid = parseInt(sel.value);
    if (!uid) return;
    _avSelectedUsers.push(uid);
    sel.value = '';
    _renderUserChips();
    _renderUsersSelect();
}

function avRemoveUser(uid) {
    _avSelectedUsers = _avSelectedUsers.filter(id => id !== uid);
    _renderUserChips();
    _renderUsersSelect();
}

function _renderUserChips() {
    const container = document.getElementById('av-users-container');
    const sel = document.getElementById('av-users-select');
    const chips = _avSelectedUsers.map(uid => {
        const u = _avUsersCache.find(x => x.id === uid);
        const name = u ? u.username : uid;
        return `<span class="inline-flex items-center gap-1 bg-violet-500/20 text-violet-300 text-xs px-2 py-0.5 rounded-full">
            ${name}
            <button onclick="avRemoveUser(${uid})" class="hover:text-[var(--text-primary)]">&times;</button>
        </span>`;
    }).join('');
    container.innerHTML = chips + sel.outerHTML;
    const newSel = document.getElementById('av-users-select');
    if (newSel) newSel.onchange = function() { avAddUser(this); };
}

/* ── CRUD actions ───────────────────────────────────────────── */

function avisosSalvar() {
    const id = document.getElementById('av-edit-id').value;
    const body = {
        titulo: document.getElementById('av-titulo').value,
        corpo: document.getElementById('av-corpo').value,
        prioridade: document.getElementById('av-prioridade').value,
        target_role: document.getElementById('av-target-role').value,
        target_user_ids: _avSelectedUsers,
        expires_at: document.getElementById('av-expires').value || null,
    };

    const url = id ? `/api/avisos/${id}` : '/api/avisos';
    const method = id ? 'PUT' : 'POST';

    api(url, { method, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
        .then(r => r.json())
        .then(data => {
            if (data.error) { toast(data.error, 'error'); return; }
            avisosCancelar();
            _loadAdmin();
        });
}

function avisosCancelar() {
    document.getElementById('av-edit-id').value = '';
    document.getElementById('av-titulo').value = '';
    document.getElementById('av-corpo').value = '';
    document.getElementById('av-prioridade').value = 'normal';
    document.getElementById('av-target-role').value = 'todos';
    document.getElementById('av-expires').value = '';
    _avSelectedUsers = [];
    _renderUserChips();
    _renderUsersSelect();
    document.getElementById('av-form-title').textContent = 'Novo Aviso';
}

function avisosEditar(id) {
    api(`/api/avisos/admin`).then(r => r.json()).then(rows => {
        const a = rows.find(x => x.id === id);
        if (!a) return;
        document.getElementById('av-edit-id').value = a.id;
        document.getElementById('av-titulo').value = a.titulo;
        document.getElementById('av-corpo').value = a.corpo;
        document.getElementById('av-prioridade').value = a.prioridade;
        document.getElementById('av-target-role').value = a.target_role;
        document.getElementById('av-expires').value = a.expires_at ? a.expires_at.slice(0, 10) : '';
        _avSelectedUsers = a.target_user_ids || [];
        _renderUserChips();
        _renderUsersSelect();
        document.getElementById('av-form-title').textContent = 'Editar Aviso';
        window.scrollTo({ top: 0, behavior: 'smooth' });
    });
}

function avisosDesativar(id) {
    if (!confirm('Desativar este aviso?')) return;
    api(`/api/avisos/${id}`, { method: 'DELETE' })
        .then(r => r.json())
        .then(() => _loadAdmin());
}

/* ── Mark read ──────────────────────────────────────────────── */

function avisosMarcarLido(id) {
    api(`/api/avisos/${id}/lido`, { method: 'POST' })
        .then(r => r.json())
        .then(() => {
            const card = document.getElementById(`aviso-card-${id}`);
            if (card) card.classList.add('opacity-60');
            _loadNaoLidos();
        });
}

function avisosMarcarTodosLidos() {
    api('/api/avisos/marcar-todos-lidos', { method: 'POST' })
        .then(r => r.json())
        .then(() => {
            _loadNaoLidos();
            if (_avCurrentTab === 'todos') _loadTodos();
        });
}

/* ── Sidebar badge ──────────────────────────────────────────── */

function _updateSidebarBadge(count) {
    const badge = document.getElementById('av-sidebar-badge');
    if (!badge) return;
    if (count > 0) {
        badge.textContent = count;
        badge.classList.remove('hidden');
    } else {
        badge.classList.add('hidden');
    }
}

/* ── Main loader ────────────────────────────────────────────── */

function loadAvisos() {
    const adminTab = document.getElementById('av-tab-admin');
    if (adminTab) {
        const role = document.body.dataset.role;
        if (role === 'admin') adminTab.classList.remove('hidden');
        else adminTab.classList.add('hidden');
    }
    avisosTab('nao-lidos');
}
