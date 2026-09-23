// ---------------------------------------------------------------------------
// Tracker de Tarefas - TI
// ---------------------------------------------------------------------------
const TT_API = '/api/tracker-tarefas';
const TT_STATUS = ['Pendente', 'Em andamento', 'Concluído'];
const TT_MESES = ['Janeiro','Fevereiro','Março','Abril','Maio','Junho','Julho','Agosto','Setembro','Outubro','Novembro','Dezembro'];
const TT_RESPONSAVEIS = ['Gustavo', 'Luan', 'Raphael', 'William'];

const ttState = {
    rows: [],
    loading: false,
    editingId: null,   // id da linha em edição
    adding: false,     // linha nova aberta
};

function ttEsc(s) {
    if (s === null || s === undefined) return '';
    const div = document.createElement('div');
    div.textContent = String(s);
    return div.innerHTML;
}

function ttParseData(row) {
    const m = String(row.data_inicio || '').match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
    if (!m) return null;
    const d = parseInt(m[1], 10), mo = parseInt(m[2], 10), y = parseInt(m[3], 10);
    if (!mo || !y) return null;
    return { y, mo, d, key: `${y}-${String(mo).padStart(2, '0')}`, label: `${TT_MESES[mo - 1] || mo}/${y}` };
}

function ttMesKey(row) {
    const mes = String(row.mes || '').trim();
    if (mes) {
        const i = TT_MESES.findIndex(n => n.localeCompare(mes, 'pt-BR', { sensitivity: 'accent' }) === 0);
        if (i >= 0) return { key: String(i + 1).padStart(2, '0'), label: TT_MESES[i] };
        return { key: mes.toLowerCase(), label: mes };
    }
    const dt = ttParseData(row);
    if (dt) return { key: String(dt.mo).padStart(2, '0'), label: TT_MESES[dt.mo - 1] };
    return null;
}

function ttStatusClass(status) {
    if (status === 'Concluído') return 'tt-status-concluido';
    if (status === 'Em andamento') return 'tt-status-andamento';
    return 'tt-status-pendente';
}

function ttAlert(msg, ok) {
    const el = document.getElementById('tt-alert');
    el.textContent = msg;
    el.className = 'tt-alert ' + (ok ? 'tt-alert-ok' : 'tt-alert-error');
    el.style.display = 'block';
    if (ok) setTimeout(() => { el.style.display = 'none'; }, 3000);
}

async function ttFetch(method, url, body) {
    const opts = { method, headers: { 'Content-Type': 'application/json' } };
    if (body !== undefined) opts.body = JSON.stringify(body);
    const res = await fetch(url, opts);
    const json = await res.json().catch(() => ({}));
    if (!res.ok || json.ok === false) {
        throw new Error(json.error || `HTTP ${res.status}`);
    }
    return json;
}

async function loadTrackerTarefas() {
    ttState.loading = true;
    ttState.editingId = null;
    ttState.adding = false;
    ttRender();
    try {
        const json = await ttFetch('GET', TT_API);
        ttState.rows = (json.rows || []).slice().sort((a, b) => {
            const da = ttParseData(a), db = ttParseData(b);
            const ka = da ? da.y * 10000 + da.mo * 100 + da.d : 0;
            const kb = db ? db.y * 10000 + db.mo * 100 + db.d : 0;
            return kb - ka || (b.id || 0) - (a.id || 0);
        });
        ttFillFilters();
    } catch (err) {
        console.error('[TrackerTI] load:', err);
        ttAlert('Erro ao carregar: ' + err.message, false);
    } finally {
        ttState.loading = false;
        ttRender();
    }
}

function ttFillFilters() {
    const mesEl = document.getElementById('tt-filtro-mes');
    const respEl = document.getElementById('tt-filtro-resp');
    if (!mesEl || !respEl) return;
    const mesAtual = mesEl.value;
    const respAtual = respEl.value;
    const meses = new Map();
    const resps = new Set();
    ttState.rows.forEach(r => {
        const mes = ttMesKey(r);
        if (mes) meses.set(mes.key, mes.label);
        if (r.responsavel) resps.add(r.responsavel);
    });
    const mesOpts = ['<option value="">Todos os meses</option>']
        .concat([...meses.entries()].sort((a, b) => b[0].localeCompare(a[0]))
            .map(([k, lab]) => `<option value="${k}">${lab}</option>`));
    const respOpts = ['<option value="">Todos</option>']
        .concat([...resps].sort((a, b) => a.localeCompare(b, 'pt-BR'))
            .map(n => `<option value="${ttEsc(n)}">${ttEsc(n)}</option>`));
    mesEl.innerHTML = mesOpts.join('');
    respEl.innerHTML = respOpts.join('');
    if ([...meses.keys()].includes(mesAtual)) mesEl.value = mesAtual;
    if ([...resps].includes(respAtual)) respEl.value = respAtual;
}

function ttFilteredRows() {
    const mes = (document.getElementById('tt-filtro-mes') || {}).value || '';
    const resp = (document.getElementById('tt-filtro-resp') || {}).value || '';
    return ttState.rows.filter(r => {
        if (mes) {
            const mk = ttMesKey(r);
            if (!mk || mk.key !== mes) return false;
        }
        if (resp && (r.responsavel || '') !== resp) return false;
        return true;
    });
}

function ttRender() {
    const tbody = document.getElementById('tt-tbody');
    if (!tbody) return;

    const visible = ttFilteredRows();
    const count = { 'Pendente': 0, 'Em andamento': 0, 'Concluído': 0 };
    visible.forEach(r => { if (count[r.status] !== undefined) count[r.status]++; });
    document.getElementById('tt-stat-pendente').textContent = count['Pendente'];
    document.getElementById('tt-stat-andamento').textContent = count['Em andamento'];
    document.getElementById('tt-stat-concluido').textContent = count['Concluído'];

    if (ttState.loading) {
        tbody.innerHTML = '<tr><td colspan="7" class="tt-empty">Carregando…</td></tr>';
        return;
    }

    let html = '';
    if (!visible.length && !ttState.adding) {
        html = '<tr><td colspan="7" class="tt-empty">Nenhuma tarefa com esses filtros.</td></tr>';
    }

    html += visible.map(r => {
        if (ttState.editingId === r.id) return ttRowEdit(r);
        return `
        <tr onclick="ttEditar(${r.id})" style="cursor:pointer" title="Clique para editar">
            <td>${ttEsc(r.data_inicio)}</td>
            <td>${ttEsc(r.responsavel)}</td>
            <td class="tt-tarefa">${ttEsc(r.descricao)}</td>
            <td>${ttEsc(r.mes)}</td>
            <td>${ttEsc(r.data_final)}</td>
            <td><span class="tt-status ${ttStatusClass(r.status)}">${ttEsc(r.status)}</span></td>
            <td onclick="event.stopPropagation()">
                <button class="tt-btn tt-btn-icon tt-btn-danger" onclick="ttExcluir(${r.id})" title="Excluir">
                    <span class="material-symbols-outlined" style="font-size:16px">delete</span>
                </button>
            </td>
        </tr>`;
    }).join('');

    if (ttState.adding) html = ttRowEdit(null) + html;

    tbody.innerHTML = html;
}

function ttRowEdit(r) {
    const isNew = !r;
    const v = k => isNew ? '' : ttEsc(r[k] || '');
    const statusAtual = isNew ? 'Pendente' : r.status;
    const mesAtual = isNew ? '' : (r.mes || '');
    const respAtual = isNew ? '' : (r.responsavel || '');
    const options = TT_STATUS.map(s =>
        `<option value="${s}" ${s === statusAtual ? 'selected' : ''}>${s}</option>`
    ).join('');
    const mesOpts = ['<option value=""></option>'].concat(TT_MESES.map(m =>
        `<option value="${m}" ${m.localeCompare(mesAtual, 'pt-BR', { sensitivity: 'accent' }) === 0 ? 'selected' : ''}>${m}</option>`
    )).join('');
    const respOpts = ['<option value=""></option>'].concat(TT_RESPONSAVEIS.map(n =>
        `<option value="${n}" ${n === respAtual ? 'selected' : ''}>${n}</option>`
    )).join('');
    return `
    <tr class="tt-new-row">
        <td><input class="tt-input" id="tt-edit-data-inicio" value="${v('data_inicio')}" placeholder="DD/MM/AAAA"></td>
        <td><select class="tt-select" id="tt-edit-responsavel">${respOpts}</select></td>
        <td><input class="tt-input" id="tt-edit-descricao" value="${v('descricao')}" placeholder="Descrição"></td>
        <td><select class="tt-select" id="tt-edit-mes">${mesOpts}</select></td>
        <td><input class="tt-input" id="tt-edit-data-final" value="${v('data_final')}" placeholder="DD/MM/AAAA"></td>
        <td><select class="tt-select" id="tt-edit-status">${options}</select></td>
        <td onclick="event.stopPropagation()" style="white-space:nowrap">
            <button class="tt-btn tt-btn-icon tt-btn-primary" onclick="ttSalvar(${isNew ? 'null' : r.id})" title="Salvar">
                <span class="material-symbols-outlined" style="font-size:16px">check</span>
            </button>
            <button class="tt-btn tt-btn-icon" onclick="ttCancelar()" title="Cancelar">
                <span class="material-symbols-outlined" style="font-size:16px">close</span>
            </button>
        </td>
    </tr>`;
}

function ttEditar(id) {
    ttState.editingId = id;
    ttState.adding = false;
    ttRender();
}

function ttNovaTarefa() {
    ttState.adding = true;
    ttState.editingId = null;
    ttRender();
    const el = document.getElementById('tt-edit-descricao');
    if (el) el.focus();
}

function ttCancelar() {
    ttState.editingId = null;
    ttState.adding = false;
    ttRender();
}

async function ttSalvar(id) {
    const body = {
        data_inicio: document.getElementById('tt-edit-data-inicio').value.trim(),
        responsavel: document.getElementById('tt-edit-responsavel').value.trim(),
        descricao: document.getElementById('tt-edit-descricao').value.trim(),
        mes: document.getElementById('tt-edit-mes').value.trim(),
        data_final: document.getElementById('tt-edit-data-final').value.trim(),
        status: document.getElementById('tt-edit-status').value,
    };
    if (!body.descricao) {
        ttAlert('Informe a descrição.', false);
        return;
    }
    try {
        if (id === null) {
            await ttFetch('POST', TT_API, body);
            ttAlert('Tarefa criada.', true);
        } else {
            await ttFetch('PATCH', `${TT_API}/${id}`, body);
            ttAlert('Tarefa atualizada.', true);
        }
        ttState.editingId = null;
        ttState.adding = false;
        await loadTrackerTarefas();
    } catch (err) {
        console.error('[TrackerTI] save:', err);
        ttAlert('Erro ao salvar: ' + err.message, false);
    }
}

async function ttExcluir(id) {
    if (!confirm(`Excluir a tarefa #${id}?`)) return;
    try {
        await ttFetch('DELETE', `${TT_API}/${id}`);
        ttAlert('Tarefa excluída.', true);
        await loadTrackerTarefas();
    } catch (err) {
        console.error('[TrackerTI] delete:', err);
        ttAlert('Erro ao excluir: ' + err.message, false);
    }
}
