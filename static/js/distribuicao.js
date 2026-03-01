// ---------------------------------------------------------------------------
// Distribuição
// ---------------------------------------------------------------------------
let _distData = [];
let _distSortCol = 'fila';
let _distSortDir = 'asc';

function sortDistCol(col) {
    if (_distSortCol === col) {
        _distSortDir = _distSortDir === 'asc' ? 'desc' : 'asc';
    } else {
        _distSortCol = col;
        _distSortDir = 'asc';
    }
    document.querySelectorAll('[id^="dist-sort-"]').forEach(el => el.textContent = '');
    const indicator = document.getElementById('dist-sort-' + col);
    if (indicator) indicator.textContent = _distSortDir === 'asc' ? '▲' : '▼';
    filterDistribuicao();
}

async function loadDistribuicao() {
    const icon = document.getElementById('dist-refresh-icon');
    const loading = document.getElementById('dist-loading');
    const tbody = document.getElementById('dist-tbody');
    if (icon) icon.classList.add('animate-spin');
    if (loading) loading.classList.remove('hidden');

    try {
        const res = await api('/api/distribuicao');
        const payload = await res.json();
        _distData = payload.distribuicao || [];

        document.getElementById('dist-fila-atendimento').textContent = (payload.fila_atendimento ?? 0).toLocaleString('pt-BR');
        document.getElementById('dist-fila-acolhimento').textContent = (payload.fila_acolhimento ?? 0).toLocaleString('pt-BR');

        const ativos = _distData.filter(d => d.status === 'Ativo');
        const emAtend = _distData.reduce((a, d) => a + (parseInt(d.fila) || 0), 0);
        const ativosAtend = ativos.filter(d => d.tipo_atendimento === 'Atendimento').length;
        const ativosAcolh = ativos.filter(d => d.tipo_atendimento === 'Acolhimento').length;

        document.getElementById('dist-em-atendimento').textContent = emAtend.toLocaleString('pt-BR');
        document.getElementById('dist-ativos-atend').textContent = ativosAtend;
        document.getElementById('dist-ativos-acolh').textContent = ativosAcolh;
        document.getElementById('dist-total-ativos').textContent = ativos.length;

        filterDistribuicao();
    } catch(e) {
        console.error('Erro ao carregar distribuição:', e);
        if (tbody) tbody.innerHTML = `<tr><td colspan="9" class="px-4 py-6 text-center text-rose-400 text-sm">Erro ao carregar: ${e.message}</td></tr>`;
    } finally {
        if (icon) icon.classList.remove('animate-spin');
        if (loading) loading.classList.add('hidden');
    }
}

function filterDistribuicao() {
    const filter = document.getElementById('dist-filter').value;
    let items = [..._distData];
    if (filter !== 'Todos') items = items.filter(d => d.tipo_atendimento === filter);
    document.querySelectorAll('[id^="dist-sort-"]').forEach(el => el.textContent = '');
    const indicator = document.getElementById('dist-sort-' + _distSortCol);
    if (indicator) indicator.textContent = _distSortDir === 'asc' ? '▲' : '▼';
    renderDistTable(items);
}

function renderDistTable(items) {
    const col = _distSortCol;
    const dir = _distSortDir;

    // Atualizar contador
    const countEl = document.getElementById('dist-count');
    if (countEl) countEl.textContent = `${items.length} registro${items.length !== 1 ? 's' : ''}`;

    items.sort((a, b) => {
        let va, vb;
        if (col === 'fila') {
            va = parseInt(a.fila) || 0;
            vb = parseInt(b.fila) || 0;
            return dir === 'asc' ? va - vb : vb - va;
        }
        if (col === 'ultima_execucao') {
            va = a.ultima_execucao || '';
            vb = b.ultima_execucao || '';
        } else if (col === 'tipo_atendimento') {
            va = a.tipo_atendimento || '';
            vb = b.tipo_atendimento || '';
        } else {
            va = ''; vb = '';
        }
        const cmp = va.localeCompare(vb, 'pt-BR');
        return dir === 'asc' ? cmp : -cmp;
    });

    const tbody = document.getElementById('dist-tbody');
    tbody.innerHTML = items.map((d, idx) => {
        const isAtivo = d.status === 'Ativo';
        const filaNum = parseInt(d.fila) || 0;
        const filaBg = filaNum > 5 ? 'bg-rose-500/15 text-rose-400 border-rose-500/30' :
                       filaNum > 0 ? 'bg-amber-500/15 text-amber-400 border-amber-500/30' :
                       'bg-emerald-500/10 text-emerald-400 border-emerald-500/20';

        // Avatar com iniciais
        const initials = d.responsavel ? d.responsavel.split(' ').map(n => n[0]).join('').slice(0, 2).toUpperCase() : '??';
        const avatarColors = ['from-indigo-500 to-purple-500', 'from-emerald-500 to-teal-500', 'from-amber-500 to-orange-500', 'from-rose-500 to-pink-500', 'from-cyan-500 to-blue-500'];
        const avatarColor = avatarColors[idx % avatarColors.length];

        // Tipo badge
        const tipoBadge = d.tipo_atendimento === 'Atendimento'
            ? 'bg-indigo-500/15 text-indigo-400 border-indigo-500/30'
            : 'bg-emerald-500/15 text-emerald-400 border-emerald-500/30';

        return `<tr data-id="${esc(d.id)}" class="group hover:bg-slate-800/40 transition-all duration-200">
            <td class="px-4 py-3">
                <div class="flex items-center gap-3">
                    <div class="w-9 h-9 rounded-xl bg-gradient-to-br ${avatarColor} flex items-center justify-center text-white text-xs font-bold shadow-lg">
                        ${initials}
                    </div>
                    <span class="text-sm font-medium text-slate-200 group-hover:text-white transition-colors">${esc(d.responsavel)}</span>
                </div>
            </td>
            <td class="px-3 py-3 text-center">
                <select data-field="status" class="input-glass px-3 py-1.5 text-xs font-semibold rounded-lg border ${isAtivo ? 'text-emerald-400 border-emerald-500/30 bg-emerald-500/10' : 'text-rose-400 border-rose-500/30 bg-rose-500/10'} cursor-pointer hover:opacity-80 transition-opacity">
                    <option value="Ativo" ${isAtivo ? 'selected' : ''}>● Ativo</option>
                    <option value="Inativo" ${!isAtivo ? 'selected' : ''}>● Inativo</option>
                </select>
            </td>
            <td class="px-3 py-3 text-center">
                <div class="relative">
                    <input type="time" value="${esc(d.almoco || '')}" class="input-glass px-3 py-1.5 text-xs text-slate-300 w-[85px] text-center rounded-lg">
                </div>
            </td>
            <td class="px-3 py-3 text-center">
                <input type="time" value="${esc(d.final_expediente || '')}" class="input-glass px-3 py-1.5 text-xs text-slate-300 w-[85px] text-center rounded-lg">
            </td>
            <td class="px-3 py-3 text-center">
                <input type="number" value="${esc(d.pausa || '')}" placeholder="0" class="input-glass px-3 py-1.5 text-xs text-slate-300 w-16 text-center rounded-lg">
            </td>
            <td class="px-3 py-3 text-center">
                <input type="number" value="${esc(d.volume || '')}" placeholder="0" class="input-glass px-3 py-1.5 text-xs text-slate-300 w-16 text-center rounded-lg">
            </td>
            <td class="px-3 py-3 text-center">
                <span class="inline-flex items-center justify-center min-w-[40px] px-2.5 py-1 rounded-lg text-xs font-bold border ${filaBg}">
                    ${esc(d.fila || '0')}
                </span>
            </td>
            <td class="px-3 py-3 text-center">
                <span class="text-xs text-slate-400 flex items-center justify-center gap-1">
                    <svg class="w-3 h-3 text-slate-600" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>
                    ${esc(d.ultima_execucao || '—')}
                </span>
            </td>
            <td class="px-3 py-3 text-center">
                <select data-field="tipo" class="input-glass px-3 py-1.5 text-xs rounded-lg border ${tipoBadge} cursor-pointer hover:opacity-80 transition-opacity">
                    <option value="Atendimento" ${d.tipo_atendimento === 'Atendimento' ? 'selected' : ''}>Atendimento</option>
                    <option value="Acolhimento" ${d.tipo_atendimento === 'Acolhimento' ? 'selected' : ''}>Acolhimento</option>
                </select>
            </td>
        </tr>`;
    }).join('');

    tbody.querySelectorAll('select[data-field="status"]').forEach(sel => {
        sel.addEventListener('change', function() {
            this.className = this.className.replace(/text-(emerald|rose)-400/g, '');
            this.classList.add(this.value === 'Ativo' ? 'text-emerald-400' : 'text-rose-400');
        });
    });
}

async function saveDistribuicao() {
    const rows = document.querySelectorAll('#dist-tbody tr');
    const dados = [];
    rows.forEach(row => {
        const id = row.getAttribute('data-id');
        const statusSel = row.querySelector('select[data-field="status"]');
        const tipoSel = row.querySelector('select[data-field="tipo"]');
        const inputs = row.querySelectorAll('input');
        dados.push({
            id,
            status: statusSel ? statusSel.value : 'Ativo',
            almoco: inputs[0].value,
            final_expediente: inputs[1].value,
            pausa: inputs[2].value,
            volume: inputs[3].value,
            tipo_atendimento: tipoSel ? tipoSel.value : 'Atendimento',
        });
    });

    try {
        const res = await api('/api/distribuicao', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(dados),
        });
        const d = await res.json();
        if (d.ok) {
            alert('Alterações salvas com sucesso!');
            loadDistribuicao();
        } else {
            alert('Erro ao salvar: ' + (d.error || 'desconhecido'));
        }
    } catch(e) {
        alert('Erro ao salvar: ' + e.message);
    }
}

// ---------------------------------------------------------------------------
// Upload
// ---------------------------------------------------------------------------
function handleDropTyped(e, tipo) {
    e.preventDefault();
    e.currentTarget.classList.remove('border-emerald-500', 'bg-emerald-950/10',
        'border-amber-500', 'bg-amber-950/10', 'border-purple-500', 'bg-purple-950/10',
        'border-sky-500', 'bg-sky-950/10', 'border-rose-500', 'bg-rose-950/10');
    const file = e.dataTransfer.files[0];
    if (file) handleUploadTyped(file, tipo);
}

function handleDropSemRemat(e, subtipo) {
    e.preventDefault();
    e.currentTarget.classList.remove('border-emerald-500', 'bg-emerald-950/10',
        'border-amber-500', 'bg-amber-950/10');
    const file = e.dataTransfer.files[0];
    if (file) handleUploadSemRemat(file, subtipo);
}

async function handleUploadSemRemat(file, subtipo) {
    if (!file) return;
    const ext = file.name.toLowerCase().split('.').pop();
    if (!['xlsx', 'xlsm'].includes(ext)) {
        alert('Aceitos: .xlsx ou .xlsm');
        return;
    }

    const card = document.querySelector('[data-upload-tipo="sem_rematricula"]');
    const progress = card.querySelector('.upload-progress');
    const bar = card.querySelector('.upload-bar');
    const msg = card.querySelector('.upload-msg');
    const statusEl = document.getElementById('sem-remat-status-' + subtipo);

    progress.classList.remove('hidden');
    bar.style.width = '30%';
    msg.textContent = `Enviando ${subtipo}: ${file.name}...`;
    msg.className = 'upload-msg text-xs text-slate-400 mt-1';
    if (statusEl) statusEl.textContent = 'Enviando...';

    const form = new FormData();
    form.append('file', file);
    form.append('tipo', 'sem_rematricula');
    form.append('subtipo', subtipo);

    try {
        bar.style.width = '60%';
        const res = await fetch('/api/upload', { method: 'POST', body: form });
        const data = await res.json();
        bar.style.width = '100%';

        if (data.error) {
            msg.textContent = data.error;
            msg.classList.add('text-red-400');
            if (statusEl) statusEl.textContent = 'Erro';
            setTimeout(() => { progress.classList.add('hidden'); }, 3000);
            return;
        }

        const color = subtipo === 'adimplente' ? 'emerald' : 'amber';
        if (data.snapshot_rows > 0) {
            msg.textContent = `✓ Snapshot criado! (${data.snapshot_rows.toLocaleString('pt-BR')} linhas)`;
            msg.className = 'upload-msg text-xs text-emerald-400 font-semibold mt-1';
            if (statusEl) {
                statusEl.textContent = `✓ ${data.snapshot_rows.toLocaleString('pt-BR')} linhas`;
                statusEl.className = `text-[10px] text-${color}-400 font-semibold mt-1 truncate`;
            }
        } else {
            msg.textContent = `✓ ${subtipo} recebido! Envie o outro arquivo.`;
            msg.className = 'upload-msg text-xs text-amber-400 font-semibold mt-1';
            if (statusEl) {
                statusEl.textContent = `✓ Recebido`;
                statusEl.className = `text-[10px] text-${color}-400 font-semibold mt-1 truncate`;
            }
        }
        loadFileInfo();

        setTimeout(() => {
            bar.style.width = '0%';
            progress.querySelector('.upload-bar').parentElement.classList.add('hidden');
        }, 1500);
    } catch (err) {
        bar.style.width = '100%';
        msg.textContent = 'Erro: ' + err.message;
        msg.classList.add('text-red-400');
        if (statusEl) statusEl.textContent = 'Erro';
        setTimeout(() => {
            progress.classList.add('hidden');
            bar.style.width = '0%';
        }, 3000);
    }
}

async function handleUploadTyped(file, tipo) {
    if (!file) return;
    const ext = file.name.toLowerCase().split('.').pop();
    const allowed = ['xlsx', 'xlsm', 'zip'];
    if (!allowed.includes(ext)) {
        alert('Aceitos: .xlsx, .xlsm ou .zip');
        return;
    }

    const card = document.querySelector(`[data-upload-tipo="${tipo}"]`);
    const progress = card.querySelector('.upload-progress');
    const bar = card.querySelector('.upload-bar');
    const msg = card.querySelector('.upload-msg');
    progress.classList.remove('hidden');
    bar.style.width = '30%';
    msg.textContent = `Enviando ${file.name}...`;
    msg.className = 'upload-msg text-xs text-slate-400 mt-1';

    const form = new FormData();
    form.append('file', file);
    form.append('tipo', tipo);

    try {
        bar.style.width = '60%';
        const res = await fetch('/api/upload', { method: 'POST', body: form });
        const data = await res.json();
        bar.style.width = '100%';

        if (data.error) {
            msg.textContent = data.error;
            msg.classList.add('text-red-400');
            setTimeout(() => { progress.classList.add('hidden'); }, 3000);
            return;
        }

        if (tipo === 'sem_rematricula' && data.snapshot_rows === 0) {
            msg.textContent = `✓ Arquivo recebido! Envie o outro arquivo (adimplente/inadimplente).`;
            msg.className = 'upload-msg text-xs text-amber-400 font-semibold mt-1';
        } else {
            const rowsTxt = data.snapshot_rows >= 0 ? ` (${data.snapshot_rows.toLocaleString('pt-BR')} linhas)` : '';
            msg.textContent = `✓ Upload concluído!${rowsTxt}`;
            msg.className = 'upload-msg text-xs text-emerald-400 font-semibold mt-1';
        }
        loadFileInfo();

        setTimeout(() => {
            bar.style.width = '0%';
            progress.querySelector('.upload-bar').parentElement.classList.add('hidden');
        }, 1500);
    } catch (err) {
        bar.style.width = '100%';
        msg.textContent = 'Erro: ' + err.message;
        msg.classList.add('text-red-400');
        setTimeout(() => {
            progress.classList.add('hidden');
            bar.style.width = '0%';
        }, 3000);
    }

    card.querySelector('input[type="file"]').value = '';
}

async function processServerFolder(tipo) {
    const card = document.querySelector(`[data-upload-tipo="${tipo}"]`);
    const progress = card.querySelector('.upload-progress');
    const bar = card.querySelector('.upload-bar');
    const msg = card.querySelector('.upload-msg');
    progress.classList.remove('hidden');
    bar.style.width = '40%';
    msg.textContent = 'Processando pasta do servidor...';
    msg.className = 'upload-msg text-xs text-slate-400 mt-1';

    try {
        const res = await fetch('/api/upload-folder', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ tipo }),
        });
        const data = await res.json();
        bar.style.width = '100%';
        if (data.error) {
            msg.textContent = data.error;
            msg.classList.add('text-red-400');
        } else {
            msg.textContent = `Processado! ${(data.snapshot_rows||0).toLocaleString('pt-BR')} linhas`;
            msg.classList.add('text-emerald-400');
            loadFileInfo();
        }
    } catch (err) {
        bar.style.width = '100%';
        msg.textContent = 'Erro: ' + err.message;
        msg.classList.add('text-red-400');
    }
    setTimeout(() => { progress.classList.add('hidden'); bar.style.width = '0%'; msg.className = 'upload-msg text-xs text-slate-400 mt-1'; }, 3000);
}

function loadFileInfo() {
    const TIPO_COLORS = {
        matriculados: 'emerald', inadimplentes: 'amber', concluintes: 'purple',
        acesso_ava: 'sky', sem_rematricula: 'rose'
    };
    fetch('/api/upload/info').then(r => r.json()).then(d => {
        const snaps = d.snapshots || {};
        for (const tipo of ['matriculados', 'inadimplentes', 'concluintes', 'acesso_ava', 'sem_rematricula']) {
            const el = document.getElementById('snap-info-' + tipo);
            if (!el) continue;
            const s = snaps[tipo];
            const c = TIPO_COLORS[tipo] || 'slate';
            if (s) {
                el.className = `snap-info mt-3 text-xs border border-${c}-500/20 bg-${c}-500/5 rounded-lg p-2.5`;
                el.innerHTML = `<div class="flex items-center gap-1.5 mb-1">` +
                    `<svg class="w-3.5 h-3.5 text-${c}-400 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/></svg>` +
                    `<span class="text-${c}-300 font-semibold truncate">${esc(s.filename)}</span></div>` +
                    `<div class="text-slate-400 pl-5">${s.row_count.toLocaleString('pt-BR')} linhas &middot; ${s.uploaded_at}</div>`;
            } else {
                el.className = 'snap-info mt-3 text-xs text-slate-500';
                el.textContent = 'Nenhum snapshot';
            }
        }
    });
}
