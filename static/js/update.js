// ---------------------------------------------------------------------------
// Update CRM
// ---------------------------------------------------------------------------
let updatePollTimer = null;
let updateLogOffset = 0;
let _lastUpdateMode = null;

function setUpdateBtnsDisabled(disabled) {
    ['btn-dryrun', 'btn-test', 'btn-execute'].forEach(id => {
        const b = document.getElementById(id);
        b.disabled = disabled;
        b.classList.toggle('opacity-50', disabled);
        b.classList.toggle('cursor-not-allowed', disabled);
    });
    document.getElementById('btn-update-stop').classList.toggle('hidden', !disabled);
}

function updateUpdateBadge(running) {
    _updateRunningFlag = running;
    refreshBadge();
}

function updateRateBar() {
    const val = parseInt(document.getElementById('rate-slider').value);
    const pct = (val / 240) * 100;
    const bar = document.getElementById('rate-bar');
    bar.style.width = pct + '%';
    if (val <= 60) bar.style.background = 'linear-gradient(90deg, #22c55e, #22c55e)';
    else if (val <= 120) bar.style.background = 'linear-gradient(90deg, #22c55e, #2563eb)';
    else if (val <= 180) bar.style.background = 'linear-gradient(90deg, #2563eb, #f59e0b)';
    else bar.style.background = 'linear-gradient(90deg, #f59e0b, #ef4444)';
}

async function startUpdate(mode) {
    setUpdateBtnsDisabled(true);
    updateUpdateBadge(true);
    _lastUpdateMode = mode;

    const rate = parseInt(document.getElementById('rate-slider').value) || 120;
    const withAddress = document.getElementById('with-address').checked;
    const body = { rate, withAddress };
    if (mode === 'execute') {
        const lim = document.getElementById('update-limit').value;
        if (lim) body.limit = parseInt(lim);
    }

    try {
        const res = await api(`/api/update/${mode}`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(body),
        });
        const data = await res.json();
        if (data.error) {
            toast(data.error, 'error');
            setUpdateBtnsDisabled(false);
            updateUpdateBadge(false);
            return;
        }
    } catch (err) {
        toast('Erro: ' + err.message, 'error');
        setUpdateBtnsDisabled(false);
        updateUpdateBadge(false);
        return;
    }

    clearUpdateLog();
    updateLogOffset = 0;
    startUpdatePolling();
}

function startUpdatePolling() {
    if (updatePollTimer) clearInterval(updatePollTimer);
    updatePollTimer = setInterval(pollUpdateLogs, 2000);
    setTimeout(pollUpdateLogs, 500);
}

let _updatePollBusy = false;
async function pollUpdateLogs() {
    if (_updatePollBusy) return;
    _updatePollBusy = true;
    try {
        const res = await fetch(`/api/update/logs?since=${updateLogOffset}`, { credentials: 'same-origin' });
        if (res.status === 401) { window.location.href = '/login'; return; }
        if (!res.ok) { appendUpdateLog(`[POLL ERRO] HTTP ${res.status}`); return; }
        const data = await res.json();
        if (data.lines && data.lines.length > 0) {
            data.lines.forEach(l => appendUpdateLog(l));
        }
        updateLogOffset = data.total || 0;
        if (!data.running) {
            clearInterval(updatePollTimer);
            updatePollTimer = null;
            setUpdateBtnsDisabled(false);
            updateUpdateBadge(false);
            if (_lastUpdateMode === 'dry-run') loadPreview();
        }
    } catch (err) {
        appendUpdateLog(`[POLL ERRO] ${err.message}`);
    } finally {
        _updatePollBusy = false;
    }
}

function confirmExecute() {
    const lim = document.getElementById('update-limit').value;
    const msg = lim
        ? `Executar atualização nos primeiros ${lim} registros?\n\nIsso vai alterar dados no CRM em PRODUÇÃO.`
        : 'Executar atualização em TODOS os registros?\n\nIsso vai alterar dados no CRM em PRODUÇÃO.';
    if (confirm(msg)) startUpdate('execute');
}

function appendUpdateLog(text) {
    const el = document.getElementById('update-log');
    let colored = text;
    if (text.includes('CONFIRMADO') || text.includes('SUCESSO') || text.includes('[FIM]'))
        colored = `<span class="text-emerald-400 font-semibold">${esc(text)}</span>`;
    else if (text.includes('INFO'))
        colored = `<span class="text-gray-300">${esc(text)}</span>`;
    else if (text.includes('WARNING'))
        colored = `<span class="text-yellow-400">${esc(text)}</span>`;
    else if (text.includes('ERROR') || text.includes('FALHOU') || text.includes('[ERRO]'))
        colored = `<span class="text-red-400">${esc(text)}</span>`;
    else if (text.includes('[INÍCIO]'))
        colored = `<span class="text-amber-400 font-semibold">${esc(text)}</span>`;
    else if (text.startsWith('='))
        colored = `<span class="text-indigo-400">${esc(text)}</span>`;
    else colored = esc(text);

    el.innerHTML += colored + '\n';
    el.scrollTop = el.scrollHeight;
}

function clearUpdateLog() {
    document.getElementById('update-log').innerHTML = '';
}

async function loadPreview() {
    try {
        const res = await fetch('/api/update/preview');
        const data = await res.json();
        if (data.error || !data.rows || !data.rows.length) return;

        document.getElementById('update-preview-section').classList.remove('hidden');
        document.getElementById('preview-count').textContent = `(${data.rows.length} registros)`;

        const tbody = document.getElementById('preview-tbody');
        tbody.innerHTML = data.rows.map(r => {
            const matchColor = { 'RGM': 'text-emerald-400', 'CPF': 'text-blue-400', 'TELEFONE': 'text-amber-400', 'NOME': 'text-purple-400' }[r.match_tipo] || 'text-gray-400';
            return `<tr class="border-b border-[var(--border)] hover:bg-gray-100 dark:hover:bg-gray-800/20 transition">
                <td class="px-3 py-1.5 ${matchColor} font-medium">${esc(r.match_tipo || '')}</td>
                <td class="px-3 py-1.5 text-gray-400 font-mono">${esc(r.rgm || '')}</td>
                <td class="px-3 py-1.5 text-gray-200">${esc(r.nome || '')}</td>
                <td class="px-3 py-1.5 text-gray-500 font-mono text-[10px]">${esc((r.lead_id || '').substring(0,8))}...</td>
                <td class="px-3 py-1.5 text-indigo-400">${esc(r.lead_mudancas || '—')}</td>
                <td class="px-3 py-1.5 text-amber-300 max-w-xs truncate" title="${esc(r.biz_mudancas || '')}">${esc(r.biz_mudancas || '—')}</td>
            </tr>`;
        }).join('');
    } catch (err) {
        console.error('Preview load error:', err);
    }
}

async function stopUpdate() {
    if (!confirm('Interromper a atualização do CRM?')) return;
    try { await api('/api/update/stop', { method: 'POST' }); } catch (err) { console.error(err); }
}

// Check update status on load
fetch('/api/update/status', { credentials: 'same-origin' }).then(r => r.json()).then(d => {
    if (d.running) { updateUpdateBadge(true); setUpdateBtnsDisabled(true); startUpdatePolling(); }
}).catch(() => {});

// ---------------------------------------------------------------------------
// Upload helpers (used by _update.html cards)
// ---------------------------------------------------------------------------

function handleDropTyped(e, tipo) {
    e.preventDefault();
    e.currentTarget.classList.remove('border-emerald-500', 'bg-emerald-950/10',
        'border-amber-500', 'bg-amber-950/10', 'border-purple-500', 'bg-purple-950/10',
        'border-sky-500', 'bg-sky-950/10', 'border-rose-500', 'bg-rose-950/10',
        'border-indigo-500', 'bg-indigo-950/10');
    const file = e.dataTransfer.files[0];
    if (file) handleUploadTyped(file, tipo);
}

async function handleUploadTyped(file, tipo) {
    if (!file) return;
    const ext = file.name.toLowerCase().split('.').pop();
    const allowed = ['xlsx', 'xlsm', 'zip'];
    if (!allowed.includes(ext)) {
        toast('Aceitos: .xlsx, .xlsm ou .zip', 'warning');
        return;
    }

    const card = document.querySelector(`[data-upload-tipo="${tipo}"]`);
    const progress = card.querySelector('.upload-progress');
    const bar = card.querySelector('.upload-bar');
    const msg = card.querySelector('.upload-msg');
    progress.classList.remove('hidden');
    bar.style.width = '30%';
    msg.textContent = `Enviando ${file.name}...`;
    msg.className = 'upload-msg text-xs text-gray-400 mt-1';

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
            msg.textContent = '✓ Arquivo recebido! Envie o outro arquivo (adimplente/inadimplente).';
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
        toast('Aceitos: .xlsx ou .xlsm', 'warning');
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
    msg.className = 'upload-msg text-xs text-gray-400 mt-1';
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
                statusEl.textContent = '✓ Recebido';
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

function handleDropBatchInadimplentes(e, nivel) {
    e.preventDefault();
    e.currentTarget.classList.remove('border-amber-500', 'bg-amber-950/10',
        'border-orange-500', 'bg-orange-950/10');
    const files = e.dataTransfer.files;
    if (files && files.length > 0) handleUploadBatchInadimplentes(files, nivel);
}

async function handleUploadBatchInadimplentes(files, nivel) {
    if (!files || files.length === 0) return;

    const card = document.querySelector('[data-upload-tipo="inadimplentes"]');
    const progress = card.querySelector('.upload-progress');
    const bar = card.querySelector('.upload-bar');
    const msg = card.querySelector('.upload-msg');

    progress.classList.remove('hidden');
    bar.style.width = '30%';
    msg.textContent = `Enviando ${files.length} arquivo(s) [${nivel}]...`;
    msg.className = 'upload-msg text-xs text-gray-400 mt-1';

    const form = new FormData();
    for (let i = 0; i < files.length; i++) {
        form.append('files', files[i]);
    }
    form.append('tipo', 'inadimplentes');
    form.append('nivel', nivel);

    try {
        bar.style.width = '60%';
        const res = await fetch('/api/upload-batch', { method: 'POST', body: form });
        const data = await res.json();
        bar.style.width = '100%';

        if (data.error) {
            msg.textContent = data.error;
            msg.classList.add('text-red-400');
            setTimeout(() => { progress.classList.add('hidden'); }, 3000);
            return;
        }

        const rowsTxt = data.snapshot_rows > 0 ? ` (${data.snapshot_rows.toLocaleString('pt-BR')} linhas)` : '';
        msg.textContent = `✓ ${data.files_count || files.length} arquivo(s) processado(s)!${rowsTxt}`;
        msg.className = 'upload-msg text-xs text-emerald-400 font-semibold mt-1';
        if (data.warning) {
            msg.textContent += ' ⚠ ' + data.warning;
            msg.className = 'upload-msg text-xs text-amber-400 font-semibold mt-1';
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
}

async function processServerFolder(tipo) {
    const card = document.querySelector(`[data-upload-tipo="${tipo}"]`);
    const progress = card.querySelector('.upload-progress');
    const bar = card.querySelector('.upload-bar');
    const msg = card.querySelector('.upload-msg');
    progress.classList.remove('hidden');
    bar.style.width = '40%';
    msg.textContent = 'Processando pasta do servidor...';
    msg.className = 'upload-msg text-xs text-gray-400 mt-1';

    try {
        const res = await fetch('/api/upload-folder', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ tipo }),
        });
        const data = await res.json();
        bar.style.width = '100%';
        if (data.error) {
            msg.textContent = data.error;
            msg.classList.add('text-red-400');
        } else {
            msg.textContent = `Processado! ${(data.snapshot_rows || 0).toLocaleString('pt-BR')} linhas`;
            msg.classList.add('text-emerald-400');
            loadFileInfo();
        }
    } catch (err) {
        bar.style.width = '100%';
        msg.textContent = 'Erro: ' + err.message;
        msg.classList.add('text-red-400');
    }
    setTimeout(() => {
        progress.classList.add('hidden');
        bar.style.width = '0%';
        msg.className = 'upload-msg text-xs text-gray-400 mt-1';
    }, 3000);
}

function loadFileInfo() {
    const TIPO_COLORS = {
        matriculados: 'emerald', inadimplentes: 'amber', concluintes: 'purple',
        acesso_ava: 'sky', sem_rematricula: 'rose', lista_alunos: 'indigo'
    };
    fetch('/api/upload/info').then(r => r.json()).then(d => {
        const snaps = d.snapshots || {};
        for (const tipo of Object.keys(TIPO_COLORS)) {
            const el = document.getElementById('snap-info-' + tipo);
            if (!el) continue;
            const s = snaps[tipo];
            const c = TIPO_COLORS[tipo] || 'slate';
            if (s) {
                el.className = `snap-info mt-3 text-xs border border-${c}-500/20 bg-${c}-500/5 rounded-lg p-2.5`;
                el.innerHTML = `<div class="flex items-center gap-1.5 mb-1">` +
                    `<svg class="w-3.5 h-3.5 text-${c}-400 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/></svg>` +
                    `<span class="text-${c}-300 font-semibold truncate">${esc(s.filename)}</span></div>` +
                    `<div class="text-gray-400 pl-5">${s.row_count.toLocaleString('pt-BR')} linhas &middot; ${s.uploaded_at}</div>`;
            } else {
                el.className = 'snap-info mt-3 text-xs text-gray-500';
                el.textContent = 'Nenhum snapshot';
            }
        }
    }).catch(() => {});
}
