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
            alert(data.error);
            setUpdateBtnsDisabled(false);
            updateUpdateBadge(false);
            return;
        }
    } catch (err) {
        alert('Erro: ' + err.message);
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
        colored = `<span class="text-slate-300">${esc(text)}</span>`;
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
            const matchColor = { 'RGM': 'text-emerald-400', 'CPF': 'text-blue-400', 'TELEFONE': 'text-amber-400', 'NOME': 'text-purple-400' }[r.match_tipo] || 'text-slate-400';
            return `<tr class="border-b border-slate-700/10 hover:bg-slate-800/20 transition">
                <td class="px-3 py-1.5 ${matchColor} font-medium">${esc(r.match_tipo || '')}</td>
                <td class="px-3 py-1.5 text-slate-400 font-mono">${esc(r.rgm || '')}</td>
                <td class="px-3 py-1.5 text-slate-200">${esc(r.nome || '')}</td>
                <td class="px-3 py-1.5 text-slate-500 font-mono text-[10px]">${esc((r.lead_id || '').substring(0,8))}...</td>
                <td class="px-3 py-1.5 text-cyan-400">${esc(r.lead_mudancas || '—')}</td>
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
