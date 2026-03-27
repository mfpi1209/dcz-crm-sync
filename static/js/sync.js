// ---------------------------------------------------------------------------
// Sync
// ---------------------------------------------------------------------------
async function loadSyncState() {
    try {
        const res = await fetch('/api/sync-state');
        const data = await res.json();

        const stateDiv = document.getElementById('sync-state-table');
        if (data.states && data.states.length) {
            stateDiv.innerHTML = `<table class="w-full text-left">
                <thead><tr class="text-xs text-slate-500 border-b border-slate-700/20">
                    <th class="pb-2 font-semibold">Entidade</th><th class="pb-2 font-semibold">Último sync</th><th class="pb-2 font-semibold">Runs</th>
                </tr></thead>
                <tbody>${data.states.map(s => `<tr class="border-b border-slate-700/10 hover:bg-slate-800/20 transition">
                    <td class="py-2 text-slate-300 font-mono text-xs">${esc(s.entity_type)}</td>
                    <td class="py-2 text-slate-400 text-xs">${s.last_sync_at ? fmtDate(s.last_sync_at) : '—'}</td>
                    <td class="py-2 text-slate-300 font-semibold">${s.run_count || 0}</td>
                </tr>`).join('')}</tbody></table>`;
        } else {
            stateDiv.textContent = 'Nenhuma sincronização realizada ainda.';
        }

        const recentDiv = document.getElementById('recent-updates');
        if (data.recent_updates && data.recent_updates.length) {
            recentDiv.innerHTML = data.recent_updates.map(u => `
                <div class="flex items-center justify-between py-2 border-b border-slate-700/10 hover:bg-slate-800/10 transition">
                    <div>
                        <span class="text-slate-300 text-sm">${esc(u.nome_lead || '—')}</span>
                        <span class="text-slate-600 text-xs ml-2">${esc(u.pipeline || '')} &rarr; ${esc(u.etapa || '')}</span>
                    </div>
                    <span class="tag-pill ${u.status === 'won' ? 'bg-emerald-500/15 text-emerald-400' : u.status === 'lost' ? 'bg-red-500/15 text-red-400' : 'bg-blue-500/15 text-blue-400'}">
                        ${{won:'Ganho', in_process:'Aberto', lost:'Perdido'}[u.status] || u.status}
                    </span>
                </div>
            `).join('');
        } else {
            recentDiv.textContent = 'Nenhuma atualização recente.';
        }
    } catch (err) {
        console.error(err);
    }
}

let syncPollTimer = null;
let syncLogOffset = 0;

async function startSync(mode) {
    try {
        const res = await api(`/api/sync/${mode}`, { method: 'POST' });
        const data = await res.json();
        if (data.error) { toast(data.error, 'error'); return; }
    } catch (err) { toast('Erro: ' + err.message, 'error'); return; }

    clearLog();
    syncLogOffset = 0;
    setSyncBtnsState(true);
    appendLog('[INÍCIO] Aguardando logs do servidor...');
    startSyncPolling();
}

function setSyncBtnsState(running) {
    const btnDelta = document.getElementById('btn-delta');
    const btnFull = document.getElementById('btn-full');
    btnDelta.disabled = btnFull.disabled = running;
    btnDelta.classList.toggle('opacity-50', running);
    btnFull.classList.toggle('opacity-50', running);
    document.getElementById('btn-sync-stop').classList.toggle('hidden', !running);
    _syncRunningFlag = running;
    refreshBadge();
}

function startSyncPolling() {
    if (syncPollTimer) clearInterval(syncPollTimer);
    syncPollTimer = setInterval(pollSyncLogs, 2000);
    setTimeout(pollSyncLogs, 500);
}

let _syncPollBusy = false;
async function pollSyncLogs() {
    if (_syncPollBusy) return;
    _syncPollBusy = true;
    try {
        const url = `/api/sync/logs?since=${syncLogOffset}`;
        const res = await fetch(url, { credentials: 'same-origin' });
        if (res.status === 401) { window.location.href = '/login'; return; }
        if (!res.ok) { appendLog(`[POLL ERRO] HTTP ${res.status}`); return; }
        const data = await res.json();
        if (data.lines && data.lines.length > 0) {
            data.lines.forEach(l => appendLog(l));
        }
        syncLogOffset = data.total || 0;
        if (!data.running) {
            clearInterval(syncPollTimer);
            syncPollTimer = null;
            setSyncBtnsState(false);
            loadSyncState();
        }
    } catch (err) {
        appendLog(`[POLL ERRO] ${err.message}`);
    } finally {
        _syncPollBusy = false;
    }
}

function appendLog(text) {
    const el = document.getElementById('sync-log');
    let colored = text;
    if (text.includes('INFO')) colored = `<span class="text-slate-300">${esc(text)}</span>`;
    else if (text.includes('WARNING')) colored = `<span class="text-yellow-400">${esc(text)}</span>`;
    else if (text.includes('ERROR') || text.includes('[ERRO]') || text.includes('POLL ERRO')) colored = `<span class="text-red-400">${esc(text)}</span>`;
    else if (text.includes('[INÍCIO]') || text.includes('[FIM]') || text.includes('[AGENDADO]')) colored = `<span class="text-indigo-400 font-semibold">${esc(text)}</span>`;
    else colored = esc(text);

    el.innerHTML += colored + '\n';
    el.scrollTop = el.scrollHeight;
}

function clearLog() {
    document.getElementById('sync-log').innerHTML = '';
}

async function stopSync() {
    if (!confirm('Interromper a sincronização?')) return;
    try { await api('/api/sync/stop', { method: 'POST' }); } catch (err) { console.error(err); }
}

// Check sync status on load
fetch('/api/sync/status', { credentials: 'same-origin' }).then(r => r.json()).then(d => {
    if (d.running) { setSyncBtnsState(true); startSyncPolling(); }
}).catch(() => {});
