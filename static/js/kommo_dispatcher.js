/* Kommo Dispatcher — Dashboard nativo */

let _kdTimer = null;

function loadKommoDispatcher() {
    _kdFetchStats();
    if (_kdTimer) clearInterval(_kdTimer);
    _kdTimer = setInterval(_kdFetchStats, 10000);
}

function _kdStopRefresh() {
    if (_kdTimer) { clearInterval(_kdTimer); _kdTimer = null; }
}

function _kdFetchStats() {
    fetch('/api/kommo-dispatcher/stats')
        .then(r => r.json())
        .then(data => {
            if (data.error) {
                _kdSetStatus('offline', data.error);
                return;
            }
            _kdSetStatus('online');
            _kdRenderKPIs(data);
            _kdRenderFila(data.n8n || {});
            _kdRenderToken(data.token || {});
            _kdRenderRate(data.rate_limit || {});
            _kdRenderPolling(data.polling_tiers || {});
            _kdRenderDispatches((data.n8n || {}).recent || []);
            _kdRenderMessages(data.recent_messages || []);
            _kdRenderTopChats(data.top_chats || []);
        })
        .catch(() => _kdSetStatus('offline', 'Sem conexão'));
}

function _kdSetStatus(state, msg) {
    const badge = document.getElementById('kd-status-badge');
    const ts = document.getElementById('kd-last-update');
    if (state === 'online') {
        badge.innerHTML = '<span class="w-1.5 h-1.5 rounded-full bg-emerald-400"></span> Online';
        badge.className = 'text-xs px-2.5 py-1 rounded-full bg-emerald-900/40 text-emerald-400 flex items-center gap-1.5';
        ts.textContent = new Date().toLocaleTimeString('pt-BR') + ' | auto-refresh 10s';
    } else {
        badge.innerHTML = '<span class="w-1.5 h-1.5 rounded-full bg-rose-400"></span> ' + (msg || 'Offline');
        badge.className = 'text-xs px-2.5 py-1 rounded-full bg-rose-900/40 text-rose-400 flex items-center gap-1.5';
    }
}

function _kdFmt(n) {
    if (n == null) return '—';
    return Number(n).toLocaleString('pt-BR');
}

function _kdRenderKPIs(d) {
    const bt = d.by_type || {};
    document.getElementById('kd-chats-monit').textContent = _kdFmt(d.active_chats);
    document.getElementById('kd-chats-sync').textContent = _kdFmt(d.synced_chats);
    document.getElementById('kd-pending-sync').textContent = _kdFmt(d.pending_sync_chats);
    document.getElementById('kd-total-msgs').textContent = _kdFmt(d.total_messages);
    document.getElementById('kd-n8n-sent').textContent = _kdFmt((d.n8n || {}).sent_ok);
    document.getElementById('kd-n8n-failed').textContent = _kdFmt((d.n8n || {}).failed);
    document.getElementById('kd-n8n-pending').textContent = _kdFmt((d.n8n || {}).pending);
    document.getElementById('kd-audios').textContent = _kdFmt(bt.voice || 0);
}

function _kdRenderFila(n8n) {
    document.getElementById('kd-fila-pending').textContent = _kdFmt(n8n.pending);
    document.getElementById('kd-fila-total').textContent = _kdFmt(n8n.total_enqueued);
    document.getElementById('kd-fila-ok').textContent = _kdFmt(n8n.sent_ok);
    document.getElementById('kd-fila-fail').textContent = _kdFmt(n8n.failed);
    const whEl = document.getElementById('kd-fila-webhook');
    const wh = n8n.webhook_url || '—';
    whEl.textContent = wh;
    whEl.title = wh;
}

function _kdRenderToken(tok) {
    const dot = document.getElementById('kd-token-dot');
    const statusEl = document.getElementById('kd-token-status');
    const expiryEl = document.getElementById('kd-token-expiry');
    const previewEl = document.getElementById('kd-token-preview');

    const expired = tok.is_expired;
    dot.className = 'w-2 h-2 rounded-full ' + (expired ? 'bg-rose-400' : 'bg-emerald-400');
    statusEl.textContent = expired ? 'EXPIRADO' : 'ATIVO';
    statusEl.className = 'font-bold ' + (expired ? 'text-rose-400' : 'text-emerald-400');

    const secs = tok.seconds_until_expiry || 0;
    if (secs > 0) {
        const h = Math.floor(secs / 3600);
        const m = Math.floor((secs % 3600) / 60);
        expiryEl.textContent = h > 0 ? `${h}h ${m}m` : `${m}m`;
    } else {
        expiryEl.textContent = '—';
    }
    previewEl.textContent = tok.token_preview || '—';
}

function _kdRenderRate(rl) {
    const rpm = rl.requests_last_minute || 0;
    const max = rl.max_rpm || 420;
    const pct = Math.min((rpm / max) * 100, 100);
    const bar = document.getElementById('kd-rate-bar');
    bar.style.width = pct + '%';

    if (pct > 85) bar.style.background = 'linear-gradient(90deg, #f59e0b, #ef4444)';
    else if (pct > 60) bar.style.background = 'linear-gradient(90deg, #22c55e, #f59e0b)';
    else bar.style.background = 'linear-gradient(90deg, #22c55e, #3b82f6)';

    document.getElementById('kd-rate-text').textContent = `${rpm} / ${max} rpm`;
}

function _kdRenderPolling(tiers) {
    document.getElementById('kd-tier-hot').textContent = _kdFmt(tiers.hot);
    document.getElementById('kd-tier-warm').textContent = _kdFmt(tiers.warm);
    document.getElementById('kd-tier-cold').textContent = _kdFmt(tiers.cold);
    document.getElementById('kd-tier-frozen').textContent = _kdFmt(tiers.frozen);
}

function _kdRenderDispatches(recent) {
    const tbody = document.getElementById('kd-dispatches-tbody');
    if (!recent.length) {
        tbody.innerHTML = '<tr><td colspan="6" class="py-4 text-center text-gray-600">Nenhum dispatch recente</td></tr>';
        return;
    }
    tbody.innerHTML = recent.slice(0, 30).map(d => {
        const httpColor = d.http && d.http >= 200 && d.http < 300 ? 'text-green-400' :
                          d.http && d.http >= 400 ? 'text-rose-400' : 'text-gray-400';
        const statusColor = d.result === 'ok' || d.result === 'sent' ? 'text-green-400' :
                            d.result === 'failed' || d.result === 'error' ? 'text-rose-400' : 'text-gray-400';
        return `<tr class="hover:bg-gray-100 dark:hover:bg-gray-800/30">
            <td class="py-1.5 px-3 text-gray-500 font-mono">${_kdTime(d.ts)}</td>
            <td class="py-1.5 px-3 text-gray-300">${d.event || '—'}</td>
            <td class="py-1.5 px-3 text-gray-400 font-mono">${d.lead || d.uid || '—'}</td>
            <td class="py-1.5 px-3 text-gray-400">${d.type || '—'}</td>
            <td class="py-1.5 px-3 ${httpColor} font-mono">${d.http || '—'}</td>
            <td class="py-1.5 px-3 ${statusColor}">${d.result || '—'}</td>
        </tr>`;
    }).join('');
}

function _kdRenderMessages(msgs) {
    const tbody = document.getElementById('kd-messages-tbody');
    if (!msgs.length) {
        tbody.innerHTML = '<tr><td colspan="6" class="py-4 text-center text-gray-600">Nenhuma mensagem recente</td></tr>';
        return;
    }
    tbody.innerHTML = msgs.slice(0, 30).map(m => {
        const dirIcon = m.direction === 'outgoing'
            ? '<span class="text-blue-400" title="Saída">↑</span>'
            : '<span class="text-green-400" title="Entrada">↓</span>';
        const typeColor = m.message_type === 'voice' ? 'text-fuchsia-400' :
                          m.message_type === 'picture' ? 'text-amber-400' : 'text-gray-400';
        const txt = (m.text || '').length > 50 ? m.text.substring(0, 50) + '…' : (m.text || '—');
        return `<tr class="hover:bg-gray-100 dark:hover:bg-gray-800/30">
            <td class="py-1.5 px-2 text-gray-500 font-mono text-[10px]">${_kdTime(m.sent_at)}</td>
            <td class="py-1.5 px-2 text-gray-400 truncate max-w-[100px]" title="${m.chat_label || m.chat_id || ''}">${m.chat_label || m.chat_id || '—'}</td>
            <td class="py-1.5 px-2 text-gray-300 truncate max-w-[80px]">${m.sender_name || m.sender_type || '—'}</td>
            <td class="py-1.5 px-2">${dirIcon}</td>
            <td class="py-1.5 px-2 ${typeColor}">${m.message_type || '—'}</td>
            <td class="py-1.5 px-2 text-gray-400 truncate max-w-[200px]" title="${(m.text || '').replace(/"/g, '&quot;')}">${txt}</td>
        </tr>`;
    }).join('');
}

function _kdRenderTopChats(chats) {
    const tbody = document.getElementById('kd-topchats-tbody');
    if (!chats.length) {
        tbody.innerHTML = '<tr><td colspan="3" class="py-4 text-center text-gray-600">Nenhum chat ativo</td></tr>';
        return;
    }
    tbody.innerHTML = chats.slice(0, 20).map(c => {
        return `<tr class="hover:bg-gray-100 dark:hover:bg-gray-800/30">
            <td class="py-1.5 px-2 text-gray-300 truncate max-w-[200px]" title="${c.label || c.chat_id}">${c.label || c.chat_id}</td>
            <td class="py-1.5 px-2 text-indigo-400 font-mono font-bold">${_kdFmt(c.msg_count)}</td>
            <td class="py-1.5 px-2 text-gray-500 font-mono text-[10px]">${_kdTime(c.last_at)}</td>
        </tr>`;
    }).join('');
}

function _kdTime(ts) {
    if (!ts) return '—';
    try {
        const d = new Date(ts);
        if (isNaN(d)) return ts;
        return d.toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    } catch { return ts; }
}
