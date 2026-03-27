// ---------------------------------------------------------------------------
// Saneamento
// ---------------------------------------------------------------------------
let _sanPolling = null;

async function startSanitize(mode) {
    const body = { rate: parseInt(document.getElementById('san-rate-slider').value) || 60 };
    if (mode === 'execute') {
        const lim = document.getElementById('sanitize-limit').value;
        if (lim) body.limit = parseInt(lim);
    }
    showPipelineLog('sanitize');
    try {
        const res = await api('/api/sanitize/' + mode, { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(body) });
        const d = await res.json();
        if (d.error) { toast(d.error, 'error'); return; }
        setSanBtns(true);
        startSanPolling();
    } catch(e) { toast('Erro: ' + e.message, 'error'); }
}

function confirmSanitize() {
    if (!confirm('Executar exclusão em massa de negócios duplicados? Esta ação é IRREVERSÍVEL.')) return;
    startSanitize('execute');
}

function setSanBtns(running) {
    ['btn-san-dryrun','btn-san-test','btn-san-execute'].forEach(id => {
        const el = document.getElementById(id);
        if (el) { el.disabled = running; el.classList.toggle('opacity-50', running); }
    });
    const stop = document.getElementById('btn-san-stop');
    if (stop) stop.classList.toggle('hidden', !running);
}

function startSanPolling() {
    let since = 0;
    if (_sanPolling) clearInterval(_sanPolling);
    _sanPolling = setInterval(async () => {
        try {
            const res = await api('/api/sanitize/logs?since=' + since);
            const d = await res.json();
            const el = document.getElementById('sanitize-log');
            if (d.lines && d.lines.length > 0) {
                d.lines.forEach(l => { el.textContent += l + '\n'; });
                el.scrollTop = el.scrollHeight;
            }
            since = d.total;
            if (!d.running) { clearInterval(_sanPolling); _sanPolling = null; setSanBtns(false); }
        } catch(e) { console.error(e); }
    }, 1500);
}

async function stopSanitize() {
    if (!confirm('Interromper o saneamento?')) return;
    try { await api('/api/sanitize/stop', { method: 'POST' }); } catch(e) { console.error(e); }
}

// Check sanitize status on load
fetch('/api/sanitize/status', { credentials: 'same-origin' }).then(r => r.json()).then(d => {
    if (d.running) { setSanBtns(true); startSanPolling(); }
}).catch(() => {});

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------
let _pipePolling = null;

async function startPipeline(mode) {
    const body = { rate: parseInt(document.getElementById('san-rate-slider').value) || 60 };
    if (mode === 'execute') {
        const lim = document.getElementById('pipeline-limit').value;
        if (lim) body.limit = parseInt(lim);
    }
    showPipelineLog('pipeline');
    try {
        const res = await api('/api/pipeline/' + mode, { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(body) });
        const d = await res.json();
        if (d.error) { toast(d.error, 'error'); return; }
        setPipeBtns(true);
        startPipePolling();
    } catch(e) { toast('Erro: ' + e.message, 'error'); }
}

function confirmPipeline() {
    if (!confirm('Executar movimentação de pipeline em massa? Negócios serão movidos/perdidos/restaurados.')) return;
    startPipeline('execute');
}

function setPipeBtns(running) {
    ['btn-pipe-dryrun','btn-pipe-test','btn-pipe-execute'].forEach(id => {
        const el = document.getElementById(id);
        if (el) { el.disabled = running; el.classList.toggle('opacity-50', running); }
    });
    const stop = document.getElementById('btn-pipe-stop');
    if (stop) stop.classList.toggle('hidden', !running);
}

function startPipePolling() {
    let since = 0;
    if (_pipePolling) clearInterval(_pipePolling);
    _pipePolling = setInterval(async () => {
        try {
            const res = await api('/api/pipeline/logs?since=' + since);
            const d = await res.json();
            const el = document.getElementById('pipeline-log');
            if (d.lines && d.lines.length > 0) {
                d.lines.forEach(l => { el.textContent += l + '\n'; });
                el.scrollTop = el.scrollHeight;
            }
            since = d.total;
            if (!d.running) { clearInterval(_pipePolling); _pipePolling = null; setPipeBtns(false); }
        } catch(e) { console.error(e); }
    }, 1500);
}

async function stopPipeline() {
    if (!confirm('Interromper o pipeline?')) return;
    try { await api('/api/pipeline/stop', { method: 'POST' }); } catch(e) { console.error(e); }
}

function showPipelineLog(which) {
    const logs = ['sanitize-log', 'enrich-log', 'merge-log', 'pipeline-log', 'inadimplentes-log', 'concluintes-log', 'lostdup-log'];
    const tabs = ['tab-san-log', 'tab-enrich-log', 'tab-merge-log', 'tab-pipe-log', 'tab-inad-log', 'tab-conc-log', 'tab-lostdup-log'];
    const map = { sanitize: 0, enrich: 1, merge: 2, pipeline: 3, inadimplentes: 4, concluintes: 5, lostdup: 6 };
    const idx = map[which] ?? 0;
    logs.forEach((id, i) => {
        const el = document.getElementById(id);
        if (el) el.classList.toggle('hidden', i !== idx);
    });
    tabs.forEach((id, i) => {
        const el = document.getElementById(id);
        if (!el) return;
        if (i === idx) {
            el.classList.add('text-indigo-400','border-b-2','border-indigo-400');
            el.classList.remove('text-gray-500');
        } else {
            el.classList.remove('text-indigo-400','border-b-2','border-indigo-400');
            el.classList.add('text-gray-500');
        }
    });
}

// ---------------------------------------------------------------------------
// Enriquecimento de duplicatas
// ---------------------------------------------------------------------------
let _enrichPolling = null;

async function startEnrich() {
    const rate = parseInt(document.getElementById('san-rate-slider').value) || 60;
    showPipelineLog('enrich');
    try {
        const res = await api('/api/enrich/start', {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({ rate })
        });
        const d = await res.json();
        if (d.error) { toast(d.error, 'error'); return; }
        setEnrichBtns(true);
        startEnrichPolling();
    } catch(e) { toast('Erro: ' + e.message, 'error'); }
}

function setEnrichBtns(running) {
    const btn = document.getElementById('btn-enrich-start');
    if (btn) { btn.disabled = running; btn.classList.toggle('opacity-50', running); }
    const stop = document.getElementById('btn-enrich-stop');
    if (stop) stop.classList.toggle('hidden', !running);
}

function startEnrichPolling() {
    let since = 0;
    if (_enrichPolling) clearInterval(_enrichPolling);
    _enrichPolling = setInterval(async () => {
        try {
            const res = await api('/api/enrich/logs?since=' + since);
            const d = await res.json();
            const el = document.getElementById('enrich-log');
            if (d.lines && d.lines.length > 0) {
                d.lines.forEach(l => { el.textContent += l + '\n'; });
                el.scrollTop = el.scrollHeight;
            }
            since = d.total;
            if (!d.running) { clearInterval(_enrichPolling); _enrichPolling = null; setEnrichBtns(false); }
        } catch(e) { console.error(e); }
    }, 1500);
}

async function stopEnrich() {
    if (!confirm('Interromper o enriquecimento?')) return;
    try { await api('/api/enrich/stop', { method: 'POST' }); } catch(e) { console.error(e); }
}

function clearPipelineLog() {
    ['sanitize-log','enrich-log','merge-log','pipeline-log','inadimplentes-log','concluintes-log','lostdup-log'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.textContent = '';
    });
}

// ---------------------------------------------------------------------------
// Merge de leads duplicados
// ---------------------------------------------------------------------------
let _mergePolling = null;

async function startMerge(mode) {
    const body = {
        mode: mode,
        rate: parseInt(document.getElementById('san-rate-slider').value) || 60,
    };
    const fase = document.getElementById('merge-fase').value;
    if (fase) body.fase = parseInt(fase);
    if (mode === 'execute') {
        const lim = document.getElementById('merge-limit').value;
        if (lim) body.limit = parseInt(lim);
    }
    showPipelineLog('merge');
    try {
        const res = await api('/api/merge/start', {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify(body),
        });
        const d = await res.json();
        if (d.error) { toast(d.error, 'error'); return; }
        setMergeBtns(true);
        startMergePolling();
    } catch(e) { toast('Erro: ' + e.message, 'error'); }
}

function confirmMerge() {
    const fase = document.getElementById('merge-fase').value;
    const label = fase ? ` (fase ${fase})` : ' (TODAS as fases)';
    if (!confirm(`Executar merge de leads duplicados${label}? Negócios serão movidos e leads vazios serão deletados.`)) return;
    startMerge('execute');
}

function setMergeBtns(running) {
    ['btn-merge-dryrun','btn-merge-test','btn-merge-execute'].forEach(id => {
        const el = document.getElementById(id);
        if (el) { el.disabled = running; el.classList.toggle('opacity-50', running); }
    });
    const stop = document.getElementById('btn-merge-stop');
    if (stop) stop.classList.toggle('hidden', !running);
}

function startMergePolling() {
    let since = 0;
    if (_mergePolling) clearInterval(_mergePolling);
    _mergePolling = setInterval(async () => {
        try {
            const res = await api('/api/merge/logs?since=' + since);
            const d = await res.json();
            const el = document.getElementById('merge-log');
            if (d.lines && d.lines.length > 0) {
                d.lines.forEach(l => { el.textContent += l + '\n'; });
                el.scrollTop = el.scrollHeight;
            }
            since = d.total;
            if (!d.running) { clearInterval(_mergePolling); _mergePolling = null; setMergeBtns(false); }
        } catch(e) { console.error(e); }
    }, 1500);
}

async function stopMerge() {
    if (!confirm('Interromper o merge?')) return;
    try { await api('/api/merge/stop', { method: 'POST' }); } catch(e) { console.error(e); }
}

// Check pipeline status on load
fetch('/api/pipeline/status', { credentials: 'same-origin' }).then(r => r.json()).then(d => {
    if (d.running) { setPipeBtns(true); startPipePolling(); showPipelineLog('pipeline'); }
}).catch(() => {});

// ---------------------------------------------------------------------------
// Inadimplentes
// ---------------------------------------------------------------------------
let _inadPolling = null;

async function startInadimplentes(mode) {
    const rate = parseInt(document.getElementById('san-rate-slider')?.value) || 120;
    showPipelineLog('inadimplentes');
    try {
        const res = await api(`/api/inadimplentes/${mode}`, {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({ rate })
        });
        const d = await res.json();
        if (d.error) { toast(d.error, 'error'); return; }
        setInadBtns(true);
        startInadPolling();
    } catch(e) { toast('Erro: ' + e.message, 'error'); }
}

function confirmInadimplentes() {
    if (!confirm('Executar atualização de inadimplentes no CRM?')) return;
    startInadimplentes('execute');
}

function setInadBtns(running) {
    ['btn-inad-dryrun','btn-inad-execute'].forEach(id => {
        const el = document.getElementById(id);
        if (el) { el.disabled = running; el.classList.toggle('opacity-50', running); }
    });
    const stop = document.getElementById('btn-inad-stop');
    if (stop) stop.classList.toggle('hidden', !running);
}

function startInadPolling() {
    let since = 0;
    if (_inadPolling) clearInterval(_inadPolling);
    _inadPolling = setInterval(async () => {
        try {
            const res = await api('/api/inadimplentes/logs?since=' + since);
            const d = await res.json();
            const el = document.getElementById('inadimplentes-log');
            if (d.lines && d.lines.length > 0) {
                d.lines.forEach(l => { el.textContent += l + '\n'; });
                el.scrollTop = el.scrollHeight;
            }
            since = d.total;
            if (!d.running) { clearInterval(_inadPolling); _inadPolling = null; setInadBtns(false); }
        } catch(e) { console.error(e); }
    }, 1500);
}

async function stopInadimplentes() {
    if (!confirm('Interromper atualização de inadimplentes?')) return;
    try { await api('/api/inadimplentes/stop', { method: 'POST' }); } catch(e) { console.error(e); }
}

fetch('/api/inadimplentes/status', { credentials: 'same-origin' }).then(r => r.json()).then(d => {
    if (d.running) { setInadBtns(true); startInadPolling(); showPipelineLog('inadimplentes'); }
}).catch(() => {});

// ---------------------------------------------------------------------------
// Concluintes
// ---------------------------------------------------------------------------
let _concPolling = null;

async function startConcluintes(mode) {
    const rate = parseInt(document.getElementById('san-rate-slider')?.value) || 120;
    showPipelineLog('concluintes');
    try {
        const res = await api(`/api/concluintes/${mode}`, {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({ rate })
        });
        const d = await res.json();
        if (d.error) { toast(d.error, 'error'); return; }
        setConcBtns(true);
        startConcPolling();
    } catch(e) { toast('Erro: ' + e.message, 'error'); }
}

function confirmConcluintes() {
    if (!confirm('Executar atualização de concluintes no CRM?')) return;
    startConcluintes('execute');
}

function setConcBtns(running) {
    ['btn-conc-dryrun','btn-conc-execute'].forEach(id => {
        const el = document.getElementById(id);
        if (el) { el.disabled = running; el.classList.toggle('opacity-50', running); }
    });
    const stop = document.getElementById('btn-conc-stop');
    if (stop) stop.classList.toggle('hidden', !running);
}

function startConcPolling() {
    let since = 0;
    if (_concPolling) clearInterval(_concPolling);
    _concPolling = setInterval(async () => {
        try {
            const res = await api('/api/concluintes/logs?since=' + since);
            const d = await res.json();
            const el = document.getElementById('concluintes-log');
            if (d.lines && d.lines.length > 0) {
                d.lines.forEach(l => { el.textContent += l + '\n'; });
                el.scrollTop = el.scrollHeight;
            }
            since = d.total;
            if (!d.running) { clearInterval(_concPolling); _concPolling = null; setConcBtns(false); }
        } catch(e) { console.error(e); }
    }, 1500);
}

async function stopConcluintes() {
    if (!confirm('Interromper atualização de concluintes?')) return;
    try { await api('/api/concluintes/stop', { method: 'POST' }); } catch(e) { console.error(e); }
}

fetch('/api/concluintes/status', { credentials: 'same-origin' }).then(r => r.json()).then(d => {
    if (d.running) { setConcBtns(true); startConcPolling(); showPipelineLog('concluintes'); }
}).catch(() => {});

// ---------------------------------------------------------------------------
// Sanitizar Perdidos — merge duplicatas em status Perdido
// ---------------------------------------------------------------------------
let _lostDupGroups = [];
let _lostDupPolling = null;

function _updateSessionBadge(status, source) {
    const badge = document.getElementById('lostdup-session-badge');
    if (!badge) return;
    if (status === 'ok') {
        badge.textContent = source === 'manual' ? 'manual' : 'dispatcher';
        badge.className = 'text-[10px] px-1.5 py-0.5 rounded font-medium bg-emerald-500/20 text-emerald-400';
    } else {
        badge.textContent = 'desconectado';
        badge.className = 'text-[10px] px-1.5 py-0.5 rounded font-medium bg-red-500/20 text-red-400';
    }
}

async function setLostDupCookies() {
    const input = document.getElementById('lostdup-cookie-input');
    const raw = (input && input.value || '').trim();
    if (!raw) { toast('Cole o cookie string', 'error'); return; }

    const body = raw.includes('=') && !raw.startsWith('{')
        ? { cookie_string: raw }
        : (() => { try { return { cookies: JSON.parse(raw) }; } catch { return { cookie_string: raw }; } })();

    try {
        const res = await api('/api/kommo/merge/manual-cookies', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
        });
        const d = await res.json();
        if (d.ok) {
            toast(`Cookies salvos (${d.count} keys)`, 'success');
            _updateSessionBadge('ok', 'manual');
            if (input) input.value = '';
        } else {
            toast(d.error || 'Erro', 'error');
        }
    } catch(e) { toast('Erro: ' + e.message, 'error'); }
}

async function clearLostDupCookies() {
    try {
        await api('/api/kommo/merge/manual-cookies', { method: 'DELETE' });
        toast('Cookies limpos', 'success');
        _updateSessionBadge('error');
    } catch(e) { toast('Erro: ' + e.message, 'error'); }
}

// Check session on load
fetch('/api/kommo/merge/session-status', { credentials: 'same-origin' })
    .then(r => r.json())
    .then(d => _updateSessionBadge(d.status, d.source))
    .catch(() => {});

// Reconnect to active job after page reload
fetch('/api/kommo/merge/lost-duplicates/active-job', { credentials: 'same-origin' })
    .then(r => r.json())
    .then(d => {
        if (!d.has_job) return;
        showPipelineLog('lostdup');
        document.getElementById('lostdup-progress-wrap')?.classList.remove('hidden');
        document.getElementById('lostdup-progress-bar').style.width = d.progress + '%';
        document.getElementById('lostdup-progress-pct').textContent = d.progress + '%';
        document.getElementById('lostdup-progress-label').textContent = `${d.processed} / ${d.total}`;
        document.getElementById('lostdup-ok').textContent = d.success;
        document.getElementById('lostdup-err').textContent = d.errors;
        if (d.running) {
            _lostDupActiveJobId = d.job_id;
            _lostDupLog(`Reconectando ao job em andamento... (${d.processed}/${d.total})`);
            _setLostDupRunning(true);
            _pollLostDupStatus(d.job_id, d.dry_run);
        } else {
            const label = d.dry_run ? 'Dry-run concluido' : 'Merge concluido';
            _lostDupLog(`${label} (job anterior). Sucesso: ${d.success}, Erros: ${d.errors}. Total log: ${d.log_total} entradas.`);
        }
    })
    .catch(() => {});

function _lostDupLog(msg) {
    const el = document.getElementById('lostdup-log');
    if (!el) return;
    const ts = new Date().toLocaleTimeString('pt-BR');
    el.textContent += `[${ts}] ${msg}\n`;
    el.scrollTop = el.scrollHeight;
}

async function detectLostDuplicates() {
    const btn = document.getElementById('btn-lostdup-detect');
    if (btn) { btn.disabled = true; btn.classList.add('opacity-50'); }

    showPipelineLog('lostdup');
    _lostDupLog('Detectando duplicatas por telefone em Perdido...');

    try {
        const res = await api('/api/kommo/merge/lost-duplicates');
        const d = await res.json();
        if (!d.ok) { toast(d.error || 'Erro na deteccao', 'error'); _lostDupLog('ERRO: ' + (d.error || 'desconhecido')); return; }

        _lostDupGroups = d.groups || [];

        document.getElementById('lostdup-contacts').textContent = d.groups_count || 0;
        document.getElementById('lostdup-total-leads').textContent = d.total_leads || 0;
        document.getElementById('lostdup-removable').textContent = d.total_removable || 0;
        document.getElementById('lostdup-summary').classList.remove('hidden');

        _renderLostDupPreview(_lostDupGroups);

        const hasDups = _lostDupGroups.length > 0;
        ['btn-lostdup-dryrun', 'btn-lostdup-execute'].forEach(id => {
            const el = document.getElementById(id);
            if (el) { el.disabled = !hasDups; el.classList.toggle('opacity-50', !hasDups); }
        });

        _lostDupLog(`Encontrados ${d.groups_count} telefones com ${d.total_removable} leads removiveis (${d.total_leads} total).`);
    } catch(e) {
        toast('Erro: ' + e.message, 'error');
        _lostDupLog('ERRO: ' + e.message);
    } finally {
        if (btn) { btn.disabled = false; btn.classList.remove('opacity-50'); }
    }
}

function _renderLostDupPreview(groups) {
    const tbody = document.getElementById('lostdup-tbody');
    const wrap = document.getElementById('lostdup-preview');
    if (!tbody || !wrap) return;

    tbody.innerHTML = '';
    if (!groups.length) { wrap.classList.add('hidden'); return; }

    groups.forEach(g => {
        const tr = document.createElement('tr');
        tr.className = 'border-t border-gray-700/20 hover:bg-gray-800/30';

        const keepLead = g.leads[0];
        const removeIds = g.remove_ids.map(id => '#' + id).join(', ');

        tr.innerHTML = `
            <td class="px-3 py-2">
                <span class="text-[var(--text-primary)] font-mono text-[11px]">${g.phone || ''}</span>
                <span class="text-gray-500 text-[10px] block">${g.contact_name || ''}</span>
            </td>
            <td class="px-3 py-2 text-center text-gray-300">${g.lead_count}</td>
            <td class="px-3 py-2">
                <span class="text-emerald-400 font-mono text-[11px]">#${g.keep_id}</span>
                <span class="text-gray-500 text-[10px] ml-1">${keepLead ? keepLead.created_at || '' : ''}</span>
            </td>
            <td class="px-3 py-2">
                <span class="text-red-400 font-mono text-[11px]">${removeIds}</span>
            </td>
        `;
        tbody.appendChild(tr);
    });

    wrap.classList.remove('hidden');
}

function executeLostDupDryRun() {
    _executeLostDup(true);
}

function confirmLostDupExecute() {
    const count = _lostDupGroups.reduce((s, g) => s + g.remove_ids.length, 0);
    if (!confirm(`Executar merge de ${count} leads duplicados em Perdido? Esta ação é IRREVERSÍVEL.`)) return;
    _executeLostDup(false);
}

async function _executeLostDup(dryRun) {
    const body = { dry_run: dryRun };
    const lim = document.getElementById('lostdup-limit').value;
    if (lim) body.limit = parseInt(lim);

    showPipelineLog('lostdup');
    _lostDupLog(dryRun ? 'Iniciando dry-run...' : 'Iniciando merge em massa...');

    _setLostDupRunning(true);

    try {
        const res = await api('/api/kommo/merge/lost-duplicates/execute', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
        });
        const d = await res.json();
        if (!d.ok) { toast(d.error || 'Erro', 'error'); _lostDupLog('ERRO: ' + (d.error || '')); _setLostDupRunning(false); return; }

        _lostDupActiveJobId = d.job_id;
        document.getElementById('lostdup-progress-wrap').classList.remove('hidden');
        _pollLostDupStatus(d.job_id, dryRun);
    } catch(e) {
        toast('Erro: ' + e.message, 'error');
        _lostDupLog('ERRO: ' + e.message);
        _setLostDupRunning(false);
    }
}

let _lostDupActiveJobId = null;

function _setLostDupRunning(running) {
    ['btn-lostdup-detect', 'btn-lostdup-dryrun', 'btn-lostdup-execute'].forEach(id => {
        const el = document.getElementById(id);
        if (el) { el.disabled = running; el.classList.toggle('opacity-50', running); }
    });
    const cancelBtn = document.getElementById('btn-lostdup-cancel');
    if (cancelBtn) cancelBtn.classList.toggle('hidden', !running);
}

async function cancelLostDupJob() {
    if (!_lostDupActiveJobId) { toast('Nenhum job ativo', 'warning'); return; }
    if (!confirm('Tem certeza que deseja parar o merge?')) return;
    try {
        const res = await api(`/api/kommo/merge/lost-duplicates/cancel/${_lostDupActiveJobId}`, { method: 'POST' });
        const d = await res.json();
        if (d.ok) {
            _lostDupLog('Cancelamento solicitado... aguardando parada.');
            toast('Cancelamento solicitado', 'info');
        } else {
            toast(d.error || 'Erro ao cancelar', 'error');
        }
    } catch(e) {
        toast('Erro: ' + e.message, 'error');
    }
}

async function forceClearLostDupJobs() {
    if (!confirm('Limpar todos os jobs? Isso interrompe qualquer execucao em andamento.')) return;
    try {
        const res = await api('/api/kommo/merge/lost-duplicates/force-clear', { method: 'POST' });
        const d = await res.json();
        if (d.ok) {
            if (_lostDupPolling) { clearInterval(_lostDupPolling); _lostDupPolling = null; }
            _lostDupActiveJobId = null;
            _setLostDupRunning(false);
            _lostDupLog(`Reset: ${d.cleared} job(s) removido(s). Pronto para nova execucao.`);
            toast('Jobs limpos', 'success');
        } else {
            toast(d.error || 'Erro', 'error');
        }
    } catch(e) {
        toast('Erro: ' + e.message, 'error');
    }
}

function _pollLostDupStatus(jobId, dryRun) {
    let sinceIdx = 0;
    if (_lostDupPolling) clearInterval(_lostDupPolling);

    _lostDupPolling = setInterval(async () => {
        try {
            const res = await api(`/api/kommo/merge/lost-duplicates/status/${jobId}?since=${sinceIdx}`);
            const d = await res.json();

            document.getElementById('lostdup-progress-bar').style.width = d.progress + '%';
            document.getElementById('lostdup-progress-pct').textContent = d.progress + '%';
            document.getElementById('lostdup-progress-label').textContent = `${d.processed} / ${d.total}`;
            document.getElementById('lostdup-ok').textContent = d.success;
            document.getElementById('lostdup-err').textContent = d.errors;

            const entries = d.log || [];
            for (const e of entries) {
                const who = e.phone ? `${e.contact} [${e.phone}]` : (e.contact || '');
                if (e.action === 'dry_run') {
                    _lostDupLog(`[DRY] Manter #${e.keep} | Remover #${e.remove} (${who})`);
                } else if (e.action === 'merge') {
                    let status = e.ok ? 'OK' : 'ERRO: ' + (e.error || '');
                    if (e.status) status += ` [${e.status}]`;
                    if (e.detail && !e.ok) status += ` ${e.detail}`;
                    _lostDupLog(`Merge #${e.keep} <- #${e.remove} (${who}): ${status}`);
                } else if (e.action === 'fatal') {
                    _lostDupLog('ERRO FATAL: ' + (e.error || ''));
                }
            }
            sinceIdx += entries.length;

            if (!d.running) {
                clearInterval(_lostDupPolling);
                _lostDupPolling = null;
                _lostDupActiveJobId = null;
                _setLostDupRunning(false);
                const label = dryRun ? 'Dry-run concluido' : 'Merge concluido';
                _lostDupLog(`${label}. Sucesso: ${d.success}, Erros: ${d.errors}`);
                toast(`${label}: ${d.success} ok, ${d.errors} erros`, d.errors ? 'warning' : 'success');
            }
        } catch(e) {
            console.error('poll lost-dup status', e);
        }
    }, 1500);
}
