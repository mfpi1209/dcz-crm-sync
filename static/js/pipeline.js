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
    const logs = ['sanitize-log', 'enrich-log', 'merge-log', 'pipeline-log', 'inadimplentes-log', 'concluintes-log'];
    const tabs = ['tab-san-log', 'tab-enrich-log', 'tab-merge-log', 'tab-pipe-log', 'tab-inad-log', 'tab-conc-log'];
    const map = { sanitize: 0, enrich: 1, merge: 2, pipeline: 3, inadimplentes: 4, concluintes: 5 };
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
            el.classList.remove('text-slate-500');
        } else {
            el.classList.remove('text-indigo-400','border-b-2','border-indigo-400');
            el.classList.add('text-slate-500');
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
    ['sanitize-log','enrich-log','merge-log','pipeline-log','inadimplentes-log','concluintes-log'].forEach(id => {
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
