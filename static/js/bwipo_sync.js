// ===========================================================================
// SYNC COMERCIAL — Bwipo CRM
// ===========================================================================
let _bwipoTaskId = null;
let _bwipoPolling = null;

function _bwipoEsc(s) {
    if (s == null) return '';
    return String(s)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}

function _bwipoFmtTs(v) {
    if (!v) return '—';
    const d = new Date(v);
    if (Number.isNaN(d.getTime())) return String(v);
    return d.toLocaleString('pt-BR');
}

async function loadBwipoSync() {
    const hours = (document.getElementById('bwipo-hours') || {}).value || 24;
    try {
        const [stRes, fnRes, rcRes, dpRes] = await Promise.all([
            api('/api/bwipo/status'),
            api('/api/bwipo/funnel'),
            api('/api/bwipo/recent-changes?hours=' + encodeURIComponent(hours)),
            api('/api/bwipo/depara'),
        ]);
        const st = await stRes.json().catch(() => ({}));
        const fn = await fnRes.json().catch(() => ({}));
        const rc = await rcRes.json().catch(() => ({}));
        const dp = await dpRes.json().catch(() => ({}));

        if (st.ok && st.data) {
            document.getElementById('bwipo-kpi-leads').textContent = Number(st.data.leads_count || 0).toLocaleString('pt-BR');
            document.getElementById('bwipo-kpi-contacts').textContent = Number(st.data.contacts_count || 0).toLocaleString('pt-BR');
            document.getElementById('bwipo-kpi-depara').textContent = Number(st.data.depara_count || 0).toLocaleString('pt-BR');
            const host = document.getElementById('bwipo-host-label');
            if (host && st.data.web) {
                try { host.textContent = new URL(st.data.web).host; } catch (_) { host.textContent = st.data.web; }
            }
            _bwipoRenderEntities(st.data.entities || []);
        }
        if (fn.ok && fn.data) _bwipoRenderFunnel(fn.data);
        if (rc.ok && rc.data) {
            document.getElementById('bwipo-kpi-updated').textContent = Number(rc.data.leads_updated || 0).toLocaleString('pt-BR');
            document.getElementById('bwipo-kpi-updated-sub').textContent = 'Últimas ' + hours + 'h';
        }
        if (dp.ok && dp.data) _bwipoRenderDepara(dp.data);
    } catch (e) {
        toast('Falha ao carregar Sync Bwipo: ' + e.message, 'error');
    }
}

function _bwipoRenderEntities(rows) {
    const tbody = document.getElementById('bwipo-entities-tbody');
    if (!rows.length) {
        tbody.innerHTML = '<tr><td colspan="4" class="py-4 text-center text-slate-500">Nenhum sync ainda</td></tr>';
        return;
    }
    const labels = { run: 'Execução', pipelines: 'Pipelines', stages: 'Etapas', contacts: 'Contatos', deals: 'Negócios', users: 'Usuários', depara: 'Depara' };
    tbody.innerHTML = rows.map((r) => {
        const st = r.status || 'pending';
        const color = st === 'completed' ? 'text-emerald-500' : (st === 'error' ? 'text-red-400' : 'text-amber-400');
        return `<tr class="border-b border-slate-200 dark:border-slate-800/40">
            <td class="py-2 pr-2 font-medium">${_bwipoEsc(labels[r.entity_type] || r.entity_type)}</td>
            <td class="py-2 pr-2 text-xs text-slate-500">${_bwipoEsc(_bwipoFmtTs(r.last_sync_at))}</td>
            <td class="py-2 pr-2 text-right">${Number(r.records_synced || 0).toLocaleString('pt-BR')}</td>
            <td class="py-2 ${color} text-xs font-semibold uppercase">${_bwipoEsc(st)}</td>
        </tr>`;
    }).join('');
}

function _bwipoRenderFunnel(data) {
    const stages = data.stages || [];
    document.getElementById('bwipo-funnel-new').textContent = Number(data.new_today || 0).toLocaleString('pt-BR');
    document.getElementById('bwipo-funnel-total').textContent = Number(data.total || 0).toLocaleString('pt-BR');
    document.getElementById('bwipo-funnel-ts').textContent = new Date().toLocaleTimeString('pt-BR');

    const cards = document.getElementById('bwipo-funnel-cards');
    if (!stages.length) {
        cards.innerHTML = '<div class="col-span-full text-center py-8 text-slate-500 text-sm">Rode o primeiro sync para popular o funil.</div>';
    } else {
        cards.innerHTML = stages.map((s) => {
            const tone = s.is_won ? 'border-emerald-400/40' : (s.is_lost ? 'border-red-400/40' : 'border-slate-200 dark:border-slate-700/40');
            return `<div class="rounded-xl border ${tone} bg-white/70 dark:bg-slate-800/40 p-3">
                <p class="text-[10px] uppercase tracking-wider text-slate-500 truncate" title="${_bwipoEsc(s.pipeline)}">${_bwipoEsc(s.name)}</p>
                <p class="text-2xl font-black text-[#00346f] dark:text-white font-display mt-1">${Number(s.count || 0).toLocaleString('pt-BR')}</p>
            </div>`;
        }).join('');
    }

    const tableRows = data.by_stage || [];
    const tbody = document.getElementById('bwipo-stages-tbody');
    const totalAll = tableRows.reduce((s, d) => s + (d.total || 0), 0);
    if (!tableRows.length) {
        tbody.innerHTML = '<tr><td colspan="4" class="py-4 text-center text-slate-500">Nenhum dado</td></tr>';
        return;
    }
    tbody.innerHTML = tableRows.map((s) => {
        const pct = totalAll > 0 ? ((s.total / totalAll) * 100).toFixed(1) : '0';
        return `<tr class="border-b border-slate-200 dark:border-slate-800/40">
            <td class="py-2 pr-2 text-xs text-slate-500">${_bwipoEsc(s.pipeline_name)}</td>
            <td class="py-2 pr-2 font-medium">${_bwipoEsc(s.stage_name)}</td>
            <td class="py-2 pr-2 text-right font-bold">${Number(s.total || 0).toLocaleString('pt-BR')}</td>
            <td class="py-2 text-right text-xs text-slate-500">${pct}%</td>
        </tr>`;
    }).join('');
}

function _bwipoResetButtons() {
    const btnD = document.getElementById('bwipo-btn-delta');
    const btnF = document.getElementById('bwipo-btn-full');
    if (btnD) { btnD.disabled = false; btnD.style.opacity = '1'; }
    if (btnF) { btnF.disabled = false; btnF.style.opacity = '1'; }
}

async function _bwipoStartSync(mode) {
    if (mode === 'full') {
        const ok = confirm('Full Sync baixa todos os negócios/contatos do Bwipo e marca o que sumiu. Continuar?');
        if (!ok) return;
    }
    const btnD = document.getElementById('bwipo-btn-delta');
    const btnF = document.getElementById('bwipo-btn-full');
    btnD.disabled = true; btnF.disabled = true;
    btnD.style.opacity = '0.5'; btnF.style.opacity = '0.5';
    const wrap = document.getElementById('bwipo-progress-wrap');
    wrap.classList.remove('hidden');
    document.getElementById('bwipo-progress-bar').style.width = '0%';
    document.getElementById('bwipo-progress-pct').textContent = '0%';
    document.getElementById('bwipo-progress-label').textContent = 'Iniciando...';
    const logEl0 = document.getElementById('bwipo-sync-log');
    if (logEl0) logEl0.innerHTML = '<p class="text-xs text-slate-500">Conectando…</p>';
    try {
        const res = await api('/api/bwipo/sync', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ mode }),
        });
        const d = await res.json().catch(() => ({}));
        if (!res.ok || !d.ok) {
            toast(d.error || ('HTTP ' + res.status), 'error');
            _bwipoResetButtons();
            return;
        }
        _bwipoTaskId = d.task_id;
        _bwipoPollTask();
    } catch (e) {
        toast('Erro: ' + e.message, 'error');
        _bwipoResetButtons();
    }
}

function _bwipoPollTask() {
    if (_bwipoPolling) clearInterval(_bwipoPolling);
    const tick = async () => {
        if (!_bwipoTaskId) return;
        try {
            const res = await api('/api/bwipo/task/' + _bwipoTaskId);
            const d = await res.json().catch(() => ({}));
            if (!res.ok || !d.ok || !d.data) {
                clearInterval(_bwipoPolling);
                _bwipoPolling = null;
                toast(d.error || 'Erro ao ler status do sync', 'error');
                _bwipoResetButtons();
                return;
            }
            const t = d.data;
            document.getElementById('bwipo-progress-bar').style.width = t.progress + '%';
            document.getElementById('bwipo-progress-pct').textContent = t.progress + '%';
            document.getElementById('bwipo-progress-label').textContent =
                (t.mode === 'full' ? '[FULL] ' : '[INCREMENTAL] ') + (t.message || '…');
            const logEl = document.getElementById('bwipo-sync-log');
            if (logEl && Array.isArray(t.log)) {
                logEl.innerHTML = t.log.map((x) =>
                    `<p class="text-[11px] font-mono text-slate-600 dark:text-slate-400"><span class="text-slate-400">${_bwipoEsc(x.time)}</span> ${_bwipoEsc(x.msg)}</p>`
                ).join('');
                logEl.scrollTop = logEl.scrollHeight;
            }
            if (t.status === 'completed' || t.status === 'error' || t.status === 'cancelled') {
                clearInterval(_bwipoPolling);
                _bwipoPolling = null;
                _bwipoResetButtons();
                if (t.status === 'completed') {
                    toast('Sync Bwipo concluído', 'success');
                    loadBwipoSync();
                } else if (t.status === 'error') {
                    toast(t.message || 'Sync falhou', 'error');
                }
            }
        } catch (e) {
            clearInterval(_bwipoPolling);
            _bwipoPolling = null;
            _bwipoResetButtons();
        }
    };
    tick();
    _bwipoPolling = setInterval(tick, 1500);
}

async function _bwipoCancelSync() {
    if (!_bwipoTaskId) return;
    await api('/api/bwipo/sync/cancel', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ task_id: _bwipoTaskId }),
    });
}

async function _bwipoSearchDepara() {
    const q = (document.getElementById('bwipo-depara-q') || {}).value || '';
    const res = await api('/api/bwipo/depara?q=' + encodeURIComponent(q.trim()));
    const d = await res.json().catch(() => ({}));
    if (!d.ok) {
        toast(d.error || 'Falha no depara', 'error');
        return;
    }
    _bwipoRenderDepara(d.data || {});
}

function _bwipoRenderDepara(data) {
    const box = document.getElementById('bwipo-depara-box');
    if (!box) return;
    const matches = data.matches || [];
    const kommo = data.kommo || [];
    const bwipo = data.bwipo || [];
    let html = '';
    if (matches.length) {
        html += '<p class="text-[10px] uppercase tracking-wider text-slate-500 mb-2">Casamentos</p>';
        html += `<table class="w-full text-sm mb-5"><thead><tr class="text-[10px] text-slate-500 uppercase">
            <th class="pb-2 text-left">Kommo ID</th><th class="pb-2 text-left">Nome Kommo</th>
            <th class="pb-2 text-left">Bwipo ID</th><th class="pb-2 text-left">Nome Bwipo</th>
            <th class="pb-2 text-left">Chave</th></tr></thead><tbody>`;
        html += matches.map((m) => `<tr class="border-b border-slate-200 dark:border-slate-800/40">
            <td class="py-1.5 font-mono text-xs">${_bwipoEsc(m.kommo_lead_id)}</td>
            <td class="py-1.5">${_bwipoEsc(m.kommo_name || '—')}</td>
            <td class="py-1.5 font-mono text-xs break-all">${_bwipoEsc(m.bwipo_deal_id || '—')}</td>
            <td class="py-1.5">${_bwipoEsc(m.bwipo_name || '—')}</td>
            <td class="py-1.5 text-xs text-slate-500">${_bwipoEsc(m.match_type)} · ${_bwipoEsc(m.confidence)}</td>
        </tr>`).join('');
        html += '</tbody></table>';
    }
    if (kommo.length || bwipo.length) {
        html += '<div class="grid grid-cols-1 lg:grid-cols-2 gap-4">';
        html += '<div><p class="text-[10px] uppercase tracking-wider text-slate-500 mb-2">Kommo</p>';
        html += kommo.length ? kommo.map((k) => `
            <div class="border border-slate-200 dark:border-slate-700/40 rounded-lg p-3 mb-2">
                <p class="font-medium">${_bwipoEsc(k.name || '—')}</p>
                <p class="text-[11px] font-mono text-slate-500">ID ${k.id}${k.rgm ? ' · RGM ' + _bwipoEsc(k.rgm) : ''}</p>
            </div>`).join('') : '<p class="text-xs text-slate-500">Nada no Kommo.</p>';
        html += '</div><div><p class="text-[10px] uppercase tracking-wider text-slate-500 mb-2">Bwipo</p>';
        html += bwipo.length ? bwipo.map((b) => `
            <div class="border border-slate-200 dark:border-slate-700/40 rounded-lg p-3 mb-2">
                <p class="font-medium">${_bwipoEsc(b.contact_name || b.title || '—')}</p>
                <p class="text-[11px] font-mono text-slate-500 break-all">ID ${ _bwipoEsc(b.id) }${b.number != null ? ' · #' + b.number : ''}${b.rgm_norm ? ' · RGM ' + _bwipoEsc(b.rgm_norm) : ''}</p>
                <p class="text-[11px] text-slate-500">${_bwipoEsc(b.stage_name || '')}${b.owner_name ? ' · ' + _bwipoEsc(b.owner_name) : ''}</p>
                ${kommo.length ? `<button class="mt-2 text-[11px] text-sky-600" onclick="_bwipoLinkDepara(${kommo[0].id}, '${_bwipoEsc(b.id)}', '${_bwipoEsc(kommo[0].name || '')}')">Vincular ao 1º Kommo da busca</button>` : ''}
            </div>`).join('') : '<p class="text-xs text-slate-500">Nada no Bwipo (ainda não migrado ou fora do espelho).</p>';
        html += '</div></div>';
    }
    if (!html) html = '<p class="text-xs text-slate-500">Nenhum casamento ainda. Os leads antigos continuam só no Kommo até a migração.</p>';
    box.innerHTML = html;
}

async function _bwipoLinkDepara(kommoId, bwipoId, kommoName) {
    const res = await api('/api/bwipo/depara', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ kommo_lead_id: kommoId, bwipo_deal_id: bwipoId, kommo_name: kommoName }),
    });
    const d = await res.json().catch(() => ({}));
    if (!res.ok || !d.ok) {
        toast(d.error || 'Não foi possível vincular', 'error');
        return;
    }
    toast('Depara gravado: Kommo ' + kommoId + ' → Bwipo ' + bwipoId, 'success');
    _bwipoSearchDepara();
}
