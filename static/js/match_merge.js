/* Match & Merge — Frontend Logic */

let _mmLogSince = 0;
let _mmExecLogSince = 0;
let _mmPollTimer = null;
let _mmExecPollTimer = null;
let _mmPreviewPage = 1;

function loadMatchMerge() {
    _mmLogSince = 0;
    _mmExecLogSince = 0;
    _mmPreviewPage = 1;
    mmRefreshFileList();
    mmCheckStatus();
}

/* ── Upload ─────────────────────────────────────── */

function mmHandleUpload(files, tipo) {
    if (!files || files.length === 0) return;
    const nivel = document.getElementById('mm-nivel').value;
    const fd = new FormData();
    for (const f of files) fd.append('files', f);
    fd.append('tipo', tipo);
    fd.append('nivel', nivel);

    const prog = document.getElementById(`mm-progress-${tipo}`);
    const bar = prog.querySelector('.mm-upload-bar');
    const msg = prog.querySelector('.mm-upload-msg');
    prog.classList.remove('hidden');
    bar.style.width = '30%';
    msg.textContent = `Enviando ${files.length} arquivo(s)...`;

    fetch('/api/match-merge/upload', { method: 'POST', body: fd })
        .then(r => r.json())
        .then(data => {
            bar.style.width = '100%';
            msg.textContent = data.ok ? `${data.saved} arquivo(s) enviado(s)` : (data.error || 'Erro');
            msg.className = data.ok
                ? 'mm-upload-msg text-xs text-green-400 mt-1'
                : 'mm-upload-msg text-xs text-red-400 mt-1';
            setTimeout(() => prog.classList.add('hidden'), 3000);
            mmRefreshFileList();
        })
        .catch(() => {
            bar.style.width = '0%';
            msg.textContent = 'Erro no upload';
            msg.className = 'mm-upload-msg text-xs text-red-400 mt-1';
        });
}

function mmHandleDrop(e, tipo) {
    e.preventDefault();
    e.currentTarget.classList.remove('border-cyan-500', 'bg-cyan-950/10', 'border-emerald-500', 'bg-emerald-950/10');
    mmHandleUpload(e.dataTransfer.files, tipo);
}

function mmRefreshFileList() {
    fetch('/api/match-merge/upload-info')
        .then(r => r.json())
        .then(data => {
            for (const tipo of ['candidatos', 'matriculados']) {
                const el = document.getElementById(`mm-files-${tipo}`);
                const files = data[tipo] || [];
                if (!files.length) {
                    el.innerHTML = '<p class="text-xs text-slate-600">Nenhum arquivo</p>';
                    continue;
                }
                el.innerHTML = files.map(f =>
                    `<div class="flex items-center gap-2 text-xs text-slate-400">
                        <svg class="w-3 h-3 text-slate-600 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"/></svg>
                        <span class="truncate">${f.name}</span>
                        <span class="text-slate-600">${(f.size / 1024).toFixed(0)}KB</span>
                        <span class="text-slate-700">${f.nivel}</span>
                    </div>`
                ).join('');
            }
        });
}

function mmClearAll() {
    if (!confirm('Limpar todos os uploads e resultados?')) return;
    fetch('/api/match-merge/clear-uploads', { method: 'POST' })
        .then(() => {
            mmRefreshFileList();
            document.getElementById('mm-results-section').classList.add('hidden');
            document.getElementById('mm-log-content').textContent = '';
            document.getElementById('mm-process-status').textContent = '';
        });
}

/* ── Process ────────────────────────────────────── */

function mmStartProcess() {
    const nivel = document.getElementById('mm-nivel').value;
    const btn = document.getElementById('mm-btn-process');
    btn.disabled = true;
    btn.classList.add('opacity-50');
    document.getElementById('mm-process-status').textContent = 'Iniciando...';
    document.getElementById('mm-log-content').textContent = '';
    document.getElementById('mm-results-section').classList.add('hidden');
    _mmLogSince = 0;

    fetch('/api/match-merge/process', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ nivel }),
    })
        .then(r => r.json())
        .then(data => {
            if (data.error) {
                document.getElementById('mm-process-status').textContent = data.error;
                btn.disabled = false;
                btn.classList.remove('opacity-50');
                return;
            }
            document.getElementById('mm-process-status').textContent = 'Pipeline em execução...';
            _mmStartLogPoll();
        })
        .catch(err => {
            document.getElementById('mm-process-status').textContent = 'Erro: ' + err;
            btn.disabled = false;
            btn.classList.remove('opacity-50');
        });
}

function _mmStartLogPoll() {
    if (_mmPollTimer) clearInterval(_mmPollTimer);
    _mmPollTimer = setInterval(_mmPollLogs, 1500);
}

function _mmPollLogs() {
    fetch(`/api/match-merge/logs?since=${_mmLogSince}`)
        .then(r => r.json())
        .then(data => {
            if (data.lines && data.lines.length) {
                const el = document.getElementById('mm-log-content');
                el.textContent += data.lines.join('\n') + '\n';
                el.scrollTop = el.scrollHeight;
                _mmLogSince = data.total;
            }
        });

    fetch('/api/match-merge/status')
        .then(r => r.json())
        .then(data => {
            if (!data.running) {
                clearInterval(_mmPollTimer);
                _mmPollTimer = null;
                const btn = document.getElementById('mm-btn-process');
                btn.disabled = false;
                btn.classList.remove('opacity-50');

                if (data.has_result) {
                    document.getElementById('mm-process-status').textContent = 'Concluído!';
                    mmLoadPreview();
                } else {
                    document.getElementById('mm-process-status').textContent = 'Erro no pipeline.';
                }
            }
        });
}

function mmCheckStatus() {
    fetch('/api/match-merge/status')
        .then(r => r.json())
        .then(data => {
            if (data.running) {
                document.getElementById('mm-process-status').textContent = 'Pipeline em execução...';
                const btn = document.getElementById('mm-btn-process');
                btn.disabled = true;
                btn.classList.add('opacity-50');
                _mmStartLogPoll();
            } else if (data.has_result) {
                mmLoadPreview();
            }
        });
}

/* ── Preview ────────────────────────────────────── */

function mmLoadPreview() {
    const filtro = document.getElementById('mm-filtro-acao').value;
    fetch(`/api/match-merge/preview?page=${_mmPreviewPage}&per_page=100&filtro=${filtro}`)
        .then(r => r.json())
        .then(data => {
            if (data.error) return;
            if (data.running) return;

            const sec = document.getElementById('mm-results-section');
            sec.classList.remove('hidden');

            const s = data.stats || {};
            const m = s.match || {};
            document.getElementById('mm-kpi-inscritos').textContent = (s.inscritos || 0).toLocaleString();
            document.getElementById('mm-kpi-matriculados').textContent = (s.matriculados || 0).toLocaleString();
            document.getElementById('mm-kpi-match').textContent = (m.com_match || 0).toLocaleString();
            document.getElementById('mm-kpi-fechado').textContent = (m.lead_fechado || 0).toLocaleString();
            document.getElementById('mm-kpi-sematch').textContent = (m.sem_match || 0).toLocaleString();
            document.getElementById('mm-kpi-acoes').textContent = (data.acoes_total || 0).toLocaleString();

            const ap = data.acoes_por_tipo || {};
            document.getElementById('mm-act-aprovado').textContent = (ap.NOVO || 0).toLocaleString();
            document.getElementById('mm-act-matriculado').textContent = (ap.MATRICULADO || 0).toLocaleString();
            document.getElementById('mm-act-sematch').textContent = (ap.ATUALIZAR || 0).toLocaleString();
            document.getElementById('mm-act-perdido').textContent = (ap.MOVER_PERDIDO || 0).toLocaleString();
            document.getElementById('mm-act-restaurar').textContent = (ap.RESTAURAR || 0).toLocaleString();
            document.getElementById('mm-act-unificar').textContent = (ap.UNIFICAR || 0).toLocaleString();
            document.getElementById('mm-act-fechado').textContent = (m.lead_fechado || 0).toLocaleString();

            const tbody = document.getElementById('mm-preview-tbody');
            const acoes = data.acoes || [];
            if (!acoes.length) {
                tbody.innerHTML = '<tr><td colspan="10" class="text-center text-slate-600 py-4">Nenhuma ação</td></tr>';
            } else {
                tbody.innerHTML = acoes.map(a => {
                    const acaoColor = a.acao === 'NOVO' ? 'text-emerald-400' :
                                      a.acao === 'ATUALIZAR' ? 'text-amber-400' :
                                      a.acao === 'MATRICULADO' ? 'text-blue-400' :
                                      a.acao === 'MOVER_PERDIDO' ? 'text-red-400' :
                                      a.acao === 'RESTAURAR' ? 'text-teal-400' :
                                      a.acao === 'UNIFICAR' ? 'text-purple-400' : 'text-slate-400';
                    const acaoBg = a.acao === 'NOVO' ? 'bg-emerald-500/10' :
                                   a.acao === 'ATUALIZAR' ? 'bg-amber-500/10' :
                                   a.acao === 'MATRICULADO' ? 'bg-blue-500/10' :
                                   a.acao === 'MOVER_PERDIDO' ? 'bg-red-500/10' :
                                   a.acao === 'RESTAURAR' ? 'bg-teal-500/10' :
                                   a.acao === 'UNIFICAR' ? 'bg-purple-500/10' : 'bg-slate-500/10';

                    let leadCell = `<span class="text-slate-500 font-mono">${a.lead_id || '—'}</span>`;
                    if (a.acao === 'UNIFICAR' && a.dup_lead_ids) {
                        const ids = a.dup_lead_ids;
                        leadCell = ids.map(id =>
                            `<a href="https://admamoeduitcombr.kommo.com/leads/detail/${id}" target="_blank" class="text-purple-400 hover:underline font-mono">${id}</a>`
                        ).join(', ');
                    }

                    let extraBtn = '';
                    if (a.acao === 'UNIFICAR' && a.dup_lead_ids) {
                        extraBtn = `<button onclick="mmOpenMergeModal('${a.cpf}', ${JSON.stringify(a.dup_lead_ids)})" class="ml-2 text-[10px] font-bold text-purple-300 bg-purple-500/20 hover:bg-purple-500/30 px-2 py-0.5 rounded-full transition">Unificar</button>`;
                    }

                    return `<tr class="hover:bg-slate-800/30">
                        <td class="py-2 px-3"><span class="${acaoBg} ${acaoColor} text-[10px] font-bold px-2 py-0.5 rounded-full">${a.acao}</span>${extraBtn}</td>
                        <td class="py-2 px-3 text-slate-300">${a.nome || ''}</td>
                        <td class="py-2 px-3 text-slate-400 font-mono">${a.cpf || ''}</td>
                        <td class="py-2 px-3 text-slate-400">${a.curso_siaa || ''}</td>
                        <td class="py-2 px-3 text-slate-400">${a.polo || ''}</td>
                        <td class="py-2 px-3 text-slate-300">${a.situacao_siaa || ''}</td>
                        <td class="py-2 px-3 text-slate-500">${a.situacao_kommo || '—'}</td>
                        <td class="py-2 px-3 text-slate-400">${a.lead_fase || '—'}</td>
                        <td class="py-2 px-3 text-slate-500">${a.match_tipo || '—'}</td>
                        <td class="py-2 px-3">${leadCell}</td>
                    </tr>`;
                }).join('');
            }

            const info = document.getElementById('mm-preview-info');
            info.textContent = `Página ${data.page}/${data.pages} (${data.acoes_total} ações)`;
        });
}

function mmPrevPage() {
    if (_mmPreviewPage > 1) { _mmPreviewPage--; mmLoadPreview(); }
}
function mmNextPage() {
    _mmPreviewPage++;
    mmLoadPreview();
}

/* ── Execute ────────────────────────────────────── */

function mmExecute() {
    const filtro = document.getElementById('mm-exec-filtro').value;
    const limitEl = document.getElementById('mm-exec-limit');
    const limit = limitEl.value ? parseInt(limitEl.value) : null;

    if (!confirm(`Executar atualizações no Kommo${filtro ? ' (' + filtro + ')' : ''}${limit ? ' — limite ' + limit : ''}?`)) return;

    const btn = document.getElementById('mm-btn-execute');
    btn.disabled = true;
    btn.classList.add('opacity-50');
    document.getElementById('mm-exec-status').textContent = 'Iniciando...';
    _mmExecLogSince = 0;

    fetch('/api/match-merge/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ filtro, limit }),
    })
        .then(r => r.json())
        .then(data => {
            if (data.error) {
                document.getElementById('mm-exec-status').textContent = data.error;
                btn.disabled = false;
                btn.classList.remove('opacity-50');
                return;
            }
            document.getElementById('mm-exec-status').textContent = `Executando ${data.total} ações...`;
            _mmStartExecPoll();
        })
        .catch(err => {
            document.getElementById('mm-exec-status').textContent = 'Erro: ' + err;
            btn.disabled = false;
            btn.classList.remove('opacity-50');
        });
}

function _mmStartExecPoll() {
    if (_mmExecPollTimer) clearInterval(_mmExecPollTimer);
    _mmExecPollTimer = setInterval(_mmPollExec, 2000);
}

function _mmPollExec() {
    fetch(`/api/match-merge/exec-status?since=${_mmExecLogSince}`)
        .then(r => r.json())
        .then(data => {
            if (data.lines && data.lines.length) {
                const el = document.getElementById('mm-log-content');
                el.textContent += data.lines.join('\n') + '\n';
                el.scrollTop = el.scrollHeight;
                _mmExecLogSince = data.total;
            }

            if (!data.running) {
                clearInterval(_mmExecPollTimer);
                _mmExecPollTimer = null;
                const btn = document.getElementById('mm-btn-execute');
                btn.disabled = false;
                btn.classList.remove('opacity-50');

                const r = data.result || {};
                if (r.error) {
                    document.getElementById('mm-exec-status').textContent = `Erro: ${r.error}`;
                } else {
                    document.getElementById('mm-exec-status').textContent =
                        `Concluído: ${r.ok || 0} OK, ${r.erro || 0} erros, ${r.skip || 0} ignorados`;
                }
            }
        });
}

/* ── Merge Modal (UNIFICAR) ────────────────────── */

let _mmMergeKeepId = null;
let _mmMergeLeadIds = [];
let _mmMergeCpf = '';

function mmOpenMergeModal(cpf, leadIds) {
    _mmMergeCpf = cpf;
    _mmMergeLeadIds = leadIds;
    _mmMergeKeepId = null;

    const modal = document.getElementById('mm-merge-modal');
    modal.classList.remove('hidden');
    document.getElementById('mm-merge-confirm-btn').disabled = true;
    document.getElementById('mm-merge-status').classList.add('hidden');

    const container = document.getElementById('mm-merge-leads');
    container.innerHTML = '<p class="text-xs text-slate-500 col-span-2 text-center py-8">Carregando dados dos leads...</p>';

    fetch('/api/kommo/merge/preview', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ lead_ids: leadIds }),
    })
        .then(r => r.json())
        .then(data => {
            if (data.error) {
                container.innerHTML = `<p class="text-xs text-red-400 col-span-2 text-center py-4">${data.error}</p>`;
                return;
            }
            _mmRenderMergeCards(container, data.leads || []);
        })
        .catch(err => {
            container.innerHTML = `<p class="text-xs text-red-400 col-span-2 text-center py-4">Erro: ${err}</p>`;
        });
}

function _mmRenderMergeCards(container, leads) {
    if (!leads.length) {
        container.innerHTML = '<p class="text-xs text-slate-500 col-span-2 text-center py-4">Nenhum lead encontrado</p>';
        return;
    }

    container.innerHTML = leads.map(lead => {
        const id = lead.id;
        const statusClass = lead.status_id === 142 ? 'text-green-400' :
                            lead.status_id === 143 ? 'text-red-400' : 'text-cyan-400';
        const statusLabel = lead.status_id === 142 ? 'Ganho' :
                            lead.status_id === 143 ? 'Perdido' : 'Ativo';
        const fields = lead.custom_fields || {};

        return `<div id="mm-merge-card-${id}" class="border border-slate-700/40 rounded-xl p-4 cursor-pointer hover:border-purple-500/60 transition-all"
                     onclick="mmSelectKeepLead(${id})">
            <div class="flex items-center justify-between mb-3">
                <a href="https://admamoeduitcombr.kommo.com/leads/detail/${id}" target="_blank"
                   class="text-sm font-bold text-purple-400 hover:underline font-mono">#${id}</a>
                <span class="text-[10px] font-bold ${statusClass} px-2 py-0.5 rounded-full bg-slate-800">${statusLabel}</span>
            </div>
            <div class="space-y-1.5 text-xs">
                <div class="flex justify-between"><span class="text-slate-500">Nome</span><span class="text-slate-300">${lead.name || '—'}</span></div>
                <div class="flex justify-between"><span class="text-slate-500">Pipeline</span><span class="text-slate-300">${lead.pipeline_name || '—'}</span></div>
                <div class="flex justify-between"><span class="text-slate-500">Fase</span><span class="text-slate-300">${lead.status_name || '—'}</span></div>
                <div class="flex justify-between"><span class="text-slate-500">Criado</span><span class="text-slate-300">${lead.created_at || '—'}</span></div>
                ${Object.entries(fields).map(([k, v]) =>
                    `<div class="flex justify-between"><span class="text-slate-500">${k}</span><span class="text-slate-300 text-right max-w-[60%] truncate">${v || '—'}</span></div>`
                ).join('')}
            </div>
            <div class="mt-3 text-center">
                <span class="mm-keep-badge hidden text-[10px] font-bold text-green-400 bg-green-500/10 px-3 py-1 rounded-full">MANTER</span>
                <span class="mm-remove-badge hidden text-[10px] font-bold text-red-400 bg-red-500/10 px-3 py-1 rounded-full">REMOVER</span>
            </div>
        </div>`;
    }).join('');
}

function mmSelectKeepLead(leadId) {
    _mmMergeKeepId = leadId;

    _mmMergeLeadIds.forEach(id => {
        const card = document.getElementById(`mm-merge-card-${id}`);
        if (!card) return;
        const keepBadge = card.querySelector('.mm-keep-badge');
        const removeBadge = card.querySelector('.mm-remove-badge');

        if (id === leadId) {
            card.classList.add('border-green-500/60', 'bg-green-950/10');
            card.classList.remove('border-red-500/40', 'bg-red-950/10', 'border-slate-700/40');
            keepBadge.classList.remove('hidden');
            removeBadge.classList.add('hidden');
        } else {
            card.classList.add('border-red-500/40', 'bg-red-950/10');
            card.classList.remove('border-green-500/60', 'bg-green-950/10', 'border-slate-700/40');
            keepBadge.classList.add('hidden');
            removeBadge.classList.remove('hidden');
        }
    });

    document.getElementById('mm-merge-confirm-btn').disabled = false;
}

function mmCloseMergeModal() {
    document.getElementById('mm-merge-modal').classList.add('hidden');
    _mmMergeKeepId = null;
    _mmMergeLeadIds = [];
}

function mmConfirmMerge() {
    if (!_mmMergeKeepId || _mmMergeLeadIds.length < 2) return;

    const removeId = _mmMergeLeadIds.find(id => id !== _mmMergeKeepId);
    if (!removeId) return;

    const btn = document.getElementById('mm-merge-confirm-btn');
    btn.disabled = true;
    btn.textContent = 'Processando...';

    const statusEl = document.getElementById('mm-merge-status');
    statusEl.classList.remove('hidden');
    statusEl.textContent = 'Enviando merge para o Kommo...';
    statusEl.className = 'text-xs text-slate-400 mb-3';

    fetch('/api/kommo/merge', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ keep_id: _mmMergeKeepId, remove_id: removeId }),
    })
        .then(r => r.json())
        .then(data => {
            if (data.error) {
                statusEl.textContent = `Erro: ${data.error}`;
                statusEl.className = 'text-xs text-red-400 mb-3';
                btn.disabled = false;
                btn.innerHTML = '<svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/></svg> Confirmar Merge';
                return;
            }
            if (data.job_key) {
                statusEl.textContent = 'Merge em andamento, verificando status...';
                _mmPollMergeJob(data.job_key);
            } else if (data.status === 'ok' || data.ok) {
                statusEl.textContent = 'Merge concluído com sucesso!';
                statusEl.className = 'text-xs text-green-400 mb-3';
                setTimeout(() => mmCloseMergeModal(), 2000);
            }
        })
        .catch(err => {
            statusEl.textContent = `Erro: ${err}`;
            statusEl.className = 'text-xs text-red-400 mb-3';
            btn.disabled = false;
            btn.innerHTML = '<svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/></svg> Confirmar Merge';
        });
}

function _mmPollMergeJob(jobKey) {
    const statusEl = document.getElementById('mm-merge-status');
    const btn = document.getElementById('mm-merge-confirm-btn');

    const poll = () => {
        fetch(`/api/kommo/merge/job/${jobKey}`)
            .then(r => r.json())
            .then(data => {
                if (data.status === 'running' || data.status === 'pending') {
                    statusEl.textContent = `Merge em andamento... (${data.status})`;
                    setTimeout(poll, 2000);
                } else if (data.status === 'done' || data.status === 'ok') {
                    statusEl.textContent = 'Merge concluído com sucesso!';
                    statusEl.className = 'text-xs text-green-400 mb-3';
                    btn.innerHTML = '<svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/></svg> Confirmar Merge';
                    setTimeout(() => mmCloseMergeModal(), 2000);
                } else {
                    statusEl.textContent = `Merge falhou: ${data.error || data.status}`;
                    statusEl.className = 'text-xs text-red-400 mb-3';
                    btn.disabled = false;
                    btn.innerHTML = '<svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/></svg> Confirmar Merge';
                }
            })
            .catch(() => {
                statusEl.textContent = 'Erro ao verificar status do merge.';
                statusEl.className = 'text-xs text-red-400 mb-3';
                btn.disabled = false;
                btn.innerHTML = '<svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/></svg> Confirmar Merge';
            });
    };
    setTimeout(poll, 2000);
}
