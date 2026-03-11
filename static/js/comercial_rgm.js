// ---------------------------------------------------------------------------
// Dashboard Comercial
// ---------------------------------------------------------------------------

let _crgmChartEvolucao = null;
let _crgmChartRanking = null;
let _crgmChartAgentes = null;
let _crgmLastData = null;

async function loadComercialRgm() {
    const filtersData = await _crgmLoadFilters();
    await _crgmLoadSnapshotInfo();

    if (filtersData && (!filtersData.agentes || filtersData.agentes.length === 0)) {
        await _crgmAutoSyncUsers();
    }

    const hoje = new Date();
    const ini = new Date(hoje.getFullYear(), hoje.getMonth(), 1);
    const elIni = document.getElementById('crgm-dt-ini');
    const elFim = document.getElementById('crgm-dt-fim');
    if (!elIni.value) elIni.value = ini.toISOString().substring(0, 10);
    if (!elFim.value) elFim.value = hoje.toISOString().substring(0, 10);

    crgmAtualizar();
}

async function _crgmAutoSyncUsers() {
    try {
        const res = await api('/api/comercial-rgm/sync-users', { method: 'POST' });
        const d = await res.json();
        if (d.ok && d.synced > 0) {
            await _crgmLoadFilters();
        }
    } catch (e) {
        console.error('auto-sync users', e);
    }
}

async function _crgmLoadFilters() {
    try {
        const res = await api('/api/comercial-rgm/filters');
        const d = await res.json();
        if (!d.ok) return null;

        const selPolo = document.getElementById('crgm-polo');
        const selNivel = document.getElementById('crgm-nivel');
        const selAgente = document.getElementById('crgm-agente');

        const curPolo = selPolo.value;
        const curNivel = selNivel.value;
        const curAgente = selAgente ? selAgente.value : '';

        selPolo.innerHTML = '<option value="">Todos</option>' +
            d.polos.map(p => `<option value="${esc(p)}">${esc(p)}</option>`).join('');
        selNivel.innerHTML = '<option value="">Todos</option>' +
            d.niveis.map(n => `<option value="${esc(n)}">${esc(n)}</option>`).join('');

        if (selAgente && d.agentes) {
            selAgente.innerHTML = '<option value="">Todos</option>' +
                d.agentes.map(a => `<option value="${a.id}">${esc(a.name)}</option>`).join('');
        }

        if (curPolo) selPolo.value = curPolo;
        if (curNivel) selNivel.value = curNivel;
        if (curAgente && selAgente) selAgente.value = curAgente;
        return d;
    } catch (e) {
        console.error('crgm filters', e);
        return null;
    }
}

async function _crgmLoadSnapshotInfo() {
    try {
        const res = await api('/api/comercial-rgm/snapshot-info');
        const d = await res.json();
        if (!d.ok || !d.total) return;

        const dt = d.uploaded_at ? new Date(d.uploaded_at).toLocaleString('pt-BR') : '';
        let info = `${d.total.toLocaleString('pt-BR')} registros CSV`;
        if (d.min_date && d.max_date) info += ` | ${d.min_date} a ${d.max_date}`;
        if (d.mm_inscritos > 0 || d.mm_matriculados > 0) {
            info += ` | M&M: ${(d.mm_inscritos || 0).toLocaleString('pt-BR')} insc. / ${(d.mm_matriculados || 0).toLocaleString('pt-BR')} matr.`;
        }
        if (dt) info += ` | ${dt}`;
        document.getElementById('crgm-snapshot-info').textContent = info;
    } catch (e) {
        console.error('crgm snapshot-info', e);
    }
}

async function crgmAtualizar() {
    const polo = document.getElementById('crgm-polo').value;
    const nivel = document.getElementById('crgm-nivel').value;
    const dtIni = document.getElementById('crgm-dt-ini').value;
    const dtFim = document.getElementById('crgm-dt-fim').value;

    _crgmLoading(true);
    _crgmErro('');

    const qs = new URLSearchParams();
    if (polo) qs.set('polo', polo);
    if (nivel) qs.set('nivel', nivel);
    if (dtIni) qs.set('dt_ini', dtIni);
    if (dtFim) qs.set('dt_fim', dtFim);

    try {
        const res = await api(`/api/comercial-rgm/data?${qs}`);
        const d = await res.json();
        if (!d.ok) { _crgmErro(d.error || 'Erro'); return; }

        _crgmLastData = d;
        _crgmRenderKPIs(d.kpis);
        _crgmRenderEvolucao(d.evolucao, d.evolucao_prev || []);
        _crgmRenderRanking(d.ranking_polo);
        _crgmRenderCicloTable(d.ranking_ciclo);
        _crgmRenderAgentes(d.ranking_agentes || []);
        _crgmRenderAgentesChart(d.ranking_agentes || []);
    } catch (e) {
        _crgmErro('Erro: ' + e.message);
    } finally {
        _crgmLoading(false);
    }
}

function _crgmRenderKPIs(k) {
    document.getElementById('crgm-vendas').textContent = k.vendas.toLocaleString('pt-BR');
    document.getElementById('crgm-ytd').textContent = k.vendas_ytd.toLocaleString('pt-BR');
    document.getElementById('crgm-media').textContent = k.media_diaria.toLocaleString('pt-BR');
    document.getElementById('crgm-ticket').textContent = 'R$ ' + k.ticket_medio.toLocaleString('pt-BR', { minimumFractionDigits: 0, maximumFractionDigits: 0 });
    document.getElementById('crgm-valor-total').textContent = 'R$ ' + (k.valor_total >= 1000 ? (k.valor_total / 1000).toFixed(0) + 'k' : k.valor_total.toLocaleString('pt-BR'));
    document.getElementById('crgm-dias').textContent = k.dias;

    const mmEl = document.getElementById('crgm-mm-inscritos');
    if (mmEl) mmEl.textContent = (k.mm_inscritos || 0).toLocaleString('pt-BR');

    document.getElementById('crgm-1a-val').textContent = k.vendas_1a.toLocaleString('pt-BR');
    _crgmSetBadge('crgm-1a-badge', k.pct_1a);
    document.getElementById('crgm-6m-val').textContent = k.vendas_6m.toLocaleString('pt-BR');
    _crgmSetBadge('crgm-6m-badge', k.pct_6m);
    document.getElementById('crgm-ytd-prev').textContent = (k.vendas_prev_ytd || 0).toLocaleString('pt-BR');
    _crgmSetBadge('crgm-ytd-badge', k.pct_ytd);
}

function _crgmSetBadge(id, pct) {
    const el = document.getElementById(id);
    if (!el) return;
    const sign = pct >= 0 ? '\u2191' : '\u2193';
    el.textContent = `${sign} ${Math.abs(pct)}%`;
    el.className = pct >= 0
        ? 'font-bold px-1.5 py-0.5 rounded text-[10px] bg-emerald-500/20 text-emerald-400'
        : 'font-bold px-1.5 py-0.5 rounded text-[10px] bg-red-500/20 text-red-400';
}

function _crgmRenderEvolucao(evolucao, evolucaoPrev) {
    const ctx = document.getElementById('crgm-chart-evolucao');
    if (_crgmChartEvolucao) _crgmChartEvolucao.destroy();

    const labels = evolucao.map(e => {
        const d = new Date(e.data + 'T00:00:00');
        return d.toLocaleDateString('pt-BR', { day: '2-digit', month: 'short' });
    });
    const values = evolucao.map(e => e.count);

    const datasets = [{
        label: 'Matrículas',
        data: values,
        borderColor: '#a78bfa',
        backgroundColor: 'rgba(167,139,250,0.08)',
        borderWidth: 2.5,
        fill: true,
        tension: 0.3,
        pointRadius: evolucao.length > 60 ? 0 : 4,
        pointBackgroundColor: '#a78bfa',
        pointHoverRadius: 6,
    }];

    if (evolucaoPrev && evolucaoPrev.length > 0) {
        const prevMap = {};
        evolucaoPrev.forEach(e => {
            const d = new Date(e.data + 'T00:00:00');
            const key = `${String(d.getDate()).padStart(2, '0')}/${String(d.getMonth() + 1).padStart(2, '0')}`;
            prevMap[key] = (prevMap[key] || 0) + e.count;
        });

        const prevValues = evolucao.map(e => {
            const d = new Date(e.data + 'T00:00:00');
            const key = `${String(d.getDate()).padStart(2, '0')}/${String(d.getMonth() + 1).padStart(2, '0')}`;
            return prevMap[key] || 0;
        });

        datasets.push({
            label: 'Ano Anterior',
            data: prevValues,
            borderColor: '#64748b',
            backgroundColor: 'transparent',
            borderWidth: 1.5,
            borderDash: [6, 4],
            fill: false,
            tension: 0.3,
            pointRadius: 0,
            pointHoverRadius: 4,
            pointBackgroundColor: '#64748b',
        });
    }

    _crgmChartEvolucao = new Chart(ctx, {
        type: 'line',
        data: { labels, datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: {
                    display: evolucaoPrev && evolucaoPrev.length > 0,
                    position: 'top',
                    align: 'end',
                    labels: { color: '#94a3b8', font: { size: 10 }, boxWidth: 12, padding: 12 }
                },
                tooltip: {
                    mode: 'index',
                    intersect: false,
                }
            },
            scales: {
                x: {
                    ticks: { color: '#64748b', maxTicksLimit: 15, font: { size: 10 } },
                    grid: { color: 'rgba(100,116,139,0.08)' }
                },
                y: {
                    beginAtZero: true,
                    ticks: { color: '#64748b', font: { size: 10 } },
                    grid: { color: 'rgba(100,116,139,0.08)' }
                }
            }
        }
    });
}

function _crgmRenderRanking(ranking) {
    const ctx = document.getElementById('crgm-chart-ranking');
    if (_crgmChartRanking) _crgmChartRanking.destroy();

    const sorted = [...ranking].sort((a, b) => a.total - b.total);
    const labels = sorted.map(r => r.nome);
    const values = sorted.map(r => r.total);

    const colors = [
        '#8b5cf6', '#a78bfa', '#c084fc', '#d946ef',
        '#06b6d4', '#14b8a6', '#22c55e', '#eab308',
        '#f97316', '#ef4444', '#ec4899', '#64748b',
    ];

    _crgmChartRanking = new Chart(ctx, {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                data: values,
                backgroundColor: labels.map((_, i) => colors[i % colors.length] + '66'),
                borderColor: labels.map((_, i) => colors[i % colors.length]),
                borderWidth: 1,
                borderRadius: 4,
            }]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                x: {
                    beginAtZero: true,
                    ticks: { color: '#64748b', font: { size: 10 } },
                    grid: { color: 'rgba(100,116,139,0.08)' }
                },
                y: {
                    ticks: { color: '#cbd5e1', font: { size: 10 } },
                    grid: { display: false }
                }
            }
        }
    });
}

function _crgmRenderCicloTable(ciclos) {
    const tbody = document.getElementById('crgm-ciclo-body');
    if (!ciclos || !ciclos.length) {
        tbody.innerHTML = '<tr><td colspan="2" class="px-5 py-6 text-center text-slate-600">Sem dados</td></tr>';
        return;
    }
    tbody.innerHTML = ciclos.map(c =>
        `<tr class="hover:bg-white/[0.02] transition-colors">
            <td class="px-5 py-2.5 text-slate-300">${esc(c.nome)}</td>
            <td class="px-5 py-2.5 text-right text-white font-semibold">${c.total.toLocaleString('pt-BR')}</td>
        </tr>`
    ).join('');
}

function _crgmRenderAgentes(agentes) {
    const tbody = document.getElementById('crgm-agentes-body');
    const countEl = document.getElementById('crgm-agentes-count');

    if (!agentes || !agentes.length) {
        tbody.innerHTML = `<tr><td colspan="10" class="px-5 py-8 text-center text-slate-600">
            <div class="flex flex-col items-center gap-2">
                <svg class="w-8 h-8 text-slate-700" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0z"/></svg>
                <span>Clique em "Sync Agentes" para carregar</span>
            </div>
        </td></tr>`;
        if (countEl) countEl.textContent = '';
        return;
    }

    const agenteFilter = document.getElementById('crgm-agente').value;
    let filtered = agentes;
    if (agenteFilter) {
        filtered = agentes.filter(a => String(a.user_id) === agenteFilter);
    }

    filtered.sort((a, b) => (b.matriculas_periodo || 0) - (a.matriculas_periodo || 0));
    filtered = filtered.filter(a => (a.matriculas_periodo || 0) > 0 || (a.ganhos || 0) > 0);

    if (countEl) countEl.textContent = `${filtered.length} agentes`;

    const medals = ['\uD83E\uDD47', '\uD83E\uDD48', '\uD83E\uDD49'];

    tbody.innerHTML = filtered.map((a, i) => {
        const taxaClass = a.taxa_conversao >= 20 ? 'text-emerald-400' : a.taxa_conversao >= 8 ? 'text-amber-400' : 'text-red-400';
        const mp = a.matriculas_periodo || 0;
        const pp = a.perdidos_periodo || 0;
        const np = a.novos_periodo || 0;
        const meta = a.meta || 0;
        const pctMeta = meta > 0 ? Math.round(mp / meta * 100) : 0;
        const metaClass = meta === 0 ? 'text-slate-600' : pctMeta >= 100 ? 'text-emerald-400' : pctMeta >= 70 ? 'text-amber-400' : 'text-red-400';
        const rank = i < 3 ? medals[i] : (i + 1);
        const rowClass = i < 3 ? 'bg-violet-500/[0.03]' : '';
        const nameIsId = a.nome && a.nome.startsWith('User #');

        return `<tr class="hover:bg-white/[0.03] transition-colors ${rowClass}" title="${meta > 0 ? 'Meta: ' + meta + ' | Progresso: ' + pctMeta + '%' : 'Sem meta definida'}">
            <td class="text-center px-3 py-2.5 font-bold text-slate-400">${rank}</td>
            <td class="px-4 py-2.5 font-medium ${nameIsId ? 'text-slate-500 italic' : 'text-white'}">${esc(a.nome)}</td>
            <td class="px-4 py-2.5 text-right font-mono text-violet-400 font-semibold">${mp.toLocaleString('pt-BR')}</td>
            <td class="px-4 py-2.5 text-right font-mono text-slate-500">${meta > 0 ? meta : '—'}</td>
            <td class="px-4 py-2.5 text-right font-mono font-bold ${metaClass}">${meta > 0 ? pctMeta + '%' : '—'}</td>
            <td class="px-4 py-2.5 text-right font-mono text-blue-400">${np.toLocaleString('pt-BR')}</td>
            <td class="px-4 py-2.5 text-right font-mono text-red-400">${pp.toLocaleString('pt-BR')}</td>
            <td class="px-4 py-2.5 text-right font-mono text-slate-300">${a.total.toLocaleString('pt-BR')}</td>
            <td class="px-4 py-2.5 text-right font-mono text-cyan-400">${a.ativos.toLocaleString('pt-BR')}</td>
            <td class="px-4 py-2.5 text-right font-mono font-bold ${taxaClass}">${a.taxa_conversao}%</td>
        </tr>`;
    }).join('');
}

function _crgmRenderAgentesChart(agentes) {
    const ctx = document.getElementById('crgm-chart-agentes');
    if (_crgmChartAgentes) _crgmChartAgentes.destroy();

    if (!agentes || !agentes.length) {
        _crgmChartAgentes = null;
        return;
    }

    const agenteFilter = document.getElementById('crgm-agente').value;
    let data = [...agentes];
    if (agenteFilter) {
        data = data.filter(a => String(a.user_id) === agenteFilter);
    }

    data = data.filter(a => (a.matriculas_periodo || 0) > 0 || (a.meta || 0) > 0);
    data.sort((a, b) => (a.matriculas_periodo || 0) - (b.matriculas_periodo || 0));

    const top = data.slice(-12);
    const labels = top.map(a => a.nome || `#${a.user_id}`);

    const datasets = [
        {
            label: 'Matrículas (per.)',
            data: top.map(a => a.matriculas_periodo || 0),
            backgroundColor: '#a78bfa',
            borderColor: '#8b5cf6',
            borderWidth: 1,
            borderRadius: 4,
            barPercentage: 0.6,
            categoryPercentage: 0.85,
        },
    ];

    const hasMetas = top.some(a => (a.meta || 0) > 0);
    if (hasMetas) {
        datasets.push({
            label: 'Meta',
            data: top.map(a => a.meta || 0),
            backgroundColor: 'transparent',
            borderColor: '#f59e0b',
            borderWidth: 2,
            borderDash: [4, 3],
            borderRadius: 4,
            barPercentage: 0.6,
            categoryPercentage: 0.85,
            type: 'bar',
        });
    }

    _crgmChartAgentes = new Chart(ctx, {
        type: 'bar',
        data: { labels, datasets },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'top',
                    align: 'end',
                    labels: { color: '#94a3b8', font: { size: 10 }, boxWidth: 10, padding: 12 }
                },
                tooltip: {
                    callbacks: {
                        afterBody: function(ctx) {
                            const i = ctx[0].dataIndex;
                            const a = top[i];
                            const meta = a.meta || 0;
                            const pct = meta > 0 ? Math.round((a.matriculas_periodo || 0) / meta * 100) : 0;
                            let lines = [`Ganhos CRM: ${(a.ganhos||0).toLocaleString('pt-BR')} | Conv.: ${a.taxa_conversao}%`];
                            if (meta > 0) lines.push(`Meta: ${meta} | Progresso: ${pct}%`);
                            return lines;
                        }
                    }
                }
            },
            scales: {
                x: {
                    beginAtZero: true,
                    ticks: { color: '#64748b', font: { size: 10 } },
                    grid: { color: 'rgba(100,116,139,0.08)' }
                },
                y: {
                    ticks: { color: '#e2e8f0', font: { size: 11, weight: 500 } },
                    grid: { display: false }
                }
            }
        }
    });
}

// --- Metas management ---

function crgmToggleMetas() {
    const panel = document.getElementById('crgm-metas-panel');
    const isHidden = panel.classList.contains('hidden');
    panel.classList.toggle('hidden');
    if (isHidden) _crgmLoadMetasPanel();
}

async function _crgmLoadMetasPanel() {
    const grid = document.getElementById('crgm-metas-grid');
    grid.innerHTML = '<p class="text-slate-500 text-xs col-span-full">Carregando...</p>';

    try {
        const [metasRes, filtersRes] = await Promise.all([
            api('/api/comercial-rgm/metas').then(r => r.json()),
            api('/api/comercial-rgm/filters').then(r => r.json()),
        ]);

        const agentes = filtersRes.ok ? filtersRes.agentes || [] : [];
        const metasMap = {};
        if (metasRes.ok) {
            metasRes.metas.forEach(m => { metasMap[m.user_id] = m.meta; });
        }

        if (!agentes.length) {
            grid.innerHTML = '<p class="text-slate-500 text-xs col-span-full">Nenhum agente encontrado. Clique em "Sync Agentes" primeiro.</p>';
            return;
        }

        grid.innerHTML = agentes
            .filter(a => !['Admin', 'T.I', 'Suporte'].includes(a.name))
            .map(a => {
                const val = metasMap[a.id] || '';
                return `<div class="flex items-center gap-2 bg-slate-800/30 rounded-lg px-3 py-2">
                    <span class="text-xs text-slate-300 flex-1 truncate">${esc(a.name)}</span>
                    <input type="number" min="0" data-uid="${a.id}" data-uname="${esc(a.name)}"
                        value="${val}" placeholder="0"
                        class="w-16 text-right text-xs font-mono bg-slate-900/50 border border-slate-700 rounded px-2 py-1 text-white focus:border-amber-500 focus:outline-none crgm-meta-input">
                </div>`;
            }).join('');
    } catch (e) {
        grid.innerHTML = `<p class="text-red-400 text-xs col-span-full">Erro: ${e.message}</p>`;
    }
}

async function crgmSaveMetas() {
    const inputs = document.querySelectorAll('.crgm-meta-input');
    const metas = [];
    inputs.forEach(inp => {
        const uid = parseInt(inp.dataset.uid);
        const meta = parseInt(inp.value) || 0;
        const name = inp.dataset.uname || '';
        metas.push({ user_id: uid, meta, user_name: name });
    });

    try {
        const res = await api('/api/comercial-rgm/metas', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ metas })
        });
        const d = await res.json();
        if (d.ok) {
            document.getElementById('crgm-metas-panel').classList.add('hidden');
            crgmAtualizar();
        } else {
            _crgmErro(d.error || 'Erro ao salvar metas');
        }
    } catch (e) {
        _crgmErro('Erro ao salvar metas: ' + e.message);
    }
}

// --- Utilities ---

async function crgmSyncUsers() {
    const btn = document.getElementById('crgm-btn-sync');
    btn.disabled = true;
    btn.classList.add('opacity-50');

    try {
        const res = await api('/api/comercial-rgm/sync-users', { method: 'POST' });
        const d = await res.json();
        if (d.error) {
            _crgmErro(d.error);
        } else {
            _crgmErro('');
            await _crgmLoadFilters();
            await crgmAtualizar();
        }
    } catch (e) {
        _crgmErro('Erro ao sincronizar: ' + e.message);
    } finally {
        btn.disabled = false;
        btn.classList.remove('opacity-50');
    }
}

async function crgmUpload(input) {
    const file = input.files[0];
    if (!file) return;
    input.value = '';

    _crgmLoading(true);
    _crgmErro('');

    const fd = new FormData();
    fd.append('file', file);

    try {
        const res = await api('/api/comercial-rgm/upload', { method: 'POST', body: fd });
        const d = await res.json();
        if (d.error) { _crgmErro(d.error); return; }

        _crgmErro('');
        await _crgmLoadFilters();
        await _crgmLoadSnapshotInfo();
        await crgmAtualizar();
    } catch (e) {
        _crgmErro('Erro no upload: ' + e.message);
    } finally {
        _crgmLoading(false);
    }
}

function _crgmLoading(show) {
    document.getElementById('crgm-loading').classList.toggle('hidden', !show);
}

function _crgmErro(msg) {
    const el = document.getElementById('crgm-erro');
    el.textContent = msg;
    el.classList.toggle('hidden', !msg);
}
