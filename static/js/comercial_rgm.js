// ---------------------------------------------------------------------------
// Dashboard Comercial RGM
// ---------------------------------------------------------------------------

let _crgmChartEvolucao = null;
let _crgmChartRanking = null;

async function loadComercialRgm() {
    await _crgmLoadFilters();
    await _crgmLoadSnapshotInfo();

    const hoje = new Date();
    const ini = new Date(hoje.getFullYear(), hoje.getMonth(), 1);
    const elIni = document.getElementById('crgm-dt-ini');
    const elFim = document.getElementById('crgm-dt-fim');
    if (!elIni.value) elIni.value = ini.toISOString().substring(0, 10);
    if (!elFim.value) elFim.value = hoje.toISOString().substring(0, 10);

    crgmAtualizar();
}

async function _crgmLoadFilters() {
    try {
        const res = await api('/api/comercial-rgm/filters');
        const d = await res.json();
        if (!d.ok) return;

        const selPolo = document.getElementById('crgm-polo');
        const selNivel = document.getElementById('crgm-nivel');

        const curPolo = selPolo.value;
        const curNivel = selNivel.value;

        selPolo.innerHTML = '<option value="">Todos</option>' +
            d.polos.map(p => `<option value="${esc(p)}">${esc(p)}</option>`).join('');
        selNivel.innerHTML = '<option value="">Todos</option>' +
            d.niveis.map(n => `<option value="${esc(n)}">${esc(n)}</option>`).join('');

        if (curPolo) selPolo.value = curPolo;
        if (curNivel) selNivel.value = curNivel;
    } catch (e) {
        console.error('crgm filters', e);
    }
}

async function _crgmLoadSnapshotInfo() {
    try {
        const res = await api('/api/comercial-rgm/snapshot-info');
        const d = await res.json();
        if (!d.ok || !d.total) return;

        const dt = d.uploaded_at ? new Date(d.uploaded_at).toLocaleString('pt-BR') : '';
        document.getElementById('crgm-snapshot-info').textContent =
            `${d.total.toLocaleString('pt-BR')} registros | ${d.min_date || ''} a ${d.max_date || ''} | Atualizado: ${dt}`;
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

        _crgmRenderKPIs(d.kpis);
        _crgmRenderEvolucao(d.evolucao);
        _crgmRenderRanking(d.ranking_polo);
        _crgmRenderCicloTable(d.ranking_ciclo);
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
    document.getElementById('crgm-valor-total').textContent = 'R$ ' + (k.valor_total / 1000).toFixed(0) + 'k';
    document.getElementById('crgm-dias').textContent = k.dias;

    document.getElementById('crgm-1a-val').textContent = k.vendas_1a.toLocaleString('pt-BR');
    _crgmSetBadge('crgm-1a-badge', k.pct_1a);
    document.getElementById('crgm-6m-val').textContent = k.vendas_6m.toLocaleString('pt-BR');
    _crgmSetBadge('crgm-6m-badge', k.pct_6m);
    document.getElementById('crgm-ytd-prev').textContent = (k.vendas_prev_ytd || 0).toLocaleString('pt-BR');
    _crgmSetBadge('crgm-ytd-badge', k.pct_ytd);
}

function _crgmSetBadge(id, pct) {
    const el = document.getElementById(id);
    const sign = pct >= 0 ? '↑' : '↓';
    el.textContent = `${sign} ${Math.abs(pct)}%`;
    el.className = pct >= 0
        ? 'font-bold px-1.5 py-0.5 rounded text-[10px] bg-emerald-500/20 text-emerald-400'
        : 'font-bold px-1.5 py-0.5 rounded text-[10px] bg-red-500/20 text-red-400';
}

function _crgmRenderEvolucao(evolucao) {
    const ctx = document.getElementById('crgm-chart-evolucao');
    if (_crgmChartEvolucao) _crgmChartEvolucao.destroy();

    const labels = evolucao.map(e => {
        const d = new Date(e.data + 'T00:00:00');
        return d.toLocaleDateString('pt-BR', { day: '2-digit', month: 'short' });
    });
    const values = evolucao.map(e => e.count);

    _crgmChartEvolucao = new Chart(ctx, {
        type: 'line',
        data: {
            labels,
            datasets: [{
                label: 'Matrículas',
                data: values,
                borderColor: '#06b6d4',
                backgroundColor: 'rgba(6,182,212,0.1)',
                borderWidth: 2,
                fill: true,
                tension: 0.3,
                pointRadius: evolucao.length > 60 ? 0 : 3,
                pointBackgroundColor: '#06b6d4',
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                x: {
                    ticks: { color: '#64748b', maxTicksLimit: 12, font: { size: 10 } },
                    grid: { color: 'rgba(100,116,139,0.1)' }
                },
                y: {
                    beginAtZero: true,
                    ticks: { color: '#64748b', font: { size: 10 } },
                    grid: { color: 'rgba(100,116,139,0.1)' }
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
        '#3b82f6', '#6366f1', '#8b5cf6', '#a855f7',
        '#06b6d4', '#14b8a6', '#22c55e', '#eab308',
        '#f97316', '#ef4444', '#ec4899', '#64748b',
    ];

    _crgmChartRanking = new Chart(ctx, {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                data: values,
                backgroundColor: labels.map((_, i) => colors[i % colors.length] + '99'),
                borderColor: labels.map((_, i) => colors[i % colors.length]),
                borderWidth: 1,
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
                    grid: { color: 'rgba(100,116,139,0.1)' }
                },
                y: {
                    ticks: { color: '#cbd5e1', font: { size: 11 } },
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
        `<tr class="hover:bg-white/[0.02]">
            <td class="px-5 py-2.5 text-slate-300">${esc(c.nome)}</td>
            <td class="px-5 py-2.5 text-right text-slate-200 font-semibold">${c.total.toLocaleString('pt-BR')}</td>
        </tr>`
    ).join('');
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
