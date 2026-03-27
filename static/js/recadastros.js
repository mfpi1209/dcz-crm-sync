/**
 * RECADASTROS - Dashboard de Recadastros por Origem
 * Consulta visual por origem com filtro de datas
 */

let recadastrosData = [];
let recadChart = null;

function normalizeRecadResponse(payload) {
    const raw = Array.isArray(payload)
        ? payload
        : Array.isArray(payload?.data)
            ? payload.data
            : Array.isArray(payload?.rows)
                ? payload.rows
                : Array.isArray(payload?.result)
                    ? payload.result
                    : Array.isArray(payload?.results)
                        ? payload.results
                        : [];

    return raw
        .map(item => ({
            origem: String(item?.origem ?? item?.site ?? item?.source ?? 'Sem origem'),
            total_recadastros: Number(item?.total_recadastros ?? item?.total ?? item?.count ?? item?.quantidade ?? 0)
        }))
        .filter(item => item.origem && !Number.isNaN(item.total_recadastros));
}

async function loadRecadastros() {
    const btn = document.getElementById('recad-btn-atualizar');
    const btnIcon = document.getElementById('recad-btn-icon');
    const btnText = document.getElementById('recad-btn-text');
    const chartContainer = document.getElementById('recad-chart-container');
    const chartLoading = document.getElementById('recad-chart-loading');
    const chartEmpty = document.getElementById('recad-chart-empty');
    const errorEl = document.getElementById('recad-error');
    const errorMsg = document.getElementById('recad-error-msg');
    const lastUpdateEl = document.getElementById('recad-last-update');

    const fromInput = document.getElementById('recad-filter-from');
    const toInput = document.getElementById('recad-filter-to');
    
    const today = new Date().toISOString().split('T')[0];
    if (fromInput && !fromInput.value) fromInput.value = today;
    if (toInput && !toInput.value) toInput.value = today;
    
    const fromDate = fromInput?.value || today;
    const toDate = toInput?.value || today;

    try {
        if (btn) btn.disabled = true;
        if (btnIcon) btnIcon.classList.add('animate-spin');
        if (btnText) btnText.textContent = 'Atualizando...';

        if (chartContainer) chartContainer.classList.add('hidden');
        if (chartEmpty) chartEmpty.classList.add('hidden');
        if (chartLoading) chartLoading.classList.remove('hidden');
        if (errorEl) errorEl.classList.add('hidden');

        let url = '/api/recadastros';
        const params = new URLSearchParams();
        if (fromDate) params.append('from', fromDate);
        if (toDate) params.append('to', toDate);
        if (params.toString()) url += '?' + params.toString();

        const res = await fetch(url);
        if (!res.ok) throw new Error(`Falha ao consultar webhook (${res.status})`);

        const payload = await res.json();
        
        if (payload.status === 'ERROR' || payload.status === 'TIMEOUT') {
            throw new Error(payload.error || 'Erro ao consultar webhook');
        }
        
        recadastrosData = normalizeRecadResponse(payload.data || payload);

        renderRecadastros();
        updateRecadMetrics();
        
        const now = new Date();
        if (lastUpdateEl) {
            lastUpdateEl.textContent = `Atualizado em ${now.toLocaleString('pt-BR')}`;
        }
        
        if (recadastrosData.length === 0) {
            if (errorEl) errorEl.classList.remove('hidden');
            if (errorMsg) errorMsg.textContent = 'Nenhum recadastro encontrado para o período selecionado.';
        }

        console.log('Recadastros carregados:', recadastrosData.length, 'origens');
    } catch (err) {
        console.error('Erro ao carregar recadastros:', err);
        recadastrosData = [];
        
        if (errorEl) errorEl.classList.remove('hidden');
        if (errorMsg) errorMsg.textContent = err?.message || 'Não foi possível carregar os dados da webhook agora.';
        
        renderRecadastros();
        updateRecadMetrics();
    } finally {
        if (btn) btn.disabled = false;
        if (btnIcon) btnIcon.classList.remove('animate-spin');
        if (btnText) btnText.textContent = 'Atualizar';
        if (chartLoading) chartLoading.classList.add('hidden');
    }
}

function updateRecadMetrics() {
    const totalEl = document.getElementById('recad-total');
    const origensEl = document.getElementById('recad-origens');
    const topOrigemEl = document.getElementById('recad-top-origem');
    const topOrigemTotalEl = document.getElementById('recad-top-origem-total');
    const periodoLabel = document.getElementById('recad-periodo-label');
    
    const fromDate = document.getElementById('recad-filter-from')?.value || '';
    const toDate = document.getElementById('recad-filter-to')?.value || '';

    const totalRecadastros = recadastrosData.reduce((acc, item) => acc + item.total_recadastros, 0);
    const totalOrigens = recadastrosData.length;
    const topOrigem = recadastrosData.length
        ? [...recadastrosData].sort((a, b) => b.total_recadastros - a.total_recadastros)[0]
        : null;

    if (totalEl) totalEl.textContent = totalRecadastros.toLocaleString('pt-BR');
    if (origensEl) origensEl.textContent = totalOrigens;
    
    if (topOrigemEl) {
        topOrigemEl.textContent = topOrigem?.origem ?? '—';
        topOrigemEl.title = topOrigem?.origem ?? '';
    }
    if (topOrigemTotalEl) {
        topOrigemTotalEl.textContent = topOrigem ? `${topOrigem.total_recadastros.toLocaleString('pt-BR')} recadastros` : 'Sem dados';
    }
    
    if (periodoLabel && fromDate && toDate) {
        periodoLabel.textContent = `Período: ${formatDateBR(fromDate)} até ${formatDateBR(toDate)}`;
    }
}

function formatDateBR(dateStr) {
    if (!dateStr) return '';
    const [year, month, day] = dateStr.split('-');
    return `${day}/${month}/${year}`;
}

function renderRecadastros() {
    renderRecadChart();
    renderRecadTable();
}

function renderRecadChart() {
    const chartContainer = document.getElementById('recad-chart-container');
    const chartEmpty = document.getElementById('recad-chart-empty');
    const chartEl = document.getElementById('recad-chart');
    if (!chartEl) return;

    if (recadastrosData.length === 0) {
        if (chartContainer) chartContainer.classList.add('hidden');
        if (chartEmpty) chartEmpty.classList.remove('hidden');
        return;
    }

    if (chartContainer) chartContainer.classList.remove('hidden');
    if (chartEmpty) chartEmpty.classList.add('hidden');

    const labels = recadastrosData.map(item => item.origem);
    const data = recadastrosData.map(item => item.total_recadastros);
    const total = data.reduce((a, b) => a + b, 0);

    const colors = ['#22d3ee','#6366f1','#a855f7','#ec4899','#fb923c','#22c55e','#0ea5e9','#f43f5e','#84cc16','#f59e0b'];

    recadChart = eInit('recad-chart');
    if (!recadChart) return;
    recadChart.setOption({
        backgroundColor: 'transparent',
        tooltip: { ...eTooltip('item'), formatter: p => `${p.name}: ${p.value.toLocaleString('pt-BR')} (${p.percent}%)` },
        legend: { orient: 'vertical', right: 0, top: 'center', textStyle: { color: eThemeColors().textColor, fontSize: 12 } },
        series: [{
            type: 'pie', radius: ['50%', '75%'], center: ['35%', '50%'],
            label: { show: false }, emphasis: { label: { show: true, fontSize: 13, fontWeight: 'bold' } },
            data: labels.map((l, i) => ({ name: l, value: data[i], itemStyle: { color: colors[i % colors.length] } })),
            itemStyle: { borderRadius: 4 },
        }],
        animationDuration: 600,
    }, true);
}

function renderRecadTable() {
    const tbody = document.getElementById('recad-table-body');
    const tableEmpty = document.getElementById('recad-table-empty');
    const tableFooter = document.getElementById('recad-table-footer');

    if (!tbody) return;

    const totalRecadastros = recadastrosData.reduce((acc, item) => acc + item.total_recadastros, 0);

    if (recadastrosData.length === 0) {
        tbody.innerHTML = '';
        if (tableEmpty) tableEmpty.classList.remove('hidden');
        return;
    }

    if (tableEmpty) tableEmpty.classList.add('hidden');

    tbody.innerHTML = recadastrosData.map(row => {
        const percentual = totalRecadastros
            ? ((row.total_recadastros / totalRecadastros) * 100).toFixed(1)
            : '0.0';

        return `
            <tr class="border-b border-[var(--border)] hover:bg-gray-100 dark:hover:bg-gray-800/30 transition-colors">
                <td class="px-6 py-4">
                    <div class="flex items-center gap-3">
                        <span class="w-2.5 h-2.5 rounded-full bg-indigo-400 shadow-lg shadow-indigo-400/50"></span>
                        <span class="text-sm font-medium text-[var(--text-primary)]">${escapeHtml(row.origem)}</span>
                    </div>
                </td>
                <td class="px-6 py-4 text-sm text-gray-300">${row.total_recadastros.toLocaleString('pt-BR')}</td>
                <td class="px-6 py-4">
                    <div class="flex items-center gap-3">
                        <div class="w-32 h-2 bg-gray-200 dark:bg-gray-800 rounded-full overflow-hidden">
                            <div class="h-full rounded-full bg-gradient-to-r from-indigo-400 to-violet-500 transition-all" style="width: ${Math.max(parseFloat(percentual), 4)}%"></div>
                        </div>
                        <span class="text-sm text-gray-300 min-w-[50px]">${percentual}%</span>
                    </div>
                </td>
            </tr>
        `;
    }).join('');

    if (tableFooter) {
        const now = new Date();
        tableFooter.textContent = `Última atualização: ${now.toLocaleString('pt-BR')}`;
    }
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

document.addEventListener('DOMContentLoaded', function() {
    const today = new Date().toISOString().split('T')[0];
    const fromInput = document.getElementById('recad-filter-from');
    const toInput = document.getElementById('recad-filter-to');
    
    if (fromInput) fromInput.value = today;
    if (toInput) toInput.value = today;

    if (location.hash === '#recadastros') {
        loadRecadastros();
    }
});
