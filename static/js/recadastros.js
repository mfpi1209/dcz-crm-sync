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

    const fromDate = document.getElementById('recad-filter-from')?.value || '';
    const toDate = document.getElementById('recad-filter-to')?.value || '';

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
        recadastrosData = normalizeRecadResponse(payload.data || payload);

        if (!recadastrosData.length) {
            throw new Error('A webhook respondeu, mas não retornou linhas no formato esperado.');
        }

        renderRecadastros();
        updateRecadMetrics();
        
        const now = new Date();
        if (lastUpdateEl) {
            lastUpdateEl.textContent = `Atualizado em ${now.toLocaleString('pt-BR')}`;
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
    const ctx = document.getElementById('recad-chart')?.getContext('2d');
    
    if (!ctx) return;

    if (recadastrosData.length === 0) {
        if (chartContainer) chartContainer.classList.add('hidden');
        if (chartEmpty) chartEmpty.classList.remove('hidden');
        return;
    }

    if (chartContainer) chartContainer.classList.remove('hidden');
    if (chartEmpty) chartEmpty.classList.add('hidden');

    if (recadChart) {
        recadChart.destroy();
    }

    const labels = recadastrosData.map(item => item.origem);
    const data = recadastrosData.map(item => item.total_recadastros);

    const gradient = ctx.createLinearGradient(0, 0, 0, 400);
    gradient.addColorStop(0, 'rgba(34, 211, 238, 0.9)');
    gradient.addColorStop(1, 'rgba(99, 102, 241, 0.9)');

    recadChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Recadastros',
                data: data,
                backgroundColor: gradient,
                borderColor: 'rgba(34, 211, 238, 0.5)',
                borderWidth: 1,
                borderRadius: 8,
                borderSkipped: false
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    backgroundColor: 'rgba(15, 23, 42, 0.95)',
                    borderColor: 'rgba(255, 255, 255, 0.1)',
                    borderWidth: 1,
                    titleColor: '#cbd5e1',
                    bodyColor: '#fff',
                    padding: 12,
                    cornerRadius: 12,
                    displayColors: false,
                    callbacks: {
                        label: function(context) {
                            return `${context.parsed.y.toLocaleString('pt-BR')} recadastros`;
                        }
                    }
                }
            },
            scales: {
                x: {
                    grid: {
                        display: false
                    },
                    ticks: {
                        color: 'rgba(148, 163, 184, 0.7)',
                        font: { size: 11 },
                        maxRotation: 45,
                        minRotation: 0
                    }
                },
                y: {
                    beginAtZero: true,
                    grid: {
                        color: 'rgba(148, 163, 184, 0.08)'
                    },
                    ticks: {
                        color: 'rgba(148, 163, 184, 0.7)',
                        font: { size: 11 },
                        callback: function(value) {
                            return value.toLocaleString('pt-BR');
                        }
                    }
                }
            }
        }
    });
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
            <tr class="border-b border-slate-800/50 hover:bg-slate-800/30 transition-colors">
                <td class="px-6 py-4">
                    <div class="flex items-center gap-3">
                        <span class="w-2.5 h-2.5 rounded-full bg-cyan-400 shadow-lg shadow-cyan-400/50"></span>
                        <span class="text-sm font-medium text-white">${escapeHtml(row.origem)}</span>
                    </div>
                </td>
                <td class="px-6 py-4 text-sm text-slate-300">${row.total_recadastros.toLocaleString('pt-BR')}</td>
                <td class="px-6 py-4">
                    <div class="flex items-center gap-3">
                        <div class="w-32 h-2 bg-slate-800 rounded-full overflow-hidden">
                            <div class="h-full rounded-full bg-gradient-to-r from-cyan-400 to-violet-500 transition-all" style="width: ${Math.max(parseFloat(percentual), 4)}%"></div>
                        </div>
                        <span class="text-sm text-slate-300 min-w-[50px]">${percentual}%</span>
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
