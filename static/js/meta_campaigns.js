/**
 * META CAMPAIGNS - Campaign Performance Dashboard
 * Lead Tracking & Conversion Dashboard
 */

let metaCampaignsData = [];
let metaSourceCounts = {
    total:    { meta: 0, google: 0, semCampanha: 0 },
    ganhos:   { meta: 0, google: 0, semCampanha: 0 },
    perdidos: { meta: 0, google: 0, semCampanha: 0 },
};
let _piesByKey = { total: null, ganhos: null, perdidos: null };

function consolidateCampaigns(campaigns) {
    const grouped = {};
    
    campaigns.forEach(c => {
        const key = c.utm_campaign || 'Sem nome';
        
        if (!grouped[key]) {
            grouped[key] = {
                utm_campaign: c.utm_campaign,
                utm_source: c.utm_source,
                utm_medium: c.utm_medium,
                novos: 0,
                ganhos: 0,
                perdidos: 0
            };
        }
        
        grouped[key].novos += parseInt(c.novos) || 0;
        grouped[key].ganhos += parseInt(c.ganhos) || 0;
        grouped[key].perdidos += parseInt(c.perdidos) || 0;
    });
    
    return Object.values(grouped).map(c => {
        c.total_funil = c.novos + c.ganhos + c.perdidos;
        c.conv_ganho_sobre_novo_pct = c.total_funil > 0 ? ((c.ganhos / c.total_funil) * 100) : 0;
        return c;
    });
}

async function loadMetaCampaigns() {
    const btn = document.getElementById('meta-btn-atualizar');
    const tableContainer = document.getElementById('meta-campaigns-table-container');
    const emptyState = document.getElementById('meta-campaigns-empty');
    const loadingState = document.getElementById('meta-loading');
    const statusEl = document.getElementById('meta-status');

    const origem = document.getElementById('meta-filter-origem')?.value || 'ambos';

    try {
        if (btn) {
            btn.innerHTML = '<svg class="w-4 h-4 animate-spin inline mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/></svg>Carregando...';
            btn.disabled = true;
        }

        if (tableContainer) tableContainer.classList.add('hidden');
        if (emptyState) emptyState.classList.add('hidden');
        if (loadingState) loadingState.classList.remove('hidden');

        const fromInput = document.getElementById('meta-filter-from');
        const toInput = document.getElementById('meta-filter-to');

        const today = new Date().toISOString().split('T')[0];
        if (fromInput && !fromInput.value) fromInput.value = today;
        if (toInput && !toInput.value) toInput.value = today;

        const fromDate = fromInput?.value || today;
        const toDate = toInput?.value || today;

        const dateParams = new URLSearchParams({ from: fromDate, to: toDate }).toString();

        let rawCampaigns = [];
        let statusText = 'OK';
        let statusClass = 'text-green-400 font-semibold';

        const _zeroedCounts = () => ({
            total:    { meta: 0, google: 0, semCampanha: 0 },
            ganhos:   { meta: 0, google: 0, semCampanha: 0 },
            perdidos: { meta: 0, google: 0, semCampanha: 0 },
        });

        if (origem === 'meta') {
            metaSourceCounts = _zeroedCounts();
            const res = await fetch(`/api/meta/campaigns?${dateParams}`);
            if (!res.ok) throw new Error('Erro ao carregar campanhas Meta');
            const data = await res.json();
            rawCampaigns = data.campaigns || [];
            console.log('Meta Ads:', rawCampaigns.length, 'registros brutos');
            statusText = data.status || 'OK';
            if (statusText !== 'OK') statusClass = 'text-amber-400 font-semibold';

        } else if (origem === 'google') {
            metaSourceCounts = _zeroedCounts();
            const res = await fetch(`/api/google/campaigns?${dateParams}`);
            if (!res.ok) throw new Error('Erro ao carregar campanhas Google');
            const data = await res.json();
            rawCampaigns = data.campaigns || [];
            console.log('Google Ads:', rawCampaigns.length, 'registros brutos');
            statusText = data.status || 'OK';
            if (statusText !== 'OK') statusClass = 'text-amber-400 font-semibold';

        } else if (origem === 'sem-campanha') {
            metaSourceCounts = _zeroedCounts();
            const res = await fetch(`/api/sem-campanha/leads?${dateParams}`);
            if (!res.ok) throw new Error('Erro ao carregar leads sem campanha');
            const data = await res.json();
            rawCampaigns = data.campaigns || [];
            console.log('Sem Campanha:', rawCampaigns.length, 'registros brutos');
            statusText = data.status || 'OK';
            if (statusText !== 'OK') statusClass = 'text-amber-400 font-semibold';

        } else {
            const [resMeta, resGoogle, resSem] = await Promise.all([
                fetch(`/api/meta/campaigns?${dateParams}`),
                fetch(`/api/google/campaigns?${dateParams}`),
                fetch(`/api/sem-campanha/leads?${dateParams}`),
            ]);

            const dataMeta = resMeta.ok ? await resMeta.json() : { campaigns: [], status: 'ERROR' };
            const dataGoogle = resGoogle.ok ? await resGoogle.json() : { campaigns: [], status: 'ERROR' };
            const dataSem = resSem.ok ? await resSem.json() : { campaigns: [], status: 'ERROR' };

            const metaCampaigns = dataMeta.campaigns || [];
            const googleCampaigns = dataGoogle.campaigns || [];
            const semCampaigns = dataSem.campaigns || [];

            console.log(
                'Meta Ads:', metaCampaigns.length,
                'registros brutos; Google Ads:', googleCampaigns.length,
                'registros brutos; Sem Campanha:', semCampaigns.length, 'registros brutos'
            );

            metaSourceCounts = _zeroedCounts();
            // 'total' agora considera só leads novos (em aberto) — ganhos/perdidos
            // têm seus próprios pies e não devem ser somados ao Total.
            metaCampaigns.forEach(c => {
                const g = parseInt(c.ganhos) || 0;
                const p = parseInt(c.perdidos) || 0;
                const n = parseInt(c.novos) || 0;
                metaSourceCounts.total.meta    += n;
                metaSourceCounts.ganhos.meta   += g;
                metaSourceCounts.perdidos.meta += p;
            });
            googleCampaigns.forEach(c => {
                const g = parseInt(c.ganhos) || 0;
                const p = parseInt(c.perdidos) || 0;
                const n = parseInt(c.novos) || 0;
                metaSourceCounts.total.google    += n;
                metaSourceCounts.ganhos.google   += g;
                metaSourceCounts.perdidos.google += p;
            });
            semCampaigns.forEach(c => {
                const g = parseInt(c.ganhos) || 0;
                const p = parseInt(c.perdidos) || 0;
                const n = parseInt(c.novos) || 0;
                metaSourceCounts.total.semCampanha    += n;
                metaSourceCounts.ganhos.semCampanha   += g;
                metaSourceCounts.perdidos.semCampanha += p;
            });

            rawCampaigns = [...metaCampaigns, ...googleCampaigns, ...semCampaigns];

            const metaStatus = dataMeta.status || 'OK';
            const googleStatus = dataGoogle.status || 'OK';
            const semStatus = dataSem.status || 'OK';
            const allOk = metaStatus === 'OK' && googleStatus === 'OK' && semStatus === 'OK';
            const allFail = metaStatus !== 'OK' && googleStatus !== 'OK' && semStatus !== 'OK';

            if (allOk) {
                statusText = 'OK';
            } else if (allFail) {
                statusText = 'ERRO';
                statusClass = 'text-red-400 font-semibold';
            } else {
                const metaCount = metaSourceCounts.total.meta;
                const googleCount = metaSourceCounts.total.google;
                const semCount = metaSourceCounts.total.semCampanha;
                statusText = `PARCIAL (meta: ${metaCount}, google: ${googleCount}, sem-campanha: ${semCount})`;
                statusClass = 'text-amber-400 font-semibold';
            }
        }

        metaCampaignsData = consolidateCampaigns(rawCampaigns);

        console.log('Campanhas brutas:', rawCampaigns.length, '-> Consolidadas:', metaCampaignsData.length);

        if (statusEl) {
            statusEl.textContent = statusText;
            statusEl.className = statusClass;
        }

        populateFilters();
        filterMetaCampaigns();

        // Mostrar/esconder pie charts de distribuição por origem
        const chartCard = document.getElementById('meta-source-chart-card');
        const totalAll = metaSourceCounts.total.meta + metaSourceCounts.total.google + metaSourceCounts.total.semCampanha
            + metaSourceCounts.ganhos.meta + metaSourceCounts.ganhos.google + metaSourceCounts.ganhos.semCampanha
            + metaSourceCounts.perdidos.meta + metaSourceCounts.perdidos.google + metaSourceCounts.perdidos.semCampanha;
        if (origem === 'ambos' && totalAll > 0) {
            if (chartCard) chartCard.classList.remove('hidden');
            renderPie('total',    'meta-pie-total',    'meta-legend-total',    metaSourceCounts.total);
            renderPie('ganhos',   'meta-pie-ganhos',   'meta-legend-ganhos',   metaSourceCounts.ganhos);
            renderPie('perdidos', 'meta-pie-perdidos', 'meta-legend-perdidos', metaSourceCounts.perdidos);
        } else {
            if (chartCard) chartCard.classList.add('hidden');
        }

        console.log('Campanhas carregadas (origem=' + origem + '):', metaCampaignsData.length, 'registros');
    } catch (err) {
        console.error('Erro ao carregar campanhas:', err);
        metaCampaignsData = [];
        if (statusEl) {
            statusEl.textContent = 'ERRO';
            statusEl.className = 'text-red-400 font-semibold';
        }
        filterMetaCampaigns();
    } finally {
        if (btn) {
            btn.innerHTML = 'Atualizar';
            btn.disabled = false;
        }
        if (loadingState) loadingState.classList.add('hidden');
    }
}

function populateFilters() {
    const criativoSelect = document.getElementById('meta-filter-criativo');
    const campanhaSelect = document.getElementById('meta-filter-campanha');
    
    if (criativoSelect) {
        const types = [...new Set(metaCampaignsData.map(c => getCampaignType(c.utm_campaign)))].filter(Boolean);
        criativoSelect.innerHTML = '<option value="">Todos os Tipos</option>';
        types.forEach(type => {
            const opt = document.createElement('option');
            opt.value = type;
            opt.textContent = type;
            criativoSelect.appendChild(opt);
        });
    }
    
    if (campanhaSelect) {
        const campaigns = [...new Set(metaCampaignsData.map(c => c.utm_campaign))].filter(Boolean);
        campanhaSelect.innerHTML = '<option value="">Todas as Campanhas</option>';
        campaigns.forEach(name => {
            const opt = document.createElement('option');
            opt.value = name;
            opt.textContent = name;
            campanhaSelect.appendChild(opt);
        });
    }
}

function filterMetaCampaigns() {
    const criativoFilter = document.getElementById('meta-filter-criativo')?.value || '';
    const campanhaFilter = document.getElementById('meta-filter-campanha')?.value || '';
    const searchFilter = (document.getElementById('meta-search')?.value || '').toLowerCase();
    
    let filtered = [...metaCampaignsData];
    
    if (criativoFilter) {
        filtered = filtered.filter(c => getCampaignType(c.utm_campaign) === criativoFilter);
    }
    
    if (campanhaFilter) {
        filtered = filtered.filter(c => c.utm_campaign === campanhaFilter);
    }
    
    if (searchFilter) {
        filtered = filtered.filter(c => 
            (c.utm_campaign || '').toLowerCase().includes(searchFilter) ||
            (c.utm_source || '').toLowerCase().includes(searchFilter) ||
            (c.utm_medium || '').toLowerCase().includes(searchFilter)
        );
    }
    
    renderCampaignsTable(filtered);
    updateMetrics(filtered);
}

function renderCampaignsTable(campaigns) {
    const tbody = document.getElementById('meta-table-body');
    const tableContainer = document.getElementById('meta-campaigns-table-container');
    const emptyState = document.getElementById('meta-campaigns-empty');
    const countEl = document.getElementById('meta-campaigns-count');
    
    if (!tbody) return;
    
    if (countEl) countEl.textContent = campaigns.length;
    
    if (campaigns.length === 0) {
        if (tableContainer) tableContainer.classList.add('hidden');
        if (emptyState) emptyState.classList.remove('hidden');
        return;
    }
    
    if (tableContainer) tableContainer.classList.remove('hidden');
    if (emptyState) emptyState.classList.add('hidden');
    
    tbody.innerHTML = campaigns.map(c => {
        const type = getCampaignType(c.utm_campaign);
        const convRate = parseFloat(c.conv_ganho_sobre_novo_pct) || 0;
        const convBarColor = convRate > 5 ? 'bg-green-500' : convRate > 0 ? 'bg-blue-500' : 'bg-slate-600';
        
        return `
            <tr class="border-b border-slate-200 dark:border-slate-800/50 hover:bg-slate-50 dark:hover:bg-slate-800/30 transition-colors">
                <td class="px-6 py-4">
                    <div class="flex items-center gap-3">
                        <div class="w-8 h-8 rounded-lg bg-slate-200 dark:bg-slate-800 flex items-center justify-center">
                            ${getCampaignIcon(type)}
                        </div>
                        <div>
                            <p class="text-sm font-semibold text-[var(--text-primary)] dark:text-white">${c.utm_campaign || 'Sem nome'}</p>
                            <p class="text-xs text-slate-500">${getSourceLabel(c.utm_source)} • ${c.utm_medium || ''}</p>
                        </div>
                    </div>
                </td>
                <td class="px-4 py-4">
                    <span class="px-2.5 py-1 text-xs font-semibold rounded-md bg-slate-200 dark:bg-slate-800 text-slate-700 dark:text-slate-300">${type}</span>
                </td>
                <td class="px-4 py-4 text-center">
                    <span class="text-sm font-bold text-[var(--text-primary)] dark:text-white">${c.total_funil || 0}</span>
                </td>
                <td class="px-4 py-4 text-center">
                    <span class="text-sm font-bold text-blue-600 dark:text-blue-400">${c.novos || 0}</span>
                </td>
                <td class="px-4 py-4 text-center">
                    <span class="text-sm font-bold text-green-600 dark:text-green-400">${c.ganhos || 0}</span>
                </td>
                <td class="px-4 py-4 text-center">
                    <span class="text-sm font-bold text-red-600 dark:text-red-400">${c.perdidos || 0}</span>
                </td>
                <td class="px-4 py-4">
                    <div class="flex items-center gap-3">
                        <div class="flex-1">
                            <div class="text-xs text-slate-500 dark:text-slate-400 mb-1">Conversão</div>
                            <div class="w-full h-1.5 bg-slate-200 dark:bg-slate-800 rounded-full overflow-hidden">
                                <div class="${convBarColor} h-full rounded-full transition-all" style="width: ${Math.min(convRate, 100)}%"></div>
                            </div>
                        </div>
                        <span class="text-sm font-semibold text-[var(--text-primary)] dark:text-white min-w-[50px] text-right">${convRate.toFixed(1)}%</span>
                    </div>
                </td>
            </tr>
        `;
    }).join('');
}

function updateMetrics(campaigns) {
    const totalNovos = campaigns.reduce((sum, c) => sum + (parseInt(c.novos) || 0), 0);
    const totalGanhos = campaigns.reduce((sum, c) => sum + (parseInt(c.ganhos) || 0), 0);
    const totalPerdidos = campaigns.reduce((sum, c) => sum + (parseInt(c.perdidos) || 0), 0);
    // Total de Leads agora reflete apenas leads novos (em aberto).
    // Ganhos e Perdidos têm seus próprios cards e não devem ser somados ao Total.
    const totalGeral = totalNovos;
    const baseFunil = totalNovos + totalGanhos + totalPerdidos;

    const ganhosPct = baseFunil > 0 ? ((totalGanhos / baseFunil) * 100).toFixed(1) : '0';
    const perdidosPct = baseFunil > 0 ? ((totalPerdidos / baseFunil) * 100).toFixed(1) : '0';

    const totalLeadsEl = document.getElementById('meta-total-leads');
    const leadsGanhosEl = document.getElementById('meta-leads-ganhos');
    const leadsPerdidosEl = document.getElementById('meta-leads-perdidos');
    const ganhosPctEl = document.getElementById('meta-ganhos-pct');
    const perdidosPctEl = document.getElementById('meta-perdidos-pct');
    
    if (totalLeadsEl) totalLeadsEl.textContent = totalGeral;
    if (leadsGanhosEl) leadsGanhosEl.textContent = totalGanhos;
    if (leadsPerdidosEl) leadsPerdidosEl.textContent = totalPerdidos;
    if (ganhosPctEl) ganhosPctEl.textContent = `(${ganhosPct}%)`;
    if (perdidosPctEl) perdidosPctEl.textContent = `(${perdidosPct}%)`;
}

function renderPie(key, canvasId, legendId, counts) {
    const canvas = document.getElementById(canvasId);
    if (!canvas) return;

    if (_piesByKey[key]) {
        _piesByKey[key].destroy();
        _piesByKey[key] = null;
    }

    // 'Sem Campanha' não compõe o pie de Total (são leads não-originados por campanha).
    // Para os pies de Ganhos/Perdidos ele continua aparecendo.
    const includeSemCampanha = key !== 'total';
    const semVal = includeSemCampanha ? (counts.semCampanha || 0) : 0;
    const total = counts.meta + counts.google + semVal;

    const labels = ['Meta Ads', 'Google Ads'];
    const data = [counts.meta, counts.google];
    const colors = ['#3B82F6', '#F59E0B'];
    if (includeSemCampanha) {
        labels.push('Sem Campanha');
        data.push(counts.semCampanha || 0);
        colors.push('#A855F7');
    }

    _piesByKey[key] = new Chart(canvas, {
        type: 'pie',
        data: {
            labels,
            datasets: [{
                data,
                backgroundColor: colors,
                borderWidth: 2,
                borderColor: 'transparent',
            }]
        },
        options: {
            responsive: true,
            plugins: {
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            const val = context.parsed;
                            const pct = total > 0 ? ((val / total) * 100).toFixed(1) : '0';
                            return ` ${val} leads (${pct}%)`;
                        }
                    }
                }
            }
        }
    });

    const legendEl = document.getElementById(legendId);
    if (legendEl) {
        const items = [
            { label: 'Meta Ads',     color: '#3B82F6', value: counts.meta },
            { label: 'Google Ads',   color: '#F59E0B', value: counts.google },
        ];
        if (includeSemCampanha) {
            items.push({ label: 'Sem Campanha', color: '#A855F7', value: counts.semCampanha || 0 });
        }
        legendEl.innerHTML = items.map(item => {
            const pct = total > 0 ? ((item.value / total) * 100).toFixed(1) : '—';
            const display = total > 0 ? `${item.value} (${pct}%)` : `0 (—)`;
            return `
                <div class="flex items-center gap-2">
                    <div class="w-2.5 h-2.5 rounded-full flex-shrink-0" style="background-color: ${item.color};"></div>
                    <span class="text-xs text-[var(--text-primary)] dark:text-white font-medium">${item.label}</span>
                    <span class="text-xs text-slate-500 ml-auto">${display}</span>
                </div>
            `;
        }).join('');
    }
}

function getSourceLabel(utmSource) {
    if (!utmSource) return 'Meta Ads';
    const s = utmSource.toLowerCase();
    if (s === 'sem_campanha') return 'Sem Campanha';
    if (s.includes('google')) return 'Google Ads';
    if (s.includes('facebook') || s.includes('meta') || s.includes('instagram') || s.includes('fb')) return 'Meta Ads';
    return utmSource;
}

function getCampaignType(campaignName) {
    if (!campaignName) return 'OTHER';
    const name = campaignName.toLowerCase();
    if (name.includes('video')) return 'VIDEO';
    if (name.includes('image') || name.includes('imagem')) return 'IMAGE';
    if (name.includes('carousel') || name.includes('carrossel')) return 'CAROUSEL';
    if (name.includes('cursos') || name.includes('graduacao')) return 'IMAGE';
    return 'IMAGE';
}

function getCampaignIcon(type) {
    const icons = {
        'VIDEO': '<svg class="w-4 h-4 text-violet-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 10l4.553-2.276A1 1 0 0121 8.618v6.764a1 1 0 01-1.447.894L15 14M5 18h8a2 2 0 002-2V8a2 2 0 00-2-2H5a2 2 0 00-2 2v8a2 2 0 002 2z"/></svg>',
        'IMAGE': '<svg class="w-4 h-4 text-blue-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16l4.586-4.586a2 2 0 012.828 0L16 16m-2-2l1.586-1.586a2 2 0 012.828 0L20 14m-6-6h.01M6 20h12a2 2 0 002-2V6a2 2 0 00-2-2H6a2 2 0 00-2 2v12a2 2 0 002 2z"/></svg>',
        'CAROUSEL': '<svg class="w-4 h-4 text-amber-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"/></svg>'
    };
    return icons[type] || icons['IMAGE'];
}

document.addEventListener('DOMContentLoaded', function() {
    const today = new Date().toISOString().split('T')[0];
    const fromInput = document.getElementById('meta-filter-from');
    const toInput = document.getElementById('meta-filter-to');
    if (fromInput) fromInput.value = today;
    if (toInput) toInput.value = today;
    
    if (location.hash === '#meta-campaigns') {
        loadMetaCampaigns();
    }
});
