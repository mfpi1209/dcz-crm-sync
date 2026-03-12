/**
 * META CAMPAIGNS - Campaign Performance Dashboard
 * Lead Tracking & Conversion Dashboard
 */

let metaCampaignsData = [];

async function loadMetaCampaigns() {
    const btn = document.getElementById('meta-btn-atualizar');
    const tableContainer = document.getElementById('meta-campaigns-table-container');
    const emptyState = document.getElementById('meta-campaigns-empty');
    const loadingState = document.getElementById('meta-loading');
    const statusEl = document.getElementById('meta-status');
    
    try {
        if (btn) {
            btn.innerHTML = '<svg class="w-4 h-4 animate-spin inline mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/></svg>Carregando...';
            btn.disabled = true;
        }
        
        if (tableContainer) tableContainer.classList.add('hidden');
        if (emptyState) emptyState.classList.add('hidden');
        if (loadingState) loadingState.classList.remove('hidden');
        
        const fromDate = document.getElementById('meta-filter-from')?.value || '';
        const toDate = document.getElementById('meta-filter-to')?.value || '';
        
        let url = '/api/meta/campaigns';
        const params = new URLSearchParams();
        if (fromDate) params.append('from', fromDate);
        if (toDate) params.append('to', toDate);
        if (params.toString()) url += '?' + params.toString();
        
        const res = await fetch(url);
        if (!res.ok) throw new Error('Erro ao carregar campanhas');
        const data = await res.json();
        
        metaCampaignsData = data.campaigns || [];
        
        if (statusEl) {
            statusEl.textContent = data.status || 'OK';
            statusEl.className = data.status === 'OK' ? 'text-green-400 font-semibold' : 'text-amber-400 font-semibold';
        }
        
        populateFilters();
        filterMetaCampaigns();
        
        console.log('Meta Campaigns carregadas:', metaCampaignsData.length, 'registros');
    } catch (err) {
        console.error('Erro ao carregar campanhas Meta:', err);
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
            <tr class="border-b border-slate-800/50 hover:bg-slate-800/30 transition-colors">
                <td class="px-6 py-4">
                    <div class="flex items-center gap-3">
                        <div class="w-8 h-8 rounded-lg bg-slate-800 flex items-center justify-center">
                            ${getCampaignIcon(type)}
                        </div>
                        <div>
                            <p class="text-sm font-semibold text-white">${c.utm_campaign || 'Sem nome'}</p>
                            <p class="text-xs text-slate-500">${c.utm_source || 'Meta'} • ${c.utm_medium || ''}</p>
                        </div>
                    </div>
                </td>
                <td class="px-4 py-4">
                    <span class="px-2.5 py-1 text-xs font-semibold rounded-md bg-slate-800 text-slate-300">${type}</span>
                </td>
                <td class="px-4 py-4 text-center">
                    <span class="text-sm font-bold text-white">${c.total_funil || 0}</span>
                </td>
                <td class="px-4 py-4 text-center">
                    <span class="text-sm font-bold text-blue-400">${c.novos || 0}</span>
                </td>
                <td class="px-4 py-4 text-center">
                    <span class="text-sm font-bold text-green-400">${c.ganhos || 0}</span>
                </td>
                <td class="px-4 py-4 text-center">
                    <span class="text-sm font-bold text-red-400">${c.perdidos || 0}</span>
                </td>
                <td class="px-4 py-4">
                    <div class="flex items-center gap-3">
                        <div class="flex-1">
                            <div class="text-xs text-slate-400 mb-1">Conversão</div>
                            <div class="w-full h-1.5 bg-slate-800 rounded-full overflow-hidden">
                                <div class="${convBarColor} h-full rounded-full transition-all" style="width: ${Math.min(convRate, 100)}%"></div>
                            </div>
                        </div>
                        <span class="text-sm font-semibold text-white min-w-[50px] text-right">${convRate.toFixed(1)}%</span>
                    </div>
                </td>
            </tr>
        `;
    }).join('');
}

function updateMetrics(campaigns) {
    const totalLeads = campaigns.reduce((sum, c) => sum + (parseInt(c.total_funil) || 0), 0);
    const totalNovos = campaigns.reduce((sum, c) => sum + (parseInt(c.novos) || 0), 0);
    const totalGanhos = campaigns.reduce((sum, c) => sum + (parseInt(c.ganhos) || 0), 0);
    
    const conversionRate = totalNovos > 0 ? ((totalGanhos / totalNovos) * 100).toFixed(1) + '%' : '—';
    
    const totalLeadsEl = document.getElementById('meta-total-leads');
    const newLeadsEl = document.getElementById('meta-new-leads');
    const conversionEl = document.getElementById('meta-conversion-rate');
    
    if (totalLeadsEl) totalLeadsEl.textContent = totalLeads;
    if (newLeadsEl) newLeadsEl.textContent = totalNovos;
    if (conversionEl) conversionEl.textContent = conversionRate;
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
