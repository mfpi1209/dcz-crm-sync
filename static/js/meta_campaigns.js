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
let metaGoogleInsights = null;

// Data local (BRT). toISOString() dá o dia em UTC e vira o dia seguinte depois das 21h.
function _metaHoje(offsetDias = 0) {
    const d = new Date();
    d.setDate(d.getDate() + offsetDias);
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
}

function metaSetPeriodo(tipo) {
    const fromInput = document.getElementById('meta-filter-from');
    const toInput = document.getElementById('meta-filter-to');
    if (!fromInput || !toInput) return;
    const hoje = _metaHoje();
    toInput.value = hoje;
    if (tipo === '7d') fromInput.value = _metaHoje(-6);
    else if (tipo === 'mes') fromInput.value = hoje.slice(0, 8) + '01';
    else fromInput.value = hoje;
    loadMetaCampaigns();
}

function _metaEsc(v) {
    return String(v ?? '').replace(/[&<>"']/g, ch => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[ch]));
}

const _META_STATUS_LABEL = { OK: 'OK', VAZIO: 'sem dados', TIMEOUT: 'sem resposta', ERROR: 'erro' };
function _metaStatusLabel(nome, st, data) {
    // Desde 01/10 Meta e Sem Campanha vêm do Bwipo; o job ainda pode estar coletando a origem dos contatos.
    const pend = parseInt(data?.pendentes) || 0;
    const extra = pend > 0 ? ` (coletando origem de ${pend} contatos)` : '';
    return `${nome}: ${_META_STATUS_LABEL[st] || st}${extra}`;
}

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
                perdidos: 0,
                matriculas: 0,
                has_matriculas: false,
            };
        }

        grouped[key].novos += parseInt(c.novos) || 0;
        grouped[key].ganhos += parseInt(c.ganhos) || 0;
        grouped[key].perdidos += parseInt(c.perdidos) || 0;
        if (c.matriculas !== undefined) {
            grouped[key].has_matriculas = true;
            grouped[key].matriculas += parseInt(c.matriculas) || 0;
        }
    });

    // Google: conversão = matrículas / leads (ganho/perdido do Kommo parou em set/2026).
    // Meta e Sem Campanha seguem com ganhos / total.
    return Object.values(grouped).map(c => {
        c.total_funil = c.novos + c.ganhos + c.perdidos;
        const num = c.has_matriculas ? c.matriculas : c.ganhos;
        c.conv_ganho_sobre_novo_pct = c.total_funil > 0 ? ((num / c.total_funil) * 100) : 0;
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

        const today = _metaHoje();
        if (fromInput && !fromInput.value) fromInput.value = today;
        if (toInput && !toInput.value) toInput.value = today;

        const fromDate = fromInput?.value || today;
        const toDate = toInput?.value || today;

        const dateParams = new URLSearchParams({ from: fromDate, to: toDate }).toString();

        let rawCampaigns = [];
        metaGoogleInsights = null;
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
            statusText = _metaStatusLabel('Meta', data.status || 'OK', data);
            if ((data.status || 'OK') !== 'OK') statusClass = 'text-amber-400 font-semibold';

        } else if (origem === 'google') {
            metaSourceCounts = _zeroedCounts();
            const res = await fetch(`/api/google/campaigns?${dateParams}`);
            if (!res.ok) throw new Error('Erro ao carregar campanhas Google');
            const data = await res.json();
            rawCampaigns = data.campaigns || [];
            metaGoogleInsights = data.insights || null;
            console.log('Google Ads:', rawCampaigns.length, 'registros brutos');
            statusText = _metaStatusLabel('Google', data.status || 'OK');
            if ((data.status || 'OK') !== 'OK') statusClass = 'text-amber-400 font-semibold';

        } else if (origem === 'sem-campanha') {
            metaSourceCounts = _zeroedCounts();
            const res = await fetch(`/api/sem-campanha/leads?${dateParams}`);
            if (!res.ok) throw new Error('Erro ao carregar leads sem campanha');
            const data = await res.json();
            rawCampaigns = data.campaigns || [];
            console.log('Sem Campanha:', rawCampaigns.length, 'registros brutos');
            statusText = _metaStatusLabel('Sem campanha', data.status || 'OK', data);
            if ((data.status || 'OK') !== 'OK') statusClass = 'text-amber-400 font-semibold';

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
            metaGoogleInsights = dataGoogle.insights || null;

            console.log(
                'Meta Ads:', metaCampaigns.length,
                'registros brutos; Google Ads:', googleCampaigns.length,
                'registros brutos; Sem Campanha:', semCampaigns.length, 'registros brutos'
            );

            metaSourceCounts = _zeroedCounts();
            // Todas as origens por safra: total = leads criados no período (aberto + ganho + perdido).
            // (Meta e Sem Campanha até 30/09 vinham do Kommo com 'novos' = só em aberto.)
            metaCampaigns.forEach(c => {
                const g = parseInt(c.ganhos) || 0;
                const p = parseInt(c.perdidos) || 0;
                const n = parseInt(c.novos) || 0;
                metaSourceCounts.total.meta    += n + g + p;
                metaSourceCounts.ganhos.meta   += g;
                metaSourceCounts.perdidos.meta += p;
            });
            googleCampaigns.forEach(c => {
                const g = parseInt(c.ganhos) || 0;
                const p = parseInt(c.perdidos) || 0;
                const n = parseInt(c.novos) || 0;
                // Google é por safra: todo lead criado no período (aberto + ganho + perdido)
                metaSourceCounts.total.google    += n + g + p;
                metaSourceCounts.ganhos.google   += g;
                metaSourceCounts.perdidos.google += p;
            });
            semCampaigns.forEach(c => {
                const g = parseInt(c.ganhos) || 0;
                const p = parseInt(c.perdidos) || 0;
                const n = parseInt(c.novos) || 0;
                metaSourceCounts.total.semCampanha    += n + g + p;
                metaSourceCounts.ganhos.semCampanha   += g;
                metaSourceCounts.perdidos.semCampanha += p;
            });

            rawCampaigns = [...metaCampaigns, ...googleCampaigns, ...semCampaigns];

            const metaStatus = dataMeta.status || 'OK';
            const googleStatus = dataGoogle.status || 'OK';
            const semStatus = dataSem.status || 'OK';
            // Status por origem: "sem dados" (n8n/tabela vazia) não é erro.
            const sts = [metaStatus, googleStatus, semStatus];
            const isErr = st => st === 'ERROR' || st === 'TIMEOUT';
            statusText = [
                _metaStatusLabel('Meta', metaStatus, dataMeta),
                _metaStatusLabel('Google', googleStatus),
                _metaStatusLabel('Sem campanha', semStatus),
            ].join(' · ');
            if (sts.every(isErr)) statusClass = 'text-red-400 font-semibold';
            else if (sts.some(isErr)) statusClass = 'text-amber-400 font-semibold';
            else if (sts.some(st => st !== 'OK')) statusClass = 'text-slate-400 font-semibold';
        }

        metaCampaignsData = consolidateCampaigns(rawCampaigns);

        console.log('Campanhas brutas:', rawCampaigns.length, '-> Consolidadas:', metaCampaignsData.length);

        if (statusEl) {
            statusEl.textContent = statusText;
            statusEl.className = statusClass;
        }

        populateFilters();
        filterMetaCampaigns();
        renderGoogleInsights(metaGoogleInsights);

        // Gráfico de origens só faz sentido com 2+ origens com dados (pizza de um sabor só não diz nada)
        const chartCard = document.getElementById('meta-source-chart-card');
        const sc = metaSourceCounts;
        const origensComDados = ['meta', 'google', 'semCampanha']
            .filter(o => (sc.total[o] + sc.ganhos[o] + sc.perdidos[o]) > 0).length;
        _metaPiesPendentes = origem === 'ambos' && origensComDados >= 2;
        if (chartCard) chartCard.classList.toggle('hidden', !_metaPiesPendentes);
        if (_metaPiesPendentes && chartCard?.open) _metaRenderPies();

        console.log('Campanhas carregadas (origem=' + origem + '):', metaCampaignsData.length, 'registros');
    } catch (err) {
        console.error('Erro ao carregar campanhas:', err);
        metaCampaignsData = [];
        renderGoogleInsights(null);
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
    const countEl2 = document.getElementById('meta-campaigns-count-2');
    if (countEl2) countEl2.textContent = campaigns.length;

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
                            <p class="text-sm font-semibold text-[var(--text-primary)] dark:text-white">${_metaEsc(c.utm_campaign || 'Sem nome')}</p>
                            <p class="text-xs text-slate-500">${_metaEsc(getSourceLabel(c.utm_source))} • ${_metaEsc(c.utm_medium || '')}</p>
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
                <td class="px-4 py-4 text-center">
                    ${c.has_matriculas
                        ? `<span class="text-sm font-bold text-amber-600 dark:text-amber-400">${c.matriculas || 0}</span>`
                        : '<span class="text-sm text-slate-400">—</span>'}
                </td>
                <td class="px-4 py-4">
                    <div class="flex items-center gap-3">
                        <div class="flex-1">
                            <div class="text-xs text-slate-500 dark:text-slate-400 mb-1">${c.has_matriculas ? 'Lead → venda' : 'Ganhos / total'}</div>
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
    // Google: leads criados no período, e desses quantos ganhou/perdeu (o resto está em aberto).
    const baseFunil = totalNovos + totalGanhos + totalPerdidos;
    const totalGeral = baseFunil;
    const totalSubEl = document.getElementById('meta-total-sub');
    if (totalSubEl) totalSubEl.textContent = baseFunil
        ? `criados no período · ${_metaFmtNum(totalNovos)} em aberto`
        : 'criados no período';

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

    // Card de matrículas: soma das campanhas Google visíveis (respeita filtros e busca)
    const matEl = document.getElementById('meta-matriculas-google');
    const convEl = document.getElementById('meta-matriculas-conv');
    const subEl = document.getElementById('meta-matriculas-sub');
    if (!matEl) return;
    const comMat = campaigns.filter(c => c.has_matriculas);
    if (!metaGoogleInsights) {
        matEl.textContent = '—';
        if (convEl) convEl.textContent = '';
        if (subEl) subEl.textContent = 'aparece na origem Todos ou Google';
        return;
    }
    const mat = comMat.reduce((s, c) => s + (c.matriculas || 0), 0);
    const leads = comMat.reduce((s, c) => s + (c.total_funil || 0), 0);
    matEl.textContent = mat;
    if (convEl) convEl.textContent = leads ? `(${((mat / leads) * 100).toFixed(1)}%)` : '';
    const k = metaGoogleInsights.kpis;
    if (subEl && k) {
        subEl.textContent = k.matriculas > mat
            ? `matrículas vindas de anúncio Google · ${k.matriculas - mat} sem campanha identificada`
            : 'matrículas vindas de anúncio Google';
    }
}

function _metaFmtNum(n, dec = 0) {
    return Number(n || 0).toLocaleString('pt-BR', { minimumFractionDigits: dec, maximumFractionDigits: dec });
}

function _metaBarList(elId, items, total, extra) {
    const el = document.getElementById(elId);
    if (!el) return;
    if (!items || !items.length) {
        el.innerHTML = '<p class="text-xs text-slate-500">Sem dados no período.</p>';
        return;
    }
    const val = i => i.n ?? i.matriculas ?? 0;
    const max = Math.max(...items.map(val), 1);
    el.innerHTML = items.map(i => {
        const n = val(i);
        const sufixo = extra ? extra(i) : (total ? ` · ${((n / total) * 100).toFixed(0)}%` : '');
        return `
            <div>
                <div class="flex items-baseline justify-between gap-2 text-xs">
                    <span class="truncate text-[var(--text-primary)] dark:text-slate-200" title="${_metaEsc(i.nome)}">${_metaEsc(i.nome)}</span>
                    <span class="flex-shrink-0 text-slate-500">${n}${sufixo}</span>
                </div>
                <div class="w-full h-1 mt-1 bg-slate-200 dark:bg-slate-800 rounded-full overflow-hidden">
                    <div class="h-full rounded-full bg-amber-500" style="width:${(n / max) * 100}%"></div>
                </div>
            </div>`;
    }).join('');
}

function renderGoogleInsights(ins) {
    const box = document.getElementById('meta-google-insights');
    if (!box) return;
    const k = ins?.kpis;
    if (!k || !k.matriculas) {
        box.classList.add('hidden');
        return;
    }
    box.classList.remove('hidden');
    _metaBarList('meta-gi-campanhas', ins.top_campanhas, 0, i => ` · ${_metaFmtNum(i.conv_pct, 1)}% de ${i.leads} leads`);
    _metaBarList('meta-gi-cursos', ins.top_cursos, k.matriculas);
    _metaBarList('meta-gi-polos', ins.top_polos, k.matriculas);
}

// Os gráficos ficam num bloco recolhido: desenha só quando ele está aberto (canvas escondido fica com tamanho 0).
let _metaPiesPendentes = false;
function _metaRenderPies() {
    _metaPiesPendentes = false;
    renderPie('total',    'meta-pie-total',    'meta-legend-total',    metaSourceCounts.total);
    renderPie('ganhos',   'meta-pie-ganhos',   'meta-legend-ganhos',   metaSourceCounts.ganhos);
    renderPie('perdidos', 'meta-pie-perdidos', 'meta-legend-perdidos', metaSourceCounts.perdidos);
}
document.addEventListener('toggle', e => {
    if (e.target?.id === 'meta-source-chart-card' && e.target.open && _metaPiesPendentes) _metaRenderPies();
}, true);

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
    const today = _metaHoje();
    const fromInput = document.getElementById('meta-filter-from');
    const toInput = document.getElementById('meta-filter-to');
    if (fromInput) fromInput.value = today;
    if (toInput) toInput.value = today;
    
    if (location.hash === '#meta-campaigns') {
        loadMetaCampaigns();
    }
});
