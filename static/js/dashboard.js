// ---------------------------------------------------------------------------
// Dashboard
// ---------------------------------------------------------------------------
(function _localhostDashboardCachePurge() {
    const h = location.hostname;
    if (h !== 'localhost' && h !== '127.0.0.1') return;
    const marker = 'dcz_localhost_purge_v4';
    if (localStorage.getItem(marker)) return;
    ['dash_ciclo_v1', 'dash_ciclo_v2', 'dsa_ciclo_v1', 'dsa_ciclo_v2'].forEach((k) => {
        localStorage.removeItem(k);
    });
    localStorage.setItem(marker, String(Date.now()));
})();

async function loadDashboard() {
    try {
        if (window._sidebarPermsReady && window._sidebarPermsReady.then) {
            await window._sidebarPermsReady;
        }
    } catch (_) { /* noop */ }

    const _dashAcad      = document.getElementById('dash-academic');
    const _dashSupComm   = document.getElementById('dash-supervisor');
    const _dashSupAcad   = document.getElementById('dash-supervisor-academico');
    const _dashAcadSimp  = document.getElementById('dash-acad-simple');
    const _hideAll = () => {
        if (_dashAcad)     _dashAcad.classList.add('hidden');
        if (_dashSupComm)  _dashSupComm.classList.add('hidden');
        if (_dashSupAcad)  _dashSupAcad.classList.add('hidden');
        if (_dashAcadSimp) _dashAcadSimp.classList.add('hidden');
    };

    const _categoria = (document.body.dataset.categoria || '').toLowerCase().trim();
    const _isAcademicoSimples = _categoria === 'acadêmico' || _categoria === 'academico';

    if (typeof isSupervisorComercial === 'function' && isSupervisorComercial()) {
        _hideAll();
        if (_dashSupComm) _dashSupComm.classList.remove('hidden');
        if (typeof loadDashboardSupervisor === 'function') loadDashboardSupervisor();
        return;
    }
    if (typeof isSupervisorAcademico === 'function' && isSupervisorAcademico()) {
        _hideAll();
        if (_dashSupAcad) _dashSupAcad.classList.remove('hidden');
        if (typeof loadDashboardSupervisorAcademico === 'function') loadDashboardSupervisorAcademico();
        return;
    }
    if (_isAcademicoSimples) {
        _hideAll();
        // Acadêmico simples agora vai direto para "Meus Atendimentos" —
        // os atalhos antigos foram removidos da experiência inicial.
        const canMA = (typeof isPageAllowed === 'function') ? isPageAllowed('meus_atendimentos') : true;
        if (canMA && typeof navigate === 'function') {
            if (typeof _dismissBootSplash === 'function') _dismissBootSplash();
            navigate('meus_atendimentos');
            return;
        }
        // Fallback: se por algum motivo não tiver acesso, mantém o painel simples.
        if (_dashAcadSimp) _dashAcadSimp.classList.remove('hidden');
        try {
            const r = await api('/api/me');
            const me = await r.json();
            const nameEl = document.getElementById('dash-acad-simple-name');
            if (nameEl) {
                const nm = (me?.username || '').split('@')[0].split('.')[0];
                nameEl.textContent = nm ? nm.charAt(0).toUpperCase() + nm.slice(1) : '';
            }
        } catch (_) { /* noop */ }
        if (typeof _dismissBootSplash === 'function') _dismissBootSplash();
        return;
    }
    _hideAll();
    if (_dashAcad) _dashAcad.classList.remove('hidden');

    try {
        const res = await api('/api/dashboard');
        const d = await res.json();
        if (d.error) {
            console.warn('Dashboard API error:', d.error);
        }
        const snapInfo = document.getElementById('dash-snap-info');
        const statusEl = document.getElementById('dash-process-status');
        if (snapInfo) {
            if (d.snapshot) {
                snapInfo.textContent = d.snapshot.filename + ' \u2014 ' + d.snapshot.row_count.toLocaleString('pt-BR') + ' registros (' + d.snapshot.uploaded_at + ')';
            } else {
                snapInfo.textContent = 'Nenhum snapshot de matriculados carregado';
                snapInfo.classList.add('text-amber-400');
            }
        }
        if (statusEl) {
            if (d.sync_running) {
                statusEl.innerHTML = '<span class="inline-block w-2.5 h-2.5 rounded-full bg-indigo-400 animate-pulse"></span> Sync...';
            } else if (d.update_running) {
                statusEl.innerHTML = '<span class="inline-block w-2.5 h-2.5 rounded-full bg-amber-400 animate-pulse"></span> Update...';
            } else {
                statusEl.innerHTML = '<span class="green-dot"></span> Conectado';
            }
        }
        if (d.diag) {
            console.info('[Dashboard diag] negocio:', d.diag.negocio_vals, '| nivel:', d.diag.nivel_vals, '| tipo_matricula:', d.diag.tipo_vals);
        }
    } catch (err) {
        console.error('Dashboard load error:', err);
    }
    _dashLoadNewLeadsToday(false);
    _dashRefreshFunnel(false);
    await populateCicloFilter();
    _restoreRgmPadraoFilter();
    _stuBindInteractiveCards();
    await applyDashboardFilters();
    if (typeof _dismissBootSplash === 'function') _dismissBootSplash();
}

function _dashBrtDateIso(daysAgo) {
    const brt = new Date(new Date().toLocaleString('en-US', { timeZone: 'America/Sao_Paulo' }));
    brt.setDate(brt.getDate() - (daysAgo || 0));
    const y = brt.getFullYear();
    const m = String(brt.getMonth() + 1).padStart(2, '0');
    const d = String(brt.getDate()).padStart(2, '0');
    return `${y}-${m}-${d}`;
}

async function _apiJsonSafe(res) {
    if (!res || !res.ok) return null;
    const ct = (res.headers.get('content-type') || '').toLowerCase();
    if (!ct.includes('json')) return null;
    try {
        return await res.json();
    } catch {
        return null;
    }
}

async function _apiJsonBody(res) {
    if (!res) return null;
    const ct = (res.headers.get('content-type') || '').toLowerCase();
    if (!ct.includes('json')) return null;
    try {
        return await res.json();
    } catch {
        return null;
    }
}

function _dashShowFunnelError(msg) {
    const container = document.getElementById('dash-funnel-cards');
    if (!container) return;
    container.innerHTML = `
        <div class="col-span-full text-center py-8 text-slate-500 text-sm flex flex-col items-center gap-3">
            <span>${msg || 'Erro ao carregar funil.'}</span>
            <button type="button" onclick="_dashRefreshFunnel(true)"
                class="text-xs text-cyan-400 hover:text-cyan-300 border border-slate-600 px-3 py-1.5 rounded-lg">
                Tentar novamente
            </button>
        </div>`;
}

async function _dashFallbackYesterdayCommercial(yStr) {
    try {
        const res = await api(`/api/comercial-rgm/data/kpis?dt_ini=${encodeURIComponent(yStr)}&dt_fim=${encodeURIComponent(yStr)}`);
        const d = await _apiJsonSafe(res);
        if (!d || !d.ok) return null;
        const row = (d.evolucao || []).find(e => e.data === yStr);
        return row ? row.count : (d.vendas_liquidas || 0);
    } catch (e) {
        console.warn('fallback yesterday vendas:', e);
        return null;
    }
}

async function _dashLoadNewLeadsToday(force) {
    const el = document.getElementById('dash-funnel-new');
    if (!el) return;
    const q = force ? '?force=1' : '';
    try {
        const res = await api('/api/kommo/new-leads-today' + q);
        const d = await _apiJsonSafe(res);
        if (!d?.ok || d.count == null) return;
        el.textContent = Number(d.count).toLocaleString('pt-BR');
        const convEl = document.getElementById('dash-funnel-conversao');
        const aceiteEl = document.getElementById('dash-funnel-aceite');
        if (convEl && aceiteEl) {
            const aceite = parseInt(String(aceiteEl.textContent).replace(/\./g, ''), 10) || 0;
            const pct = d.count > 0 ? ((aceite / d.count) * 100).toFixed(1) : '0.0';
            convEl.textContent = pct + '%';
        }
    } catch (e) {
        console.warn('new-leads-today:', e);
    }
}

async function _dashLoadYesterdayKpi(force) {
    const q = force ? '?force=1' : '';
    const yStr = _dashBrtDateIso(1);

    for (const path of ['/api/kommo/yesterday-summary', '/api/dashboard/funnel-yesterday']) {
        try {
            const res = await api(path + q);
            const y = await _apiJsonSafe(res);
            if (y?.ok && y.data) {
                let ys = y.data;
                if (!ys.vendas) {
                    const vendas = await _dashFallbackYesterdayCommercial(ys.date || yStr);
                    if (vendas != null) ys = { ...ys, vendas };
                }
                if (typeof _renderYesterdaySummary === 'function') {
                    _renderYesterdaySummary(ys, 'dash-funnel');
                }
                return;
            }
        } catch (e) {
            console.warn('yesterday endpoint', path, e);
        }
    }

    const vendas = await _dashFallbackYesterdayCommercial(yStr);

    if (typeof _renderYesterdaySummary === 'function') {
        _renderYesterdaySummary({
            date: yStr,
            vendas: vendas || 0,
            leads: 0,
            leads_prev: 0,
            leads_delta_pct: 0,
        }, 'dash-funnel');
    }
}

async function _dashRefreshFunnel(force) {
    const btn = document.getElementById('dash-funnel-refresh-btn');
    if (btn) { btn.disabled = true; btn.style.opacity = '0.5'; }
    const q = force ? '?force=1' : '';
    _dashLoadNewLeadsToday(force);
    const slowTimer = setTimeout(() => {
        const container = document.getElementById('dash-funnel-cards');
        if (!container) return;
        const stillLoading = container.textContent.includes('Buscando dados do Kommo');
        if (stillLoading) {
            container.innerHTML = `
                <div class="col-span-full text-center py-8 text-slate-500 text-sm flex flex-col items-center gap-3">
                    <svg class="animate-spin h-6 w-6 text-primary" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"/><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"/></svg>
                    Buscando dados do Kommo… (pode levar até 40s)
                </div>`;
        }
    }, 8000);
    try {
        const ctrl = typeof AbortController !== 'undefined' ? new AbortController() : null;
        const timer = ctrl ? setTimeout(() => ctrl.abort(), 120000) : null;
        const res = await api('/api/kommo/funnel-live' + q, ctrl ? { signal: ctrl.signal } : {});
        if (timer) clearTimeout(timer);
        const d = await _apiJsonBody(res);
        if (d?.ok && typeof _renderFunnelCards === 'function') {
            _renderFunnelCards(d.data, 'dash-funnel');
            if (d.data?.yesterday_summary && typeof _renderYesterdaySummary === 'function') {
                _renderYesterdaySummary(d.data.yesterday_summary, 'dash-funnel');
            }
        } else if (d?.ok && typeof _renderFunnelCards !== 'function') {
            console.error('dash funnel-live: _renderFunnelCards ausente — recarregue a página');
            _dashShowFunnelError('Erro de carregamento do JS. Recarregue a página (Ctrl+Shift+R).');
        } else {
            const errMsg = d?.error
                || (res && !res.ok ? `HTTP ${res.status}` : null)
                || 'Não foi possível carregar o funil.';
            console.error('dash funnel-live error:', errMsg, d);
            _dashShowFunnelError(errMsg);
        }
    } catch (e) {
        console.error('dash funnel-live error:', e);
        _dashShowFunnelError(e.name === 'AbortError'
            ? 'Tempo esgotado ao buscar o funil. Clique em Tentar novamente.'
            : 'Erro de rede ao carregar o funil.');
    } finally {
        clearTimeout(slowTimer);
        if (btn) { btn.disabled = false; btn.style.opacity = '1'; }
    }
    _dashLoadYesterdayKpi(force);
}

// ---------------------------------------------------------------------------
// Timeline Charts (drill-down)
// ---------------------------------------------------------------------------
const _tlCharts = {};
let _tlGranularity = 'month';
let _tlDrillMonth = null;

const _tlColors = {
    novos:       { line: '#3b82f6', bg: 'rgba(59,130,246,0.06)' },
    rematricula: { line: '#10b981', bg: 'rgba(16,185,129,0.06)' },
    regresso:    { line: '#f59e0b', bg: 'rgba(245,158,11,0.06)' },
    recompra:    { line: '#06b6d4', bg: 'rgba(6,182,212,0.06)' },
    total:       { line: '#2563eb', bg: 'rgba(37,99,235,0.08)' },
    calouros_agg:{ line: '#3b82f6', bg: 'rgba(59,130,246,0.06)' },
};
let _tlMode = 'agregado';
let _tlLastSeries = {};
let _tlFetchGen = 0;
let _dashGrandTotal = null;
/** Filtros globais da página — fonte única para timeline e demais blocos. */
const _DASH_RGM_PADRAO_KEY = 'dash_rgm_padrao_v1';
const _RGM_PADRAO_LABELS = {
    todos: 'Todos os RGMs',
    padrao: 'Excluir fora do padrão',
    fora_padrao: 'Somente fora do padrão',
};
let _dashActiveFilters = { ciclo: '', nivel: '', dtFrom: '', dtTo: '', situacao: '', tipo: '', polo: '', rgmPadrao: 'todos' };

function _restoreRgmPadraoFilter() {
    const rgmSel = document.getElementById('students-rgm-padrao');
    if (!rgmSel) return;
    const saved = localStorage.getItem(_DASH_RGM_PADRAO_KEY);
    if (saved && rgmSel.querySelector(`option[value="${saved}"]`)) {
        rgmSel.value = saved;
    }
}

function _readDashboardFilters() {
    const rgmSel = document.getElementById('students-rgm-padrao');
    const rgmPadrao = rgmSel?.value || 'todos';
    return {
        ciclo: document.getElementById('students-ciclo')?.value || '',
        nivel: document.getElementById('students-nivel')?.value || '',
        dtFrom: document.getElementById('students-from')?.value || '',
        dtTo: document.getElementById('students-to')?.value || '',
        situacao: _stuActiveSituacao || '',
        tipo: _stuActiveTipo || '',
        polo: _stuActivePolo || '',
        rgmPadrao: rgmPadrao === 'padrao' || rgmPadrao === 'fora_padrao' ? rgmPadrao : 'todos',
    };
}

function _appendStudentFilterParams(params) {
    const f = _readDashboardFilters();
    if (f.ciclo) params.set('ciclo', f.ciclo);
    if (f.dtFrom) params.set('from', f.dtFrom);
    if (f.dtTo) params.set('to', f.dtTo);
    if (f.nivel) params.set('nivel', f.nivel);
    if (f.situacao) params.set('situacao', f.situacao);
    if (f.tipo) params.set('tipo', f.tipo);
    if (f.polo) params.set('polo', f.polo);
    if (f.rgmPadrao !== 'todos') params.set('rgm_padrao', f.rgmPadrao);
    if (location.hostname === 'localhost' || location.hostname === '127.0.0.1') {
        console.debug('[Dashboard] GET /api/dashboard/students?' + params.toString());
    }
    return f;
}

function toggleTlMode() {
    _tlMode = _tlMode === 'agregado' ? 'detalhado' : 'agregado';
    document.getElementById('tl-mode-btn').textContent = _tlMode === 'agregado' ? 'Ver Detalhado' : 'Ver Agregado';
    _renderGeralChart();
}

function _buildChartOpts() {
    return {
        responsive: true, maintainAspectRatio: false, animation: { duration: 400 },
        interaction: { mode: 'index', intersect: false },
        onClick: (evt, elements) => { if (elements.length && _tlGranularity === 'month') timelineDrillDown(elements[0].index); },
        plugins: {
            legend: { display: false },
            tooltip: {
                backgroundColor: 'rgba(15,23,42,0.95)', borderColor: 'rgba(100,116,139,0.3)', borderWidth: 1,
                titleFont: { family: 'Inter', size: 11 }, bodyFont: { family: 'JetBrains Mono', size: 12 },
                callbacks: { label: c => c.dataset.label + ': ' + c.parsed.y.toLocaleString('pt-BR') },
            },
        },
        scales: {
            x: { grid: { color: 'rgba(100,116,139,0.08)' }, ticks: { color: '#64748b', font: { size: 10, family: 'Inter' }, maxRotation: 0 } },
            y: { grid: { color: 'rgba(100,116,139,0.08)' }, ticks: { color: '#64748b', font: { size: 10, family: 'JetBrains Mono' },
                callback: v => v >= 1000 ? (v/1000).toFixed(v%1000?1:0)+'k' : v } },
        },
    };
}

function _dsCfg(color, label) {
    return {
        label, data: [], borderColor: color.line, backgroundColor: color.bg,
        borderWidth: 2, pointRadius: 3, pointHoverRadius: 6, pointBackgroundColor: color.line,
        fill: false, tension: 0.35,
    };
}

function _renderGeralChart() {
    const s = _tlLastSeries;
    const labels = window._tlGeralLabels || [];
    const rematLabel = document.getElementById('tl-remat-label')?.textContent || 'Rematrículas';

    if (_tlCharts['chart-geral']) { _tlCharts['chart-geral'].destroy(); delete _tlCharts['chart-geral']; }
    const ctx = document.getElementById('chart-geral');
    if (!ctx) return;

    let datasets;
    if (_tlMode === 'agregado') {
        const novos = s.novos || [];
        const regresso = s.regresso || [];
        const recompra = s.recompra || [];
        const calouros = novos.map((v, i) => (v || 0) + (regresso[i] || 0) + (recompra[i] || 0));
        datasets = [
            { ..._dsCfg(_tlColors.calouros_agg, 'Calouros (Novos+Regresso+Recompra)'), data: calouros },
            { ..._dsCfg(_tlColors.rematricula, rematLabel), data: s.rematricula || [] },
            { ..._dsCfg(_tlColors.total, 'Total'), data: s.total || [] },
        ];
    } else {
        datasets = [
            { ..._dsCfg(_tlColors.novos, 'Novos'), data: s.novos || [] },
            { ..._dsCfg(_tlColors.rematricula, rematLabel), data: s.rematricula || [] },
            { ..._dsCfg(_tlColors.regresso, 'Regresso'), data: s.regresso || [] },
            { ..._dsCfg(_tlColors.recompra, 'Recompra'), data: s.recompra || [] },
            { ..._dsCfg(_tlColors.total, 'Total'), data: s.total || [] },
        ];
    }

    const chart = new Chart(ctx, { type: 'line', data: { labels, datasets }, options: _buildChartOpts() });
    _tlCharts['chart-geral'] = chart;
}

function _formatLabel(period, gran) {
    if (gran === 'month') {
        const [y, m] = period.split('-');
        const months = ['jan','fev','mar','abr','mai','jun','jul','ago','set','out','nov','dez'];
        return months[parseInt(m)-1] + ' ' + y;
    }
    const [y, m, d] = period.split('-');
    return parseInt(d) + '/' + parseInt(m);
}

async function loadTimeline(from, to, opts = {}) {
    const gen = ++_tlFetchGen;
    const f = {
        ciclo: opts.ciclo ?? _dashActiveFilters.ciclo ?? '',
        nivel: opts.nivel ?? _dashActiveFilters.nivel ?? '',
        dtFrom: opts.dtFrom ?? _dashActiveFilters.dtFrom ?? '',
        dtTo: opts.dtTo ?? _dashActiveFilters.dtTo ?? '',
        situacao: opts.situacao ?? _dashActiveFilters.situacao ?? '',
        tipo: opts.tipo ?? _dashActiveFilters.tipo ?? '',
        polo: opts.polo ?? _dashActiveFilters.polo ?? '',
    };
    const params = new URLSearchParams({ granularity: _tlGranularity });
    if (f.nivel) params.set('nivel', f.nivel);
    if (f.ciclo) params.set('ciclo', f.ciclo);
    if (f.situacao) params.set('situacao', f.situacao);
    if (f.tipo) params.set('tipo', f.tipo);
    if (f.polo) params.set('polo', f.polo);
    if (from) params.set('from', from);
    else if (f.dtFrom) params.set('from', f.dtFrom);
    if (to) params.set('to', to);
    else if (f.dtTo) params.set('to', f.dtTo);

    const url = '/api/dashboard/timeline?' + params.toString();

    try {
        const res = await api(url);
        const d = await res.json();
        if (gen !== _tlFetchGen) return;
        if (d.error) {
            console.warn('[Timeline] API error:', d.error, url);
            return;
        }

        const labels = (d.periods || []).map(p => _formatLabel(p, _tlGranularity));
        const rawPeriods = d.periods || [];
        const s = d.series || {};
        const fmt = n => (n||0).toLocaleString('pt-BR');

        const isPosOnly = f.nivel === 'Pós-Graduação';
        const rematLbl = document.getElementById('tl-remat-label');
        if (rematLbl) rematLbl.textContent = isPosOnly ? 'Veteranos' : 'Rematrículas';

        _tlLastSeries = s;
        window._tlGeralLabels = labels;

        const sum = arr => (arr || []).reduce((a,b) => a+b, 0);
        const novosSum = sum(s.novos);
        const regressoSum = sum(s.regresso);
        const recompraSum = sum(s.recompra);
        const rematSum = sum(s.rematricula);

        if (_tlMode === 'agregado') {
            document.getElementById('tl-novos-label').textContent = 'Calouros (N+Rg+Rc)';
            document.getElementById('tl-novos-total').textContent = fmt(novosSum + regressoSum + recompraSum);
            document.getElementById('tl-leg-regresso').classList.add('hidden');
            document.getElementById('tl-leg-recompra').classList.add('hidden');
        } else {
            document.getElementById('tl-novos-label').textContent = 'Novos';
            document.getElementById('tl-novos-total').textContent = fmt(novosSum);
            document.getElementById('tl-leg-regresso').classList.remove('hidden');
            document.getElementById('tl-leg-recompra').classList.remove('hidden');
        }
        document.getElementById('tl-remat-total').textContent = fmt(rematSum);
        document.getElementById('tl-regresso-total').textContent = fmt(regressoSum);
        document.getElementById('tl-recompra-total').textContent = fmt(recompraSum);
        document.getElementById('tl-total-total').textContent = fmt(sum(s.total));

        _renderGeralChart();

        const rangeTxt = d.range ? d.range.from + ' → ' + d.range.to : '';
        const filterParts = [];
        if (f.ciclo) filterParts.push('Ciclo ' + f.ciclo);
        if (f.situacao) filterParts.push(f.situacao);
        if (f.tipo) filterParts.push(_TIPO_LABELS[f.tipo] || f.tipo);
        if (f.polo) filterParts.push(f.polo);
        if (f.nivel) filterParts.push(f.nivel);
        document.getElementById('tl-period-label').textContent =
            _tlGranularity === 'day' && _tlDrillMonth
                ? _tlDrillMonth
                : [filterParts.join(' · '), rangeTxt].filter(Boolean).join(' · ');

        document.getElementById('tl-drillup').classList.toggle('hidden', _tlGranularity !== 'day');

        window._tlRawPeriods = rawPeriods;

        const tlTotal = (s.total || []).reduce((a, b) => a + (Number(b) || 0), 0);
        const metaCiclo = d.meta?.ciclo || null;
        if (f.ciclo && metaCiclo !== f.ciclo) {
            console.warn('[Timeline] ciclo não aplicado no servidor — reinicie o backend. pedido=', f.ciclo, 'resposta=', metaCiclo, url);
        } else if (
            f.ciclo &&
            _dashGrandTotal != null &&
            !f.tipo &&
            !f.situacao &&
            !f.polo &&
            Math.abs(tlTotal - _dashGrandTotal) > 1
        ) {
            console.warn('[Timeline] total diverge dos KPIs:', tlTotal, 'vs', _dashGrandTotal, url);
        }
    } catch (e) { console.error('Timeline error:', e); }
}

function timelineDrillDown(index) {
    const period = window._tlRawPeriods?.[index];
    if (!period || _tlGranularity !== 'month') return;
    const [y, m] = period.split('-');
    const from = `${y}-${m}-01`;
    const lastDay = new Date(parseInt(y), parseInt(m), 0).getDate();
    const to = `${y}-${m}-${String(lastDay).padStart(2,'0')}`;
    _tlGranularity = 'day';
    _tlDrillMonth = period;
    loadTimeline(from, to);
}

function timelineDrillUp() {
    _tlGranularity = 'month';
    _tlDrillMonth = null;
    loadTimeline();
}

// ---------------------------------------------------------------------------
// Ciclo Master Panel
// ---------------------------------------------------------------------------
let _cicloMasterData = null;

async function loadCicloMaster() {
    const loading = document.getElementById('ciclo-master-loading');
    const empty = document.getElementById('ciclo-master-empty');
    const content = document.getElementById('ciclo-master-content');
    if (!loading) return;
    loading.classList.remove('hidden');
    empty.classList.add('hidden');
    content.classList.add('hidden');

    try {
        const _nivelParam = document.getElementById('students-nivel')?.value || '';
        const qs = new URLSearchParams();
        if (_nivelParam) qs.set('nivel', _nivelParam);
        const res = await api('/api/dashboard/ciclos' + (qs.toString() ? '?' + qs : ''));
        const d = await res.json();
        if (d.error) { loading.textContent = 'Erro: ' + d.error; return; }

        _cicloMasterData = d;
        loading.classList.add('hidden');

        if (!(d.ciclos || []).length && !(d.comparisons)) {
            empty.classList.remove('hidden');
            return;
        }

        content.classList.remove('hidden');
        renderCicloMaster(d);
    } catch (e) {
        loading.textContent = 'Erro ao carregar ciclos.';
        console.error(e);
    }
}

function renderCicloMaster(data) {
    const fmt = n => (n || 0).toLocaleString('pt-BR');
    const pct = (cur, prev) => {
        if (!prev && !cur) return { txt: '—', cls: 'text-slate-600' };
        if (!prev) return { txt: '+100%', cls: 'text-emerald-400' };
        const d = ((cur - prev) / prev * 100);
        return { txt: (d >= 0 ? '+' : '') + d.toFixed(1) + '%', cls: d > 0 ? 'text-emerald-400' : d < 0 ? 'text-rose-400' : 'text-slate-400' };
    };

    const nivelFilter = document.getElementById('ciclo-filter-nivel').value;
    const isPosOnly = nivelFilter === 'Pós-Graduação';
    const rematLabel = isPosOnly ? 'Veteranos' : 'Rematr.';
    const rematLabelFull = isPosOnly ? 'Veteranos' : 'Rematrículas';

    const cmp = data.comparisons || {};
    const ytd = cmp.ytd?.current || {grand_total:0, totals:{}};
    const ytdP = cmp.ytd_prev?.current || {grand_total:0, totals:{}};
    const m6 = cmp.m6?.current || {grand_total:0, totals:{}};
    const m6P = cmp.m6_prev?.current || {grand_total:0, totals:{}};

    const ytdChg = pct(ytd.grand_total, ytdP.grand_total);
    const m6Chg = pct(m6.grand_total, m6P.grand_total);

    function temporalCard(label, period, cur, prev, accent, bgFrom, bgTo, borderC) {
        const total = cur.grand_total || 0;
        const prevTotal = prev.grand_total || 0;
        const ch = pct(total, prevTotal);
        const t = cur.totals || {};
        return `<div class="glass-card p-4">
            <div class="flex items-center justify-between mb-1">
                <span class="text-[10px] font-bold text-${accent} dark:text-${accent} uppercase tracking-wider">${label}</span>
                <span class="text-[10px] font-bold ${ch.cls} bg-slate-50 dark:bg-slate-800/40 px-1.5 py-0.5 rounded-full">${ch.txt}</span>
            </div>
            <p class="text-[9px] text-slate-400 mb-1.5">${period}</p>
            <p class="text-xl font-black text-slate-900 dark:text-white font-display mb-1.5">${fmt(total)}</p>
            <div class="grid grid-cols-2 gap-x-2 gap-y-0.5 text-[10px]">
                <div class="flex justify-between"><span class="text-slate-500">Novos</span><span class="text-slate-700 dark:text-slate-300 font-medium">${fmt(t.novos||0)}</span></div>
                <div class="flex justify-between"><span class="text-slate-500">${rematLabel}</span><span class="text-slate-700 dark:text-slate-300 font-medium">${fmt(t.rematricula||0)}</span></div>
                <div class="flex justify-between"><span class="text-slate-500">Regresso</span><span class="text-slate-700 dark:text-slate-300 font-medium">${fmt(t.regresso||0)}</span></div>
                <div class="flex justify-between"><span class="text-slate-500">Recompra</span><span class="text-slate-700 dark:text-slate-300 font-medium">${fmt(t.recompra||0)}</span></div>
            </div>
            <div class="mt-1.5 pt-1.5 border-t border-slate-100 dark:border-slate-700/20 text-[9px] text-slate-400">vs anterior: <span class="text-slate-600 dark:text-slate-400 font-medium">${fmt(prevTotal)}</span></div>
        </div>`;
    }

    document.getElementById('ciclo-temporal-cards').innerHTML =
        temporalCard(cmp.ytd?.label||'YTD', cmp.ytd?.period||'', ytd, ytdP, 'indigo-400', 'indigo-500', 'blue-500', 'indigo-500') +
        temporalCard(cmp.ytd_prev?.label||'YTD Ant.', cmp.ytd_prev?.period||'', ytdP, {grand_total:0,totals:{}}, 'slate-400', 'slate-500', 'slate-600', 'slate-600') +
        temporalCard(cmp.m6?.label||'6 meses', cmp.m6?.period||'', m6, m6P, 'cyan-400', 'cyan-500', 'teal-500', 'cyan-500') +
        temporalCard(cmp.m6_prev?.label||'6m Ant.', cmp.m6_prev?.period||'', m6P, {grand_total:0,totals:{}}, 'slate-400', 'slate-500', 'slate-600', 'slate-600');

    // --- Collapsible cycle cards ---
    const filtered = data.ciclos || [];
    const maxTotal = Math.max(...filtered.map(c => c.grand_total), 1);

    const colors = ['cyan', 'violet', 'amber', 'emerald', 'rose', 'indigo'];

    document.getElementById('ciclo-cards').innerHTML = filtered.map((c, i) => {
        const color = colors[i % colors.length];
        const prev = filtered[i + 1];
        const chg = prev ? pct(c.grand_total, prev.grand_total) : null;
        const barW = Math.round(c.grand_total / maxTotal * 100);
        const id = 'ciclo-expand-' + i;
        const t = c.totals || {};
        const sits = Object.entries(c.by_situacao || {}).slice(0, 6);
        const polos = Object.entries(typeof mergePoloBreakdown === 'function' ? mergePoloBreakdown(c.by_polo || {}) : (c.by_polo || {})).slice(0, 8);

        const cardIsPos = (c.nivel || '').includes('Pós');
        const cardRematShort = cardIsPos ? 'Veteranos' : 'Rematr.';
        const cardRematFull  = cardIsPos ? 'Veteranos' : 'Rematrículas';

        return `<div class="glass-card overflow-hidden">
            <button onclick="document.getElementById('${id}').classList.toggle('hidden')" class="w-full px-5 py-4 flex items-center justify-between hover:bg-slate-50 dark:hover:bg-slate-800/30 transition-all">
                <div class="flex items-center gap-4 min-w-0">
                    <div class="flex items-center gap-2">
                        <span class="text-xs font-bold text-${color}-600 dark:text-${color}-400 uppercase tracking-wider">${esc(c.nome)}</span>
                        <span class="text-[10px] text-slate-500">${esc(c.nivel)}</span>
                    </div>
                    <div class="flex items-center gap-3 text-[11px]">
                        <span class="text-slate-500">Novos <span class="text-slate-700 dark:text-slate-300 font-medium">${fmt(t.novos||0)}</span></span>
                        <span class="text-slate-500">${cardRematShort} <span class="text-slate-700 dark:text-slate-300 font-medium">${fmt(t.rematricula||0)}</span></span>
                        <span class="text-slate-500">Regresso <span class="text-slate-700 dark:text-slate-300 font-medium">${fmt(t.regresso||0)}</span></span>
                        <span class="text-slate-500">Recompra <span class="text-slate-700 dark:text-slate-300 font-medium">${fmt(t.recompra||0)}</span></span>
                    </div>
                </div>
                <div class="flex items-center gap-3 flex-shrink-0">
                    <span class="text-lg font-bold text-slate-900 dark:text-white font-display">${fmt(c.grand_total)}</span>
                    ${chg ? `<span class="text-[10px] font-bold ${chg.cls}">${chg.txt}</span>` : ''}
                    <span class="material-symbols-outlined text-base text-slate-400">expand_more</span>
                </div>
            </button>
            <div class="relative progress-bar-bg !rounded-none !h-0.5"><div class="progress-bar-fill bg-${color}-500 !rounded-none" style="width:${barW}%"></div></div>
            <div id="${id}" class="hidden px-5 py-4 bg-slate-50 dark:bg-slate-800/20">
                <div class="grid grid-cols-2 lg:grid-cols-3 gap-4">
                    <div>
                        <p class="text-[10px] font-bold text-slate-500 uppercase tracking-wider mb-2">Por Tipo</p>
                        <div class="space-y-1 text-[12px]">
                            <div class="flex justify-between"><span class="text-slate-500 dark:text-slate-400">Novos (Calouros)</span><span class="text-slate-900 dark:text-white font-mono">${fmt(t.novos||0)}</span></div>
                            <div class="flex justify-between"><span class="text-slate-500 dark:text-slate-400">${cardRematFull}</span><span class="text-slate-900 dark:text-white font-mono">${fmt(t.rematricula||0)}</span></div>
                            <div class="flex justify-between"><span class="text-slate-500 dark:text-slate-400">Regresso</span><span class="text-slate-900 dark:text-white font-mono">${fmt(t.regresso||0)}</span></div>
                            <div class="flex justify-between"><span class="text-slate-500 dark:text-slate-400">Recompra</span><span class="text-slate-900 dark:text-white font-mono">${fmt(t.recompra||0)}</span></div>
                            <div class="flex justify-between border-t border-slate-200 dark:border-slate-700/30 pt-1 mt-1"><span class="text-slate-900 dark:text-white font-bold">Total</span><span class="text-slate-900 dark:text-white font-mono font-bold">${fmt(c.grand_total)}</span></div>
                        </div>
                    </div>
                    <div>
                        <p class="text-[10px] font-bold text-slate-500 uppercase tracking-wider mb-2">Por Situação</p>
                        <div class="space-y-1 text-[12px]">${sits.map(([k,v]) => {
                            const sp = c.grand_total ? Math.round(v/c.grand_total*100) : 0;
                            return `<div class="flex items-center gap-2"><span class="text-slate-500 dark:text-slate-400 flex-1 truncate">${esc(k)}</span><span class="text-slate-900 dark:text-white font-mono">${fmt(v)}</span><span class="text-slate-400 dark:text-slate-600 text-[10px] w-8 text-right">${sp}%</span></div>`;
                        }).join('')}</div>
                    </div>
                    <div>
                        <p class="text-[10px] font-bold text-slate-500 uppercase tracking-wider mb-2">Top Polos</p>
                        <div class="space-y-1 text-[12px]">${polos.map(([k,v]) => {
                            const pp = c.grand_total ? Math.round(v/c.grand_total*100) : 0;
                            return `<div class="flex items-center gap-2"><span class="text-slate-500 dark:text-slate-400 flex-1 truncate">${esc(k)}</span><span class="text-slate-900 dark:text-white font-mono">${fmt(v)}</span><span class="text-slate-400 dark:text-slate-600 text-[10px] w-8 text-right">${pp}%</span></div>`;
                        }).join('')}</div>
                    </div>
                </div>
            </div>
        </div>`;
    }).join('');

    const diag = document.getElementById('ciclo-diag');
    if (diag && data.distinct_nivels) {
        const dn = data.distinct_nivels;
        const cfgNivels = [...new Set((data.config || []).map(c => c.nivel))];
        const missing = Object.keys(dn).filter(n => !cfgNivels.includes(n));
        if (missing.length) {
            diag.innerHTML = missing.map(n =>
                `<span class="text-amber-500">⚠ Existem ${dn[n].toLocaleString('pt-BR')} negócios com nível "${n}" mas nenhum ciclo configurado para esse nível.</span>`
            ).join('<br>');
        } else {
            diag.innerHTML = '';
        }
    }
}

const _DASH_CICLO_KEY = 'dash_ciclo_v2';

function _syncDashboardFilterUi() {
    const f = _readDashboardFilters();
    const tlBadge = document.getElementById('tl-filter-badge');
    if (tlBadge) {
        const parts = [];
        if (f.ciclo) parts.push('Ciclo ' + f.ciclo);
        if (f.situacao) parts.push(f.situacao);
        if (f.tipo) parts.push(_TIPO_LABELS[f.tipo] || f.tipo);
        if (f.polo) parts.push(f.polo);
        if (f.nivel) parts.push(f.nivel);
        if (f.rgmPadrao && f.rgmPadrao !== 'todos') {
            parts.push(_RGM_PADRAO_LABELS[f.rgmPadrao] || f.rgmPadrao);
        }
        if (parts.length) {
            tlBadge.textContent = parts.join(' · ');
            tlBadge.classList.remove('hidden');
        } else {
            tlBadge.classList.add('hidden');
        }
    }
    const cmNivel = document.getElementById('ciclo-filter-nivel');
    if (cmNivel) cmNivel.value = f.nivel;
}

/** Recarrega todas as seções do Dashboard Acadêmico com os filtros do topo. */
async function applyDashboardFilters() {
    const cicloSel = document.getElementById('students-ciclo');
    if (cicloSel) {
        if (cicloSel.value) localStorage.setItem(_DASH_CICLO_KEY, cicloSel.value);
        else localStorage.removeItem(_DASH_CICLO_KEY);
    }
    const sitSel = document.getElementById('students-situacao');
    if (sitSel) _stuActiveSituacao = sitSel.value || null;
    if (_stuActiveSituacao) _syncSitSelectFromActive();
    const rgmSel = document.getElementById('students-rgm-padrao');
    if (rgmSel) {
        localStorage.setItem(_DASH_RGM_PADRAO_KEY, rgmSel.value || 'todos');
    }
    _dashActiveFilters = _readDashboardFilters();
    _tlGranularity = 'month';
    _tlDrillMonth = null;
    document.getElementById('tl-drillup')?.classList.add('hidden');
    _syncDashboardFilterUi();
    await _stuRefreshFiltered();
    await loadCicloMaster();
}

function applyCicloFilter() {
    applyDashboardFilters();
}

async function populateCicloFilter() {
    try {
        if (localStorage.getItem(_DASH_CICLO_KEY) === '') {
            localStorage.removeItem(_DASH_CICLO_KEY);
        }
        const res = await api('/api/dashboard/ciclos-distinct');
        const list = await res.json();
        const arr = Array.isArray(list) ? list : [];
        const sel = document.getElementById('students-ciclo');
        if (!sel) return;
        sel.innerHTML =
            '<option value="">Todos os ciclos</option>' +
            arr
                .map((c) => {
                    const n = c.nome;
                    const tot =
                        c.total != null ? ` (${Number(c.total).toLocaleString('pt-BR')})` : '';
                    return `<option value="${esc(n)}">${esc(n)}${tot}</option>`;
                })
                .join('');
        const names = arr.map((c) => c.nome);
        const mostRecent = arr[0]?.nome || '';
        const saved = localStorage.getItem(_DASH_CICLO_KEY);
        if (saved && names.includes(saved)) {
            sel.value = saved;
        } else {
            sel.value = mostRecent;
        }
    } catch (e) {
        console.error('Erro ao carregar ciclos:', e);
    }
}

// ---------------------------------------------------------------------------
// Filtro ativo por tipo / situação / polo (cards clicáveis)
// ---------------------------------------------------------------------------
let _stuActiveTipo = null;
let _stuActiveSituacao = null;
let _stuActivePolo = null;

const _TIPO_LABELS = {
    novos_agg: 'Novos (Calouros+Regresso+Recompra)',
    novos: 'Calouros',
    rematricula: 'Rematrículas',
    regresso: 'Regresso',
    recompra: 'Recompra',
};

function _normSitKey(s) {
    return String(s || '')
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .trim()
        .toLowerCase();
}

function _syncSitSelectFromActive() {
    const sitSel = document.getElementById('students-situacao');
    if (!sitSel) return;
    if (!_stuActiveSituacao) {
        sitSel.value = '';
        return;
    }
    const want = _normSitKey(_stuActiveSituacao);
    for (const opt of sitSel.options) {
        if (_normSitKey(opt.value) === want) {
            sitSel.value = opt.value;
            return;
        }
    }
    sitSel.value = _stuActiveSituacao;
}

function _stuBindInteractiveCards() {
    const sitEl = document.getElementById('stu-by-situacao');
    if (sitEl && !sitEl.dataset.boundClick) {
        sitEl.dataset.boundClick = '1';
        sitEl.addEventListener('click', (ev) => {
            const card = ev.target.closest('[data-situacao]');
            if (!card) return;
            _stuToggleSituacao(card.getAttribute('data-situacao'));
        });
    }
}

async function _stuRefreshFiltered() {
    _dashActiveFilters = _readDashboardFilters();
    _syncDashboardFilterUi();
    await loadStudentMetrics();
    await loadTimeline(undefined, undefined, _dashActiveFilters);
    await _loadInadimplenciaCard();
}

function _stuToggleTipo(tipo) {
    _stuActiveTipo = _stuActiveTipo === tipo ? null : tipo;
    _stuRefreshFiltered();
}

function _stuToggleSituacao(sit) {
    const next = _normSitKey(_stuActiveSituacao) === _normSitKey(sit) ? null : sit;
    _stuActiveSituacao = next;
    _syncSitSelectFromActive();
    _stuRefreshFiltered();
}

function _stuTogglePolo(polo) {
    _stuActivePolo = _stuActivePolo === polo ? null : polo;
    _stuRefreshFiltered();
}

function _stuToggleNivel(nivel) {
    const nivelSel = document.getElementById('students-nivel');
    if (!nivelSel) return;
    const next = nivelSel.value === nivel ? '' : nivel;
    nivelSel.value = next;
    _stuRefreshFiltered();
}

function _stuClearCrossFilters() {
    _stuActiveTipo = null;
    _stuActiveSituacao = null;
    _stuActivePolo = null;
    const sitSel = document.getElementById('students-situacao');
    if (sitSel) sitSel.value = '';
    _stuRefreshFiltered();
}

function _stuUpdateActiveFilterBar() {
    const bar = document.getElementById('stu-active-filter-bar');
    const text = document.getElementById('stu-active-filter-text');
    if (!bar) return;
    const parts = [];
    if (_stuActiveTipo) parts.push('Tipo: ' + (_TIPO_LABELS[_stuActiveTipo] || _stuActiveTipo));
    if (_stuActiveSituacao) parts.push('Situação: ' + _stuActiveSituacao);
    if (_stuActivePolo) parts.push('Polo: ' + _stuActivePolo);
    const rgmPadrao = document.getElementById('students-rgm-padrao')?.value || 'todos';
    if (rgmPadrao && rgmPadrao !== 'todos') {
        parts.push(_RGM_PADRAO_LABELS[rgmPadrao] || rgmPadrao);
    }
    const dtFrom = document.getElementById('students-from')?.value;
    const dtTo = document.getElementById('students-to')?.value;
    if (dtFrom || dtTo) parts.push(`Período: ${dtFrom || '…'} → ${dtTo || '…'}`);
    const nivel = document.getElementById('students-nivel')?.value;
    if (nivel) parts.push('Nível: ' + nivel);
    if (parts.length) {
        text.textContent = 'Filtrando por: ' + parts.join(' · ');
        bar.classList.remove('hidden');
    } else {
        bar.classList.add('hidden');
    }
}

async function loadStudentMetrics() {
    _dashActiveFilters = _readDashboardFilters();
    const f = _dashActiveFilters;
    const params = new URLSearchParams();
    _appendStudentFilterParams(params);

    const stuContainer = document.getElementById('stu-tipo-cards');
    if (stuContainer) stuContainer.innerHTML = `
        <div class="grid grid-cols-1 md:grid-cols-2 gap-6 mb-4">
            <div class="skeleton skeleton-card p-6"><div class="skeleton skeleton-title"></div><div class="skeleton skeleton-text w-3/4"></div><div class="skeleton" style="height:36px;width:50%;margin-top:12px"></div></div>
            <div class="skeleton skeleton-card p-6"><div class="skeleton skeleton-title"></div><div class="skeleton skeleton-text w-3/4"></div><div class="skeleton" style="height:36px;width:50%;margin-top:12px"></div></div>
        </div>`;

    try {
        const res = await api('/api/dashboard/students?' + params);
        const d = await res.json();
        if (!res.ok || d.error) {
            console.warn('Student metrics error:', d.error || res.status);
            if (stuContainer) {
                stuContainer.innerHTML = '<div class="text-center py-4 text-rose-400 text-sm">Erro ao carregar: ' + esc(d.error || ('HTTP ' + res.status)) + '</div>';
            }
            return;
        }

        const serverRgmFilter = d.active_rgm_padrao ?? d.rgm_padrao?.filter ?? null;
        const rpHint = document.getElementById('stu-rgm-padrao-hint');
        if (f.rgmPadrao !== 'todos' && serverRgmFilter !== f.rgmPadrao) {
            console.warn(
                '[Dashboard] Filtro RGM ignorado pelo servidor. Enviado:',
                f.rgmPadrao,
                '| Resposta:',
                serverRgmFilter,
                '| Reinicie o Flask (scripts/restart-flask.ps1)'
            );
            if (rpHint) {
                rpHint.textContent =
                    'Filtro RGM selecionado, mas o servidor ainda não aplicou. Feche Flask antigos, rode scripts/restart-flask.ps1 e recarregue (Ctrl+F5).';
                rpHint.classList.remove('hidden');
            }
        }

        const fmt = n => (n || 0).toLocaleString('pt-BR');
        const t = d.totals || {};
        const novosAgg = (t.novos || 0) + (t.regresso || 0) + (t.recompra || 0);
        const remat = t.rematricula || 0;

        let gt;
        if (_stuActiveTipo === 'novos_agg') gt = novosAgg;
        else if (_stuActiveTipo === 'rematricula') gt = remat;
        else if (_stuActiveTipo === 'novos') gt = t.novos || 0;
        else if (_stuActiveTipo === 'regresso') gt = t.regresso || 0;
        else if (_stuActiveTipo === 'recompra') gt = t.recompra || 0;
        else gt = d.grand_total ?? Object.values(t).reduce((a, v) => a + (v || 0), 0);
        _dashGrandTotal = gt;

        const stuIsPosOnly = f.nivel === 'Pós-Graduação';
        const stuRematLabel = stuIsPosOnly ? 'Veteranos' : 'Rematrículas';

        const isNovosAgg = _stuActiveTipo === 'novos_agg';
        const isRemat = _stuActiveTipo === 'rematricula';
        const isNovos = _stuActiveTipo === 'novos';
        const isRegresso = _stuActiveTipo === 'regresso';
        const isRecompra = _stuActiveTipo === 'recompra';

        const ringActive = 'ring-2 ring-offset-2 ring-offset-white dark:ring-offset-[#101f22] scale-[1.02]';

        const pctNovos = gt ? Math.round(novosAgg / gt * 100) : 0;
        const pctRemat = gt ? Math.round(remat / gt * 100) : 0;

        stuContainer.innerHTML = `
            <div class="flex items-center justify-end mb-3">
                <div class="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-slate-50 dark:bg-slate-800/40 border border-slate-100 dark:border-slate-700/20">
                    <span class="material-symbols-outlined text-base text-violet-500 dark:text-violet-400">groups</span>
                    <span class="text-[10px] text-slate-500 uppercase tracking-wider font-bold">Total</span>
                    <span class="text-lg font-bold text-slate-900 dark:text-white font-display">${fmt(gt)}</span>
                </div>
            </div>
            <div class="grid grid-cols-1 md:grid-cols-2 gap-6 mb-4">
                <!-- Big Number: Novos -->
                <div class="glass-card p-6 relative overflow-hidden cursor-pointer transition-all hover:shadow-md ${isNovosAgg ? ringActive + ' ring-blue-500' : ''}"
                     onclick="_stuToggleTipo('novos_agg')">
                    <div class="flex items-center justify-between mb-4">
                        <div class="w-12 h-12 bg-blue-50 dark:bg-blue-500/10 rounded-xl flex items-center justify-center">
                            <span class="material-symbols-outlined text-blue-600 dark:text-blue-400">person_add</span>
                        </div>
                        <span class="text-blue-600 dark:text-blue-400 text-xs font-bold bg-blue-50 dark:bg-blue-500/10 px-2 py-1 rounded-full">${pctNovos}%</span>
                    </div>
                    <p class="text-slate-500 text-sm font-medium">Novos</p>
                    <p class="text-[10px] text-slate-400 mb-1">Calouros + Regresso + Recompra</p>
                    <p class="text-3xl font-black text-slate-900 dark:text-white mt-1" data-count="${novosAgg}">0</p>
                    <div class="grid grid-cols-3 gap-2 mt-4">
                        <div class="rounded-lg px-3 py-2 cursor-pointer transition-all ${isNovos ? 'bg-blue-50 dark:bg-indigo-500/20 ring-1 ring-blue-300 dark:ring-indigo-400/50' : 'bg-slate-50 dark:bg-slate-800/40 hover:bg-slate-100 dark:hover:bg-slate-700/40'}"
                             onclick="event.stopPropagation(); _stuToggleTipo('novos')">
                            <p class="text-[9px] text-slate-500 uppercase tracking-wider font-bold">Calouros</p>
                            <p class="text-lg font-bold text-slate-900 dark:text-white font-display" data-count="${t.novos || 0}">0</p>
                        </div>
                        <div class="rounded-lg px-3 py-2 cursor-pointer transition-all ${isRegresso ? 'bg-amber-50 dark:bg-amber-500/20 ring-1 ring-amber-300 dark:ring-amber-400/50' : 'bg-slate-50 dark:bg-slate-800/40 hover:bg-slate-100 dark:hover:bg-slate-700/40'}"
                             onclick="event.stopPropagation(); _stuToggleTipo('regresso')">
                            <p class="text-[9px] text-amber-600 dark:text-amber-400 uppercase tracking-wider font-bold">Regresso</p>
                            <p class="text-lg font-bold text-slate-900 dark:text-white font-display" data-count="${t.regresso || 0}">0</p>
                        </div>
                        <div class="rounded-lg px-3 py-2 cursor-pointer transition-all ${isRecompra ? 'bg-cyan-50 dark:bg-cyan-500/20 ring-1 ring-cyan-300 dark:ring-cyan-400/50' : 'bg-slate-50 dark:bg-slate-800/40 hover:bg-slate-100 dark:hover:bg-slate-700/40'}"
                             onclick="event.stopPropagation(); _stuToggleTipo('recompra')">
                            <p class="text-[9px] text-cyan-600 dark:text-cyan-400 uppercase tracking-wider font-bold">Recompra</p>
                            <p class="text-lg font-bold text-slate-900 dark:text-white font-display" data-count="${t.recompra || 0}">0</p>
                        </div>
                    </div>
                </div>
                <!-- Big Number: Rematrículas -->
                <div class="glass-card p-6 relative overflow-hidden cursor-pointer transition-all hover:shadow-md ${isRemat ? ringActive + ' ring-emerald-500' : ''}"
                     onclick="_stuToggleTipo('rematricula')">
                    <div class="flex items-center justify-between mb-4">
                        <div class="w-12 h-12 bg-emerald-50 dark:bg-emerald-500/10 rounded-xl flex items-center justify-center">
                            <span class="material-symbols-outlined text-emerald-600 dark:text-emerald-400">autorenew</span>
                        </div>
                        <span class="text-emerald-600 dark:text-emerald-400 text-xs font-bold bg-emerald-50 dark:bg-emerald-500/10 px-2 py-1 rounded-full">${pctRemat}%</span>
                    </div>
                    <p class="text-slate-500 text-sm font-medium">${esc(stuRematLabel)}</p>
                    <p class="text-[10px] text-slate-400 mb-1">Renovações de matrícula</p>
                    <p class="text-3xl font-black text-slate-900 dark:text-white mt-1" data-count="${remat}">0</p>
                </div>
            </div>`;

        countUpAll(stuContainer);
        _renderSituacaoCardsClickable('stu-by-situacao', d.by_situacao);
        renderProportionBars('stu-by-nivel', d.by_nivel);
        renderPoloRankingTable('stu-by-polo', mergePoloBreakdown(d.by_polo));
        renderBreakdown('stu-by-turma', d.by_turma);
        renderBreakdown('stu-by-ciclo', d.by_ciclo);

        _stuUpdateActiveFilterBar();

        const badge = document.getElementById('stu-filter-badge');
        const parts = [];
        if (f.ciclo) parts.push(`Ciclo ${f.ciclo}`);
        if (f.nivel) parts.push(f.nivel);
        if (f.dtFrom || f.dtTo) parts.push(`${f.dtFrom || '…'} → ${f.dtTo || '…'}`);
        if (f.situacao) parts.push(f.situacao);
        if (f.tipo) parts.push(_TIPO_LABELS[f.tipo] || f.tipo);
        if (f.polo) parts.push(f.polo);
        if (f.rgmPadrao && f.rgmPadrao !== 'todos') {
            parts.push(_RGM_PADRAO_LABELS[f.rgmPadrao] || f.rgmPadrao);
        }
        if (parts.length) {
            badge.textContent = parts.join(' · ');
            badge.classList.remove('hidden');
        } else {
            badge.classList.add('hidden');
        }

        const rp = d.rgm_padrao || {};
        if (rpHint && !(f.rgmPadrao !== 'todos' && (d.active_rgm_padrao || 'todos') !== f.rgmPadrao)) {
            if ((rp.fora_padrao_total || 0) > 0 && f.rgmPadrao === 'todos') {
                const pfx = rp.dominant_prefix != null ? ` (padrão: prefixo ${rp.dominant_prefix})` : '';
                rpHint.textContent =
                    `${(rp.fora_padrao_total || 0).toLocaleString('pt-BR')} matrícula(s) EM CURSO fora do padrão RGM${pfx} — ` +
                    'use o filtro RGM para excluir ou ver só estes.';
                rpHint.classList.remove('hidden');
            } else if (f.rgmPadrao === 'padrao' && rp.dominant_prefix != null) {
                rpHint.textContent = `Exibindo só RGMs no padrão (prefixo ${rp.dominant_prefix}+) — alinhado ao Comercial.`;
                rpHint.classList.remove('hidden');
            } else if (f.rgmPadrao === 'fora_padrao') {
                rpHint.textContent = 'Exibindo somente matrículas EM CURSO fora do padrão RGM do período.';
                rpHint.classList.remove('hidden');
            } else {
                rpHint.classList.add('hidden');
            }
        }
    } catch (err) {
        console.error('Student metrics error:', err);
        if (stuContainer) stuContainer.innerHTML = '<div class="text-center py-4 text-rose-400 text-sm">Erro ao carregar métricas</div>';
    }
}

function renderBreakdown(elId, data) {
    const el = document.getElementById(elId);
    if (!data || !Object.keys(data).length) { el.textContent = '—'; return; }
    const total = Object.values(data).reduce((a, b) => a + b, 0);
    el.innerHTML = Object.entries(data).map(([k, v]) => {
        const pct = total ? Math.round(v / total * 100) : 0;
        return `<div class="flex items-center justify-between gap-2">
            <div class="flex items-center gap-2 min-w-0 flex-1">
                <span class="truncate text-slate-700 dark:text-slate-300">${esc(k)}</span>
                <div class="flex-1 progress-bar-bg min-w-[40px] !h-1.5">
                    <div class="progress-bar-fill bg-primary" style="width:${pct}%"></div>
                </div>
            </div>
            <span class="text-xs font-mono text-slate-600 dark:text-slate-400 whitespace-nowrap">${v.toLocaleString('pt-BR')} <span class="text-slate-400 dark:text-slate-600">(${pct}%)</span></span>
        </div>`;
    }).join('');
}

const _sitMeta = {
    'em curso': {
        from: 'emerald-500', to: 'green-500', text: 'emerald', bg: 'emerald', primary: true,
        icon: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 14l9-5-9-5-9 5 9 5z"/><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 14l6.16-3.422a12.083 12.083 0 01.665 6.479A11.952 11.952 0 0012 20.055a11.952 11.952 0 00-6.824-2.998 12.078 12.078 0 01.665-6.479L12 14z"/><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 14l9-5-9-5-9 5 9 5zm0 0l6.16-3.422a12.083 12.083 0 01.665 6.479A11.952 11.952 0 0012 20.055a11.952 11.952 0 00-6.824-2.998 12.078 12.078 0 01.665-6.479L12 14zm-4 6v-7.5l4-2.222"/>',
        desc: 'Alunos ativos cursando',
    },
    'cancelado': {
        from: 'rose-500', to: 'red-600', text: 'rose', bg: 'rose',
        icon: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1"/>',
        desc: 'Evadiram do curso',
    },
    'trancado': {
        from: 'amber-500', to: 'orange-500', text: 'amber', bg: 'amber',
        icon: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 9v6m4-6v6m7-3a9 9 0 11-18 0 9 9 0 0118 0z"/>',
        desc: 'Interromperam o curso',
    },
    'transferido': {
        from: 'violet-500', to: 'purple-600', text: 'violet', bg: 'violet',
        icon: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 7h12m0 0l-4-4m4 4l-4 4m0 6H4m0 0l4 4m-4-4l4-4"/>',
        desc: 'Foram para outro polo',
    },
    '_default': {
        from: 'slate-500', to: 'slate-600', text: 'slate', bg: 'slate',
        icon: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8.228 9c.549-1.165 2.03-2 3.772-2 2.21 0 4 1.343 4 3 0 1.4-1.278 2.575-3.006 2.907-.542.104-.994.54-.994 1.093m0 3h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/>',
        desc: '',
    },
};
const _sitOrder = ['em curso', 'cancelado', 'trancado', 'transferido'];

function _sitLookup(k) { return _sitMeta[k.toLowerCase()] || _sitMeta['_default']; }

function renderSituacaoCards(elId, data) {
    _renderSituacaoCardsClickable(elId, data);
}

const _sitIcons = {
    'em curso': 'school',
    'cancelado': 'cancel',
    'trancado': 'pause_circle',
    'transferido': 'swap_horiz',
    '_default': 'help',
};

function _renderSituacaoCardsClickable(elId, data) {
    const el = document.getElementById(elId);
    if (!data || !Object.keys(data).length) { el.innerHTML = '<span class="text-slate-500 text-sm col-span-4">—</span>'; return; }
    const total = Object.values(data).reduce((a, b) => a + b, 0);
    const keys = Object.keys(data);
    const ordered = _sitOrder
        .map(sk => keys.find(k => k.toLowerCase() === sk))
        .filter(Boolean)
        .concat(keys.filter(k => !_sitOrder.includes(k.toLowerCase())));

    const ringActive = 'ring-2 ring-offset-2 ring-offset-white dark:ring-offset-[#101f22] scale-[1.02]';

    el.innerHTML = ordered.map(k => {
        const v = data[k];
        const pct = total ? Math.round(v / total * 100) : 0;
        const c = _sitLookup(k);
        const icon = _sitIcons[k.toLowerCase()] || _sitIcons['_default'];
        const isActive = _normSitKey(_stuActiveSituacao) === _normSitKey(k);
        const activeRing = isActive ? `${ringActive} ring-${c.text}-500` : '';

        return `<div class="glass-card p-5 relative overflow-hidden cursor-pointer transition-all hover:shadow-md ${activeRing}"
                     data-situacao="${esc(k)}" role="button" tabindex="0">
            <div class="flex items-center justify-between mb-3">
                <div class="w-10 h-10 bg-${c.bg}-50 dark:bg-${c.bg}-500/10 rounded-xl flex items-center justify-center">
                    <span class="material-symbols-outlined text-${c.text}-600 dark:text-${c.text}-400">${icon}</span>
                </div>
                <span class="text-${c.text}-600 dark:text-${c.text}-400 text-xs font-bold bg-${c.bg}-50 dark:bg-${c.bg}-500/10 px-2 py-1 rounded-full">${pct}%</span>
            </div>
            <p class="text-slate-500 text-sm font-medium">${esc(k)}</p>
            <p class="text-2xl font-black text-slate-900 dark:text-white mt-1" data-count="${v}">0</p>
            <div class="w-full progress-bar-bg mt-3 !h-1.5">
                <div class="progress-bar-fill bg-${c.from}" style="width:${Math.min(pct,100)}%"></div>
            </div>
        </div>`;
    }).join('');
    countUpAll(el);
}

function renderPoloRankingTable(elId, byPolo) {
    const tbody = document.getElementById(elId);
    if (!tbody) return;
    const merged = typeof mergePoloBreakdown === 'function' ? mergePoloBreakdown(byPolo) : (byPolo || {});
    const ranking = Object.entries(merged).sort((a, b) => b[1] - a[1]);
    if (!ranking.length) {
        tbody.innerHTML = '<tr><td colspan="4" class="px-5 py-6 text-center text-slate-500">Sem dados</td></tr>';
        return;
    }
    const max = Math.max(...ranking.map(([, v]) => v), 1);
    const ringActive = 'ring-2 ring-inset ring-cyan-500/60 bg-cyan-50/50 dark:bg-cyan-500/10';
    tbody.innerHTML = ranking.map(([nome, total], i) => {
        const isActive = _stuActivePolo === nome;
        return `<tr class="cursor-pointer hover:bg-slate-50 dark:hover:bg-white/[0.02] transition-colors ${isActive ? ringActive : ''}"
                    onclick="_stuTogglePolo(${JSON.stringify(nome)})">
            <td class="text-center px-3 py-2.5 text-slate-500 font-medium text-xs">${i + 1}</td>
            <td class="px-4 py-2.5 text-slate-700 dark:text-slate-300 text-xs font-medium">${esc(nome)}</td>
            <td class="px-4 py-2.5 text-right font-mono text-[#00346f] dark:text-white font-semibold text-xs tabular-nums">${total.toLocaleString('pt-BR')}</td>
            <td class="px-4 py-2.5"><div class="h-3 rounded-full bg-slate-200 dark:bg-slate-800 overflow-hidden"><div class="h-full rounded-full bg-gradient-to-r from-cyan-500 to-blue-500" style="width:${Math.round(total / max * 100)}%"></div></div></td>
        </tr>`;
    }).join('');
}

function renderProportionBars(elId, data) {
    const el = document.getElementById(elId);
    if (!el) return;
    if (!data || !Object.keys(data).length) {
        el.innerHTML = '<p class="px-4 py-6 text-center text-slate-500 text-xs">Sem dados</p>';
        return;
    }
    const entries = Object.entries(data).sort((a, b) => b[1] - a[1]);
    const max = Math.max(...entries.map(([, v]) => v), 1);
    const total = entries.reduce((s, [, v]) => s + v, 0);
    const nivelSel = document.getElementById('students-nivel')?.value || '';
    const ringActive = 'ring-2 ring-inset ring-indigo-500/60 bg-indigo-50/50 dark:bg-indigo-500/10';
    el.innerHTML = entries.map(([k, v]) => {
        const pct = total ? Math.round(v / total * 100) : 0;
        const isNivelFilter = k === 'Graduação' || k === 'Pós-Graduação';
        const isActive = isNivelFilter && nivelSel === k;
        const clickAttr = isNivelFilter
            ? `onclick="_stuToggleNivel(${JSON.stringify(k)})" role="button" tabindex="0"`
            : '';
        return `<div class="px-4 py-3 border-b border-slate-200 dark:border-slate-700/10 last:border-0 transition-colors ${isNivelFilter ? 'cursor-pointer hover:bg-slate-50 dark:hover:bg-white/[0.02]' : ''} ${isActive ? ringActive : ''}"
                 ${clickAttr}>
            <div class="flex items-center justify-between gap-3 mb-2">
                <span class="text-xs font-medium text-slate-700 dark:text-slate-300">${esc(k)}</span>
                <span class="text-xs font-mono font-semibold text-[#00346f] dark:text-white tabular-nums">${v.toLocaleString('pt-BR')} <span class="text-slate-400 font-normal">(${pct}%)</span></span>
            </div>
            <div class="h-3 rounded-full bg-slate-200 dark:bg-slate-800 overflow-hidden">
                <div class="h-full rounded-full bg-gradient-to-r from-cyan-500 to-blue-500" style="width:${Math.round(v / max * 100)}%"></div>
            </div>
        </div>`;
    }).join('');
}

function renderBreakdownBars(elId, data) {
    const el = document.getElementById(elId);
    if (!data || !Object.keys(data).length) { el.textContent = '—'; return; }
    const total = Object.values(data).reduce((a, b) => a + b, 0);
    el.innerHTML = Object.entries(data).map(([k, v]) => {
        const pct = total ? Math.round(v / total * 100) : 0;
        return `<div class="flex items-center justify-between gap-3">
            <div class="flex items-center gap-2 min-w-0 flex-1">
                <span class="truncate text-sm text-slate-700 dark:text-slate-300">${esc(k)}</span>
                <div class="flex-1 progress-bar-bg min-w-[60px] overflow-hidden !h-2">
                    <div class="progress-bar-fill bg-primary" style="width:${pct}%"></div>
                </div>
            </div>
            <span class="text-sm font-mono text-slate-900 dark:text-white font-semibold whitespace-nowrap">${v.toLocaleString('pt-BR')} <span class="text-slate-400 dark:text-slate-500 text-xs">(${pct}%)</span></span>
        </div>`;
    }).join('');
}

function clearStudentFilter() {
    const cicloSel = document.getElementById('students-ciclo');
    if (cicloSel) {
        const recent = Array.from(cicloSel.options).find((o) => o.value);
        cicloSel.value = recent ? recent.value : '';
        localStorage.setItem(_DASH_CICLO_KEY, cicloSel.value || '');
    }
    document.getElementById('students-from').value = '';
    document.getElementById('students-to').value = '';
    document.getElementById('students-nivel').value = '';
    document.getElementById('students-situacao').value = '';
    const rgmSel = document.getElementById('students-rgm-padrao');
    if (rgmSel) {
        rgmSel.value = 'todos';
        localStorage.setItem(_DASH_RGM_PADRAO_KEY, 'todos');
    }
    document.getElementById('stu-filter-badge').classList.add('hidden');
    _stuActiveTipo = null;
    _stuActiveSituacao = null;
    _stuActivePolo = null;
    applyDashboardFilters();
}

// ---------------------------------------------------------------------------
// Saúde Financeira (Lista de Alunos) — cards clicáveis no Dashboard
// ---------------------------------------------------------------------------
let _inadGeneration = 0;

function _inadToggleCard(key) {
    navigate('inadimplencia');
}

function _inadRenderCards() {
    const container = document.getElementById('dash-inad-cards');
    if (!container || !window._inadLatest) return;
    const d = window._inadLatest;
    const fmt = n => (n || 0).toLocaleString('pt-BR');
    const pct = d.pct_inadimplencia || 0;
    const pctAdim = d.total_alunos ? ((d.adimplentes / d.total_alunos) * 100).toFixed(1) : '0';

    container.innerHTML = `
        <div class="glass-card p-5 cursor-pointer transition-all hover:shadow-md"
             onclick="_inadToggleCard('total')">
            <div class="flex items-center justify-between mb-3">
                <div class="w-10 h-10 bg-teal-50 dark:bg-teal-500/10 rounded-xl flex items-center justify-center">
                    <span class="material-symbols-outlined text-teal-600 dark:text-teal-400">group</span>
                </div>
            </div>
            <p class="text-slate-500 text-sm font-medium">Total Alunos</p>
            <p class="text-2xl font-black text-slate-900 dark:text-white mt-1" data-count="${d.total_alunos || 0}">0</p>
        </div>
        <div class="glass-card p-5 cursor-pointer transition-all hover:shadow-md"
             onclick="_inadToggleCard('adim')">
            <div class="flex items-center justify-between mb-3">
                <div class="w-10 h-10 bg-emerald-50 dark:bg-emerald-500/10 rounded-xl flex items-center justify-center">
                    <span class="material-symbols-outlined text-emerald-600 dark:text-emerald-400">check_circle</span>
                </div>
                <span class="text-emerald-600 dark:text-emerald-400 text-xs font-bold bg-emerald-50 dark:bg-emerald-500/10 px-2 py-1 rounded-full">${pctAdim.replace('.', ',')}%</span>
            </div>
            <p class="text-slate-500 text-sm font-medium">Adimplentes</p>
            <p class="text-2xl font-black text-emerald-600 dark:text-emerald-400 mt-1" data-count="${d.adimplentes || 0}">0</p>
        </div>
        <div class="glass-card p-5 cursor-pointer transition-all hover:shadow-md"
             onclick="_inadToggleCard('inadim')">
            <div class="flex items-center justify-between mb-3">
                <div class="w-10 h-10 bg-amber-50 dark:bg-amber-500/10 rounded-xl flex items-center justify-center">
                    <span class="material-symbols-outlined text-amber-600 dark:text-amber-400">warning</span>
                </div>
                <span class="text-amber-600 dark:text-amber-400 text-xs font-bold bg-amber-50 dark:bg-amber-500/10 px-2 py-1 rounded-full">${pct.toFixed(1).replace('.', ',')}%</span>
            </div>
            <p class="text-slate-500 text-sm font-medium">Inadimplentes</p>
            <p class="text-2xl font-black text-amber-600 dark:text-amber-400 mt-1" data-count="${d.inadimplentes || 0}">0</p>
        </div>
        <div class="glass-card p-5 cursor-pointer transition-all hover:shadow-md"
             onclick="_inadToggleCard('pct')">
            <div class="flex items-center justify-between mb-3">
                <div class="w-10 h-10 bg-rose-50 dark:bg-rose-500/10 rounded-xl flex items-center justify-center">
                    <span class="material-symbols-outlined text-rose-600 dark:text-rose-400">percent</span>
                </div>
            </div>
            <p class="text-slate-500 text-sm font-medium">% Inadimplência</p>
            <p class="text-2xl font-black text-slate-900 dark:text-white mt-1">${pct.toFixed(1).replace('.', ',')}%</p>
            <div class="w-full progress-bar-bg mt-3 !h-1.5">
                <div class="progress-bar-fill bg-gradient-to-r from-amber-500 to-rose-500" style="width:${Math.min(pct, 100)}%"></div>
            </div>
        </div>`;

    countUpAll(container);
}

async function _loadInadimplenciaCard() {
    const gen = ++_inadGeneration;

    const section = document.getElementById('dash-inadimplencia-card');
    if (!section) return;

    const cardsEl = document.getElementById('dash-inad-cards');
    if (cardsEl) cardsEl.style.opacity = '0.5';

    const tipo = _stuActiveTipo;
    const situacao = _stuActiveSituacao;
    const polo = _stuActivePolo;
    const nivelEl = document.getElementById('students-nivel');
    const nivel = nivelEl ? nivelEl.value : '';
    const cicloEl = document.getElementById('students-ciclo');
    const ciclo = cicloEl ? cicloEl.value : '';

    try {
        const p = new URLSearchParams();
        if (tipo) p.set('tipo', tipo);
        if (situacao) p.set('situacao', situacao);
        if (polo) p.set('polo', polo);
        if (nivel) p.set('nivel', nivel);
        if (ciclo) p.set('ciclo', ciclo);
        const qs = p.toString();
        const url = '/api/lista-alunos/latest' + (qs ? '?' + qs : '');

        const res = await api(url);

        if (gen !== _inadGeneration) return;

        const d = await res.json();

        if (gen !== _inadGeneration) return;

        if (!d.ok && d.error) {
            if (cardsEl) cardsEl.style.opacity = '1';
            return;
        }
        if (!d.ok || !d.has_data) { section.classList.add('hidden'); return; }

        section.classList.remove('hidden');
        window._inadLatest = d;
        _inadRenderCards();

        const dateEl = document.getElementById('dash-inad-date');
        if (dateEl && d.snapshot) {
            let label = d.snapshot.uploaded_at;
            const filterParts = [];
            if (d.filtered_tipo) {
                const tipoLabels = { novos: 'Calouros', rematricula: 'Rematrículas', regresso: 'Regresso', recompra: 'Recompra', novos_agg: 'Novos (Calouros+Regresso+Recompra)' };
                filterParts.push(tipoLabels[d.filtered_tipo] || d.filtered_tipo);
            }
            if (d.filtered_situacao) filterParts.push(d.filtered_situacao);
            if (d.filtered_nivel) filterParts.push(d.filtered_nivel);
            if (filterParts.length) label += '  ·  Filtro: ' + filterParts.join(' + ');
            dateEl.textContent = label;
        }
    } catch (e) {
        if (cardsEl) cardsEl.style.opacity = '1';
    }
}

// onclick nos cards / botão limpar — expor no escopo global
window._stuToggleSituacao = _stuToggleSituacao;
window._stuToggleTipo = _stuToggleTipo;
window._stuTogglePolo = _stuTogglePolo;
window._stuToggleNivel = _stuToggleNivel;
window._stuClearCrossFilters = _stuClearCrossFilters;
