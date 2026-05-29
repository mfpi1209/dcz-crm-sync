// ===========================================================================
// INADIMPLÊNCIA — Taxa de inadimplência e evolução temporal
// Endpoints: /api/inadimplencia/{list,atual,comparar,evolucao}
// ===========================================================================
(function () {
    'use strict';

    let _evolChart = null;
    let _currentRange = 30;

    // ── Formatação ──────────────────────────────────────────────────────────

    function _fmt(n) {
        if (n == null || n === '') return '—';
        return Number(n).toLocaleString('pt-BR');
    }

    function _fmtPct(n) {
        if (n == null || n === '') return '—';
        return Number(n).toFixed(2).replace('.', ',') + '%';
    }

    function _fmtDate(iso) {
        if (!iso) return '—';
        try {
            // Suporta tanto YYYY-MM-DD quanto ISO completo
            const s = String(iso).slice(0, 10);
            const [y, m, d] = s.split('-');
            return `${d}/${m}/${y}`;
        } catch (_) {
            return iso;
        }
    }

    function _deltaHtml(delta, suffix) {
        if (delta == null) return '<span class="text-slate-500 text-xs">—</span>';
        const sign = delta > 0 ? '+' : '';
        const color = delta < 0
            ? 'text-emerald-500'
            : delta > 0 ? 'text-rose-500' : 'text-slate-500';
        return `<span class="text-xs font-semibold ${color}">${sign}${delta}${suffix} vs 7d atrás</span>`;
    }

    function _fallbackBadge(side) {
        if (!side || !side.is_fallback) return '';
        const req = _fmtDate(side.requested_date);
        const fall = _fmtDate(side.fallback_date);
        const dias = side.dias_diferenca || 0;
        return `<span class="inline-flex items-center gap-1 text-[11px] font-medium bg-amber-500/10 text-amber-600 dark:text-amber-400 border border-amber-500/30 rounded-full px-2 py-0.5 mt-1">
            ⚠ Sem upload em ${req} — exibindo dados de ${fall} (${dias} dia${dias !== 1 ? 's' : ''} antes)
        </span>`;
    }

    // ── KPIs ────────────────────────────────────────────────────────────────

    function _renderKPIs(atual, comp) {
        const taxa = atual ? atual.taxa_pct : null;
        const inad = atual ? atual.inadimplentes : null;
        const emCurso = atual ? atual.em_curso : null;

        const el = id => document.getElementById(id);

        const taxaEl = el('inad-taxa-pct');
        const inadEl = el('inad-total-inad');
        const emCursoEl = el('inad-em-curso');
        if (taxaEl) taxaEl.textContent = _fmtPct(taxa);
        if (inadEl) inadEl.textContent = _fmt(inad);
        if (emCursoEl) emCursoEl.textContent = _fmt(emCurso);

        const taxaDeltaEl = el('inad-taxa-delta');
        const inadDeltaEl = el('inad-inad-delta');

        if (comp && comp.a && comp.b && comp.a.snapshot_id != null) {
            const deltaTaxaVal = comp.delta_taxa_pct != null
                ? Number(comp.delta_taxa_pct).toFixed(2).replace('.', ',') + '%'
                : '—';
            const sign = comp.delta_taxa_pct > 0 ? '+' : '';
            const color = comp.delta_taxa_pct < 0
                ? 'text-emerald-500'
                : comp.delta_taxa_pct > 0 ? 'text-rose-500' : 'text-slate-500';
            if (taxaDeltaEl) taxaDeltaEl.innerHTML =
                `<span class="text-xs font-semibold ${color}">${sign}${deltaTaxaVal} vs 7d</span>`;

            const deltaInadSign = comp.delta_inadimplentes > 0 ? '+' : '';
            const inadColor = comp.delta_inadimplentes < 0
                ? 'text-emerald-500'
                : comp.delta_inadimplentes > 0 ? 'text-rose-500' : 'text-slate-500';
            if (inadDeltaEl) inadDeltaEl.innerHTML =
                `<span class="text-xs font-semibold ${inadColor}">${deltaInadSign}${_fmt(comp.delta_inadimplentes)} vs 7d</span>`;
        } else {
            if (taxaDeltaEl) taxaDeltaEl.innerHTML = '';
            if (inadDeltaEl) inadDeltaEl.innerHTML = '';
        }
    }

    // ── Comparação ──────────────────────────────────────────────────────────

    function _renderComparison(comp) {
        const el = document.getElementById('inad-comp-result');
        if (!el) return;

        if (!comp || (!comp.a && !comp.b)) {
            el.innerHTML = '<p class="text-slate-500 text-sm text-center py-4">Nenhum dado disponível.</p>';
            return;
        }

        const { a, b, delta_taxa_pct, delta_inadimplentes } = comp;

        if (a.snapshot_id == null && b.snapshot_id == null) {
            el.innerHTML = '<p class="text-slate-500 text-sm text-center py-4">Nenhum snapshot encontrado para as datas selecionadas.</p>';
            return;
        }

        const deltaSign = delta_taxa_pct > 0 ? '+' : '';
        const deltaColor = delta_taxa_pct < 0
            ? 'text-emerald-500'
            : delta_taxa_pct > 0 ? 'text-rose-500' : 'text-slate-400';
        const inadDeltaColor = delta_inadimplentes < 0
            ? 'text-emerald-500'
            : delta_inadimplentes > 0 ? 'text-rose-500' : 'text-slate-400';
        const inadSign = delta_inadimplentes > 0 ? '+' : '';

        function _sideHtml(side, label) {
            const date = side.snapshot_date || side.fallback_date;
            return `<div class="glass-card border border-[var(--border)] rounded-xl p-4">
                <p class="text-[10px] font-bold text-slate-500 uppercase tracking-wider mb-1">${label} — ${_fmtDate(date)}</p>
                ${_fallbackBadge(side)}
                <p class="text-2xl font-black text-rose-500 tabular-nums mt-2">${_fmtPct(side.taxa_pct)}</p>
                <p class="text-xs text-slate-500 mt-1">${_fmt(side.inadimplentes)} inadimplentes</p>
                <p class="text-xs text-slate-500">${_fmt(side.em_curso)} em curso</p>
            </div>`;
        }

        el.innerHTML = `
            <div class="grid grid-cols-1 md:grid-cols-3 gap-4 mt-4">
                ${_sideHtml(a, 'De')}
                <div class="flex items-center justify-center">
                    <div class="text-center py-4">
                        <p class="text-[10px] font-bold text-slate-500 uppercase tracking-wider mb-2">Variação</p>
                        <p class="text-3xl font-black ${deltaColor} tabular-nums">${deltaSign}${_fmtPct(delta_taxa_pct)}</p>
                        <p class="text-xs ${inadDeltaColor} mt-1">${inadSign}${_fmt(delta_inadimplentes)} alunos</p>
                    </div>
                </div>
                ${_sideHtml(b, 'Até')}
            </div>
        `;
    }

    // ── Gráfico de Evolução ─────────────────────────────────────────────────

    function _renderEvolucaoChart(data) {
        const canvas = document.getElementById('inad-evolucao-chart');
        const emptyEl = document.getElementById('inad-chart-empty');
        if (!canvas) return;

        if (_evolChart) { _evolChart.destroy(); _evolChart = null; }

        if (!data || !data.points || data.points.length === 0) {
            canvas.style.display = 'none';
            if (emptyEl) emptyEl.classList.remove('hidden');
            return;
        }

        canvas.style.display = 'block';
        if (emptyEl) emptyEl.classList.add('hidden');

        const points = data.points;
        const labels = points.map(p => {
            if (!p.date) return '—';
            const [y, m, d] = p.date.split('-');
            return `${d}/${m}`;
        });
        const taxas = points.map(p => p.taxa_pct);

        _evolChart = new Chart(canvas, {
            type: 'line',
            data: {
                labels,
                datasets: [{
                    label: '% Inadimplência',
                    data: taxas,
                    borderColor: '#f43f5e',
                    backgroundColor: 'rgba(244,63,94,0.08)',
                    fill: true,
                    tension: 0.35,
                    borderWidth: 2.5,
                    pointRadius: 4,
                    pointHoverRadius: 6,
                    pointBackgroundColor: '#f43f5e',
                    pointBorderColor: 'rgba(244,63,94,0.3)',
                    pointBorderWidth: 2,
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: ctx => ` Taxa: ${ctx.parsed.y.toFixed(2).replace('.', ',')}%`,
                            afterBody: items => {
                                const idx = items[0].dataIndex;
                                const p = points[idx];
                                return [
                                    ` Inadimplentes: ${_fmt(p.inadimplentes)}`,
                                    ` Em curso: ${_fmt(p.em_curso)}`,
                                ];
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        ticks: { color: '#64748b', maxRotation: 45, font: { size: 11 } },
                        grid: { color: 'rgba(100,116,139,0.15)' }
                    },
                    y: {
                        ticks: {
                            color: '#64748b',
                            callback: v => v.toFixed(1).replace('.', ',') + '%',
                            font: { size: 11 }
                        },
                        grid: { color: 'rgba(100,116,139,0.15)' },
                        beginAtZero: false,
                    }
                }
            }
        });
    }

    // ── Tabela de Histórico ─────────────────────────────────────────────────

    function _renderHistoryTable(snapshots) {
        const tbody = document.getElementById('inad-hist-tbody');
        if (!tbody) return;

        if (!snapshots || snapshots.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" class="py-6 text-center text-slate-500 text-sm">Nenhum snapshot de inadimplência encontrado.</td></tr>';
            return;
        }

        tbody.innerHTML = snapshots.map(s => {
            const dateStr = _fmtDate(s.uploaded_at);
            const fname = s.filename ? esc(s.filename) : '—';
            return `<tr class="border-b border-slate-200 dark:border-slate-800/40 hover:bg-slate-50 dark:hover:bg-slate-800/30 transition-colors">
                <td class="py-2.5 px-4 text-xs text-slate-500 dark:text-slate-400 whitespace-nowrap">${dateStr}</td>
                <td class="py-2.5 px-4 text-xs text-slate-600 dark:text-slate-300 max-w-[200px] truncate" title="${fname}">${fname}</td>
                <td class="py-2.5 px-4 text-right font-bold text-[var(--text-primary)] tabular-nums text-sm">${_fmt(s.inadimplentes)}</td>
                <td class="py-2.5 px-4 text-right text-slate-500 tabular-nums text-xs">${_fmt(s.em_curso)}</td>
                <td class="py-2.5 px-4 text-right font-semibold text-rose-500 tabular-nums text-sm">${_fmtPct(s.taxa_pct)}</td>
            </tr>`;
        }).join('');
    }

    // ── Botões de range ─────────────────────────────────────────────────────

    function _setActiveRange(range) {
        _currentRange = range;
        ['7', '30', '90', 'all'].forEach(r => {
            const btn = document.getElementById(`inad-range-${r}`);
            if (!btn) return;
            const isActive = String(r) === String(range);
            btn.classList.toggle('bg-rose-500', isActive);
            btn.classList.toggle('text-white', isActive);
            btn.classList.toggle('text-slate-600', !isActive);
            btn.classList.toggle('dark:text-slate-400', !isActive);
            btn.classList.toggle('hover:bg-slate-100', !isActive);
            btn.classList.toggle('dark:hover:bg-slate-700/50', !isActive);
        });
        _reloadEvolucao();
    }

    async function _reloadEvolucao() {
        try {
            const res = await api(`/api/inadimplencia/evolucao?days=${_currentRange}`);
            const data = await res.json();
            _renderEvolucaoChart(data);
        } catch (e) {
            console.error('Erro ao carregar evolução:', e);
        }
    }

    // ── Comparação manual ───────────────────────────────────────────────────

    async function _doCompare() {
        const dateA = (document.getElementById('inad-date-a') || {}).value || '';
        const dateB = (document.getElementById('inad-date-b') || {}).value || '';
        const qs = [];
        if (dateA) qs.push('date_a=' + dateA);
        if (dateB) qs.push('date_b=' + dateB);
        try {
            const res = await api('/api/inadimplencia/comparar' + (qs.length ? '?' + qs.join('&') : ''));
            const data = await res.json();
            _renderComparison(data);
        } catch (e) {
            console.error('Erro ao comparar datas:', e);
        }
    }

    // ── Estado vazio / conteúdo ─────────────────────────────────────────────

    function _showEmpty() {
        const emptyEl = document.getElementById('inad-empty-msg');
        const contentEl = document.getElementById('inad-main-content');
        if (emptyEl) emptyEl.classList.remove('hidden');
        if (contentEl) contentEl.classList.add('hidden');
    }

    function _hideEmpty() {
        const emptyEl = document.getElementById('inad-empty-msg');
        const contentEl = document.getElementById('inad-main-content');
        if (emptyEl) emptyEl.classList.add('hidden');
        if (contentEl) contentEl.classList.remove('hidden');
    }

    // ── loadInadimplencia — ponto de entrada chamado por utils.js ───────────

    async function loadInadimplencia() {
        try {
            const today = new Date().toISOString().slice(0, 10);
            const sevenAgo = new Date(Date.now() - 7 * 86400000).toISOString().slice(0, 10);

            // Preenche defaults nos inputs de comparação
            const dateAEl = document.getElementById('inad-date-a');
            const dateBEl = document.getElementById('inad-date-b');
            if (dateAEl && !dateAEl.value) dateAEl.value = sevenAgo;
            if (dateBEl && !dateBEl.value) dateBEl.value = today;

            // Dispara todos os 4 endpoints em paralelo
            const [listRes, atualRes, compRes, evolRes] = await Promise.all([
                api('/api/inadimplencia/list'),
                api('/api/inadimplencia/atual'),
                api('/api/inadimplencia/comparar'),
                api(`/api/inadimplencia/evolucao?days=${_currentRange}`),
            ]);

            const listData = await listRes.json();
            const atualData = atualRes.ok ? await atualRes.json() : null;
            const compData = await compRes.json();
            const evolData = await evolRes.json();

            if (!listData.snapshots || listData.snapshots.length === 0) {
                _showEmpty();
                return;
            }

            _hideEmpty();
            _renderKPIs(atualData, compData);
            _renderComparison(compData);
            _renderEvolucaoChart(evolData);
            _renderHistoryTable(listData.snapshots);
            _setActiveRange(_currentRange);

        } catch (e) {
            console.error('Erro ao carregar inadimplência:', e);
        }
    }

    // Exporta para o escopo global
    window.loadInadimplencia = loadInadimplencia;
    window._inadDoCompare = _doCompare;
    window._inadSetRange = _setActiveRange;

})();
