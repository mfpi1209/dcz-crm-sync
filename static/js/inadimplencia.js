// ===========================================================================
// INADIMPLÊNCIA — Taxa de inadimplência e evolução temporal
// Endpoints: /api/inadimplencia/{list,atual,comparar,evolucao,competencias,
//            comparar-periodo,reincidencia}
// ===========================================================================
(function () {
    'use strict';

    let _evolChart = null;
    let _chartMeses = null;
    let _chartMesesData = null;
    let _chartMesesDate = null;
    let _currentRange = 30;

    function _currentMonthCompetencia() {
        const d = new Date();
        const y = d.getFullYear();
        const m = String(d.getMonth() + 1).padStart(2, '0');
        return `${y}-${m}`;
    }

    function _competenciaToLabelPt(comp) {
        const meses = ['', 'Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
                       'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro'];
        const [y, m] = (comp || '').split('-');
        const mi = parseInt(m, 10);
        if (!y || !mi || mi < 1 || mi > 12) return comp;
        return `${meses[mi]}/${y}`;
    }

    // Estado dos filtros — padrão: "" = "Recentes (linha do tempo)" (bloco contíguo via backend)
    window._inadFilters = {
        competencia: '',
        date_a: null,
        date_b: null,
        recent_months: '3',
    };

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
            const s = String(iso).slice(0, 10);
            const [y, m, d] = s.split('-');
            return `${d}/${m}/${y}`;
        } catch (_) {
            return iso;
        }
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

    // ── Comparação (endpoint /comparar — formato data a/b) ──────────────────

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

    // ── Comparação por período (endpoint /comparar-periodo) ─────────────────

    function _renderPeriodoComparison(data) {
        const el = document.getElementById('inad-comp-result');
        if (!el) return;

        if (!data) {
            el.innerHTML = '<p class="text-slate-500 text-sm text-center py-4">Nenhum dado disponível.</p>';
            return;
        }

        if (data.insuficiente) {
            const count = data.snapshots_count || 0;
            el.innerHTML = `<p class="text-slate-500 text-sm text-center py-4 mt-4">
                É necessário ter pelo menos dois uploads no período para comparar a evolução.
                <span class="block text-xs mt-1">(${count} snapshot${count !== 1 ? 's' : ''} encontrado${count !== 1 ? 's' : ''} para a competência selecionada)</span>
            </p>`;
            return;
        }

        const { competencia_label, snapshots_count, primeiro, ultimo, variacao_pp } = data;

        const varSign = variacao_pp > 0 ? '+' : '';
        const varColor = variacao_pp < 0 ? 'text-emerald-500' : variacao_pp > 0 ? 'text-rose-500' : 'text-slate-400';

        function _sideHtml(snap, label) {
            return `<div class="glass-card border border-[var(--border)] rounded-xl p-4">
                <p class="text-[10px] font-bold text-slate-500 uppercase tracking-wider mb-1">${label} — ${_fmtDate(snap.uploaded_at)}</p>
                <p class="text-2xl font-black text-rose-500 tabular-nums mt-2">${_fmtPct(snap.taxa_pct)}</p>
                <p class="text-xs text-slate-500 mt-1">${_fmt(snap.inadimplentes)} inadimplentes</p>
            </div>`;
        }

        el.innerHTML = `
            <div class="mt-3 mb-1">
                <span class="text-xs font-semibold text-slate-500 uppercase tracking-wider">${competencia_label}</span>
                <span class="text-xs text-slate-400 ml-2">${snapshots_count} snapshot${snapshots_count !== 1 ? 's' : ''} no período</span>
            </div>
            <div class="grid grid-cols-1 md:grid-cols-3 gap-4 mt-2">
                ${_sideHtml(primeiro, 'Primeiro upload')}
                <div class="flex items-center justify-center">
                    <div class="text-center py-4">
                        <p class="text-[10px] font-bold text-slate-500 uppercase tracking-wider mb-2">Variação</p>
                        <p class="text-3xl font-black ${varColor} tabular-nums">${varSign}${Number(variacao_pp).toFixed(2).replace('.', ',')} pp</p>
                        <p class="text-xs text-slate-400 mt-1">pontos percentuais</p>
                    </div>
                </div>
                ${_sideHtml(ultimo, 'Último upload')}
            </div>
        `;
    }

    // ── Reincidência ────────────────────────────────────────────────────────

    function _renderReincidencia(data) {
        const el2 = document.getElementById('inad-reinc-2');
        const el3 = document.getElementById('inad-reinc-3');
        const el4 = document.getElementById('inad-reinc-4_plus');
        const elMeta = document.getElementById('inad-reinc-meta');

        if (!data || data.error) {
            if (el2) el2.textContent = '—';
            if (el3) el3.textContent = '—';
            if (el4) el4.textContent = '—';
            if (elMeta) elMeta.textContent = data && data.error ? `Erro: ${data.error}` : '—';
            return;
        }

        const { buckets, competencias_usadas, rgms_analisados } = data;
        if (el2) el2.textContent = _fmt(buckets['2'] || 0);
        if (el3) el3.textContent = _fmt(buckets['3'] || 0);
        if (el4) el4.textContent = _fmt(buckets['4_plus'] || 0);

        const nComp = (competencias_usadas || []).length;
        const metaText = nComp > 0
            ? `${_fmt(rgms_analisados)} RGM${rgms_analisados !== 1 ? 's' : ''} único${rgms_analisados !== 1 ? 's' : ''} analisado${rgms_analisados !== 1 ? 's' : ''} · ${nComp} competência${nComp !== 1 ? 's' : ''} considerada${nComp !== 1 ? 's' : ''}: ${competencias_usadas.join(', ')}`
            : 'Nenhuma competência com dados disponível.';
        if (elMeta) elMeta.textContent = metaText;
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

    // ── Gráfico Comparativo por Competência ─────────────────────────────────

    const _CHART_MESES_PALETTE = [
        '#f43f5e', '#6366f1', '#10b981', '#f59e0b',
        '#06b6d4', '#a855f7', '#ec4899', '#84cc16',
        '#0ea5e9', '#f97316',
    ];

    function _hexToRgba(hex, alpha) {
        const h = hex.replace('#', '');
        const r = parseInt(h.substring(0, 2), 16);
        const g = parseInt(h.substring(2, 4), 16);
        const b = parseInt(h.substring(4, 6), 16);
        return `rgba(${r},${g},${b},${alpha})`;
    }

    function _collectDates(data) {
        const set = new Set();
        const comps = (data && data.competencias) ? data.competencias : [];
        comps.forEach(c => (c.points || []).forEach(p => { if (p.date) set.add(p.date); }));
        return Array.from(set).sort();
    }

    function _syncDateInput(data) {
        const input = document.getElementById('inad-meses-data');
        if (!input) return;
        const dates = _collectDates(data);
        if (dates.length === 0) {
            input.value = '';
            input.removeAttribute('min');
            input.removeAttribute('max');
            return;
        }
        input.min = dates[0];
        input.max = dates[dates.length - 1];
        if (!_chartMesesDate || !dates.includes(_chartMesesDate)) {
            _chartMesesDate = dates[dates.length - 1];
        }
        input.value = _chartMesesDate;
    }

    function _renderChartMeses(data) {
        const canvas = document.getElementById('inad-chart-meses');
        const emptyEl = document.getElementById('inad-chart-meses-empty');
        if (!canvas) return;

        if (_chartMeses) { _chartMeses.destroy(); _chartMeses = null; }

        const comps = (data && data.competencias) ? data.competencias : [];
        const items = comps
            .map(c => {
                const pts = c.points || [];
                const match = _chartMesesDate
                    ? pts.find(p => p.date === _chartMesesDate)
                    : pts[pts.length - 1];
                return match ? { label: c.label || c.nivel, nivel: c.nivel, last: match } : null;
            })
            .filter(Boolean)
            .sort((a, b) => a.nivel.localeCompare(b.nivel));

        if (items.length === 0) {
            canvas.style.display = 'none';
            if (emptyEl) {
                emptyEl.textContent = _chartMesesDate
                    ? `Nenhuma competência com snapshot em ${_chartMesesDate.split('-').reverse().join('/')}.`
                    : 'Sem snapshots para comparar entre competências.';
                emptyEl.classList.remove('hidden');
            }
            return;
        }
        canvas.style.display = 'block';
        if (emptyEl) emptyEl.classList.add('hidden');

        const labels = items.map(i => i.label);
        const values = items.map(i => i.last.taxa_pct);
        const colors = items.map((_, idx) =>
            _CHART_MESES_PALETTE[idx % _CHART_MESES_PALETTE.length]);

        _chartMeses = new Chart(canvas, {
            type: 'bar',
            data: {
                labels,
                datasets: [{
                    label: '% Inadimplência',
                    data: values,
                    backgroundColor: colors.map(c => _hexToRgba(c, 0.7)),
                    borderColor: colors,
                    borderWidth: 1.5,
                    borderRadius: 6,
                    _items: items,
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            title: ctx => ctx[0].label,
                            label: ctx => ` Taxa: ${ctx.parsed.y.toFixed(2).replace('.', ',')}%`,
                            afterBody: ctx => {
                                const it = ctx[0].dataset._items[ctx[0].dataIndex].last;
                                const dataFmt = it.date ? it.date.split('-').reverse().join('/') : '—';
                                return [
                                    ` Atualizado em: ${dataFmt}`,
                                    ` Inadimplentes: ${_fmt(it.inadimplentes)}`,
                                    ` Em curso: ${_fmt(it.em_curso)}`,
                                ];
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        ticks: { color: '#64748b', font: { size: 11 } },
                        grid: { display: false }
                    },
                    y: {
                        ticks: {
                            color: '#64748b',
                            callback: v => v.toFixed(1).replace('.', ',') + '%',
                            font: { size: 11 }
                        },
                        grid: { color: 'rgba(100,116,139,0.15)' },
                        beginAtZero: true,
                    }
                }
            }
        });
    }

    async function _reloadChartMeses() {
        try {
            const f = window._inadFilters || {};
            const parts = [];
            if (f.date_a) parts.push('date_a=' + f.date_a);
            if (f.date_b) parts.push('date_b=' + f.date_b);
            const qs = parts.length ? '?' + parts.join('&') : '';
            const res = await api('/api/inadimplencia/evolucao-por-mes' + qs);
            const data = await res.json();
            _chartMesesData = data;
            _syncDateInput(data);
            _wireChartMesesDateInput();
            _renderChartMeses(data);
        } catch (e) {
            console.error('Erro ao carregar comparativo por competência:', e);
        }
    }

    function _wireChartMesesDateInput() {
        const input = document.getElementById('inad-meses-data');
        if (!input || input._wired) return;
        input.addEventListener('change', () => {
            _chartMesesDate = input.value || null;
            if (_chartMesesData) _renderChartMeses(_chartMesesData);
        });
        input._wired = true;
    }

    // ── Tabela de Histórico ─────────────────────────────────────────────────

    function _renderHistoryTable(snapshots) {
        const tbody = document.getElementById('inad-hist-tbody');
        if (!tbody) return;

        if (!snapshots || snapshots.length === 0) {
            tbody.innerHTML = '<tr><td colspan="7" class="py-6 text-center text-slate-500 text-sm">Nenhum snapshot de inadimplência encontrado.</td></tr>';
            return;
        }

        tbody.innerHTML = snapshots.map(s => {
            const dateStr = _fmtDate(s.uploaded_at);
            const fname = s.filename ? esc(s.filename) : '—';
            const snapId = s.snapshot_id || s.id;
            const compLabel = s.competencia_label
                ? `<span>${esc(s.competencia_label)}</span>`
                : `<span class="italic text-slate-400">—</span>`;
            return `<tr class="border-b border-slate-200 dark:border-slate-800/40 hover:bg-slate-50 dark:hover:bg-slate-800/30 transition-colors">
                <td class="py-2.5 px-4 text-xs text-slate-500 dark:text-slate-400 whitespace-nowrap">${dateStr}</td>
                <td class="py-2.5 px-4 text-xs text-slate-600 dark:text-slate-300 max-w-[200px] truncate" title="${fname}">${fname}</td>
                <td class="py-2.5 px-4 text-xs text-slate-500 dark:text-slate-400 whitespace-nowrap">${compLabel}</td>
                <td class="py-2.5 px-4 text-right font-bold text-[var(--text-primary)] tabular-nums text-sm">${_fmt(s.inadimplentes)}</td>
                <td class="py-2.5 px-4 text-right text-slate-500 tabular-nums text-xs">${_fmt(s.em_curso)}</td>
                <td class="py-2.5 px-4 text-right font-semibold text-rose-500 tabular-nums text-sm">${_fmtPct(s.taxa_pct)}</td>
                <td class="py-2.5 px-4 text-right">
                    <button onclick="_inadDeleteSnapshot(${snapId}, '${esc(s.filename || '')}')"
                            class="text-xs text-red-500 hover:text-red-400 hover:bg-red-500/10 px-2 py-1 rounded transition">
                        Apagar
                    </button>
                </td>
            </tr>`;
        }).join('');
    }

    async function _inadDeleteSnapshot(snapId, filename) {
        if (!confirm(`Apagar snapshot ${filename}?\nEsta ação não pode ser desfeita.`)) return;
        try {
            const res = await fetch(`/api/inadimplencia/snapshot/${snapId}`, { method: 'DELETE' });
            const data = await res.json();
            if (!res.ok) {
                alert('Erro: ' + (data.error || 'falha desconhecida'));
                return;
            }
            loadInadimplencia();
        } catch (err) {
            alert('Erro: ' + err.message);
        }
    }

    // ── Carregar competências no select ─────────────────────────────────────

    async function loadCompetencias() {
        const select = document.getElementById('inad-filter-competencia');
        if (!select) return;

        try {
            const res = await api('/api/inadimplencia/competencias');
            const data = await res.json();
            const list = data.competencias || [];
            const currentComp = _currentMonthCompetencia();
            const currentLabel = _competenciaToLabelPt(currentComp);

            // Sempre incluir o mês atual no topo (mesmo sem snapshot ainda)
            const hasCurrent = list.some(c => c.value === currentComp);
            let html = '<option value="">Recentes (linha do tempo)</option>';
            if (!hasCurrent) {
                html += `<option value="${currentComp}">${currentLabel} (mês atual)</option>`;
            }
            for (const c of list) {
                const isCurrent = c.value === currentComp;
                html += `<option value="${c.value}">${c.label}${isCurrent ? ' (mês atual)' : ''}</option>`;
            }
            select.innerHTML = html;
            select.value = _inadFilters.competencia || '';
        } catch (e) {
            console.error('Erro ao carregar competências:', e);
        }
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
            const f = window._inadFilters || {};
            const qs = [];
            // Quando há filtro de data, não aplica o range por dias
            if (f.date_a || f.date_b) {
                if (f.competencia) qs.push('competencia=' + encodeURIComponent(f.competencia));
                if (f.date_a) qs.push('date_a=' + f.date_a);
                if (f.date_b) qs.push('date_b=' + f.date_b);
                if (!f.competencia && f.recent_months) qs.push('recent_months=' + encodeURIComponent(f.recent_months));
            } else {
                qs.push(`days=${_currentRange}`);
                if (f.competencia) qs.push('competencia=' + encodeURIComponent(f.competencia));
                if (!f.competencia && f.recent_months) qs.push('recent_months=' + encodeURIComponent(f.recent_months));
            }
            const res = await api('/api/inadimplencia/evolucao' + (qs.length ? '?' + qs.join('&') : ''));
            const data = await res.json();
            _renderEvolucaoChart(data);
        } catch (e) {
            console.error('Erro ao carregar evolução:', e);
        }
    }

    // ── Comparação manual (botão "Comparar" no card) ────────────────────────

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

    // ── Filtros: Aplicar / Limpar ────────────────────────────────────────────

    function _applyFilters() {
        const competencia = (document.getElementById('inad-filter-competencia') || {}).value || '';
        const date_a = (document.getElementById('inad-filter-date-a') || {}).value || '';
        const date_b = (document.getElementById('inad-filter-date-b') || {}).value || '';
        const recent_months = (document.getElementById('inad-filter-recent-months') || {}).value || '3';
        window._inadFilters = { competencia, date_a, date_b, recent_months };
        loadInadimplencia();
    }

    function _clearFilters() {
        window._inadFilters = {
            competencia: '',
            date_a: null,
            date_b: null,
            recent_months: '3',
        };
        const sel = document.getElementById('inad-filter-competencia');
        if (sel) sel.value = '';
        const da = document.getElementById('inad-filter-date-a');
        const db = document.getElementById('inad-filter-date-b');
        if (da) da.value = '';
        if (db) db.value = '';
        const rm = document.getElementById('inad-filter-recent-months');
        if (rm) rm.value = '3';
        loadInadimplencia();
    }

    function _wireFilterButtons() {
        const applyBtn = document.getElementById('inad-filter-apply');
        const clearBtn = document.getElementById('inad-filter-clear');
        const recentSel = document.getElementById('inad-filter-recent-months');
        if (applyBtn && !applyBtn._wired) {
            applyBtn.addEventListener('click', _applyFilters);
            applyBtn._wired = true;
        }
        if (clearBtn && !clearBtn._wired) {
            clearBtn.addEventListener('click', _clearFilters);
            clearBtn._wired = true;
        }
        // Trocar "últimos meses" recarrega na hora (sem precisar clicar Aplicar)
        if (recentSel && !recentSel._wired) {
            recentSel.addEventListener('change', _applyFilters);
            recentSel._wired = true;
        }
    }

    // ── Estado vazio / conteúdo ─────────────────────────────────────────────

    function _showEmpty() {
        const emptyEl = document.getElementById('inad-empty-msg');
        const contentEl = document.getElementById('inad-main-content');
        const titleEl = document.getElementById('inad-empty-title');
        const descEl = document.getElementById('inad-empty-desc');
        const f = window._inadFilters || {};
        if (f.competencia) {
            const label = _competenciaToLabelPt(f.competencia);
            if (titleEl) titleEl.textContent = `Sem dados para ${label}`;
            if (descEl) descEl.innerHTML = `Nenhum snapshot de inadimpl\u00eancia para esta compet\u00eancia.<br>Selecione outra compet\u00eancia acima ou suba um arquivo em <strong>Upload Acad\u00eamico \u2192 Inadimplentes</strong>.`;
        } else {
            if (titleEl) titleEl.textContent = 'Nenhum dado de inadimpl\u00eancia';
            if (descEl) descEl.innerHTML = 'Nenhum snapshot de inadimpl\u00eancia foi feito ainda.<br>Suba o primeiro arquivo em <strong>Upload Acad\u00eamico \u2192 Inadimplentes</strong>.';
        }
        if (emptyEl) emptyEl.classList.remove('hidden');
        if (contentEl) contentEl.classList.add('hidden');
    }

    function _hideEmpty() {
        const emptyEl = document.getElementById('inad-empty-msg');
        const contentEl = document.getElementById('inad-main-content');
        if (emptyEl) emptyEl.classList.add('hidden');
        if (contentEl) contentEl.classList.remove('hidden');
    }

    // ── Construir query string dos filtros ativos ────────────────────────────

    function _filterQs(includeCompetencia = true) {
        const f = window._inadFilters || {};
        const parts = [];
        if (includeCompetencia && f.competencia) parts.push('competencia=' + encodeURIComponent(f.competencia));
        if (f.date_a) parts.push('date_a=' + f.date_a);
        if (f.date_b) parts.push('date_b=' + f.date_b);
        // Só envia recent_months quando NÃO há competência específica (é o filtro do "Recentes")
        if (!f.competencia && f.recent_months) parts.push('recent_months=' + encodeURIComponent(f.recent_months));
        return parts.length ? '?' + parts.join('&') : '';
    }

    // ── loadInadimplencia — ponto de entrada chamado por utils.js ───────────

    async function loadInadimplencia() {
        try {
            _wireFilterButtons();
            loadCompetencias();

            const f = window._inadFilters || {};
            const today = new Date().toISOString().slice(0, 10);
            const sevenAgo = new Date(Date.now() - 7 * 86400000).toISOString().slice(0, 10);

            // Preenche defaults nos inputs de comparação manual (não os de filtro)
            const dateAEl = document.getElementById('inad-date-a');
            const dateBEl = document.getElementById('inad-date-b');
            if (dateAEl && !dateAEl.value) dateAEl.value = sevenAgo;
            if (dateBEl && !dateBEl.value) dateBEl.value = today;

            // Monta URL da evolução
            const evolQs = (() => {
                const parts = [`days=${_currentRange}`];
                if (f.competencia) parts.push('competencia=' + encodeURIComponent(f.competencia));
                if (f.date_a) parts.push('date_a=' + f.date_a);
                if (f.date_b) parts.push('date_b=' + f.date_b);
                if (!f.competencia && f.recent_months) parts.push('recent_months=' + encodeURIComponent(f.recent_months));
                return '?' + parts.join('&');
            })();

            // Monta URL de comparação
            const compUrl = f.competencia
                ? '/api/inadimplencia/comparar-periodo' + _filterQs()
                : '/api/inadimplencia/comparar';

            // Monta URL de reincidência (nunca usa competencia, só datas)
            const reincQs = _filterQs(false);

            // Dispara todos os endpoints em paralelo
            const [listRes, atualRes, compRes, evolRes, reincRes] = await Promise.all([
                api('/api/inadimplencia/list' + _filterQs()),
                api('/api/inadimplencia/atual' + _filterQs()),
                api(compUrl),
                api('/api/inadimplencia/evolucao' + evolQs),
                api('/api/inadimplencia/reincidencia' + reincQs),
            ]);

            const listData = await listRes.json();
            const atualData = atualRes.ok ? await atualRes.json() : null;
            const compData = await compRes.json();
            const evolData = await evolRes.json();
            const reincData = reincRes.ok ? await reincRes.json() : null;

            if (!listData.snapshots || listData.snapshots.length === 0) {
                _showEmpty();
                return;
            }

            _hideEmpty();
            _renderKPIs(atualData, f.competencia ? null : compData);

            if (f.competencia) {
                _renderPeriodoComparison(compData);
            } else {
                _renderComparison(compData);
            }

            _renderEvolucaoChart(evolData);
            _renderHistoryTable(listData.snapshots);
            _renderReincidencia(reincData);
            _setActiveRange(_currentRange);
            _reloadChartMeses();

        } catch (e) {
            console.error('Erro ao carregar inadimplência:', e);
        }
    }

    // Exporta para o escopo global
    window.loadInadimplencia = loadInadimplencia;
    window._inadDoCompare = _doCompare;
    window._inadSetRange = _setActiveRange;
    window._inadDeleteSnapshot = _inadDeleteSnapshot;

})();
