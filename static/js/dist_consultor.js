// ---------------------------------------------------------------------------
// Distribuição por Consultor — Dashboard (vanilla JS + Chart.js)
// ---------------------------------------------------------------------------

(function () {
    const WEBHOOK_URL = "https://n8n-new-n8n.ca31ey.easypanel.host/webhook/distribuicaoporconsultor-origens";
    const CHART_COLORS = ["#2563eb", "#0ea5e9", "#14b8a6", "#22c55e", "#eab308", "#f97316", "#ef4444", "#8b5cf6", "#06b6d4", "#84cc16"];
    let _rows = [];
    let _rawTotalVendas = 0;
    let _rawTotalLeads = 0;
    let _fechadasPeriodo = {};       // map: consultor -> { do_periodo, fora_periodo, total }
    let _matriculasPorOrigem = [];   // [{origem, do_periodo, fora_periodo, total}]
    let _chartConsultores = null;
    let _chartOrigens = null;
    let _chartDiaOrigem = null;
    let _loaded = false;
    let _filtersListening = false;

    // ── Date helpers ──────────────────────────────────────────────────────

    function dpPresetRange(id, incluirHoje) {
        var n = new Date();
        var todayEnd   = new Date(n.getFullYear(), n.getMonth(), n.getDate(), 23, 59, 59, 999);
        var todayStart = new Date(n.getFullYear(), n.getMonth(), n.getDate());
        var yestEnd    = new Date(todayStart.getTime() - 1);
        var yestStart  = new Date(yestEnd.getFullYear(), yestEnd.getMonth(), yestEnd.getDate());
        var rangeEnd   = incluirHoje ? todayEnd : yestEnd;
        switch (id) {
            case 'all':       return { start: null, end: null };
            case 'today':     return { start: todayStart, end: todayEnd };
            case 'yesterday': return { start: yestStart, end: yestEnd };
            case '7d':  { var s = new Date(todayStart); s.setDate(s.getDate()-6);  return { start: s, end: rangeEnd }; }
            case '30d': { var s = new Date(todayStart); s.setDate(s.getDate()-29); return { start: s, end: rangeEnd }; }
            case 'thismonth': return { start: new Date(n.getFullYear(), n.getMonth(), 1), end: rangeEnd };
            case 'lastmonth': return {
                start: new Date(n.getFullYear(), n.getMonth()-1, 1),
                end:   new Date(n.getFullYear(), n.getMonth(), 0, 23, 59, 59, 999)
            };
            default: return { start: null, end: null };
        }
    }

    function dpLabel(preset, customStart, customEnd, incluirHoje) {
        var p = DP_PRESETS.find(function(x) { return x.id === preset; });
        if (preset === 'custom') {
            if (customStart && customEnd) {
                var fmt = function(d) { return d.toLocaleDateString('pt-BR', { day:'2-digit', month:'2-digit', year:'numeric' }); };
                return fmt(customStart) + ' – ' + fmt(customEnd);
            }
            return 'Personalizado';
        }
        if (!p) return 'Tudo';
        var suffix = (preset === '7d' || preset === '30d' || preset === 'thismonth')
            ? (incluirHoje ? ', até hoje' : ', sem hoje') : '';
        return p.label + suffix;
    }

    // ── Date Picker (simple inline inputs + preset chips) ────────────────

    function dcDateInit() {
        var n = new Date();
        var today = fmtDateISO(n);
        var ago6  = fmtDateISO(new Date(n.getFullYear(), n.getMonth(), n.getDate() - 6));
        var inStart = document.getElementById('dc-date-start');
        var inEnd   = document.getElementById('dc-date-end');
        if (inStart) inStart.value = ago6;
        if (inEnd)   inEnd.value   = today;
        dcPresetHighlight('7d');
    }

    function dcPresetHighlight(id) {
        document.querySelectorAll('.dc-preset-chip').forEach(function(el) {
            el.classList.toggle('active', el.dataset.preset === id);
        });
    }

    window.dcPresetClick = function(id) {
        var n = new Date();
        var today = fmtDateISO(n);
        var inStart = document.getElementById('dc-date-start');
        var inEnd   = document.getElementById('dc-date-end');
        var s, e;
        switch(id) {
            case 'yesterday': {
                var y = new Date(n.getFullYear(), n.getMonth(), n.getDate() - 1);
                s = fmtDateISO(y); e = s; break;
            }
            case '7d':
                s = fmtDateISO(new Date(n.getFullYear(), n.getMonth(), n.getDate() - 6));
                e = today; break;
            case '30d':
                s = fmtDateISO(new Date(n.getFullYear(), n.getMonth(), n.getDate() - 29));
                e = today; break;
            case 'thismonth':
                s = fmtDateISO(new Date(n.getFullYear(), n.getMonth(), 1));
                e = today; break;
            case 'lastmonth':
                s = fmtDateISO(new Date(n.getFullYear(), n.getMonth() - 1, 1));
                e = fmtDateISO(new Date(n.getFullYear(), n.getMonth(), 0));
                break;
            default: s = null; e = null;
        }
        if (inStart && s) inStart.value = s;
        if (inEnd   && e) inEnd.value   = e;
        dcPresetHighlight(id);
    };

    // ── helpers ────────────────────────────────────────────────────────────

    function fmtDateISO(d) {
        return d.getFullYear() + '-' +
            String(d.getMonth() + 1).padStart(2, '0') + '-' +
            String(d.getDate()).padStart(2, '0');
    }

    function fmtDateDisplay(val) {
        if (!val) return null;
        var d = new Date(val);
        if (isNaN(d.getTime())) return String(val);
        return d.toLocaleDateString("pt-BR", { timeZone: "UTC" });
    }

    function fmtNumber(v) {
        return new Intl.NumberFormat("pt-BR").format(Number(v || 0));
    }

    function toNum(v) {
        var n = Number(v);
        return Number.isFinite(n) ? n : 0;
    }

    function safe(v, fb) {
        if (v === null || v === undefined) return fb || "Sem informação";
        var t = String(v).trim();
        return t || (fb || "Sem informação");
    }

    function slugOrigin(v) {
        return String(v || "origem")
            .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
            .toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "") || "origem";
    }

    // ── normalize webhook response ────────────────────────────────────────

    function unwrapItem(i) {
        return (i && typeof i === "object" && !Array.isArray(i) && i.json && typeof i.json === "object" && !Array.isArray(i.json))
            ? i.json : i;
    }

    function flattenAll(payload) {
        if (typeof payload === "string") {
            try { return flattenAll(JSON.parse(payload)); } catch(e) { return []; }
        }
        if (Array.isArray(payload)) {
            var out = [];
            payload.forEach(function(item) {
                if (Array.isArray(item)) {
                    item.forEach(function(sub) { out.push(unwrapItem(sub)); });
                } else {
                    var uw = unwrapItem(item);
                    if (Array.isArray(uw)) {
                        uw.forEach(function(sub) { out.push(unwrapItem(sub)); });
                    } else {
                        out.push(uw);
                    }
                }
            });
            return out;
        }
        if (payload && typeof payload === "object") {
            var out2 = [];
            Object.values(payload).forEach(function(val) {
                if (Array.isArray(val)) {
                    val.forEach(function(item) { out2.push(unwrapItem(item)); });
                }
            });
            return out2;
        }
        return [];
    }

    var SEM_CONSULTOR = "sem consultor";

    function isSemConsultor(c) {
        return !c || String(c).trim().toLowerCase() === SEM_CONSULTOR;
    }

    function computeRawTotals(rawItems) {
        var totalVendas = 0, totalLeads = 0;
        rawItems.forEach(function(item) {
            if (!item || typeof item !== "object" || Array.isArray(item)) return;
            totalVendas += toNum(item.total_vendas);
            totalLeads += toNum(item.total_leads ?? item.total);
        });
        return { totalVendas: totalVendas, totalLeads: totalLeads };
    }

    function mapRows(rawItems) {
        return rawItems.filter(function(item) {
            return item && typeof item === "object" && !Array.isArray(item);
        }).map(function(item) {
            var origemStr = String(item.origem || "").trim();
            if (!origemStr) return null;
            var consultorStr = String(item.consultor || "").trim();
            return {
                origem: origemStr,
                consultor: consultorStr || "Sem consultor",
                _semConsultor: isSemConsultor(consultorStr),
                id_consultor: item.id_consultor ?? "-",
                diaRaw: item.dia || null,
                dia: fmtDateDisplay(item.dia),
                total_leads: toNum(item.total_leads ?? item.total),
                total_dia_origem: toNum(item.total_dia_origem),
                total_vendas: toNum(item.total_vendas),
                conversao_pct: toNum(item.conversao_pct)
            };
        }).filter(function(r) { return r && r.total_leads > 0; });
    }

    // ── filter + compute ──────────────────────────────────────────────────

    function getFiltered(excludeSemConsultor) {
        var cf  = document.getElementById("dc-consultor-filter")?.value || "";
        var of_ = document.getElementById("dc-origem-filter")?.value  || "";
        return _rows.filter(function(r) {
            if (excludeSemConsultor && r._semConsultor) return false;
            if (cf  && r.consultor !== cf)  return false;
            if (of_ && r.origem   !== of_)  return false;
            return true;
        });
    }

    function computeSummary(allRows, chartRows) {
        var totalLeads  = allRows.reduce(function(a, r) { return a + r.total_leads;  }, 0);
        var totalVendas = allRows.reduce(function(a, r) { return a + r.total_vendas; }, 0);
        var taxaConversao = totalLeads > 0 ? (totalVendas / totalLeads * 100) : 0;

        var consultores = new Set(chartRows.map(function(r) { return r.consultor; })).size;
        var origens = new Set(chartRows.map(function(r) { return r.origem; })).size;
        var allDias = new Set(allRows.map(function(r) { return r.dia; }).filter(Boolean)).size;
        var chartDias = new Set(chartRows.map(function(r) { return r.dia; }).filter(Boolean)).size;
        return {
            totalLeads: totalLeads, consultores: consultores, origens: origens,
            mediaPorDia: allDias ? totalLeads / allDias : 0, hasDias: chartDias > 0,
            totalVendas: totalVendas, taxaConversao: taxaConversao
        };
    }

    // ── aggregation helpers ───────────────────────────────────────────────

    function leadsByConsultor(filtered) {
        var g = {};
        filtered.forEach(function(r) { g[r.consultor] = (g[r.consultor] || 0) + r.total_leads; });
        return Object.entries(g).map(function(e) { return { consultor: e[0], total: e[1] }; })
            .sort(function(a, b) { return b.total - a.total; });
    }

    function leadsByOrigem(filtered) {
        var g = {};
        filtered.forEach(function(r) { g[r.origem] = (g[r.origem] || 0) + r.total_leads; });
        return Object.entries(g).map(function(e) { return { origem: e[0], total: e[1] }; })
            .filter(function(i) { return i.total > 0; }).sort(function(a, b) { return b.total - a.total; });
    }

    function leadsByDiaOrigem(filtered, origensData) {
        var originKeys = origensData.map(function(o, i) {
            return { origem: o.origem, key: "origem_" + slugOrigin(o.origem) + "_" + i, color: CHART_COLORS[i % CHART_COLORS.length] };
        });
        var keyMap = {};
        originKeys.forEach(function(o) { keyMap[o.origem] = o.key; });

        var grouped = new Map();
        filtered.forEach(function(r) {
            if (!r.dia) return;
            var ck = keyMap[r.origem];
            if (!ck) return;
            if (!grouped.has(r.dia)) grouped.set(r.dia, { dia: r.dia, diaRaw: r.diaRaw });
            var cur = grouped.get(r.dia);
            cur[ck] = (cur[ck] || 0) + r.total_leads;
        });

        var days = Array.from(grouped.values()).sort(function(a, b) { return new Date(a.diaRaw).getTime() - new Date(b.diaRaw).getTime(); });
        return { days: days, originKeys: originKeys };
    }

    function matriculasByOrigem(filtered) {
        var g = {};
        filtered.forEach(function(r) {
            if (!g[r.origem]) g[r.origem] = { leads: 0, vendas: 0 };
            g[r.origem].leads += r.total_leads;
            g[r.origem].vendas += r.total_vendas;
        });
        return Object.entries(g).map(function(e) {
            return {
                origem: e[0], leads: e[1].leads, vendas: e[1].vendas,
                conversao: e[1].leads > 0 ? (e[1].vendas / e[1].leads * 100) : 0
            };
        }).filter(function(i) { return i.vendas > 0; }).sort(function(a, b) { return b.vendas - a.vendas; });
    }

    function matriculasByConsultor(filtered) {
        var g = {};
        filtered.forEach(function(r) {
            if (!g[r.consultor]) g[r.consultor] = { leads: 0, vendas: 0 };
            g[r.consultor].leads += r.total_leads;
            g[r.consultor].vendas += r.total_vendas;
        });
        // Inclui consultores que só aparecem em _fechadasPeriodo
        Object.keys(_fechadasPeriodo).forEach(function(c) {
            if (!g[c]) g[c] = { leads: 0, vendas: 0 };
        });
        return Object.entries(g).map(function(e) {
            var f = _fechadasPeriodo[e[0]] || { do_periodo: 0, fora_periodo: 0, total: 0 };
            return {
                consultor:    e[0],
                leads:        e[1].leads,
                vendas:       e[1].vendas,
                do_periodo:   f.do_periodo,
                fora_periodo: f.fora_periodo,
                total_fechadas: f.total,
                conversao: e[1].leads > 0 ? (e[1].vendas / e[1].leads * 100) : 0
            };
        }).filter(function(i) { return i.vendas > 0 || i.total_fechadas > 0; })
          .sort(function(a, b) { return (b.total_fechadas || b.vendas) - (a.total_fechadas || a.vendas); });
    }

    // ── populate filters ──────────────────────────────────────────────────

    function populateFilters() {
        var cSel = document.getElementById("dc-consultor-filter");
        var oSel = document.getElementById("dc-origem-filter");
        var cVal = cSel.value, oVal = oSel.value;

        var consultores = [...new Set(_rows.filter(function(r) { return !r._semConsultor; }).map(function(r) { return r.consultor; }))].sort();
        var origens = [...new Set(_rows.map(function(r) { return r.origem; }))].sort();

        cSel.innerHTML = '<option value="">Todos</option>' +
            consultores.map(function(c) { return '<option value="' + c + '">' + c + '</option>'; }).join('');
        oSel.innerHTML = '<option value="">Todas</option>' +
            origens.map(function(o) { return '<option value="' + o + '">' + o + '</option>'; }).join('');

        if (consultores.includes(cVal)) cSel.value = cVal;
        if (origens.includes(oVal)) oSel.value = oVal;
    }

    // ── render ─────────────────────────────────────────────────────────────

    function isDark() { return document.documentElement.classList.contains('dark'); }
    function chartTextColor() { return isDark() ? '#94a3b8' : '#475569'; }
    function chartGridColor() { return isDark() ? 'rgba(51,65,85,0.4)' : '#e2e8f0'; }

    function render() {
        var allFiltered = getFiltered(false);
        var chartFiltered = getFiltered(true);
        var s = computeSummary(allFiltered, chartFiltered);

        document.getElementById("dc-m-leads").textContent = fmtNumber(s.totalLeads);
        document.getElementById("dc-m-consultores").textContent = fmtNumber(s.consultores);
        document.getElementById("dc-m-origens").textContent = fmtNumber(s.origens);
        document.getElementById("dc-m-media").textContent = s.hasDias ? fmtNumber(s.mediaPorDia.toFixed(1)) : "—";

        // Total matrículas respeita filtros de consultor e origem ativos
        var cf  = document.getElementById("dc-consultor-filter")?.value || "";
        var of_ = document.getElementById("dc-origem-filter")?.value   || "";
        var totalMatriculas;
        if (cf && of_) {
            // Ambos filtros ativos → usa total_vendas do webhook (já filtrado por ambos)
            totalMatriculas = s.totalVendas;
        } else if (cf) {
            // Só consultor → _fechadasPeriodo (fonte do Dashboard Comercial)
            var fp = _fechadasPeriodo[cf];
            totalMatriculas = fp ? fp.total : s.totalVendas;
        } else if (of_ && _matriculasPorOrigem.length) {
            // Só origem → filtra _matriculasPorOrigem por origem
            var matFilt = _matriculasPorOrigem.filter(function(r) { return r.origem === of_; });
            totalMatriculas = matFilt.reduce(function(acc, r) { return acc + r.total; }, 0);
        } else if (_matriculasPorOrigem.length) {
            // Sem filtro → soma todos
            totalMatriculas = _matriculasPorOrigem.reduce(function(acc, r) { return acc + r.total; }, 0);
        } else {
            totalMatriculas = s.totalVendas;
        }
        document.getElementById("dc-m-conversoes").textContent = fmtNumber(totalMatriculas);
        document.getElementById("dc-m-taxa").textContent = s.taxaConversao.toFixed(2) + "%";

        renderBarConsultores(chartFiltered);
        renderPieOrigens(chartFiltered);
        renderStackedDiaOrigem(chartFiltered, s.hasDias);
        renderOrigemTable(allFiltered);
        renderConsultorCards();
    }

    function renderBarConsultores(filtered) {
        var data = leadsByConsultor(filtered);
        if (_chartConsultores) _chartConsultores.destroy();
        var ctx = document.getElementById("dc-chart-consultores").getContext("2d");
        _chartConsultores = new Chart(ctx, {
            type: "bar",
            data: {
                labels: data.map(function(d) { return d.consultor; }),
                datasets: [{ label: "Leads", data: data.map(function(d) { return d.total; }), backgroundColor: "#2563eb", borderRadius: 8, maxBarThickness: 48 }]
            },
            options: {
                responsive: true, maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: {
                    x: { ticks: { color: chartTextColor(), font: { size: 11 } }, grid: { display: false } },
                    y: { ticks: { color: chartTextColor(), font: { size: 11 } }, grid: { color: chartGridColor() }, beginAtZero: true }
                }
            }
        });
    }

    function renderPieOrigens(filtered) {
        var data = leadsByOrigem(filtered);
        if (_chartOrigens) _chartOrigens.destroy();
        var ctx = document.getElementById("dc-chart-origens").getContext("2d");
        _chartOrigens = new Chart(ctx, {
            type: "pie",
            data: {
                labels: data.map(function(d) { return d.origem; }),
                datasets: [{ data: data.map(function(d) { return d.total; }), backgroundColor: data.map(function(_, i) { return CHART_COLORS[i % CHART_COLORS.length]; }), borderWidth: 0 }]
            },
            options: {
                responsive: true, maintainAspectRatio: false,
                plugins: { legend: { position: "right", labels: { color: chartTextColor(), font: { size: 12 }, padding: 12, usePointStyle: true, pointStyle: "circle" } } }
            }
        });
    }

    function renderStackedDiaOrigem(filtered, hasDias) {
        if (_chartDiaOrigem) _chartDiaOrigem.destroy();
        var container = document.getElementById("dc-chart-dia-origem").closest(".dc-chart-card");
        if (!hasDias) {
            if (container) container.style.display = "none";
            return;
        }
        if (container) container.style.display = "";
        var origensData = leadsByOrigem(filtered);
        var result = leadsByDiaOrigem(filtered, origensData);
        var ctx = document.getElementById("dc-chart-dia-origem").getContext("2d");

        var datasets = result.originKeys.map(function(ok) {
            return { label: ok.origem, data: result.days.map(function(d) { return d[ok.key] || 0; }), backgroundColor: ok.color, borderRadius: 4, maxBarThickness: 40 };
        });

        _chartDiaOrigem = new Chart(ctx, {
            type: "bar",
            data: { labels: result.days.map(function(d) { return d.dia; }), datasets: datasets },
            options: {
                responsive: true, maintainAspectRatio: false,
                plugins: { legend: { labels: { color: chartTextColor(), font: { size: 11 }, usePointStyle: true, pointStyle: "circle", padding: 12 } } },
                scales: {
                    x: { stacked: true, ticks: { color: chartTextColor(), font: { size: 11 } }, grid: { display: false } },
                    y: { stacked: true, ticks: { color: chartTextColor(), font: { size: 11 } }, grid: { color: chartGridColor() }, beginAtZero: true }
                }
            }
        });
    }

    // ── fetch data ────────────────────────────────────────────────────────

    function renderDetalheTable(filtered) {
        var rows = filtered.filter(function(r) { return r.total_vendas > 0; })
            .sort(function(a, b) {
                if (a.consultor < b.consultor) return -1;
                if (a.consultor > b.consultor) return 1;
                return b.total_vendas - a.total_vendas;
            });

        var tbody = document.querySelector("#dc-table-detalhe tbody");
        if (!tbody) return;

        if (rows.length === 0) {
            tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;padding:24px;color:var(--dc-text-muted)">Nenhuma matrícula encontrada no período</td></tr>';
            return;
        }

        // Agrega fechadas por consultor para mostrar na primeira linha de cada grupo
        var fechadasPorConsultor = {};
        rows.forEach(function(r) {
            if (!fechadasPorConsultor[r.consultor]) {
                var fp = _fechadasPeriodo[r.consultor] || { do_periodo: 0, fora_periodo: 0, total: 0 };
                fechadasPorConsultor[r.consultor] = {
                    do_periodo:   fp.do_periodo   || 0,
                    fora_periodo: fp.fora_periodo  || 0,
                    total:        fp.total         || 0
                };
            }
        });

        var html = "";
        var lastConsultor = "";
        rows.forEach(function(r) {
            var conv = r.total_leads > 0 ? (r.total_vendas / r.total_leads * 100) : 0;
            var isNew = r.consultor !== lastConsultor;
            lastConsultor = r.consultor;

            var badgeColor, badgeBg;
            if (conv >= 5) { badgeColor = "#10b981"; badgeBg = "rgba(16,185,129,0.12)"; }
            else if (conv >= 2) { badgeColor = "#f59e0b"; badgeBg = "rgba(245,158,11,0.12)"; }
            else { badgeColor = "#94a3b8"; badgeBg = "rgba(148,163,184,0.1)"; }

            var fechadasCell = "";
            var naoDistCell  = "";
            if (isNew) {
                var fc = fechadasPorConsultor[r.consultor] || { do_periodo: 0, fora_periodo: 0 };
                var fColor = fc.do_periodo > 0 ? "#10b981" : "#64748b";
                fechadasCell = '<span style="font-weight:700;color:' + fColor + '">' + fc.do_periodo + '</span>';
                var ndColor = fc.fora_periodo > 0 ? "#f59e0b" : "#64748b";
                naoDistCell  = '<span style="font-weight:700;color:' + ndColor + '">' + fc.fora_periodo + '</span>';
            }

            html += '<tr' + (isNew ? ' class="dc-row-highlight"' : '') + '>';
            html += '<td>' + (isNew ? r.consultor : '') + '</td>';
            html += '<td>' + r.origem + '</td>';
            html += '<td style="text-align:right">' + fmtNumber(r.total_leads) + '</td>';
            html += '<td style="text-align:right;font-weight:700">' + fmtNumber(r.total_vendas) + '</td>';
            html += '<td style="text-align:right">' + fechadasCell + '</td>';
            html += '<td style="text-align:right">' + naoDistCell + '</td>';
            html += '<td style="text-align:right"><span class="dc-badge-conv" style="color:' + badgeColor + ';background:' + badgeBg + '">' + conv.toFixed(2) + '%</span></td>';
            html += '</tr>';
        });
        tbody.innerHTML = html;
    }

    // ── Bloco 1: Tabela de Leads por Origem ───────────────────────────────

    function renderOrigemTable(filtered) {
        var tbody = document.querySelector("#dc-table-origem tbody");
        if (!tbody) return;

        // Ambas as colunas do webhook — mesma base, taxa de conversão consistente
        var g = {};
        filtered.forEach(function(r) {
            if (!g[r.origem]) g[r.origem] = { leads: 0, vendas: 0 };
            g[r.origem].leads  += r.total_leads;
            g[r.origem].vendas += r.total_vendas;
        });

        var data = Object.entries(g).map(function(e) {
            return {
                origem: e[0],
                leads:  e[1].leads,
                vendas: e[1].vendas,
                taxa:   e[1].leads > 0 ? (e[1].vendas / e[1].leads * 100) : 0
            };
        }).sort(function(a, b) { return b.taxa - a.taxa || b.leads - a.leads; });

        if (data.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;padding:24px;color:var(--dc-text-muted)">Sem dados</td></tr>';
            return;
        }

        var maxTaxa = Math.max.apply(null, data.map(function(d) { return d.taxa; })) || 1;

        var html = "";
        data.forEach(function(d) {
            var badgeColor, badgeBg;
            if (d.taxa >= 6)      { badgeColor = "#10b981"; badgeBg = "rgba(16,185,129,0.12)"; }
            else if (d.taxa >= 3) { badgeColor = "#3b82f6"; badgeBg = "rgba(59,130,246,0.12)"; }
            else if (d.taxa >= 1) { badgeColor = "#f59e0b"; badgeBg = "rgba(245,158,11,0.12)"; }
            else                  { badgeColor = "#94a3b8"; badgeBg = "rgba(148,163,184,0.08)"; }

            var barW = maxTaxa > 0 ? (d.taxa / maxTaxa * 100).toFixed(1) : 0;
            html += '<tr>';
            html += '<td style="font-weight:600">' + d.origem + '</td>';
            html += '<td style="text-align:right">' + fmtNumber(d.leads) + '</td>';
            html += '<td style="text-align:right;font-weight:700;color:#2563eb">' + fmtNumber(d.vendas) + '</td>';
            html += '<td style="text-align:right"><span class="dc-badge-conv" style="color:' + badgeColor + ';background:' + badgeBg + '">' + d.taxa.toFixed(2) + '%</span></td>';
            html += '<td><div style="background:var(--dc-border);border-radius:4px;height:8px;overflow:hidden"><div style="width:' + barW + '%;height:100%;background:' + badgeColor + ';border-radius:4px;transition:width 0.4s"></div></div></td>';
            html += '</tr>';
        });
        tbody.innerHTML = html;
    }

    // ── Bloco 2: Cards por Consultor (Dashboard Comercial) ────────────────

    function renderConsultorCards() {
        var container = document.getElementById("dc-consultor-cards");
        if (!container) return;

        var cf  = document.getElementById("dc-consultor-filter")?.value || "";
        var of_ = document.getElementById("dc-origem-filter")?.value   || "";

        var consultores;

        if (of_) {
            // Origem filtrada: agrega a partir do webhook (já filtrado por origem)
            var filtRows = getFiltered(true);
            var g = {};
            filtRows.forEach(function(r) {
                if (!g[r.consultor]) g[r.consultor] = 0;
                g[r.consultor] += r.total_vendas;
            });
            consultores = Object.entries(g)
                .map(function(e) { return { nome: e[0], total: e[1], do_periodo: e[1], fora_periodo: 0 }; })
                .filter(function(c) { return c.total > 0 && (!cf || c.nome === cf); })
                .sort(function(a, b) { return b.total - a.total; });
        } else {
            // Sem filtro de origem: usa _fechadasPeriodo (fonte do Dashboard Comercial)
            consultores = Object.entries(_fechadasPeriodo).map(function(e) {
                return {
                    nome:         e[0],
                    total:        e[1].total        || 0,
                    do_periodo:   e[1].do_periodo    || 0,
                    fora_periodo: e[1].fora_periodo  || 0
                };
            }).filter(function(c) {
                return c.total > 0 && (!cf || c.nome === cf);
            }).sort(function(a, b) { return b.total - a.total; });
        }

        if (consultores.length === 0) {
            container.innerHTML = '<p style="color:var(--dc-text-muted);font-size:13px">Sem dados de matrículas no período.</p>';
            return;
        }

        var sourceNote = of_
            ? '<p style="font-size:11px;color:var(--dc-text-muted);margin:0 0 12px;padding:6px 12px;background:rgba(37,99,235,0.07);border:1px solid rgba(37,99,235,0.15);border-radius:8px">'
              + 'Filtrado por origem <strong style="color:#60a5fa">' + of_ + '</strong> — valores baseados nas conversões do webhook.'
              + '</p>'
            : '';

        var html = sourceNote + consultores.map(function(c) {
            var pctDo   = c.total > 0 ? (c.do_periodo   / c.total * 100).toFixed(0) : 0;
            var pctFora = c.total > 0 ? (c.fora_periodo / c.total * 100).toFixed(0) : 0;
            var nomeEsc = c.nome.replace(/&/g,'&amp;').replace(/"/g,'&quot;');
            return '<div data-consultor="' + nomeEsc + '" class="dc-consultor-card" '
                + 'style="background:var(--dc-bg-main);border:1px solid var(--dc-border);border-radius:12px;padding:16px;cursor:pointer;transition:border-color 0.15s">'
                + '<div style="font-size:13px;font-weight:700;color:var(--dc-text-primary);margin-bottom:10px">' + c.nome + '</div>'
                + '<div style="font-size:28px;font-weight:800;color:var(--dc-text-primary);margin-bottom:10px">' + c.total + '</div>'
                + '<div style="display:flex;height:6px;border-radius:4px;overflow:hidden;margin-bottom:8px">'
                +   '<div style="width:' + pctDo + '%;background:#10b981"></div>'
                +   '<div style="width:' + pctFora + '%;background:#f59e0b"></div>'
                + '</div>'
                + '<div style="display:flex;gap:10px;font-size:11px">'
                +   (c.do_periodo > 0 ? '<span style="color:#10b981;font-weight:600">' + c.do_periodo + ' desta semana</span>' : '')
                +   (c.fora_periodo > 0 ? '<span style="color:#f59e0b;font-weight:600">' + c.fora_periodo + ' carteira/sem dist.</span>' : '')
                + '</div>'
                + '</div>';
        }).join('');
        container.innerHTML = html;

        // Attach click listeners via JS (more reliable than inline onclick)
        container.querySelectorAll('.dc-consultor-card').forEach(function(card) {
            card.addEventListener('mouseenter', function() { this.style.borderColor = '#3b82f6'; });
            card.addEventListener('mouseleave', function() { this.style.borderColor = 'var(--dc-border)'; });
            card.addEventListener('click', function() {
                var nome = this.getAttribute('data-consultor');
                dcAbrirDetalheInterno(nome);
            });
        });
    }

    // ── Conversão por Tipo de Origem (chart — mantido para compatibilidade) ─
    var _chartConvOrigem = null;

    function renderConversaoOrigem(filtered) {
        var canvas = document.getElementById("dc-chart-conv-origem");
        if (!canvas) return;

        // Agrega TODAS as origens (inclusive sem conversão)
        var g = {};
        filtered.forEach(function(r) {
            if (!g[r.origem]) g[r.origem] = { leads: 0, vendas: 0 };
            g[r.origem].leads  += r.total_leads;
            g[r.origem].vendas += r.total_vendas;
        });

        var data = Object.entries(g).map(function(e) {
            return {
                origem:   e[0],
                leads:    e[1].leads,
                vendas:   e[1].vendas,
                taxa:     e[1].leads > 0 ? (e[1].vendas / e[1].leads * 100) : 0
            };
        }).sort(function(a, b) { return b.taxa - a.taxa; });

        if (data.length === 0) {
            if (_chartConvOrigem) { _chartConvOrigem.destroy(); _chartConvOrigem = null; }
            return;
        }

        // Ajusta altura do canvas dinamicamente
        var wrap = document.getElementById("dc-conv-origem-wrap");
        var barH = 36, minH = 120;
        var h = Math.max(minH, data.length * barH + 60);
        if (wrap) wrap.style.height = h + "px";
        canvas.style.height = h + "px";

        var labels  = data.map(function(d) { return d.origem; });
        var taxas   = data.map(function(d) { return parseFloat(d.taxa.toFixed(2)); });
        var colors  = taxas.map(function(t) {
            if (t >= 6)  return "rgba(16,185,129,0.85)";
            if (t >= 3)  return "rgba(59,130,246,0.85)";
            if (t >= 1)  return "rgba(245,158,11,0.85)";
            return "rgba(100,116,139,0.55)";
        });

        if (_chartConvOrigem) { _chartConvOrigem.destroy(); _chartConvOrigem = null; }

        _chartConvOrigem = new Chart(canvas.getContext("2d"), {
            type: "bar",
            data: {
                labels: labels,
                datasets: [{
                    label: "Taxa de Conversão (%)",
                    data: taxas,
                    backgroundColor: colors,
                    borderRadius: 6,
                    barThickness: 24
                }]
            },
            options: {
                indexAxis: "y",
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: function(ctx) {
                                var d = data[ctx.dataIndex];
                                return [
                                    " Taxa: " + ctx.parsed.x.toFixed(2) + "%",
                                    " Matrículas: " + d.vendas,
                                    " Leads: " + d.leads
                                ];
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        beginAtZero: true,
                        grid:  { color: chartGridColor() },
                        ticks: { color: chartTextColor(), callback: function(v) { return v + "%"; } }
                    },
                    y: {
                        grid:  { display: false },
                        ticks: { color: chartTextColor(), font: { size: 12 } }
                    }
                },
                animation: { duration: 400 }
            },
            plugins: [{
                id: "convOrigemLabels",
                afterDatasetsDraw: function(chart) {
                    var ctx2 = chart.ctx;
                    chart.data.datasets[0].data.forEach(function(val, i) {
                        var meta = chart.getDatasetMeta(0);
                        var bar  = meta.data[i];
                        if (!bar) return;
                        var d = data[i];
                        ctx2.save();
                        ctx2.fillStyle = chartTextColor();
                        ctx2.font = "bold 11px Inter, sans-serif";
                        ctx2.textAlign  = "left";
                        ctx2.textBaseline = "middle";
                        var x = bar.x + 6;
                        var y = bar.y;
                        ctx2.fillText(val.toFixed(2) + "% (" + d.vendas + "/" + d.leads + ")", x, y);
                        ctx2.restore();
                    });
                }
            }]
        });
    }

    // ── fetch data ────────────────────────────────────────────────────────

    window.dcConsultorFetch = async function () {
        var btn = document.getElementById("dc-btn-fetch");

        var startDate = (document.getElementById('dc-date-start')?.value || '').trim();
        var endDate   = (document.getElementById('dc-date-end')?.value   || '').trim();
        if (!startDate || !endDate) {
            var n = new Date();
            endDate   = endDate   || fmtDateISO(n);
            var ago = new Date(n.getFullYear(), n.getMonth(), n.getDate() - 6);
            startDate = startDate || fmtDateISO(ago);
        }

        btn.disabled = true;
        btn.innerHTML = '<div class="dc-spinner"></div> Carregando...';
        document.getElementById("dc-alert-box").style.display = "none";
        document.getElementById("dc-raw-box").style.display = "none";

        var rawText = "";

        try {
            var body = JSON.stringify({ start_date: startDate, end_date: endDate });
            var resp = await fetch(WEBHOOK_URL, {
                method: "POST",
                headers: { "Content-Type": "application/json", "Accept": "application/json, text/plain, */*" },
                body: body
            });

            if (!resp.ok) {
                resp = await fetch(
                    WEBHOOK_URL + "?start_date=" + encodeURIComponent(startDate) + "&end_date=" + encodeURIComponent(endDate),
                    { method: "GET", headers: { "Accept": "application/json, text/plain, */*" } }
                );
            }

            if (!resp.ok) throw new Error("Falha ao consultar webhook: " + resp.status + " " + resp.statusText);

            rawText = await resp.text();
            var payload;
            try { payload = JSON.parse(rawText); } catch(e) { payload = rawText; }

            var allItems = flattenAll(payload);
            var rawTotals = computeRawTotals(allItems);
            _rawTotalVendas = rawTotals.totalVendas;
            _rawTotalLeads = rawTotals.totalLeads;
            _rows = mapRows(allItems);

            // Busca fechamentos do período em paralelo (por consultor e por origem)
            _fechadasPeriodo = {};
            _matriculasPorOrigem = [];
            var qs = 'start_date=' + encodeURIComponent(startDate) + '&end_date=' + encodeURIComponent(endDate);
            try {
                var [fechResp, matOrigResp] = await Promise.all([
                    fetch('/api/dist-consultor/fechadas-periodo?' + qs),
                    fetch('/api/dist-consultor/matriculas-por-origem?' + qs)
                ]);
                if (fechResp.ok) {
                    var fechJson = await fechResp.json();
                    if (fechJson.ok && Array.isArray(fechJson.data)) {
                        fechJson.data.forEach(function(item) {
                            _fechadasPeriodo[item.consultor] = {
                                do_periodo:   item.do_periodo   || 0,
                                fora_periodo: item.fora_periodo || 0,
                                total:        item.total        || 0
                            };
                        });
                    }
                }
                if (matOrigResp.ok) {
                    var matOrigJson = await matOrigResp.json();
                    if (matOrigJson.ok && Array.isArray(matOrigJson.data)) {
                        _matriculasPorOrigem = matOrigJson.data;
                        _rawTotalVendas = matOrigJson.total || _rawTotalVendas;
                    }
                }
            } catch (fe) {
                console.warn("Erro ao buscar fechadas/matriculas-por-origem:", fe);
            }

            document.getElementById("dc-last-update").textContent = "Última atualização: " + new Date().toLocaleString("pt-BR");

            if (_rows.length === 0) {
                showAlert("warn", "A webhook respondeu, mas o dashboard não encontrou linhas no formato esperado. Verifique o retorno bruto abaixo.");
                showRawPreview(rawText);
            }

            populateFilters();
            render();
        } catch (err) {
            showAlert("error", err.message || "Erro ao carregar os dados.");
            _rows = [];
            _rawTotalVendas = 0;
            _rawTotalLeads = 0;
            populateFilters();
            render();
        } finally {
            btn.disabled = false;
            btn.innerHTML = '<svg fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/></svg> Atualizar';
        }
    };

    function showAlert(type, msg) {
        var box = document.getElementById("dc-alert-box");
        box.style.display = "block";
        box.innerHTML = '<div class="dc-alert dc-alert-' + type + '">' + msg + '</div>';
    }

    function showRawPreview(text) {
        var box = document.getElementById("dc-raw-box");
        box.style.display = "block";
        document.getElementById("dc-raw-content").textContent = (text || "").slice(0, 3000);
    }

    // ── init ──────────────────────────────────────────────────────────────

    // ── Modal de detalhe por consultor ───────────────────────────────────

    function dcModalFecharInterno() {
        var el = document.getElementById('dc-modal-overlay');
        if (el) el.classList.remove('open');
    }

    window.dcModalFechar = dcModalFecharInterno;

    window.dcModalClose = function (e) {
        if (e.target === document.getElementById('dc-modal-overlay')) dcModalFecharInterno();
    };

    async function dcAbrirDetalheInterno(nome) {
        var overlay = document.getElementById('dc-modal-overlay');
        var body    = document.getElementById('dc-modal-body');
        var title   = document.getElementById('dc-modal-title');

        if (!overlay) { console.error('[dcz] #dc-modal-overlay não encontrado no DOM'); return; }

        // Move para o body para garantir que position:fixed seja relativo ao viewport
        if (overlay.parentNode !== document.body) {
            document.body.appendChild(overlay);
        }

        title.textContent = nome;
        body.innerHTML = '<div class="dc-modal-loading">Carregando...</div>';
        overlay.classList.add('open');

        var startDate = (document.getElementById('dc-date-start')?.value || '').trim();
        var endDate   = (document.getElementById('dc-date-end')?.value   || '').trim();
        var qs = 'consultor=' + encodeURIComponent(nome) +
                 '&start_date=' + encodeURIComponent(startDate) +
                 '&end_date='   + encodeURIComponent(endDate);

        try {
            var resp = await fetch('/api/dist-consultor/detalhe-consultor?' + qs);
            if (!resp.ok) throw new Error('HTTP ' + resp.status);
            var json = await resp.json();
            if (!json.ok) throw new Error(json.error || 'Erro desconhecido');

            body.innerHTML = renderModalBody(json);
        } catch (err) {
            body.innerHTML = '<div class="dc-modal-loading" style="color:#f87171">Erro: ' + err.message + '</div>';
        }
    }

    window.dcAbrirDetalhe = dcAbrirDetalheInterno;

    function renderModalBody(data) {
        function tabelaLeads(lista, tipo) {
            if (!lista.length) return '<p style="color:var(--dc-text-muted);font-size:12px;padding:8px 0">Nenhum lead nesta categoria.</p>';
            var rows = lista.map(function(l) {
                var distInfo = l.ultima_dist
                    ? l.ultima_dist + (l.dist_consultor ? ' <span style="color:var(--dc-text-muted)">(' + l.dist_consultor + ')</span>' : '')
                    : '<span style="color:var(--dc-text-muted)">Nunca dist.</span>';
                var kommoLink = '<a href="https://eduitbr.kommo.com/leads/detail/' + l.lead_id + '" target="_blank" '
                    + 'style="color:#60a5fa;text-decoration:none" title="Abrir no Kommo">'
                    + l.lead_id + ' ↗</a>';
                return '<tr>'
                    + '<td style="font-weight:600;color:var(--dc-text-primary)">' + (l.rgm || '—') + '</td>'
                    + '<td>' + (l.nome || '—') + '</td>'
                    + '<td>' + kommoLink + '</td>'
                    + '<td>' + (l.lead_criado || '—') + '</td>'
                    + '<td>' + (l.data_matricula || '—') + '</td>'
                    + '<td>' + distInfo + '</td>'
                    + '</tr>';
            }).join('');
            return '<div style="overflow-x:auto"><table class="dc-modal-table">'
                + '<thead><tr>'
                + '<th>RGM</th><th>Nome</th><th>Lead ID</th>'
                + '<th>Criado</th><th>Matrícula</th><th>Última dist.</th>'
                + '</tr></thead><tbody>' + rows + '</tbody></table></div>';
        }

        var total = data.do_periodo.length + data.fora_periodo.length;
        return '<div class="dc-modal-section-title" style="color:#10b981">'
            + '<span style="width:10px;height:10px;border-radius:50%;background:#10b981;display:inline-block"></span>'
            + 'Desta semana — ' + data.do_periodo.length + ' de ' + total
            + '</div>'
            + tabelaLeads(data.do_periodo, 'verde')
            + '<div class="dc-modal-section-title" style="color:#f59e0b;margin-top:8px">'
            + '<span style="width:10px;height:10px;border-radius:50%;background:#f59e0b;display:inline-block"></span>'
            + 'Carteira / sem dist. — ' + data.fora_periodo.length + ' de ' + total
            + '</div>'
            + tabelaLeads(data.fora_periodo, 'laranja');
    }

    window.loadDistConsultor = function () {
        if (!_filtersListening) {
            _filtersListening = true;
            document.getElementById("dc-consultor-filter").addEventListener("change", render);
            document.getElementById("dc-origem-filter").addEventListener("change", render);
            ['dc-date-start','dc-date-end'].forEach(function(id) {
                var el = document.getElementById(id);
                if (el) el.addEventListener('change', function() { dcPresetHighlight('custom'); });
            });

            // Modal: close on backdrop click or X button
            var overlay = document.getElementById('dc-modal-overlay');
            if (overlay) {
                overlay.addEventListener('click', function(e) {
                    if (e.target === overlay) dcModalFecharInterno();
                });
            }
            var btnClose = document.getElementById('dc-modal-close');
            if (btnClose) {
                btnClose.addEventListener('click', dcModalFecharInterno);
            }
        }

        if (!_loaded) {
            _loaded = true;
            dcDateInit();
            dcConsultorFetch();
        }
    };
})();
