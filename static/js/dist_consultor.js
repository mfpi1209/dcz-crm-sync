// ---------------------------------------------------------------------------
// Distribuição por Consultor — Dashboard (vanilla JS + Chart.js)
// ---------------------------------------------------------------------------

(function () {
    const WEBHOOK_URL = "https://n8n-new-n8n.ca31ey.easypanel.host/webhook/distribuicaoporconsultor-origens";
    const CHART_COLORS = ["#2563eb", "#0ea5e9", "#14b8a6", "#22c55e", "#eab308", "#f97316", "#ef4444", "#8b5cf6", "#06b6d4", "#84cc16"];
    const MONTHS = ['Janeiro','Fevereiro','Março','Abril','Maio','Junho','Julho','Agosto','Setembro','Outubro','Novembro','Dezembro'];
    const DP_PRESETS = [
        { id: 'all',       label: 'Tudo' },
        { id: 'today',     label: 'Hoje' },
        { id: 'yesterday', label: 'Ontem' },
        { id: '7d',        label: 'Últimos 7 dias' },
        { id: '30d',       label: 'Últimos 30 dias' },
        { id: 'thismonth', label: 'Este mês' },
        { id: 'lastmonth', label: 'Mês passado' },
        { id: 'custom',    label: 'Personalizado' },
    ];

    let _rows = [];
    let _chartConsultores = null;
    let _chartOrigens = null;
    let _chartDiaOrigem = null;
    let _loaded = false;
    let _filtersListening = false;

    // ── Date Picker State ─────────────────────────────────────────────────

    const now = new Date();
    let DC_DP = {
        preset: '7d', customStart: null, customEnd: null,
        startYear: now.getFullYear(), startMonth: now.getMonth(),
        endYear: now.getFullYear(),   endMonth: now.getMonth(),
        selecting: 'start', incluirHoje: true,
    };

    let DC_PICKER = {
        start: null, end: null, preset: '7d', incluirHoje: true, label: 'Últimos 7 dias',
    };

    // Init default range
    (function () {
        var r = dpPresetRange('7d', true);
        DC_PICKER.start = r.start;
        DC_PICKER.end = r.end;
    })();

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

    // ── Date Picker UI ────────────────────────────────────────────────────

    window.dcDpOpen = function (triggerEl) {
        DC_DP.preset      = DC_PICKER.preset;
        DC_DP.incluirHoje = DC_PICKER.incluirHoje;
        DC_DP.customStart = DC_PICKER.start;
        DC_DP.customEnd   = DC_PICKER.end;
        DC_DP.selecting   = 'start';
        var n = new Date();
        DC_DP.startYear  = DC_DP.customStart ? DC_DP.customStart.getFullYear() : n.getFullYear();
        DC_DP.startMonth = DC_DP.customStart ? DC_DP.customStart.getMonth()    : n.getMonth();
        DC_DP.endYear    = DC_DP.customEnd   ? DC_DP.customEnd.getFullYear()   : n.getFullYear();
        DC_DP.endMonth   = DC_DP.customEnd   ? DC_DP.customEnd.getMonth()      : n.getMonth();

        var popup = document.getElementById('dcDpPopup');
        var rect  = triggerEl.getBoundingClientRect();
        var isMobile = window.innerWidth <= 640;

        if (isMobile) {
            var estH = Math.min(window.innerHeight * 0.8, 560);
            var spaceBelow = window.innerHeight - rect.bottom - 8;
            popup.style.top = (spaceBelow >= estH || spaceBelow >= window.innerHeight * 0.45)
                ? (rect.bottom + 6) + 'px'
                : Math.max(8, rect.top - estH - 6) + 'px';
            popup.style.left = '12px';
        } else {
            popup.style.top  = (rect.bottom + 6) + 'px';
            popup.style.left = rect.left + 'px';
        }

        dcDpRender();
        document.getElementById('dcDpOverlay').classList.add('open');
        popup.classList.add('open');

        if (!isMobile) {
            setTimeout(function () {
                var pr = popup.getBoundingClientRect();
                if (pr.right > window.innerWidth - 12)
                    popup.style.left = Math.max(8, window.innerWidth - pr.width - 12) + 'px';
                if (pr.bottom > window.innerHeight - 12)
                    popup.style.top = (rect.top - pr.height - 6) + 'px';
            }, 0);
        }
    };

    window.dcDpClose = function () {
        document.getElementById('dcDpOverlay').classList.remove('open');
        document.getElementById('dcDpPopup').classList.remove('open');
    };

    function dcDpRender() {
        var popup = document.getElementById('dcDpPopup');
        var curPreset = DP_PRESETS.find(function(p) { return p.id === DC_DP.preset; }) || DP_PRESETS[0];
        var previewStart, previewEnd;

        if (DC_DP.preset === 'custom') {
            previewStart = DC_DP.customStart;
            previewEnd   = DC_DP.customEnd;
        } else {
            var r = dpPresetRange(DC_DP.preset, DC_DP.incluirHoje);
            previewStart = r.start;
            previewEnd   = r.end;
        }

        var presetSuffix = (DC_DP.preset==='7d'||DC_DP.preset==='30d'||DC_DP.preset==='thismonth')
            ? (DC_DP.incluirHoje ? ', até hoje' : ', sem hoje') : '';

        popup.innerHTML =
            '<div class="voc-dp-top">' +
                '<label class="voc-dp-include-today">' +
                    '<input type="checkbox" id="dcDpHoje" ' + (DC_DP.incluirHoje ? 'checked' : '') +
                    ' onchange="dcDpHojeToggle(this.checked)"> Incluir hoje' +
                '</label>' +
                '<div class="voc-dp-presets-wrap">' +
                    '<button class="voc-dp-preset-label" onclick="dcDpTogglePresets(event)">' +
                        '<span>' + curPreset.label + presetSuffix + '</span>' +
                        '<span class="material-symbols-outlined" style="font-size:18px">expand_more</span>' +
                    '</button>' +
                    '<div class="voc-dp-presets" id="dcDpPresets">' +
                        DP_PRESETS.map(function(p) {
                            return '<div class="voc-dp-preset-item' + (p.id === DC_DP.preset ? ' active' : '') +
                                '" onclick="dcDpSelectPreset(\'' + p.id + '\')">' + p.label + '</div>';
                        }).join('') +
                    '</div>' +
                '</div>' +
            '</div>' +
            '<div class="voc-dp-calendars">' +
                '<div class="voc-dp-cal">' +
                    '<div class="voc-dp-cal-label">Data de início</div>' +
                    dcDpCalHtml('start', DC_DP.startYear, DC_DP.startMonth, previewStart, previewEnd) +
                '</div>' +
                '<div class="voc-dp-cal">' +
                    '<div class="voc-dp-cal-label">Data de término</div>' +
                    dcDpCalHtml('end', DC_DP.endYear, DC_DP.endMonth, previewStart, previewEnd) +
                '</div>' +
            '</div>' +
            '<div class="voc-dp-footer">' +
                '<button class="voc-dp-btn voc-dp-btn-cancel" onclick="dcDpClose()">Cancelar</button>' +
                '<button class="voc-dp-btn voc-dp-btn-apply" onclick="dcDpApply()">Aplicar</button>' +
            '</div>';
    }
    window.dcDpRender = dcDpRender;

    function dcDpCalHtml(side, year, month, selStart, selEnd) {
        var DAYS = ['D','S','T','Q','Q','S','S'];
        var firstDow = new Date(year, month, 1).getDay();
        var lastDay  = new Date(year, month+1, 0).getDate();
        var n = new Date();
        var todayY = n.getFullYear(), todayM = n.getMonth(), todayD = n.getDate();

        var grid =
            '<div class="voc-dp-cal-header">' +
                '<button class="voc-dp-nav-btn" onclick="dcDpNav(\'' + side + '\',-1)">' +
                    '<span class="material-symbols-outlined" style="font-size:18px">chevron_left</span>' +
                '</button>' +
                '<span class="voc-dp-cal-title">' + MONTHS[month].slice(0,3).toUpperCase() + '. DE ' + year + '</span>' +
                '<button class="voc-dp-nav-btn" onclick="dcDpNav(\'' + side + '\',1)">' +
                    '<span class="material-symbols-outlined" style="font-size:18px">chevron_right</span>' +
                '</button>' +
            '</div>' +
            '<div class="voc-dp-weekdays">' + DAYS.map(function(d) { return '<div class="voc-dp-weekday">' + d + '</div>'; }).join('') + '</div>' +
            '<div class="voc-dp-days">';

        for (var i = 0; i < firstDow; i++) grid += '<div class="voc-dp-day other-month"></div>';

        for (var d = 1; d <= lastDay; d++) {
            var isToday = (year===todayY && month===todayM && d===todayD);
            var cls = 'voc-dp-day clickable';
            if (isToday) cls += ' today';

            if (selStart && selEnd) {
                var cur = new Date(year, month, d);
                var ss  = new Date(selStart.getFullYear(), selStart.getMonth(), selStart.getDate());
                var se  = new Date(selEnd.getFullYear(),   selEnd.getMonth(),   selEnd.getDate());
                if (+cur === +ss && +cur === +se) cls += ' selected';
                else if (+cur === +ss)            cls += ' range-start';
                else if (+cur === +se)            cls += ' range-end';
                else if (cur > ss && cur < se)   cls += ' in-range';
            } else if (selStart) {
                var cur2 = new Date(year, month, d);
                var ss2  = new Date(selStart.getFullYear(), selStart.getMonth(), selStart.getDate());
                if (+cur2 === +ss2) cls += ' selected';
            }

            grid += '<div class="' + cls + '" onclick="dcDpClickDay(' + year + ',' + month + ',' + d + ')">' + d + '</div>';
        }
        grid += '</div>';
        return grid;
    }

    window.dcDpNav = function (side, delta) {
        if (side === 'start') {
            DC_DP.startMonth += delta;
            if (DC_DP.startMonth > 11) { DC_DP.startMonth = 0; DC_DP.startYear++; }
            if (DC_DP.startMonth < 0)  { DC_DP.startMonth = 11; DC_DP.startYear--; }
        } else {
            DC_DP.endMonth += delta;
            if (DC_DP.endMonth > 11) { DC_DP.endMonth = 0; DC_DP.endYear++; }
            if (DC_DP.endMonth < 0)  { DC_DP.endMonth = 11; DC_DP.endYear--; }
        }
        dcDpRender();
    };

    window.dcDpClickDay = function (year, month, day) {
        if (DC_DP.preset !== 'custom') {
            DC_DP.preset = 'custom';
            DC_DP.customStart = null;
            DC_DP.customEnd   = null;
            DC_DP.selecting   = 'start';
        }
        var date = new Date(year, month, day);
        if (!DC_DP.customStart || DC_DP.customEnd) {
            DC_DP.customStart = date; DC_DP.customEnd = null; DC_DP.selecting = 'end';
        } else {
            if (date < DC_DP.customStart) { DC_DP.customEnd = DC_DP.customStart; DC_DP.customStart = date; }
            else { DC_DP.customEnd = new Date(year, month, day, 23, 59, 59, 999); }
            DC_DP.selecting = 'start';
        }
        dcDpRender();
    };

    window.dcDpSelectPreset = function (id) {
        DC_DP.preset = id;
        if (id === 'custom') { DC_DP.customStart = null; DC_DP.customEnd = null; DC_DP.selecting = 'start'; }
        var el = document.getElementById('dcDpPresets');
        if (el) el.classList.remove('open');
        dcDpRender();
    };

    window.dcDpTogglePresets = function (e) {
        e.stopPropagation();
        var el = document.getElementById('dcDpPresets');
        if (el) el.classList.toggle('open');
    };

    window.dcDpHojeToggle = function (checked) {
        DC_DP.incluirHoje = checked;
        dcDpRender();
    };

    window.dcDpApply = function () {
        var start, end;
        if (DC_DP.preset === 'custom') {
            start = DC_DP.customStart;
            end   = DC_DP.customEnd || (DC_DP.customStart
                ? new Date(DC_DP.customStart.getFullYear(), DC_DP.customStart.getMonth(), DC_DP.customStart.getDate(), 23,59,59,999)
                : null);
        } else {
            var r = dpPresetRange(DC_DP.preset, DC_DP.incluirHoje);
            start = r.start; end = r.end;
        }

        DC_PICKER = {
            start: start, end: end,
            preset: DC_DP.preset,
            incluirHoje: DC_DP.incluirHoje,
            label: dpLabel(DC_DP.preset, DC_DP.customStart, DC_DP.customEnd, DC_DP.incluirHoje)
        };

        document.getElementById('dc-date-label').textContent = DC_PICKER.label;
        dcDpClose();
        dcConsultorFetch();
    };

    // ── helpers ────────────────────────────────────────────────────────────

    function fmtDateISO(d) {
        return d.getFullYear() + '-' +
            String(d.getMonth() + 1).padStart(2, '0') + '-' +
            String(d.getDate()).padStart(2, '0');
    }

    function fmtDateDisplay(val) {
        if (!val) return "Sem data";
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

    function normalizeRows(payload) {
        var candidates = [
            payload, payload?.data, payload?.items, payload?.result,
            payload?.rows, payload?.results, payload?.message
        ];
        for (var ci = 0; ci < candidates.length; ci++) {
            var c = candidates[ci];
            if (Array.isArray(c))
                return c.map(function(i) { return (i?.json && typeof i.json === "object") ? i.json : i; });
        }
        if (typeof payload === "string") {
            try { return normalizeRows(JSON.parse(payload)); } catch(e) { return []; }
        }
        if (payload && typeof payload === "object") {
            var arrs = Object.values(payload).filter(Array.isArray);
            if (arrs.length)
                return arrs[0].map(function(i) { return (i?.json && typeof i.json === "object") ? i.json : i; });
        }
        return [];
    }

    function mapApiRows(raw) {
        return raw.flatMap(function (item, idx) {
            if (item?.origem !== undefined && (item?.total_leads !== undefined || item?.total !== undefined)) {
                return [{
                    id: (item?.dia || "sem-dia") + "-" + (item?.consultor || "sem-consultor") + "-" + (item?.origem || "sem-origem") + "-" + idx,
                    diaRaw: item?.dia,
                    dia: fmtDateDisplay(item?.dia),
                    consultor: safe(item?.consultor, "Sem consultor"),
                    id_consultor: item?.id_consultor ?? "-",
                    origem: safe(item?.origem, "Sem origem"),
                    total_leads: toNum(item?.total_leads ?? item?.total),
                    total_dia_origem: toNum(item?.total_dia_origem)
                }];
            }
            var skipKeys = ["dia", "consultor", "id_consultor", "total_geral", "id"];
            return Object.keys(item || {})
                .filter(function(k) { return !skipKeys.includes(k) && k !== "outras_origens"; })
                .map(function(k) {
                    return {
                        id: (item?.dia || "sem-dia") + "-" + (item?.consultor || "sem-consultor") + "-" + k + "-" + idx,
                        diaRaw: item?.dia,
                        dia: fmtDateDisplay(item?.dia),
                        consultor: safe(item?.consultor, "Sem consultor"),
                        id_consultor: item?.id_consultor ?? "-",
                        origem: safe(k, "Sem origem"),
                        total_leads: toNum(item?.[k]),
                        total_dia_origem: 0
                    };
                })
                .filter(function(r) { return r.total_leads > 0; });
        });
    }

    // ── filter + compute ──────────────────────────────────────────────────

    function getFiltered() {
        var cf = document.getElementById("dc-consultor-filter")?.value || "";
        var of_ = document.getElementById("dc-origem-filter")?.value || "";
        return _rows.filter(function(r) {
            if (cf && r.consultor !== cf) return false;
            if (of_ && r.origem !== of_) return false;
            return true;
        });
    }

    function computeSummary(filtered) {
        var totalLeads = filtered.reduce(function(a, r) { return a + r.total_leads; }, 0);
        var consultores = new Set(filtered.map(function(r) { return r.consultor; })).size;
        var origens = new Set(filtered.map(function(r) { return r.origem; })).size;
        var dias = new Set(filtered.map(function(r) { return r.dia; })).size;
        return { totalLeads: totalLeads, consultores: consultores, origens: origens, mediaPorDia: dias ? totalLeads / dias : 0 };
    }

    function leadsByConsultor(filtered) {
        var g = {};
        filtered.forEach(function(r) { g[r.consultor] = (g[r.consultor] || 0) + r.total_leads; });
        return Object.entries(g).map(function(e) { return { consultor: e[0], total: e[1] }; })
            .sort(function(a, b) { return b.total - a.total; }).slice(0, 10);
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
            var ck = keyMap[r.origem];
            if (!ck) return;
            if (!grouped.has(r.dia)) grouped.set(r.dia, { dia: r.dia, diaRaw: r.diaRaw });
            var cur = grouped.get(r.dia);
            cur[ck] = (cur[ck] || 0) + r.total_leads;
        });

        var days = Array.from(grouped.values()).sort(function(a, b) { return new Date(a.diaRaw).getTime() - new Date(b.diaRaw).getTime(); });
        return { days: days, originKeys: originKeys };
    }

    // ── populate filters ──────────────────────────────────────────────────

    function populateFilters() {
        var cSel = document.getElementById("dc-consultor-filter");
        var oSel = document.getElementById("dc-origem-filter");
        var cVal = cSel.value, oVal = oSel.value;

        var consultores = [...new Set(_rows.map(function(r) { return r.consultor; }))].sort();
        var origens = [...new Set(_rows.map(function(r) { return r.origem; }))].sort();

        cSel.innerHTML = '<option value="">Todos</option>' +
            consultores.map(function(c) { return '<option value="' + c + '">' + c + '</option>'; }).join('');
        oSel.innerHTML = '<option value="">Todas</option>' +
            origens.map(function(o) { return '<option value="' + o + '">' + o + '</option>'; }).join('');

        if (consultores.includes(cVal)) cSel.value = cVal;
        if (origens.includes(oVal)) oSel.value = oVal;
    }

    // ── render charts ─────────────────────────────────────────────────────

    function isDark() { return document.documentElement.classList.contains('dark'); }
    function chartTextColor() { return isDark() ? '#94a3b8' : '#475569'; }
    function chartGridColor() { return isDark() ? 'rgba(51,65,85,0.4)' : '#e2e8f0'; }

    function render() {
        var filtered = getFiltered();
        var s = computeSummary(filtered);

        document.getElementById("dc-m-leads").textContent = fmtNumber(s.totalLeads);
        document.getElementById("dc-m-consultores").textContent = fmtNumber(s.consultores);
        document.getElementById("dc-m-origens").textContent = fmtNumber(s.origens);
        document.getElementById("dc-m-media").textContent = fmtNumber(s.mediaPorDia.toFixed(1));

        renderBarConsultores(filtered);
        renderPieOrigens(filtered);
        renderStackedDiaOrigem(filtered);
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

    function renderStackedDiaOrigem(filtered) {
        var origensData = leadsByOrigem(filtered);
        var result = leadsByDiaOrigem(filtered, origensData);
        if (_chartDiaOrigem) _chartDiaOrigem.destroy();
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

    window.dcConsultorFetch = async function () {
        var btn = document.getElementById("dc-btn-fetch");

        var startDate, endDate;
        if (DC_PICKER.start) {
            startDate = fmtDateISO(DC_PICKER.start);
        } else {
            var ago = new Date(); ago.setDate(ago.getDate() - 6);
            startDate = fmtDateISO(ago);
        }
        if (DC_PICKER.end) {
            endDate = fmtDateISO(DC_PICKER.end);
        } else {
            endDate = fmtDateISO(new Date());
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

            var rawRows = normalizeRows(payload);
            _rows = mapApiRows(rawRows);

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

    window.loadDistConsultor = function () {
        if (!_filtersListening) {
            _filtersListening = true;
            document.getElementById("dc-consultor-filter").addEventListener("change", render);
            document.getElementById("dc-origem-filter").addEventListener("change", render);
        }

        if (!_loaded) {
            _loaded = true;
            dcConsultorFetch();
        }
    };
})();
