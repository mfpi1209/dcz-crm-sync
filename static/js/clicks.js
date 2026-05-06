// ---------------------------------------------------------------------------
// QR Codes (Clicks) — Short.io webhook
// ---------------------------------------------------------------------------
const CLK_WEBHOOK_URL = 'https://n8n-new-n8n.ca31ey.easypanel.host/webhook/clicks_shortio';

const MESES_ABR = ['JAN','FEV','MAR','ABR','MAI','JUN','JUL','AGO','SET','OUT','NOV','DEZ'];

const clkState = {
    startView: { year: new Date().getFullYear(), month: new Date().getMonth() },
    endView: { year: new Date().getFullYear(), month: new Date().getMonth() },
    selectedStart: null,
    selectedEnd: null,
    startDate: '',
    endDate: '',
    allRows: [],
    filteredRows: [],
    campanhas: [],
    initialized: false
};

function clkPad(n) { return String(n).padStart(2, '0'); }

function clkFormatDateBR(dateStr) {
    const [y, m, d] = dateStr.split('-');
    return `${d}/${m}/${y}`;
}

function clkFormatDateClick(dateStr) {
    if (!dateStr) return '-';
    const base = String(dateStr).slice(0, 10);
    const [y, m, d] = base.split('-');
    if (!y || !m || !d) return dateStr;
    return `${d}/${m}/${y}`;
}

function clkFormatNumber(v) {
    return Number(v || 0).toLocaleString('pt-BR');
}

function clkNormalizeRows(data) {
    if (Array.isArray(data)) return data;
    if (data && typeof data === 'object') {
        if (Array.isArray(data.data)) return data.data;
        if (Array.isArray(data.result)) return data.result;
        if (Array.isArray(data.rows)) return data.rows;
    }
    return [];
}

function clkAbrirCalendario() {
    if (clkState.selectedStart) {
        const [y, m] = clkState.selectedStart.split('-');
        clkState.startView = { year: parseInt(y), month: parseInt(m) - 1 };
    }
    if (clkState.selectedEnd) {
        const [y, m] = clkState.selectedEnd.split('-');
        clkState.endView = { year: parseInt(y), month: parseInt(m) - 1 };
    }
    document.getElementById('clk-cal-overlay').classList.add('open');
    clkRenderCalendars();
}

function clkFecharCalendario() {
    document.getElementById('clk-cal-overlay').classList.remove('open');
}

function clkNavMonth(type, dir) {
    const view = type === 'start' ? clkState.startView : clkState.endView;
    view.month += dir;
    if (view.month < 0) { view.month = 11; view.year--; }
    if (view.month > 11) { view.month = 0; view.year++; }
    clkRenderCalendars();
}

function clkRenderCalendars() {
    clkRenderMonth('start', clkState.startView, clkState.selectedStart);
    clkRenderMonth('end', clkState.endView, clkState.selectedEnd);
}

function clkRenderMonth(type, view, selected) {
    const hoje = new Date();
    const todayStr = `${hoje.getFullYear()}-${clkPad(hoje.getMonth()+1)}-${clkPad(hoje.getDate())}`;
    const firstDay = new Date(view.year, view.month, 1).getDay();
    const daysInMonth = new Date(view.year, view.month + 1, 0).getDate();

    document.getElementById(`clk-cal-${type}-label`).textContent = `${MESES_ABR[view.month]}. DE ${view.year}`;

    let html = '';
    for (let i = 0; i < firstDay; i++) html += '<span class="empty"></span>';
    for (let d = 1; d <= daysInMonth; d++) {
        const dateStr = `${view.year}-${clkPad(view.month+1)}-${clkPad(d)}`;
        let cls = '';
        if (selected === dateStr) cls = 'selected';
        else if (dateStr === todayStr) cls = 'today';
        html += `<span class="${cls}" onclick="clkSelectDay('${type}','${dateStr}')">${d}</span>`;
    }
    document.getElementById(`clk-cal-${type}-days`).innerHTML = html;
}

function clkSelectDay(type, dateStr) {
    if (type === 'start') clkState.selectedStart = dateStr;
    else clkState.selectedEnd = dateStr;
    clkRenderCalendars();
}

function clkAplicarDatas() {
    const hoje = new Date();
    const t = `${hoje.getFullYear()}-${clkPad(hoje.getMonth()+1)}-${clkPad(hoje.getDate())}`;
    const incluirHoje = document.getElementById('clk-chk-hoje').checked;

    let s = clkState.selectedStart;
    let e = clkState.selectedEnd;
    if (incluirHoje && !e) e = t;
    if (incluirHoje && !s) s = t;

    if (s) clkState.startDate = s;
    if (e) clkState.endDate = e;

    if (s && e) {
        document.getElementById('clk-date-display').textContent = `${clkFormatDateBR(s)}  →  ${clkFormatDateBR(e)}`;
    } else if (s) {
        document.getElementById('clk-date-display').textContent = `${clkFormatDateBR(s)}  →  ...`;
    }

    clkFecharCalendario();
}

async function clkEnviarConsulta() {
    const statusEl = document.getElementById('clk-status-msg');
    const badge = document.getElementById('clk-badge');

    if (!clkState.startDate || !clkState.endDate) {
        statusEl.textContent = 'Selecione o período no calendário.';
        statusEl.className = 'clk-status-msg error';
        return;
    }
    if (clkState.startDate > clkState.endDate) {
        statusEl.textContent = 'A data inicial não pode ser maior que a data final.';
        statusEl.className = 'clk-status-msg error';
        return;
    }

    const btn = document.getElementById('clk-btn-submit');
    btn.disabled = true;
    btn.innerHTML = '<span class="material-symbols-outlined text-base animate-spin">progress_activity</span> CONSULTANDO...';
    statusEl.className = 'clk-status-msg';
    badge.className = 'clk-badge clk-badge-loading';
    badge.textContent = 'Consultando...';
    document.getElementById('clk-results').classList.remove('show');

    try {
        const response = await fetch(CLK_WEBHOOK_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ start_date: clkState.startDate, end_date: clkState.endDate })
        });
        const ct = response.headers.get('content-type') || '';
        const data = ct.includes('application/json') ? await response.json() : await response.text();
        if (!response.ok) throw new Error(typeof data === 'string' ? data : JSON.stringify(data));

        statusEl.textContent = 'Consulta enviada com sucesso.';
        statusEl.className = 'clk-status-msg success';
        badge.className = 'clk-badge clk-badge-ok';
        badge.textContent = 'Concluído';

        if (typeof data === 'string') {
            clkState.allRows = [];
            clkState.filteredRows = [];
            clkState.campanhas = [];
        } else {
            const rows = clkNormalizeRows(data);
            const camps = [...new Set(rows.map(r => r.campanha || r.campaign || 'Sem campanha'))].sort();
            clkState.allRows = rows;
            clkState.filteredRows = rows;
            clkState.campanhas = camps;
        }

        clkRenderResults();
        clkUpdateResumo();
        document.getElementById('clk-results').classList.add('show');

    } catch (error) {
        statusEl.textContent = `Erro ao enviar para a webhook: ${error.message}`;
        statusEl.className = 'clk-status-msg error';
        badge.className = 'clk-badge clk-badge-err';
        badge.textContent = 'Erro';
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<span class="material-symbols-outlined text-base">search</span> CONSULTAR';
    }
}

function clkUpdateResumo() {
    const rows = clkState.filteredRows;
    const totalCliques = rows.reduce((s, r) => s + Number(r.total_cliques ?? r.total ?? r.cliques ?? 0), 0);
    const uniqueCamps = new Set(rows.map(r => r.campanha || r.campaign || 'Sem campanha')).size;

    document.getElementById('clk-res-registros').textContent = clkFormatNumber(rows.length);
    document.getElementById('clk-res-cliques').textContent = clkFormatNumber(totalCliques);
    document.getElementById('clk-res-campanhas').textContent = clkFormatNumber(uniqueCamps);
}

function clkRenderResults() {
    const filter = document.getElementById('clk-filter');
    filter.innerHTML = '<option value="">Todas as campanhas</option>' +
        clkState.campanhas.map(c => `<option value="${c}">${c}</option>`).join('');

    clkRenderTable();
}

function clkApplyFilter() {
    const val = document.getElementById('clk-filter').value;
    clkState.filteredRows = val
        ? clkState.allRows.filter(r => (r.campanha || r.campaign || 'Sem campanha') === val)
        : clkState.allRows;
    clkRenderTable();
    clkUpdateResumo();
}

function clkRenderTable() {
    const tbody = document.getElementById('clk-table-body');
    const empty = document.getElementById('clk-empty-results');
    const rows = clkState.filteredRows;

    if (rows.length === 0) {
        tbody.innerHTML = '';
        empty.style.display = 'block';
        return;
    }

    empty.style.display = 'none';
    tbody.innerHTML = rows.map(row => `
        <tr>
            <td>${clkFormatDateClick(row.dia || row.date || row.created_at)}</td>
            <td><span class="clk-badge-camp">${row.campanha || row.campaign || 'Sem campanha'}</span></td>
            <td style="font-weight:700">${clkFormatNumber(row.total_cliques ?? row.total ?? row.cliques)}</td>
        </tr>
    `).join('');
}

function loadClicks() {
    if (!clkState.initialized) {
        clkState.initialized = true;
    }
    clkRenderCalendars();
}
