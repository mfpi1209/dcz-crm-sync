// ---------------------------------------------------------------------------
// IA Comercial — Dashboard de monitoramento do agente de IA.
// Lê direto da tabela `mensagens_ia` no Supabase principal via REST.
// ---------------------------------------------------------------------------
const IAC_SUPABASE_URL = 'https://fcwuhwedretyomtrbgzb.supabase.co';
const IAC_SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZjd3Vod2VkcmV0eW9tdHJiZ3piIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTA3OTI0NTAsImV4cCI6MjA2NjM2ODQ1MH0.IbDvSLmrg_ihyCZMhpDDeA6-solYN-2RhcY8PCHzc6I';

const iacState = {
    executions: [],
    dtIni: null,
    dtFim: null,
    preset: 7,
    verTodosErros: false,
    loading: false,
    chartMsgs: null,
    chartTopicos: null,
    initialized: false,
};

const IAC_TOPIC_LABELS = {
    buscar_precos: 'Pediu preço',
    buscar_informacoes: 'Pediu informações do curso',
    buscar_pos: 'Pediu pós-graduação',
    buscar_perguntas: 'Fez uma pergunta (FAQ)',
    localizacao: 'Pediu polo / localização',
    inscricao: 'Inscrição / matrícula',
    distribuir_humano: 'Distribuição para humano',
};

const IAC_TOPIC_COLORS = {
    'Pediu preço': '#f472b6',
    'Pediu informações do curso': '#34d399',
    'Pediu pós-graduação': '#c084fc',
    'Fez uma pergunta (FAQ)': '#fbbf24',
    'Pediu polo / localização': '#38bdf8',
    'Inscrição / matrícula': '#f87171',
    'Distribuição para humano': '#94a3b8',
};

const IAC_FALLBACK_PALETTE = ['#a78bfa', '#fb923c', '#22d3ee', '#facc15', '#fb7185', '#4ade80', '#60a5fa'];

const IAC_TOOL_LABELS = {
    buscar_informacoes: 'Buscar Informações',
    buscar_precos: 'Buscar Preços',
    buscar_perguntas: 'Buscar Perguntas',
    buscar_pos: 'Buscar Pós',
    localizacao: 'Localização',
    distribuir_humano: 'Distribuir humano',
    inscricao: 'Inscrição',
    buscar_historico_conversa: 'Histórico conversa',
};

// ---------------------------------------------------------------------------
// Utils
// ---------------------------------------------------------------------------
function iacToInputDate(date) { return date.toISOString().slice(0, 10); }

function iacFromInputDate(s) {
    if (!s) return null;
    const d = new Date(s + 'T00:00:00');
    return isNaN(d.getTime()) ? null : d;
}

function iacDaysBetween(start, end) {
    const s = new Date(start); s.setHours(0,0,0,0);
    const e = new Date(end); e.setHours(0,0,0,0);
    return Math.round((e - s) / 86400000) + 1;
}

function iacGetDayLabel(date) {
    return date.toLocaleDateString('pt-BR', { weekday: 'short', day: '2-digit', month: '2-digit' }).replace('.', '');
}

function iacFormatBRL(value) {
    return Number(value || 0).toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
}

function iacFormatTokens(n) {
    n = Number(n) || 0;
    if (n >= 1_000_000) return (n / 1_000_000).toFixed(2) + 'M';
    if (n >= 1_000) return (n / 1_000).toFixed(2) + 'k';
    return String(n);
}

function iacFormatInt(n) {
    return Number(n || 0).toLocaleString('pt-BR');
}

function iacEsc(s) {
    const el = document.createElement('span');
    el.textContent = s == null ? '' : String(s);
    return el.innerHTML;
}

function iacFmtDateTime(iso) {
    if (!iso) return '—';
    try {
        const d = new Date(iso);
        return d.toLocaleString('pt-BR', { day: '2-digit', month: '2-digit', year: 'numeric', hour: '2-digit', minute: '2-digit' });
    } catch (e) { return iso; }
}

function fingerprintError(err) {
    return String(err || '')
        .split('\n')[0]
        .replace(/\b\d+\b/g, 'N')
        .replace(/EX-[\w-]+/g, 'EX-X')
        .replace(/\s+/g, ' ')
        .trim()
        .slice(0, 200);
}

function relativeTime(iso) {
    if (!iso) return '-';
    const diffMs = Date.now() - new Date(iso).getTime();
    const min = Math.floor(diffMs / 60000);
    if (min < 1) return 'agora';
    if (min < 60) return `há ${min}min`;
    const h = Math.floor(min / 60);
    if (h < 24) return `há ${h}h`;
    const d = Math.floor(h / 24);
    return `há ${d}d`;
}

// ---------------------------------------------------------------------------
// Boot / nav
// ---------------------------------------------------------------------------
function loadIaComercial() {
    if (!iacState.initialized) {
        _iacInit();
        iacState.initialized = true;
    }
    iacRefresh();
}

function _iacInit() {
    const today = new Date();
    const start = new Date();
    start.setDate(today.getDate() - 6);
    iacState.dtIni = iacToInputDate(start);
    iacState.dtFim = iacToInputDate(today);
    iacState.preset = 7;

    const ini = document.getElementById('iac-dt-ini');
    const fim = document.getElementById('iac-dt-fim');
    if (ini) ini.value = iacState.dtIni;
    if (fim) fim.value = iacState.dtFim;

    document.querySelectorAll('#iac-presets .iac-preset-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            const days = parseInt(btn.dataset.preset, 10);
            iacApplyPreset(days);
        });
    });

    if (ini) ini.addEventListener('change', () => { iacState.dtIni = ini.value; iacState.preset = null; iacSyncPresetUI(); iacRefresh(); });
    if (fim) fim.addEventListener('change', () => { iacState.dtFim = fim.value; iacState.preset = null; iacSyncPresetUI(); iacRefresh(); });

    const btn = document.getElementById('iac-refresh-btn');
    if (btn) btn.addEventListener('click', iacRefresh);

    iacSyncPresetUI();
}

function iacApplyPreset(days) {
    const today = new Date();
    const start = new Date();
    start.setDate(today.getDate() - Math.max(0, days - 1));
    if (days === 0) start.setTime(today.getTime());
    iacState.dtIni = iacToInputDate(days === 0 ? today : start);
    iacState.dtFim = iacToInputDate(today);
    iacState.preset = days;
    const ini = document.getElementById('iac-dt-ini');
    const fim = document.getElementById('iac-dt-fim');
    if (ini) ini.value = iacState.dtIni;
    if (fim) fim.value = iacState.dtFim;
    iacSyncPresetUI();
    iacRefresh();
}

function iacSyncPresetUI() {
    document.querySelectorAll('#iac-presets .iac-preset-btn').forEach(btn => {
        const v = parseInt(btn.dataset.preset, 10);
        btn.classList.toggle('is-active', v === iacState.preset);
    });
}

// ---------------------------------------------------------------------------
// Fetch
// ---------------------------------------------------------------------------
async function iacRefresh() {
    if (iacState.loading) return;
    iacState.loading = true;

    const btn = document.getElementById('iac-refresh-btn');
    if (btn) { btn.disabled = true; btn.style.opacity = '0.6'; }

    iacRenderRangeLabel();

    try {
        const url = `${IAC_SUPABASE_URL}/rest/v1/mensagens_ia`
            + `?select=id,created_at,user_message,model,steps,tool_calls,response,error,total_duration_ms,usage`
            + `&created_at=gte.${encodeURIComponent(iacState.dtIni + 'T00:00:00')}`
            + `&created_at=lte.${encodeURIComponent(iacState.dtFim + 'T23:59:59.999')}`
            + `&order=created_at.desc&limit=500`;
        const res = await fetch(url, {
            headers: {
                apikey: IAC_SUPABASE_KEY,
                Authorization: `Bearer ${IAC_SUPABASE_KEY}`,
            },
        });
        if (!res.ok) {
            const errText = await res.text();
            console.error('[IA Comercial] fetch failed', res.status, errText);
            iacShowError(`Não consegui acessar a tabela mensagens_ia (HTTP ${res.status}). Verifique se RLS permite SELECT pro role anon.`);
            iacState.executions = [];
            iacRender();
            return;
        }
        const rows = await res.json();
        iacState.executions = (Array.isArray(rows) ? rows : []).map((r) => {
            const usage = r.usage || {};
            const aiMeta = usage && typeof usage === 'object' ? (usage._meta || null) : null;
            return {
                id: r.id,
                timestamp: r.created_at,
                userMessage: r.user_message,
                model: r.model,
                steps: r.steps,
                toolCalls: r.tool_calls,
                response: r.response,
                error: r.error,
                totalDurationMs: r.total_duration_ms,
                usage,
                aiMeta,
            };
        });
        iacRender();
    } catch (e) {
        console.error('[IA Comercial] refresh', e);
        iacShowError('Erro ao buscar dados. Veja o console.');
    } finally {
        iacState.loading = false;
        if (btn) { btn.disabled = false; btn.style.opacity = '1'; }
    }
}

function iacShowError(msg) {
    if (typeof toast === 'function') toast(msg, 'error', 6000);
}

// ---------------------------------------------------------------------------
// Render
// ---------------------------------------------------------------------------
function iacRender() {
    iacRenderRangeLabel();
    iacRenderKPIs();
    iacRenderCustoComponente();
    const errorData = iacGroupErrors(iacState.executions);
    iacRenderBanner(errorData);
    iacRenderErros(errorData);
    iacRenderChartMsgs();
    iacRenderChartTopicos();
    iacRenderTools();
}

function iacRenderRangeLabel() {
    const lbl = document.getElementById('iac-range-label');
    const tot = document.getElementById('iac-total-label');
    if (lbl) {
        const a = iacState.dtIni ? iacState.dtIni.split('-').reverse().join('/') : '—';
        const b = iacState.dtFim ? iacState.dtFim.split('-').reverse().join('/') : '—';
        lbl.textContent = `${a} — ${b}`;
    }
    if (tot) {
        const n = iacState.executions.length;
        tot.textContent = `${iacFormatInt(n)} mensagem${n === 1 ? '' : 's'}`;
    }
}

// ---------- KPIs ----------
function iacRenderKPIs() {
    const wrap = document.getElementById('iac-kpis');
    if (!wrap) return;

    const execs = iacState.executions;
    const total = execs.length;
    let tokens = 0, custo = 0, durMs = 0, durN = 0, errCount = 0;
    for (const e of execs) {
        const u = e.usage || {};
        tokens += Number(u.total_tokens) || 0;
        custo += iacCalcCost(u, e.model, e.aiMeta);
        if (e.totalDurationMs != null && !isNaN(e.totalDurationMs)) {
            durMs += Number(e.totalDurationMs);
            durN++;
        }
        if (e.error) errCount++;
    }
    const avgMs = durN > 0 ? durMs / durN : 0;
    const avgSec = (avgMs / 1000);
    const errPct = total > 0 ? ((errCount / total) * 100) : 0;

    const cards = [
        { icon: 'chat', title: 'Mensagens', value: iacFormatInt(total), sub: '' },
        { icon: 'bolt', title: 'Tokens usados', value: iacFormatTokens(tokens), sub: 'Total de tokens consumidos' },
        { icon: 'attach_money', title: 'Custo estimado', value: iacFormatBRL(custo), sub: 'Soma de todos os componentes' },
        { icon: 'schedule', title: 'Tempo médio', value: `${avgSec.toFixed(1)}s`, sub: '' },
        { icon: 'warning', title: 'Erros', value: iacFormatInt(errCount), sub: total ? `${errPct.toFixed(1)}% do total` : '' },
    ];

    wrap.innerHTML = cards.map(c => `
        <div class="glass-card border border-[var(--border)] rounded-xl p-5">
            <div class="flex items-center gap-2 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider mb-3">
                <span class="material-symbols-outlined text-base">${c.icon}</span>
                ${iacEsc(c.title)}
            </div>
            <p class="text-3xl font-black text-[var(--text-primary)] dark:text-white tabular-nums">${iacEsc(c.value)}</p>
            ${c.sub ? `<p class="text-xs text-slate-500 dark:text-slate-400 mt-1">${iacEsc(c.sub)}</p>` : ''}
        </div>
    `).join('');
}

// ---------- Custo por componente ----------
function iacCalcCost(usage, model, aiMeta) {
    let total = (typeof calcCostBRL === 'function') ? calcCostBRL(usage, model) : 0;
    const extras = [
        ...((aiMeta && aiMeta.queryRewriteUsage) || []),
        ...((aiMeta && aiMeta.toolUsage) || []),
        ...((aiMeta && aiMeta.embeddingsUsage) || []),
    ];
    for (const x of extras) total += (typeof calcCostBRL === 'function') ? calcCostBRL(x && x.usage || {}, x && x.model) : 0;
    return total;
}

function _iacAggExtras(execs, key) {
    const out = { cost: 0, tokens: 0, models: new Map() };
    for (const e of execs) {
        const arr = (e.aiMeta && e.aiMeta[key]) || [];
        for (const x of arr) {
            const u = (x && x.usage) || {};
            const m = (x && x.model) || '—';
            const c = (typeof calcCostBRL === 'function') ? calcCostBRL(u, m) : 0;
            const t = Number(u.total_tokens) || ((Number(u.prompt_tokens) || 0) + (Number(u.completion_tokens) || 0));
            out.cost += c;
            out.tokens += t;
            out.models.set(m, (out.models.get(m) || 0) + t);
        }
    }
    return out;
}

function iacRenderCustoComponente() {
    const wrap = document.getElementById('iac-custo-componente');
    const totLbl = document.getElementById('iac-custo-total-label');
    if (!wrap) return;

    const execs = iacState.executions;
    const orch = { cost: 0, tokens: 0, models: new Map() };
    for (const e of execs) {
        const u = e.usage || {};
        const m = e.model || '—';
        const c = (typeof calcCostBRL === 'function') ? calcCostBRL(u, m) : 0;
        const t = Number(u.total_tokens) || ((Number(u.prompt_tokens) || 0) + (Number(u.completion_tokens) || 0));
        orch.cost += c;
        orch.tokens += t;
        orch.models.set(m, (orch.models.get(m) || 0) + t);
    }
    const qr = _iacAggExtras(execs, 'queryRewriteUsage');
    const em = _iacAggExtras(execs, 'embeddingsUsage');
    const tu = _iacAggExtras(execs, 'toolUsage');

    const rows = [
        { key: 'orch', label: 'Orquestrador',         icon: 'smart_toy',  color: '#10b981', data: orch },
        { key: 'qr',   label: 'Reescrita de query',   icon: 'autorenew',  color: '#a78bfa', data: qr },
        { key: 'em',   label: 'Embeddings (RAG)',     icon: 'storage',    color: '#38bdf8', data: em },
        { key: 'tu',   label: 'Tools auxiliares',     icon: 'extension',  color: '#fb7185', data: tu },
    ];
    const total = rows.reduce((s, r) => s + r.data.cost, 0);
    if (totLbl) totLbl.textContent = `${iacFormatBRL(total)} no total`;

    wrap.innerHTML = rows.map(r => {
        const pct = total > 0 ? (r.data.cost / total) * 100 : 0;
        const modelsStr = Array.from(r.data.models.entries())
            .map(([m, t]) => `${m} · ${iacFormatInt(t)} tokens`)
            .join(' &middot; ') || '—';
        return `
            <div>
                <div class="flex items-center justify-between gap-3 mb-1.5">
                    <div class="flex items-center gap-2 min-w-0">
                        <span class="material-symbols-outlined text-base" style="color:${r.color}">${r.icon}</span>
                        <span class="text-sm font-semibold text-[var(--text-primary)] dark:text-white">${iacEsc(r.label)}</span>
                        <span class="text-xs text-slate-500 dark:text-slate-400 truncate">${modelsStr}</span>
                    </div>
                    <div class="flex items-center gap-2 flex-shrink-0">
                        <span class="text-sm font-bold text-[var(--text-primary)] dark:text-white tabular-nums">${iacFormatBRL(r.data.cost)}</span>
                        <span class="text-xs text-slate-500 dark:text-slate-400 tabular-nums">${pct.toFixed(1)}%</span>
                    </div>
                </div>
                <div class="iac-bar-track">
                    <div class="iac-bar-fill" style="width:${pct.toFixed(2)}%; background:${r.color};"></div>
                </div>
            </div>
        `;
    }).join('');
}

// ---------- Erros ----------
function iacGroupErrors(executions) {
    const errosNaJanela = executions.filter(e => e.error);
    const cutoff = Date.now() - 24 * 60 * 60 * 1000;
    const errosUltimas24h = errosNaJanela.filter(e => new Date(e.timestamp).getTime() >= cutoff);
    const groupMap = {};
    for (const e of errosNaJanela) {
        const fp = fingerprintError(e.error);
        if (!groupMap[fp]) {
            groupMap[fp] = { fingerprint: fp, count: 0, ultima: null, exemplo: null };
        }
        groupMap[fp].count++;
        if (!groupMap[fp].ultima || new Date(e.timestamp) > new Date(groupMap[fp].ultima.timestamp)) {
            groupMap[fp].ultima = { timestamp: e.timestamp, executionId: e.id };
            groupMap[fp].exemplo = e;
        }
    }
    const gruposErro = Object.values(groupMap)
        .sort((a, b) => b.count - a.count)
        .slice(0, 5);
    let dominante = null;
    if (errosUltimas24h.length > 0) {
        const map24 = {};
        for (const e of errosUltimas24h) {
            const fp = fingerprintError(e.error);
            map24[fp] = (map24[fp] || 0) + 1;
        }
        dominante = Object.entries(map24).sort((a, b) => b[1] - a[1])[0];
    }
    return { errosNaJanela, errosUltimas24h, gruposErro, dominante };
}

function iacRenderBanner(errorData) {
    const banner = document.getElementById('iac-banner-erros');
    const txt = document.getElementById('iac-banner-text');
    if (!banner || !txt) return;
    if (!errorData.errosUltimas24h.length) {
        banner.classList.add('hidden');
        return;
    }
    const n = errorData.errosUltimas24h.length;
    let extra = '';
    if (errorData.dominante) {
        const [fp, cnt] = errorData.dominante;
        const fpShort = fp.length > 80 ? fp.slice(0, 80) + '…' : fp;
        extra = ` Padrão dominante: "${fpShort}" (${cnt}× nas últimas 24h).`;
    }
    txt.textContent = `Atenção: ${n} erro${n === 1 ? '' : 's'} nas últimas 24h.${extra} Veja detalhes abaixo.`;
    banner.classList.remove('hidden');
}

function iacScrollToErros() {
    const el = document.getElementById('erros-painel');
    if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function iacRenderErros(errorData) {
    const wrap = document.getElementById('iac-erros-grupos');
    const cntLbl = document.getElementById('iac-erros-count');
    const toggleWrap = document.getElementById('iac-erros-toggle-wrap');
    const todosWrap = document.getElementById('iac-erros-todos');
    if (!wrap || !cntLbl || !toggleWrap || !todosWrap) return;

    const totalErros = errorData.errosNaJanela.length;
    cntLbl.textContent = `${totalErros} no período`;

    if (totalErros === 0) {
        wrap.innerHTML = `<div class="text-center py-10 text-sm text-slate-500 dark:text-slate-400">Nenhum erro na janela selecionada.</div>`;
        toggleWrap.innerHTML = '';
        todosWrap.classList.add('hidden');
        todosWrap.innerHTML = '';
        return;
    }

    wrap.innerHTML = errorData.gruposErro.map((g, idx) => {
        const ex = g.exemplo || {};
        const fullErr = ex.error || '';
        const tempo = relativeTime(g.ultima && g.ultima.timestamp);
        const execId = ex.id || '';
        return `
            <div class="rounded-xl border border-[var(--border)] p-4 mb-3" style="background: var(--bg-card);">
                <pre class="iac-error-pre">${iacEsc(fullErr)}</pre>
                <div class="flex items-center justify-between gap-3 mt-3">
                    <p class="text-xs text-slate-500 dark:text-slate-400">
                        <span class="font-semibold text-[var(--text-primary)] dark:text-white">${g.count}</span> ocorrência${g.count === 1 ? '' : 's'} · última ${iacEsc(tempo)}
                    </p>
                    <button type="button" onclick="iacOpenExecModal('${iacEsc(execId)}')"
                        class="text-xs font-semibold text-[var(--primary)] hover:underline">
                        Ver execução
                    </button>
                </div>
            </div>
        `;
    }).join('');

    toggleWrap.innerHTML = `
        <button type="button" id="iac-toggle-todos"
            class="text-xs font-semibold text-[var(--primary)] hover:underline flex items-center gap-1">
            <span class="material-symbols-outlined text-base">${iacState.verTodosErros ? 'remove' : 'add'}</span>
            ${iacState.verTodosErros ? 'Ocultar' : `Ver todos os ${totalErros} erros`}
        </button>
    `;
    const tBtn = document.getElementById('iac-toggle-todos');
    if (tBtn) tBtn.addEventListener('click', () => {
        iacState.verTodosErros = !iacState.verTodosErros;
        iacRenderErros(iacGroupErrors(iacState.executions));
    });

    if (iacState.verTodosErros) {
        todosWrap.classList.remove('hidden');
        todosWrap.innerHTML = errorData.errosNaJanela.map(e => {
            return `
                <div class="rounded-xl border border-[var(--border)] p-4" style="background: var(--bg-card);">
                    <div class="flex items-center justify-between gap-3 mb-2">
                        <p class="text-xs text-slate-500 dark:text-slate-400">
                            <span class="font-semibold text-[var(--text-primary)] dark:text-white">${iacEsc(iacFmtDateTime(e.timestamp))}</span>
                            ${e.userMessage ? `· <span class="italic">${iacEsc(String(e.userMessage).slice(0, 120))}${String(e.userMessage).length > 120 ? '…' : ''}</span>` : ''}
                        </p>
                        <button type="button" onclick="iacOpenExecModal('${iacEsc(e.id || '')}')"
                            class="text-xs font-semibold text-[var(--primary)] hover:underline">
                            Ver execução
                        </button>
                    </div>
                    <pre class="iac-error-pre">${iacEsc(e.error || '')}</pre>
                </div>
            `;
        }).join('');
    } else {
        todosWrap.classList.add('hidden');
        todosWrap.innerHTML = '';
    }
}

// ---------- Modal ----------
function iacOpenExecModal(execId) {
    const exec = iacState.executions.find(e => String(e.id) === String(execId));
    if (!exec) return;
    const modal = document.getElementById('iac-exec-modal');
    const title = document.getElementById('iac-exec-modal-title');
    const sub = document.getElementById('iac-exec-modal-sub');
    const body = document.getElementById('iac-exec-modal-body');
    if (!modal || !body) return;
    if (title) title.textContent = `Execução ${exec.id}`;
    if (sub) sub.textContent = `${iacFmtDateTime(exec.timestamp)} · ${exec.model || '—'}${exec.error ? ' · ERRO' : ''}`;
    body.textContent = JSON.stringify(exec, null, 2);
    modal.classList.remove('hidden');
}

function iacCloseExecModal(ev) {
    if (ev && ev.target && ev.target.id !== 'iac-exec-modal' && ev.type !== 'click') return;
    const modal = document.getElementById('iac-exec-modal');
    if (modal) modal.classList.add('hidden');
}

// ---------- Chart mensagens por dia ----------
function iacRenderChartMsgs() {
    const canvas = document.getElementById('iac-chart-msgs');
    if (!canvas || typeof Chart === 'undefined') return;

    const start = iacFromInputDate(iacState.dtIni) || new Date();
    const end = iacFromInputDate(iacState.dtFim) || new Date();
    const days = Math.max(1, iacDaysBetween(start, end));
    const bucketSize = days > 14 ? Math.ceil(days / 14) : 1;
    const buckets = [];
    for (let i = 0; i < days; i += bucketSize) {
        const bStart = new Date(start); bStart.setDate(start.getDate() + i);
        const bEnd = new Date(start); bEnd.setDate(start.getDate() + Math.min(i + bucketSize - 1, days - 1));
        buckets.push({
            start: bStart, end: bEnd,
            label: bucketSize === 1 ? iacGetDayLabel(bStart) : `${iacGetDayLabel(bStart)} — ${iacGetDayLabel(bEnd)}`,
            count: 0,
        });
    }
    for (const e of iacState.executions) {
        const t = new Date(e.timestamp);
        for (const b of buckets) {
            const bs = new Date(b.start); bs.setHours(0,0,0,0);
            const be = new Date(b.end);   be.setHours(23,59,59,999);
            if (t >= bs && t <= be) { b.count++; break; }
        }
    }

    const data = {
        labels: buckets.map(b => b.label),
        datasets: [{
            data: buckets.map(b => b.count),
            borderColor: '#60a5fa',
            backgroundColor: 'rgba(96,165,250,0.15)',
            fill: true,
            tension: 0.35,
            borderWidth: 2,
            pointRadius: 3,
            pointHoverRadius: 5,
            pointBackgroundColor: '#60a5fa',
        }],
    };

    if (iacState.chartMsgs) { iacState.chartMsgs.destroy(); iacState.chartMsgs = null; }
    iacState.chartMsgs = new Chart(canvas, {
        type: 'line',
        data,
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                x: { grid: { display: false }, ticks: { font: { size: 11 } } },
                y: { beginAtZero: true, ticks: { precision: 0, font: { size: 11 } } },
            },
        },
    });
}

// ---------- Tópicos (donut) ----------
function _iacToolCallsList(e) {
    const tc = e.toolCalls;
    if (Array.isArray(tc)) return tc;
    if (tc && typeof tc === 'object' && Array.isArray(tc.calls)) return tc.calls;
    return [];
}

function iacRenderChartTopicos() {
    const canvas = document.getElementById('iac-chart-topicos');
    const leg = document.getElementById('iac-topicos-legenda');
    const totEl = document.getElementById('iac-topicos-total');
    if (!canvas || typeof Chart === 'undefined') return;

    const counts = new Map();
    for (const e of iacState.executions) {
        const calls = _iacToolCallsList(e);
        for (const c of calls) {
            const raw = c && (c.tool || c.name || c.function);
            if (!raw) continue;
            const label = IAC_TOPIC_LABELS[raw] || raw;
            counts.set(label, (counts.get(label) || 0) + 1);
        }
    }
    const entries = Array.from(counts.entries()).sort((a, b) => b[1] - a[1]);
    const total = entries.reduce((s, [, n]) => s + n, 0);
    if (totEl) totEl.textContent = String(entries.length);

    const colors = entries.map(([label], i) => IAC_TOPIC_COLORS[label] || IAC_FALLBACK_PALETTE[i % IAC_FALLBACK_PALETTE.length]);

    if (iacState.chartTopicos) { iacState.chartTopicos.destroy(); iacState.chartTopicos = null; }

    if (entries.length === 0) {
        if (leg) leg.innerHTML = `<p class="text-sm text-slate-500 dark:text-slate-400">Sem tópicos no período.</p>`;
        const ctx = canvas.getContext('2d');
        ctx.clearRect(0, 0, canvas.width, canvas.height);
        return;
    }

    iacState.chartTopicos = new Chart(canvas, {
        type: 'doughnut',
        data: {
            labels: entries.map(([l]) => l),
            datasets: [{
                data: entries.map(([, n]) => n),
                backgroundColor: colors,
                borderWidth: 0,
            }],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            cutout: '70%',
            plugins: { legend: { display: false }, tooltip: { enabled: true } },
        },
    });

    if (leg) {
        leg.innerHTML = entries.map(([label, n], i) => {
            const pct = total > 0 ? Math.round((n / total) * 100) : 0;
            const c = colors[i];
            return `
                <div class="flex items-center gap-2">
                    <span class="inline-block w-2.5 h-2.5 rounded-full flex-shrink-0" style="background:${c}"></span>
                    <span class="text-xs text-[var(--text-primary)] dark:text-slate-200 truncate flex-1">${iacEsc(label)}</span>
                    <span class="text-xs font-bold text-[var(--text-primary)] dark:text-white tabular-nums">${iacFormatInt(n)}</span>
                    <span class="text-[10px] font-semibold px-1.5 py-0.5 rounded-full" style="background:${c}1f; color:${c};">${pct}%</span>
                </div>
            `;
        }).join('');
    }
}

// ---------- Tools mais usadas ----------
function iacRenderTools() {
    const wrap = document.getElementById('iac-tools-ranking');
    const totLbl = document.getElementById('iac-tools-total-label');
    if (!wrap) return;
    const counts = new Map();
    for (const e of iacState.executions) {
        const calls = _iacToolCallsList(e);
        for (const c of calls) {
            const raw = c && (c.tool || c.name || c.function);
            if (!raw) continue;
            counts.set(raw, (counts.get(raw) || 0) + 1);
        }
    }
    const total = Array.from(counts.values()).reduce((s, n) => s + n, 0);
    if (totLbl) totLbl.textContent = `${iacFormatInt(total)} chamada${total === 1 ? '' : 's'}`;

    if (total === 0) {
        wrap.innerHTML = `<p class="text-sm text-slate-500 dark:text-slate-400 text-center py-6">Sem tools chamadas no período.</p>`;
        return;
    }
    const entries = Array.from(counts.entries()).sort((a, b) => b[1] - a[1]);

    wrap.innerHTML = entries.map(([raw, n], idx) => {
        const label = IAC_TOOL_LABELS[raw] || raw;
        const pct = total > 0 ? (n / total) * 100 : 0;
        const color = IAC_FALLBACK_PALETTE[idx % IAC_FALLBACK_PALETTE.length];
        return `
            <div>
                <div class="flex items-center justify-between gap-3 mb-1.5">
                    <div class="flex items-center gap-2 min-w-0">
                        <span class="text-xs font-bold text-slate-500 dark:text-slate-400 tabular-nums w-5 text-right">${idx + 1}</span>
                        <span class="text-sm font-semibold text-[var(--text-primary)] dark:text-white truncate">${iacEsc(label)}</span>
                    </div>
                    <div class="flex items-center gap-2 flex-shrink-0">
                        <span class="text-sm font-bold text-[var(--text-primary)] dark:text-white tabular-nums">${iacFormatInt(n)}</span>
                        <span class="text-xs text-slate-500 dark:text-slate-400 tabular-nums">${pct.toFixed(1)}%</span>
                    </div>
                </div>
                <div class="iac-bar-track">
                    <div class="iac-bar-fill" style="width:${pct.toFixed(2)}%; background:${color};"></div>
                </div>
            </div>
        `;
    }).join('');
}
