// ---------------------------------------------------------------------------
// Ativações Acadêmicas — proxy via Flask
// ---------------------------------------------------------------------------
const _ATIV_API = "/api/ativacoes/dados";

function loadAtivacoes() {
    const hoje = new Date();
    const amanha = new Date(hoje.getTime() + 86400000);

    const elIni = document.getElementById("ativ-data-inicio");
    const elFim = document.getElementById("ativ-data-fim");
    if (!elIni.value) elIni.value = _ativFmtDate(hoje);
    if (!elFim.value) elFim.value = _ativFmtDate(amanha);

    ativAtualizar();
}

async function ativAtualizar() {
    const dtIni = document.getElementById("ativ-data-inicio").value;
    const dtFim = document.getElementById("ativ-data-fim").value;
    if (!dtIni || !dtFim) { _ativErro("Selecione o período."); return; }

    _ativLoading(true);
    _ativErro("");
    _ativStatus("");

    try {
        const cur = await _ativFetch(dtIni, dtFim);
        const ativacoes = cur.ativacoes;
        const retornos  = cur.retornos;

        _ativRenderTblAtivacoes(ativacoes);
        _ativRenderTblRetornos(retornos);

        const totalAtiv = ativacoes.reduce((s, a) => s + a.total, 0);
        const totalRet  = retornos
            .filter(r => r.retorno.toLowerCase() !== "sem resposta")
            .reduce((s, r) => s + r.quantidade, 0);

        const prev = await _ativFetchPrevious(dtIni, dtFim);
        const prevAtiv = prev.ativacoes.reduce((s, a) => s + a.total, 0);
        const prevRet  = prev.retornos
            .filter(r => r.retorno.toLowerCase() !== "sem resposta")
            .reduce((s, r) => s + r.quantidade, 0);

        const pctAtiv = _ativPct(totalAtiv, prevAtiv);
        const pctRet  = _ativPct(totalRet, prevRet);

        _ativRenderKPIs(totalAtiv, prevAtiv, pctAtiv, totalRet, prevRet, pctRet);
        _ativRenderComparativo(totalAtiv, prevAtiv, pctAtiv, totalRet, prevRet, pctRet);

        _ativStatus(`Período: ${dtIni} a ${dtFim} · Atualizado em ${new Date().toLocaleString("pt-BR")}`);
    } catch (e) {
        _ativErro("Erro ao buscar dados: " + e.message);
    } finally {
        _ativLoading(false);
    }
}

// ---------------------------------------------------------------------------
// Data fetching
// ---------------------------------------------------------------------------
async function _ativFetch(dtIni, dtFim) {
    const url = `${_ATIV_API}?data_inicio=${dtIni}&data_fim=${dtFim}`;
    const res = await fetch(url);
    if (!res.ok) throw new Error("HTTP " + res.status);
    const data = await res.json();
    return _ativExtract(data);
}

async function _ativFetchPrevious(dtIni, dtFim) {
    const d0 = new Date(dtIni), d1 = new Date(dtFim);
    const days = Math.ceil(Math.abs(d1 - d0) / 86400000);
    const prevEnd   = new Date(d0.getTime() - 86400000);
    const prevStart = new Date(prevEnd.getTime() - (days - 1) * 86400000);
    try {
        return await _ativFetch(_ativFmtDate(prevStart), _ativFmtDate(prevEnd));
    } catch (_) {
        return { ativacoes: [], retornos: [] };
    }
}

function _ativExtract(data) {
    let p = null;
    if (data && typeof data === "object" && !Array.isArray(data)) {
        p = data.payload || (data.json && data.json.payload) || data;
    } else if (Array.isArray(data) && data.length) {
        const d0 = data[0];
        p = (d0 && (d0.payload || (d0.json && d0.json.payload))) || d0;
    }
    const ativacoes = Array.isArray(p?.ativacoes_por_dia)
        ? p.ativacoes_por_dia.map(a => ({ dia: String(a.dia), total: Number(a.total) || 0 }))
        : [];
    const retornos = Array.isArray(p?.retornos_por_status)
        ? p.retornos_por_status.map(r => ({ retorno: String(r.retorno || ""), quantidade: Number(r.quantidade) || 0 }))
        : [];
    return { ativacoes, retornos };
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------
const _ativFmt = n => new Intl.NumberFormat("pt-BR").format(n);

function _ativRenderKPIs(curA, prevA, pctA, curR, prevR, pctR) {
    document.getElementById("ativ-kpi-ativ-val").textContent  = _ativFmt(curA);
    document.getElementById("ativ-kpi-ativ-prev").textContent = _ativFmt(prevA);
    _ativSetBadge("ativ-kpi-ativ-badge", pctA);
    document.getElementById("ativ-kpi-ativ-icon").textContent = pctA > 0 ? "▲" : pctA < 0 ? "▼" : "—";
    document.getElementById("ativ-kpi-ativ-icon").className   = `text-sm ${pctA > 0 ? "text-green-400" : pctA < 0 ? "text-red-400" : "text-slate-500"}`;

    document.getElementById("ativ-kpi-ret-val").textContent  = _ativFmt(curR);
    document.getElementById("ativ-kpi-ret-prev").textContent = _ativFmt(prevR);
    _ativSetBadge("ativ-kpi-ret-badge", pctR);
    document.getElementById("ativ-kpi-ret-icon").textContent = pctR > 0 ? "▲" : pctR < 0 ? "▼" : "—";
    document.getElementById("ativ-kpi-ret-icon").className   = `text-sm ${pctR > 0 ? "text-green-400" : pctR < 0 ? "text-red-400" : "text-slate-500"}`;
}

function _ativRenderComparativo(curA, prevA, pctA, curR, prevR, pctR) {
    document.getElementById("ativ-cmp-ativ-cur").textContent  = _ativFmt(curA);
    document.getElementById("ativ-cmp-ativ-prev").textContent = _ativFmt(prevA);
    _ativSetBadge("ativ-cmp-ativ-badge", pctA);

    document.getElementById("ativ-cmp-ret-cur").textContent  = _ativFmt(curR);
    document.getElementById("ativ-cmp-ret-prev").textContent = _ativFmt(prevR);
    _ativSetBadge("ativ-cmp-ret-badge", pctR);
}

function _ativSetBadge(id, pct) {
    const el = document.getElementById(id);
    const sign = pct > 0 ? "+" : "";
    el.textContent = `${sign}${pct}%`;
    if (pct > 0)      el.className = "px-2 py-0.5 rounded-full font-semibold text-xs bg-green-500/20 text-green-400";
    else if (pct < 0) el.className = "px-2 py-0.5 rounded-full font-semibold text-xs bg-red-500/20 text-red-400";
    else               el.className = "px-2 py-0.5 rounded-full font-semibold text-xs bg-slate-500/20 text-slate-400";
}

function _ativRenderTblAtivacoes(rows) {
    const tb = document.getElementById("ativ-tbl-ativacoes");
    if (!rows.length) {
        tb.innerHTML = '<tr><td colspan="2" class="px-5 py-6 text-center text-slate-600">Sem dados disponíveis</td></tr>';
        return;
    }
    tb.innerHTML = rows.map(d =>
        `<tr class="hover:bg-white/[0.02]">
            <td class="px-5 py-2.5 text-slate-300">${esc(d.dia)}</td>
            <td class="px-5 py-2.5 text-right text-slate-200 font-semibold">${_ativFmt(d.total)}</td>
        </tr>`
    ).join("");
}

function _ativRenderTblRetornos(rows) {
    const tb = document.getElementById("ativ-tbl-retornos");
    if (!rows.length) {
        tb.innerHTML = '<tr><td colspan="2" class="px-5 py-6 text-center text-slate-600">Sem dados disponíveis</td></tr>';
        return;
    }
    tb.innerHTML = rows.map(r =>
        `<tr class="hover:bg-white/[0.02]">
            <td class="px-5 py-2.5 text-blue-400 font-medium">${esc(r.retorno.toUpperCase())}</td>
            <td class="px-5 py-2.5 text-right text-slate-200 font-semibold">${_ativFmt(r.quantidade)}</td>
        </tr>`
    ).join("");
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function _ativPct(cur, prev) {
    if (prev === 0) return cur > 0 ? 100 : 0;
    return Math.round(((cur - prev) / prev) * 100);
}

function _ativFmtDate(d) {
    return d.toISOString().substring(0, 10);
}

function _ativLoading(show) {
    document.getElementById("ativ-loading").classList.toggle("hidden", !show);
    const btn = document.getElementById("ativ-btn-atualizar");
    if (btn) btn.disabled = show;
    const icon = document.getElementById("ativ-btn-icon");
    if (icon) icon.classList.toggle("animate-spin", show);
}

function _ativErro(msg) {
    const el = document.getElementById("ativ-erro");
    el.textContent = msg;
    el.classList.toggle("hidden", !msg);
}

function _ativStatus(msg) {
    const el = document.getElementById("ativ-status");
    if (!el) return;
    el.textContent = msg;
    el.classList.toggle("hidden", !msg);
}
