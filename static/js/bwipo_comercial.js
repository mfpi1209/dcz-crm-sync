let _bwcChartDia = null;
let _bwcChartAg = null;
let _bwcPayload = null;
let _bwcDia = null;

function loadBwipoComercial() {
    if (_bwcIsAdmin()) document.querySelectorAll("[data-bwc-admin]").forEach((el) => el.classList.remove("hidden"));
    bwcPreparar().then(() => bwcAtualizar());
}

async function bwcPreparar() {
    const ini = document.getElementById("bwc-dt-ini");
    const fim = document.getElementById("bwc-dt-fim");
    try {
        const [metas, camps] = await Promise.all([
            fetch("/api/comercial-rgm/metas?categoria=matriculas").then((r) => r.json()),
            fetch("/api/premiacao/campanhas-periodos").then((r) => r.json()),
        ]);
        const periods = [];
        (metas.metas || []).forEach((m) => { if (m.dt_inicio && m.dt_fim) periods.push(m); });
        (camps.campanhas || []).forEach((c) => { if (c.dt_inicio && c.dt_fim) periods.push(c); });
        const ultima = typeof _crgmPickLatestMetaPeriod === "function" ? _crgmPickLatestMetaPeriod(periods) : null;
        if (ultima && ini && fim && (!ini.value || !fim.value)) {
            ini.value = String(ultima.dt_inicio).slice(0, 10);
            fim.value = String(ultima.dt_fim).slice(0, 10);
        }
    } catch (_) {}
    if (ini && fim && (!ini.value || !fim.value)) {
        const hoje = new Date();
        ini.value = ini.value || new Date(hoje.getFullYear(), hoje.getMonth(), 1).toISOString().slice(0, 10);
        fim.value = fim.value || hoje.toISOString().slice(0, 10);
    }
    await Promise.all([
        typeof _crgmLoadCiclos === "function" ? _crgmLoadCiclos() : null,
        typeof _crgmLoadTurmas === "function" ? _crgmLoadTurmas() : null,
        bwcSnapshot(),
        bwcBadgeSolicitacoes(),
    ]);
    bwcCopiarSelect("crgm-ciclo", "bwc-ciclo", "Todos");
    bwcCopiarSelect("crgm-turma", "bwc-turma", "Todas");
}

function bwcCopiarSelect(origemId, destinoId, primeiro) {
    const origem = document.getElementById(origemId);
    const destino = document.getElementById(destinoId);
    if (!origem || !destino) return;
    const atual = destino.value;
    destino.innerHTML = origem.innerHTML;
    if ([...destino.options].some((o) => o.value === atual)) destino.value = atual;
}

function bwcSnapshot() {
    return fetch("/api/comercial-rgm/snapshot-info").then((r) => r.json()).then((d) => {
        const el = document.getElementById("bwc-snapshot");
        if (!el || !d.ok || !d.total) return;
        let info = Number(d.total).toLocaleString("pt-BR") + " registros CSV";
        if (d.min_date && d.max_date) info += " | " + d.min_date + " a " + d.max_date;
        if (d.mm_inscritos || d.mm_matriculados) {
            info += " | M&M: " + (d.mm_inscritos || 0).toLocaleString("pt-BR") + " insc. / " + (d.mm_matriculados || 0).toLocaleString("pt-BR") + " matr.";
        }
        el.textContent = info;
    }).catch(() => {});
}

function bwcBadgeSolicitacoes() {
    return fetch("/api/ajustes-matricula?status=pendente").then((r) => r.json()).then((d) => {
        const n = (d.ajustes || []).length;
        document.querySelectorAll(".bwc-sol-badge").forEach((badge) => {
            badge.textContent = n > 99 ? "99+" : String(n);
            badge.classList.toggle("hidden", n <= 0);
        });
    }).catch(() => {});
}

function bwcAbrirPainel(panelId, toggleName) {
    const panel = document.getElementById(panelId);
    const host = document.getElementById("bwc-paineis");
    if (panel && host && !host.contains(panel)) host.appendChild(panel);
    const fn = window[toggleName];
    if (typeof fn === "function") fn();
}

function bwcCicloChanged() {
    const id = parseInt(document.getElementById("bwc-ciclo")?.value, 10);
    const ciclo = (window._crgmCiclosData || []).find((c) => c.id === id);
    if (ciclo && typeof _crgmNormalizePeriodInputs === "function") {
        const n = _crgmNormalizePeriodInputs(ciclo.dt_inicio, ciclo.dt_fim);
        document.getElementById("bwc-dt-ini").value = n.dtIni;
        document.getElementById("bwc-dt-fim").value = n.dtFim;
    }
    if (typeof _crgmLoadTurmas === "function") {
        _crgmLoadTurmas(id || null).then(() => bwcCopiarSelect("crgm-turma", "bwc-turma", "Todas"));
    }
    bwcAtualizar();
}

function bwcTurmaChanged() {
    const id = parseInt(document.getElementById("bwc-turma")?.value, 10);
    const turma = (window._crgmTurmasData || []).find((t) => t.id === id);
    if (turma) {
        document.getElementById("bwc-dt-ini").value = String(turma.dt_inicio).slice(0, 10);
        document.getElementById("bwc-dt-fim").value = String(turma.dt_fim).slice(0, 10);
        if (turma.nivel) document.getElementById("bwc-nivel").value = turma.nivel;
    }
    bwcAtualizar();
}

function _bwcQs() {
    const p = new URLSearchParams();
    const polo = document.getElementById("bwc-polo")?.value || "";
    const nivel = document.getElementById("bwc-nivel")?.value || "";
    const agente = document.getElementById("bwc-agente")?.value || "";
    const ini = document.getElementById("bwc-dt-ini")?.value || "";
    const fim = document.getElementById("bwc-dt-fim")?.value || "";
    const cicloSel = document.getElementById("bwc-ciclo");
    const ciclo = (window._crgmCiclosData || []).find((c) => String(c.id) === (cicloSel?.value || ""));
    const turma = (window._crgmTurmasData || []).find((t) => String(t.id) === (document.getElementById("bwc-turma")?.value || ""));
    if (polo) p.set("polo", polo);
    if (nivel) p.set("nivel", nivel);
    if (ciclo) p.set("ciclo", ciclo.nome);
    if (turma) p.set("turma", turma.nome);
    if (agente) p.set("owner", agente);
    if (ini) p.set("dt_ini", ini);
    if (fim) p.set("dt_fim", fim);
    return p;
}

function _bwcNum(n) {
    return (n || 0).toLocaleString("pt-BR");
}

function _bwcBadge(id, pct) {
    const el = document.getElementById(id);
    if (!el) return;
    if (pct == null) {
        el.textContent = "";
        el.className = "hidden";
        return;
    }
    el.classList.remove("hidden");
    el.textContent = (pct >= 0 ? "↑ " : "↓ ") + Math.abs(pct).toLocaleString("pt-BR") + "%";
    el.className = pct >= 0
        ? "font-bold px-1.5 py-0.5 rounded text-[10px] bg-emerald-500/20 text-emerald-700 dark:text-emerald-400"
        : "font-bold px-1.5 py-0.5 rounded text-[10px] bg-red-500/20 text-red-600 dark:text-red-400";
}

function _bwcCompare(prefix, valor, delta, periodo) {
    const val = document.getElementById("bwc-" + prefix + "-val");
    const sub = document.getElementById("bwc-" + prefix + "-sub");
    if (val) val.textContent = valor > 0 ? _bwcNum(valor) : "—";
    if (sub) {
        sub.textContent = (valor > 0 && delta != null)
            ? (delta >= 0 ? "+" : "") + delta.toLocaleString("pt-BR") + " matrículas"
            : "sem histórico no período";
        sub.title = periodo || "Período de referência";
    }
}

function _bwcDataBr(s) {
    const t = String(s || "").slice(0, 10);
    const iso = t.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    return iso ? `${iso[3]}/${iso[2]}/${iso[1]}` : (t || "—");
}

function _bwcFillSelect(id, values, atual) {
    const sel = document.getElementById(id);
    if (!sel) return;
    const first = sel.options[0] ? sel.options[0].outerHTML : '<option value="">Todos</option>';
    const extra = id === "bwc-nivel"
        ? '<option value="Graduação">Graduação</option><option value="Pós-Graduação">Pós-Graduação</option>'
        : values.map((v) => `<option value="${String(v).replace(/"/g, "&quot;")}">${v}</option>`).join("");
    sel.innerHTML = first + extra;
    if ([...sel.options].some((o) => o.value === atual)) sel.value = atual;
}

function bwcAtualizar() {
    const err = document.getElementById("bwc-erro");
    const btn = document.getElementById("bwc-btn");
    if (err) err.classList.add("hidden");
    if (btn) {
        btn.disabled = true;
        btn.textContent = "Atualizando...";
    }
    const polo = document.getElementById("bwc-polo")?.value || "";
    const nivel = document.getElementById("bwc-nivel")?.value || "";
    const agente = document.getElementById("bwc-agente")?.value || "";
    fetch("/api/bwipo/painel?" + _bwcQs().toString())
        .then((r) => r.json())
        .then((d) => {
            if (!d.ok) throw new Error(d.error || "falha");
            const k = d.kpis || {};
            document.getElementById("bwc-matriculas").textContent = _bwcNum(k.matriculas);
            document.getElementById("bwc-ganho-label").textContent = _bwcNum(k.em_curso);
            document.getElementById("bwc-aberto").textContent = _bwcNum(k.aberto);
            document.getElementById("bwc-perdido").textContent = _bwcNum(k.perdido);
            _bwcCompare("6m", k.vendas_6m, k.delta_6m, k.compare_6m_period);
            _bwcCompare("1a", k.vendas_1a, k.delta_1a, k.compare_1a_period);
            _bwcBadge("bwc-6m-badge", k.pct_6m);
            _bwcBadge("bwc-1a-badge", k.pct_1a);
            document.getElementById("bwc-evasao").textContent = _bwcNum(k.evasao);
            document.getElementById("bwc-fora").textContent = _bwcNum(k.fora_padrao);
            const foraHero = document.getElementById("bwc-fora-hero");
            if (foraHero) {
                foraHero.classList.toggle("hidden", !k.fora_padrao);
                document.getElementById("bwc-fora-hero-n").textContent = _bwcNum(k.fora_padrao);
                const emCursoFora = (d.linhas || []).filter((l) => l.fora && l.situacao === "EM CURSO").length;
                document.getElementById("bwc-fora-hero-sub").textContent = emCursoFora
                    ? emCursoFora.toLocaleString("pt-BR") + " alunos em curso — ver lista"
                    : "ver lista";
            }
            document.getElementById("bwc-prefixo").textContent = k.prefixo || "—";
            document.getElementById("bwc-media").textContent = (k.media || 0).toLocaleString("pt-BR");
            document.getElementById("bwc-dias").textContent = k.dias || "—";
            document.getElementById("bwc-ticket").textContent = k.ticket
                ? k.ticket.toLocaleString("pt-BR", { style: "currency", currency: "BRL" })
                : "—";
            document.getElementById("bwc-prefixos").innerHTML = (d.prefixos || []).map((p) =>
                `<span class="text-[11px] px-2 py-0.5 rounded-full bg-orange-500/15 text-orange-200">${p.pfx} · ${p.n}</span>`
            ).join("");
            const op = d.opcoes || {};
            _bwcFillSelect("bwc-polo", op.polos || [], polo);
            _bwcFillSelect("bwc-agente", op.agentes || [], agente);
            if (nivel) document.getElementById("bwc-nivel").value = nivel;
            _bwcPayload = d;
            _bwcDia = null;
            const rows = _bwcPaint();
            const polos = d.polos || [];
            const max = polos.reduce((m, p) => Math.max(m, p.total), 1);
            document.getElementById("bwc-polos").innerHTML = polos.length ? polos.map((p, i) => `
                <tr>
                    <td class="text-center px-3 py-2 text-slate-500">${i + 1}</td>
                    <td class="px-4 py-2">${p.polo}</td>
                    <td class="px-4 py-2 text-right font-bold tabular-nums">${_bwcNum(p.total)}</td>
                    <td class="px-4 py-2"><span class="inline-block h-1.5 rounded bg-cyan-500" style="width:${Math.max(6, Math.round(100 * p.total / max))}%"></span></td>
                </tr>
            `).join("") : '<tr><td colspan="4" class="px-5 py-6 text-center text-slate-500">Sem polo.</td></tr>';
            _bwcCharts(d.evolucao || [], rows);
            _bwcEvasaoChips();
            _bwcCarregarExtras();
            _bwcCarregarConflitos();
        })
        .catch((e) => {
            if (err) {
                err.textContent = e.message || String(e);
                err.classList.remove("hidden");
            }
        })
        .finally(() => {
            if (btn) {
                btn.disabled = false;
                btn.textContent = "Atualizar";
            }
        });
}

function _bwcEsc(s) {
    return String(s || "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
}

function _bwcLinhas() {
    const all = (_bwcPayload && _bwcPayload.linhas) || [];
    return _bwcDia ? all.filter((l) => l.data === _bwcDia) : all;
}

function _bwcRankingRows() {
    const base = ((_bwcPayload && _bwcPayload.ranking) || []).slice();
    if (!_bwcDia) return base.filter((r) => r.matriculas || r.aberto || r.perdido || r.evasao);
    const acc = {};
    _bwcLinhas().forEach((l) => {
        const b = acc[l.agente] || { matriculas: 0, evasao: 0, pos: 0 };
        if (l.conta) {
            b.matriculas += 1;
            if (l.nivel === "Pós-Graduação") b.pos += 1;
        } else if (l.situacao !== "EM CURSO") b.evasao += 1;
        acc[l.agente] = b;
    });
    return base.map((r) => {
        const b = acc[r.agente] || { matriculas: 0, evasao: 0, pos: 0 };
        const tot = b.matriculas + b.evasao;
        return Object.assign({}, r, b, { conv: tot ? Math.round((1000 * b.matriculas) / tot) / 10 : 0 });
    }).filter((r) => r.matriculas || r.evasao).sort((a, b) => b.matriculas - a.matriculas);
}

function _bwcNivel(r) {
    const mp = r.matriculas || 0;
    if (r.supermeta && mp >= r.supermeta) return "Super";
    if (r.meta && mp >= r.meta) return "Meta";
    if (r.intermediaria && mp >= r.intermediaria) return "Interm.";
    if (r.meta) return "Abaixo";
    return r.pos ? (r.pos === r.matriculas ? "Pós" : "Misto") : "Grad.";
}

const _BWC_FONTE = { manual: "decisão manual", bwipo: "Bwipo", kommo: "Kommo", sem_dono: "sem dono" };

function _bwcIsAdmin() {
    return document.body?.dataset?.role === "admin";
}

function _bwcLista(rows, contarVenda) {
    if (!rows.length) return '<p class="px-4 py-3 text-slate-500">Ninguém neste recorte.</p>';
    const admin = contarVenda && _bwcIsAdmin();
    const body = rows.slice(0, 200).map((l) => {
        const sit = (l.situacao || "").toUpperCase();
        const sitCls = sit === "EM CURSO"
            ? "bg-emerald-500/15 text-emerald-700 dark:text-emerald-300"
            : (sit.includes("CANCEL") || sit.includes("TRANC") ? "bg-rose-500/15 text-rose-500" : "bg-violet-500/15 text-violet-300");
        const fonte = _BWC_FONTE[l.fonte] || "—";
        return `<tr class="hover:bg-slate-50 dark:hover:bg-white/[0.03]">
            <td class="px-3 py-2 whitespace-nowrap"><button type="button" class="font-mono font-semibold underline decoration-dotted" onclick="bwcConsultarRgm('${_bwcEsc(l.rgm)}')">${_bwcEsc(l.rgm)}</button></td>
            <td class="px-3 py-2 text-[var(--text-primary)]">${_bwcEsc(l.nome)}${l.fora ? ' <span class="text-orange-500">fora do padrão</span>' : ""}</td>
            <td class="px-3 py-2 whitespace-nowrap"><span class="inline-block px-1.5 py-0.5 rounded-full ${sitCls}">${_bwcEsc(sit || "—")}</span></td>
            <td class="px-3 py-2 whitespace-nowrap font-mono text-blue-700 dark:text-blue-300">${_bwcDataBr(l.data)}</td>
            <td class="px-3 py-2 whitespace-nowrap">${_bwcEsc(l.polo || "—")}</td>
            <td class="px-3 py-2 whitespace-nowrap text-slate-500">${_bwcEsc(l.nivel || "—")}</td>
            <td class="px-3 py-2 whitespace-nowrap text-slate-500">${_bwcEsc(l.agente)} · ${_bwcEsc(fonte)}</td>
            ${admin ? `<td class="px-3 py-2 whitespace-nowrap"><button type="button" onclick="bwcContarVenda('${_bwcEsc(l.rgm)}', true)" class="px-2 py-0.5 rounded text-[10px] font-semibold bg-emerald-500/20 text-emerald-700 dark:text-emerald-300 border border-emerald-500/40">Contar venda</button></td>` : ""}
        </tr>`;
    }).join("");
    const mais = rows.length > 200 ? `<p class="px-4 py-2 text-slate-500">+ ${rows.length - 200} linhas</p>` : "";
    return `<div class="overflow-x-auto"><table class="w-full text-xs">
        <thead class="sticky top-0 bg-[var(--bg-card)] text-[10px] uppercase tracking-wider text-slate-500">
            <tr class="border-b border-slate-200 dark:border-slate-700/40">
                <th class="px-3 py-2 text-left font-medium">RGM</th>
                <th class="px-3 py-2 text-left font-medium">Nome</th>
                <th class="px-3 py-2 text-left font-medium">Situação</th>
                <th class="px-3 py-2 text-left font-medium">Data</th>
                <th class="px-3 py-2 text-left font-medium">Polo</th>
                <th class="px-3 py-2 text-left font-medium">Nível</th>
                <th class="px-3 py-2 text-left font-medium">Fonte</th>
                ${admin ? "<th></th>" : ""}
            </tr>
        </thead>
        <tbody class="divide-y divide-slate-200/70 dark:divide-slate-700/30">${body}</tbody>
    </table></div>${mais}`;
}

function _bwcModal(titulo, sub, corpo) {
    let modal = document.getElementById("bwc-modal");
    if (!modal) {
        modal = document.createElement("div");
        modal.id = "bwc-modal";
        modal.className = "fixed inset-0 z-[80] flex items-start justify-center bg-black/60 p-4 overflow-y-auto";
        modal.addEventListener("click", (e) => { if (e.target === modal) bwcFecharModal(); });
        document.body.appendChild(modal);
    }
    modal.innerHTML = `<div class="bg-[var(--bg-card)] rounded-2xl max-w-5xl w-full my-8 border border-slate-200 dark:border-slate-700">
        <div class="px-5 py-3 flex items-center justify-between border-b border-slate-200 dark:border-slate-700">
            <div><p class="font-bold">${titulo}</p><p class="text-xs text-slate-500">${sub}</p></div>
            <button type="button" class="text-sm px-2" onclick="bwcFecharModal()">fechar</button>
        </div>
        <div class="max-h-[70vh] overflow-y-auto text-xs">${corpo}</div>
    </div>`;
}

async function _bwcSend(url, method, body) {
    const r = await fetch(url, {
        method,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body || {}),
    });
    const d = await r.json().catch(() => ({ ok: false, error: "Resposta inválida do servidor" }));
    if (!d.ok) throw new Error(d.error || "falha");
    return d;
}

function _bwcCandidatos(rgm, cands, resolucao) {
    if (!cands.length) return '<p class="px-4 py-3 text-slate-500">Nenhum lead no Kommo nem negócio no Bwipo com esse RGM.</p>';
    return cands.map((c) => {
        const escolhido = resolucao && c.user_id && resolucao.user_id === c.user_id;
        const ref = c.origem === "kommo" ? `Kommo #${c.id}` : `Bwipo #${c.number || "—"}`;
        const fixar = c.user_id
            ? `<button type="button" onclick="bwcFixarVenda('${rgm}', ${c.user_id}, '${_bwcEsc(c.agente).replace(/'/g, "\\'")}')" class="px-2 py-0.5 rounded text-[10px] font-semibold border border-amber-500/40 text-amber-600 dark:text-amber-300 ${escolhido ? "bg-amber-500/25" : ""}">${escolhido ? "Venda fixada aqui" : "Fixar venda aqui"}</button>`
            : `<span class="text-[10px] text-slate-500" title="Responsável do Bwipo sem depara com o Kommo">sem depara</span>`;
        const sync = c.origem === "kommo"
            ? `<button type="button" onclick="bwcSyncCandidato({lead_id: ${c.id}}, '${rgm}')" class="px-2 py-0.5 rounded text-[10px] border border-emerald-500/40 text-emerald-600 dark:text-emerald-300">Mini-sync</button>`
            : `<button type="button" onclick="bwcSyncCandidato({deal_id: '${_bwcEsc(c.id)}'}, '${rgm}')" class="px-2 py-0.5 rounded text-[10px] border border-emerald-500/40 text-emerald-600 dark:text-emerald-300">Mini-sync</button>`;
        return `<div class="px-4 py-2 flex justify-between items-center gap-3 border-b border-slate-200/60 dark:border-slate-700/30">
            <span><b>${_bwcEsc(c.agente)}</b> <span class="text-slate-500">· ${ref} · ${_bwcEsc(c.etapa)}${c.data ? " · " + _bwcDataBr(c.data) : ""}</span></span>
            <span class="flex gap-2">${fixar}${sync}</span>
        </div>`;
    }).join("");
}

function _bwcCarregarConflitos() {
    const rgms = [...new Set(((_bwcPayload && _bwcPayload.linhas) || []).map((l) => l.rgm))];
    const lista = document.getElementById("bwc-conflito-lista");
    document.getElementById("bwc-conflitos").textContent = "…";
    _bwcSend("/api/bwipo/painel/conflitos", "POST", {
        rgms,
        dt_ini: document.getElementById("bwc-dt-ini")?.value || "",
        dt_fim: document.getElementById("bwc-dt-fim")?.value || "",
    })
        .then((d) => {
            document.getElementById("bwc-conflitos").textContent = _bwcNum(d.total);
            document.getElementById("bwc-conflitos-sub").textContent = d.total
                ? ` · ${_bwcNum(d.total_nao_resolvidos)} sem decisão`
                : "";
            const porRgm = {};
            ((_bwcPayload && _bwcPayload.linhas) || []).forEach((l) => { porRgm[l.rgm] = l; });
            lista.innerHTML = d.conflitos.length ? d.conflitos.map((c) => {
                const l = porRgm[c.rgm] || {};
                const res = c.resolucao
                    ? `<span class="text-emerald-500">decidido: ${_bwcEsc(c.resolucao.user_name || c.resolucao.user_id)}</span>
                       <button type="button" class="underline text-slate-500" onclick="bwcDesfazerVenda('${c.rgm}')">desfazer</button>`
                    : `<span class="text-amber-500">sem decisão · hoje com ${_bwcEsc(l.agente || "—")}</span>`;
                return `<div class="border-b border-amber-500/15">
                    <div class="px-4 pt-2 flex justify-between gap-3"><span><b>${c.rgm}</b> ${_bwcEsc(l.nome || "")} <span class="text-slate-500">${_bwcEsc(l.data || "")}</span></span><span class="flex gap-2">${res}</span></div>
                    ${_bwcCandidatos(c.rgm, c.candidatos, c.resolucao)}
                </div>`;
            }).join("") : '<p class="px-4 py-3 text-slate-500">Nenhum conflito neste recorte.</p>';
        })
        .catch((e) => {
            document.getElementById("bwc-conflitos").textContent = "—";
            lista.innerHTML = `<p class="px-4 py-3 text-red-400">${_bwcEsc(e.message)}</p>`;
        });
}

function _bwcDepoisDeDecidir(rgm, msg) {
    toast(msg, "success");
    const modal = document.getElementById("bwc-modal");
    if (modal && rgm) bwcConsultarRgm(rgm);
    bwcAtualizar();
}

function bwcFixarVenda(rgm, userId, nome) {
    if (!window.confirm(`Fixar a venda do RGM ${rgm} em ${nome}?`)) return;
    _bwcSend("/api/bwipo/painel/conflitos/resolver", "POST", { rgm, user_id: userId, user_name: nome })
        .then(() => _bwcDepoisDeDecidir(rgm, `Venda do RGM ${rgm} fixada em ${nome}.`))
        .catch((e) => toast(e.message, "error"));
}

function bwcDesfazerVenda(rgm) {
    if (!window.confirm(`Tirar a decisão manual do RGM ${rgm}? A venda volta para a regra automática.`)) return;
    _bwcSend("/api/bwipo/painel/conflitos/resolver", "DELETE", { rgm })
        .then(() => _bwcDepoisDeDecidir(rgm, `RGM ${rgm} voltou para a regra automática.`))
        .catch((e) => toast(e.message, "error"));
}

function bwcContarVenda(rgm, contar) {
    const txt = contar ? "passar a contar como venda" : "deixar de contar como venda";
    if (!window.confirm(`RGM ${rgm}: ${txt}?`)) return;
    _bwcSend("/api/bwipo/painel/contar-venda", contar ? "POST" : "DELETE", { rgm })
        .then(() => _bwcDepoisDeDecidir(rgm, contar ? `RGM ${rgm} passa a contar.` : `RGM ${rgm} deixou de contar.`))
        .catch((e) => toast(e.message, "error"));
}

function bwcSyncCandidato(body, rgm) {
    toast("Sincronizando…", "info", 2000);
    _bwcSend("/api/bwipo/painel/mini-sync", "POST", body)
        .then((d) => _bwcDepoisDeDecidir(rgm || d.rgm, d.msg || "Sincronizado."))
        .catch((e) => toast(e.message, "error", 7000));
}

function bwcMiniSync() {
    const v = (window.prompt("Mini-sync\n\nID do lead no Kommo (ex.: 21870683)\nou número do card no Bwipo com # (ex.: #4997)\nou RGM com 8 dígitos para escolher o lead:") || "").trim();
    if (!v) return;
    if (v.startsWith("#")) return bwcSyncCandidato({ number: v.replace(/\D/g, "") });
    const digits = v.replace(/\D/g, "");
    if (digits.length === 8) return bwcConsultarRgm(digits);
    if (digits) return bwcSyncCandidato({ lead_id: digits });
}

function _bwcPaint() {
    const rows = _bwcRankingRows();
    const banner = document.getElementById("bwc-cross");
    if (banner) {
        if (_bwcDia) {
            const p = _bwcDia.split("-");
            banner.classList.remove("hidden");
            banner.innerHTML = `Dia ${p[2]}/${p[1]} filtrando o ranking. <button type="button" class="underline" onclick="bwcLimparDia()">limpar</button>`;
        } else banner.classList.add("hidden");
    }
    document.getElementById("bwc-agentes-count").textContent = rows.length + " agentes";
    document.getElementById("bwc-ranking").innerHTML = rows.length ? rows.map((r, i) => `
        <tr class="hover:bg-slate-50 dark:hover:bg-slate-800/40 cursor-pointer" onclick="bwcAbrirAgente(${i})">
            <td class="text-center px-3 py-2 text-slate-500">${i + 1}</td>
            <td class="px-4 py-2 font-medium">${_bwcEsc(r.agente)}</td>
            <td class="px-4 py-2 text-right font-bold tabular-nums">${_bwcNum(r.matriculas)}</td>
            <td class="px-4 py-2 text-right tabular-nums">${r.intermediaria ? _bwcNum(r.intermediaria) : "—"}</td>
            <td class="px-4 py-2 text-right tabular-nums">${r.meta ? _bwcNum(r.meta) : "—"}</td>
            <td class="px-4 py-2 text-right tabular-nums">${r.supermeta ? _bwcNum(r.supermeta) : "—"}</td>
            <td class="px-4 py-2 text-right text-slate-500">${_bwcNivel(r)}</td>
            <td class="px-4 py-2 text-right tabular-nums">${_bwcNum(r.aberto)}</td>
            <td class="px-4 py-2 text-right tabular-nums text-slate-500">${_bwcNum(r.perdido)}</td>
            <td class="px-4 py-2 text-right tabular-nums">${_bwcNum(r.evasao)}</td>
            <td class="px-4 py-2 text-right tabular-nums">${(r.conv || 0).toLocaleString("pt-BR")}%</td>
        </tr>`).join("") : '<tr><td colspan="11" class="px-5 py-8 text-center text-slate-500">Nenhuma matrícula neste período.</td></tr>';
    const linhas = _bwcLinhas();
    const ev = document.getElementById("bwc-evasao-lista");
    const fp = document.getElementById("bwc-fora-lista");
    if (ev) ev.innerHTML = _bwcLista(linhas.filter((l) => l.situacao && l.situacao !== "EM CURSO"));
    if (fp) fp.innerHTML = _bwcLista(linhas.filter((l) => l.fora), true);
    return rows;
}

function _bwcEvasaoChips() {
    const chips = document.getElementById("bwc-evasao-chips");
    if (!chips) return;
    const linhas = _bwcLinhas().filter((l) => l.situacao && l.situacao !== "EM CURSO");
    const n = (re) => linhas.filter((l) => re.test(l.situacao || "")).length;
    const partes = [
        ["Cancelado", n(/cancel/i), "bg-rose-500/20 text-rose-200"],
        ["Trancado", n(/tranc/i), "bg-amber-500/20 text-amber-200"],
        ["Transferido", n(/transfer/i), "bg-violet-500/20 text-violet-200"],
    ].filter((p) => p[1]);
    chips.innerHTML = partes.map((p) =>
        `<span class="inline-block px-2 py-0.5 mr-1 rounded-full ${p[2]}">${p[0]}: ${p[1].toLocaleString("pt-BR")}</span>`
    ).join("");
}

function _bwcCarregarExtras() {
    const ytd = document.getElementById("bwc-ytd");
    const insc = document.getElementById("bwc-inscritos");
    if (ytd) ytd.textContent = "…";
    if (insc) insc.textContent = "…";
    fetch("/api/bwipo/painel/extras?" + _bwcQs().toString())
        .then((r) => r.json())
        .then((d) => {
            if (!d.ok) throw new Error(d.error || "falha");
            if (ytd) ytd.textContent = _bwcNum(d.ytd);
            if (insc) insc.textContent = _bwcNum(d.inscritos);
        })
        .catch(() => {
            if (ytd && ytd.textContent === "…") ytd.textContent = "—";
            if (insc && insc.textContent === "…") insc.textContent = "—";
        });
}

function bwcToggle(id) {
    const el = document.getElementById(id);
    if (el) el.classList.toggle("hidden");
}

function bwcLimparDia() {
    _bwcDia = null;
    const rows = _bwcPaint();
    if (_bwcPayload) _bwcCharts(_bwcPayload.evolucao || [], rows);
}

function _bwcTipoBadge(tipo) {
    const t = String(tipo || "").toUpperCase();
    if (!t) return "";
    if (t === "NOVA MATRICULA") return '<span class="inline-block px-1.5 py-0.5 rounded text-[10px] font-semibold bg-emerald-500/20 text-emerald-300">NOVA</span>';
    if (t === "RETORNO") return '<span class="inline-block px-1.5 py-0.5 rounded text-[10px] font-semibold bg-amber-500/20 text-amber-300">RETORNO</span>';
    if (t === "RECOMPRA") return '<span class="inline-block px-1.5 py-0.5 rounded text-[10px] font-semibold bg-blue-500/20 text-blue-300">RECOMPRA</span>';
    return `<span class="inline-block px-1.5 py-0.5 rounded text-[10px] font-semibold bg-slate-600/40 text-slate-400">${_bwcEsc(tipo)}</span>`;
}

function _bwcContagemCell(l) {
    if (!l.fora) return '<span class="text-slate-400">—</span>';
    if (!l.conta) {
        return _bwcIsAdmin()
            ? `<button type="button" onclick="bwcContarVenda('${_bwcEsc(l.rgm)}', true)" class="px-2 py-0.5 rounded text-[10px] font-semibold bg-orange-500/20 text-orange-300 border border-orange-500/40">Contar venda</button>`
            : '<span class="text-[10px] text-orange-400/70">Não conta</span>';
    }
    const desfazer = _bwcIsAdmin()
        ? ` <button type="button" onclick="bwcContarVenda('${_bwcEsc(l.rgm)}', false)" class="ml-1 text-[10px] text-slate-500 hover:text-red-400 underline">Desfazer</button>`
        : "";
    return `<span class="inline-flex items-center gap-1 px-2 py-0.5 rounded text-[10px] font-semibold bg-emerald-500/20 text-emerald-300 border border-emerald-500/30">✓ Contando</span>${desfazer}`;
}

function _bwcAgenteRow(l) {
    const rowCls = l.fora
        ? "hover:bg-slate-50 dark:hover:bg-white/[0.03] bg-orange-50 dark:bg-orange-500/5"
        : "hover:bg-slate-50 dark:hover:bg-white/[0.03]";
    const rgmTag = l.fora
        ? `<button type="button" class="font-mono text-orange-700 dark:text-orange-300 underline decoration-dotted" title="RGM fora do padrão do ciclo" onclick="bwcConsultarRgm('${_bwcEsc(l.rgm)}')">${_bwcEsc(l.rgm)} ⚠</button>`
        : `<button type="button" class="font-mono text-slate-700 dark:text-slate-300 underline decoration-dotted" onclick="bwcConsultarRgm('${_bwcEsc(l.rgm)}')">${_bwcEsc(l.rgm)}</button>`;
    return `<tr class="${rowCls}">
        <td class="px-3 py-2 whitespace-nowrap">${rgmTag}</td>
        <td class="px-3 py-2 text-[var(--text-primary)]">${_bwcEsc(l.nome)}</td>
        <td class="px-3 py-2 font-mono text-slate-600 dark:text-slate-400 whitespace-nowrap">${_bwcEsc(l.cpf || "")}</td>
        <td class="px-3 py-2 font-mono text-slate-600 dark:text-slate-400 whitespace-nowrap">${_bwcEsc(l.telefone || "")}</td>
        <td class="px-3 py-2 whitespace-nowrap">${_bwcTipoBadge(l.tipo)}</td>
        <td class="px-3 py-2 text-slate-700 dark:text-slate-300 whitespace-nowrap">${_bwcEsc(l.polo || "")}</td>
        <td class="px-3 py-2 text-slate-600 dark:text-slate-400 whitespace-nowrap">${_bwcEsc(l.nivel || "")}</td>
        <td class="px-3 py-2 font-mono text-blue-700 dark:text-blue-300 whitespace-nowrap">${_bwcDataBr(l.data)}</td>
        <td class="px-3 py-2 text-slate-700 dark:text-slate-300 max-w-[200px] truncate" title="${_bwcEsc(l.curso || "")}">${_bwcEsc(l.curso || "")}</td>
        <td class="px-3 py-2 whitespace-nowrap">${_bwcContagemCell(l)}</td>
    </tr>`;
}

function bwcAbrirAgente(nomeOuIdx) {
    const nome = typeof nomeOuIdx === "number" ? (_bwcRankingRows()[nomeOuIdx] || {}).agente : nomeOuIdx;
    if (!nome) return;
    const linhas = (_bwcPayload && _bwcPayload.linhas || []).filter((l) => l.agente === nome && (!_bwcDia || l.data === _bwcDia));
    const rk = ((_bwcPayload && _bwcPayload.ranking) || []).find((r) => r.agente === nome) || {};
    _bwcAgenteLinhas = linhas.map((l) => Object.assign({ cpf: "", telefone: "" }, l));
    _bwcAgenteNome = nome;
    _bwcAgenteUid = rk.user_id || null;
    const vendas = linhas.filter((l) => l.conta).length;
    const fora = linhas.filter((l) => l.fora).length;
    const ini = _bwcDataBr(document.getElementById("bwc-dt-ini")?.value);
    const fim = _bwcDataBr(document.getElementById("bwc-dt-fim")?.value);
    const titulo = vendas < linhas.length
        ? `${vendas} venda(s) · ${linhas.length} matrícula(s)`
        : `${linhas.length} matrícula(s)`;
    const totalLabel = vendas !== linhas.length
        ? `<span class="text-emerald-600 dark:text-emerald-400 font-bold">${vendas} vendas</span> <span class="text-slate-500">· ${linhas.length} matrículas listadas</span>`
        : `<span class="text-[var(--text-primary)] font-semibold">${linhas.length}</span>`;
    const outlier = fora
        ? `<span class="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-semibold bg-orange-500/20 text-orange-300 border border-orange-500/30">⚠ ${fora} RGM${fora > 1 ? "s" : ""} fora do padrão</span>`
        : '<span class="text-[10px] text-slate-500">Todos os RGMs dentro do padrão do ciclo</span>';
    const perf = _bwcAgenteUid
        ? `<button type="button" onclick="bwcFecharModal();navigateToPerformance(${_bwcAgenteUid})" class="text-xs bg-indigo-600 hover:bg-indigo-500 text-white px-3 py-1.5 rounded-lg" title="Ver painel motivacional completo deste agente">Ver Performance</button>`
        : "";
    const corpo = `
        <div class="mb-3 flex items-center justify-between gap-3 flex-wrap">
            <div class="flex items-center gap-2 flex-wrap text-xs">
                <span class="text-slate-600 dark:text-slate-400">Total: ${totalLabel}</span>
                <span class="text-slate-400">·</span>
                ${outlier}
            </div>
            <button type="button" onclick="bwcAgenteCsv()" class="text-xs bg-emerald-600 hover:bg-emerald-500 text-white px-3 py-1.5 rounded-lg shrink-0">Baixar CSV</button>
        </div>
        <div class="mb-3 flex items-center gap-2">
            <input id="bwc-agente-busca" type="text" placeholder="Buscar RGM ou nome" oninput="bwcFiltrarAgente()"
                onkeydown="if(event.key==='Enter')bwcFiltrarAgente()"
                class="input-glass px-3 py-2 text-sm flex-1 font-mono" />
            <button type="button" onclick="bwcFiltrarAgente()" class="px-4 py-2 rounded-lg bg-blue-600 hover:bg-blue-500 text-white text-sm font-semibold shrink-0">Buscar</button>
        </div>
        <div class="overflow-auto max-h-[52vh]">
            <table class="w-full text-xs">
                <thead class="sticky top-0 bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-400 uppercase tracking-wider text-[10px]">
                    <tr>
                        <th class="px-3 py-2 text-left">RGM</th>
                        <th class="px-3 py-2 text-left">Nome</th>
                        <th class="px-3 py-2 text-left">CPF</th>
                        <th class="px-3 py-2 text-left">Telefone</th>
                        <th class="px-3 py-2 text-left">Tipo</th>
                        <th class="px-3 py-2 text-left">Polo</th>
                        <th class="px-3 py-2 text-left">Nível</th>
                        <th class="px-3 py-2 text-left">Data Matrícula</th>
                        <th class="px-3 py-2 text-left">Curso</th>
                        <th class="px-3 py-2 text-left">Contagem</th>
                    </tr>
                </thead>
                <tbody id="bwc-agente-body" class="divide-y divide-slate-200 dark:divide-slate-700/30"></tbody>
            </table>
        </div>`;
    let modal = document.getElementById("bwc-modal");
    if (!modal) {
        modal = document.createElement("div");
        modal.id = "bwc-modal";
        modal.className = "fixed inset-0 z-[80] flex items-center justify-center bg-black/70 backdrop-blur-sm p-4";
        modal.addEventListener("click", (e) => { if (e.target === modal) bwcFecharModal(); });
        document.body.appendChild(modal);
    }
    modal.className = "fixed inset-0 z-[80] flex items-center justify-center bg-black/70 backdrop-blur-sm p-4";
    modal.innerHTML = `<div class="glass-card rounded-2xl shadow-2xl w-full max-w-5xl max-h-[90vh] flex flex-col overflow-hidden">
        <div class="px-5 py-4 border-b border-[var(--border)] flex items-center justify-between gap-3">
            <h3 class="text-sm font-bold text-[var(--text-primary)]">${_bwcEsc(nome)} · ${titulo} — período ${ini} a ${fim}</h3>
            <div class="flex items-center gap-2 shrink-0">${perf}
                <button type="button" onclick="bwcFecharModal()" class="text-slate-500 hover:text-[var(--text-primary)] text-lg leading-none">&times;</button>
            </div>
        </div>
        <div class="p-5 overflow-auto flex-1">${corpo}</div>
    </div>`;
    bwcFiltrarAgente();
    document.getElementById("bwc-agente-busca")?.focus();
    fetch("/api/bwipo/painel/contatos", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ rgms: linhas.map((l) => l.rgm) }),
    })
        .then((r) => r.json())
        .then((d) => {
            if (!d.ok || !document.getElementById("bwc-agente-body")) return;
            const mapa = d.contatos || {};
            _bwcAgenteLinhas.forEach((l) => {
                const c = mapa[l.rgm];
                if (!c) return;
                l.cpf = c.cpf || "";
                l.telefone = c.telefone || "";
            });
            bwcFiltrarAgente();
        })
        .catch(() => {});
}

let _bwcAgenteLinhas = [];
let _bwcAgenteNome = "";
let _bwcAgenteUid = null;

function bwcFiltrarAgente() {
    const body = document.getElementById("bwc-agente-body");
    if (!body) return;
    const q = (document.getElementById("bwc-agente-busca")?.value || "").trim().toLowerCase();
    const digits = q.replace(/\D/g, "");
    const rows = _bwcAgenteLinhas.filter((l) => {
        if (!q) return true;
        const blob = [l.rgm, l.nome, l.cpf, l.telefone, l.polo, l.curso, l.tipo].join(" ").toLowerCase();
        return blob.includes(q) || (digits.length >= 2 && String(l.rgm).includes(digits));
    });
    body.innerHTML = rows.length
        ? rows.map(_bwcAgenteRow).join("")
        : '<tr><td colspan="10" class="px-3 py-4 text-slate-500">Nenhum RGM nesse filtro.</td></tr>';
}

function bwcAgenteCsv() {
    const cols = ["rgm", "nome", "cpf", "telefone", "tipo", "polo", "nivel", "data", "curso", "situacao", "fonte"];
    const esc = (v) => `"${String(v ?? "").replace(/"/g, '""')}"`;
    const csv = [cols.join(";")].concat(_bwcAgenteLinhas.map((l) => cols.map((c) => esc(l[c])).join(";"))).join("\n");
    const a = document.createElement("a");
    a.href = URL.createObjectURL(new Blob(["\ufeff" + csv], { type: "text/csv;charset=utf-8" }));
    a.download = "matriculas_" + (_bwcAgenteNome || "agente").replace(/\W+/g, "_") + ".csv";
    a.click();
    URL.revokeObjectURL(a.href);
}

function _bwcRgmsRecorte() {
    return [...new Set(((_bwcPayload && _bwcPayload.linhas) || []).map((l) => l.rgm))];
}

function _bwcLinhaPorRgm() {
    const m = {};
    ((_bwcPayload && _bwcPayload.linhas) || []).forEach((l) => { m[l.rgm] = l; });
    return m;
}

function bwcAbrirDuplicatas() {
    _bwcModal("Duplicatas no Bwipo", "Carregando…", "");
    _bwcSend("/api/bwipo/painel/duplicatas", "POST", { rgms: _bwcRgmsRecorte() })
        .then((d) => {
            const lin = _bwcLinhaPorRgm();
            const corpo = d.duplicatas.length ? d.duplicatas.map((g) => `
                <div class="border-b border-slate-200 dark:border-slate-700">
                    <div class="px-4 pt-2"><button type="button" class="font-bold underline decoration-dotted" onclick="bwcConsultarRgm('${g.rgm}')">${g.rgm}</button> ${_bwcEsc((lin[g.rgm] || {}).nome || "")} <span class="text-slate-500">· ${g.count} negócios</span></div>
                    ${g.negocios.map((n) => `<div class="px-6 py-1 flex justify-between gap-3">
                        <span>#${n.number || "—"} ${_bwcEsc(n.title)}</span>
                        <span class="text-slate-500">${_bwcEsc(n.agente)} · <span class="${n.ganho ? "text-emerald-500" : n.perdido ? "text-rose-400" : ""}">${_bwcEsc(n.etapa)}</span></span>
                    </div>`).join("")}
                </div>`).join("") : '<p class="px-4 py-3 text-slate-500">Nenhum RGM do recorte com mais de um negócio no Pipeline Principal.</p>';
            _bwcModal("Duplicatas no Bwipo", `${_bwcNum(d.total)} RGMs do recorte com mais de um negócio no Pipeline Principal`, corpo);
        })
        .catch((e) => _bwcModal("Duplicatas no Bwipo", "Erro", `<p class="px-4 py-3 text-red-400">${_bwcEsc(e.message)}</p>`));
}

function bwcAbrirSemData() {
    _bwcModal("Matrículas sem data no Bwipo", "Carregando…", "");
    _bwcSend("/api/bwipo/painel/sem-data", "POST", { rgms: _bwcRgmsRecorte() })
        .then((d) => {
            const lin = _bwcLinhaPorRgm();
            const item = (rgm, dir) => `<div class="px-4 py-1.5 flex justify-between gap-3 border-b border-slate-200/60 dark:border-slate-700/30">
                <span><button type="button" class="font-bold underline decoration-dotted" onclick="bwcConsultarRgm('${rgm}')">${rgm}</button> ${_bwcEsc((lin[rgm] || {}).nome || "")}</span>
                <span class="text-slate-500">${dir}</span></div>`;
            const corpo = `<p class="px-4 py-2 font-semibold">Negócio no Bwipo sem data de matrícula (${_bwcNum(d.sem_data.length)})</p>`
                + (d.sem_data.map((s) => item(s.rgm, `#${s.number || "—"} · ${_bwcEsc(s.agente)} · ${_bwcEsc(s.etapa)}`)).join("") || '<p class="px-4 py-2 text-slate-500">Nenhum.</p>')
                + `<p class="px-4 py-2 font-semibold border-t border-slate-200 dark:border-slate-700">Matrícula sem negócio no Bwipo (${_bwcNum(d.sem_negocio.length)})</p>`
                + (d.sem_negocio.map((r) => item(r, `${_bwcEsc((lin[r] || {}).agente || "")} · ${_BWC_FONTE[(lin[r] || {}).fonte] || ""}`)).join("") || '<p class="px-4 py-2 text-slate-500">Nenhuma.</p>');
            _bwcModal("Matrículas sem data no Bwipo", "Matrícula do recorte (relatório SIAA) cruzada com o negócio no Bwipo", corpo);
        })
        .catch((e) => _bwcModal("Matrículas sem data no Bwipo", "Erro", `<p class="px-4 py-3 text-red-400">${_bwcEsc(e.message)}</p>`));
}

function bwcAbrirDiagnostico() {
    _bwcModal("Diagnóstico Kommo ↔ Bwipo", "Carregando…", "");
    fetch("/api/bwipo/painel/diagnostico")
        .then((r) => r.json())
        .then((d) => {
            if (!d.ok) throw new Error(d.error || "falha");
            const n = d.numeros || {};
            const linhas = (_bwcPayload && _bwcPayload.linhas) || [];
            const fontes = {};
            linhas.forEach((l) => { fontes[l.fonte] = (fontes[l.fonte] || 0) + 1; });
            const kv = (k, v) => `<div class="flex justify-between px-4 py-1 border-b border-slate-200/60 dark:border-slate-700/30"><span>${k}</span><b class="tabular-nums">${v}</b></div>`;
            const corpo = `<p class="px-4 pt-3 pb-1 font-semibold">Quem ficou com as vendas deste recorte (${_bwcNum(linhas.length)})</p>`
                + Object.entries(fontes).map(([k, v]) => kv(_BWC_FONTE[k] || k, _bwcNum(v))).join("")
                + `<p class="px-4 pt-3 pb-1 font-semibold">Espelho Bwipo</p>`
                + kv("Negócios", _bwcNum(n.negocios)) + kv("No Pipeline Principal", _bwcNum(n.negocios_principal))
                + kv("Com RGM", _bwcNum(n.negocios_com_rgm)) + kv("Sem responsável", _bwcNum(n.negocios_sem_dono))
                + kv("Responsável sem depara com o Kommo", _bwcNum(n.negocios_dono_sem_depara))
                + `<p class="px-4 pt-3 pb-1 font-semibold">Depara</p>`
                + kv("Leads Kommo casados", _bwcNum(n.depara_leads)) + kv("Negócios Bwipo casados", _bwcNum(n.depara_negocios))
                + Object.entries(d.depara_por_tipo || {}).map(([k, v]) => kv("· por " + k, _bwcNum(v))).join("")
                + kv("Consultores ligados", _bwcNum(n.depara_consultores)) + kv("Consultores tombados", _bwcNum(n.consultores_tombados))
                + kv("Decisões manuais de venda", _bwcNum(n.decisoes_manuais))
                + `<p class="px-4 pt-3 pb-1 font-semibold">Responsáveis do Bwipo sem depara</p>`
                + ((d.donos_sem_depara || []).map((x) => kv(_bwcEsc(x.agente), _bwcNum(x.negocios) + " negócios")).join("") || '<p class="px-4 py-1 text-slate-500">Nenhum.</p>')
                + `<p class="px-4 pt-3 pb-1 font-semibold">Últimos syncs</p>`
                + (d.syncs || []).map((s) => kv(`${_bwcEsc(s.entidade)} <span class="text-slate-500">${_bwcEsc(s.status || "")}${s.erro ? " · " + _bwcEsc(s.erro) : ""}</span>`,
                    s.em ? new Date(s.em).toLocaleString("pt-BR") : "—")).join("");
            _bwcModal("Diagnóstico Kommo ↔ Bwipo", "Saúde da ligação entre os dois CRMs", corpo);
        })
        .catch((e) => _bwcModal("Diagnóstico Kommo ↔ Bwipo", "Erro", `<p class="px-4 py-3 text-red-400">${_bwcEsc(e.message)}</p>`));
}

function bwcSyncAgentes() {
    toast("Sincronizando agentes…", "info", 2500);
    _bwcSend("/api/bwipo/painel/sync-agentes", "POST", {})
        .then((d) => {
            toast(`Kommo: ${d.kommo_users ?? "—"} usuários · Bwipo ligados: ${d.depara} · sem depara: ${d.bwipo_sem_depara}`, "success", 7000);
            bwcAtualizar();
        })
        .catch((e) => toast(e.message, "error"));
}

let _bwcCons = null;

function bwcAbrirConsultores() {
    _bwcModal("Editar consultores", "Carregando…", "");
    fetch("/api/bwipo/painel/consultores")
        .then((r) => r.json())
        .then((d) => {
            if (!d.ok) throw new Error(d.error || "falha");
            _bwcCons = d;
            _bwcPintarConsultores();
        })
        .catch((e) => _bwcModal("Editar consultores", "Erro", `<p class="px-4 py-3 text-red-400">${_bwcEsc(e.message)}</p>`));
}

function _bwcPintarConsultores(filtro) {
    const d = _bwcCons;
    const f = (filtro || "").toLowerCase();
    const opts = d.consultores.map((k) => `<option value="${k.id}">${_bwcEsc(k.display_name || k.name)}</option>`).join("");
    const lista = d.consultores
        .filter((k) => !f || (k.name + " " + (k.display_name || "") + " " + k.email).toLowerCase().includes(f))
        .map((k) => {
            const adm = k.id === d.admin_sistema_uid;
            const bw = k.bwipo.length
                ? k.bwipo.map((b) => `<span class="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-blue-500/10">${_bwcEsc(b.name)} · ${_bwcNum(b.deals)}${b.match === "manual" ? " · manual" : ""}
                    <button type="button" title="Desligar" onclick="bwcLigarBwipo('${_bwcEsc(b.id)}', null)">×</button></span>`).join(" ")
                : '<span class="text-slate-500">sem usuário no Bwipo</span>';
            return `<div class="px-4 py-2 border-b border-slate-200/60 dark:border-slate-700/30 ${k.excluido || k.hidden ? "opacity-60" : ""}">
                <div class="flex flex-wrap items-center gap-2">
                    <input value="${_bwcEsc(k.display_name || "")}" placeholder="${_bwcEsc(k.name)}" data-bwc-nome="${k.id}" class="input-glass px-2 py-1 text-xs w-44">
                    <span class="text-slate-500 text-[10px]">Kommo #${k.id}</span>
                    <label class="flex items-center gap-1"><input type="checkbox" data-bwc-oculto="${k.id}" ${k.hidden ? "checked" : ""}> ocultar</label>
                    ${adm ? "" : `<label class="flex items-center gap-1"><input type="checkbox" data-bwc-excluir="${k.id}" ${k.excluido ? "checked" : ""}> excluir → Admin Sistema</label>`}
                    <button type="button" onclick="bwcSalvarConsultor(${k.id})" class="px-2 py-0.5 rounded border border-blue-500/40 text-blue-500">Salvar</button>
                    <button type="button" onclick="bwcRestaurarConsultor(${k.id})" class="px-2 py-0.5 rounded border border-slate-400/40">Restaurar</button>
                    <label class="flex items-center gap-1 ml-auto">No Bwipo desde
                        <input type="date" value="${k.tombado_desde || ""}" onchange="bwcTombar(${k.id}, this.value)" class="input-glass px-2 py-0.5 text-xs"></label>
                </div>
                <div class="mt-1 text-[11px]">${bw}</div>
            </div>`;
        }).join("");
    const semDepara = d.bwipo_sem_depara.length
        ? d.bwipo_sem_depara.map((b) => `<div class="px-4 py-1.5 flex items-center justify-between gap-2 border-b border-slate-200/60 dark:border-slate-700/30">
            <span>${_bwcEsc(b.name)} <span class="text-slate-500">${_bwcEsc(b.email)} · ${_bwcNum(b.deals)} negócios</span></span>
            <select onchange="if(this.value)bwcLigarBwipo('${_bwcEsc(b.id)}', this.value)" class="input-glass px-2 py-0.5 text-xs"><option value="">ligar a…</option>${opts}</select>
        </div>`).join("")
        : '<p class="px-4 py-2 text-slate-500">Todos os responsáveis do Bwipo estão ligados a um consultor do Kommo.</p>';
    _bwcModal("Editar consultores",
        "O consultor é o usuário do Kommo: é ele que carrega meta e histórico. Ligue o usuário do Bwipo a ele e marque a data em que passou a trabalhar no Bwipo.",
        `<div class="px-4 py-2"><input id="bwc-cons-busca" value="${_bwcEsc(filtro || "")}" oninput="_bwcPintarConsultores(this.value)" placeholder="Buscar consultor" class="input-glass px-2 py-1 text-xs w-60"></div>`
        + `<p class="px-4 pt-2 pb-1 font-semibold">Responsáveis do Bwipo sem consultor (${d.bwipo_sem_depara.length})</p>${semDepara}`
        + `<p class="px-4 pt-3 pb-1 font-semibold">Consultores</p>${lista}`);
    const busca = document.getElementById("bwc-cons-busca");
    if (busca && filtro !== undefined) {
        busca.focus();
        busca.setSelectionRange(busca.value.length, busca.value.length);
    }
}

function _bwcConsRecarregar(msg) {
    toast(msg, "success");
    const filtro = document.getElementById("bwc-cons-busca")?.value || "";
    fetch("/api/bwipo/painel/consultores").then((r) => r.json()).then((d) => {
        if (d.ok) { _bwcCons = d; _bwcPintarConsultores(filtro); }
    });
    _bwcConsMudou = true;
}

let _bwcConsMudou = false;

function bwcFecharModal() {
    document.getElementById("bwc-modal")?.remove();
    if (_bwcConsMudou) {
        _bwcConsMudou = false;
        bwcAtualizar();
    }
}

function bwcSalvarConsultor(uid) {
    const nome = document.querySelector(`[data-bwc-nome="${uid}"]`)?.value || "";
    const hidden = !!document.querySelector(`[data-bwc-oculto="${uid}"]`)?.checked;
    const excluir = !!document.querySelector(`[data-bwc-excluir="${uid}"]`)?.checked;
    if (excluir && !window.confirm("Excluir este consultor? As vendas dele passam a contar para o Admin Sistema.")) return;
    _bwcSend(`/api/comercial-rgm/consultores/${uid}`, "POST", { display_name: nome, hidden, excluir })
        .then(() => _bwcConsRecarregar("Consultor salvo."))
        .catch((e) => toast(e.message, "error"));
}

function bwcRestaurarConsultor(uid) {
    _bwcSend(`/api/comercial-rgm/consultores/${uid}`, "DELETE", {})
        .then(() => _bwcConsRecarregar("Consultor restaurado."))
        .catch((e) => toast(e.message, "error"));
}

function bwcTombar(uid, desde) {
    const txt = desde
        ? `A partir de ${desde.split("-").reverse().join("/")}, as matrículas deste consultor passam a vir do Bwipo. Confirmar?`
        : "Tirar a data? O consultor volta a ser lido do Kommo.";
    if (!window.confirm(txt)) { _bwcPintarConsultores(document.getElementById("bwc-cons-busca")?.value || ""); return; }
    _bwcSend("/api/bwipo/painel/tombamento", "POST", { kommo_user_id: uid, desde: desde || null })
        .then(() => _bwcConsRecarregar(desde ? "Data de tombamento gravada." : "Consultor voltou para o Kommo."))
        .catch((e) => toast(e.message, "error"));
}

function bwcLigarBwipo(bwipoId, kommoId) {
    const k = kommoId ? (_bwcCons.consultores.find((c) => String(c.id) === String(kommoId)) || {}) : null;
    if (!kommoId && !window.confirm("Desligar este usuário do Bwipo do consultor?")) return;
    _bwcSend("/api/bwipo/painel/user-depara", "POST", { bwipo_user_id: bwipoId, kommo_user_id: kommoId, kommo_name: k ? k.name : null })
        .then(() => _bwcConsRecarregar(kommoId ? "Usuário ligado." : "Usuário desligado."))
        .catch((e) => toast(e.message, "error"));
}

let _bwcMetas = null;

function bwcAbrirMetas() {
    const ini = document.getElementById("bwc-dt-ini")?.value || "";
    const fim = document.getElementById("bwc-dt-fim")?.value || "";
    _bwcModal("Metas", "Carregando…", "");
    fetch(`/api/comercial-rgm/metas?dt_ini=${ini}&dt_fim=${fim}`)
        .then((r) => r.json())
        .then((d) => {
            if (!d.ok) throw new Error(d.error || "falha");
            _bwcMetas = d;
            _bwcPintarMetas(ini, fim);
        })
        .catch((e) => _bwcModal("Metas", "Erro", `<p class="px-4 py-3 text-red-400">${_bwcEsc(e.message)}</p>`));
}

function _bwcPintarMetas(ini, fim) {
    const d = _bwcMetas;
    const dataBr = (s) => (s ? s.split("-").reverse().join("/") : "—");
    const camps = (d.premiacao_campanhas || []).filter((c) => c.ativa && (!ini || !fim || (c.dt_inicio <= fim && c.dt_fim >= ini)));
    const campHtml = camps.length ? camps.map((c) => `<div class="px-4 py-1.5 border-b border-slate-200/60 dark:border-slate-700/30">
        <b>${_bwcEsc(c.nome)}</b> <span class="text-slate-500">${dataBr(c.dt_inicio)} a ${dataBr(c.dt_fim)}</span><br>
        <span class="text-slate-500">Padrão: interm. ${c.metas_padrao.meta_intermediaria ?? "—"} · meta ${c.metas_padrao.meta ?? "—"} · super ${c.metas_padrao.supermeta ?? "—"} · ${c.pcm_totais.agentes} agentes com meta própria</span>
    </div>`).join("") : '<p class="px-4 py-2 text-slate-500">Nenhuma campanha ativa no período.</p>';
    const rk = ((_bwcPayload && _bwcPayload.ranking) || []).filter((r) => r.user_id);
    const rkHtml = rk.map((r) => `<div class="px-4 py-1 flex justify-between border-b border-slate-200/60 dark:border-slate-700/30">
        <span>${_bwcEsc(r.agente)}</span><span class="tabular-nums text-slate-500">${_bwcNum(r.matriculas)} · interm. ${r.intermediaria || "—"} · meta ${r.meta || "—"} · super ${r.supermeta || "—"}</span></div>`).join("");
    const metas = d.metas || [];
    const metasHtml = metas.length ? metas.map((m) => `<div class="px-4 py-1.5 flex flex-wrap items-center gap-2 border-b border-slate-200/60 dark:border-slate-700/30">
        <span class="w-40">${_bwcEsc(m.user_name || m.user_id)}</span>
        <span class="text-slate-500">${dataBr(m.dt_inicio)} a ${dataBr(m.dt_fim)} · ${_bwcEsc(m.categoria)}</span>
        <input type="number" data-bwc-mi="${m.id}" value="${m.meta_intermediaria}" class="input-glass px-1 py-0.5 text-xs w-16" title="Intermediária">
        <input type="number" data-bwc-mm="${m.id}" value="${m.meta}" class="input-glass px-1 py-0.5 text-xs w-16" title="Meta">
        <input type="number" data-bwc-ms="${m.id}" value="${m.supermeta}" class="input-glass px-1 py-0.5 text-xs w-16" title="Super">
        <button type="button" onclick="bwcSalvarMeta(${m.id})" class="px-2 py-0.5 rounded border border-blue-500/40 text-blue-500">Salvar</button>
        <button type="button" onclick="bwcExcluirMeta(${m.id})" class="px-2 py-0.5 rounded border border-rose-500/40 text-rose-400">Excluir</button>
    </div>`).join("") : '<p class="px-4 py-2 text-slate-500">Nenhuma meta avulsa no período.</p>';
    const agOpts = ((_bwcPayload && _bwcPayload.ranking) || []).filter((r) => r.user_id)
        .map((r) => `<option value="${r.user_id}">${_bwcEsc(r.agente)}</option>`).join("");
    const campAlvo = camps[0];
    const novo = `<div class="px-4 py-3 space-y-2">
        <p class="text-slate-500">Se o período cair na campanha ativa, salva a meta desse consultor na Premiação — o mesmo número do ranking. Campanha nova (nome, datas, equipe e R$) se cria na aba Premiação.</p>
        <div class="grid grid-cols-2 sm:grid-cols-4 gap-2">
            <label class="col-span-2 text-[11px] text-slate-500">Consultor
                <select id="bwc-nm-ag" class="input-glass mt-0.5 w-full px-2 py-1 text-xs">${agOpts}</select>
            </label>
            <label class="text-[11px] text-slate-500">De
                <input type="date" id="bwc-nm-ini" value="${ini}" class="input-glass mt-0.5 w-full px-2 py-1 text-xs">
            </label>
            <label class="text-[11px] text-slate-500">Até
                <input type="date" id="bwc-nm-fim" value="${fim}" class="input-glass mt-0.5 w-full px-2 py-1 text-xs">
            </label>
            <label class="text-[11px] text-slate-500">Intermediária
                <input type="number" id="bwc-nm-i" min="0" class="input-glass mt-0.5 w-full px-2 py-1 text-xs">
            </label>
            <label class="text-[11px] text-slate-500">Meta
                <input type="number" id="bwc-nm-m" min="0" class="input-glass mt-0.5 w-full px-2 py-1 text-xs">
            </label>
            <label class="text-[11px] text-slate-500">Supermeta
                <input type="number" id="bwc-nm-s" min="0" class="input-glass mt-0.5 w-full px-2 py-1 text-xs">
            </label>
        </div>
        <div class="flex flex-wrap gap-2 pt-1">
            <button type="button" onclick="bwcNovaMeta()" class="px-3 py-1.5 rounded-lg bg-emerald-600 text-white font-semibold">Salvar meta</button>
            <button type="button" onclick="bwcIrPremiacao()" class="px-3 py-1.5 rounded-lg border border-slate-300 dark:border-slate-600">Abrir Premiação</button>
        </div>
        <p class="text-slate-500">${campAlvo ? `Período atual cai em <b>${_bwcEsc(campAlvo.nome)}</b>. Salvar altera a meta do consultor nessa campanha.` : "Nenhuma campanha cobre este período. Salvar grava uma meta avulsa."}</p>
    </div>`;
    _bwcModal("Metas",
        "O ranking lê a Premiação: meta do agente na campanha, depois o padrão da campanha, depois a meta avulsa.",
        `<p class="px-4 pt-3 pb-1 font-semibold">Campanha ativa</p>${campHtml}`
        + `<p class="px-4 pt-3 pb-1 font-semibold">Meta aplicada no ranking</p>${rkHtml || '<p class="px-4 py-2 text-slate-500">Sem ranking carregado.</p>'}`
        + `<p class="px-4 pt-3 pb-1 font-semibold">Metas avulsas (sem campanha)</p>${metasHtml}`
        + `<p class="px-4 pt-3 pb-1 font-semibold">Salvar meta do consultor</p>${novo}`);
}

function _bwcMetasRecarregar(msg) {
    toast(msg, "success");
    bwcAbrirMetas();
    bwcAtualizar();
}

function bwcSalvarMeta(id) {
    const v = (a) => Number(document.querySelector(`[data-bwc-${a}="${id}"]`)?.value || 0);
    _bwcSend(`/api/comercial-rgm/metas/${id}`, "PUT", { meta_intermediaria: v("mi"), meta: v("mm"), supermeta: v("ms") })
        .then(() => _bwcMetasRecarregar("Meta salva."))
        .catch((e) => toast(e.message, "error"));
}

function bwcExcluirMeta(id) {
    if (!window.confirm("Excluir esta meta?")) return;
    _bwcSend(`/api/comercial-rgm/metas/${id}`, "DELETE", {})
        .then(() => _bwcMetasRecarregar("Meta excluída."))
        .catch((e) => toast(e.message, "error"));
}

function bwcIrPremiacao() {
    bwcFecharModal();
    if (typeof navigate === "function") navigate("premiacao_admin");
}

function bwcNovaMeta() {
    const sel = document.getElementById("bwc-nm-ag");
    const val = (id) => document.getElementById(id)?.value || "";
    if (!sel || !sel.value) {
        toast("Escolha o consultor.", "error");
        return;
    }
    const body = {
        user_id: sel.value,
        user_name: sel.options[sel.selectedIndex]?.text || "",
        dt_inicio: val("bwc-nm-ini"),
        dt_fim: val("bwc-nm-fim"),
        meta_intermediaria: Number(val("bwc-nm-i") || 0),
        meta: Number(val("bwc-nm-m") || 0),
        supermeta: Number(val("bwc-nm-s") || 0),
    };
    if (!body.dt_inicio || !body.dt_fim) {
        toast("Informe o período da meta.", "error");
        return;
    }
    _bwcSend("/api/bwipo/painel/meta-agente", "POST", body)
        .then((d) => {
            const msg = d.onde === "campanha"
                ? `Meta gravada na campanha ${d.campanha}. A Premiação usa o mesmo número.`
                : "Meta avulsa gravada. Neste período não há campanha.";
            _bwcMetasRecarregar(msg);
        })
        .catch((e) => toast(e.message, "error"));
}

function bwcConsultarRgm(rgmArg) {
    const q = String(rgmArg || document.getElementById("bwc-rgm")?.value || "").replace(/\D/g, "");
    if (q.length !== 8) {
        toast("Informe um RGM com 8 dígitos.", "error");
        return;
    }
    fetch("/api/bwipo/painel/rgm?rgm=" + q)
        .then((r) => r.json())
        .then((d) => {
            if (!d.ok) throw new Error(d.error || "falha");
            const linha = ((_bwcPayload && _bwcPayload.linhas) || []).find((l) => l.rgm === q);
            const recorte = linha
                ? `${_bwcEsc(linha.nome)} · ${_bwcEsc(linha.situacao)} · ${_bwcEsc(linha.data)}${linha.fora ? " · fora do padrão" : ""}`
                : "Fora das matrículas deste filtro";
            const res = d.resolucao
                ? `<p>Decisão manual: <b>${_bwcEsc(d.resolucao.user_name || d.resolucao.user_id)}</b> (${_bwcEsc(d.resolucao.por || "manual")}) <button type="button" class="underline text-slate-500" onclick="bwcDesfazerVenda('${q}')">desfazer</button></p>`
                : "";
            const contar = _bwcIsAdmin()
                ? `<button type="button" onclick="bwcContarVenda('${q}', ${!d.contar_venda})" class="px-2 py-0.5 rounded text-[10px] border border-emerald-500/40 text-emerald-600 dark:text-emerald-300">${d.contar_venda ? "Deixar de contar (fora do padrão)" : "Contar venda mesmo fora do padrão"}</button>`
                : "";
            const topo = `<div class="px-4 py-3 space-y-1 border-b border-slate-200 dark:border-slate-700">
                <p>Venda com <b>${_bwcEsc(d.agente || "—")}</b> <span class="text-slate-500">pela regra: ${_BWC_FONTE[d.fonte] || d.fonte}</span></p>
                ${res}
                <div>${contar}</div>
            </div>`;
            _bwcModal("RGM " + q, recorte, topo + _bwcCandidatos(q, d.candidatos || [], d.resolucao));
        })
        .catch((e) => toast(e.message, "error"));
}

function _bwcCharts(evolucao, ranking) {
    if (typeof Chart === "undefined") return;
    const dias = evolucao.map((e) => {
        const p = (e.dia || "").slice(0, 10).split("-");
        return p.length === 3 ? p[2] + "/" + p[1] : e.dia;
    });
    const ctx1 = document.getElementById("bwc-chart-dia");
    if (_bwcChartDia) _bwcChartDia.destroy();
    _bwcChartDia = new Chart(ctx1, {
        type: "line",
        data: {
            labels: dias,
            datasets: [{
                label: "Matrículas",
                data: evolucao.map((e) => e.total),
                borderColor: "#3b82f6",
                backgroundColor: "rgba(59,130,246,0.15)",
                fill: true,
                tension: 0.25,
            }],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            onClick: (_evt, els) => {
                if (!els.length || !_bwcPayload) return;
                const dia = (_bwcPayload.evolucao || [])[els[0].index];
                if (!dia) return;
                _bwcDia = String(dia.dia).slice(0, 10);
                const rows = _bwcPaint();
                _bwcCharts(_bwcPayload.evolucao || [], rows);
            },
        },
    });
    const top = ranking.filter((r) => r.matriculas > 0).slice(0, 15);
    const ctx2 = document.getElementById("bwc-chart-agentes");
    if (_bwcChartAg) _bwcChartAg.destroy();
    _bwcChartAg = new Chart(ctx2, {
        type: "bar",
        data: {
            labels: top.map((r) => r.agente),
            datasets: [{ label: "Matrículas", data: top.map((r) => r.matriculas), backgroundColor: "#00346f" }],
        },
        options: {
            indexAxis: "y",
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
        },
    });
}
