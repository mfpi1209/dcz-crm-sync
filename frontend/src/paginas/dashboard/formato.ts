import type { DadosFunil, FiltrosDash } from "./tipos";

/** Mesmos nomes do _TIPO_LABELS antigo. */
export const TIPO_ROTULOS: Record<string, string> = {
  novos_agg: "Novos (Calouros+Regresso+Recompra)",
  novos: "Calouros",
  rematricula: "Rematrículas",
  regresso: "Regresso",
  recompra: "Recompra",
};

export const FILTROS_VAZIOS: FiltrosDash = { ciclo: "", nivel: "", de: "", ate: "", situacao: "", tipo: "", polo: "" };

export const POS = "Pós-Graduação";

export function numero(n: unknown): string {
  return Number(n || 0).toLocaleString("pt-BR");
}

/** Uma casa decimal fixa, com vírgula (a antiga usava toFixed(1)). */
export function decimal1(n: number): string {
  return n.toLocaleString("pt-BR", { minimumFractionDigits: 1, maximumFractionDigits: 1 });
}

/** Variação entre ciclos/períodos (pct() do renderCicloMaster). */
export function variacao(atual: number, anterior: number): { texto: string; sinal: "sobe" | "desce" | "neutro" } {
  if (!anterior && !atual) return { texto: "—", sinal: "neutro" };
  if (!anterior) return { texto: "+100%", sinal: "sobe" };
  const d = ((atual - anterior) / anterior) * 100;
  return { texto: `${d >= 0 ? "+" : ""}${decimal1(d)}%`, sinal: d > 0 ? "sobe" : d < 0 ? "desce" : "neutro" };
}

const MESES = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"];

/** Rótulo do eixo do gráfico (_formatLabel). */
export function rotuloPeriodo(periodo: string, granularidade: "month" | "day"): string {
  if (granularidade === "month") {
    const [a, m] = periodo.split("-");
    return `${MESES[parseInt(m, 10) - 1]} ${a}`;
  }
  const [, m, d] = periodo.split("-");
  return `${parseInt(d, 10)}/${parseInt(m, 10)}`;
}

/** Eixo Y: 1500 → "1.5k", 2000 → "2k" (igual à antiga). */
export function rotuloEixo(v: number): string | number {
  return v >= 1000 ? (v / 1000).toFixed(v % 1000 ? 1 : 0) + "k" : v;
}

/** Situação sem acento e minúscula (_normSitKey). */
export function normSit(s: string | null | undefined): string {
  return String(s || "")
    .normalize("NFD")
    .replace(/[̀-ͯ]/g, "")
    .trim()
    .toLowerCase();
}

const ORDEM_SITUACAO = ["em curso", "cancelado", "trancado", "transferido"];

/** Em Curso, Cancelado, Trancado, Transferido e depois as demais (ordem do servidor). */
export function ordenarSituacoes(dados: Record<string, number>): string[] {
  const chaves = Object.keys(dados);
  return ORDEM_SITUACAO.map((s) => chaves.find((k) => k.toLowerCase() === s))
    .filter((k): k is string => !!k)
    .concat(chaves.filter((k) => !ORDEM_SITUACAO.includes(k.toLowerCase())));
}

// ── Polos: nomes canônicos (cópia de normalizePoloDisplay, static/js/utils.js) ──

function chavePolo(polo: string): string {
  let p = String(polo).normalize("NFD").replace(/[̀-ͯ]/g, "").toLowerCase();
  p = p.replace(/^\d+\s*[-–]\s*/, "");
  p = p.replace(/^ceb\s+/, "");
  p = p.replace(/^polo\s+sp_/, "");
  p = p.replace(/^polo\s+/, "");
  p = p.replace(/\([^)]*\)/g, "");
  p = p.replace(/\s+/g, " ").trim();
  return p;
}

export function nomePolo(polo: string): string {
  if (!polo || !String(polo).trim()) return "";
  const k = chavePolo(polo);
  if (k.includes("taboao") || k.includes("taboa")) {
    if (k.includes("mituzi") || k.includes("jardim")) return "Taboão da Serra_Jardim Mituzi";
    return "Taboão da Serra_Centro";
  }
  if (k.includes("barra funda")) return "Barra Funda";
  if (k.includes("sapopemba")) return "Sapopemba";
  if (k.includes("vila prudente")) return "Vila Prudente";
  if (k.includes("santana")) return "Santana 2";
  if (k.includes("ibirapuera")) return "Ibirapuera";
  if (k.includes("morumbi")) return "Morumbi";
  if (k.includes("campinas")) return "Campinas";
  if (k.includes("capivari")) return "Capivari";
  if (k.includes("itapira")) return "Itapira";
  if (k.includes("freguesia")) return "Freguesia do Ó";
  if (k.includes("vila mariana")) return "Vila Mariana";
  const limpo = k.replace(/_/g, " ");
  if (!limpo) return String(polo).trim();
  return limpo
    .split(" ")
    .map((w, i) => (["da", "de", "do", "dos", "das", "e"].includes(w) && i > 0 ? w : w.charAt(0).toUpperCase() + w.slice(1)))
    .join(" ");
}

/** Junta os polos pelo nome canônico, do maior para o menor (mergePoloBreakdown). */
export function juntarPolos(porPolo: Record<string, number> | undefined): [string, number][] {
  const out: Record<string, number> = {};
  for (const [bruto, n] of Object.entries(porPolo || {})) {
    const nome = nomePolo(bruto) || bruto;
    out[nome] = (out[nome] || 0) + (n || 0);
  }
  return Object.entries(out).sort((a, b) => b[1] - a[1]);
}

// ── Parâmetros das rotas, na mesma ordem da tela antiga ──

export function urlAlunos(f: FiltrosDash): string {
  const p = new URLSearchParams();
  if (f.ciclo) p.set("ciclo", f.ciclo);
  if (f.de) p.set("from", f.de);
  if (f.ate) p.set("to", f.ate);
  if (f.nivel) p.set("nivel", f.nivel);
  if (f.situacao) p.set("situacao", f.situacao);
  if (f.tipo) p.set("tipo", f.tipo);
  if (f.polo) p.set("polo", f.polo);
  return "/api/dashboard/students?" + p.toString();
}

/** Mês aberto no gráfico (clique num ponto do gráfico mensal). */
export interface Detalhe {
  mes: string; // AAAA-MM
  de: string;
  ate: string;
}

export function detalheDoMes(mes: string): Detalhe {
  const [a, m] = mes.split("-");
  const ultimo = new Date(parseInt(a, 10), parseInt(m, 10), 0).getDate();
  return { mes, de: `${a}-${m}-01`, ate: `${a}-${m}-${String(ultimo).padStart(2, "0")}` };
}

export function urlEvolucao(f: FiltrosDash, detalhe: Detalhe | null): string {
  const p = new URLSearchParams({ granularity: detalhe ? "day" : "month" });
  if (f.nivel) p.set("nivel", f.nivel);
  if (f.ciclo) p.set("ciclo", f.ciclo);
  if (f.situacao) p.set("situacao", f.situacao);
  if (f.tipo) p.set("tipo", f.tipo);
  if (f.polo) p.set("polo", f.polo);
  const de = detalhe ? detalhe.de : f.de;
  const ate = detalhe ? detalhe.ate : f.ate;
  if (de) p.set("from", de);
  if (ate) p.set("to", ate);
  return "/api/dashboard/timeline?" + p.toString();
}

/** Saúde financeira: sem período (como a antiga). */
export function urlSaude(f: FiltrosDash): string {
  const p = new URLSearchParams();
  if (f.tipo) p.set("tipo", f.tipo);
  if (f.situacao) p.set("situacao", f.situacao);
  if (f.polo) p.set("polo", f.polo);
  if (f.nivel) p.set("nivel", f.nivel);
  if (f.ciclo) p.set("ciclo", f.ciclo);
  const q = p.toString();
  return "/api/lista-alunos/latest" + (q ? "?" + q : "");
}

/** Transferências recebidas: sem situação (como a antiga). */
export function urlTransferencias(f: FiltrosDash): string {
  const p = new URLSearchParams();
  if (f.ciclo) p.set("ciclo", f.ciclo);
  if (f.nivel) p.set("nivel", f.nivel);
  if (f.de) p.set("from", f.de);
  if (f.ate) p.set("to", f.ate);
  if (f.polo) p.set("polo", f.polo);
  if (f.tipo) p.set("tipo", f.tipo);
  return "/api/dashboard/inbound-transfers?" + p.toString();
}

export function urlCiclos(nivel: string): string {
  return "/api/dashboard/ciclos" + (nivel ? "?" + new URLSearchParams({ nivel }).toString() : "");
}

/** Texto curto dos filtros (badge da timeline antiga): ciclo · situação · tipo · polo · nível. */
export function resumoFiltros(f: FiltrosDash): string {
  const partes: string[] = [];
  if (f.ciclo) partes.push("Ciclo " + f.ciclo);
  if (f.situacao) partes.push(f.situacao);
  if (f.tipo) partes.push(TIPO_ROTULOS[f.tipo] || f.tipo);
  if (f.polo) partes.push(f.polo);
  if (f.nivel) partes.push(f.nivel);
  return partes.join(" · ");
}

/** Barra "Filtrando por" (_stuUpdateActiveFilterBar). */
export function textoFiltrando(f: FiltrosDash): string {
  const partes: string[] = [];
  if (f.tipo) partes.push("Tipo: " + (TIPO_ROTULOS[f.tipo] || f.tipo));
  if (f.situacao) partes.push("Situação: " + f.situacao);
  if (f.polo) partes.push("Polo: " + f.polo);
  if (f.de || f.ate) partes.push(`Período: ${f.de || "…"} → ${f.ate || "…"}`);
  if (f.nivel) partes.push("Nível: " + f.nivel);
  return partes.length ? "Filtrando por: " + partes.join(" · ") : "";
}

/** Texto do "!" dos cartões (_stuExplainAppliedFilters). */
export function explicarFiltros(base: string, f: FiltrosDash): string {
  const partes: string[] = [];
  if (f.ciclo) partes.push(`ciclo ${f.ciclo}`);
  if (f.nivel) partes.push(`nível ${f.nivel}`);
  if (f.de || f.ate) partes.push(`período ${f.de || "…"} até ${f.ate || "…"}`);
  if (f.polo) partes.push(`polo ${f.polo}`);
  if (f.situacao) partes.push(`situação ${f.situacao}`);
  return `${base} ${partes.length ? "Filtros ativos: " + partes.join(" · ") + "." : "Sem filtro extra além do card selecionado."}`;
}

// ── Funil ──

/** Erro do Bwipo/Kommo legível (_dashFmtKommoErr). */
export function erroKommo(msg: unknown): string {
  if (!msg) return "";
  const s = String(msg);
  if (/<html|<!doctype/i.test(s)) {
    return /403|forbidden/i.test(s) ? "O CRM recusou a consulta." : "Resposta inválida do Bwipo.";
  }
  return s.length > 160 ? s.slice(0, 160) + "…" : s;
}

/** Legenda de origem do funil (Espelho Bwipo / Bwipo hh:mm · atualizando…). */
export function origemFunil(d: DadosFunil): string {
  let rotulo = "";
  if (d.source === "db") {
    rotulo = "Espelho Bwipo";
    if (d.synced_at) rotulo += " (" + d.synced_at + ")";
  } else if (d.fetched_at) {
    rotulo = "Bwipo " + d.fetched_at;
    if (d.stale) rotulo += " · atualizando…";
  }
  if (d.live_error) rotulo += (rotulo ? " · " : "") + d.live_error;
  return rotulo;
}

/** 1500 → "1.5k", 12000 → "12k" (_fmtKCount). */
export function contagemK(n: number): string {
  n = Number(n) || 0;
  if (n >= 1000) {
    const v = n / 1000;
    return (v >= 10 ? v.toFixed(0) : v.toFixed(1).replace(/\.0$/, "")) + "k";
  }
  return n.toLocaleString("pt-BR");
}
