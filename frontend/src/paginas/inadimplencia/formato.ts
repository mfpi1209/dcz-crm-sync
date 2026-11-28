import type { Faixa, Filtros } from "./tipos";

/** Mesmas formatações de static/js/inadimplencia.js. */

export function fmt(n: number | null | undefined): string {
  if (n == null) return "—";
  return Number(n).toLocaleString("pt-BR");
}

export function fmtPct(n: number | null | undefined): string {
  if (n == null) return "—";
  return Number(n).toFixed(2).replace(".", ",") + "%";
}

/** "2026-11-27..." → "27/11/2026" (só os 10 primeiros caracteres, como a antiga). */
export function fmtData(iso: string | null | undefined): string {
  if (!iso) return "—";
  const [y, m, d] = String(iso).slice(0, 10).split("-");
  return `${d}/${m}/${y}`;
}

/** "2026-11-27" → "27/11". */
export function diaMes(iso: string | null | undefined): string {
  if (!iso) return "—";
  const [, m, d] = iso.split("-");
  return `${d}/${m}`;
}

export function sinal(n: number): string {
  return n > 0 ? "+" : "";
}

/** Classe de cor da variação: queda é boa (verde), alta é ruim (vermelho). */
export function corVariacao(n: number): string {
  if (n < 0) return "text-emerald-600 dark:text-emerald-400";
  if (n > 0) return "text-rose-600 dark:text-rose-400";
  return "";
}

export function plural(n: number, s = "s"): string {
  return n !== 1 ? s : "";
}

const MESES = ["", "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
  "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"];

export function rotuloCompetencia(comp: string): string {
  const [y, m] = (comp || "").split("-");
  const mi = parseInt(m, 10);
  if (!y || !mi || mi < 1 || mi > 12) return comp;
  return `${MESES[mi]}/${y}`;
}

/** Mês atual no fuso do navegador (igual ao _currentMonthCompetencia). */
export function mesAtual(agora: Date = new Date()): string {
  return `${agora.getFullYear()}-${String(agora.getMonth() + 1).padStart(2, "0")}`;
}

/**
 * Datas iniciais do cartão "Comparar 2 datas". Iguais às da antiga, que usa
 * `toISOString()` (data em UTC), para o cartão abrir com as mesmas datas.
 */
export function datasComparacaoPadrao(agora: number = Date.now()): { a: string; b: string } {
  return {
    a: new Date(agora - 7 * 86400000).toISOString().slice(0, 10),
    b: new Date(agora).toISOString().slice(0, 10),
  };
}

// ── Query strings (mesma ordem de parâmetros da antiga) ─────────────────────

export const FILTROS_PADRAO: Filtros = { competencia: "", date_a: null, date_b: null, recent_months: "3" };

function montar(partes: string[]): string {
  return partes.length ? "?" + partes.join("&") : "";
}

/** _filterQs: competência, datas e, sem competência, os meses recentes. */
export function qsFiltros(f: Filtros, incluirCompetencia = true): string {
  const p: string[] = [];
  if (incluirCompetencia && f.competencia) p.push("competencia=" + encodeURIComponent(f.competencia));
  if (f.date_a) p.push("date_a=" + f.date_a);
  if (f.date_b) p.push("date_b=" + f.date_b);
  if (!f.competencia && f.recent_months) p.push("recent_months=" + encodeURIComponent(f.recent_months));
  return montar(p);
}

/** _evolucaoQs: botão de faixa ativo ignora o filtro de cima. */
export function qsEvolucao(f: Filtros, faixa: Faixa): string {
  if (faixa != null) return `?days=${faixa}&recent_months=all`;
  const p = ["days=all"];
  if (f.competencia) p.push("competencia=" + encodeURIComponent(f.competencia));
  else if (f.recent_months) p.push("recent_months=" + encodeURIComponent(f.recent_months));
  if (f.date_a) p.push("date_a=" + f.date_a);
  if (f.date_b) p.push("date_b=" + f.date_b);
  return montar(p);
}

/** _reloadChartMeses. */
export function qsPorMes(f: Filtros): string {
  const p: string[] = [];
  if (f.competencia) p.push("competencia=" + encodeURIComponent(f.competencia));
  if (!f.competencia && f.recent_months) p.push("recent_months=" + encodeURIComponent(f.recent_months));
  if (f.date_a) p.push("date_a=" + f.date_a);
  if (f.date_b) p.push("date_b=" + f.date_b);
  return montar(p);
}
