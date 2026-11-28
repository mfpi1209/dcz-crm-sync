import { api, ErroApi, navegador } from "../../api";
import type { DadosComercial, DiaHeatmap, Insights, Periodo } from "./tipos";

const BRL = new Intl.NumberFormat("pt-BR", { style: "currency", currency: "BRL" });
const INTEIRO = new Intl.NumberFormat("pt-BR", { minimumFractionDigits: 0, maximumFractionDigits: 0 });

/** Dinheiro como _mpFmt. */
export function dinheiro(v: number | null | undefined): string {
  return BRL.format(Number(v || 0));
}

/** Inteiro com milhar como _mpFmtN. */
export function inteiro(v: number | null | undefined): string {
  return INTEIRO.format(Number(v || 0));
}

/** Data como _mpFmtDate: "AAAA-MM-DD" → "DD/MM/AAAA"; já em DD/MM/AAAA fica igual. */
export function data(d: string | null | undefined): string {
  if (!d) return "";
  const s = String(d).trim();
  if (/^\d{2}\/\d{2}\/\d{4}/.test(s)) return s.slice(0, 10);
  const p = s.split("-");
  return p.length === 3 ? `${p[2]}/${p[1]}/${p[0]}` : s;
}

export const ROTULO_TIER: Record<string, string> = {
  base: "Base",
  intermediaria: "Intermediária",
  meta: "Meta",
  supermeta: "Supermeta",
};

/** _mpClassifySituacao: TRANSFERIDO conta como evadido (igual ao Dashboard Comercial). */
export function classificarSituacao(sit: string | null | undefined): "ativo" | "evadido" | "outros" {
  const s = (sit || "").toUpperCase();
  if (!s) return "outros";
  if (s === "EM CURSO") return "ativo";
  if (s.includes("CANCEL") || s.includes("EVAD") || s.includes("DESIST") || s.includes("TRANC") || s.includes("TRANSFER"))
    return "evadido";
  return "outros";
}

/** _mpPickLatestMetaPeriod: período mais recente (início, depois fim), sem repetir. */
export function ultimoPeriodo(periodos: Periodo[]): { dt_inicio: string; dt_fim: string } | null {
  const uniq = new Map<string, { dt_inicio: string; dt_fim: string }>();
  for (const p of periodos) {
    if (!p?.dt_inicio || !p?.dt_fim) continue;
    const di = String(p.dt_inicio).trim().substring(0, 10);
    const df = String(p.dt_fim).trim().substring(0, 10);
    if (!di || !df) continue;
    const k = `${di}|${df}`;
    if (!uniq.has(k)) uniq.set(k, { dt_inicio: di, dt_fim: df });
  }
  const arr = [...uniq.values()];
  arr.sort((a, b) => {
    if (a.dt_inicio !== b.dt_inicio) return a.dt_inicio < b.dt_inicio ? 1 : -1;
    return a.dt_fim < b.dt_fim ? 1 : a.dt_fim > b.dt_fim ? -1 : 0;
  });
  return arr[0] || null;
}

/** _mpFallbackMesCorrente. */
export function mesCorrente(agora = new Date()): { dt_inicio: string; dt_fim: string } {
  const y = agora.getFullYear();
  const m = String(agora.getMonth() + 1).padStart(2, "0");
  const last = new Date(y, agora.getMonth() + 1, 0).getDate();
  return { dt_inicio: `${y}-${m}-01`, dt_fim: `${y}-${m}-${String(last).padStart(2, "0")}` };
}

/** Data local AAAA-MM-DD (o toLocaleDateString('sv-SE') da antiga). */
export function hojeLocal(agora = new Date()): string {
  const y = agora.getFullYear();
  const m = String(agora.getMonth() + 1).padStart(2, "0");
  const d = String(agora.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

/** _mpCalcMaxPotencial. */
export function potencialMaximo(d: Insights): number {
  const prem = d.premiacao || {};
  const desb = prem.desbloqueie || [];
  if (!desb.length) return prem.total || 0;
  const maxTier = desb.reduce((max, t) => Math.max(max, t.ganho_total), 0);
  return maxTier + (prem.daily_bonus || 0) + (prem.receb_bonus || 0);
}

/** Visão de suporte (dados): a antiga mesclava as vendas do Comercial nestes casos. */
export function ehVisaoSuporte(d: Insights, suporteEquipe: boolean): boolean {
  return !!(suporteEquipe || d.painel_suporte || d.suporte_equipe_view);
}

/** Layout de suporte (_mpApplySuporteLayout): também quando a meta do time diz is_suporte. */
export function ehLayoutSuporte(d: Insights, suporteEquipe: boolean): boolean {
  return !!(suporteEquipe || d.painel_suporte || d.suporte_equipe_view || d.meta_equipe_suporte?.is_suporte);
}

/**
 * apply() de _mpMergeComercialVendas: devolve uma cópia de `d` com o total e a
 * evolução do Dashboard Comercial (mesmos números de lá). `null` se a resposta não serve.
 */
export function aplicarVendasComercial(d: Insights, cr: DadosComercial | null): { insights: Insights; temVendas: boolean } | null {
  if (!cr?.ok || !cr.kpis) return null;
  const total = cr.kpis.vendas ?? 0;
  const media = cr.kpis.media_diaria ?? 0;
  const novo: Insights = {
    ...d,
    total_matriculas_time: total,
    media_vendas: media,
    pace_atual: media,
    chart_matriculas_dia: (cr.evolucao || []).map((e) => ({ data: e.data, matriculas: e.count ?? 0 })),
  };
  if (d.meta_equipe_suporte) {
    const meta = d.meta_equipe_suporte.meta || 0;
    novo.meta_equipe_suporte = {
      ...d.meta_equipe_suporte,
      total_matriculas: total,
      pct: meta > 0 ? Math.round((total / meta) * 1000) / 10 : 0,
      meta_batida: meta > 0 && total >= meta,
      matriculas_fonte: "comercial",
    };
  }
  return { insights: novo, temVendas: total > 0 };
}

/** _mpSyncHeatmapFromChart: o calendário do suporte segue o gráfico Matrículas por Dia. */
export function sincronizarHeatmap(d: Insights, hoje: string): Insights {
  const serie = d.chart_matriculas_dia || [];
  if (!serie.length || !Array.isArray(d.heatmap)) return d;
  const porData: Record<string, number> = {};
  for (const p of serie) {
    const k = String(p.data || "").split("T")[0];
    if (k) porData[k] = Number(p.matriculas) || 0;
  }
  const heatmap: DiaHeatmap[] = d.heatmap.map((h) => {
    const k = String(h.data || "").split("T")[0];
    if (!k) return h;
    if (k > hoje) return { ...h, data: k, realizadas: null, mat: 0, aceites: 0, status: "future" };
    const n = porData[k] ?? 0;
    const meta = Number(h.meta) || 0;
    let status = h.status;
    if (meta > 0) status = n >= meta ? "hit" : n > 0 ? "partial" : "miss";
    else if (n > 0) status = "partial";
    return { ...h, data: k, realizadas: n, mat: n, aceites: 0, status };
  });
  return { ...d, heatmap };
}

async function talvez<T>(caminho: string, signal?: AbortSignal): Promise<T | null> {
  try {
    return await api<T>(caminho, { signal });
  } catch (e) {
    if (signal?.aborted) throw e;
    return null;
  }
}

/**
 * _mpMergeComercialVendas inteiro: tenta o período da campanha; sem vendas, cai no
 * período de meta mais recente (metas do Comercial + campanhas). Falhas não derrubam a tela.
 */
export async function mesclarVendasComercial(d: Insights, signal?: AbortSignal): Promise<Insights> {
  const c = d.campanha;
  if (!c) return d;
  let di = String(c.dt_inicio || "").slice(0, 10);
  let df = String(c.dt_fim || "").slice(0, 10);
  if (!di || !df) return d;
  const url = (a: string, b: string) => `/api/comercial-rgm/data?dt_ini=${encodeURIComponent(a)}&dt_fim=${encodeURIComponent(b)}`;
  let atual = d;
  const primeiro = aplicarVendasComercial(d, await talvez<DadosComercial>(url(di, df), signal));
  if (primeiro) {
    atual = primeiro.insights;
    if (primeiro.temVendas) return atual;
  }
  const periodos: Periodo[] = [{ dt_inicio: di, dt_fim: df }];
  const metas = await talvez<{ ok?: boolean; metas?: Periodo[] }>("/api/comercial-rgm/metas?categoria=matriculas", signal);
  if (metas?.ok && metas.metas?.length) for (const m of metas.metas) if (m.dt_inicio && m.dt_fim) periodos.push(m);
  const camp = await talvez<{ ok?: boolean; campanhas?: Periodo[] }>("/api/premiacao/campanhas-periodos", signal);
  if (camp?.ok && camp.campanhas?.length) for (const x of camp.campanhas) if (x.dt_inicio && x.dt_fim) periodos.push(x);
  const ultima = ultimoPeriodo(periodos);
  if (ultima && (ultima.dt_inicio !== di || ultima.dt_fim !== df)) {
    di = ultima.dt_inicio;
    df = ultima.dt_fim;
    const segundo = aplicarVendasComercial(atual, await talvez<DadosComercial>(url(di, df), signal));
    if (segundo) atual = segundo.insights;
  }
  return atual;
}

/** As rotas respondem {ok: false, error} quando algo falha; trata como erro. */
export async function apiOk<T extends { ok?: boolean; error?: string }>(caminho: string, init?: RequestInit): Promise<T> {
  const d = await api<T>(caminho, init);
  if (d && d.ok === false) throw new Error(d.error || "Erro");
  return d;
}

/**
 * Pedido que precisa do corpo mesmo com erro (409 do sync Bwipo traz a lista de
 * negócios). 401 vai para o login, como no api().
 */
export async function pedirComCorpo<T>(caminho: string, init: RequestInit): Promise<{ status: number; ok: boolean; corpo: T }> {
  const r = await fetch(caminho, {
    ...init,
    credentials: "same-origin",
    headers: { Accept: "application/json", ...((init.headers as Record<string, string>) || {}) },
  });
  if (r.status === 401) {
    navegador.irPara("/login");
    throw new ErroApi(401, "Sessão expirada. Entre de novo.");
  }
  let corpo = {} as T;
  try {
    corpo = (await r.json()) as T;
  } catch {
    /* sem JSON */
  }
  return { status: r.status, ok: r.ok, corpo };
}

export function textoErro(e: unknown): string {
  return e instanceof Error && e.message ? e.message : "Falha ao carregar";
}
