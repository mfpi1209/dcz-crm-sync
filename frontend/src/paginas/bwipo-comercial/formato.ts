import { api } from "../../api";
import type { Filtros, Linha, LinhaRanking, Painel } from "./tipos";

const BRL = new Intl.NumberFormat("pt-BR", { style: "currency", currency: "BRL" });

/** Número pt-BR; vazio vira 0 (igual a _bwcNum). */
export function num(n: number | null | undefined): string {
  return (n || 0).toLocaleString("pt-BR");
}

export function dinheiro(v: number): string {
  return BRL.format(v);
}

/** AAAA-MM-DD → DD/MM/AAAA; outra coisa volta como está, vazio vira "—" (igual a _bwcDataBr). */
export function dataBr(s: string | null | undefined): string {
  const t = String(s || "").slice(0, 10);
  const iso = t.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  return iso ? `${iso[3]}/${iso[2]}/${iso[1]}` : t || "—";
}

/** AAAA-MM-DD → DD/MM (rótulo do gráfico por dia). */
export function diaMes(s: string): string {
  const p = (s || "").slice(0, 10).split("-");
  return p.length === 3 ? `${p[2]}/${p[1]}` : s;
}

export const FONTE: Record<string, string> = { manual: "decisão manual", bwipo: "Bwipo", kommo: "Kommo", sem_dono: "sem dono" };

export const NIVEIS = ["Graduação", "Pós-Graduação"];

/** Query string do painel na ordem da antiga (_bwcQs). Ciclo e turma não vão para a API. */
export function qsPainel(f: Filtros): string {
  const p = new URLSearchParams();
  if (f.polo) p.set("polo", f.polo);
  if (f.nivel) p.set("nivel", f.nivel);
  if (f.agente) p.set("owner", f.agente);
  if (f.ini) p.set("dt_ini", f.ini);
  if (f.fim) p.set("dt_fim", f.fim);
  return p.toString();
}

/** Período de meta/campanha mais recente (igual a _crgmPickLatestMetaPeriod). */
export function periodoMaisRecente(
  periodos: { dt_inicio?: string | null; dt_fim?: string | null }[],
): { dt_inicio: string; dt_fim: string } | null {
  const uniq = new Map<string, { dt_inicio: string; dt_fim: string }>();
  for (const p of periodos) {
    if (!p?.dt_inicio || !p?.dt_fim) continue;
    const di = String(p.dt_inicio).trim().substring(0, 10);
    const df = String(p.dt_fim).trim().substring(0, 10);
    if (!di || !df) continue;
    const k = `${di}|${df}`;
    if (!uniq.has(k)) uniq.set(k, { dt_inicio: di, dt_fim: df });
  }
  const arr = Array.from(uniq.values());
  arr.sort((a, b) => {
    if (a.dt_inicio !== b.dt_inicio) return a.dt_inicio < b.dt_inicio ? 1 : -1;
    return a.dt_fim < b.dt_fim ? 1 : a.dt_fim > b.dt_fim ? -1 : 0;
  });
  return arr[0] || null;
}

/** Ranking da tela (_bwcRankingRows): sem dia, esconde zerados; com dia, recalcula pelas linhas. */
export function linhasRanking(p: Painel | undefined, dia: string | null): LinhaRanking[] {
  const base = (p?.ranking ?? []).slice();
  if (!dia) return base.filter((r) => r.matriculas || r.aberto || r.perdido || r.evasao);
  const acc: Record<string, { matriculas: number; evasao: number; pos: number }> = {};
  for (const l of (p?.linhas ?? []).filter((x) => x.data === dia)) {
    const b = acc[l.agente] || { matriculas: 0, evasao: 0, pos: 0 };
    if (l.conta) {
      b.matriculas += 1;
      if (l.nivel === "Pós-Graduação") b.pos += 1;
    } else if (l.situacao !== "EM CURSO") b.evasao += 1;
    acc[l.agente] = b;
  }
  return base
    .map((r) => {
      const b = acc[r.agente] || { matriculas: 0, evasao: 0, pos: 0 };
      const tot = b.matriculas + b.evasao;
      return { ...r, ...b, conv: tot ? Math.round((1000 * b.matriculas) / tot) / 10 : 0 };
    })
    .filter((r) => r.matriculas || r.evasao)
    .sort((a, b) => b.matriculas - a.matriculas);
}

/** Coluna Nível do ranking (_bwcNivel). */
export function nivelRanking(r: LinhaRanking): string {
  const mp = r.matriculas || 0;
  if (r.supermeta && mp >= r.supermeta) return "Super";
  if (r.meta && mp >= r.meta) return "Meta";
  if (r.intermediaria && mp >= r.intermediaria) return "Interm.";
  if (r.meta) return "Abaixo";
  return r.pos ? (r.pos === r.matriculas ? "Pós" : "Misto") : "Grad.";
}

export type LinhaAgente = Linha & { cpf: string; telefone: string };

const COLUNAS_CSV = ["rgm", "nome", "cpf", "telefone", "tipo", "polo", "nivel", "data", "curso", "situacao", "fonte"] as const;

/** CSV da lista do agente (bwcAgenteCsv): BOM, ";" e tudo entre aspas. */
export function csvAgente(linhas: LinhaAgente[]): string {
  const esc = (v: unknown) => `"${String(v ?? "").replace(/"/g, '""')}"`;
  const corpo = [COLUNAS_CSV.join(";")].concat(linhas.map((l) => COLUNAS_CSV.map((c) => esc(l[c])).join(";")));
  return "﻿" + corpo.join("\n");
}

export function baixarCsvAgente(nome: string, linhas: LinhaAgente[]) {
  const a = document.createElement("a");
  a.href = URL.createObjectURL(new Blob([csvAgente(linhas)], { type: "text/csv;charset=utf-8" }));
  a.download = "matriculas_" + (nome || "agente").replace(/\W+/g, "_") + ".csv";
  a.click();
  URL.revokeObjectURL(a.href);
}

/** GET que trata {ok: false, error} como erro. */
export async function apiOk<T extends { ok?: boolean; error?: string }>(caminho: string, signal?: AbortSignal): Promise<T> {
  const d = await api<T>(caminho, signal ? { signal } : undefined);
  if (d && d.ok === false) throw new Error(d.error || "falha");
  return d;
}

/** POST/DELETE com corpo JSON (igual a _bwcSend): {ok: false} vira erro com a mensagem do servidor. */
export async function enviar<T extends { ok?: boolean; error?: string }>(
  url: string,
  metodo: string,
  corpo?: unknown,
  signal?: AbortSignal,
): Promise<T> {
  const d = await api<T>(url, {
    method: metodo,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(corpo || {}),
    ...(signal ? { signal } : {}),
  });
  if (!d || d.ok === false) throw new Error(d?.error || "falha");
  return d;
}

/** Só os dígitos. */
export const digitos = (s: string) => String(s || "").replace(/\D/g, "");

export function textoErro(e: unknown): string {
  return e instanceof Error && e.message ? e.message : "falha";
}
