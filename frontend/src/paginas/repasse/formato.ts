import { useEffect, useState } from "react";
import { api } from "../../api";

const BRL = new Intl.NumberFormat("pt-BR", { style: "currency", currency: "BRL" });

/** Dinheiro como na tela antiga (_repFmtMoeda): R$ com 2 casas. */
export function dinheiro(v: number | null | undefined): string {
  return BRL.format(v || 0);
}

/** Duas iniciais do nome (igual a _repInitials). */
export function iniciais(nome: string): string {
  const partes = (nome || "").trim().split(/\s+/).filter(Boolean);
  if (partes.length >= 2) return (partes[0][0] + partes[1][0]).toUpperCase();
  return ((nome || "").trim() || "?").substring(0, 2).toUpperCase();
}

/** Taxa no rótulo: "20" ou "12.5" (igual a _repSyncTaxaLabels). */
export function rotuloTaxa(pct: number): string {
  return pct % 1 === 0 ? pct.toFixed(0) : pct.toFixed(1);
}

/**
 * Query string na mesma ordem e codificação da tela antiga; valores vazios ficam
 * de fora. O valor vai como está (a busca já chega aparada, como na antiga).
 */
export function qs(pares: [string, string | number | undefined | null][]): string {
  return pares
    .filter(([, v]) => v !== undefined && v !== null && String(v) !== "")
    .map(([k, v]) => `${k}=${encodeURIComponent(String(v))}`)
    .join("&");
}

/**
 * As rotas respondem {ok: false, error} quando algo falha; trata como erro.
 * `signal` (do TanStack Query) cancela o pedido que ficou velho.
 */
export async function apiOk<T extends { ok?: boolean; error?: string }>(caminho: string, signal?: AbortSignal): Promise<T> {
  const d = await api<T>(caminho, signal ? { signal } : undefined);
  if (d && d.ok === false) throw new Error(d.error || "Erro");
  return d;
}

/** Valor que só muda depois de `ms` sem mudanças (busca enquanto digita, 300 ms como a antiga). */
export function useAtrasado<T>(valor: T, ms = 300): T {
  const [atrasado, setAtrasado] = useState(valor);
  useEffect(() => {
    const t = setTimeout(() => setAtrasado(valor), ms);
    return () => clearTimeout(t);
  }, [valor, ms]);
  return atrasado;
}
