import { useQuery } from "@tanstack/react-query";
import { useEffect, useRef, useState } from "react";
import { Avatar, LinhaAviso, textoErro } from "./comum";
import { apiOk, dinheiro, qs, rotuloTaxa } from "./formato";
import type { Agente, Detalhe, FiltrosTela } from "./tipos";

/** Alunos de um consultor (GET /api/repasse/detalhe), como o painel de detalhe antigo. */
export function DetalheAgente({
  agente,
  indice,
  filtros,
  taxa,
  admin,
  onFechar,
}: {
  agente: Agente;
  indice: number;
  filtros: FiltrosTela;
  taxa: number;
  admin: boolean;
  onFechar: () => void;
}) {
  const [busca, setBusca] = useState("");
  const caixa = useRef<HTMLElement>(null);
  // Admin escolheu um cartão: leva a tela até o detalhe, como a antiga (scrollIntoView).
  // O consultor abre direto no detalhe, então não precisa rolar.
  useEffect(() => {
    if (admin) caixa.current?.scrollIntoView?.({ behavior: "smooth", block: "start" });
  }, [admin]);
  const q = useQuery({
    queryKey: ["repasse", "detalhe", String(agente.id), filtros.ciclo, filtros.tipo, filtros.turma],
    queryFn: ({ signal }) =>
      apiOk<Detalhe>(
        `/api/repasse/detalhe?${qs([
          ["kommo_uid", String(agente.id)],
          ["ciclo", filtros.ciclo],
          ["tipo", filtros.tipo],
          ["turma", filtros.turma],
        ])}`,
        signal,
      ),
  });

  const alunos = q.data?.alunos ?? [];
  const termo = busca.trim().toLowerCase();
  // Mesmo filtro do repFilterDetalhe: RGM, tipo de pagamento e turma.
  const visiveis = termo
    ? alunos.filter((a) => `${a.rgm} ${a.tipo_pagamento} ${a.turma}`.toLowerCase().includes(termo))
    : alunos;
  // Enquanto carrega, o repasse do cartão; depois, o total da resposta (igual à antiga).
  const recebido = q.data ? q.data.total || 0 : agente.total_valor || 0;
  const pct = rotuloTaxa(taxa * 100);
  const colunas = admin ? 6 : 5;

  return (
    <section ref={caixa} className="cartao overflow-hidden scroll-mt-4" aria-label={`Detalhe de ${agente.nome}`}>
      <div className="flex flex-wrap items-center gap-4 px-5 pt-5 pb-3">
        <Avatar nome={agente.nome} indice={indice} grande />
        <div className="min-w-0 flex-1">
          <h2 className="text-base truncate">{agente.nome}</h2>
          <div className="flex flex-wrap gap-x-5 gap-y-1 text-xs mt-1">
            {admin && (
              <span className="flex gap-1">
                <span style={{ color: "var(--text-muted)" }}>Recebido</span>
                <span className="tabular-nums font-semibold text-emerald-700 dark:text-emerald-400">
                  {q.data ? dinheiro(recebido) : "…"}
                </span>
              </span>
            )}
            <span className="flex gap-1">
              <span style={{ color: "var(--text-muted)" }}>Repasse ({pct}%)</span>
              <span className="tabular-nums font-bold text-amber-800 dark:text-amber-300">{dinheiro(recebido * taxa)}</span>
            </span>
            <span className="flex gap-1">
              <span style={{ color: "var(--text-muted)" }}>Alunos</span>
              <span className="tabular-nums font-semibold">{q.data ? alunos.length.toLocaleString("pt-BR") : "…"}</span>
            </span>
          </div>
        </div>
        <input
          type="search"
          className="controle font-normal w-full sm:w-56"
          placeholder="Buscar RGM, tipo…"
          aria-label="Buscar no detalhe"
          value={busca}
          onChange={(e) => setBusca(e.target.value)}
        />
        <button type="button" className="controle" aria-label="Fechar detalhe" onClick={onFechar}>
          <span className="material-symbols-outlined text-[18px]" aria-hidden>close</span>
        </button>
      </div>
      <div className="overflow-auto max-h-[480px]">
        <table className="tabela-cartao">
          <thead>
            <tr>
              <th className="text-left">RGM</th>
              <th className="text-left">Tipo de pagamento</th>
              <th className="text-left">Turma</th>
              <th className="text-left">Ciclo</th>
              {admin && <th className="text-right">Valor recebido</th>}
              <th className="text-right">Repasse ({pct}%)</th>
            </tr>
          </thead>
          <tbody>
            {q.isPending ? (
              <LinhaAviso colunas={colunas} texto="Carregando…" />
            ) : q.isError ? (
              <LinhaAviso colunas={colunas} erro texto={`Erro ao carregar: ${textoErro(q.error)}`} />
            ) : visiveis.length ? (
              visiveis.map((a) => (
                <tr key={a.rgm}>
                  <td className="font-mono" style={{ color: "var(--text-secondary)" }}>{a.rgm || "—"}</td>
                  <td>{a.tipo_pagamento || "—"}</td>
                  <td style={{ color: "var(--text-secondary)" }}>{a.turma || "—"}</td>
                  <td style={{ color: "var(--text-secondary)" }}>{a.ciclo || "—"}</td>
                  {admin && <td className="text-right tabular-nums text-emerald-700 dark:text-emerald-400">{dinheiro(a.valor)}</td>}
                  <td className="text-right tabular-nums font-semibold text-amber-800 dark:text-amber-300">{dinheiro((a.valor || 0) * taxa)}</td>
                </tr>
              ))
            ) : (
              <LinhaAviso colunas={colunas} texto="Nenhum aluno encontrado" />
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}
