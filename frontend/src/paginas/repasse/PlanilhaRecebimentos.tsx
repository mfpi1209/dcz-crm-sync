import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { LinhaAviso, Paginacao, textoErro } from "./comum";
import { apiOk, dinheiro, qs, useAtrasado } from "./formato";
import type { FiltrosTela, Planilha } from "./tipos";

/** Planilha crua de recebimentos (só admin): mesmos filtros de cima + busca por RGM, 50 por página. */
export function PlanilhaRecebimentos({ filtros }: { filtros: FiltrosTela }) {
  const [busca, setBusca] = useState("");
  const termo = useAtrasado(busca.trim());
  const filtro = qs([
    ["ciclo", filtros.ciclo],
    ["tipo", filtros.tipo],
    ["turma", filtros.turma],
    ["q", termo],
  ]);
  // A página volta a 1 quando muda o filtro ou a busca (sem um pedido a mais com a página velha).
  const [pg, setPg] = useState({ filtro, n: 1 });
  const pagina = pg.filtro === filtro ? pg.n : 1;

  const q = useQuery({
    queryKey: ["repasse", "planilha", filtro, pagina],
    queryFn: ({ signal }) =>
      apiOk<Planilha>(`/api/repasse/planilha?${qs([["page", pagina]])}${filtro ? `&${filtro}` : ""}`, signal),
    placeholderData: keepPreviousData,
  });
  const d = q.data;

  return (
    <section className="cartao overflow-hidden" aria-label="Planilha de recebimentos">
      <div className="flex flex-wrap items-center gap-3 px-5 pt-5 pb-3">
        <div className="min-w-0 flex-1">
          <h2 className="text-sm flex items-center gap-2">
            <span className="material-symbols-outlined text-emerald-500" aria-hidden>table_view</span>
            Planilha de recebimentos
          </h2>
          <p className="text-xs mt-1" style={{ color: "var(--text-muted)" }}>
            {d ? `${(d.total || 0).toLocaleString("pt-BR")} linhas · ${dinheiro(d.valor)} pagos` : "RGM, valor pago, turma, beleza e ciclo"}
          </p>
        </div>
        <input
          type="search"
          className="controle font-normal w-full sm:w-48"
          placeholder="Buscar RGM…"
          aria-label="Buscar RGM na planilha"
          value={busca}
          onChange={(e) => setBusca(e.target.value)}
        />
        <a
          className="controle"
          href={`/api/repasse/planilha.csv${filtro ? `?${filtro}` : ""}`}
          download={`repasse_recebimentos_${new Date().toISOString().slice(0, 10)}.csv`}
        >
          <span className="material-symbols-outlined text-[18px]" aria-hidden>download</span>
          Baixar CSV
        </a>
      </div>
      <div className="overflow-auto max-h-[480px]">
        <table className="tabela-cartao">
          <thead>
            <tr>
              <th className="text-left">RGM</th>
              <th className="text-right">Valor pago</th>
              <th className="text-left">Turma</th>
              <th className="text-left">Beleza</th>
              <th className="text-left">Ciclo</th>
            </tr>
          </thead>
          <tbody>
            {q.isPending ? (
              <LinhaAviso colunas={5} texto="Carregando…" />
            ) : q.isError ? (
              <LinhaAviso colunas={5} erro texto={textoErro(q.error)} />
            ) : d && d.rows.length ? (
              d.rows.map((r, i) => (
                <tr key={`${r.rgm}-${i}`}>
                  <td className="font-mono">{r.rgm || "—"}</td>
                  <td className="text-right tabular-nums text-emerald-700 dark:text-emerald-400">{dinheiro(r.valor)}</td>
                  <td style={{ color: "var(--text-secondary)" }}>{r.turma || "—"}</td>
                  <td style={{ color: "var(--text-secondary)" }}>{r.beleza || "—"}</td>
                  <td style={{ color: "var(--text-secondary)" }}>{r.ciclo || "—"}</td>
                </tr>
              ))
            ) : (
              <LinhaAviso colunas={5} texto="Nenhum recebimento nesse filtro." />
            )}
          </tbody>
        </table>
      </div>
      <Paginacao
        pagina={d?.page ?? pagina}
        paginas={d?.pages ?? 1}
        ocupado={q.isFetching}
        onIr={(n) => setPg({ filtro, n })}
      />
    </section>
  );
}
