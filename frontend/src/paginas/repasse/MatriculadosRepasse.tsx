import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { LinhaAviso, Paginacao, textoErro } from "./comum";
import { apiOk, dinheiro, qs, useAtrasado } from "./formato";
import type { Matriculados } from "./tipos";

/** Tipos de matrícula da tela antiga; "Nova matrícula" é o padrão (o servidor também usa). */
const TIPOS_MAT = [
  { valor: "NOVA MATRICULA", rotulo: "Nova matrícula" },
  { valor: "RETORNO", rotulo: "Retorno" },
  { valor: "RECOMPRA", rotulo: "Recompra" },
  { valor: "todos", rotulo: "Todos" },
];

interface FiltrosMat {
  pago: string;
  mes: string;
  dia: string;
  ciclo_mat: string;
  nivel: string;
  tipo_mat: string;
}

const INICIAIS: FiltrosMat = { pago: "", mes: "", dia: "", ciclo_mat: "", nivel: "", tipo_mat: "NOVA MATRICULA" };

/**
 * Ingressantes da base diária, pagos e não pagos (só admin). Filtros próprios,
 * independentes dos de cima, como na tela antiga. Sem ciclo escolhido, o
 * servidor usa o mais recente e devolve qual usou em `filtros.ciclo`.
 */
export function MatriculadosRepasse() {
  const [busca, setBusca] = useState("");
  const termo = useAtrasado(busca.trim());
  const [f, setF] = useState<FiltrosMat>(INICIAIS);
  // Mesma ordem dos parâmetros do _repMatQs antigo.
  const filtro = qs([
    ["q", termo],
    ["pago", f.pago],
    ["mes", f.mes],
    ["dia", f.dia],
    ["ciclo_mat", f.ciclo_mat],
    ["nivel", f.nivel],
    ["tipo_mat", f.tipo_mat],
  ]);
  const [pg, setPg] = useState({ filtro, n: 1 });
  const pagina = pg.filtro === filtro ? pg.n : 1;

  const q = useQuery({
    queryKey: ["repasse", "matriculados", filtro, pagina],
    queryFn: () => apiOk<Matriculados>(`/api/repasse/matriculados?${qs([["page", pagina]])}${filtro ? `&${filtro}` : ""}`),
    placeholderData: keepPreviousData,
  });
  const d = q.data;
  const opcoes = d?.filtros;
  const ciclos = opcoes?.ciclos ?? [];
  const cicloAtual = f.ciclo_mat || opcoes?.ciclo || "";

  const mudar = (campo: keyof FiltrosMat) => (e: { target: { value: string } }) =>
    setF((atual) => ({ ...atual, [campo]: e.target.value }));

  return (
    <section className="cartao overflow-hidden" aria-label="Matriculados">
      <div className="flex flex-wrap items-center gap-3 px-5 pt-5 pb-3">
        <div className="min-w-0 flex-1">
          <h2 className="text-sm flex items-center gap-2">
            <span className="material-symbols-outlined text-sky-500" aria-hidden>how_to_reg</span>
            Matriculados
          </h2>
          <p className="text-xs mt-1" style={{ color: "var(--text-muted)" }}>
            {d
              ? `${(d.total || 0).toLocaleString("pt-BR")} ingressantes · ${(d.pagos || 0).toLocaleString("pt-BR")} pagaram · ${dinheiro(d.valor)}`
              : "Base diária, pagos e não pagos"}
          </p>
        </div>
        <input
          type="search"
          className="controle font-normal w-full sm:w-48"
          placeholder="Buscar RGM…"
          aria-label="Buscar RGM nos matriculados"
          value={busca}
          onChange={(e) => setBusca(e.target.value)}
        />
        <a
          className="controle"
          href={`/api/repasse/matriculados.csv${filtro ? `?${filtro}` : ""}`}
          download={`repasse_ingressantes_${new Date().toISOString().slice(0, 10)}.csv`}
        >
          <span className="material-symbols-outlined text-[18px]" aria-hidden>download</span>
          Baixar CSV
        </a>
      </div>

      <div className="flex flex-wrap gap-2 px-5 pb-4">
        <select className="controle font-normal" aria-label="Pagamento" value={f.pago} onChange={mudar("pago")}>
          <option value="">Pagamento: todos</option>
          <option value="sim">Pagou</option>
          <option value="nao">Não pagou</option>
        </select>
        <select className="controle font-normal" aria-label="Mês de ingresso" value={f.mes} onChange={mudar("mes")}>
          <option value="">Mês: todos</option>
          {(opcoes?.meses ?? []).map((m) => (
            <option key={m.id} value={m.id}>{m.label}</option>
          ))}
        </select>
        <input type="date" className="controle font-normal" aria-label="Dia" value={f.dia} onChange={mudar("dia")} />
        <select className="controle font-normal" aria-label="Ciclo dos matriculados" value={cicloAtual} onChange={mudar("ciclo_mat")}>
          {ciclos.length === 0 && <option value="">Ciclo</option>}
          {cicloAtual && !ciclos.includes(cicloAtual) && <option value={cicloAtual}>{cicloAtual}</option>}
          {ciclos.map((c) => (
            <option key={c} value={c}>{c}</option>
          ))}
        </select>
        <select className="controle font-normal" aria-label="Nível" value={f.nivel} onChange={mudar("nivel")}>
          <option value="">Nível: todos</option>
          {(opcoes?.niveis ?? []).map((n) => (
            <option key={n} value={n}>{n}</option>
          ))}
        </select>
        <select className="controle font-normal" aria-label="Tipo de matrícula" value={f.tipo_mat} onChange={mudar("tipo_mat")}>
          {TIPOS_MAT.map((t) => (
            <option key={t.valor} value={t.valor}>{t.rotulo}</option>
          ))}
        </select>
      </div>

      <div className="overflow-auto max-h-[480px]">
        <table className="tabela-cartao">
          <thead>
            <tr>
              <th className="text-left">RGM</th>
              <th className="text-left">Polo</th>
              <th className="text-left">Nível</th>
              <th className="text-left">Modalidade</th>
              <th className="text-left">Data de matrícula</th>
              <th className="text-left">Ciclo</th>
              <th className="text-left">Turma</th>
              <th className="text-left">Pagou</th>
              <th className="text-right">Valor pago</th>
              <th className="text-left">Tipo de pagamento</th>
            </tr>
          </thead>
          <tbody>
            {q.isPending ? (
              <LinhaAviso colunas={10} texto="Carregando…" />
            ) : q.isError ? (
              <LinhaAviso colunas={10} erro texto={textoErro(q.error)} />
            ) : d && d.rows.length ? (
              d.rows.map((r, i) => (
                <tr key={`${r.rgm}-${i}`}>
                  <td className="font-mono">{r.rgm || "—"}</td>
                  <td style={{ color: "var(--text-secondary)" }}>{r.polo || "—"}</td>
                  <td style={{ color: "var(--text-secondary)" }}>{r.nivel || "—"}</td>
                  <td style={{ color: "var(--text-secondary)" }}>{r.modalidade || "—"}</td>
                  <td className="whitespace-nowrap" style={{ color: "var(--text-secondary)" }}>{r.data_matricula || "—"}</td>
                  <td style={{ color: "var(--text-secondary)" }}>{r.ciclo || "—"}</td>
                  <td style={{ color: "var(--text-secondary)" }}>{r.turma || "—"}</td>
                  <td className={r.pagou ? "text-emerald-700 dark:text-emerald-400 font-semibold" : ""} style={r.pagou ? undefined : { color: "var(--text-muted)" }}>
                    {r.pagou ? "Sim" : "Não"}
                  </td>
                  <td className="text-right tabular-nums text-emerald-700 dark:text-emerald-400">{r.pagou ? dinheiro(r.valor) : "—"}</td>
                  <td style={{ color: "var(--text-secondary)" }}>{r.tipo_pagamento || "—"}</td>
                </tr>
              ))
            ) : (
              <LinhaAviso colunas={10} texto="Nenhum ingressante nesse filtro." />
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
