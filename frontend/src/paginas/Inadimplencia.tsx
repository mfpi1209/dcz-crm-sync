import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { api } from "../api";
import { ComparacaoDatas, ComparacaoPeriodoView, ErroComparacao } from "./inadimplencia/Comparacao";
import { Cartao, Indicador, textoErro } from "./inadimplencia/comum";
import {
  corVariacao,
  datasComparacaoPadrao,
  FILTROS_PADRAO,
  fmt,
  fmtPct,
  mesAtual,
  plural,
  qsEvolucao,
  qsFiltros,
  qsComparar,
  qsPorMes,
  rotuloCompetencia,
  sinal,
} from "./inadimplencia/formato";
import { GraficoCompetencias } from "./inadimplencia/GraficoCompetencias";
import { GraficoEvolucao } from "./inadimplencia/GraficoEvolucao";
import { Historico } from "./inadimplencia/Historico";
import type {
  Comparacao,
  ComparacaoPeriodo,
  Competencias,
  Evolucao,
  EvolucaoPorMes,
  Faixa,
  Filtros,
  Lado,
  Lista,
  Reincidencia,
  Snapshot,
} from "./inadimplencia/tipos";

/**
 * Inadimplência (página `inadimplencia`). Mesmas APIs e mesmos números da tela
 * antiga (templates/partials/_inadimplencia.html + static/js/inadimplencia.js);
 * as contas ficam em routes/inadimplencia.py. Não há modo admin: quem tem a
 * página vê tudo, inclusive o Apagar (a permissão é conferida no servidor).
 *
 * Buscas, como o loadInadimplencia antigo: list, atual, comparar (ou
 * comparar-periodo com competência), evolucao e reincidencia juntas; depois,
 * havendo snapshots, evolucao-por-mes. Os botões 7d/30d/90d/Tudo refazem só a
 * evolução. Os filtros de cima mexem num rascunho e só o Aplicar busca.
 */

interface Rascunho {
  competencia: string;
  recent: string;
  date_a: string;
  date_b: string;
}

const RASCUNHO_PADRAO: Rascunho = { competencia: "", recent: "3", date_a: "", date_b: "" };

const MESES_RECENTES = [
  { valor: "1", rotulo: "1 mês (atual)" },
  { valor: "3", rotulo: "3 meses" },
  { valor: "6", rotulo: "6 meses" },
  { valor: "12", rotulo: "12 meses" },
  { valor: "all", rotulo: "Tudo" },
];

/** Só um filtro vale por vez. Prioridade: datas > competência > últimos meses (_applyFilters). */
function filtrosDoRascunho(r: Rascunho): Filtros {
  if (r.date_a || r.date_b)
    return { competencia: "", date_a: r.date_a || null, date_b: r.date_b || null, recent_months: null };
  if (r.competencia) return { competencia: r.competencia, date_a: null, date_b: null, recent_months: null };
  return { competencia: "", date_a: null, date_b: null, recent_months: r.recent || "3" };
}

type ResultadoComparacao = { tipo: "datas"; dados: Comparacao } | { tipo: "periodo"; dados: ComparacaoPeriodo };

const P = "/api/inadimplencia/";
const manter = <T,>(anterior: T | undefined) => anterior;

export function Inadimplencia() {
  const cliente = useQueryClient();
  const [rascunho, setRascunho] = useState<Rascunho>(RASCUNHO_PADRAO);
  const [f, setF] = useState<Filtros>(FILTROS_PADRAO);
  const [faixa, setFaixa] = useState<Faixa>(null);
  // Cada Aplicar/Limpar/Apagar é uma rodada nova: busca de novo mesmo com os mesmos filtros.
  const [rodada, setRodada] = useState(0);
  const [datasComp, setDatasComp] = useState(datasComparacaoPadrao);
  const [manual, setManual] = useState<{ a: string; b: string; n: number } | null>(null);

  const competenciasQ = useQuery({
    queryKey: ["inadimplencia", "competencias"],
    queryFn: ({ signal }) => api<Competencias>(P + "competencias", { signal }),
  });

  const base = ["inadimplencia", rodada, f] as const;
  const listaQ = useQuery({
    queryKey: [...base, "list"],
    queryFn: ({ signal }) => api<Lista>(P + "list" + qsFiltros(f), { signal }),
    placeholderData: manter,
  });
  const atualQ = useQuery({
    queryKey: [...base, "atual"],
    queryFn: ({ signal }) => api<Lado>(P + "atual" + qsFiltros(f), { signal }),
    placeholderData: manter,
  });
  const compQ = useQuery({
    queryKey: [...base, "comparacao"],
    queryFn: async ({ signal }): Promise<ResultadoComparacao> =>
      f.competencia
        ? { tipo: "periodo", dados: await api<ComparacaoPeriodo>(P + "comparar-periodo" + qsFiltros(f), { signal }) }
        : { tipo: "datas", dados: await api<Comparacao>(P + "comparar", { signal }) },
    placeholderData: manter,
  });
  const evolQ = useQuery({
    queryKey: [...base, "evolucao", faixa],
    queryFn: ({ signal }) => api<Evolucao>(P + "evolucao" + qsEvolucao(f, faixa), { signal }),
    placeholderData: manter,
  });
  const reincQ = useQuery({
    queryKey: [...base, "reincidencia"],
    queryFn: ({ signal }) => api<Reincidencia>(P + "reincidencia" + qsFiltros(f, false), { signal }),
    placeholderData: manter,
  });

  const listaAtual = !listaQ.isPlaceholderData ? listaQ.data : undefined;
  const temSnapshots = !!listaAtual?.snapshots?.length;
  const porMesQ = useQuery({
    queryKey: [...base, "por-mes"],
    queryFn: ({ signal }) => api<EvolucaoPorMes>(P + "evolucao-por-mes" + qsPorMes(f), { signal }),
    enabled: temSnapshots,
    placeholderData: manter,
  });
  const manualQ = useQuery({
    queryKey: ["inadimplencia", "comparar-manual", manual],
    queryFn: ({ signal }) =>
      api<Comparacao>(P + "comparar" + qsComparar(manual!.a, manual!.b), { signal }),
    enabled: manual != null,
  });

  const apagar = useMutation({
    mutationFn: (id: number) => api<{ ok: boolean }>(`${P}snapshot/${id}`, { method: "DELETE" }),
    onSuccess: () => {
      novaRodada();
      cliente.invalidateQueries({ queryKey: ["inadimplencia", "competencias"] });
    },
    onError: (e) => window.alert("Erro: " + (e as Error).message),
  });

  function novaRodada() {
    setRodada((r) => r + 1);
    setManual(null);
  }

  function aplicar(r: Rascunho) {
    setF(filtrosDoRascunho(r));
    setFaixa(null);
    novaRodada();
  }

  function limpar() {
    setRascunho(RASCUNHO_PADRAO);
    aplicar(RASCUNHO_PADRAO);
  }

  function pedirApagar(s: Snapshot) {
    if (!window.confirm(`Apagar snapshot ${s.filename || ""}?\nEsta ação não pode ser desfeita.`)) return;
    apagar.mutate(s.snapshot_id || s.id);
  }

  const carregando =
    // Os botões de faixa refazem só a evolução: não contam como "Aplicando…".
    listaQ.isFetching || atualQ.isFetching || compQ.isFetching || (evolQ.isFetching && faixa == null) || reincQ.isFetching;

  // Opções de competência: Recentes, o mês atual no topo (mesmo sem snapshot) e as do servidor.
  const atualMes = mesAtual();
  const comps = competenciasQ.data?.competencias ?? [];
  const opcoes = [
    { valor: "", rotulo: "Recentes (linha do tempo)" },
    ...(comps.some((c) => c.value === atualMes) ? [] : [{ valor: atualMes, rotulo: `${rotuloCompetencia(atualMes)} (mês atual)` }]),
    ...comps.map((c) => ({ valor: c.value, rotulo: c.label + (c.value === atualMes ? " (mês atual)" : "") })),
  ];

  const atual = atualQ.isError ? null : atualQ.data;
  const comp = compQ.data;
  const delta =
    !compQ.isError && comp?.tipo === "datas" && comp.dados.a && comp.dados.b && comp.dados.a.snapshot_id != null
      ? comp.dados
      : null;

  return (
    <div className="flex flex-col gap-6 max-w-6xl">
      <section className="flex flex-wrap gap-2 items-center" aria-label="Filtros">
        <select
          className="controle min-w-[190px]"
          aria-label="Competência"
          title="Competência"
          value={rascunho.competencia}
          onChange={(e) => setRascunho((r) => ({ ...r, competencia: e.target.value, date_a: "", date_b: "" }))}
        >
          {opcoes.map((o) => (
            <option key={o.valor} value={o.valor}>{o.rotulo}</option>
          ))}
        </select>
        <select
          className="controle"
          aria-label="Últimos meses"
          title="Últimos meses"
          value={rascunho.recent}
          onChange={(e) => setRascunho({ ...RASCUNHO_PADRAO, recent: e.target.value })}
        >
          {MESES_RECENTES.map((o) => (
            <option key={o.valor} value={o.valor}>{o.rotulo}</option>
          ))}
        </select>
        <div className="intervalo">
          <span className="material-symbols-outlined text-[18px] mr-1" aria-hidden>calendar_month</span>
          <input
            type="date"
            aria-label="Data inicial"
            value={rascunho.date_a}
            onChange={(e) => setRascunho((r) => ({ ...r, competencia: "", date_a: e.target.value }))}
          />
          <span className="px-1" aria-hidden>–</span>
          <input
            type="date"
            aria-label="Data final"
            value={rascunho.date_b}
            onChange={(e) => setRascunho((r) => ({ ...r, competencia: "", date_b: e.target.value }))}
          />
        </div>
        <div className="flex gap-2 md:ml-auto">
          <button type="button" className="controle" onClick={limpar}>
            Limpar
          </button>
          <button
            type="button"
            className="controle controle-primario"
            disabled={carregando}
            onClick={() => aplicar(rascunho)}
          >
            {carregando ? "Aplicando…" : "Aplicar"}
          </button>
        </div>
      </section>

      {carregando && (
        <p role="status" className="text-sm flex items-center gap-2 -mt-3" style={{ color: "var(--text-muted)" }}>
          <span className="inline-block w-4 h-4 rounded-full border-2 border-current border-t-transparent animate-spin" aria-hidden />
          Carregando dados…
        </p>
      )}

      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        <Indicador
          rotulo="Taxa atual"
          valor={fmtPct(atual?.taxa_pct)}
          cor="text-rose-600 dark:text-rose-400"
          rodape={
            delta && (
              <span className={`font-semibold ${corVariacao(delta.delta_taxa_pct)}`}>
                {`${sinal(delta.delta_taxa_pct)}${fmtPct(delta.delta_taxa_pct)} vs 7d`}
              </span>
            )
          }
        />
        <Indicador
          rotulo="Total inadimplentes"
          valor={fmt(atual?.inadimplentes)}
          rodape={
            delta && (
              <span className={`font-semibold ${corVariacao(delta.delta_inadimplentes)}`}>
                {`${sinal(delta.delta_inadimplentes)}${fmt(delta.delta_inadimplentes)} vs 7d`}
              </span>
            )
          }
        />
        <Indicador rotulo="Alunos em curso" valor={fmt(atual?.em_curso)} rodape="Denominador atual" />
      </div>

      {listaQ.isError ? (
        <div role="alert" className="cartao p-5 text-sm">{textoErro(listaQ.error)}</div>
      ) : listaQ.data && !listaQ.data.snapshots?.length ? (
        <Vazio competencia={f.competencia} />
      ) : listaQ.data ? (
        <>
          <Cartao titulo="Comparar 2 datas" icone="compare_arrows">
            <div className="flex flex-wrap items-center gap-2">
              <div className="intervalo">
                <input
                  type="date"
                  aria-label="Comparar de"
                  value={datasComp.a}
                  onChange={(e) => setDatasComp((d) => ({ ...d, a: e.target.value }))}
                />
                <span className="px-1" aria-hidden>–</span>
                <input
                  type="date"
                  aria-label="Comparar até"
                  value={datasComp.b}
                  onChange={(e) => setDatasComp((d) => ({ ...d, b: e.target.value }))}
                />
              </div>
              <button
                type="button"
                className="controle"
                onClick={() => setManual((m) => ({ a: datasComp.a, b: datasComp.b, n: (m?.n ?? 0) + 1 }))}
              >
                Comparar
              </button>
            </div>
            {manual ? (
              manualQ.isError ? (
                <ErroComparacao erro={manualQ.error} />
              ) : manualQ.data ? (
                <ComparacaoDatas dados={manualQ.data} />
              ) : null
            ) : compQ.isError ? (
              <ErroComparacao erro={compQ.error} />
            ) : comp?.tipo === "periodo" ? (
              <ComparacaoPeriodoView dados={comp.dados} />
            ) : comp?.tipo === "datas" ? (
              <ComparacaoDatas dados={comp.dados} />
            ) : null}
          </Cartao>

          <ReincidenciaCartao dados={reincQ.isError ? undefined : reincQ.data} erro={reincQ.error} />

          <GraficoEvolucao
            dados={evolQ.isError ? undefined : evolQ.data}
            erro={evolQ.error}
            carregando={evolQ.isPending}
            faixa={faixa}
            onFaixa={setFaixa}
          />

          <GraficoCompetencias
            dados={porMesQ.isError ? undefined : porMesQ.data}
            erro={porMesQ.error}
            carregando={porMesQ.isPending}
          />

          <Historico
            snapshots={listaQ.data.snapshots}
            apagando={apagar.isPending ? (apagar.variables ?? null) : null}
            onApagar={pedirApagar}
          />
        </>
      ) : null}
    </div>
  );
}

function Vazio({ competencia }: { competencia: string }) {
  return (
    <div className="cartao p-12 text-center">
      <span className="material-symbols-outlined text-[48px] mb-3" style={{ color: "var(--text-muted)" }} aria-hidden>
        description
      </span>
      {competencia ? (
        <>
          <p className="text-lg font-semibold mb-2">{`Sem dados para ${rotuloCompetencia(competencia)}`}</p>
          <p className="text-sm" style={{ color: "var(--text-muted)" }}>
            Nenhum snapshot de inadimplência para esta competência.
            <br />
            Selecione outra competência acima ou suba um arquivo em <strong>Upload Acadêmico → Inadimplentes</strong>.
          </p>
        </>
      ) : (
        <>
          <p className="text-lg font-semibold mb-2">Nenhum dado de inadimplência</p>
          <p className="text-sm" style={{ color: "var(--text-muted)" }}>
            Nenhum snapshot de inadimplência foi feito ainda.
            <br />
            Suba o primeiro arquivo em <strong>Upload Acadêmico → Inadimplentes</strong>.
          </p>
        </>
      )}
    </div>
  );
}

const FAIXAS_REINC = [
  { chave: "1", rotulo: "1 competência" },
  { chave: "2", rotulo: "2 competências" },
  { chave: "3", rotulo: "3 competências" },
  { chave: "4_plus", rotulo: "4+ competências" },
] as const;

function ReincidenciaCartao({ dados, erro }: { dados: Reincidencia | undefined; erro: unknown }) {
  const n = dados?.rgms_analisados ?? 0;
  const usadas = dados?.competencias_usadas ?? [];
  const meta = erro
    ? `Erro: ${(erro as Error).message}`
    : !dados
      ? "—"
      : usadas.length > 0
        ? `${fmt(n)} RGM${plural(n)} único${plural(n)} analisado${plural(n)} · ${usadas.length} competência${plural(usadas.length)} considerada${plural(usadas.length)}: ${usadas.join(", ")}`
        : "Nenhuma competência com dados disponível.";
  return (
    <Cartao titulo="Reincidência de inadimplência" sufixo="(RGMs em múltiplas competências)" icone="autorenew">
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {FAIXAS_REINC.map((b) => (
          <div
            key={b.chave}
            role="group"
            aria-label={b.rotulo}
            className="rounded-xl p-4 text-center"
            style={{ background: "var(--bg-elevated)", border: "1px solid var(--border-subtle)" }}
          >
            <p className="rotulo mb-1">{b.rotulo}</p>
            <p className="text-3xl font-extrabold tabular-nums">{dados ? fmt(dados.buckets[b.chave] || 0) : "—"}</p>
            <p className="text-xs mt-1" style={{ color: "var(--text-muted)" }}>alunos</p>
          </div>
        ))}
      </div>
      <p className="text-xs mt-4" style={{ color: "var(--text-muted)" }}>{meta}</p>
    </Cartao>
  );
}
