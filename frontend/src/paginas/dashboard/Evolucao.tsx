import { useMemo, useState } from "react";
import { POS, numero, resumoFiltros, rotuloPeriodo, type Detalhe } from "./formato";
import { GraficoLinha, type Serie } from "./Grafico";
import type { Evolucao as RespEvolucao, FiltrosDash } from "./tipos";

/**
 * Evolução de matrículas (loadTimeline + _renderGeralChart). Agregado: calouros
 * (novos + regresso + recompra), rematrículas e total; detalhado: as cinco séries.
 * Clique num mês abre os dias dele; Voltar retorna aos meses.
 */
export function EvolucaoMatriculas({
  dados,
  erro,
  atualizando,
  filtros,
  detalhe,
  abrirMes,
  voltar,
}: {
  dados: RespEvolucao | undefined;
  erro: string | null;
  atualizando: boolean;
  filtros: FiltrosDash;
  detalhe: Detalhe | null;
  abrirMes: (periodo: string) => void;
  voltar: () => void;
}) {
  const [detalhado, setDetalhado] = useState(false);
  const periodos = dados?.periods ?? [];
  const s = dados?.series ?? {};
  const remat = filtros.nivel === POS ? "Veteranos" : "Rematrículas";
  const soma = (a?: number[]) => (a ?? []).reduce((x, y) => x + (y || 0), 0);
  const granularidade = detalhe ? "day" : "month";

  const rotulos = useMemo(() => periodos.map((p) => rotuloPeriodo(p, granularidade)), [periodos, granularidade]);
  const series = useMemo<Serie[]>(() => {
    if (!detalhado) {
      const novos = s.novos ?? [];
      const calouros = novos.map((v, i) => (v || 0) + (s.regresso?.[i] || 0) + (s.recompra?.[i] || 0));
      return [
        { rotulo: "Calouros (Novos+Regresso+Recompra)", dados: calouros, cor: "--grafico-novos" },
        { rotulo: remat, dados: s.rematricula ?? [], cor: "--grafico-rematricula" },
        { rotulo: "Total", dados: s.total ?? [], cor: "--grafico-total" },
      ];
    }
    return [
      { rotulo: "Novos", dados: s.novos ?? [], cor: "--grafico-novos" },
      { rotulo: remat, dados: s.rematricula ?? [], cor: "--grafico-rematricula" },
      { rotulo: "Regresso", dados: s.regresso ?? [], cor: "--grafico-regresso" },
      { rotulo: "Recompra", dados: s.recompra ?? [], cor: "--grafico-recompra" },
      { rotulo: "Total", dados: s.total ?? [], cor: "--grafico-total" },
    ];
  }, [s, detalhado, remat]);

  const legenda: { rotulo: string; valor: number; cor: string }[] = detalhado
    ? [
        { rotulo: "Novos", valor: soma(s.novos), cor: "--grafico-novos" },
        { rotulo: remat, valor: soma(s.rematricula), cor: "--grafico-rematricula" },
        { rotulo: "Regresso", valor: soma(s.regresso), cor: "--grafico-regresso" },
        { rotulo: "Recompra", valor: soma(s.recompra), cor: "--grafico-recompra" },
        { rotulo: "Total", valor: soma(s.total), cor: "--grafico-total" },
      ]
    : [
        { rotulo: "Calouros (N+Rg+Rc)", valor: soma(s.novos) + soma(s.regresso) + soma(s.recompra), cor: "--grafico-novos" },
        { rotulo: remat, valor: soma(s.rematricula), cor: "--grafico-rematricula" },
        { rotulo: "Total", valor: soma(s.total), cor: "--grafico-total" },
      ];

  const faixa = dados?.range ? `${dados.range.from} → ${dados.range.to}` : "";
  const legendaPeriodo = detalhe ? detalhe.mes : [resumoFiltros(filtros), faixa].filter(Boolean).join(" · ");

  return (
    <section className="cartao overflow-hidden" aria-label="Evolução de matrículas">
      <div className="px-5 pt-5 pb-3 flex flex-wrap items-center justify-between gap-3">
        <h2 className="text-base flex items-center gap-2">
          <span className="material-symbols-outlined text-[20px]" aria-hidden style={{ color: "var(--primary)" }}>show_chart</span>
          Evolução de matrículas
        </h2>
        <div className="flex flex-wrap items-center gap-2">
          {detalhe && (
            <button type="button" className="controle" onClick={voltar}>
              <span className="material-symbols-outlined text-[18px]" aria-hidden>arrow_back</span>
              Voltar
            </button>
          )}
          <button type="button" className="controle" onClick={() => setDetalhado((d) => !d)}>
            {detalhado ? "Ver agregado" : "Ver detalhado"}
          </button>
          {legendaPeriodo && <span className="text-[11px] font-mono" style={{ color: "var(--text-muted)" }}>{legendaPeriodo}</span>}
        </div>
      </div>
      <div className="px-5 pb-5">
        <ul className="flex flex-wrap items-center gap-x-6 gap-y-2 mb-4" aria-label="Legenda">
          {legenda.map((l) => (
            <li key={l.rotulo} className="flex items-center gap-2">
              <span className="w-3 h-3 rounded-full" aria-hidden style={{ background: `var(${l.cor})` }} />
              <span className="text-xs" style={{ color: "var(--text-secondary)" }}>{l.rotulo}</span>
              <span className="text-sm font-bold font-mono tabular-nums">{numero(l.valor)}</span>
            </li>
          ))}
        </ul>
        <div
          className="rounded-2xl p-3 h-[300px] relative"
          style={{ background: "var(--bg-elevated)", border: "1px solid var(--border-subtle)", opacity: atualizando ? 0.6 : 1 }}
        >
          {erro ? (
            <p role="alert" className="h-full flex items-center justify-center text-sm" style={{ color: "var(--danger)" }}>
              Não foi possível carregar a evolução: {erro}
            </p>
          ) : !dados ? (
            <p className="h-full flex items-center justify-center text-sm" style={{ color: "var(--text-muted)" }}>Carregando…</p>
          ) : !periodos.length ? (
            <p className="h-full flex items-center justify-center text-sm" style={{ color: "var(--text-muted)" }}>Sem matrículas no período.</p>
          ) : (
            <GraficoLinha
              rotulos={rotulos}
              series={series}
              descricao="Gráfico da evolução de matrículas"
              aoClicar={detalhe ? undefined : (i) => periodos[i] && abrirMes(periodos[i])}
            />
          )}
        </div>
        {!detalhe && periodos.length > 0 && (
          <p className="text-[11px] mt-2" style={{ color: "var(--text-muted)" }}>Clique num mês para ver os dias.</p>
        )}
      </div>
    </section>
  );
}
