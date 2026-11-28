import type { ChartConfiguration } from "chart.js";
import { useMemo } from "react";
import { Barra, Secao, Selo, TEXTO_FRACO } from "./comum";
import { inteiro } from "./formato";
import { Grafico } from "./Grafico";
import type { Insights } from "./tipos";

/** _mpRenderSuportePainel + _mpRenderEquipeSuporte + _mpRenderSuporteMatChart. */
export function PainelSuporte({ d, campanhaId }: { d: Insights; campanhaId: number | null }) {
  const eq = d.meta_equipe_suporte;
  const comercial = eq?.matriculas_fonte === "comercial";
  const selo = comercial ? "Comercial inteiro" : eq ? `${eq.agentes_count || 0} no time` : "";
  const totalMat = d.total_matriculas_time ?? eq?.total_matriculas ?? 0;
  const media = d.media_vendas ?? d.pace_atual ?? 0;
  const serie = d.chart_matriculas_dia || [];

  const config = useMemo<ChartConfiguration | null>(() => {
    if (!serie.length) return null;
    const labels = serie.map((p) => {
      const partes = String(p.data || "").split("-");
      return partes.length === 3 ? `${partes[2]}/${partes[1]}` : p.data;
    });
    return {
      type: "line",
      data: {
        labels,
        datasets: [{ label: "Em curso", data: serie.map((p) => p.matriculas || 0), borderColor: "#3b82f6", backgroundColor: "#3b82f633", fill: true, tension: 0.4, borderWidth: 3, pointRadius: 2 }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: true, labels: { boxWidth: 10, font: { size: 11 } } } },
        scales: { x: { ticks: { font: { size: 10 }, maxRotation: labels.length > 8 ? 45 : 0 } }, y: { beginAtZero: true, ticks: { font: { size: 10 } } } },
      },
    };
  }, [serie]);

  let metaTime;
  if (!eq || !(eq.meta > 0)) {
    const cid = d.campanha?.id || campanhaId || "";
    metaTime = (
      <p className="text-xs" style={TEXTO_FRACO}>
        Nenhuma meta do time para esta campanha.
        <br />
        Configure em <strong>Premiação</strong> → Suporte Comercial (campanha #{cid || "…"}).
      </p>
    );
  } else {
    const total = eq.total_matriculas || 0;
    const meta = eq.meta || 0;
    const pct = eq.pct ?? (meta > 0 ? Math.min(100, Math.round((total / meta) * 100)) : 0);
    const bateu = eq.meta_batida ?? total >= meta;
    metaTime = (
      <div className="flex flex-col gap-2">
        <div className="flex flex-wrap items-center gap-2">
          <Selo cor={bateu ? "bg-emerald-500/15 text-emerald-700 dark:text-emerald-300 border-emerald-500/30" : "bg-violet-500/15 text-violet-700 dark:text-violet-300 border-violet-500/30"}>
            {bateu ? "Meta batida" : "Em andamento"}
          </Selo>
          <span className="text-xs" style={TEXTO_FRACO}>
            {comercial ? "Comercial inteiro" : `${eq.agentes_count || 0} no time`}
          </span>
        </div>
        <div className="flex justify-between text-xs" style={TEXTO_FRACO}>
          <span>Meta do time</span>
          <span className="tabular-nums font-semibold text-blue-700 dark:text-blue-400">{`${total}/${meta} (${pct}%)`}</span>
        </div>
        <Barra pct={pct} cor="bg-blue-500" />
        <p className="text-xs" style={TEXTO_FRACO}>
          {comercial
            ? "Matrículas em curso de todo o Comercial, alinhadas ao Dashboard Comercial."
            : eq.is_suporte
              ? "Soma de todo o Suporte Comercial no período."
              : "Visão do time de suporte."}
        </p>
      </div>
    );
  }

  return (
    <Secao titulo="Suporte comercial — time" icone="groups" extra={selo ? <Selo cor="bg-violet-500/10 text-violet-700 dark:text-violet-300 border-violet-500/25">{selo}</Selo> : null}>
      <div className="cartao p-5 flex flex-col gap-3">
        <p className="text-xs" style={TEXTO_FRACO}>
          Gráfico e calendário pela <strong>data de matrícula</strong> (base comercial, em curso). PIX diário pela fila de aceite (todos consultores).
        </p>
        {metaTime}
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
        <div className="cartao p-5" role="group" aria-label="Matrículas no período">
          <p className="rotulo mb-1">Matrículas no período</p>
          <p className="text-3xl font-headline font-extrabold tabular-nums">{inteiro(totalMat)}</p>
        </div>
        <div className="cartao p-5" role="group" aria-label="Média de vendas">
          <p className="rotulo mb-1">Média de vendas</p>
          <p className="text-3xl font-headline font-extrabold tabular-nums">{Number(media).toFixed(1)}</p>
          <p className="text-xs" style={TEXTO_FRACO}>
            matrículas / dia útil
          </p>
        </div>
      </div>
      <div className="cartao p-4 flex flex-col gap-2">
        <p className="text-sm font-semibold">Matrículas por dia</p>
        {config ? (
          <Grafico config={config} altura={300} rotulo="Matrículas por dia" />
        ) : (
          <p className="text-xs text-center py-12" style={TEXTO_FRACO}>
            Sem matrículas no período
          </p>
        )}
      </div>
    </Secao>
  );
}
