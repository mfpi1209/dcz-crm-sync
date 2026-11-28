import { useRef } from "react";
import { diaMes, fmt } from "./formato";
import { cor, hexRgba, useGrafico } from "./grafico";
import { Cartao, MensagemCartao, textoErro } from "./comum";
import type { Evolucao, Faixa } from "./tipos";

const FAIXAS: { valor: Exclude<Faixa, null>; rotulo: string }[] = [
  { valor: "7", rotulo: "7d" },
  { valor: "30", rotulo: "30d" },
  { valor: "90", rotulo: "90d" },
  { valor: "all", rotulo: "Tudo" },
];

/** "Evolução da taxa": linha por data (dd/mm), com os botões de faixa. */
export function GraficoEvolucao({
  dados,
  erro,
  carregando,
  faixa,
  onFaixa,
}: {
  dados: Evolucao | undefined;
  erro: unknown;
  carregando: boolean;
  faixa: Faixa;
  onFaixa: (f: Exclude<Faixa, null>) => void;
}) {
  const canvas = useRef<HTMLCanvasElement>(null);
  const pontos = dados?.points ?? [];

  useGrafico(
    canvas,
    () => {
      if (!pontos.length) return null;
      const taxa = cor("--grafico-taxa");
      const texto = cor("--grafico-texto");
      const grade = cor("--grafico-grade");
      return {
        type: "line",
        data: {
          labels: pontos.map((p) => diaMes(p.date)),
          datasets: [
            {
              label: "% Inadimplência",
              data: pontos.map((p) => p.taxa_pct),
              borderColor: taxa,
              backgroundColor: taxa.startsWith("#") ? hexRgba(taxa, 0.08) : taxa,
              fill: true,
              tension: 0.35,
              borderWidth: 2.5,
              pointRadius: 4,
              pointHoverRadius: 6,
              pointBackgroundColor: taxa,
              pointBorderColor: taxa.startsWith("#") ? hexRgba(taxa, 0.3) : taxa,
              pointBorderWidth: 2,
            },
          ],
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          interaction: { mode: "index", intersect: false },
          plugins: {
            legend: { display: false },
            tooltip: {
              callbacks: {
                label: (ctx) => ` Taxa: ${ctx.parsed.y.toFixed(2).replace(".", ",")}%`,
                afterBody: (items) => {
                  const p = pontos[items[0].dataIndex];
                  return [` Inadimplentes: ${fmt(p.inadimplentes)}`, ` Em curso: ${fmt(p.em_curso)}`];
                },
              },
            },
          },
          scales: {
            x: { ticks: { color: texto, maxRotation: 45, font: { size: 11 } }, grid: { color: grade } },
            y: {
              ticks: {
                color: texto,
                callback: (v) => Number(v).toFixed(1).replace(".", ",") + "%",
                font: { size: 11 },
              },
              grid: { color: grade },
              beginAtZero: false,
            },
          },
        },
      };
    },
    [dados],
  );

  return (
    <Cartao
      titulo="Evolução da taxa"
      icone="show_chart"
      acoes={
        <div className="segmentado" role="group" aria-label="Janela da evolução">
          {FAIXAS.map((f) => (
            <button key={f.valor} type="button" aria-pressed={faixa === f.valor} onClick={() => onFaixa(f.valor)}>
              {f.rotulo}
            </button>
          ))}
        </div>
      }
    >
      <div className="relative h-72">
        {erro ? (
          <MensagemCartao>{textoErro(erro)}</MensagemCartao>
        ) : carregando && !dados ? (
          <MensagemCartao>Carregando…</MensagemCartao>
        ) : pontos.length ? (
          <canvas ref={canvas} aria-label="Gráfico da evolução da taxa" role="img" />
        ) : (
          <MensagemCartao>Sem snapshots no período selecionado.</MensagemCartao>
        )}
      </div>
    </Cartao>
  );
}
