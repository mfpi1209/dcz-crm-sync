import { CategoryScale, Chart, LinearScale, LineController, LineElement, PointElement, Tooltip, type ChartOptions } from "chart.js";
import { useEffect, useRef, useState } from "react";
import { rotuloEixo } from "./formato";

// Só o necessário para um gráfico de linhas com dica (sem legenda do chart.js: a tela tem a sua).
Chart.register(LineController, LineElement, PointElement, CategoryScale, LinearScale, Tooltip);

export interface Serie {
  rotulo: string;
  dados: number[];
  /** Nome da variável CSS da cor (ex.: "--grafico-novos"). */
  cor: string;
}

/** Valor de uma variável CSS agora (muda com o tema claro/escuro). */
function lerCor(nome: string): string {
  return getComputedStyle(document.documentElement).getPropertyValue(nome).trim() || "#64748b";
}

/** Muda quando o tema troca (classe "dark" no <html>), para redesenhar com as cores novas. */
function useTema(): string {
  const [tema, setTema] = useState(() => document.documentElement.className);
  useEffect(() => {
    const obs = new MutationObserver(() => setTema(document.documentElement.className));
    obs.observe(document.documentElement, { attributes: true, attributeFilter: ["class"] });
    return () => obs.disconnect();
  }, []);
  return tema;
}

export function GraficoLinha({
  rotulos,
  series,
  aoClicar,
  descricao,
}: {
  rotulos: string[];
  series: Serie[];
  aoClicar?: (indice: number) => void;
  descricao: string;
}) {
  const tela = useRef<HTMLCanvasElement>(null);
  const clique = useRef(aoClicar);
  clique.current = aoClicar;
  const tema = useTema();

  useEffect(() => {
    if (!tela.current) return;
    const grade = lerCor("--grafico-grade");
    const texto = lerCor("--text-muted");
    const opcoes: ChartOptions<"line"> = {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 400 },
      interaction: { mode: "index", intersect: false },
      onClick: (_evt, elementos) => {
        if (elementos.length) clique.current?.(elementos[0].index);
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (c) => `${c.dataset.label}: ${(c.parsed.y ?? 0).toLocaleString("pt-BR")}`,
          },
        },
      },
      scales: {
        x: { grid: { color: grade }, ticks: { color: texto, font: { size: 10 }, maxRotation: 0 } },
        y: { grid: { color: grade }, ticks: { color: texto, font: { size: 10 }, callback: (v) => rotuloEixo(Number(v)) } },
      },
    };
    const grafico = new Chart(tela.current, {
      type: "line",
      data: {
        labels: rotulos,
        datasets: series.map((s) => {
          const c = lerCor(s.cor);
          return {
            label: s.rotulo,
            data: s.dados,
            borderColor: c,
            backgroundColor: c,
            pointBackgroundColor: c,
            borderWidth: 2,
            pointRadius: 3,
            pointHoverRadius: 6,
            fill: false,
            tension: 0.35,
          };
        }),
      },
      options: opcoes,
    });
    return () => grafico.destroy();
  }, [rotulos, series, tema]);

  return <canvas ref={tela} role="img" aria-label={descricao} />;
}
