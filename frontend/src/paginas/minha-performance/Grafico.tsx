import {
  BarController,
  BarElement,
  CategoryScale,
  Chart,
  Filler,
  Legend,
  LineController,
  LineElement,
  LinearScale,
  PointElement,
  Tooltip,
  type ChartConfiguration,
} from "chart.js";
import { useEffect, useRef } from "react";

// Só o que os gráficos desta tela usam (barras, linhas, área).
Chart.register(BarController, LineController, BarElement, LineElement, PointElement, CategoryScale, LinearScale, Filler, Legend, Tooltip);

/** Cor de uma variável CSS do tema atual (chart.js desenha em canvas e não lê var()). */
export function corCss(nome: string, reserva: string): string {
  if (typeof window === "undefined") return reserva;
  const v = getComputedStyle(document.documentElement).getPropertyValue(nome).trim();
  return v || reserva;
}

/** Canvas do chart.js; refaz o gráfico quando a configuração muda e destrói ao sair. */
export function Grafico({ config, altura, rotulo }: { config: ChartConfiguration; altura: number; rotulo: string }) {
  const ref = useRef<HTMLCanvasElement>(null);
  useEffect(() => {
    if (!ref.current) return;
    const g = new Chart(ref.current, config);
    return () => g.destroy();
  }, [config]);
  return (
    <div style={{ height: altura, position: "relative" }}>
      <canvas ref={ref} role="img" aria-label={rotulo} />
    </div>
  );
}
