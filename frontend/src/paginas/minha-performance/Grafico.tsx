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
import { useEffect, useRef, useState } from "react";

// Só o que os gráficos desta tela usam (barras, linhas, área).
Chart.register(BarController, LineController, BarElement, LineElement, PointElement, CategoryScale, LinearScale, Filler, Legend, Tooltip);

/** Cor de uma variável CSS do tema atual (chart.js desenha em canvas e não lê var()). */
export function corCss(nome: string, reserva: string): string {
  if (typeof window === "undefined") return reserva;
  const v = getComputedStyle(document.documentElement).getPropertyValue(nome).trim();
  return v || reserva;
}

/** "claro" | "escuro", acompanhando a classe `dark` do <html> (troca de tema na barra de cima). */
export function useTema(): "claro" | "escuro" {
  const ler = (): "claro" | "escuro" => (document.documentElement.classList.contains("dark") ? "escuro" : "claro");
  const [tema, setTema] = useState(ler);
  useEffect(() => {
    const obs = new MutationObserver(() => setTema(ler()));
    obs.observe(document.documentElement, { attributes: true, attributeFilter: ["class"] });
    return () => obs.disconnect();
  }, []);
  return tema;
}

type Opcoes = Record<string, Record<string, unknown> | undefined>;

/** Textos (eixos, legenda) e grade nas cores do tema atual; o que a configuração já define fica. */
function comTema(config: ChartConfiguration): ChartConfiguration {
  const texto = corCss("--text-muted", "#64748b");
  const grade = corCss("--border-subtle", "rgba(148,163,184,.15)");
  const opcoes = { ...(config.options as Record<string, unknown>) };
  const scales = { ...((opcoes.scales as Opcoes) || {}) };
  for (const k of Object.keys(scales)) {
    const s = scales[k] || {};
    scales[k] = {
      ...s,
      ticks: { color: texto, ...((s.ticks as object) || {}) },
      grid: { color: grade, ...((s.grid as object) || {}) },
    };
  }
  const plugins = { ...((opcoes.plugins as Opcoes) || {}) };
  const legenda = plugins.legend || {};
  plugins.legend = { ...legenda, labels: { color: texto, ...((legenda.labels as object) || {}) } };
  return { ...config, options: { ...opcoes, color: texto, scales, plugins } } as ChartConfiguration;
}

/** Canvas do chart.js; refaz o gráfico quando a configuração ou o tema mudam e destrói ao sair. */
export function Grafico({ config, altura, rotulo }: { config: ChartConfiguration; altura: number; rotulo: string }) {
  const ref = useRef<HTMLCanvasElement>(null);
  const tema = useTema();
  useEffect(() => {
    if (!ref.current) return;
    const g = new Chart(ref.current, comTema(config));
    return () => g.destroy();
  }, [config, tema]);
  return (
    <div style={{ height: altura, position: "relative" }}>
      <canvas ref={ref} role="img" aria-label={rotulo} />
    </div>
  );
}
