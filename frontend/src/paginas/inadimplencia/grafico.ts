import {
  BarController,
  BarElement,
  CategoryScale,
  Chart,
  type ChartConfiguration,
  Filler,
  LinearScale,
  LineController,
  LineElement,
  PointElement,
  Tooltip,
} from "chart.js";
import { type RefObject, useEffect, useState } from "react";

// Só o que os dois gráficos da tela usam (linha da evolução e barras por competência).
Chart.register(
  LineController,
  LineElement,
  PointElement,
  Filler,
  BarController,
  BarElement,
  CategoryScale,
  LinearScale,
  Tooltip,
);

/** Valores de reserva (cores da antiga), usados quando o CSS não carregou (testes). */
const RESERVA: Record<string, string> = {
  "--grafico-taxa": "#f43f5e",
  "--grafico-texto": "#64748b",
  "--grafico-texto-fraco": "#94a3b8",
  "--grafico-grade": "rgba(100,116,139,0.15)",
};
const PALETA_RESERVA = [
  "#f43f5e", "#6366f1", "#10b981", "#f59e0b", "#06b6d4",
  "#a855f7", "#ec4899", "#84cc16", "#0ea5e9", "#f97316",
];

export function cor(nome: string): string {
  const v = getComputedStyle(document.documentElement).getPropertyValue(nome).trim();
  return v || RESERVA[nome] || "#64748b";
}

/** Paleta das barras (--grafico-1 … --grafico-10, em hex). */
export function paleta(): string[] {
  return PALETA_RESERVA.map((reserva, i) => {
    const v = getComputedStyle(document.documentElement).getPropertyValue(`--grafico-${i + 1}`).trim();
    return v || reserva;
  });
}

export function hexRgba(hex: string, alfa: number): string {
  const h = hex.replace("#", "");
  const r = parseInt(h.substring(0, 2), 16);
  const g = parseInt(h.substring(2, 4), 16);
  const b = parseInt(h.substring(4, 6), 16);
  return `rgba(${r},${g},${b},${alfa})`;
}

/** Tema atual ("dark" | "light"); muda quando a casca troca a classe do <html>. */
export function useTema(): string {
  const ler = () => (document.documentElement.classList.contains("dark") ? "dark" : "light");
  const [tema, setTema] = useState(ler);
  useEffect(() => {
    const obs = new MutationObserver(() => setTema(ler()));
    obs.observe(document.documentElement, { attributes: true, attributeFilter: ["class"] });
    return () => obs.disconnect();
  }, []);
  return tema;
}

/**
 * Desenha (e redesenha) um gráfico no canvas. `montar` é chamado quando
 * `deps` ou o tema mudam; devolver null apaga o gráfico.
 */
export function useGrafico(
  canvas: RefObject<HTMLCanvasElement>,
  montar: () => ChartConfiguration | null,
  deps: unknown[],
) {
  const tema = useTema();
  useEffect(() => {
    const config = montar();
    if (!config || !canvas.current) return;
    const g = new Chart(canvas.current, config);
    return () => g.destroy();
    // `montar` muda a cada render; quem manda são as deps.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...deps, tema]);
}
