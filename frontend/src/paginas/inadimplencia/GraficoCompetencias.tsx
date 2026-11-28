import { useEffect, useMemo, useRef, useState } from "react";
import { Cartao, MensagemCartao, textoErro } from "./comum";
import { fmt } from "./formato";
import { cor, hexRgba, paleta, useGrafico } from "./grafico";
import type { EvolucaoPorMes, PontoMes } from "./tipos";

interface Item {
  label: string;
  nivel: string;
  ponto: PontoMes;
  exato: boolean;
}

function todasAsDatas(d: EvolucaoPorMes | undefined): string[] {
  const s = new Set<string>();
  for (const c of d?.competencias ?? []) for (const p of c.points ?? []) if (p.date) s.add(p.date);
  return Array.from(s).sort();
}

/** Uma barra por competência: o ponto do dia escolhido, ou o último disponível. */
function itens(d: EvolucaoPorMes | undefined, dia: string | null): Item[] {
  return (d?.competencias ?? [])
    .map((c) => {
      const pts = c.points ?? [];
      if (!pts.length) return null;
      const exato = dia ? pts.find((p) => p.date === dia) : undefined;
      return { label: c.label || c.nivel, nivel: c.nivel, ponto: exato ?? pts[pts.length - 1], exato: !!exato };
    })
    .filter((i): i is Item => i !== null)
    .sort((a, b) => a.nivel.localeCompare(b.nivel));
}

const dataBr = (iso: string) => iso.split("-").reverse().join("/");

/**
 * "Comparativo por competência" (evolucao-por-mes). Trocar o dia só redesenha;
 * dados novos mantêm o dia escolhido se ele ainda existir, senão vai ao mais recente.
 */
export function GraficoCompetencias({
  dados,
  erro,
  carregando,
}: {
  dados: EvolucaoPorMes | undefined;
  erro: unknown;
  carregando: boolean;
}) {
  const canvas = useRef<HTMLCanvasElement>(null);
  const datas = useMemo(() => todasAsDatas(dados), [dados]);
  const [dia, setDia] = useState<string | null>(null);

  useEffect(() => {
    if (!datas.length) {
      setDia(null); // como o _syncDateInput: sem datas, o campo fica vazio
      return;
    }
    setDia((atual) => (atual && datas.includes(atual) ? atual : datas[datas.length - 1]));
  }, [datas]);

  const lista = useMemo(() => itens(dados, dia), [dados, dia]);

  useGrafico(
    canvas,
    () => {
      if (!lista.length) return null;
      const cores = paleta();
      const corDe = (i: number) => cores[i % cores.length];
      const texto = cor("--grafico-texto");
      const fraco = cor("--grafico-texto-fraco");
      const grade = cor("--grafico-grade");
      return {
        type: "bar",
        data: {
          labels: lista.map((i) => i.label),
          datasets: [
            {
              label: "% Inadimplência",
              data: lista.map((i) => i.ponto.taxa_pct),
              // Barra de dia que faltou: cor mais clara e rótulo "(últ.)" em cinza (como na antiga).
              backgroundColor: lista.map((it, i) => hexRgba(corDe(i), it.exato ? 0.7 : 0.35)),
              borderColor: lista.map((_, i) => corDe(i)),
              borderWidth: 1.5,
              borderRadius: 6,
              _itens: lista,
            } as never,
          ],
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { display: false },
            tooltip: {
              callbacks: {
                title: (ctx) => ctx[0].label,
                label: (ctx) => ` Taxa: ${ctx.parsed.y.toFixed(2).replace(".", ",")}%`,
                afterBody: (ctx) => {
                  const item = (ctx[0].dataset as unknown as { _itens: Item[] })._itens[ctx[0].dataIndex];
                  const p = item.ponto;
                  const linhas = [
                    ` Atualizado em: ${p.date ? dataBr(p.date) : "—"}`,
                    ` Inadimplentes: ${fmt(p.inadimplentes)}`,
                    ` Em curso: ${fmt(p.em_curso)}`,
                  ];
                  if (!item.exato && dia) linhas.unshift(` Sem snapshot em ${dataBr(dia)} — exibindo último disponível`);
                  return linhas;
                },
              },
            },
          },
          scales: {
            x: {
              ticks: {
                color: (ctx) => (lista[ctx.index] && !lista[ctx.index].exato ? fraco : texto),
                font: { size: 11 },
                callback: function (this: { getLabelForValue: (v: number) => string }, val, idx) {
                  const it = lista[idx];
                  const label = this.getLabelForValue(Number(val));
                  if (!it || !it.ponto || !it.ponto.date) return label;
                  const d = it.ponto.date.split("-").reverse().slice(0, 2).join("/");
                  return [label, it.exato ? d : `${d} (últ.)`];
                },
              },
              grid: { display: false },
            },
            y: {
              ticks: {
                color: texto,
                callback: (v) => Number(v).toFixed(1).replace(".", ",") + "%",
                font: { size: 11 },
              },
              grid: { color: grade },
              beginAtZero: true,
            },
          },
        },
      };
    },
    [lista],
  );

  return (
    <Cartao
      titulo="Comparativo por competência"
      icone="bar_chart"
      acoes={
        <label className="flex items-center gap-2">
          <span className="rotulo">Dia</span>
          <span className="intervalo">
            <input
              type="date"
              aria-label="Dia"
              value={dia ?? ""}
              min={datas[0]}
              max={datas[datas.length - 1]}
              onChange={(e) => setDia(e.target.value || null)}
            />
          </span>
        </label>
      }
    >
      <div className="relative h-80">
        {erro ? (
          <MensagemCartao>{textoErro(erro)}</MensagemCartao>
        ) : carregando && !dados ? (
          <MensagemCartao>Carregando…</MensagemCartao>
        ) : lista.length ? (
          <canvas ref={canvas} aria-label="Gráfico da taxa por competência" role="img" />
        ) : (
          <MensagemCartao>Sem snapshots para comparar entre competências.</MensagemCartao>
        )}
      </div>
    </Cartao>
  );
}
