import { diaMes, num } from "./formato";
import type { LinhaRanking } from "./tipos";

/** Matrículas por dia: colunas clicáveis; o clique filtra o ranking por aquele dia. */
export function GraficoDias({
  evolucao,
  dia,
  onDia,
}: {
  evolucao: { dia: string; total: number }[];
  dia: string | null;
  onDia: (dia: string) => void;
}) {
  if (!evolucao.length) {
    return (
      <p className="text-sm py-8 text-center" style={{ color: "var(--text-muted)" }}>
        Sem matrícula no período.
      </p>
    );
  }
  const max = evolucao.reduce((m, e) => Math.max(m, e.total), 1);
  return (
    <div className="overflow-x-auto">
      <div className="flex items-end gap-1 h-56 min-w-full pt-5" style={{ minWidth: `${evolucao.length * 1.6}rem` }}>
        {evolucao.map((e) => {
          const d = String(e.dia).slice(0, 10);
          const ativo = d === dia;
          const rotulo = diaMes(d);
          return (
            <button
              key={d}
              type="button"
              aria-label={`Filtrar o ranking pelo dia ${rotulo} (${num(e.total)})`}
              aria-pressed={ativo}
              title={`${rotulo}: ${num(e.total)} matrículas`}
              onClick={() => onDia(d)}
              className="group flex-1 min-w-[1.2rem] h-full flex flex-col justify-end items-center gap-1"
            >
              <span className="text-[10px] tabular-nums" style={{ color: "var(--text-muted)" }}>{num(e.total)}</span>
              <span
                className="w-full rounded-t-md transition-opacity group-hover:opacity-80"
                style={{
                  height: `${Math.max(3, Math.round((100 * e.total) / max))}%`,
                  background: ativo ? "var(--primary)" : "rgb(var(--primary-rgb) / 0.45)",
                }}
              />
              <span className="text-[10px] tabular-nums whitespace-nowrap" style={{ color: "var(--text-muted)" }}>{rotulo}</span>
            </button>
          );
        })}
      </div>
    </div>
  );
}

/** Matrículas por agente: os 15 primeiros com matrícula, em barras proporcionais. */
export function GraficoAgentes({ ranking }: { ranking: LinhaRanking[] }) {
  const top = ranking.filter((r) => r.matriculas > 0).slice(0, 15);
  if (!top.length) {
    return (
      <p className="text-sm py-8 text-center" style={{ color: "var(--text-muted)" }}>
        Sem matrícula no período.
      </p>
    );
  }
  const max = top.reduce((m, r) => Math.max(m, r.matriculas), 1);
  return (
    <ul className="flex flex-col gap-2">
      {top.map((r) => (
        <li key={r.agente} className="grid grid-cols-[minmax(0,10rem)_1fr_auto] items-center gap-3 text-xs">
          <span className="truncate" title={r.agente}>{r.agente}</span>
          <span className="h-2 rounded-full" style={{ background: "var(--surface-variant)" }}>
            <span
              className="block h-2 rounded-full barra-animada"
              style={{ width: `${Math.round((100 * r.matriculas) / max)}%`, background: "var(--primary)" }}
            />
          </span>
          <span className="tabular-nums font-semibold">{num(r.matriculas)}</span>
        </li>
      ))}
    </ul>
  );
}
