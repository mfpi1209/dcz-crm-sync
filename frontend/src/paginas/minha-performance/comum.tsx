import type { ReactNode } from "react";

/** Bloco da tela com título em sentença normal (o título vira o nome da região). */
export function Secao({
  titulo,
  icone,
  extra,
  children,
  className = "",
}: {
  titulo: string;
  icone: string;
  extra?: ReactNode;
  children: ReactNode;
  className?: string;
}) {
  return (
    <section aria-label={titulo} className={`flex flex-col gap-3 ${className}`}>
      <h3 className="text-sm font-semibold flex items-center gap-2 flex-wrap" style={{ color: "var(--text-secondary)" }}>
        <span className="material-symbols-outlined text-[18px]" aria-hidden>
          {icone}
        </span>
        {titulo}
        {extra}
      </h3>
      {children}
    </section>
  );
}

/** Barra de proporção (0–100%). */
export function Barra({ pct, cor, altura = "h-2.5" }: { pct: number; cor: string; altura?: string }) {
  return (
    <div className={`${altura} rounded-full overflow-hidden`} style={{ background: "var(--surface-variant)" }}>
      <div className={`h-full rounded-full barra-animada ${cor}`} style={{ width: `${Math.max(0, Math.min(100, pct))}%` }} />
    </div>
  );
}

export function Selo({ children, cor }: { children: ReactNode; cor: string }) {
  return <span className={`px-2 py-0.5 rounded-full text-[11px] font-semibold border ${cor}`}>{children}</span>;
}

export const TEXTO_FRACO = { color: "var(--text-muted)" } as const;

/** Cores por nível (mesma ideia da antiga: bronze, prata/azul, ouro). */
export const COR_TIER: Record<string, { barra: string; texto: string; selo: string }> = {
  base: { barra: "bg-emerald-500", texto: "text-emerald-700 dark:text-emerald-400", selo: "bg-emerald-500/15 text-emerald-700 dark:text-emerald-300 border-emerald-500/30" },
  intermediaria: { barra: "bg-orange-500", texto: "text-orange-700 dark:text-orange-400", selo: "bg-orange-500/15 text-orange-700 dark:text-orange-300 border-orange-500/30" },
  meta: { barra: "bg-blue-500", texto: "text-blue-700 dark:text-blue-400", selo: "bg-blue-500/15 text-blue-700 dark:text-blue-300 border-blue-500/30" },
  supermeta: { barra: "bg-amber-500", texto: "text-amber-700 dark:text-amber-400", selo: "bg-amber-500/15 text-amber-700 dark:text-amber-300 border-amber-500/30" },
};
