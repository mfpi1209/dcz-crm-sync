import { iniciais } from "./formato";

/** Mesma paleta dos avatares da tela antiga (_repColors), pela posição no ranking. */
const CORES = [
  "bg-violet-500",
  "bg-blue-500",
  "bg-emerald-500",
  "bg-amber-500",
  "bg-rose-500",
  "bg-cyan-500",
  "bg-indigo-500",
  "bg-pink-500",
  "bg-teal-500",
  "bg-orange-500",
];

export function Avatar({ nome, indice, grande = false }: { nome: string; indice: number; grande?: boolean }) {
  const tam = grande ? "w-12 h-12 text-lg rounded-xl" : "w-11 h-11 text-base rounded-xl";
  return (
    <span
      aria-hidden
      className={`${tam} ${CORES[indice % CORES.length]} text-white font-bold flex items-center justify-center flex-shrink-0 select-none`}
    >
      {iniciais(nome)}
    </span>
  );
}

export function Indicador({ rotulo, valor, cor, contexto }: { rotulo: string; valor: string; cor?: string; contexto?: string }) {
  return (
    <div className="cartao p-5" role="group" aria-label={rotulo}>
      <p className="rotulo mb-1">{rotulo}</p>
      <p className={`text-2xl font-headline font-extrabold tabular-nums ${cor ?? ""}`}>{valor}</p>
      {contexto && (
        <p className="text-xs mt-1" style={{ color: "var(--text-muted)" }}>{contexto}</p>
      )}
    </div>
  );
}

/** Linha única da tabela para carregando, vazio e erro. */
export function LinhaAviso({ colunas, texto, erro = false }: { colunas: number; texto: string; erro?: boolean }) {
  return (
    <tr>
      <td colSpan={colunas} className="py-8 text-center" style={{ color: erro ? "var(--danger)" : "var(--text-muted)" }}>
        {texto}
      </td>
    </tr>
  );
}

export function Paginacao({
  pagina,
  paginas,
  onIr,
  ocupado,
}: {
  pagina: number;
  paginas: number;
  onIr: (p: number) => void;
  ocupado: boolean;
}) {
  return (
    <div className="flex items-center justify-between gap-2 px-5 py-3 text-xs" style={{ color: "var(--text-muted)", borderTop: "1px solid var(--border-subtle)" }}>
      <span>
        Página {pagina} de {paginas}
      </span>
      <div className="flex gap-2">
        <button type="button" className="controle" aria-label="Página anterior" disabled={ocupado || pagina <= 1} onClick={() => onIr(pagina - 1)}>
          <span className="material-symbols-outlined text-[18px]" aria-hidden>chevron_left</span>
        </button>
        <button type="button" className="controle" aria-label="Próxima página" disabled={ocupado || pagina >= paginas} onClick={() => onIr(pagina + 1)}>
          <span className="material-symbols-outlined text-[18px]" aria-hidden>chevron_right</span>
        </button>
      </div>
    </div>
  );
}

/** Mensagem do erro de uma consulta (403 já vem com o texto de sem permissão do api()). */
export function textoErro(e: unknown): string {
  return e instanceof Error && e.message ? e.message : "Falha ao carregar";
}
