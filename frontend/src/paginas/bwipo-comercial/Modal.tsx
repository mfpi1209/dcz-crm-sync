import { useEffect, useRef, type ReactNode } from "react";

/** Janela sobre a tela (o `bwc-modal` da antiga): fecha no X, no Esc ou clicando fora. */
export function Modal({
  titulo,
  sub,
  acoes,
  onFechar,
  children,
}: {
  titulo: string;
  sub?: ReactNode;
  acoes?: ReactNode;
  onFechar: () => void;
  children: ReactNode;
}) {
  const painel = useRef<HTMLDivElement>(null);
  // Guarda a função mais nova sem refazer o efeito (o foco não pode pular a cada render).
  const fechar = useRef(onFechar);
  fechar.current = onFechar;
  useEffect(() => {
    const anterior = document.activeElement as HTMLElement | null;
    // Foco no painel só se nada lá dentro (ex.: campo com autoFocus) já pegou o foco.
    if (!painel.current?.contains(document.activeElement)) painel.current?.focus();
    const tecla = (e: KeyboardEvent) => {
      if (e.key === "Escape") fechar.current();
    };
    document.addEventListener("keydown", tecla);
    return () => {
      document.removeEventListener("keydown", tecla);
      anterior?.focus?.();
    };
  }, []);

  return (
    <div
      className="fixed inset-0 z-[80] flex items-start justify-center p-4 overflow-y-auto fundo-gaveta"
      onMouseDown={(e) => {
        if (e.target === e.currentTarget) onFechar();
      }}
    >
      <div
        ref={painel}
        role="dialog"
        aria-modal="true"
        aria-label={titulo}
        tabIndex={-1}
        className="cartao w-full max-w-5xl my-8 flex flex-col overflow-hidden outline-none"
        style={{ background: "var(--bg-card)" }}
      >
        <div className="px-5 py-3 flex items-start justify-between gap-3" style={{ borderBottom: "1px solid var(--border)" }}>
          <div className="min-w-0">
            <h3 className="text-sm">{titulo}</h3>
            {sub && (
              <div className="text-xs mt-0.5" style={{ color: "var(--text-muted)" }}>
                {sub}
              </div>
            )}
          </div>
          <div className="flex items-center gap-2 shrink-0">
            {acoes}
            <button type="button" className="botao-icone" aria-label="Fechar" onClick={onFechar}>
              <span className="material-symbols-outlined" aria-hidden>close</span>
            </button>
          </div>
        </div>
        <div className="max-h-[70vh] overflow-y-auto text-xs">{children}</div>
      </div>
    </div>
  );
}

/** Linha chave → valor dos diagnósticos. */
export function ChaveValor({ chave, valor }: { chave: ReactNode; valor: ReactNode }) {
  return (
    <div className="flex justify-between gap-3 px-4 py-1" style={{ borderBottom: "1px solid var(--border-subtle)" }}>
      <span>{chave}</span>
      <b className="tabular-nums">{valor}</b>
    </div>
  );
}

export function TituloSecao({ children }: { children: ReactNode }) {
  return <p className="px-4 pt-3 pb-1 font-semibold">{children}</p>;
}

export function Vazio({ children, erro = false }: { children: ReactNode; erro?: boolean }) {
  return (
    <p className="px-4 py-3" style={{ color: erro ? "var(--danger)" : "var(--text-muted)" }}>
      {children}
    </p>
  );
}
