import { useEffect, useRef, type ReactNode } from "react";

/** Janela sobre a tela: fecha no Esc, no fundo e em Cancelar; o foco entra no primeiro campo. */
export function Modal({ titulo, onFechar, children }: { titulo: string; onFechar: () => void; children: ReactNode }) {
  const ref = useRef<HTMLDivElement>(null);
  const fechar = useRef(onFechar);
  fechar.current = onFechar;
  useEffect(() => {
    const anterior = document.activeElement as HTMLElement | null;
    ref.current?.querySelector<HTMLElement>("input, select, textarea, button")?.focus();
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
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
      onMouseDown={(e) => {
        if (e.target === e.currentTarget) onFechar();
      }}
    >
      <div ref={ref} role="dialog" aria-modal="true" aria-label={titulo} className="cartao w-full max-w-2xl max-h-[90vh] overflow-y-auto p-6 flex flex-col gap-4" style={{ background: "var(--bg-card)" }}>
        <h3 className="text-base font-bold">{titulo}</h3>
        {children}
      </div>
    </div>
  );
}
