import { type ReactNode, useId } from "react";
import { ErroApi, SEM_PERMISSAO } from "../../api";

export function textoErro(e: unknown): string {
  if (e instanceof ErroApi && e.status === 403) return SEM_PERMISSAO;
  return `Não foi possível carregar: ${(e as Error)?.message ?? e}`;
}

/** Cartão com título (vira uma região com nome para leitores de tela). */
export function Cartao({
  titulo,
  sufixo,
  icone,
  acoes,
  children,
  semPadding,
}: {
  titulo: string;
  sufixo?: string;
  icone: string;
  acoes?: ReactNode;
  children: ReactNode;
  semPadding?: boolean;
}) {
  const id = useId();
  return (
    <section className={`cartao ${semPadding ? "overflow-hidden" : "p-5"}`} aria-labelledby={id}>
      <div className={`flex flex-wrap items-center justify-between gap-3 ${semPadding ? "px-5 pt-5 pb-3" : "mb-4"}`}>
        <h2 className="text-sm flex items-center gap-2">
          <span className="material-symbols-outlined text-[18px]" style={{ color: "var(--text-muted)" }} aria-hidden>
            {icone}
          </span>
          <span id={id}>{titulo}</span>
          {sufixo && (
            <span className="font-normal text-xs" style={{ color: "var(--text-muted)" }}>
              {sufixo}
            </span>
          )}
        </h2>
        {acoes}
      </div>
      {children}
    </section>
  );
}

export function MensagemCartao({ children }: { children: ReactNode }) {
  return (
    <p className="absolute inset-0 flex items-center justify-center text-sm text-center px-4" style={{ color: "var(--text-muted)" }}>
      {children}
    </p>
  );
}

export function Indicador({
  rotulo,
  valor,
  cor,
  rodape,
}: {
  rotulo: string;
  valor: string;
  cor?: string;
  rodape?: ReactNode;
}) {
  return (
    <div className="cartao p-5" role="group" aria-label={rotulo}>
      <p className="rotulo mb-1">{rotulo}</p>
      <p className={`text-3xl font-headline font-extrabold tabular-nums ${cor ?? ""}`}>{valor}</p>
      <div className="text-xs mt-1 min-h-[1.1rem]" style={{ color: "var(--text-muted)" }}>
        {rodape}
      </div>
    </div>
  );
}
