import { useEffect, useState } from "react";
import type { Aviso } from "../fixadas";

/** Duração da saída (igual ao CSS de .toast-sai). */
const SAIDA_MS = 220;

const VISUAL = {
  erro: { titulo: "Não deu certo", icone: "close" },
  sucesso: { titulo: "Pronto", icone: "check" },
} as const;

/**
 * Aviso flutuante no canto da tela: entra deslizando, some sozinho (o tempo
 * vem de quem mostra o aviso) e pode ser fechado no X. A região `role="status"`
 * fica sempre na página para leitores de tela anunciarem o texto.
 */
export function Toast({ aviso, onFechar }: { aviso: Aviso | null; onFechar: () => void }) {
  // Guarda o último aviso enquanto a animação de saída roda.
  const [mostrado, setMostrado] = useState<Aviso | null>(aviso);
  const [saindo, setSaindo] = useState(false);

  useEffect(() => {
    if (aviso) {
      setMostrado(aviso);
      setSaindo(false);
      return;
    }
    if (!mostrado) return;
    setSaindo(true);
    const t = setTimeout(() => {
      setMostrado(null);
      setSaindo(false);
    }, SAIDA_MS);
    return () => clearTimeout(t);
    // `mostrado` fica de fora de propósito: só a chegada/saída do aviso dispara.
  }, [aviso]);

  const visual = mostrado ? VISUAL[mostrado.tipo] : null;

  return (
    <div className="toast-area" aria-live="polite">
      {mostrado && visual && (
        <div className={`toast toast-${mostrado.tipo} ${saindo ? "toast-sai" : "toast-entra"}`}>
          <span className="toast-icone" aria-hidden>
            <span className="material-symbols-outlined">{visual.icone}</span>
          </span>
          <div className="min-w-0 flex-1">
            <p className="toast-titulo">{visual.titulo}</p>
            <p role="status" className="toast-texto">
              {aviso?.texto ?? mostrado.texto}
            </p>
          </div>
          <button type="button" className="toast-fechar" aria-label="Fechar aviso" onClick={onFechar}>
            <span className="material-symbols-outlined" aria-hidden>close</span>
          </button>
        </div>
      )}
      {!mostrado && <p role="status" className="sr-only" />}
    </div>
  );
}
