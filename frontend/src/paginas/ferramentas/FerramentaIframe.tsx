import { useEffect, useState } from "react";

interface Props {
  /** Nome da ferramenta: vira o `title` do iframe (leitores de tela). */
  titulo: string;
  /** Endereço da ferramenta; vazio = não configurada. */
  url: string | null | undefined;
}

/**
 * Ferramenta externa em iframe, ocupando toda a área útil (a casca tira o
 * padding e dá a altura: viewport menos a barra de cima). Mostra uma
 * sobreposição discreta até o `load` do iframe.
 */
export function FerramentaIframe({ titulo, url }: Props) {
  const [carregado, setCarregado] = useState(false);
  // Trocou o endereço: volta a carregar.
  useEffect(() => setCarregado(false), [url]);

  if (!url) {
    return (
      <div className="h-full flex items-center justify-center p-6">
        <div className="cartao max-w-md text-center">
          <p className="font-semibold">{titulo}</p>
          <p className="mt-2 text-sm" style={{ color: "var(--text-secondary)" }}>
            Esta ferramenta ainda não está configurada.
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="relative h-full w-full overflow-hidden" style={{ background: "var(--bg-main)" }}>
      <iframe
        title={titulo}
        src={url}
        onLoad={() => setCarregado(true)}
        className="block w-full h-full border-0"
        style={{ background: "var(--bg-main)" }}
      />
      {!carregado && (
        <div
          role="status"
          className="absolute inset-0 flex items-center justify-center text-sm pointer-events-none"
          style={{ background: "var(--bg-main)", color: "var(--text-muted)" }}
        >
          Carregando {titulo}…
        </div>
      )}
    </div>
  );
}
