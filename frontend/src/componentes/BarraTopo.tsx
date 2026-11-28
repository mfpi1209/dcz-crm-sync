import { useState } from "react";
import { useAvisosNaoLidos } from "../avisos";
import { MAX_FIXADAS, useFixadas } from "../fixadas";

type Tema = "dark" | "light";

function temaAtual(): Tema {
  return document.documentElement.classList.contains("dark") ? "dark" : "light";
}

/** Mesma chave do painel atual (eduit-theme), para o tema valer nos dois. */
function aplicarTema(novo: Tema) {
  const html = document.documentElement;
  html.classList.remove("dark", "light");
  html.classList.add(novo);
  try {
    localStorage.setItem("eduit-theme", novo);
  } catch {
    /* navegador sem armazenamento: vale só nesta aba */
  }
}

/** Estrela da tela aberta. Só aparece quando as fixadas carregaram. */
function Estrela({ pagina }: { pagina: string }) {
  const { dados, fixada, alternar } = useFixadas();
  if (!dados) return null;
  const ativa = fixada(pagina);
  const cheio = !ativa && dados.fixadas.length >= MAX_FIXADAS;
  return (
    <button
      type="button"
      className="botao-icone"
      aria-pressed={ativa}
      aria-label={ativa ? "Desafixar esta tela" : "Fixar esta tela"}
      title={cheio ? `Máximo de ${MAX_FIXADAS} fixadas` : ativa ? "Tirar das fixadas" : "Fixar no menu"}
      disabled={cheio}
      onClick={() => alternar(pagina)}
    >
      <span className={`material-symbols-outlined ${ativa ? "icone-cheio text-primary" : ""}`} aria-hidden>
        star
      </span>
    </button>
  );
}

function Sino() {
  const n = useAvisosNaoLidos();
  return (
    <a
      href="/#avisos"
      className="botao-icone"
      aria-label={n ? `Avisos, ${n} não lidos` : "Avisos"}
      title="Avisos (abre no painel atual)"
    >
      <span className="material-symbols-outlined" aria-hidden>notifications</span>
      {n > 0 && (
        <span data-bolinha className="bolinha" aria-hidden>
          {n > 99 ? "99+" : n}
        </span>
      )}
    </a>
  );
}

function BotaoTema() {
  const [tema, setTema] = useState<Tema>(temaAtual);
  const proximo: Tema = tema === "dark" ? "light" : "dark";
  return (
    <button
      type="button"
      className="botao-icone"
      aria-label={proximo === "light" ? "Mudar para tema claro" : "Mudar para tema escuro"}
      title={proximo === "light" ? "Tema claro" : "Tema escuro"}
      onClick={() => {
        aplicarTema(proximo);
        setTema(proximo);
      }}
    >
      <span className="material-symbols-outlined" aria-hidden>
        {tema === "dark" ? "light_mode" : "dark_mode"}
      </span>
    </button>
  );
}

/**
 * Barra de cima de cada tela: grupo (pequeno) e título, e à direita a estrela
 * (só em tela migrada: `pagina`), o sino de avisos e o tema.
 */
export function BarraTopo({ titulo, grupo, pagina }: { titulo: string; grupo?: string; pagina?: string }) {
  return (
    <header
      aria-label="Barra da tela"
      className="flex items-center justify-between gap-4 px-4 md:px-8 py-4 border-b"
      style={{ borderColor: "var(--border-subtle)" }}
    >
      <div className="min-w-0">
        {grupo && (
          <p className="text-xs" style={{ color: "var(--text-muted)" }}>
            {grupo}
          </p>
        )}
        <h1 className="text-lg md:text-xl truncate">{titulo}</h1>
      </div>
      <div className="flex items-center gap-1 shrink-0">
        {pagina && <Estrela pagina={pagina} />}
        <Sino />
        <BotaoTema />
      </div>
    </header>
  );
}
