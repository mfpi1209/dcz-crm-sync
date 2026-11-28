import { useEffect, useId, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { navegador } from "../api";
import { filtrarTelas } from "../busca";
import type { ItemMenu } from "../menu";

export interface ItemBusca extends ItemMenu {
  grupo?: string;
}

/**
 * Busca de telas (Ctrl K): painel no centro com campo e lista. Foco preso no
 * diálogo enquanto aberto; ao fechar, volta para quem tinha o foco antes.
 */
export function BuscaTelas({
  aberta,
  itens,
  onFechar,
}: {
  aberta: boolean;
  itens: ItemBusca[];
  onFechar: () => void;
}) {
  if (!aberta) return null;
  return <Painel itens={itens} onFechar={onFechar} />;
}

function Painel({ itens, onFechar }: { itens: ItemBusca[]; onFechar: () => void }) {
  const navigate = useNavigate();
  const id = useId();
  const [texto, setTexto] = useState("");
  const [ativo, setAtivo] = useState(0);
  const campo = useRef<HTMLInputElement>(null);
  const lista = useRef<HTMLUListElement>(null);

  // Sem repetir tela (ela pode estar em mais de um lugar do menu).
  const unicos = useMemo(() => {
    const vistos = new Set<string>();
    return itens.filter((i) => {
      if (vistos.has(i.pagina)) return false;
      vistos.add(i.pagina);
      return true;
    });
  }, [itens]);
  const resultados = useMemo(() => filtrarTelas(unicos, texto), [unicos, texto]);
  const indice = resultados.length ? Math.min(ativo, resultados.length - 1) : -1;

  // Guarda o foco de antes e devolve ao fechar.
  useEffect(() => {
    const anterior = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    campo.current?.focus();
    return () => anterior?.focus();
  }, []);

  // Mantém a linha destacada à vista na lista rolável.
  useEffect(() => {
    lista.current?.children[indice]?.scrollIntoView?.({ block: "nearest" });
  }, [indice]);

  function abrir(item: ItemBusca) {
    onFechar();
    if (item.nova) navigate("/" + item.pagina);
    else navegador.irPara(item.href);
  }

  function tecla(e: globalThis.KeyboardEvent) {
    const n = resultados.length;
    if (e.key === "Escape") {
      e.preventDefault();
      onFechar();
    } else if (e.key === "ArrowDown") {
      e.preventDefault();
      if (n) setAtivo((indice + 1) % n);
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      if (n) setAtivo((indice - 1 + n) % n);
    } else if (e.key === "Enter") {
      e.preventDefault();
      if (indice >= 0) abrir(resultados[indice]);
    } else if (e.key === "Tab") {
      // O único elemento focável do diálogo é o campo: o foco fica nele.
      e.preventDefault();
      campo.current?.focus();
    }
  }

  // Teclado no documento: se o foco sair do campo (clique no painel), Esc e Tab continuam valendo.
  // Na captura, para tratar o Esc antes da gaveta do menu (que respeita defaultPrevented).
  useEffect(() => {
    document.addEventListener("keydown", tecla, true);
    return () => document.removeEventListener("keydown", tecla, true);
  });

  const idOpcao = (i: number) => `${id}-op-${i}`;

  return (
    <div
      className="busca-fundo fundo-gaveta"
      data-testid="busca-fundo"
      onClick={(e) => e.target === e.currentTarget && onFechar()}
    >
      <div role="dialog" aria-modal="true" aria-label="Buscar tela" tabIndex={-1} className="busca-painel">
        <div className="busca-campo">
          <span className="material-symbols-outlined" aria-hidden>
            search
          </span>
          <input
            ref={campo}
            type="text"
            role="combobox"
            aria-label="Buscar tela"
            aria-expanded="true"
            aria-controls={`${id}-lista`}
            aria-autocomplete="list"
            aria-activedescendant={indice >= 0 ? idOpcao(indice) : undefined}
            placeholder="Buscar tela…"
            autoComplete="off"
            value={texto}
            onChange={(e) => {
              setTexto(e.target.value);
              setAtivo(0);
            }}
          />
        </div>
        <ul ref={lista} id={`${id}-lista`} role="listbox" aria-label="Telas" className="busca-lista">
          {resultados.map((item, i) => (
            <li
              key={item.pagina}
              id={idOpcao(i)}
              role="option"
              aria-selected={i === indice}
              className="busca-item"
              onMouseMove={() => i !== indice && setAtivo(i)}
              onClick={() => abrir(item)}
            >
              <span className="flex-1 truncate">{item.titulo}</span>
              {item.grupo && <span className="busca-grupo">{item.grupo}</span>}
            </li>
          ))}
        </ul>
        {resultados.length === 0 && <p className="busca-vazio">Nenhuma tela encontrada</p>}
        <p className="busca-dicas">↑↓ navegar · Enter abrir · Esc fechar</p>
      </div>
    </div>
  );
}
