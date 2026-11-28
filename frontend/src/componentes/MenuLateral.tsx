import { useEffect, useState } from "react";
import { NavLink } from "react-router-dom";
import { MAX_FIXADAS, useFixadas } from "../fixadas";
import type { GrupoMenu, ItemMenu, Menu } from "../menu";
import { grupoDaTela } from "../menu";
import { iconeDaTela } from "../icones";

/** Grupos abertos ficam no navegador (nomes dos grupos, em JSON). */
export const CHAVE_GRUPOS = "eduit-menu-grupos";

function lerGrupos(): Set<string> {
  try {
    const salvo: unknown = JSON.parse(localStorage.getItem(CHAVE_GRUPOS) ?? "[]");
    return new Set(Array.isArray(salvo) ? salvo.filter((n): n is string => typeof n === "string") : []);
  } catch {
    return new Set();
  }
}

function gravarGrupos(abertos: Set<string>) {
  try {
    localStorage.setItem(CHAVE_GRUPOS, JSON.stringify([...abertos]));
  } catch {
    /* navegador sem armazenamento: vale só nesta aba */
  }
}

/** "Ana Souza" → "AS"; "ana.souza@eduit.com.br" → "AS"; "adm" → "AD". */
export function iniciais(nome: string): string {
  const partes = nome.split("@")[0].split(/[\s._-]+/).filter(Boolean);
  if (!partes.length) return "?";
  const primeira = partes[0][0];
  const segunda = partes.length > 1 ? partes[partes.length - 1][0] : (partes[0][1] ?? "");
  return (primeira + segunda).toUpperCase();
}

type Fixadas = ReturnType<typeof useFixadas>;

/** Estrela do item: aparece ao passar o mouse ou com foco; sempre visível nas fixadas. */
function EstrelaItem({ i, fixadas }: { i: ItemMenu; fixadas: Fixadas }) {
  if (!fixadas.dados) return null;
  const ativa = fixadas.fixada(i.pagina);
  const cheio = !ativa && fixadas.dados.fixadas.length >= MAX_FIXADAS;
  return (
    <button
      type="button"
      className="estrela-item"
      aria-pressed={ativa}
      aria-label={ativa ? `Desafixar ${i.titulo}` : `Fixar ${i.titulo}`}
      title={cheio ? `Máximo de ${MAX_FIXADAS} fixadas` : ativa ? "Tirar das fixadas" : "Fixar no menu"}
      disabled={cheio}
      onClick={() => fixadas.alternar(i.pagina)}
    >
      <span className={`material-symbols-outlined ${ativa ? "icone-cheio" : ""}`} aria-hidden>
        star
      </span>
    </button>
  );
}

function Item({
  i,
  fixadas,
  onEscolher,
  comIcone = false,
}: {
  i: ItemMenu;
  fixadas: Fixadas;
  onEscolher: () => void;
  comIcone?: boolean;
}) {
  // Itens soltos (Fixadas, Geral) levam ícone; subitens de grupo, a linha-guia.
  const conteudo = (
    <>
      {comIcone && (
        <span className="material-symbols-outlined icone-item" aria-hidden>
          {iconeDaTela(i.pagina)}
        </span>
      )}
      <span className="truncate">{i.titulo}</span>
    </>
  );
  return (
    <li className="linha-menu">
      {i.nova ? (
        <NavLink to={`/${i.pagina}`} className="item-menu" onClick={onEscolher}>
          {conteudo}
        </NavLink>
      ) : (
        // Tela ainda no painel atual: navegação normal do navegador.
        <a href={i.href} className="item-menu" onClick={onEscolher}>
          {conteudo}
        </a>
      )}
      <EstrelaItem i={i} fixadas={fixadas} />
    </li>
  );
}

function Secao({
  id,
  titulo,
  itens,
  fixadas,
  onEscolher,
}: {
  id: string;
  titulo: string;
  itens: ItemMenu[];
  fixadas: Fixadas;
  onEscolher: () => void;
}) {
  return (
    <section>
      <h2 id={id} className="secao-menu">
        {titulo}
      </h2>
      <ul aria-labelledby={id} className="flex flex-col gap-px">
        {itens.map((i) => (
          <Item key={i.pagina} i={i} fixadas={fixadas} onEscolher={onEscolher} comIcone />
        ))}
      </ul>
    </section>
  );
}

function Grupo({
  g,
  aberto,
  alternar,
  fixadas,
  onEscolher,
}: {
  g: GrupoMenu;
  aberto: boolean;
  alternar: () => void;
  fixadas: Fixadas;
  onEscolher: () => void;
}) {
  const idItens = `grupo-${g.nome}-itens`;
  return (
    <div>
      <button type="button" className="grupo-menu" aria-expanded={aberto} aria-controls={idItens} onClick={alternar}>
        <span className="material-symbols-outlined icone-item" aria-hidden>
          {g.icone}
        </span>
        <span className="flex-1 text-left truncate">{g.titulo}</span>
        <span className="material-symbols-outlined seta-grupo" aria-hidden>
          chevron_right
        </span>
      </button>
      <ul id={idItens} aria-label={g.titulo} hidden={!aberto} className="grupo-itens">
        {g.itens.map((i) => (
          <Item key={i.pagina} i={i} fixadas={fixadas} onEscolher={onEscolher} />
        ))}
      </ul>
    </div>
  );
}

/**
 * Conteúdo do menu lateral: marca, botão de busca, Fixadas (ou Mais usadas),
 * Geral, grupos que abrem e fecham e o rodapé com o usuário.
 */
export function MenuLateral({
  menu,
  paginaAtual,
  nome,
  onBuscar,
  onEscolher,
}: {
  menu: Menu;
  paginaAtual: string | null;
  nome: string;
  onBuscar: () => void;
  onEscolher: () => void;
}) {
  const fixadas = useFixadas();
  const grupoAtual = paginaAtual ? (grupoDaTela(menu, paginaAtual)?.nome ?? null) : null;
  const [abertos, setAbertos] = useState<Set<string>>(() => {
    const salvos = lerGrupos();
    if (grupoAtual) salvos.add(grupoAtual);
    return salvos;
  });

  // Mudou de tela: o grupo dela abre (sem fechar os outros).
  useEffect(() => {
    if (grupoAtual) setAbertos((atual) => (atual.has(grupoAtual) ? atual : new Set(atual).add(grupoAtual)));
  }, [grupoAtual]);

  function alternarGrupo(nomeGrupo: string) {
    const novo = new Set(abertos);
    if (novo.has(nomeGrupo)) novo.delete(nomeGrupo);
    else novo.add(nomeGrupo);
    setAbertos(novo);
    gravarGrupos(novo);
  }

  return (
    <div className="flex flex-col h-full">
      <div className="px-4 pt-5 pb-4">
        <a href="/" className="marca" title="Painel atual">
          <span className="marca-selo" aria-hidden>
            e
          </span>
          <span className="flex flex-col leading-none">
            <span className="marca-nome">eduit.</span>
            <span className="marca-sub">Painel de gestão</span>
          </span>
        </a>
      </div>
      <div className="px-3 pb-3">
        <button type="button" className="busca-botao" onClick={onBuscar} aria-keyshortcuts="Control+K">
          <span className="material-symbols-outlined" aria-hidden>
            search
          </span>
          <span className="flex-1 text-left">Buscar tela</span>
          <kbd className="atalho">Ctrl K</kbd>
        </button>
      </div>

      <nav aria-label="Menu principal" className="rolagem-menu flex-1 overflow-y-auto px-3 pt-1 pb-4 flex flex-col gap-5">
        {menu.fixadas && (
          <Secao
            id="secao-fixadas"
            titulo={menu.fixadas.titulo}
            itens={menu.fixadas.itens}
            fixadas={fixadas}
            onEscolher={onEscolher}
          />
        )}
        {menu.avulsas.length > 0 && (
          <Secao id="secao-geral" titulo="Geral" itens={menu.avulsas} fixadas={fixadas} onEscolher={onEscolher} />
        )}
        {menu.grupos.length > 0 && (
          <div className="flex flex-col gap-px">
            {menu.grupos.map((g) => (
              <Grupo
                key={g.nome}
                g={g}
                aberto={abertos.has(g.nome)}
                alternar={() => alternarGrupo(g.nome)}
                fixadas={fixadas}
                onEscolher={onEscolher}
              />
            ))}
          </div>
        )}
      </nav>

      <div role="group" aria-label="Usuário" className="rodape-menu">
        <span className="avatar" aria-hidden>
          {iniciais(nome)}
        </span>
        <span className="flex-1 min-w-0 flex flex-col leading-tight">
          <span className="truncate text-sm font-semibold" style={{ color: "var(--text-primary)" }}>
            {nome}
          </span>
          <span className="text-xs" style={{ color: "var(--text-muted)" }}>
            Conectado
          </span>
        </span>
        <a href="/logout" className="botao-icone" aria-label="Sair" title="Sair">
          <span className="material-symbols-outlined" aria-hidden>
            logout
          </span>
        </a>
      </div>
    </div>
  );
}
