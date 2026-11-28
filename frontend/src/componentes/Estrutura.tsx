import { useEffect, useMemo, useRef, useState } from "react";
import { Outlet, useLocation, useMatch } from "react-router-dom";
import { ErroApi } from "../api";
import { useFixadas } from "../fixadas";
import { useMe } from "../me";
import { grupoDaTela, montarMenu } from "../menu";
import { NOMES_TELAS_NOVAS } from "../telas";
import { TITULOS } from "../titulos";
import { BarraTopo } from "./BarraTopo";
import { BuscaTelas, type ItemBusca } from "./BuscaTelas";
import { MenuLateral } from "./MenuLateral";

/**
 * Casca do front novo: menu lateral (gaveta no celular), barra de cima com o
 * título da rota e, embaixo, a tela.
 */
export function Estrutura() {
  const me = useMe();
  // useFixadas já descarta resposta fora do formato (dados = null).
  const { dados: fixadas } = useFixadas();
  const local = useLocation();
  const pagina = useMatch("/:pagina")?.params.pagina ?? null;
  const [gavetaAberta, setGavetaAberta] = useState(false);
  const [buscaAberta, setBuscaAberta] = useState(false);
  const botaoMenu = useRef<HTMLButtonElement>(null);

  const menu = useMemo(
    () => (me.data ? montarMenu(me.data.nav, NOMES_TELAS_NOVAS, fixadas) : null),
    [me.data, fixadas],
  );

  // Telas da busca: avulsas + grupos (as fixadas só repetem o que já está aí).
  const itensBusca = useMemo<ItemBusca[]>(
    () =>
      menu ? [...menu.avulsas, ...menu.grupos.flatMap((g) => g.itens.map((i) => ({ ...i, grupo: g.titulo })))] : [],
    [menu],
  );

  // Ctrl/Cmd+K abre (ou fecha) a busca de telas.
  useEffect(() => {
    function atalho(e: KeyboardEvent) {
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        setBuscaAberta((v) => !v);
      }
    }
    document.addEventListener("keydown", atalho);
    return () => document.removeEventListener("keydown", atalho);
  }, []);

  // Trocou de tela: a gaveta fecha.
  useEffect(() => setGavetaAberta(false), [local.pathname]);

  // Esc fecha a gaveta e devolve o foco ao botão Menu.
  useEffect(() => {
    if (!gavetaAberta) return;
    function tecla(e: KeyboardEvent) {
      // Busca aberta trata o Esc antes (e marca defaultPrevented): fecha só ela.
      if (e.key === "Escape" && !e.defaultPrevented) {
        setGavetaAberta(false);
        botaoMenu.current?.focus();
      }
    }
    document.addEventListener("keydown", tecla);
    return () => document.removeEventListener("keydown", tecla);
  }, [gavetaAberta]);

  if (me.isPending) {
    return <p className="p-8 text-sm" style={{ color: "var(--text-muted)" }}>Carregando…</p>;
  }
  if (me.isError || !menu) {
    const msg = me.error instanceof ErroApi ? me.error.message : "Não foi possível carregar o painel.";
    return (
      <div className="p-8">
        <p role="alert">{msg}</p>
        <a className="underline" href="/">Voltar ao painel</a>
      </div>
    );
  }

  // Título vem da rota: início, tela do menu ou endereço desconhecido.
  const inicio = local.pathname === "/";
  const conhecida = pagina !== null && Object.prototype.hasOwnProperty.call(TITULOS, pagina) ? pagina : null;
  const titulo = inicio ? "Início" : conhecida ? TITULOS[conhecida] : "Painel eduit";
  const grupo = conhecida ? grupoDaTela(menu, conhecida)?.titulo : undefined;
  // Estrela só em tela migrada que está no menu (mesma regra `visivel` do servidor).
  const comEstrela =
    !!conhecida && NOMES_TELAS_NOVAS.has(conhecida) && itensBusca.some((i) => i.pagina === conhecida);

  return (
    <div className="min-h-screen md:flex">
      {gavetaAberta && (
        <div className="md:hidden fixed inset-0 z-30 fundo-gaveta" aria-hidden onClick={() => setGavetaAberta(false)} />
      )}

      <aside
        id="menu-lateral"
        aria-label="Menu"
        className={`casca-menu ${gavetaAberta ? "flex" : "hidden"} md:flex flex-col fixed md:sticky top-0 left-0 z-40 h-dvh w-[264px] max-w-[85vw] shrink-0 border-r`}
        style={{ borderColor: "var(--border-subtle)" }}
      >
        <MenuLateral
          menu={menu}
          paginaAtual={pagina}
          nome={me.data.display_name || me.data.username}
          onBuscar={() => setBuscaAberta(true)}
          onEscolher={() => setGavetaAberta(false)}
        />
      </aside>

      <div className="flex-1 min-w-0 flex flex-col">
        <BarraTopo
          titulo={titulo}
          grupo={grupo}
          pagina={comEstrela && conhecida ? conhecida : undefined}
          menuAberto={gavetaAberta}
          onMenu={() => setGavetaAberta((v) => !v)}
          refMenu={botaoMenu}
        />
        <main className="flex-1 min-w-0 px-4 py-5 md:px-8 md:py-6">
          <Outlet />
        </main>
      </div>

      <BuscaTelas aberta={buscaAberta} itens={itensBusca} onFechar={() => setBuscaAberta(false)} />
    </div>
  );
}
