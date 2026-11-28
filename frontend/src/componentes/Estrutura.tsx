import { useEffect, useMemo, useRef, useState } from "react";
import { Outlet, useLocation, useMatch } from "react-router-dom";
import { ErroApi } from "../api";
import { type DadosFixadas, useFixadas } from "../fixadas";
import { useMe } from "../me";
import { grupoDaTela, montarMenu } from "../menu";
import { NOMES_TELAS_NOVAS } from "../telas";
import { TITULOS } from "../titulos";
import { BarraTopo } from "./BarraTopo";
import { MenuLateral } from "./MenuLateral";

/** Só usa a resposta das fixadas se tiver o formato esperado; senão o menu segue sem a seção. */
function fixadasValidas(dados: DadosFixadas | null): DadosFixadas | null {
  return dados && Array.isArray(dados.fixadas) && Array.isArray(dados.mais_usadas) ? dados : null;
}

/**
 * Casca do front novo: menu lateral (gaveta no celular), barra de cima com o
 * título da rota e, embaixo, a tela.
 */
export function Estrutura() {
  const me = useMe();
  const { dados } = useFixadas();
  const fixadas = fixadasValidas(dados);
  const local = useLocation();
  const pagina = useMatch("/:pagina")?.params.pagina ?? null;
  const [gavetaAberta, setGavetaAberta] = useState(false);
  // Task 4 encaixa a busca de telas (BuscaTelas) neste estado.
  const [buscaAberta, setBuscaAberta] = useState(false);
  const botaoMenu = useRef<HTMLButtonElement>(null);

  const menu = useMemo(
    () => (me.data ? montarMenu(me.data.nav, NOMES_TELAS_NOVAS, fixadas) : null),
    [me.data, fixadas],
  );

  // Trocou de tela: a gaveta fecha.
  useEffect(() => setGavetaAberta(false), [local.pathname]);

  // Esc fecha a gaveta e devolve o foco ao botão Menu.
  useEffect(() => {
    if (!gavetaAberta) return;
    function tecla(e: KeyboardEvent) {
      if (e.key === "Escape") {
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
  // Estrela só em tela migrada que a pessoa vê.
  const comEstrela =
    !!conhecida && NOMES_TELAS_NOVAS.has(conhecida) && me.data.nav.paginas.includes(conhecida);

  return (
    <div className="min-h-screen md:flex">
      <header
        aria-label="Topo do celular"
        className="md:hidden flex items-center justify-between px-4 py-3 border-b casca-menu"
        style={{ borderColor: "var(--border-subtle)" }}
      >
        <a href="/" className="font-headline text-lg font-extrabold text-primary">eduit.</a>
        <button
          ref={botaoMenu}
          type="button"
          className="botao-texto"
          aria-expanded={gavetaAberta}
          aria-controls="menu-lateral"
          onClick={() => setGavetaAberta((v) => !v)}
        >
          <span className="material-symbols-outlined" aria-hidden>{gavetaAberta ? "close" : "menu"}</span>
          Menu
        </button>
      </header>

      {gavetaAberta && (
        <div className="md:hidden fixed inset-0 z-30 fundo-gaveta" aria-hidden onClick={() => setGavetaAberta(false)} />
      )}

      <aside
        id="menu-lateral"
        aria-label="Menu"
        className={`casca-menu ${gavetaAberta ? "flex" : "hidden"} md:flex flex-col fixed md:sticky top-0 left-0 z-40 h-screen w-[248px] max-w-[85vw] shrink-0 border-r`}
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
        <BarraTopo titulo={titulo} grupo={grupo} pagina={comEstrela && conhecida ? conhecida : undefined} />
        <main className="flex-1 min-w-0 p-4 md:p-8">
          <Outlet />
        </main>
      </div>

      {/* Task 4: busca de telas (Ctrl K) entra aqui, ex.: <BuscaTelas aberta={buscaAberta} onFechar=... /> */}
      {buscaAberta && null}
    </div>
  );
}
