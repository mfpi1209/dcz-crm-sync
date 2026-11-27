import { useMemo, useState } from "react";
import { Outlet } from "react-router-dom";
import { ErroApi } from "../api";
import { useMe } from "../me";
import { montarMenu } from "../menu";
import { NOMES_TELAS_NOVAS } from "../telas";
import { MenuLateral } from "./MenuLateral";

function alternarTema() {
  const html = document.documentElement;
  const novo = html.classList.contains("dark") ? "light" : "dark";
  html.classList.remove("dark", "light");
  html.classList.add(novo);
  try {
    localStorage.setItem("eduit-theme", novo);
  } catch {
    /* navegador sem armazenamento: vale só nesta aba */
  }
}

export function Estrutura() {
  const me = useMe();
  const [menuAberto, setMenuAberto] = useState(false);
  const menu = useMemo(() => (me.data ? montarMenu(me.data.nav, NOMES_TELAS_NOVAS) : null), [me.data]);

  if (me.isPending) {
    return <p className="p-8 text-sm" style={{ color: "var(--text-muted)" }}>Carregando…</p>;
  }
  if (me.isError) {
    const msg = me.error instanceof ErroApi ? me.error.message : "Não foi possível carregar o painel.";
    return (
      <div className="p-8">
        <p role="alert">{msg}</p>
        <a className="underline" href="/">Voltar ao painel</a>
      </div>
    );
  }

  return (
    <div className="min-h-screen md:flex">
      <header
        className="md:hidden flex items-center justify-between px-4 py-3 border-b"
        style={{ borderColor: "var(--border)" }}
      >
        <span className="font-headline font-extrabold text-primary">eduit.</span>
        <button
          type="button"
          className="item-menu"
          aria-expanded={menuAberto}
          aria-controls="menu-lateral"
          onClick={() => setMenuAberto((v) => !v)}
        >
          <span className="material-symbols-outlined" aria-hidden>menu</span>
          Menu
        </button>
      </header>
      <aside
        id="menu-lateral"
        className={`${menuAberto ? "block" : "hidden"} md:block md:w-64 shrink-0 p-4 border-r overflow-y-auto md:h-screen md:sticky md:top-0`}
        style={{ background: "var(--bg-card)", borderColor: "var(--border)" }}
      >
        <div className="hidden md:block px-3 mb-6">
          <a href="/" className="font-headline text-xl font-extrabold text-primary">eduit.</a>
        </div>
        {menu && <MenuLateral menu={menu} />}
        <div className="mt-6 pt-4 border-t flex flex-col gap-0.5" style={{ borderColor: "var(--border)" }}>
          <span className="px-3 text-xs truncate" style={{ color: "var(--text-muted)" }}>
            {me.data.display_name || me.data.username}
          </span>
          <button type="button" className="item-menu" onClick={alternarTema}>
            <span className="material-symbols-outlined" aria-hidden>contrast</span>
            Tema claro/escuro
          </button>
          <a href="/logout" className="item-menu">
            <span className="material-symbols-outlined" aria-hidden>logout</span>
            Sair
          </a>
        </div>
      </aside>
      <main className="flex-1 min-w-0 p-4 md:p-8">
        <Outlet />
      </main>
    </div>
  );
}
