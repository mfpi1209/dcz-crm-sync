import { NavLink } from "react-router-dom";
import type { ItemMenu, Menu } from "../menu";

function Item({ i }: { i: ItemMenu }) {
  if (i.nova) {
    return (
      <NavLink to={`/${i.pagina}`} className="item-menu">
        {i.titulo}
      </NavLink>
    );
  }
  // Tela ainda no painel atual: navegação normal do navegador.
  return (
    <a href={i.href} className="item-menu">
      {i.titulo}
    </a>
  );
}

export function MenuLateral({ menu }: { menu: Menu }) {
  return (
    <nav aria-label="Menu principal" className="flex flex-col gap-4">
      <ul className="flex flex-col gap-0.5">
        {menu.avulsas.map((i) => (
          <li key={i.pagina}>
            <Item i={i} />
          </li>
        ))}
      </ul>
      {menu.grupos.map((g) => (
        <section key={g.nome} aria-labelledby={`grupo-${g.nome}`}>
          <h2 id={`grupo-${g.nome}`} className="rotulo px-3 mb-1">
            {g.titulo}
          </h2>
          <ul className="flex flex-col gap-0.5">
            {g.itens.map((i) => (
              <li key={i.pagina}>
                <Item i={i} />
              </li>
            ))}
          </ul>
        </section>
      ))}
    </nav>
  );
}
