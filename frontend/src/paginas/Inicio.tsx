import { Link } from "react-router-dom";
import { useMe } from "../me";
import { item } from "../menu";
import { NOMES_TELAS_NOVAS } from "../telas";

/** Página inicial do front novo enquanto as telas são migradas uma a uma. */
export function Inicio() {
  const me = useMe();
  const permitidas = new Set(me.data?.nav.paginas ?? []);
  const novas = [...NOMES_TELAS_NOVAS].filter((p) => permitidas.has(p)).map((p) => item(p, NOMES_TELAS_NOVAS));
  const inicial = me.data ? item(me.data.nav.pagina_inicial, NOMES_TELAS_NOVAS) : null;

  return (
    <div className="max-w-3xl flex flex-col gap-6">
      <div>
        <p className="rotulo">Front novo</p>
        <h1 className="text-2xl mt-1">Olá, {me.data?.display_name || me.data?.username}</h1>
        <p className="mt-2 text-sm" style={{ color: "var(--text-secondary)" }}>
          Esta é a nova versão do painel, em construção. As telas chegam aqui uma por vez; as demais
          continuam abrindo no painel atual, com o mesmo login e as mesmas permissões.
        </p>
      </div>

      <section className="cartao p-5">
        <h2 className="text-base">Telas já no front novo</h2>
        {novas.length ? (
          <ul className="mt-3 flex flex-col gap-1">
            {novas.map((i) => (
              <li key={i.pagina}>
                <Link to={`/${i.pagina}`} className="item-menu">{i.titulo}</Link>
              </li>
            ))}
          </ul>
        ) : (
          <p className="mt-2 text-sm" style={{ color: "var(--text-muted)" }}>
            Nenhuma ainda. A primeira entra na próxima etapa.
          </p>
        )}
      </section>

      {inicial && (
        <p className="text-sm">
          <a className="underline text-primary" href={inicial.href}>
            Ir para {inicial.titulo} no painel atual
          </a>
        </p>
      )}
    </div>
  );
}
