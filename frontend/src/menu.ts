import type { Nav } from "./me";
import { TITULOS } from "./titulos";

/**
 * Monta o menu a partir do /api/me. Quem pode ver o quê vem do servidor
 * (navegacao.py); aqui só se decide a ordem, o nome e para onde cada item leva:
 * tela já migrada abre no front novo (/app/<tela>), as demais no painel atual (/#<tela>).
 */

export interface ItemMenu {
  pagina: string;
  titulo: string;
  href: string;
  nova: boolean;
}

export interface GrupoMenu {
  nome: string;
  titulo: string;
  itens: ItemMenu[];
}

export interface Menu {
  avulsas: ItemMenu[];
  grupos: GrupoMenu[];
}

/** Grupos na ordem do menu lateral. Subgrupos de Sistema ficam dentro de "sistema". */
export const GRUPOS_DO_MENU: { nome: string; titulo: string }[] = [
  { nome: "comercial", titulo: "Comercial" },
  { nome: "academico", titulo: "Acadêmico" },
  { nome: "premiacoes_internas", titulo: "Premiações" },
  { nome: "ferramentas", titulo: "Ferramentas" },
  { nome: "vocacional", titulo: "Vocacional" },
  { nome: "meta", titulo: "Campanhas Meta" },
  { nome: "chamados", titulo: "Chamados" },
  { nome: "ti", titulo: "TI" },
  { nome: "sistema", titulo: "Sistema" },
];

export function item(pagina: string, telasNovas: ReadonlySet<string>): ItemMenu {
  const nova = telasNovas.has(pagina);
  return {
    pagina,
    titulo: TITULOS[pagina] ?? pagina,
    href: nova ? `/app/${pagina}` : `/#${pagina}`,
    nova,
  };
}

export function montarMenu(nav: Nav, telasNovas: ReadonlySet<string>): Menu {
  const permitidas = new Set(nav.paginas.filter((p) => p in TITULOS));
  const usadas = new Set<string>();
  const grupos: GrupoMenu[] = [];

  // Tela de grupo escondido (ex.: TI para quem não é admin) some junto, como no menu atual.
  const ocultas = new Set<string>();
  for (const g of GRUPOS_DO_MENU) {
    if (!nav.secoes[g.nome]) (nav.grupos[g.nome] ?? []).forEach((p) => ocultas.add(p));
  }

  for (const g of GRUPOS_DO_MENU) {
    if (!nav.secoes[g.nome]) continue;
    const paginas = (nav.grupos[g.nome] ?? []).filter((p) => permitidas.has(p));
    // Minha Performance entra no Comercial para quem não a tem como tela inicial.
    if (g.nome === "comercial" && nav.secoes.minha_perf_no_comercial && permitidas.has("minha_performance")) {
      paginas.unshift("minha_performance");
    }
    const unicas = paginas.filter((p) => !usadas.has(p));
    unicas.forEach((p) => usadas.add(p));
    if (unicas.length) {
      grupos.push({ nome: g.nome, titulo: g.titulo, itens: unicas.map((p) => item(p, telasNovas)) });
    }
  }

  // Telas fora de grupo (Dashboard, Buscar, Meu Perfil...), com a tela inicial primeiro.
  const ordem = Object.keys(TITULOS);
  const avulsas = [...permitidas]
    .filter((p) => !usadas.has(p) && !ocultas.has(p))
    .sort((a, b) => {
      if (a === nav.pagina_inicial) return -1;
      if (b === nav.pagina_inicial) return 1;
      return ordem.indexOf(a) - ordem.indexOf(b);
    })
    .map((p) => item(p, telasNovas));

  return { avulsas, grupos };
}
