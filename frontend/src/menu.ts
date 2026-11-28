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
  icone: string;
  itens: ItemMenu[];
}

/** Seção do topo do menu: as fixadas do usuário ou, sem elas, as mais usadas. */
export interface SecaoFixadas {
  titulo: "Fixadas" | "Mais usadas";
  itens: ItemMenu[];
}

export interface Menu {
  fixadas: SecaoFixadas | null;
  avulsas: ItemMenu[];
  grupos: GrupoMenu[];
}

/** Grupos na ordem do menu lateral. Subgrupos de Sistema ficam dentro de "sistema". */
export const GRUPOS_DO_MENU: { nome: string; titulo: string; icone: string }[] = [
  { nome: "comercial", titulo: "Comercial", icone: "trending_up" },
  { nome: "academico", titulo: "Acadêmico", icone: "school" },
  { nome: "premiacoes_internas", titulo: "Premiações", icone: "emoji_events" },
  { nome: "ferramentas", titulo: "Ferramentas", icone: "construction" },
  { nome: "vocacional", titulo: "Vocacional", icone: "explore" },
  { nome: "meta", titulo: "Campanhas Meta", icone: "campaign" },
  { nome: "chamados", titulo: "Chamados", icone: "support_agent" },
  { nome: "ti", titulo: "TI", icone: "memory" },
  { nome: "sistema", titulo: "Sistema", icone: "settings" },
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

export function montarMenu(
  nav: Nav,
  telasNovas: ReadonlySet<string>,
  fixadas?: { fixadas: string[]; mais_usadas: string[] } | null,
): Menu {
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
      grupos.push({ nome: g.nome, titulo: g.titulo, icone: g.icone, itens: unicas.map((p) => item(p, telasNovas)) });
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

  return { fixadas: secaoFixadas(fixadas, avulsas, grupos), avulsas, grupos };
}

/** Só entra o que o próprio menu mostra: não se confia só na API. */
function secaoFixadas(
  dados: { fixadas: string[]; mais_usadas: string[] } | null | undefined,
  avulsas: ItemMenu[],
  grupos: GrupoMenu[],
): SecaoFixadas | null {
  if (!dados) return null;
  const visiveis = new Map<string, ItemMenu>();
  for (const i of [...avulsas, ...grupos.flatMap((g) => g.itens)]) visiveis.set(i.pagina, i);
  const escolher = (paginas: string[]) =>
    paginas.map((p) => visiveis.get(p)).filter((i): i is ItemMenu => i !== undefined);
  const fix = escolher(dados.fixadas);
  if (fix.length) return { titulo: "Fixadas", itens: fix };
  const mais = escolher(dados.mais_usadas);
  return mais.length ? { titulo: "Mais usadas", itens: mais } : null;
}

/** Grupo do menu que contém a tela, ou null (tela avulsa). */
export function grupoDaTela(menu: Menu, pagina: string): GrupoMenu | null {
  return menu.grupos.find((g) => g.itens.some((i) => i.pagina === pagina)) ?? null;
}
