import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import { App, FUTURO_ROUTER } from "./App";
import type { Me, Nav } from "./me";
import { GRUPOS_DO_MENU, grupoDaTela, montarMenu } from "./menu";
import { NOMES_TELAS_NOVAS } from "./telas";

function nav(parcial: Partial<Nav>): Nav {
  return {
    is_admin: false,
    is_comercial: false,
    is_suporte_comercial: false,
    pagina_inicial: "dashboard",
    paginas: [],
    secoes: {},
    grupos: {},
    ...parcial,
  };
}

const VAZIO: ReadonlySet<string> = new Set();

describe("montarMenu", () => {
  it("consultor comercial: Minha Performance primeiro, grupo Comercial, sem Dashboard", () => {
    const m = montarMenu(
      nav({
        is_comercial: true,
        pagina_inicial: "minha_performance",
        paginas: ["avisos", "minha_performance", "profile", "repasse", "bwipo_comercial"],
        secoes: { comercial: true, minha_perf_no_comercial: false },
        grupos: { comercial: ["bwipo_comercial", "repasse"] },
      }),
      VAZIO,
    );
    expect(m.avulsas.map((i) => i.pagina)).toEqual(["minha_performance", "avisos", "profile"]);
    expect(m.grupos.map((g) => g.nome)).toEqual(["comercial"]);
    expect(m.grupos[0].itens.map((i) => i.pagina)).toEqual(["bwipo_comercial", "repasse"]);
  });

  it("Minha Performance vai para o Comercial de quem não a tem como início", () => {
    const m = montarMenu(
      nav({
        paginas: ["dashboard", "minha_performance", "repasse"],
        secoes: { comercial: true, minha_perf_no_comercial: true },
        grupos: { comercial: ["repasse"] },
      }),
      VAZIO,
    );
    expect(m.grupos[0].itens.map((i) => i.pagina)).toEqual(["minha_performance", "repasse"]);
    expect(m.avulsas.map((i) => i.pagina)).toEqual(["dashboard"]);
  });

  it("grupo escondido pelo servidor some com as telas dele", () => {
    const m = montarMenu(
      nav({
        paginas: ["dashboard", "page_views"],
        secoes: { ti: false },
        grupos: { ti: ["page_views"] },
      }),
      VAZIO,
    );
    expect(m.grupos).toEqual([]);
    expect(m.avulsas.map((i) => i.pagina)).toEqual(["dashboard"]);
  });

  it("subpermissão sem nome de tela não vira item", () => {
    const m = montarMenu(nav({ paginas: ["dashboard", "disparador_whatsapp_metas", "chamados_marketing"] }), VAZIO);
    expect(m.avulsas.map((i) => i.pagina)).toEqual(["dashboard"]);
  });

  it("tela migrada abre no front novo; as demais no painel atual", () => {
    const m = montarMenu(nav({ paginas: ["avisos", "page_views"] }), new Set(["page_views"]));
    const porNome = Object.fromEntries(m.avulsas.map((i) => [i.pagina, i]));
    expect(porNome.page_views).toMatchObject({ nova: true, href: "/app/page_views", titulo: "Uso do Dashboard" });
    expect(porNome.avisos).toMatchObject({ nova: false, href: "/#avisos" });
  });

  it("Repasse já é tela do front novo", () => {
    const m = montarMenu(
      nav({ paginas: ["repasse", "meus_atendimentos"], secoes: { comercial: true }, grupos: { comercial: ["repasse", "meus_atendimentos"] } }),
      NOMES_TELAS_NOVAS,
    );
    const porNome = Object.fromEntries(m.grupos[0].itens.map((i) => [i.pagina, i]));
    expect(porNome.repasse).toMatchObject({ nova: true, href: "/app/repasse", titulo: "Repasse" });
    expect(porNome.meus_atendimentos).toMatchObject({ nova: false, href: "/#meus_atendimentos" });
  });
});

describe("montarMenu: fixadas e grupos", () => {
  const NAV = nav({
    paginas: ["dashboard", "avisos", "repasse", "bwipo_comercial"],
    secoes: { comercial: true },
    grupos: { comercial: ["bwipo_comercial", "repasse"] },
  });

  it("fixadas primeiro, com título Fixadas", () => {
    const m = montarMenu(NAV, VAZIO, { fixadas: ["repasse", "avisos"], mais_usadas: ["dashboard"] });
    expect(m.fixadas?.titulo).toBe("Fixadas");
    expect(m.fixadas?.itens.map((i) => i.pagina)).toEqual(["repasse", "avisos"]);
    // continuam também no lugar normal
    expect(m.grupos[0].itens.map((i) => i.pagina)).toContain("repasse");
    expect(m.avulsas.map((i) => i.pagina)).toContain("avisos");
  });

  it("sem fixadas usa Mais usadas", () => {
    const m = montarMenu(NAV, VAZIO, { fixadas: [], mais_usadas: ["dashboard", "repasse"] });
    expect(m.fixadas?.titulo).toBe("Mais usadas");
    expect(m.fixadas?.itens.map((i) => i.pagina)).toEqual(["dashboard", "repasse"]);
  });

  it("fixada invisível não aparece", () => {
    const m = montarMenu(NAV, VAZIO, { fixadas: ["page_views", "repasse"], mais_usadas: [] });
    expect(m.fixadas?.itens.map((i) => i.pagina)).toEqual(["repasse"]);
    const so = montarMenu(NAV, VAZIO, { fixadas: ["page_views"], mais_usadas: ["repasse"] });
    expect(so.fixadas?.titulo).toBe("Mais usadas");
    const nada = montarMenu(NAV, VAZIO, { fixadas: ["page_views"], mais_usadas: ["ti_x"] });
    expect(nada.fixadas).toBeNull();
  });

  it("sem fixadas quando a API falha", () => {
    expect(montarMenu(NAV, VAZIO, null).fixadas).toBeNull();
    expect(montarMenu(NAV, VAZIO).fixadas).toBeNull();
  });

  it("grupoDaTela acha o grupo", () => {
    const m = montarMenu(NAV, VAZIO);
    expect(grupoDaTela(m, "repasse")?.nome).toBe("comercial");
    expect(grupoDaTela(m, "dashboard")).toBeNull();
    expect(grupoDaTela(m, "nao_existe")).toBeNull();
  });

  it("todo grupo tem ícone", () => {
    expect(Object.fromEntries(GRUPOS_DO_MENU.map((g) => [g.nome, g.icone]))).toEqual({
      comercial: "trending_up",
      academico: "school",
      premiacoes_internas: "emoji_events",
      ferramentas: "construction",
      vocacional: "explore",
      meta: "campaign",
      chamados: "support_agent",
      ti: "memory",
      sistema: "settings",
    });
  });
});

describe("App", () => {
  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
  });

  function abrir(me: Me, caminho = "/") {
    const json = (corpo: unknown, status = 200) =>
      new Response(JSON.stringify(corpo), { status, headers: { "Content-Type": "application/json" } });
    vi.spyOn(globalThis, "fetch").mockImplementation(async (entrada) => {
      const url = String(entrada);
      if (url === "/api/me") return json(me);
      if (url === "/api/me/fixadas") return json({ fixadas: [], mais_usadas: ["meus_atendimentos"] });
      if (url === "/api/avisos/nao-lidos") return json({ count: 0 });
      return json({}, 404);
    });
    const cliente = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={cliente}>
        <MemoryRouter initialEntries={[caminho]} future={FUTURO_ROUTER}>
          <App />
        </MemoryRouter>
      </QueryClientProvider>,
    );
  }

  const ME: Me = {
    user_id: 7,
    username: "fulano@eduit.com.br",
    role: "viewer",
    categoria: "Comercial",
    display_name: "Fulano",
    nav: nav({
      is_comercial: true,
      pagina_inicial: "minha_performance",
      paginas: ["minha_performance", "meus_atendimentos", "profile"],
      secoes: { comercial: true },
      grupos: { comercial: ["meus_atendimentos"] },
    }),
  };

  it("monta o menu com o /api/me e leva as telas ao painel atual", async () => {
    abrir(ME);
    expect(await screen.findByText("Olá, Fulano")).toBeTruthy();
    const menu = within(screen.getByRole("navigation", { name: "Menu principal" }));
    // Meus Atendimentos (ainda no painel atual) aparece em Mais usadas e no grupo Comercial (que começa fechado).
    fireEvent.click(menu.getByRole("button", { name: "Comercial" }));
    const link = within(menu.getByRole("list", { name: "Comercial" })).getByRole("link", { name: "Meus Atendimentos" });
    expect(link.getAttribute("href")).toBe("/#meus_atendimentos");
    const maisUsadas = within(await menu.findByRole("list", { name: "Mais usadas" }));
    expect(maisUsadas.getByRole("link", { name: "Meus Atendimentos" }).getAttribute("href")).toBe("/#meus_atendimentos");
    expect(screen.queryByRole("link", { name: "Dashboard" })).toBeNull();
  });

  it("tela que não existe no front novo mostra aviso", async () => {
    abrir(ME, "/meus_atendimentos");
    expect(await screen.findByText("Tela não encontrada")).toBeTruthy();
  });

  it("falha do /api/me mostra erro com caminho de volta", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("x", { status: 500 }));
    const cliente = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={cliente}>
        <MemoryRouter future={FUTURO_ROUTER}>
          <App />
        </MemoryRouter>
      </QueryClientProvider>,
    );
    expect(await screen.findByRole("alert")).toBeTruthy();
    expect(screen.getByRole("link", { name: "Voltar ao painel" }).getAttribute("href")).toBe("/");
  });
});
