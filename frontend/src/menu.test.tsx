import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import { App, FUTURO_ROUTER } from "./App";
import type { Me, Nav } from "./me";
import { montarMenu } from "./menu";

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
    const m = montarMenu(nav({ paginas: ["dashboard", "page_views"] }), new Set(["page_views"]));
    const porNome = Object.fromEntries(m.avulsas.map((i) => [i.pagina, i]));
    expect(porNome.page_views).toMatchObject({ nova: true, href: "/app/page_views", titulo: "Uso do Dashboard" });
    expect(porNome.dashboard).toMatchObject({ nova: false, href: "/#dashboard" });
  });
});

describe("App", () => {
  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
  });

  function abrir(me: Me, caminho = "/") {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify(me), { status: 200, headers: { "Content-Type": "application/json" } }),
    );
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
      paginas: ["minha_performance", "repasse", "profile"],
      secoes: { comercial: true },
      grupos: { comercial: ["repasse"] },
    }),
  };

  it("monta o menu com o /api/me e leva as telas ao painel atual", async () => {
    abrir(ME);
    expect(await screen.findByText("Olá, Fulano")).toBeTruthy();
    const link = screen.getByRole("link", { name: "Repasse" });
    expect(link.getAttribute("href")).toBe("/#repasse");
    expect(screen.queryByRole("link", { name: "Dashboard" })).toBeNull();
  });

  it("tela que não existe no front novo mostra aviso", async () => {
    abrir(ME, "/repasse");
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
