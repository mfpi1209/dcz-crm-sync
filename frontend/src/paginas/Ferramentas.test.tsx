import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import { App, FUTURO_ROUTER } from "../App";
import type { Me } from "../me";
import { FerramentaIframe } from "./ferramentas/FerramentaIframe";

vi.mock("../rastreio", () => ({ registrarAcesso: vi.fn() }));

const TELAS = [
  { pagina: "comparar_cursos", titulo: "Comparar cursos", url: "/static/eduit/app.html#comparar" },
  { pagina: "recomendacao_cursos", titulo: "Recomendação de cursos", url: "/static/eduit/app.html#recomendacao" },
  { pagina: "localizacao_polos", titulo: "Localização dos polos", url: "/static/eduit/app.html#localizacao" },
  { pagina: "info_cursos", titulo: "Informações de cursos", url: "/static/eduit/app.html#informacoes-cursos" },
];

function json(corpo: unknown, status = 200) {
  return new Response(JSON.stringify(corpo), { status, headers: { "Content-Type": "application/json" } });
}

function me(paginas: string[]): Me {
  return {
    user_id: 1,
    username: "ana",
    role: "viewer",
    categoria: null,
    display_name: "Ana",
    nav: {
      is_admin: false,
      is_comercial: false,
      is_suporte_comercial: false,
      pagina_inicial: "dashboard",
      paginas,
      secoes: { ferramentas: true },
      grupos: { ferramentas: paginas },
    },
  };
}

function abrir(pagina: string, paginas = TELAS.map((t) => t.pagina)) {
  vi.spyOn(globalThis, "fetch").mockImplementation(async (entrada) => {
    const url = String(entrada);
    if (url === "/api/me") return json(me(paginas));
    if (url === "/api/me/fixadas") return json({ fixadas: [], mais_usadas: [] });
    if (url === "/api/avisos/nao-lidos") return json({ count: 0 });
    return json({}, 404);
  });
  const cliente = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={cliente}>
      <MemoryRouter initialEntries={["/" + pagina]} future={FUTURO_ROUTER}>
        <App />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("Ferramentas em iframe", () => {
  for (const t of TELAS) {
    it(`${t.pagina}: iframe com a URL certa e título acessível`, async () => {
      abrir(t.pagina);
      const quadro = (await screen.findByTitle(t.titulo)) as HTMLIFrameElement;
      expect(quadro.tagName).toBe("IFRAME");
      expect(quadro.getAttribute("src")).toBe(t.url);
      expect(screen.getByText(`Carregando ${t.titulo}…`)).toBeTruthy();
      fireEvent.load(quadro);
      expect(screen.queryByText(`Carregando ${t.titulo}…`)).toBeNull();
    });
  }

  it("sem a página no /api/me, a tela não abre", async () => {
    abrir("info_cursos", ["comparar_cursos"]);
    expect(await screen.findByText("Tela não encontrada")).toBeTruthy();
    expect(screen.queryByTitle("Informações de cursos")).toBeNull();
  });

  it("não chama nenhuma rota da API além das da casca", async () => {
    abrir("comparar_cursos");
    await screen.findByTitle("Comparar cursos");
    const urls = vi.mocked(globalThis.fetch).mock.calls.map((c) => String(c[0]));
    expect(urls.every((u) => ["/api/me", "/api/me/fixadas", "/api/avisos/nao-lidos"].includes(u))).toBe(true);
  });

  it("ocupa a área de conteúdo toda (sem padding da casca)", async () => {
    abrir("comparar_cursos");
    const quadro = await screen.findByTitle("Comparar cursos");
    expect(screen.getByRole("main").className).not.toContain("px-4");
    expect(quadro.className).toContain("h-full");
  });

  it("tela comum (page_views) mantém o padding do main e a coluna sem h-dvh", async () => {
    abrir("page_views", ["page_views"]);
    await screen.findByRole("main");
    const main = screen.getByRole("main");
    expect(main.className).toContain("px-4");
    expect(main.parentElement?.className).not.toContain("h-dvh");
    expect(document.querySelector("iframe")).toBeNull();
  });
});

describe("FerramentaIframe", () => {
  it("sem URL: mensagem amigável e nenhum iframe", () => {
    render(<FerramentaIframe titulo="Comparar cursos" url="" />);
    expect(screen.getByText("Esta ferramenta ainda não está configurada.")).toBeTruthy();
    expect(document.querySelector("iframe")).toBeNull();
    expect(screen.queryByRole("status")).toBeNull();
  });

  it("URL nula também é não configurada", () => {
    render(<FerramentaIframe titulo="X" url={null} />);
    expect(screen.getByText("X")).toBeTruthy();
    expect(screen.getByText("Esta ferramenta ainda não está configurada.")).toBeTruthy();
  });

  it("trocar a URL volta ao estado de carregando", () => {
    const { rerender } = render(<FerramentaIframe titulo="X" url="/a" />);
    fireEvent.load(screen.getByTitle("X"));
    expect(screen.queryByRole("status")).toBeNull();
    rerender(<FerramentaIframe titulo="X" url="/b" />);
    expect(screen.getByRole("status")).toBeTruthy();
    expect(screen.getByTitle("X").getAttribute("src")).toBe("/b");
  });
});
