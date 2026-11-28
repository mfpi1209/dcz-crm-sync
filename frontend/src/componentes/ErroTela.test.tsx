import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { lazy } from "react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import { App, FUTURO_ROUTER } from "../App";
import type { Me } from "../me";

vi.mock("../rastreio", () => ({ registrarAcesso: vi.fn() }));

// Uma tela sob demanda cujo pacote não carrega (ex.: deploy novo apagou o arquivo antigo).
vi.mock("../telas", () => {
  const Quebrada = lazy(() => Promise.reject(new Error("Failed to fetch dynamically imported module")));
  return { TELAS_NOVAS: { repasse: Quebrada }, NOMES_TELAS_NOVAS: new Set(["repasse"]) };
});

const ME: Me = {
  user_id: 1,
  username: "adm",
  role: "admin",
  categoria: null,
  display_name: "Adm",
  nav: {
    is_admin: true,
    is_comercial: false,
    is_suporte_comercial: false,
    pagina_inicial: "dashboard",
    paginas: ["dashboard", "repasse"],
    secoes: { comercial: true },
    grupos: { comercial: ["repasse"] },
  },
};

function json(corpo: unknown) {
  return new Response(JSON.stringify(corpo), { status: 200, headers: { "Content-Type": "application/json" } });
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("ErroTela", () => {
  it("tela sob demanda que falha mostra o aviso com Recarregar e o menu continua", async () => {
    vi.spyOn(console, "error").mockImplementation(() => {});
    vi.spyOn(globalThis, "fetch").mockImplementation(async (e) => {
      const url = String(e);
      if (url === "/api/me") return json(ME);
      if (url === "/api/me/fixadas") return json({ fixadas: [], mais_usadas: [] });
      return json({ count: 0 });
    });
    const recarregar = vi.fn();
    vi.spyOn(window, "location", "get").mockReturnValue({ ...window.location, reload: recarregar });
    render(
      <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
        <MemoryRouter initialEntries={["/repasse"]} future={FUTURO_ROUTER}>
          <App />
        </MemoryRouter>
      </QueryClientProvider>,
    );
    expect(await screen.findByText("Não deu para abrir esta tela.")).toBeTruthy();
    expect(screen.getByRole("navigation", { name: "Menu principal" })).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Recarregar" }));
    expect(recarregar).toHaveBeenCalled();
  });
});
