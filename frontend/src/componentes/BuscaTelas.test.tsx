import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import { navegador } from "../api";
import { App, FUTURO_ROUTER } from "../App";
import type { Me } from "../me";

vi.mock("../rastreio", () => ({ registrarAcesso: vi.fn() }));

const irParaRota = vi.fn();
vi.mock("react-router-dom", async (original) => ({
  ...(await original<typeof import("react-router-dom")>()),
  useNavigate: () => irParaRota,
}));

function json(corpo: unknown, status = 200) {
  return new Response(JSON.stringify(corpo), { status, headers: { "Content-Type": "application/json" } });
}

const me: Me = {
  user_id: 1,
  username: "ana@eduit.com.br",
  role: "admin",
  categoria: null,
  display_name: "Ana Souza",
  nav: {
    is_admin: true,
    is_comercial: false,
    is_suporte_comercial: false,
    pagina_inicial: "dashboard",
    paginas: ["dashboard", "avisos", "repasse", "page_views"],
    secoes: { comercial: true, ti: true },
    grupos: { comercial: ["repasse"], ti: ["page_views"] },
  },
};

async function abrir() {
  vi.spyOn(globalThis, "fetch").mockImplementation(async (entrada) => {
    const url = String(entrada);
    if (url === "/api/me") return json(me);
    if (url === "/api/me/fixadas") return json({ fixadas: [], mais_usadas: [] });
    if (url === "/api/avisos/nao-lidos") return json({ count: 0 });
    return json({}, 404);
  });
  const cliente = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={cliente}>
      <MemoryRouter initialEntries={["/"]} future={FUTURO_ROUTER}>
        <App />
      </MemoryRouter>
    </QueryClientProvider>,
  );
  await screen.findByRole("button", { name: /Buscar tela/ });
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  irParaRota.mockReset();
});

describe("busca de telas", () => {
  it("teclado de ponta a ponta", async () => {
    await abrir();
    fireEvent.keyDown(document, { key: "k", ctrlKey: true });
    const dialogo = await screen.findByRole("dialog", { name: "Buscar tela" });
    const campo = screen.getByRole("combobox", { name: "Buscar tela" });
    expect(dialogo.contains(campo)).toBe(true);
    expect(document.activeElement).toBe(campo);

    fireEvent.change(campo, { target: { value: "dash" } });
    expect(screen.getAllByRole("option").map((o) => o.textContent)).toEqual([
      expect.stringContaining("Dashboard"),
      expect.stringContaining("Uso do Dashboard"),
    ]);
    fireEvent.keyDown(campo, { key: "ArrowDown" });
    fireEvent.keyDown(campo, { key: "Enter" });
    expect(irParaRota).toHaveBeenCalledWith("/page_views");
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());

    // tela antiga: navegação completa
    const ir = vi.spyOn(navegador, "irPara").mockImplementation(() => {});
    fireEvent.keyDown(document, { key: "k", metaKey: true });
    const campo2 = await screen.findByRole("combobox", { name: "Buscar tela" });
    fireEvent.change(campo2, { target: { value: "repasse" } });
    fireEvent.keyDown(campo2, { key: "Enter" });
    expect(ir).toHaveBeenCalledWith("/#repasse");
  });

  it("ArrowUp dá a volta e clique abre a tela", async () => {
    await abrir();
    fireEvent.keyDown(document, { key: "k", ctrlKey: true });
    const campo = await screen.findByRole("combobox", { name: "Buscar tela" });
    const total = screen.getAllByRole("option").length;
    fireEvent.keyDown(campo, { key: "ArrowUp" });
    expect(screen.getAllByRole("option")[total - 1].getAttribute("aria-selected")).toBe("true");
    fireEvent.change(campo, { target: { value: "uso" } });
    fireEvent.click(screen.getByRole("option", { name: /Uso do Dashboard/ }));
    expect(irParaRota).toHaveBeenCalledWith("/page_views");
  });

  it("Esc fecha e devolve o foco", async () => {
    await abrir();
    const botao = screen.getByRole("button", { name: /Buscar tela/ });
    botao.focus();
    fireEvent.click(botao);
    const campo = await screen.findByRole("combobox", { name: "Buscar tela" });
    fireEvent.keyDown(campo, { key: "Escape" });
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    expect(document.activeElement).toBe(botao);
  });

  it("Tab não sai do diálogo e o fundo fecha", async () => {
    await abrir();
    fireEvent.keyDown(document, { key: "k", ctrlKey: true });
    const dialogo = await screen.findByRole("dialog");
    const campo = screen.getByRole("combobox");
    fireEvent.keyDown(campo, { key: "Tab" });
    expect(dialogo.contains(document.activeElement)).toBe(true);
    fireEvent.keyDown(campo, { key: "Tab", shiftKey: true });
    expect(dialogo.contains(document.activeElement)).toBe(true);
    fireEvent.click(screen.getByTestId("busca-fundo"));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
  });

  it("só telas visíveis", async () => {
    await abrir();
    fireEvent.keyDown(document, { key: "k", ctrlKey: true });
    const campo = await screen.findByRole("combobox", { name: "Buscar tela" });
    const titulos = screen.getAllByRole("option").map((o) => o.textContent ?? "");
    expect(titulos).toHaveLength(4);
    fireEvent.change(campo, { target: { value: "chamados" } });
    expect(screen.queryAllByRole("option")).toHaveLength(0);
    expect(screen.getByText("Nenhuma tela encontrada")).toBeTruthy();
  });
});
