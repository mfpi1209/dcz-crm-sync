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
    paginas: ["dashboard", "avisos", "meus_atendimentos", "page_views"],
    secoes: { comercial: true, ti: true },
    grupos: { comercial: ["meus_atendimentos"], ti: ["page_views"] },
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
    fireEvent.change(campo2, { target: { value: "atendimentos" } });
    fireEvent.keyDown(campo2, { key: "Enter" });
    expect(ir).toHaveBeenCalledWith("/#meus_atendimentos");
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

  it("Tab não sai do diálogo, mesmo com o foco fora do campo", async () => {
    await abrir();
    fireEvent.keyDown(document, { key: "k", ctrlKey: true });
    const dialogo = await screen.findByRole("dialog");
    const campo = screen.getByRole("combobox");
    for (const shiftKey of [false, true]) {
      // foco no painel (clique numa parte sem campo) e, depois, no corpo da página
      for (const fora of [dialogo, document.body]) {
        fora.focus();
        if (fora === document.body) (document.activeElement as HTMLElement).blur();
        expect(document.activeElement).not.toBe(campo);
        const seguiu = fireEvent.keyDown(document.activeElement ?? document.body, { key: "Tab", shiftKey });
        expect(seguiu).toBe(false); // preventDefault: o navegador não move o foco
        expect(document.activeElement).toBe(campo);
      }
    }
  });

  it("clicar no fundo fecha", async () => {
    await abrir();
    fireEvent.keyDown(document, { key: "k", ctrlKey: true });
    await screen.findByRole("dialog");
    fireEvent.click(screen.getByTestId("busca-fundo"));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
  });

  it("Esc fecha depois de clicar no painel (foco fora do campo)", async () => {
    await abrir();
    fireEvent.keyDown(document, { key: "k", ctrlKey: true });
    const dialogo = await screen.findByRole("dialog");
    fireEvent.click(screen.getByText(/navegar/));
    // clique em área sem campo: o foco sai do input (para o painel ou o corpo)
    dialogo.focus();
    expect(document.activeElement).toBe(dialogo);
    fireEvent.keyDown(dialogo, { key: "Escape" });
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());

    fireEvent.keyDown(document, { key: "k", ctrlKey: true });
    await screen.findByRole("dialog");
    (document.activeElement as HTMLElement).blur();
    fireEvent.keyDown(document.body, { key: "Escape" });
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
