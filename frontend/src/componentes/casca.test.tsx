import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { App, FUTURO_ROUTER } from "../App";
import type { Me } from "../me";

vi.mock("../rastreio", () => ({ registrarAcesso: vi.fn() }));

function json(corpo: unknown, status = 200) {
  return new Response(JSON.stringify(corpo), { status, headers: { "Content-Type": "application/json" } });
}

function me(): Me {
  return {
    user_id: 1,
    username: "ana.souza@eduit.com.br",
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
}

interface Opcoes {
  caminho?: string;
  fixadas?: unknown;
  avisos?: number;
  usuario?: Me;
  /** Resposta do PUT das fixadas; sem ela o servidor aceita a lista. */
  respostaPut?: () => Response;
}

let puts: unknown[];

function abrir(opcoes: Opcoes = {}) {
  puts = [];
  // Como o servidor de verdade, guarda a lista quando o PUT dá certo (o front relê depois).
  let salvo: unknown = opcoes.fixadas ?? { fixadas: [], mais_usadas: [] };
  vi.spyOn(globalThis, "fetch").mockImplementation(async (entrada, init) => {
    const url = String(entrada);
    if (url === "/api/me") return json(opcoes.usuario ?? me());
    if (url === "/api/me/fixadas") {
      if (init?.method === "PUT") {
        const corpo = JSON.parse(String(init.body));
        puts.push(corpo);
        if (opcoes.respostaPut) return opcoes.respostaPut();
        salvo = { ...(salvo as object), fixadas: corpo.paginas };
        return json({ fixadas: corpo.paginas });
      }
      return json(salvo);
    }
    if (url === "/api/avisos/nao-lidos") return json({ count: opcoes.avisos ?? 0 });
    if (url.startsWith("/api/page-views/stats")) {
      return json({ ok: true, start_date: "", end_date: "", totais: {}, por_pagina: [], nao_acessadas: [] });
    }
    return json({}, 404);
  });
  const cliente = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={cliente}>
      <MemoryRouter initialEntries={[opcoes.caminho ?? "/"]} future={FUTURO_ROUTER}>
        <App />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

async function menuPrincipal() {
  return within(await screen.findByRole("navigation", { name: "Menu principal" }));
}

beforeEach(() => {
  localStorage.clear();
  document.documentElement.classList.remove("light");
  document.documentElement.classList.add("dark");
});

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("casca nova", () => {
  it("grupos abrem e fecham e lembram", async () => {
    const { unmount } = abrir();
    const menu = await menuPrincipal();
    const grupo = menu.getByRole("button", { name: "Comercial" });
    expect(grupo.getAttribute("aria-expanded")).toBe("false");
    expect(menu.queryByRole("link", { name: "Meus Atendimentos" })).toBeNull();

    fireEvent.click(grupo);
    expect(grupo.getAttribute("aria-expanded")).toBe("true");
    expect(menu.getByRole("link", { name: "Meus Atendimentos" }).getAttribute("href")).toBe("/#meus_atendimentos");
    expect(JSON.parse(localStorage.getItem("eduit-menu-grupos") ?? "[]")).toContain("comercial");

    unmount();
    abrir();
    const denovo = await menuPrincipal();
    expect(denovo.getByRole("button", { name: "Comercial" }).getAttribute("aria-expanded")).toBe("true");

    fireEvent.click(denovo.getByRole("button", { name: "Comercial" }));
    expect(denovo.getByRole("button", { name: "Comercial" }).getAttribute("aria-expanded")).toBe("false");
    expect(JSON.parse(localStorage.getItem("eduit-menu-grupos") ?? "[]")).not.toContain("comercial");
  });

  it("estado salvo inválido não quebra o menu", async () => {
    localStorage.setItem("eduit-menu-grupos", "{isso não é json");
    abrir();
    const menu = await menuPrincipal();
    expect(menu.getByRole("button", { name: "Comercial" }).getAttribute("aria-expanded")).toBe("false");
  });

  it("grupo da tela atual começa aberto", async () => {
    abrir({ caminho: "/page_views" });
    const menu = await menuPrincipal();
    expect(menu.getByRole("button", { name: "TI" }).getAttribute("aria-expanded")).toBe("true");
    expect(menu.getByRole("button", { name: "Comercial" }).getAttribute("aria-expanded")).toBe("false");
  });

  it("item ativo marcado", async () => {
    abrir({ caminho: "/page_views" });
    const menu = await menuPrincipal();
    const grupoTi = within(menu.getByRole("list", { name: "TI" }));
    expect(grupoTi.getByRole("link", { name: "Uso do Dashboard" }).getAttribute("aria-current")).toBe("page");
    const geral = within(menu.getByRole("list", { name: "Geral" }));
    expect(geral.getByRole("link", { name: "Dashboard" }).getAttribute("aria-current")).toBeNull();
  });

  it("barra de cima mostra título e grupo da rota", async () => {
    abrir({ caminho: "/page_views" });
    const barra = within(await screen.findByRole("banner", { name: "Barra da tela" }));
    expect(barra.getByRole("heading", { level: 1, name: "Uso do Dashboard" })).toBeTruthy();
    expect(barra.getByText("TI")).toBeTruthy();
  });

  it("início: título Início, sem estrela", async () => {
    abrir();
    const barra = within(await screen.findByRole("banner", { name: "Barra da tela" }));
    expect(barra.getByRole("heading", { level: 1, name: "Início" })).toBeTruthy();
    expect(barra.queryByRole("button", { name: /Fixar/ })).toBeNull();
  });

  it("estrela fixa a tela", async () => {
    abrir({ caminho: "/page_views", fixadas: { fixadas: ["dashboard"], mais_usadas: [] } });
    const barra = within(await screen.findByRole("banner", { name: "Barra da tela" }));
    const estrela = await barra.findByRole("button", { name: "Fixar esta tela" });
    expect(estrela.getAttribute("aria-pressed")).toBe("false");
    fireEvent.click(estrela);
    await waitFor(() => expect(puts).toEqual([{ paginas: ["dashboard", "page_views"] }]));
    expect(barra.getByRole("button", { name: "Desafixar esta tela" }).getAttribute("aria-pressed")).toBe("true");
    // e aparece na seção Fixadas do menu
    const menu = await menuPrincipal();
    const fixadas = within(menu.getByRole("list", { name: "Fixadas" }));
    expect(fixadas.getByRole("link", { name: "Uso do Dashboard" })).toBeTruthy();
  });

  it("estrela no item do menu fixa a tela", async () => {
    abrir({ fixadas: { fixadas: [], mais_usadas: [] } });
    const menu = await menuPrincipal();
    fireEvent.click(await menu.findByRole("button", { name: "Fixar Avisos" }));
    await waitFor(() => expect(puts).toEqual([{ paginas: ["avisos"] }]));
  });

  it("sino mostra contagem", async () => {
    abrir({ avisos: 3 });
    const barra = within(await screen.findByRole("banner", { name: "Barra da tela" }));
    const sino = await barra.findByRole("link", { name: "Avisos, 3 não lidos" });
    expect(sino.getAttribute("href")).toBe("/#avisos");
    expect(within(sino).getByText("3")).toBeTruthy();
  });

  it("sino sem não lidos fica sem bolinha", async () => {
    abrir({ avisos: 0 });
    const barra = within(await screen.findByRole("banner", { name: "Barra da tela" }));
    const sino = barra.getByRole("link", { name: "Avisos" });
    expect(sino.getAttribute("href")).toBe("/#avisos");
    await new Promise((r) => setTimeout(r, 20));
    expect(within(sino).queryByText("0")).toBeNull();
    expect(sino.querySelector("[data-bolinha]")).toBeNull();
  });

  it("tema alterna e grava eduit-theme", async () => {
    abrir();
    const barra = within(await screen.findByRole("banner", { name: "Barra da tela" }));
    fireEvent.click(barra.getByRole("button", { name: "Mudar para tema claro" }));
    expect(document.documentElement.classList.contains("light")).toBe(true);
    expect(document.documentElement.classList.contains("dark")).toBe(false);
    expect(localStorage.getItem("eduit-theme")).toBe("light");
    fireEvent.click(barra.getByRole("button", { name: "Mudar para tema escuro" }));
    expect(document.documentElement.classList.contains("dark")).toBe(true);
    expect(localStorage.getItem("eduit-theme")).toBe("dark");
  });

  it("gaveta fecha ao escolher", async () => {
    abrir({ caminho: "/page_views" });
    const botao = await screen.findByRole("button", { name: "Menu" });
    expect(botao.getAttribute("aria-expanded")).toBe("false");
    const gaveta = document.getElementById(botao.getAttribute("aria-controls") ?? "");
    expect(gaveta).not.toBeNull();
    fireEvent.click(botao);
    expect(botao.getAttribute("aria-expanded")).toBe("true");
    const menu = await menuPrincipal();
    fireEvent.click(within(menu.getByRole("list", { name: "TI" })).getByRole("link", { name: "Uso do Dashboard" }));
    expect(botao.getAttribute("aria-expanded")).toBe("false");
  });

  it("Esc fecha a gaveta", async () => {
    abrir();
    const botao = await screen.findByRole("button", { name: "Menu" });
    fireEvent.click(botao);
    expect(botao.getAttribute("aria-expanded")).toBe("true");
    fireEvent.keyDown(document, { key: "Escape" });
    expect(botao.getAttribute("aria-expanded")).toBe("false");
  });

  it("celular tem uma barra só: o botão Menu fica na barra da tela", async () => {
    abrir({ caminho: "/page_views" });
    const barra = within(await screen.findByRole("banner", { name: "Barra da tela" }));
    const botao = barra.getByRole("button", { name: "Menu" });
    expect(botao.getAttribute("aria-controls")).toBe("menu-lateral");
    expect(botao.className).toContain("md:hidden");
    expect(document.querySelectorAll("header").length).toBe(1);
  });

  it("Esc devolve o foco ao botão Menu", async () => {
    abrir();
    const botao = await screen.findByRole("button", { name: "Menu" });
    fireEvent.click(botao);
    fireEvent.keyDown(document, { key: "Escape" });
    expect(document.activeElement).toBe(botao);
  });

  it("Esc com a busca aberta sobre a gaveta fecha só a busca", async () => {
    abrir();
    const botao = await screen.findByRole("button", { name: "Menu" });
    fireEvent.click(botao);
    fireEvent.keyDown(document, { key: "k", ctrlKey: true });
    const campo = await screen.findByRole("combobox", { name: "Buscar tela" });
    fireEvent.keyDown(campo, { key: "Escape" });
    expect(screen.queryByRole("dialog", { name: "Buscar tela" })).toBeNull();
    expect(botao.getAttribute("aria-expanded")).toBe("true");
    // Segundo Esc fecha a gaveta.
    fireEvent.keyDown(document, { key: "Escape" });
    expect(botao.getAttribute("aria-expanded")).toBe("false");
  });

  it.each([
    [400, json({ error: "Tela não permitida" }, 400), "Tela não permitida"],
    [500, new Response("", { status: 500 }), "Não deu para salvar as fixadas. Tente de novo."],
  ])("PUT %s: aviso discreto aparece e a estrela volta", async (_status, resposta, texto) => {
    abrir({ caminho: "/page_views", fixadas: { fixadas: [], mais_usadas: [] }, respostaPut: () => resposta });
    const barra = within(await screen.findByRole("banner", { name: "Barra da tela" }));
    fireEvent.click(await barra.findByRole("button", { name: "Fixar esta tela" }));
    await waitFor(() => expect(barra.getByRole("status").textContent).toBe(texto));
    const estrela = barra.getByRole("button", { name: "Fixar esta tela" });
    expect(estrela.getAttribute("aria-pressed")).toBe("false");
  });

  it("fixadas fora do formato: menu sem a seção", async () => {
    abrir({ fixadas: {} });
    const menu = await menuPrincipal();
    expect(menu.getByRole("link", { name: "Dashboard" })).toBeTruthy();
    await new Promise((r) => setTimeout(r, 20));
    expect(menu.queryByRole("list", { name: "Fixadas" })).toBeNull();
    expect(menu.queryByRole("list", { name: "Mais usadas" })).toBeNull();
  });

  it("sem estrela em tela fora do menu (grupo TI oculto para quem não é admin)", async () => {
    const usuario = me();
    usuario.role = "consultor";
    usuario.nav = { ...usuario.nav, is_admin: false, secoes: { comercial: true, ti: false } };
    abrir({ caminho: "/page_views", usuario, fixadas: { fixadas: [], mais_usadas: [] } });
    const barra = within(await screen.findByRole("banner", { name: "Barra da tela" }));
    expect(barra.getByRole("heading", { level: 1, name: "Uso do Dashboard" })).toBeTruthy();
    await new Promise((r) => setTimeout(r, 20));
    expect(barra.queryByRole("button", { name: /Fixar|Desafixar/ })).toBeNull();
  });

  it("rodapé com usuário e sair", async () => {
    abrir();
    const rodape = within(await screen.findByRole("group", { name: "Usuário" }));
    expect(rodape.getByText("Ana Souza")).toBeTruthy();
    expect(rodape.getByText("AS")).toBeTruthy();
    expect(rodape.getByRole("link", { name: "Sair" }).getAttribute("href")).toBe("/logout");
  });

  it("botão Buscar tela está no menu", async () => {
    abrir();
    expect(await screen.findByRole("button", { name: /Buscar tela/ })).toBeTruthy();
  });

  it("falha das fixadas e dos avisos não quebra o menu", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation(async (entrada) =>
      String(entrada) === "/api/me" ? json(me()) : json({ error: "fora" }, 500),
    );
    const cliente = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={cliente}>
        <MemoryRouter future={FUTURO_ROUTER}>
          <App />
        </MemoryRouter>
      </QueryClientProvider>,
    );
    const menu = await menuPrincipal();
    expect(menu.getByRole("link", { name: "Dashboard" })).toBeTruthy();
    await new Promise((r) => setTimeout(r, 20));
    expect(menu.queryByRole("list", { name: "Fixadas" })).toBeNull();
    expect(menu.queryByRole("list", { name: "Mais usadas" })).toBeNull();
  });
});
