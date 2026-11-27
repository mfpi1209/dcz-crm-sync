import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { App, FUTURO_ROUTER } from "../App";
import { hojeIso, somarDias } from "../datas";
import type { Me } from "../me";
import { registrarAcesso } from "../rastreio";
import type { UsoStats } from "./UsoDoDashboard";

vi.mock("../rastreio", () => ({ registrarAcesso: vi.fn() }));

const STATS: UsoStats = {
  ok: true,
  start_date: "2026-10-29",
  end_date: "2026-11-27",
  totais: { total_hits: 1234, total_users: 9, total_pages: 2, dias_ativos: 20 },
  por_pagina: [
    { page: "bwipo_comercial", hits: 1000, users: 8 },
    { page: "repasse", hits: 234, users: 3 },
  ],
  nao_acessadas: [
    { page: "vocacional", users_with_access: [{ username: "adm", role: "admin" }, { username: "ana", role: "viewer" }] },
    { page: "logs", users_with_access: [] },
  ],
};

function me(admin = true): Me {
  return {
    user_id: 1,
    username: "adm",
    role: admin ? "admin" : "viewer",
    categoria: null,
    display_name: "Adm",
    nav: {
      is_admin: admin,
      is_comercial: false,
      is_suporte_comercial: false,
      pagina_inicial: "dashboard",
      paginas: admin ? ["dashboard", "page_views"] : ["dashboard"],
      secoes: { ti: admin },
      grupos: { ti: ["page_views"] },
    },
  };
}

function json(corpo: unknown, status = 200) {
  return new Response(JSON.stringify(corpo), { status, headers: { "Content-Type": "application/json" } });
}

let chamadas: string[];

function abrir(opcoes: { admin?: boolean; stats?: (url: string) => Response } = {}) {
  chamadas = [];
  vi.spyOn(globalThis, "fetch").mockImplementation(async (entrada) => {
    const url = String(entrada);
    chamadas.push(url);
    if (url === "/api/me") return json(me(opcoes.admin ?? true));
    if (url.startsWith("/api/page-views/stats")) return (opcoes.stats ?? (() => json(STATS)))(url);
    return json({}, 404);
  });
  const cliente = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={cliente}>
      <MemoryRouter initialEntries={["/page_views"]} future={FUTURO_ROUTER}>
        <App />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

function urlsStats() {
  return chamadas.filter((u) => u.startsWith("/api/page-views/stats"));
}

beforeEach(() => {
  vi.mocked(registrarAcesso).mockClear();
});

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("Uso do Dashboard", () => {
  it("abre com 30 dias e mostra os números da API", async () => {
    abrir();
    expect(await screen.findByText("1.234")).toBeTruthy();
    const fim = hojeIso();
    expect(urlsStats()[0]).toBe(`/api/page-views/stats?start_date=${somarDias(fim, -29)}&end_date=${fim}`);
    // páginas com e sem acesso
    expect(screen.getByText("Dashboard Comercial Bwipo")).toBeTruthy();
    expect(screen.getByText("1.000")).toBeTruthy();
    expect(screen.getByText("Dashboard Vocacional")).toBeTruthy();
    expect(screen.getByText("adm · admin")).toBeTruthy();
    expect(screen.getByText("Ninguém tem permissão · página órfã")).toBeTruthy();
    expect(screen.getByRole("button", { name: "30 dias" }).getAttribute("aria-pressed")).toBe("true");
  });

  it("indicadores: total, páginas com acesso e sem acesso", async () => {
    abrir();
    await screen.findByText("1.234");
    const valor = (rotulo: string) => screen.getByText(rotulo).nextElementSibling?.textContent;
    expect(valor("Total de acessos")).toBe("1.234");
    expect(valor("Páginas com acesso")).toBe("2");
    expect(valor("Páginas sem acesso")).toBe("2");
  });

  it("preset troca o período e busca de novo", async () => {
    abrir();
    await screen.findByText("1.234");
    fireEvent.click(screen.getByRole("button", { name: "Hoje" }));
    const hoje = hojeIso();
    await waitFor(() => expect(urlsStats()).toContain(`/api/page-views/stats?start_date=${hoje}&end_date=${hoje}`));
  });

  it("datas livres só valem ao clicar em Aplicar", async () => {
    abrir();
    await screen.findByText("1.234");
    fireEvent.change(screen.getByLabelText("De"), { target: { value: "2026-11-01" } });
    fireEvent.change(screen.getByLabelText("Até"), { target: { value: "2026-11-10" } });
    expect(urlsStats().some((u) => u.includes("2026-11-01"))).toBe(false);
    fireEvent.click(screen.getByRole("button", { name: "Aplicar" }));
    await waitFor(() =>
      expect(urlsStats()).toContain("/api/page-views/stats?start_date=2026-11-01&end_date=2026-11-10"),
    );
    expect(screen.getByRole("button", { name: "30 dias" }).getAttribute("aria-pressed")).toBe("false");
  });

  it("Atualizar busca de novo o mesmo período", async () => {
    abrir();
    await screen.findByText("1.234");
    fireEvent.click(screen.getByRole("button", { name: /Atualizar/ }));
    await waitFor(() => expect(urlsStats().length).toBe(2));
    expect(urlsStats()[1]).toBe(urlsStats()[0]);
  });

  it("período sem dados mostra os avisos das tabelas", async () => {
    abrir({ stats: () => json({ ...STATS, totais: { total_hits: 0 }, por_pagina: [], nao_acessadas: [] }) });
    expect(await screen.findByText("Nenhuma página acessada no período.")).toBeTruthy();
    expect(screen.getByText("Todas as páginas foram acessadas no período.")).toBeTruthy();
  });

  it("403 do servidor mostra acesso negado", async () => {
    abrir({ stats: () => json({ error: "Acesso negado" }, 403) });
    expect(within(await screen.findByRole("alert")).getByText("Acesso negado. Apenas administrador.")).toBeTruthy();
  });

  it("erro do servidor aparece na tela (a antiga ficava em branco)", async () => {
    abrir({ stats: () => json({ ok: false, error: "banco fora" }, 500) });
    expect((await screen.findByRole("alert")).textContent).toContain("banco fora");
  });

  it("registra o acesso, como o painel atual", async () => {
    abrir();
    await screen.findByText("1.234");
    expect(registrarAcesso).toHaveBeenCalledWith("page_views");
  });

  it("quem não tem a página não abre nem chama a API", async () => {
    abrir({ admin: false });
    expect(await screen.findByText("Tela não encontrada")).toBeTruthy();
    expect(urlsStats()).toEqual([]);
    expect(registrarAcesso).not.toHaveBeenCalled();
  });

  it("no menu, a tela aponta para o front novo", async () => {
    abrir();
    await screen.findByText("1.234");
    const link = screen.getByRole("link", { name: "Uso do Dashboard" });
    expect(link.getAttribute("href")).toBe("/page_views");
  });
});
