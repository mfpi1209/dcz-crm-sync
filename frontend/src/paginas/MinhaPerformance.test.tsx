import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { App, FUTURO_ROUTER } from "../App";
import type { Me } from "../me";
import { registrarAcesso } from "../rastreio";
import {
  aplicarVendasComercial,
  classificarSituacao,
  data,
  potencialMaximo,
  sincronizarHeatmap,
  ultimoPeriodo,
} from "./minha-performance/formato";
import type { Ajustes, Campanhas, Historico, Insights, MatriculasOficiais, MinhasMatriculas } from "./minha-performance/tipos";

vi.mock("../rastreio", () => ({ registrarAcesso: vi.fn() }));

/** chart.js não desenha no jsdom: guarda as configurações para conferir os dados. */
const graficos = vi.hoisted(() => [] as { type: string; data: { labels?: unknown[]; datasets: { label?: string; data: unknown[] }[] } }[]);
vi.mock("chart.js", () => {
  class Chart {
    static register() {}
    constructor(_c: unknown, cfg: (typeof graficos)[number]) {
      graficos.push(cfg);
    }
    destroy() {}
  }
  const nada = {};
  return {
    Chart,
    BarController: nada,
    LineController: nada,
    BarElement: nada,
    LineElement: nada,
    PointElement: nada,
    CategoryScale: nada,
    LinearScale: nada,
    Filler: nada,
    Legend: nada,
    Tooltip: nada,
  };
});

const R = "R$ ";

const CAMPANHAS: Campanhas = {
  ok: true,
  campanhas: [
    { id: 7, nome: "Campanha Novembro", dt_inicio: "2026-11-01", dt_fim: "2026-11-30", is_active: true },
    { id: 6, nome: "Campanha Outubro", dt_inicio: "2026-10-01", dt_fim: "2026-10-31", is_active: false },
  ],
  default_id: 7,
};

const INSIGHTS: Insights = {
  ok: true,
  campanha: { id: 7, nome: "Campanha Novembro", dt_inicio: "2026-11-01", dt_fim: "2026-11-30", is_active: true },
  total_matriculas: 12,
  metas: { intermediaria: 10, meta: 15, supermeta: 20 },
  tier: "intermediaria",
  pace_atual: 1.5,
  pace_meta: 0.8,
  projecao: 18,
  projecao_tier: "meta",
  projecao_financeira: 2500.5,
  dias_restantes: 2,
  hoje: { meta: 3, realizadas: 1, aceites_fila: 2, aceites_hoje: 0, bonus_fixo: 50, bonus_extra: 10, ontem_realizadas: 2 },
  sequencia: 3,
  streak_nivel: "aquecendo",
  heatmap: [
    { data: "2026-11-02", meta: 2, realizadas: 2, mat: 1, aceites: 1, status: "hit" },
    { data: "2026-11-03", meta: 2, realizadas: 1, mat: 1, aceites: 0, status: "partial" },
  ],
  mensagem: '🔥 <strong>Faltam 3</strong> para a meta<br><span class="x">Bora!</span><img src=x onerror="alert(1)">',
  tier_progress: [
    { tier: "base", target: 0, pct: 100, atingido: true, valor_por_mat: 0, ganho: 0 },
    { tier: "intermediaria", target: 10, pct: 100, atingido: true, valor_por_mat: 20, ganho: 240 },
    { tier: "meta", target: 15, pct: 80, atingido: false, valor_por_mat: 30, ganho: 360 },
    { tier: "supermeta", target: 20, pct: 60, atingido: false, valor_por_mat: 40, ganho: 480 },
  ],
  premiacao: {
    tier_bonus: 240,
    daily_bonus: 100,
    receb_bonus: 0,
    total: 340,
    daily_breakdown: [
      { data: "2026-11-02", realizadas: 2, meta: 2, total: 50 },
      { data: "2026-11-03", realizadas: 1, meta: 2, total: 0 },
    ],
    desbloqueie: [
      { tier: "intermediaria", target: 10, falta: 0, atingido: true, valor_por_mat: 20, ganho_total: 200, ganho_adicional: 0 },
      { tier: "meta", target: 15, falta: 3, atingido: false, valor_por_mat: 30, ganho_total: 450, ganho_adicional: 210 },
      { tier: "supermeta", target: 20, falta: 8, atingido: false, valor_por_mat: 40, ganho_total: 800, ganho_adicional: 560 },
    ],
  },
  ranking: { posicao: 3, total_agentes: 10, diferenca_lider: 2, meu_total: 14, minhas_mat: 12, meus_aceites: 2, media_time: 10 },
  conquistas: [{ id: "primeira_mat", nome: "Primeira Matrícula", icone: "school", desc: "Abriu o placar" }],
};

const HISTORICO: Historico = {
  ok: true,
  historico: [
    { campanha_id: 7, nome: "Campanha Novembro", dt_inicio: "2026-11-01", dt_fim: "2026-11-30", ativa: true, total_matriculas: 12, tier: "intermediaria", total_premiacao: 340 },
    { campanha_id: 6, nome: "Campanha Outubro", dt_inicio: "2026-10-01", dt_fim: "2026-10-31", ativa: false, total_matriculas: 9, tier: "meta", total_premiacao: 700 },
  ],
};

const SUPORTE: Insights = {
  ok: true,
  campanha: INSIGHTS.campanha,
  painel_suporte: true,
  suporte_equipe_view: true,
  meta_equipe_suporte: { meta: 100, total_matriculas: 40, agentes_count: 4, is_suporte: true },
  hoje: { meta: 0, realizadas: 0, aceites_fila: 0 },
  heatmap: [
    { data: "2026-11-02", meta: 4, realizadas: 1, mat: 1, aceites: 0, status: "partial" },
    { data: "2026-11-03", meta: 4, realizadas: 0, mat: 0, aceites: 0, status: "miss" },
  ],
  chart_matriculas_dia: [{ data: "2026-11-02", matriculas: 3 }],
  total_matriculas_time: 40,
  media_vendas: 2,
};

const COMERCIAL = { ok: true, kpis: { vendas: 55, media_diaria: 2.5 }, evolucao: [{ data: "2026-11-02", count: 5 }, { data: "2026-11-03", count: 6 }] };

const PERIODOS = {
  ok: true,
  campanhas: [
    { nome: "Meta Nov", dt_inicio: "2026-11-01", dt_fim: "2026-11-30" },
    { nome: "Meta Out", dt_inicio: "2026-10-01", dt_fim: "2026-10-31" },
  ],
};

const OFICIAIS: MatriculasOficiais = {
  ok: true,
  matriculas: [
    { nome: "Carla Dias", rgm: "11111111", curso: "Direito", nivel: "Graduação", polo: "Centro", data_matricula: "2026-11-05", situacao: "EM CURSO", conta_para_meta: true },
    { nome: "Bruno Alves", rgm: "22222222", curso: "", nivel: "Pós-Graduação", polo: "Norte", data_matricula: "2026-11-06", situacao: "CANCELADO" },
    { nome: "Ana Lima", rgm: "99", curso: "ADM", polo: "Sul", data_matricula: "2026-11-07", situacao: "EM CURSO", outlier: true, conta_para_meta: false },
    { nome: "Davi", rgm: "33333333", curso: "Letras", situacao: "PENDENTE" },
  ],
  total_contando: 1,
  ciclos_painel: { Graduação: "2026/2" },
};

const MINHAS: MinhasMatriculas = {
  ok: true,
  matriculas: [
    { id: 5, nome: "Carla Dias", rgm: "11111111", curso: "Direito", polo: "Centro", data_matricula: "2026-11-05", ciclo: "2026/2", nivel: "Graduação", kommo_lead_id: "18420", observacao: "" },
  ],
};

const AJUSTES: Ajustes = {
  ok: true,
  ajustes: [
    { id: 1, tipo: "dados_incorretos", status: "em_analise", nome_aluno: "Ana Lima", rgm: "99", kommo_lead_id: "777", descricao: "RGM errado", resposta_admin: "Vamos ver", created_at: "2026-11-10T12:00:00" },
  ],
};

type Perfil = "consultor" | "admin" | "suporte" | "premiacao" | "sem_kommo";

function me(perfil: Perfil): Me & Record<string, unknown> {
  const admin = perfil === "admin";
  return {
    user_id: 5,
    username: "ana",
    role: admin ? "admin" : "viewer",
    categoria: perfil === "suporte" ? "Suporte Comercial" : "Comercial",
    display_name: "Ana",
    kommo_user_id: perfil === "sem_kommo" ? null : 42,
    pages: perfil === "premiacao" ? ["minha_performance", "premiacao_admin"] : ["minha_performance"],
    is_suporte_comercial: perfil === "suporte",
    nav: {
      is_admin: admin,
      is_comercial: true,
      is_suporte_comercial: perfil === "suporte",
      pagina_inicial: "minha_performance",
      paginas: ["minha_performance"],
      secoes: { comercial: true },
      grupos: { comercial: [] },
    },
  };
}

function json(corpo: unknown, status = 200) {
  return new Response(JSON.stringify(corpo), { status, headers: { "Content-Type": "application/json" } });
}

interface Chamada {
  url: string;
  metodo: string;
  corpo?: string;
}
let chamadas: Chamada[];
type Rota = (url: string, init?: RequestInit) => Response;

function abrir(perfil: Perfil = "consultor", rotas: Record<string, Rota> = {}, caminho = "/minha_performance") {
  chamadas = [];
  vi.spyOn(globalThis, "fetch").mockImplementation(async (entrada, init) => {
    const url = String(entrada);
    const metodo = (init?.method || "GET").toUpperCase();
    chamadas.push({ url, metodo, corpo: init?.body as string | undefined });
    const chave = `${metodo} ${url.split("?")[0]}`;
    if (rotas[chave]) return rotas[chave](url, init);
    if (url === "/api/me") return json(me(perfil));
    if (url === "/api/me/fixadas") return json({ fixadas: [], mais_usadas: [] });
    if (url === "/api/avisos/nao-lidos") return json({ count: 0 });
    if (url === "/api/minha-performance/campanhas") return json(CAMPANHAS);
    if (url === "/api/minha-performance/agentes")
      return json({ ok: true, agentes: [{ kommo_uid: 42, name: "Ana" }, { kommo_uid: 55, name: "Bruno" }, { kommo_uid: 60, name: "Sara Suporte" }], suporte_uids: [60] });
    if (url.startsWith("/api/minha-performance/insights")) return json(url.includes("suporte_equipe=1") ? SUPORTE : INSIGHTS);
    if (url.startsWith("/api/minha-performance/historico")) return json(HISTORICO);
    if (url.startsWith("/api/comercial-rgm/data")) return json(COMERCIAL);
    if (url.startsWith("/api/premiacao/campanhas-periodos")) return json(PERIODOS);
    if (url.startsWith("/api/minha-performance/matriculas")) return json(OFICIAIS);
    if (url.startsWith("/api/minha-performance/minhas-matriculas") && metodo === "GET") return json(MINHAS);
    if (url.startsWith("/api/minha-performance/ajustes") && metodo === "GET") return json(AJUSTES);
    return json({ ok: true });
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

const gets = (prefixo: string) => chamadas.filter((c) => c.metodo === "GET" && c.url.startsWith(prefixo)).map((c) => c.url);
const regiao = (nome: string) => within(screen.getByRole("region", { name: nome }));

async function abrirMatriculas() {
  fireEvent.click(await screen.findByRole("button", { name: "Minhas matrículas" }));
  await within(await screen.findByRole("region", { name: "Matrículas oficiais" })).findByText("Carla Dias");
}

beforeEach(() => {
  vi.mocked(registrarAcesso).mockClear();
  graficos.length = 0;
});

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("regras puras (iguais às da tela antiga)", () => {
  it("data, situação e período mais recente", () => {
    expect(data("2026-11-05")).toBe("05/11/2026");
    expect(data("05/11/2026 10:00")).toBe("05/11/2026");
    expect(classificarSituacao("em curso")).toBe("ativo");
    expect(classificarSituacao("TRANSFERIDO")).toBe("evadido");
    expect(classificarSituacao("TRANCADO")).toBe("evadido");
    expect(classificarSituacao("PENDENTE")).toBe("outros");
    expect(classificarSituacao("")).toBe("outros");
    expect(
      ultimoPeriodo([
        { dt_inicio: "2026-10-01", dt_fim: "2026-10-31" },
        { dt_inicio: "2026-11-01", dt_fim: "2026-11-15" },
        { dt_inicio: "2026-11-01T00:00", dt_fim: "2026-11-30" },
      ]),
    ).toEqual({ dt_inicio: "2026-11-01", dt_fim: "2026-11-30" });
    expect(ultimoPeriodo([])).toBeNull();
  });

  it("potencial máximo = maior ganho de nível + PIX + recebimentos", () => {
    expect(potencialMaximo(INSIGHTS)).toBe(900);
    expect(potencialMaximo({ campanha: null, premiacao: { total: 12 } })).toBe(12);
  });

  it("vendas do Comercial substituem o total do time e a meta do time", () => {
    const r = aplicarVendasComercial(SUPORTE, COMERCIAL)!;
    expect(r.temVendas).toBe(true);
    expect(r.insights.total_matriculas_time).toBe(55);
    expect(r.insights.media_vendas).toBe(2.5);
    expect(r.insights.meta_equipe_suporte).toMatchObject({ total_matriculas: 55, pct: 55, meta_batida: false, matriculas_fonte: "comercial" });
    expect(r.insights.chart_matriculas_dia).toEqual([{ data: "2026-11-02", matriculas: 5 }, { data: "2026-11-03", matriculas: 6 }]);
    expect(aplicarVendasComercial(SUPORTE, { ok: false })).toBeNull();
  });

  it("calendário do suporte segue o gráfico por dia; depois de hoje é futuro", () => {
    const d = sincronizarHeatmap(
      { ...SUPORTE, chart_matriculas_dia: [{ data: "2026-11-02", matriculas: 5 }] },
      "2026-11-02",
    );
    expect(d.heatmap).toEqual([
      { data: "2026-11-02", meta: 4, realizadas: 5, mat: 5, aceites: 0, status: "hit" },
      { data: "2026-11-03", meta: 4, realizadas: null, mat: 0, aceites: 0, status: "future" },
    ]);
  });
});

describe("Minha Performance — consultor", () => {
  it("carrega campanhas, insights e histórico com o próprio kommo_uid e nunca a lista de agentes", async () => {
    abrir();
    await screen.findByRole("region", { name: "Saldo" });
    expect(gets("/api/minha-performance/campanhas")).toEqual(["/api/minha-performance/campanhas"]);
    expect(gets("/api/minha-performance/insights")).toEqual(["/api/minha-performance/insights?kommo_uid=42&campanha_id=7"]);
    await waitFor(() => expect(gets("/api/minha-performance/historico")).toEqual(["/api/minha-performance/historico?kommo_uid=42&campanha_id=7"]));
    expect(gets("/api/minha-performance/agentes")).toEqual([]);
    expect(gets("/api/comercial-rgm")).toEqual([]);
    expect(screen.queryByLabelText("Consultor")).toBeNull();
    expect(registrarAcesso).toHaveBeenCalledWith("minha_performance");
  });

  it("saldo, potencial, nível, gauge e mensagem do servidor (sem HTML perigoso)", async () => {
    abrir();
    const saldo = within(await screen.findByRole("region", { name: "Saldo" }));
    expect(saldo.getByText(`${R}340,00`)).toBeTruthy();
    expect(saldo.getByText(`${R}900,00`)).toBeTruthy();
    expect(saldo.getByText("Intermediária")).toBeTruthy();
    expect(saldo.getByText("12 matrículas")).toBeTruthy();
    expect(saldo.getByText("2 dias restantes")).toBeTruthy();
    expect(saldo.getByText("12 / 20")).toBeTruthy();
    expect(saldo.getByText("60%")).toBeTruthy();
    expect(saldo.getByText("Faltam 3", { selector: "strong" })).toBeTruthy();
    const msg = saldo.getByText(/para a meta/);
    expect(msg.textContent).toBe("🔥 Faltam 3 para a metaBora!");
    expect(msg.querySelector("br")).toBeTruthy();
    expect(msg.querySelector("span")).toBeNull();
    expect(document.querySelector("img")).toBeNull();
  });

  it("níveis de meta, PIX do dia, ranking e conquistas", async () => {
    abrir();
    await screen.findByRole("region", { name: "Níveis de meta" });
    const niveis = regiao("Níveis de meta");
    expect(niveis.getByText("Atingido")).toBeTruthy();
    expect(niveis.getByText(`${R}360,00`)).toBeTruthy();
    expect(niveis.getByText(`/ ${R}450,00`)).toBeTruthy();
    expect(niveis.getByText("12/15")).toBeTruthy();
    expect(niveis.getByText("80%")).toBeTruthy();

    const pix = regiao("PIX do dia");
    expect(pix.getByText("Faltam 2 para o PIX!")).toBeTruthy();
    expect(pix.getByText(/Hoje: 1\/3 \(mat \+ aceites do dia\)/)).toBeTruthy();
    expect(pix.getByText("🔥 Ontem você fez 2 — bora superar!")).toBeTruthy();
    expect(pix.getByText(`Prêmio: ${R}50,00`)).toBeTruthy();
    expect(pix.getByText("2 aceites na fila total")).toBeTruthy();
    expect(pix.getByText("de 3")).toBeTruthy();

    const rk = regiao("Ranking");
    expect(rk.getByText("3°")).toBeTruthy();
    expect(rk.getByText("de 10")).toBeTruthy();
    expect(rk.getByText("12 mat + 2 aceites")).toBeTruthy();
    expect(rk.getByText("🥉 Top 3! Pódio!")).toBeTruthy();
    expect(rk.getByText(/Quase lá! Só/)).toBeTruthy();
    expect(rk.getByText("📈 Você está 4.0 acima da média! Continue assim!")).toBeTruthy();

    expect(regiao("Conquistas").getByText("Primeira Matrícula")).toBeTruthy();
  });

  it("desbloqueie, momentum, sequência, faixas, financeiro, timeline e campanhas anteriores", async () => {
    abrir();
    await screen.findByRole("region", { name: "Desbloqueie mais" });
    const desb = regiao("Desbloqueie mais");
    expect(desb.getByText(`+${R}210,00`)).toBeTruthy();
    expect(desb.getByText(`+${R}560,00`)).toBeTruthy();
    expect(desb.getAllByText("Próximo")).toHaveLength(1);
    expect(desb.queryByText(`+${R}0,00`)).toBeNull();

    const mom = regiao("Momentum");
    expect(mom.getByText("1.5")).toBeTruthy();
    expect(mom.getByText("0.8")).toBeTruthy();
    expect(mom.getByText("18 mat")).toBeTruthy();
    expect(mom.getByText("Meta")).toBeTruthy();
    expect(mom.getByText(`${R}2.500,50`)).toBeTruthy();

    const seq = regiao("Sequência");
    expect(seq.getByText("3 dias consecutivos batendo a meta diária — Aquecendo!")).toBeTruthy();
    expect(seq.getByTitle(`02/11/2026: 2/2 (1m+1a) · ${R}50,00`)).toBeTruthy();

    const faixas = regiao("Progresso por faixa");
    expect(faixas.queryByText("Base")).toBeNull();
    expect(faixas.getByText(`✅ ${R}240,00`)).toBeTruthy();
    expect(faixas.getByText(`falta 3 · +${R}360,00`)).toBeTruthy();

    const fin = regiao("Resumo financeiro");
    expect(within(fin.getByRole("group", { name: "Bônus tier" })).getByText(`${R}240,00`)).toBeTruthy();
    expect(within(fin.getByRole("group", { name: "PIX diários" })).getByText(`${R}100,00`)).toBeTruthy();
    expect(within(fin.getByRole("group", { name: "Total" })).getByText(`${R}340,00`)).toBeTruthy();

    const tl = graficos.find((g) => g.data.datasets.some((s) => s.label === "Bônus R$"))!;
    expect(tl.data.labels).toEqual(["02/11/2026", "03/11/2026"]);
    expect(tl.data.datasets.map((s) => s.data)).toEqual([[2, 1], [2, 2], [50, 0]]);

    const hist = within(await screen.findByRole("region", { name: "Campanhas anteriores" }));
    expect(hist.getByText("Campanha Outubro")).toBeTruthy();
    expect(hist.queryByText("Campanha Novembro")).toBeNull();
    expect(hist.getByText(`${R}700,00`)).toBeTruthy();
  });

  it("trocar a campanha busca de novo com o id; campanha histórica mostra o aviso", async () => {
    abrir("consultor", {
      "GET /api/minha-performance/insights": (url) =>
        json(url.includes("campanha_id=6") ? { ...INSIGHTS, campanha: { ...INSIGHTS.campanha!, id: 6, nome: "Campanha Outubro", is_active: false } } : INSIGHTS),
    });
    await screen.findByRole("region", { name: "Saldo" });
    expect(screen.queryByText(/Não há campanha vigente/)).toBeNull();
    fireEvent.change(screen.getByLabelText("Campanha"), { target: { value: "6" } });
    await screen.findByText(/Não há campanha vigente/);
    expect(gets("/api/minha-performance/insights").slice(-1)[0]).toBe("/api/minha-performance/insights?kommo_uid=42&campanha_id=6");
    expect(screen.getByText("Histórica")).toBeTruthy();
  });

  it("Atualizar busca de novo só insights e histórico", async () => {
    abrir();
    await screen.findByRole("region", { name: "Saldo" });
    await waitFor(() => expect(gets("/api/minha-performance/historico")).toHaveLength(1));
    fireEvent.click(screen.getByRole("button", { name: /Atualizar/ }));
    await waitFor(() => expect(gets("/api/minha-performance/insights")).toHaveLength(2));
    await waitFor(() => expect(gets("/api/minha-performance/historico")).toHaveLength(2));
    expect(gets("/api/minha-performance/campanhas")).toHaveLength(1);
  });

  it("sem vínculo com o Kommo: aviso e nenhuma busca de insights", async () => {
    abrir("sem_kommo");
    await screen.findByText(/Sua conta ainda não está vinculada ao Kommo/);
    expect(gets("/api/minha-performance/insights")).toEqual([]);
  });

  it("sem campanha: aviso, sem abas", async () => {
    abrir("consultor", { "GET /api/minha-performance/insights": () => json({ ok: true, campanha: null }) });
    await screen.findByText("Nenhuma campanha ativa no momento.");
    expect(screen.queryByRole("button", { name: "Minhas matrículas" })).toBeNull();
  });

  it("erro do servidor aparece na tela", async () => {
    abrir("consultor", { "GET /api/minha-performance/insights": () => json({ ok: false, error: "Banco fora do ar" }, 500) });
    expect((await screen.findByRole("alert")).textContent).toContain("Banco fora do ar");
  });
});

describe("Minha Performance — admin, Premiação e suporte", () => {
  it("admin abre na equipe de suporte com as vendas do Comercial", async () => {
    abrir("admin");
    const painel = within(await screen.findByRole("region", { name: "Suporte comercial — time" }));
    expect(gets("/api/minha-performance/agentes")).toEqual(["/api/minha-performance/agentes"]);
    expect(gets("/api/minha-performance/insights")).toEqual(["/api/minha-performance/insights?suporte_equipe=1&campanha_id=7"]);
    expect(gets("/api/comercial-rgm/data")).toEqual(["/api/comercial-rgm/data?dt_ini=2026-11-01&dt_fim=2026-11-30"]);
    expect(painel.getByText("55/100 (55%)")).toBeTruthy();
    expect(painel.getAllByText("Comercial inteiro").length).toBeGreaterThan(0);
    expect(within(painel.getByRole("group", { name: "Matrículas no período" })).getByText("55")).toBeTruthy();
    expect(within(painel.getByRole("group", { name: "Média de vendas" })).getByText("2.5")).toBeTruthy();
    const serie = graficos.find((g) => g.data.datasets[0]?.label === "Em curso")!;
    expect(serie.data.labels).toEqual(["02/11", "03/11"]);
    expect(serie.data.datasets[0].data).toEqual([5, 6]);
    expect(screen.getByText("Painel: Equipe Suporte Comercial")).toBeTruthy();
    expect(screen.queryByRole("region", { name: "Saldo" })).toBeNull();
    expect(screen.queryByRole("region", { name: "Ranking" })).toBeNull();
    expect(regiao("PIX do dia").getByText("Sem meta diária hoje")).toBeTruthy();
    expect(screen.getByRole("region", { name: "Calendário de resultados" })).toBeTruthy();
  });

  it("sem vendas no período da campanha, cai no período de meta mais recente", async () => {
    abrir("admin", {
      "GET /api/comercial-rgm/data": (url) => json(url.includes("2026-12-01") ? COMERCIAL : { ok: true, kpis: { vendas: 0, media_diaria: 0 }, evolucao: [] }),
      "GET /api/comercial-rgm/metas": () => json({ ok: true, metas: [{ dt_inicio: "2026-12-01", dt_fim: "2026-12-31" }] }),
    });
    await screen.findByText("55/100 (55%)");
    expect(gets("/api/comercial-rgm/data")).toEqual([
      "/api/comercial-rgm/data?dt_ini=2026-11-01&dt_fim=2026-11-30",
      "/api/comercial-rgm/data?dt_ini=2026-12-01&dt_fim=2026-12-31",
    ]);
    expect(gets("/api/comercial-rgm/metas")).toEqual(["/api/comercial-rgm/metas?categoria=matriculas"]);
  });

  it("escolher um consultor e voltar ao painel", async () => {
    abrir("admin");
    await screen.findByRole("region", { name: "Suporte comercial — time" });
    const sel = screen.getByLabelText("Consultor") as HTMLSelectElement;
    expect([...sel.options].map((o) => o.textContent)).toEqual([
      "★ Equipe Suporte Comercial",
      "— Suporte (individual) —",
      "Sara Suporte",
      "— Demais agentes —",
      "Ana",
      "Bruno",
    ]);
    fireEvent.change(sel, { target: { value: "55" } });
    await screen.findByRole("region", { name: "Saldo" });
    expect(gets("/api/minha-performance/insights").slice(-1)[0]).toBe("/api/minha-performance/insights?kommo_uid=55&campanha_id=7");
    expect(screen.getByText("Visualizando: Bruno")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Voltar ao meu painel" }));
    await screen.findByRole("region", { name: "Suporte comercial — time" });
    expect(sel.value).toBe("suporte_equipe");
  });

  it("quem tem Premiação também escolhe; ?uid= abre direto no consultor", async () => {
    abrir("premiacao", {}, "/minha_performance?uid=55");
    await screen.findByRole("region", { name: "Saldo" });
    expect(gets("/api/minha-performance/insights")).toEqual(["/api/minha-performance/insights?kommo_uid=55&campanha_id=7"]);
    expect((screen.getByLabelText("Consultor") as HTMLSelectElement).value).toBe("55");
  });

  it("login do suporte sem Premiação vê a equipe e não chama a lista de agentes", async () => {
    abrir("suporte");
    await screen.findByRole("region", { name: "Suporte comercial — time" });
    expect(gets("/api/minha-performance/insights")).toEqual(["/api/minha-performance/insights?suporte_equipe=1&campanha_id=7"]);
    expect(gets("/api/minha-performance/agentes")).toEqual([]);
    expect(screen.queryByLabelText("Consultor")).toBeNull();
  });
});

describe("Minhas matrículas", () => {
  it("abre com o período de meta mais recente e carrega as três listas", async () => {
    abrir();
    await abrirMatriculas();
    expect(gets("/api/premiacao/campanhas-periodos")).toEqual(["/api/premiacao/campanhas-periodos"]);
    expect(gets("/api/minha-performance/matriculas")).toEqual(["/api/minha-performance/matriculas?kommo_uid=42&dt_ini=2026-11-01&dt_fim=2026-11-30"]);
    await waitFor(() =>
      expect(gets("/api/minha-performance/minhas-matriculas")).toEqual(["/api/minha-performance/minhas-matriculas?kommo_uid=42&dt_ini=2026-11-01&dt_fim=2026-11-30"]),
    );
    expect(gets("/api/minha-performance/ajustes")).toEqual(["/api/minha-performance/ajustes?kommo_uid=42"]);
    expect((screen.getByLabelText("De") as HTMLInputElement).value).toBe("2026-11-01");
  });

  it("contagens, pílulas, busca e marca de fora do padrão", async () => {
    abrir();
    await abrirMatriculas();
    const of = regiao("Matrículas oficiais");
    expect(of.getByText("1 em curso")).toBeTruthy();
    expect(of.getByText("1 fora do padrão")).toBeTruthy();
    expect(of.getByRole("button", { name: /Todos 4/ })).toBeTruthy();
    expect(of.getByRole("button", { name: /Em curso 1/ })).toBeTruthy();
    expect(of.getByRole("button", { name: /Evadidos 1/ })).toBeTruthy();
    expect(of.getByRole("button", { name: /Outros 1/ })).toBeTruthy();
    expect(of.getByText("99 ⚠")).toBeTruthy();
    expect(of.getByText("Não conta")).toBeTruthy();
    expect(of.queryByRole("button", { name: "Contar venda" })).toBeNull();
    // curso vazio mostra o nível
    expect(of.getByText("Pós-Graduação")).toBeTruthy();

    fireEvent.click(of.getByRole("button", { name: /Evadidos 1/ }));
    expect(of.queryByText("Carla Dias")).toBeNull();
    expect(of.getByText("Bruno Alves")).toBeTruthy();
    fireEvent.click(of.getByRole("button", { name: /Todos 4/ }));
    fireEvent.change(of.getByPlaceholderText("Buscar por nome, RGM, curso..."), { target: { value: "LETRAS" } });
    expect(of.getByText("Davi")).toBeTruthy();
    expect(of.queryByText("Carla Dias")).toBeNull();
    fireEvent.change(of.getByPlaceholderText("Buscar por nome, RGM, curso..."), { target: { value: "zzz" } });
    expect(of.getByText("Nenhuma matrícula corresponde aos filtros.")).toBeTruthy();
  });

  it("Filtrar e Limpar buscam oficiais e minha lista (não os ajustes)", async () => {
    abrir();
    await abrirMatriculas();
    await waitFor(() => expect(gets("/api/minha-performance/minhas-matriculas")).toHaveLength(1));
    fireEvent.change(screen.getByLabelText("De"), { target: { value: "2026-11-10" } });
    expect(gets("/api/minha-performance/matriculas")).toHaveLength(1);
    fireEvent.click(screen.getByRole("button", { name: /Filtrar/ }));
    await waitFor(() =>
      expect(gets("/api/minha-performance/matriculas").slice(-1)[0]).toBe("/api/minha-performance/matriculas?kommo_uid=42&dt_ini=2026-11-10&dt_fim=2026-11-30"),
    );
    await waitFor(() =>
      expect(gets("/api/minha-performance/minhas-matriculas").slice(-1)[0]).toBe(
        "/api/minha-performance/minhas-matriculas?kommo_uid=42&dt_ini=2026-11-10&dt_fim=2026-11-30",
      ),
    );
    fireEvent.click(screen.getByRole("button", { name: "Limpar" }));
    await waitFor(() => expect(gets("/api/minha-performance/matriculas").slice(-1)[0]).toBe("/api/minha-performance/matriculas?kommo_uid=42"));
    await waitFor(() => expect(gets("/api/minha-performance/minhas-matriculas").slice(-1)[0]).toBe("/api/minha-performance/minhas-matriculas?kommo_uid=42"));
    expect(gets("/api/minha-performance/ajustes")).toHaveLength(1);
  });

  it("Aplicar meta usa as datas do período escolhido", async () => {
    abrir();
    await abrirMatriculas();
    fireEvent.click(screen.getByRole("button", { name: /Aplicar meta/ }));
    fireEvent.click(await screen.findByRole("button", { name: /Meta Out/ }));
    await waitFor(() =>
      expect(gets("/api/minha-performance/matriculas").slice(-1)[0]).toBe("/api/minha-performance/matriculas?kommo_uid=42&dt_ini=2026-10-01&dt_fim=2026-10-31"),
    );
    expect((screen.getByLabelText("Até") as HTMLInputElement).value).toBe("2026-10-31");
  });

  it("admin conta a venda fora do padrão", async () => {
    abrir("admin");
    await screen.findByRole("region", { name: "Suporte comercial — time" });
    fireEvent.change(screen.getByLabelText("Consultor"), { target: { value: "55" } });
    await screen.findByRole("region", { name: "Saldo" });
    await abrirMatriculas();
    expect(gets("/api/minha-performance/matriculas")[0]).toContain("kommo_uid=55");
    fireEvent.click(screen.getByRole("button", { name: "Contar venda" }));
    await waitFor(() => expect(chamadas.some((c) => c.metodo === "POST" && c.url === "/api/comercial-rgm/outlier/contar-venda")).toBe(true));
    const post = chamadas.find((c) => c.metodo === "POST" && c.url === "/api/comercial-rgm/outlier/contar-venda")!;
    expect(JSON.parse(post.corpo!)).toEqual({ rgm: "99" });
    await waitFor(() => expect(gets("/api/minha-performance/matriculas")).toHaveLength(2));
  });

  it("minha lista: total, adicionar pelas oficiais com validações e POST", async () => {
    abrir();
    await abrirMatriculas();
    const lista = regiao("Minha lista de vendas");
    await lista.findByText("1 no período");
    expect(lista.getByText("18420")).toBeTruthy();
    fireEvent.click(lista.getByRole("button", { name: /Adicionar/ }));
    const dlg = within(screen.getByRole("dialog", { name: "Adicionar matrícula" }));
    const nomes = dlg.getAllByRole("button", { name: /RGM/ }).map((b) => b.querySelector("div")?.textContent);
    expect(nomes).toEqual(["Ana Lima", "Bruno Alves", "Carla Dias", "Davi"]);
    fireEvent.click(dlg.getByRole("button", { name: /Carla Dias/ }));
    expect((dlg.getByLabelText("Nome do aluno") as HTMLInputElement).value).toBe("Carla Dias");
    expect((dlg.getByLabelText("Data matrícula") as HTMLInputElement).value).toBe("05/11/2026");
    expect((dlg.getByLabelText("Ciclo") as HTMLInputElement).value).toBe("2026/2");
    expect((dlg.getByLabelText("Observação") as HTMLTextAreaElement).value).toBe("Importado das matrículas oficiais");
    fireEvent.click(dlg.getByRole("button", { name: "Salvar" }));
    expect(dlg.getByRole("alert").textContent).toBe("Preencha o campo obrigatório: Nº do negócio.");
    fireEvent.change(dlg.getByLabelText("Nº do negócio"), { target: { value: "999" } });
    fireEvent.click(dlg.getByRole("button", { name: "Salvar" }));
    expect(dlg.getByRole("alert").textContent).toBe(
      "RGM 11111111 já está na sua lista de vendas. Não é permitido cadastrar o mesmo RGM duas vezes.",
    );
    fireEvent.change(dlg.getByLabelText("RGM"), { target: { value: "123.456-78" } });
    fireEvent.click(dlg.getByRole("button", { name: "Salvar" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    const post = chamadas.find((c) => c.metodo === "POST" && c.url === "/api/minha-performance/minhas-matriculas")!;
    expect(JSON.parse(post.corpo!)).toEqual({
      nome: "Carla Dias",
      rgm: "12345678",
      curso: "Direito",
      polo: "Centro",
      data_matricula: "05/11/2026",
      ciclo: "2026/2",
      nivel: "Graduação",
      kommo_lead_id: "999",
      observacao: "Importado das matrículas oficiais",
    });
    await screen.findByText("Matrícula adicionada à sua lista!");
    await waitFor(() => expect(gets("/api/minha-performance/minhas-matriculas")).toHaveLength(2));
  });

  it("erro do servidor ao salvar aparece no formulário", async () => {
    abrir("consultor", { "POST /api/minha-performance/minhas-matriculas": () => json({ ok: false, error: "Data inválida" }, 400) });
    await abrirMatriculas();
    fireEvent.click(regiao("Minha lista de vendas").getByRole("button", { name: /Adicionar/ }));
    const dlg = within(screen.getByRole("dialog", { name: "Adicionar matrícula" }));
    fireEvent.click(dlg.getByRole("button", { name: "Preencher manual" }));
    for (const [rotulo, valor] of [
      ["Nome do aluno", "Zé"],
      ["RGM", "55556666"],
      ["Curso", "X"],
      ["Polo", "Y"],
      ["Data matrícula", "99/99/9999"],
      ["Ciclo", "2026/2"],
      ["Nível", "Graduação"],
      ["Nº do negócio", "1"],
    ]) {
      fireEvent.change(dlg.getByLabelText(rotulo), { target: { value: valor } });
    }
    fireEvent.click(dlg.getByRole("button", { name: "Salvar" }));
    expect((await dlg.findByRole("alert")).textContent).toBe("Erro ao salvar: Data inválida");
  });

  it("sync Bwipo: valida, mostra a escolha quando há vários negócios e preenche", async () => {
    abrir("consultor", {
      "POST /api/minha-performance/sync-bwipo-prefill": (_u, init) => {
        const b = JSON.parse(String(init?.body));
        if (b.rgm) return json({ ok: false, error: "Vários negócios com esse RGM. Escolha o número:", deals: [{ number: 10, etapa: "Aceite" }, { number: 11, etapa: "" }] }, 409);
        return json({ ok: true, msg: "Negócio #11 · Ganho.", prefill: { nome: "Lia", rgm: "77778888", curso: "Pedagogia", polo: "Leste", data_matricula: "01/11/2026", ciclo: "2026/2", nivel: "Graduação", kommo_lead_id: "11" } });
      },
    });
    await abrirMatriculas();
    fireEvent.click(regiao("Minha lista de vendas").getByRole("button", { name: /Adicionar/ }));
    const dlg = within(screen.getByRole("dialog", { name: "Adicionar matrícula" }));
    fireEvent.click(dlg.getByRole("button", { name: "Sync Bwipo" }));
    fireEvent.click(dlg.getByRole("button", { name: "Buscar e preencher" }));
    expect(dlg.getByText("Informe o número do negócio ou um RGM com 8 dígitos.")).toBeTruthy();
    fireEvent.change(dlg.getByLabelText("RGM no Bwipo"), { target: { value: "7777-8888" } });
    fireEvent.click(dlg.getByRole("button", { name: "Buscar e preencher" }));
    fireEvent.click(await dlg.findByRole("button", { name: "#11" }));
    await dlg.findByText("Negócio #11 · Ganho. RGM 77778888.");
    const posts = chamadas.filter((c) => c.url === "/api/minha-performance/sync-bwipo-prefill").map((c) => JSON.parse(c.corpo!));
    expect(posts).toEqual([{ rgm: "77778888" }, { number: 11 }]);
    expect((dlg.getByLabelText("Nome do aluno") as HTMLInputElement).value).toBe("Lia");
    expect((dlg.getByLabelText("Nº do negócio") as HTMLInputElement).value).toBe("11");
    expect((dlg.getByLabelText("Observação") as HTMLTextAreaElement).value).toBe("Preenchido via sync Bwipo");
  });

  it("editar faz PUT; excluir pede confirmação e faz DELETE", async () => {
    abrir();
    await abrirMatriculas();
    const lista = regiao("Minha lista de vendas");
    await lista.findByText("1 no período");
    fireEvent.click(lista.getByRole("button", { name: "Editar" }));
    const dlg = within(screen.getByRole("dialog", { name: "Editar matrícula" }));
    expect(dlg.queryByRole("button", { name: "Sync Bwipo" })).toBeNull();
    expect((dlg.getByLabelText("Data matrícula") as HTMLInputElement).value).toBe("05/11/2026");
    fireEvent.change(dlg.getByLabelText("Polo"), { target: { value: "Polo Novo" } });
    fireEvent.click(dlg.getByRole("button", { name: "Salvar" }));
    await waitFor(() => expect(chamadas.some((c) => c.metodo === "PUT")).toBe(true));
    const put = chamadas.find((c) => c.metodo === "PUT")!;
    expect(put.url).toBe("/api/minha-performance/minhas-matriculas/5");
    expect(JSON.parse(put.corpo!).polo).toBe("Polo Novo");

    const confirmar = vi.spyOn(window, "confirm").mockReturnValueOnce(false).mockReturnValueOnce(true);
    fireEvent.click(lista.getByRole("button", { name: "Excluir" }));
    expect(chamadas.some((c) => c.metodo === "DELETE")).toBe(false);
    fireEvent.click(lista.getByRole("button", { name: "Excluir" }));
    expect(confirmar).toHaveBeenLastCalledWith("Excluir esta matrícula da sua lista?");
    await waitFor(() => expect(chamadas.find((c) => c.metodo === "DELETE")?.url).toBe("/api/minha-performance/minhas-matriculas/5"));
  });

  it("solicitações de ajuste: lista, validação e envio", async () => {
    abrir();
    await abrirMatriculas();
    const aj = regiao("Solicitações de ajuste");
    await aj.findByText("Em análise");
    expect(aj.getByText("Dados incorretos")).toBeTruthy();
    expect(aj.getByText("10/11/2026")).toBeTruthy();
    expect(aj.getByText(/Vamos ver/)).toBeTruthy();
    fireEvent.click(aj.getByRole("button", { name: /Nova solicitação/ }));
    const dlg = within(screen.getByRole("dialog", { name: "Nova solicitação de ajuste" }));
    fireEvent.click(dlg.getByRole("button", { name: "Enviar solicitação" }));
    expect(dlg.getByRole("alert").textContent).toBe("Lead Kommo ID é obrigatório");
    fireEvent.change(dlg.getByLabelText("Lead Kommo ID"), { target: { value: " 123 " } });
    fireEvent.click(dlg.getByRole("button", { name: "Enviar solicitação" }));
    expect(dlg.getByRole("alert").textContent).toBe("Justificativa é obrigatória");
    fireEvent.change(dlg.getByLabelText("Tipo"), { target: { value: "evasao_indevida" } });
    fireEvent.change(dlg.getByLabelText("Nome do aluno"), { target: { value: "Rui" } });
    fireEvent.change(dlg.getByLabelText("Justificativa"), { target: { value: " Saiu errado " } });
    fireEvent.click(dlg.getByRole("button", { name: "Enviar solicitação" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    const post = chamadas.find((c) => c.metodo === "POST" && c.url === "/api/minha-performance/ajustes")!;
    expect(JSON.parse(post.corpo!)).toEqual({
      tipo: "evasao_indevida",
      nome_aluno: "Rui",
      rgm: "",
      curso: "",
      polo: "",
      data_matricula: null,
      kommo_lead_id: "123",
      descricao: "Saiu errado",
    });
    await waitFor(() => expect(gets("/api/minha-performance/ajustes")).toHaveLength(2));
  });

  it("erro ao carregar a lista mostra a mensagem do servidor", async () => {
    abrir("consultor", { "GET /api/minha-performance/minhas-matriculas": () => json({ ok: false, error: "Tabela sumiu" }, 500) });
    await abrirMatriculas();
    expect(await regiao("Minha lista de vendas").findByText("Tabela sumiu")).toBeTruthy();
  });
});
