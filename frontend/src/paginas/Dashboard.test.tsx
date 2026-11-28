import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { App, FUTURO_ROUTER } from "../App";
import { navegador } from "../api";
import type { Me } from "../me";
import { montarMenu } from "../menu";
import { NOMES_TELAS_NOVAS } from "../telas";
import { ESPERA_FUNIL } from "./dashboard/Funil";
import type { Alunos, Ciclos, Evolucao, Fonte, Funil, SaudeFinanceira } from "./dashboard/tipos";

vi.mock("../rastreio", () => ({ registrarAcesso: vi.fn() }));

// chart.js não desenha no jsdom: um Chart falso guarda a configuração de cada gráfico.
const graficos = vi.hoisted(() => [] as { config: any; destruido: boolean }[]);
vi.mock("chart.js", () => {
  class Chart {
    static register() {}
    config: any;
    destruido = false;
    constructor(_ctx: unknown, config: any) {
      this.config = config;
      graficos.push(this);
    }
    destroy() {
      this.destruido = true;
    }
  }
  const nada = {};
  return {
    Chart,
    LineController: nada,
    LineElement: nada,
    PointElement: nada,
    CategoryScale: nada,
    LinearScale: nada,
    Tooltip: nada,
  };
});

const CICLOS_DISTINTOS = [
  { nome: "2026/2", total: 27000, matriculados: 27000, rematricula: 100 },
  { nome: "2026/1", total: 30000, matriculados: 30000, rematricula: 200 },
];

const ALUNOS: Alunos = {
  totals: { novos: 600, regresso: 100, recompra: 50, rematricula: 250, outros: 0 },
  by_situacao: { "EM CURSO": 800, CANCELADO: 150, Transferido: 50 },
  by_nivel: { Graduação: 700, "Pós-Graduação": 300 },
  by_polo: { "Polo Barra Funda": 600, "Barra Funda": 100, "Vila Mariana": 300 },
  by_turma: { "T-2026": 1000 },
  by_ciclo: { "2026/2": 1000 },
  grand_total: 1000,
  inbound_transfers: 42,
  card_filters: { novos: "Regra dos novos.", rematricula: "Regra das rematrículas." },
};

const EVOLUCAO: Evolucao = {
  periods: ["2026-05", "2026-06"],
  series: { novos: [10, 20], rematricula: [5, 5], regresso: [1, 2], recompra: [0, 3], total: [16, 30] },
  range: { from: "2026-05-01", to: "2026-06-30" },
};

const SAUDE: SaudeFinanceira = {
  ok: true,
  has_data: true,
  snapshot: { uploaded_at: "27/11/2026 10:00" },
  total_alunos: 2000,
  adimplentes: 1500,
  inadimplentes: 500,
  pct_inadimplencia: 25,
};

const CICLOS: Ciclos = {
  ciclos: [
    {
      nome: "2026/2",
      nivel: "Graduação",
      totals: { novos: 80, rematricula: 40, regresso: 5, recompra: 5 },
      by_situacao: { "EM CURSO": 120, CANCELADO: 10 },
      by_polo: { "Polo Barra Funda": 100, Morumbi: 30 },
      grand_total: 130,
    },
    {
      nome: "2026/1",
      nivel: "Graduação",
      totals: { novos: 50, rematricula: 40, regresso: 5, recompra: 5 },
      by_situacao: { "EM CURSO": 100 },
      by_polo: { Morumbi: 100 },
      grand_total: 100,
    },
  ],
  config: [{ nome: "2026/2", nivel: "Graduação" }],
  distinct_nivels: { Graduação: 900, "Pós-Graduação": 1234 },
  comparisons: {
    ytd: { label: "YTD 2026", period: "2026-01-01 → 2026-11-28", current: { grand_total: 1100, totals: { novos: 700 } } },
    ytd_prev: { label: "YTD 2025", period: "2025-01-01 → 2025-11-28", current: { grand_total: 1000, totals: {} } },
    m6: { label: "Últimos 6 meses", period: "x", current: { grand_total: 500, totals: {} } },
    m6_prev: { label: "6 meses anteriores", period: "y", current: { grand_total: 0, totals: {} } },
  },
};

const FONTE: Fonte = {
  snapshot: { id: "1", filename: "matriculados.xlsx", row_count: 12345, uploaded_at: "27/11/2026 08:00" },
  sync_running: false,
  update_running: false,
};

const FUNIL: Funil = {
  ok: true,
  data: {
    total: 5000,
    new_today: 200,
    source: "live",
    fetched_at: "10:30",
    stale: false,
    stages: [
      { key: "sem_resposta", label: "Sem Resposta", count: 3000, pct: 60, delta: 5, delta_pct: 1.2, highlight: true },
      { key: "aceite", label: "Aceite", count: 50, pct: 1, delta: 3, highlight: true },
      { key: "perdido", label: "Perdido", count: 900, pct: 18 },
    ],
    yesterday_summary: { vendas: 7, leads: 100, leads_prev: 80, leads_delta_pct: 25 },
  },
};

function me(parcial: Partial<Me> = {}, paginas: string[] = ["dashboard", "inadimplencia", "config"]): Me {
  return {
    user_id: 1,
    username: "ana.silva@x.com",
    role: "viewer",
    categoria: null,
    display_name: "Ana",
    nav: {
      is_admin: false,
      is_comercial: false,
      is_suporte_comercial: false,
      pagina_inicial: "dashboard",
      paginas,
      secoes: {},
      grupos: {},
    },
    ...parcial,
  };
}

function json(corpo: unknown, status = 200) {
  return new Response(JSON.stringify(corpo), { status, headers: { "Content-Type": "application/json" } });
}

type Rota = (url: string) => Response | Promise<Response>;
let chamadas: string[];

const PADRAO: Record<string, Rota> = {
  "/api/dashboard/ciclos-distinct": () => json(CICLOS_DISTINTOS),
  "/api/dashboard/students": () => json(ALUNOS),
  "/api/dashboard/timeline": () => json(EVOLUCAO),
  "/api/dashboard/ciclos": () => json(CICLOS),
  "/api/dashboard/inbound-transfers": () =>
    json({ ok: true, alunos: [{ rgm: "123", nome: "Bia", polo: "Morumbi", tipo: "Calouro", ciclo: "2026/2" }] }),
  "/api/dashboard/funnel-yesterday": () => json({ ok: true, data: { vendas: 1, leads: 1 } }),
  "/api/dashboard": () => json(FONTE),
  "/api/lista-alunos/latest": () => json(SAUDE),
  "/api/kommo/funnel-live": () => json(FUNIL),
  "/api/kommo/new-leads-today": () => json({ ok: true, count: 200 }),
  "/api/kommo/yesterday-summary": () => json({ ok: true, data: { vendas: 9, leads: 120, leads_prev: 100, leads_delta_pct: 20 } }),
};

function abrir(opcoes: { me?: Me; rotas?: Record<string, Rota> } = {}) {
  chamadas = [];
  const rotas = { ...PADRAO, ...opcoes.rotas };
  // Prefixos mais longos primeiro (/api/dashboard/students antes de /api/dashboard).
  const chaves = Object.keys(rotas).sort((a, b) => b.length - a.length);
  vi.spyOn(globalThis, "fetch").mockImplementation(async (entrada) => {
    const url = String(entrada);
    chamadas.push(url);
    if (url === "/api/me") return json(opcoes.me ?? me());
    if (url === "/api/me/fixadas") return json({ fixadas: [], mais_usadas: [] });
    if (url === "/api/avisos/nao-lidos") return json({ count: 0 });
    const caminho = url.split("?")[0];
    const chave = chaves.find((k) => caminho === k);
    if (chave) return rotas[chave](url);
    return json({}, 404);
  });
  const cliente = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={cliente}>
      <MemoryRouter initialEntries={["/dashboard"]} future={FUTURO_ROUTER}>
        <App />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

const de = (prefixo: string) => chamadas.filter((u) => u.split("?")[0] === prefixo);
const alunos = () => de("/api/dashboard/students");
const evolucao = () => de("/api/dashboard/timeline");
const saude = () => de("/api/lista-alunos/latest");
const ciclos = () => de("/api/dashboard/ciclos");
const grupo = (nome: string) => within(screen.getByRole("group", { name: nome }));

async function aberto() {
  await screen.findByRole("button", { name: /^Novos/ });
  await waitFor(() => expect(ciclos().length).toBeGreaterThan(0));
}

/** Último gráfico ainda vivo. */
function grafico() {
  const vivos = graficos.filter((g) => !g.destruido);
  return vivos[vivos.length - 1];
}

beforeEach(() => {
  graficos.length = 0;
  localStorage.clear();
  ESPERA_FUNIL.primeira = 1;
  ESPERA_FUNIL.demais = 1;
});

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("Dashboard: abertura", () => {
  it("usa o ciclo mais recente e busca cada bloco com os mesmos parâmetros da antiga", async () => {
    abrir();
    await aberto();
    expect(chamadas).toContain("/api/dashboard/ciclos-distinct");
    expect(alunos()).toEqual(["/api/dashboard/students?ciclo=2026%2F2"]);
    expect(evolucao()).toEqual(["/api/dashboard/timeline?granularity=month&ciclo=2026%2F2"]);
    expect(saude()).toEqual(["/api/lista-alunos/latest?ciclo=2026%2F2"]);
    expect(ciclos()).toEqual(["/api/dashboard/ciclos"]);
    expect((screen.getByRole("combobox", { name: "Ciclo" }) as HTMLSelectElement).value).toBe("2026/2");
    expect(screen.getByRole("option", { name: "2026/2 (27.000)" })).toBeTruthy();
    expect(localStorage.getItem("dash_ciclo_v2")).toBe("2026/2");
  });

  it("alunos: uma chamada só (rota pesada, com cache no servidor)", async () => {
    abrir();
    await aberto();
    await screen.findByText("Visualização do funil");
    expect(alunos()).toHaveLength(1);
  });

  it("um bloco por vez: a evolução espera os alunos, a saúde espera a evolução", async () => {
    let soltar!: () => void;
    abrir({
      rotas: {
        "/api/dashboard/students": () => new Promise<Response>((r) => (soltar = () => r(json(ALUNOS)))),
      },
    });
    await waitFor(() => expect(alunos()).toHaveLength(1));
    await new Promise((r) => setTimeout(r, 20));
    expect(evolucao()).toHaveLength(0);
    expect(saude()).toHaveLength(0);
    expect(ciclos()).toHaveLength(0);
    await act(async () => soltar());
    await waitFor(() => expect(ciclos()).toHaveLength(1));
    expect(evolucao()).toHaveLength(1);
    expect(saude()).toHaveLength(1);
  });

  it("ciclo salvo no navegador vale se ainda existe", async () => {
    localStorage.setItem("dash_ciclo_v2", "2026/1");
    abrir();
    await aberto();
    expect(alunos()).toEqual(["/api/dashboard/students?ciclo=2026%2F1"]);
  });

  it("ciclo salvo que sumiu cai no mais recente", async () => {
    localStorage.setItem("dash_ciclo_v2", "2019/1");
    abrir();
    await aberto();
    expect(alunos()).toEqual(["/api/dashboard/students?ciclo=2026%2F2"]);
  });

  it("fonte de dados: arquivo, registros e status", async () => {
    abrir();
    expect(await screen.findByText("matriculados.xlsx — 12.345 registros (27/11/2026 08:00)")).toBeTruthy();
    expect(within(screen.getByRole("region", { name: "Fonte de dados" })).getByText("Conectado")).toBeTruthy();
  });

  it("fonte de dados sem snapshot e com sync rodando", async () => {
    abrir({ rotas: { "/api/dashboard": () => json({ snapshot: null, sync_running: true }) } });
    expect(await screen.findByText("Nenhum snapshot de matriculados carregado")).toBeTruthy();
    expect(screen.getByText("Sync...")).toBeTruthy();
  });
});

describe("Dashboard: cartões de tipo", () => {
  it("total, novos (calouros+regresso+recompra) e rematrículas com percentuais", async () => {
    abrir();
    await aberto();
    expect(grupo("Total").getByText("1.000")).toBeTruthy();
    const novos = screen.getByRole("button", { name: /^Novos/ });
    expect(within(novos).getByText("750")).toBeTruthy();
    expect(within(novos).getByText("75%")).toBeTruthy();
    expect(within(screen.getByRole("button", { name: /^Calouros/ })).getByText("600")).toBeTruthy();
    expect(within(screen.getByRole("button", { name: /^Regresso/ })).getByText("100")).toBeTruthy();
    expect(within(screen.getByRole("button", { name: /^Recompra/ })).getByText("50")).toBeTruthy();
    const remat = screen.getByRole("button", { name: /^Rematrículas/ });
    expect(within(remat).getByText("250")).toBeTruthy();
    expect(within(remat).getByText("25%")).toBeTruthy();
  });

  it("! mostra a regra do cartão com os filtros ativos", async () => {
    abrir();
    await aberto();
    fireEvent.click(screen.getByRole("button", { name: "Regra do cartão Novos" }));
    expect(screen.getByText("Regra dos novos. Filtros ativos: ciclo 2026/2.")).toBeTruthy();
  });

  it("clicar em Rematrículas filtra alunos, evolução e saúde (não os ciclos)", async () => {
    abrir();
    await aberto();
    fireEvent.click(screen.getByRole("button", { name: /^Rematrículas/ }));
    await waitFor(() => expect(saude()).toHaveLength(2));
    expect(alunos()[1]).toBe("/api/dashboard/students?ciclo=2026%2F2&tipo=rematricula");
    expect(evolucao()[1]).toBe("/api/dashboard/timeline?granularity=month&ciclo=2026%2F2&tipo=rematricula");
    expect(saude()[1]).toBe("/api/lista-alunos/latest?tipo=rematricula&ciclo=2026%2F2");
    expect(ciclos()).toHaveLength(1);
    expect(screen.getByText("Filtrando por: Tipo: Rematrículas")).toBeTruthy();
    expect(screen.getByRole("button", { name: /^Rematrículas/ }).getAttribute("aria-pressed")).toBe("true");
  });

  it("clicar de novo no mesmo tipo tira o filtro; Limpar da barra limpa tipo, situação e polo", async () => {
    abrir();
    await aberto();
    fireEvent.click(screen.getByRole("button", { name: /^Calouros/ }));
    await waitFor(() => expect(alunos()).toHaveLength(2));
    expect(alunos()[1]).toBe("/api/dashboard/students?ciclo=2026%2F2&tipo=novos");
    fireEvent.click(screen.getByRole("button", { name: /^Calouros/ }));
    await waitFor(() => expect(screen.queryByText(/Filtrando por/)).toBeNull());
    fireEvent.click(screen.getByRole("button", { name: /^Regresso/ }));
    await screen.findByText("Filtrando por: Tipo: Regresso");
    fireEvent.click(screen.getByRole("button", { name: "Limpar filtros dos cartões" }));
    await waitFor(() => expect(screen.queryByText(/Filtrando por/)).toBeNull());
  });

  it("Pós-Graduação troca Rematrículas por Veteranos", async () => {
    abrir();
    await aberto();
    fireEvent.change(screen.getByRole("combobox", { name: "Nível" }), { target: { value: "Pós-Graduação" } });
    expect(await screen.findByRole("button", { name: /^Veteranos/ })).toBeTruthy();
    await waitFor(() => expect(ciclos()).toEqual(["/api/dashboard/ciclos", "/api/dashboard/ciclos?nivel=P%C3%B3s-Gradua%C3%A7%C3%A3o"]));
    expect(alunos()[1]).toBe("/api/dashboard/students?ciclo=2026%2F2&nivel=P%C3%B3s-Gradua%C3%A7%C3%A3o");
  });

  it("erro dos alunos aparece com a mensagem do servidor", async () => {
    abrir({ rotas: { "/api/dashboard/students": () => json({ error: "banco fora" }, 500) } });
    expect(await screen.findByText("Erro ao carregar: banco fora")).toBeTruthy();
  });

  it("403 nos alunos mostra o aviso de permissão", async () => {
    abrir({ rotas: { "/api/dashboard/students": () => json({ error: "x" }, 403) } });
    expect(await screen.findByText("Erro ao carregar: Você não tem permissão para esta tela.")).toBeTruthy();
  });
});

describe("Dashboard: filtros", () => {
  it("trocar o ciclo busca de novo e guarda no navegador; Todos os ciclos apaga", async () => {
    abrir();
    await aberto();
    fireEvent.change(screen.getByRole("combobox", { name: "Ciclo" }), { target: { value: "2026/1" } });
    await waitFor(() => expect(alunos()).toHaveLength(2));
    expect(alunos()[1]).toBe("/api/dashboard/students?ciclo=2026%2F1");
    expect(localStorage.getItem("dash_ciclo_v2")).toBe("2026/1");
    fireEvent.change(screen.getByRole("combobox", { name: "Ciclo" }), { target: { value: "" } });
    await waitFor(() => expect(alunos()).toHaveLength(3));
    expect(alunos()[2]).toBe("/api/dashboard/students?");
    expect(localStorage.getItem("dash_ciclo_v2")).toBeNull();
  });

  it("datas só valem no Aplicar", async () => {
    abrir();
    await aberto();
    fireEvent.click(screen.getByRole("button", { name: "Filtros" }));
    fireEvent.change(screen.getByLabelText("De"), { target: { value: "2026-03-01" } });
    fireEvent.change(screen.getByLabelText("Até"), { target: { value: "2026-03-31" } });
    await new Promise((r) => setTimeout(r, 20));
    expect(alunos()).toHaveLength(1);
    fireEvent.click(screen.getByRole("button", { name: "Aplicar" }));
    await waitFor(() => expect(evolucao()).toHaveLength(2));
    expect(alunos()[1]).toBe("/api/dashboard/students?ciclo=2026%2F2&from=2026-03-01&to=2026-03-31");
    expect(evolucao()[1]).toBe("/api/dashboard/timeline?granularity=month&ciclo=2026%2F2&from=2026-03-01&to=2026-03-31");
    // saúde financeira não usa período (como a antiga): mesma chave, sem nova chamada
    await new Promise((r) => setTimeout(r, 20));
    expect(saude()).toEqual(["/api/lista-alunos/latest?ciclo=2026%2F2"]);
  });

  it("situação do painel de filtros e do cabeçalho são o mesmo filtro", async () => {
    abrir();
    await aberto();
    fireEvent.click(screen.getByRole("button", { name: "Filtros" }));
    fireEvent.change(screen.getByRole("combobox", { name: "Situação" }), { target: { value: "Cancelado" } });
    await waitFor(() => expect(alunos()).toHaveLength(2));
    expect(alunos()[1]).toBe("/api/dashboard/students?ciclo=2026%2F2&situacao=Cancelado");
    expect((screen.getByRole("combobox", { name: "Filtrar por situação" }) as HTMLSelectElement).value).toBe("Cancelado");
    expect(screen.getByText("Filtro: Cancelado")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Limpar situação" }));
    await waitFor(() => expect(alunos()).toHaveLength(3));
    expect(alunos()[2]).toBe("/api/dashboard/students?ciclo=2026%2F2");
  });

  it("Limpar volta ao ciclo mais recente e zera os outros filtros", async () => {
    localStorage.setItem("dash_ciclo_v2", "2026/1");
    abrir();
    await aberto();
    fireEvent.change(screen.getByRole("combobox", { name: "Nível" }), { target: { value: "Graduação" } });
    await waitFor(() => expect(alunos()).toHaveLength(2));
    fireEvent.click(screen.getByRole("button", { name: "Limpar" }));
    await waitFor(() => expect(alunos()).toHaveLength(3));
    expect(alunos()[2]).toBe("/api/dashboard/students?ciclo=2026%2F2");
    expect(localStorage.getItem("dash_ciclo_v2")).toBe("2026/2");
    expect((screen.getByRole("combobox", { name: "Nível" }) as HTMLSelectElement).value).toBe("");
  });

  it("ciclos-distinct falhando: segue com Todos os ciclos", async () => {
    abrir({ rotas: { "/api/dashboard/ciclos-distinct": () => json({ error: "x" }, 500) } });
    await aberto();
    expect(alunos()).toEqual(["/api/dashboard/students?"]);
  });
});

describe("Dashboard: situação, polo e nível", () => {
  it("cartões de situação na ordem da antiga, com percentual; clique filtra", async () => {
    abrir();
    await aberto();
    const secao = within(screen.getByRole("region", { name: "Matrículas por situação" }));
    const botoes = secao.getAllByRole("button", { pressed: false }).filter((b) => b.dataset.situacao);
    expect(botoes.map((b) => b.dataset.situacao)).toEqual(["EM CURSO", "CANCELADO", "Transferido"]);
    expect(within(botoes[0]).getByText("80%")).toBeTruthy();
    expect(within(botoes[0]).getByText("800")).toBeTruthy();
    fireEvent.click(botoes[1]);
    await waitFor(() => expect(alunos()).toHaveLength(2));
    expect(alunos()[1]).toBe("/api/dashboard/students?ciclo=2026%2F2&situacao=CANCELADO");
    expect(ciclos()).toHaveLength(1);
  });

  it("transferências recebidas: lista sem o filtro de situação", async () => {
    abrir();
    await aberto();
    fireEvent.click(screen.getByRole("button", { name: /42 transferências de outro polo/ }));
    const painel = within(await screen.findByRole("region", { name: "Transferências recebidas de outro polo" }));
    expect(await painel.findByText("Bia")).toBeTruthy();
    expect(painel.getByText("1 aluno(s)")).toBeTruthy();
    expect(de("/api/dashboard/inbound-transfers")).toEqual(["/api/dashboard/inbound-transfers?ciclo=2026%2F2"]);
    fireEvent.click(painel.getByRole("button", { name: "Fechar" }));
    expect(screen.queryByRole("region", { name: "Transferências recebidas de outro polo" })).toBeNull();
  });

  it("transferências: vazio e erro", async () => {
    abrir({ rotas: { "/api/dashboard/inbound-transfers": () => json({ ok: true, alunos: [] }) } });
    await aberto();
    fireEvent.click(screen.getByRole("button", { name: /transferências de outro polo/ }));
    expect(await screen.findByText("Nenhuma transferência inbound neste recorte.")).toBeTruthy();
    cleanup();
    vi.restoreAllMocks();
    abrir({ rotas: { "/api/dashboard/inbound-transfers": () => json({ ok: false, error: "falhou" }, 500) } });
    await aberto();
    fireEvent.click(screen.getByRole("button", { name: /transferências de outro polo/ }));
    expect(await screen.findByText("falhou")).toBeTruthy();
  });

  it("polos juntados pelo nome canônico, em ranking; clique filtra pelo polo", async () => {
    abrir();
    await aberto();
    const tabela = within(screen.getByRole("table", { name: "Matrículas por polo" }));
    const linhas = tabela.getAllByRole("row").slice(1);
    expect(linhas.map((l) => l.textContent)).toEqual(["1Barra Funda700", "2Vila Mariana300"]);
    fireEvent.click(tabela.getByRole("button", { name: "Vila Mariana" }));
    await waitFor(() => expect(alunos()).toHaveLength(2));
    expect(alunos()[1]).toBe("/api/dashboard/students?ciclo=2026%2F2&polo=Vila+Mariana");
    await waitFor(() => expect(saude()).toContain("/api/lista-alunos/latest?polo=Vila+Mariana&ciclo=2026%2F2"));
  });

  it("nível: Graduação clicável muda o filtro de nível", async () => {
    abrir();
    await aberto();
    const secao = within(screen.getByRole("region", { name: "Por nível" }));
    expect(secao.getByText("700 (70%)")).toBeTruthy();
    fireEvent.click(secao.getByRole("button", { name: /^Graduação/ }));
    await waitFor(() => expect(alunos()).toHaveLength(2));
    expect(alunos()[1]).toBe("/api/dashboard/students?ciclo=2026%2F2&nivel=Gradua%C3%A7%C3%A3o");
    expect((screen.getByRole("combobox", { name: "Nível" }) as HTMLSelectElement).value).toBe("Graduação");
  });

  it("mais detalhes mostra turma e ciclo", async () => {
    abrir();
    await aberto();
    fireEvent.click(screen.getByRole("button", { name: /Mais detalhes/ }));
    const secao = within(screen.getByRole("region", { name: "Por turma" }));
    expect(secao.getByText("T-2026")).toBeTruthy();
    expect(secao.getByText("1.000 (100%)")).toBeTruthy();
  });
});

describe("Dashboard: saúde financeira", () => {
  it("quatro cartões com os números e link para Inadimplência", async () => {
    abrir();
    await aberto();
    const secao = within(await screen.findByRole("region", { name: "Saúde financeira" }));
    expect(within(secao.getByRole("link", { name: /Total alunos/ })).getByText("2.000")).toBeTruthy();
    const adim = secao.getByRole("link", { name: /Adimplentes/ });
    expect(within(adim).getByText("1.500")).toBeTruthy();
    expect(within(adim).getByText("75,0%")).toBeTruthy();
    expect(secao.getAllByText("25,0%").length).toBeGreaterThan(0);
    expect(adim.getAttribute("href")).toBe("/#inadimplencia");
    expect(secao.getByText("27/11/2026 10:00")).toBeTruthy();
  });

  it("sem dados: a seção não aparece", async () => {
    abrir({ rotas: { "/api/lista-alunos/latest": () => json({ ok: true, has_data: false }) } });
    await aberto();
    await waitFor(() => expect(saude()).toHaveLength(1));
    expect(screen.queryByRole("region", { name: "Saúde financeira" })).toBeNull();
  });
});

describe("Dashboard: evolução de matrículas", () => {
  it("agregado: calouros somados, rematrículas e total; detalhado separa", async () => {
    abrir();
    await aberto();
    await waitFor(() => expect(grafico()).toBeTruthy());
    expect(grafico().config.data.labels).toEqual(["mai 2026", "jun 2026"]);
    expect(grafico().config.data.datasets.map((d: any) => [d.label, d.data])).toEqual([
      ["Calouros (Novos+Regresso+Recompra)", [11, 25]],
      ["Rematrículas", [5, 5]],
      ["Total", [16, 30]],
    ]);
    const legenda = within(screen.getByRole("list", { name: "Legenda" }));
    expect(legenda.getByText("Calouros (N+Rg+Rc)")).toBeTruthy();
    expect(legenda.getByText("36")).toBeTruthy();
    expect(legenda.getByText("46")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Ver detalhado" }));
    expect(grafico().config.data.datasets.map((d: any) => d.label)).toEqual(["Novos", "Rematrículas", "Regresso", "Recompra", "Total"]);
    expect(screen.getByRole("button", { name: "Ver agregado" })).toBeTruthy();
    expect(screen.getByText("Ciclo 2026/2 · 2026-05-01 → 2026-06-30")).toBeTruthy();
  });

  it("clique num mês abre os dias do mês; Voltar retorna aos meses", async () => {
    abrir();
    await aberto();
    await waitFor(() => expect(grafico()).toBeTruthy());
    act(() => grafico().config.options.onClick({}, [{ index: 1 }]));
    await waitFor(() => expect(evolucao()).toHaveLength(2));
    expect(evolucao()[1]).toBe("/api/dashboard/timeline?granularity=day&ciclo=2026%2F2&from=2026-06-01&to=2026-06-30");
    fireEvent.click(await screen.findByRole("button", { name: "Voltar" }));
    await waitFor(() => expect(screen.queryByRole("button", { name: "Voltar" })).toBeNull());
  });

  it("sem períodos: aviso de vazio", async () => {
    abrir({ rotas: { "/api/dashboard/timeline": () => json({ periods: [], series: { total: [] } }) } });
    await aberto();
    expect(await screen.findByText("Sem matrículas no período.")).toBeTruthy();
  });
});

describe("Dashboard: comparação de ciclos", () => {
  it("cartões de período, ciclos com variação e aviso de nível sem ciclo", async () => {
    abrir();
    await aberto();
    const secao = within(screen.getByRole("region", { name: "Comparação de ciclos" }));
    expect(await secao.findByText("YTD 2026")).toBeTruthy();
    expect(secao.getByText("+10,0%")).toBeTruthy();
    expect(secao.getAllByText("+100%")).toHaveLength(2);
    const ciclo = secao.getByRole("button", { name: /^2026\/2/ });
    expect(within(ciclo).getByText("130")).toBeTruthy();
    expect(within(ciclo).getByText("+30,0%")).toBeTruthy();
    fireEvent.click(ciclo);
    expect(secao.getByText("Novos (calouros)")).toBeTruthy();
    expect(secao.getByText("Barra Funda")).toBeTruthy();
    expect(
      secao.getByText('Existem 1.234 negócios com nível "Pós-Graduação" mas nenhum ciclo configurado para esse nível.'),
    ).toBeTruthy();
  });

  it("erro mostra a mensagem", async () => {
    abrir({ rotas: { "/api/dashboard/ciclos": () => json({ error: "sem config" }, 500) } });
    await screen.findByRole("button", { name: /^Novos/ });
    expect(await screen.findByText("Erro: sem config")).toBeTruthy();
  });
});

describe("Dashboard: funil", () => {
  it("indicadores, etapas em destaque e conversão", async () => {
    abrir();
    expect(await screen.findByText("Visualização do funil")).toBeTruthy();
    expect(grupo("Novos leads hoje").getByText("200")).toBeTruthy();
    expect(grupo("Total no funil").getByText("5.000")).toBeTruthy();
    expect(grupo("Aceites").getByText("50")).toBeTruthy();
    expect(grupo("Aceites").getByText("+3")).toBeTruthy();
    expect(grupo("Conversão").getByText("25,0%")).toBeTruthy();
    expect(screen.getByText("Bwipo 10:30")).toBeTruthy();
    const etapas = within(screen.getByRole("list", { name: "Etapas em destaque" }));
    expect(etapas.getByText("Sem Resposta")).toBeTruthy();
    expect(etapas.getByText("3.000")).toBeTruthy();
    // ontem: a rota própria vale mais que o resumo leve do funil
    await waitFor(() => expect(grupo("Fechado — ganho ontem").getByText("9")).toBeTruthy());
    expect(grupo("Fechado — ganho ontem").getByText("120")).toBeTruthy();
    expect(grupo("Fechado — ganho ontem").getByText("+20%")).toBeTruthy();
    expect(chamadas).toContain("/api/kommo/funnel-live");
    expect(chamadas).toContain("/api/kommo/new-leads-today");
    expect(chamadas).toContain("/api/kommo/yesterday-summary");
    expect(chamadas).not.toContain("/api/dashboard/funnel-yesterday");
  });

  it("ontem: sem a rota do Kommo, usa a do dashboard", async () => {
    abrir({
      rotas: {
        "/api/kommo/yesterday-summary": () => json({ ok: false, error: "x" }, 500),
        "/api/dashboard/funnel-yesterday": () => json({ ok: true, data: { vendas: 4, leads: 0, leads_prev: 0 } }),
      },
    });
    await screen.findByText("Visualização do funil");
    await waitFor(() => expect(grupo("Fechado — ganho ontem").getByText("4")).toBeTruthy());
  });

  it("Atualizar força funil, leads de hoje e ontem; alunos não", async () => {
    abrir();
    await aberto();
    await waitFor(() => expect(chamadas).toContain("/api/kommo/yesterday-summary"));
    fireEvent.click(screen.getByRole("button", { name: "Atualizar funil" }));
    await waitFor(() => expect(chamadas).toContain("/api/kommo/yesterday-summary?force=1"));
    expect(chamadas).toContain("/api/kommo/funnel-live?force=1");
    expect(chamadas).toContain("/api/kommo/new-leads-today?force=1");
    expect(alunos()).toHaveLength(1);
    expect(ciclos()).toHaveLength(1);
  });

  it("funil aquecendo: espera e pergunta de novo, sem forçar", async () => {
    let n = 0;
    abrir({
      rotas: {
        "/api/kommo/funnel-live": () =>
          ++n < 3 ? json({ ok: false, warming: true, error: "Funil carregando (primeira carga). Aguarde ~30s." }) : json(FUNIL),
      },
    });
    expect(await screen.findByText("Visualização do funil")).toBeTruthy();
    expect(de("/api/kommo/funnel-live")).toEqual(["/api/kommo/funnel-live", "/api/kommo/funnel-live", "/api/kommo/funnel-live"]);
    // como a antiga: depois de aquecer, o resumo de ontem vem do próprio funil
    expect(chamadas).not.toContain("/api/kommo/yesterday-summary");
    expect(grupo("Fechado — ganho ontem").getByText("7")).toBeTruthy();
  });

  it("funil aquecendo mostra o aviso", async () => {
    abrir({ rotas: { "/api/kommo/funnel-live": () => json({ ok: false, warming: true, error: "Funil carregando." }) } });
    ESPERA_FUNIL.primeira = 60000;
    expect(await screen.findByText("Funil carregando.")).toBeTruthy();
    expect(screen.getByText("Contagem no Bwipo pode levar cerca de 20s")).toBeTruthy();
  });

  it("erro do funil: mensagem e Tentar novamente", async () => {
    abrir({ rotas: { "/api/kommo/funnel-live": () => json({ ok: false, error: "BWIPO_COMERCIAL_API_TOKEN não configurado no servidor." }, 500) } });
    expect(await screen.findByText("BWIPO_COMERCIAL_API_TOKEN não configurado no servidor.")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Tentar novamente" }));
    await waitFor(() => expect(chamadas).toContain("/api/kommo/funnel-live?force=1"));
  });
});

describe("Dashboard: quem vê o quê", () => {
  it("supervisor comercial: painel próprio fica no painel atual, sem buscar dados da home", async () => {
    abrir({ me: me({ categoria: "Supervisor Comercial" }) });
    const link = await screen.findByRole("link", { name: "Abrir no painel atual" });
    expect(link.getAttribute("href")).toBe("/#dashboard");
    await new Promise((r) => setTimeout(r, 20));
    expect(chamadas.filter((u) => u.startsWith("/api/dashboard") || u.startsWith("/api/kommo") || u.startsWith("/api/lista"))).toEqual([]);
  });

  it("supervisor acadêmico: idem", async () => {
    abrir({ me: me({ categoria: "Supervisor Acadêmico" }) });
    expect(await screen.findByRole("link", { name: "Abrir no painel atual" })).toBeTruthy();
    expect(chamadas.filter((u) => u.startsWith("/api/dashboard"))).toEqual([]);
  });

  it("acadêmico com Meus Atendimentos vai direto para lá", async () => {
    const ir = vi.spyOn(navegador, "irPara").mockImplementation(() => {});
    abrir({ me: me({ categoria: "Acadêmico" }, ["dashboard", "meus_atendimentos"]) });
    await waitFor(() => expect(ir).toHaveBeenCalledWith("/#meus_atendimentos"));
    expect(chamadas.filter((u) => u.startsWith("/api/dashboard"))).toEqual([]);
  });

  it("acadêmico sem Meus Atendimentos vê os atalhos", async () => {
    const ir = vi.spyOn(navegador, "irPara").mockImplementation(() => {});
    abrir({ me: me({ categoria: "academico" }, ["dashboard"]) });
    expect(await screen.findByText("Bem-vindo(a) Ana!")).toBeTruthy();
    expect(screen.getByRole("link", { name: /Distribuição Acadêmica/ }).getAttribute("href")).toBe("/#distribuicao");
    expect(screen.getByRole("link", { name: /Recadastros/ }).getAttribute("href")).toBe("/#recadastros");
    expect(ir).not.toHaveBeenCalled();
    expect(chamadas.filter((u) => u.startsWith("/api/dashboard"))).toEqual([]);
  });

  it("menu: Dashboard abre no front novo", () => {
    const m = montarMenu(me().nav, NOMES_TELAS_NOVAS);
    expect(m.avulsas.find((i) => i.pagina === "dashboard")).toMatchObject({ nova: true, href: "/app/dashboard" });
  });
});
