import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { App, FUTURO_ROUTER } from "../App";
import { SEM_PERMISSAO } from "../api";
import type { Me } from "../me";
import { registrarAcesso } from "../rastreio";
import type {
  Comparacao,
  ComparacaoPeriodo,
  Competencias,
  Evolucao,
  EvolucaoPorMes,
  Lado,
  Lista,
  Reincidencia,
} from "./inadimplencia/tipos";

vi.mock("../rastreio", () => ({ registrarAcesso: vi.fn() }));

// chart.js não desenha no jsdom (sem canvas): guardamos a configuração de cada gráfico.
/* eslint-disable @typescript-eslint/no-explicit-any */
const graficos = vi.hoisted(() => [] as { config: any; destruido: boolean }[]);
vi.mock("chart.js", () => {
  class Chart {
    config: any;
    destruido = false;
    constructor(_canvas: unknown, config: any) {
      this.config = config;
      graficos.push(this);
    }
    destroy() {
      this.destruido = true;
    }
    static register() {}
  }
  const vazio = {};
  return {
    Chart,
    BarController: vazio,
    BarElement: vazio,
    CategoryScale: vazio,
    Filler: vazio,
    LinearScale: vazio,
    LineController: vazio,
    LineElement: vazio,
    PointElement: vazio,
    Tooltip: vazio,
  };
});

function grafico(tipo: "line" | "bar") {
  const vivos = graficos.filter((g) => !g.destruido && g.config.type === tipo);
  return vivos[vivos.length - 1];
}

// ── Respostas falsas ────────────────────────────────────────────────────────

const COMPETENCIAS: Competencias = {
  competencias: [
    { value: "2026-10", label: "Outubro/2026" },
    { value: "2026-09", label: "Setembro/2026" },
  ],
};

const LISTA: Lista = {
  snapshots: [
    {
      id: 11,
      snapshot_id: 11,
      effective_date: "2026-11-27",
      uploaded_at: "2026-11-27T10:00:00-03:00",
      filename: "inad 27.11.2026.xlsx",
      inadimplentes: 1234,
      em_curso: 10000,
      taxa_pct: 12.34,
      nivel: "2026-10",
      competencia_label: "Outubro/2026",
    },
    {
      id: 10,
      snapshot_id: 10,
      effective_date: "2026-11-20",
      uploaded_at: "2026-11-20T09:00:00-03:00",
      filename: "antigo.xlsx",
      inadimplentes: 1100,
      em_curso: 10000,
      taxa_pct: 11,
      nivel: null,
      competencia_label: null,
    },
  ],
};

function lado(p: Partial<Lado>): Lado {
  return {
    snapshot_id: 1,
    snapshot_date: null,
    inadimplentes: 0,
    em_curso: 10000,
    taxa_pct: 0,
    is_fallback: false,
    requested_date: null,
    fallback_date: null,
    dias_diferenca: 0,
    ...p,
  };
}

const ATUAL: Lado = lado({ snapshot_id: 11, snapshot_date: "2026-11-27", inadimplentes: 1234, em_curso: 10000, taxa_pct: 12.34 });

const COMPARAR: Comparacao = {
  a: lado({
    snapshot_id: 9,
    snapshot_date: "2026-11-19",
    inadimplentes: 1150,
    taxa_pct: 11.5,
    is_fallback: true,
    requested_date: "2026-11-21",
    fallback_date: "2026-11-19",
    dias_diferenca: 2,
  }),
  b: lado({ snapshot_id: 11, snapshot_date: "2026-11-27", inadimplentes: 1234, taxa_pct: 12.34 }),
  delta_taxa_pct: 0.84,
  delta_inadimplentes: 84,
};

const PERIODO: ComparacaoPeriodo = {
  competencia: "2026-10",
  competencia_label: "Outubro/2026",
  snapshots_count: 3,
  primeiro: { uploaded_at: "2026-10-26T10:00:00-03:00", inadimplentes: 1500, taxa_pct: 15 },
  ultimo: { uploaded_at: "2026-11-27T10:00:00-03:00", inadimplentes: 1234, taxa_pct: 12.34 },
  variacao_pp: -2.66,
};

const EVOLUCAO: Evolucao = {
  points: [
    { date: "2026-11-20", inadimplentes: 1100, em_curso: 10000, taxa_pct: 11 },
    { date: "2026-11-27", inadimplentes: 1234, em_curso: 10000, taxa_pct: 12.34 },
  ],
};

const POR_MES: EvolucaoPorMes = {
  competencias: [
    {
      nivel: "2026-09",
      label: "Setembro/2026",
      points: [{ day: 20, date: "2026-11-20", taxa_pct: 9.5, inadimplentes: 950, em_curso: 10000 }],
    },
    {
      nivel: "2026-10",
      label: "Outubro/2026",
      points: [
        { day: 20, date: "2026-11-20", taxa_pct: 11, inadimplentes: 1100, em_curso: 10000 },
        { day: 27, date: "2026-11-27", taxa_pct: 12.34, inadimplentes: 1234, em_curso: 10000 },
      ],
    },
  ],
};

const REINC: Reincidencia = {
  buckets: { "1": 1500, "2": 300, "3": 40, "4_plus": 5 },
  competencias_usadas: ["2026-09", "2026-10"],
  rgms_analisados: 1845,
};

function me(opcoes: { admin?: boolean; temPagina?: boolean } = {}): Me {
  const admin = opcoes.admin ?? true;
  const paginas = opcoes.temPagina === false ? ["dashboard"] : ["dashboard", "inadimplencia"];
  return {
    user_id: 1,
    username: admin ? "adm" : "ana",
    role: admin ? "admin" : "viewer",
    categoria: null,
    display_name: admin ? "Adm" : "Ana",
    nav: {
      is_admin: admin,
      is_comercial: false,
      is_suporte_comercial: false,
      pagina_inicial: "dashboard",
      paginas,
      secoes: { academico: opcoes.temPagina !== false },
      grupos: { academico: opcoes.temPagina === false ? [] : ["inadimplencia"] },
    },
  };
}

function json(corpo: unknown, status = 200) {
  return new Response(JSON.stringify(corpo), { status, headers: { "Content-Type": "application/json" } });
}

interface Chamada {
  url: string;
  metodo: string;
}

let chamadas: Chamada[];

type Rota = (url: string) => Response;

interface Opcoes {
  admin?: boolean;
  temPagina?: boolean;
  lista?: Rota;
  atual?: Rota;
  comparar?: Rota;
  periodo?: Rota;
  evolucao?: Rota;
  porMes?: Rota;
  reinc?: Rota;
  apagar?: Rota;
}

const PREFIXO = "/api/inadimplencia/";

function abrir(opcoes: Opcoes = {}) {
  chamadas = [];
  vi.spyOn(globalThis, "fetch").mockImplementation(async (entrada, init) => {
    const url = String(entrada);
    const metodo = (init?.method || "GET").toUpperCase();
    chamadas.push({ url, metodo });
    if (url === "/api/me") return json(me({ admin: opcoes.admin, temPagina: opcoes.temPagina }));
    if (url === "/api/me/fixadas") return json({ fixadas: [], mais_usadas: [] });
    if (url === "/api/avisos/nao-lidos") return json({ count: 0 });
    if (url.startsWith(PREFIXO + "snapshot/") && metodo === "DELETE")
      return (opcoes.apagar ?? (() => json({ ok: true })))(url);
    if (url === PREFIXO + "competencias") return json(COMPETENCIAS);
    if (url.startsWith(PREFIXO + "list")) return (opcoes.lista ?? (() => json(LISTA)))(url);
    if (url.startsWith(PREFIXO + "atual")) return (opcoes.atual ?? (() => json(ATUAL)))(url);
    if (url.startsWith(PREFIXO + "comparar-periodo")) return (opcoes.periodo ?? (() => json(PERIODO)))(url);
    if (url.startsWith(PREFIXO + "comparar")) return (opcoes.comparar ?? (() => json(COMPARAR)))(url);
    if (url.startsWith(PREFIXO + "evolucao-por-mes")) return (opcoes.porMes ?? (() => json(POR_MES)))(url);
    if (url.startsWith(PREFIXO + "evolucao")) return (opcoes.evolucao ?? (() => json(EVOLUCAO)))(url);
    if (url.startsWith(PREFIXO + "reincidencia")) return (opcoes.reinc ?? (() => json(REINC)))(url);
    return json({}, 404);
  });
  const cliente = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={cliente}>
      <MemoryRouter initialEntries={["/inadimplencia"]} future={FUTURO_ROUTER}>
        <App />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

function urls(prefixo: string) {
  return chamadas.filter((c) => c.metodo === "GET" && c.url.startsWith(PREFIXO + prefixo)).map((c) => c.url);
}

/** Todas as GETs da tela, sem /competencias, na ordem. */
function urlsDados() {
  return chamadas
    .filter((c) => c.metodo === "GET" && c.url.startsWith(PREFIXO) && c.url !== PREFIXO + "competencias")
    .map((c) => c.url);
}

async function carregado() {
  await screen.findByText("inad 27.11.2026.xlsx");
  await waitFor(() => expect(grafico("bar")).toBeTruthy());
}

function cartao(nome: string) {
  return within(screen.getByRole("group", { name: nome }));
}

function secao(nome: string) {
  return within(screen.getByRole("region", { name: nome }));
}

function escolher(rotulo: string, valor: string) {
  fireEvent.change(screen.getByLabelText(rotulo), { target: { value: valor } });
}

const URLS_PADRAO = [
  "/api/inadimplencia/list?recent_months=3",
  "/api/inadimplencia/atual?recent_months=3",
  "/api/inadimplencia/comparar",
  "/api/inadimplencia/evolucao?days=all&recent_months=3",
  "/api/inadimplencia/reincidencia?recent_months=3",
];

beforeEach(() => {
  graficos.length = 0;
  vi.mocked(registrarAcesso).mockClear();
  // Só o relógio é falso (o TanStack Query continua com os timers de verdade).
  vi.useFakeTimers({ toFake: ["Date"] });
  vi.setSystemTime(new Date("2026-11-28T15:00:00Z"));
});

afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe("Inadimplência: abertura", () => {
  it("busca as mesmas rotas da antiga, com os 3 meses recentes, e depois o comparativo por competência", async () => {
    abrir();
    await carregado();
    expect(urlsDados().slice(0, 5).sort()).toEqual([...URLS_PADRAO].sort());
    expect(urls("evolucao-por-mes")).toEqual(["/api/inadimplencia/evolucao-por-mes?recent_months=3"]);
    expect(urls("competencias")).toEqual(["/api/inadimplencia/competencias"]);
    expect(urlsDados()).toHaveLength(6);
  });

  it("indicadores: taxa, inadimplentes e em curso, com a variação vs 7d do /comparar", async () => {
    abrir();
    await carregado();
    expect(cartao("Taxa atual").getByText("12,34%")).toBeTruthy();
    expect(cartao("Taxa atual").getByText("+0,84% vs 7d")).toBeTruthy();
    expect(cartao("Total inadimplentes").getByText("1.234")).toBeTruthy();
    expect(cartao("Total inadimplentes").getByText("+84 vs 7d")).toBeTruthy();
    expect(cartao("Alunos em curso").getByText("10.000")).toBeTruthy();
    expect(cartao("Alunos em curso").getByText("Denominador atual")).toBeTruthy();
  });

  it("sem snapshot no lado A do /comparar, os indicadores ficam sem a variação", async () => {
    abrir({ comparar: () => json({ ...COMPARAR, a: lado({ snapshot_id: null }) }) });
    await carregado();
    expect(cartao("Taxa atual").queryByText(/vs 7d/)).toBeNull();
    expect(cartao("Total inadimplentes").queryByText(/vs 7d/)).toBeNull();
  });

  it("competências: Recentes, o mês atual no topo e as do servidor", async () => {
    abrir();
    await carregado();
    const sel = screen.getByLabelText("Competência") as HTMLSelectElement;
    await waitFor(() => expect(sel.options.length).toBe(4));
    expect(Array.from(sel.options).map((o) => [o.value, o.text])).toEqual([
      ["", "Recentes (linha do tempo)"],
      ["2026-11", "Novembro/2026 (mês atual)"],
      ["2026-10", "Outubro/2026"],
      ["2026-09", "Setembro/2026"],
    ]);
    const meses = screen.getByLabelText("Últimos meses") as HTMLSelectElement;
    expect(meses.value).toBe("3");
    expect(Array.from(meses.options).map((o) => o.text)).toEqual(["1 mês (atual)", "3 meses", "6 meses", "12 meses", "Tudo"]);
  });

  it("registra a abertura da tela", async () => {
    abrir();
    await carregado();
    expect(registrarAcesso).toHaveBeenCalledWith("inadimplencia");
  });
});

describe("Inadimplência: filtros", () => {
  it("mexer nos filtros não busca; Aplicar com competência usa comparar-periodo e tira a competência da reincidência", async () => {
    abrir();
    await carregado();
    const antes = urlsDados().length;
    escolher("Competência", "2026-10");
    expect(urlsDados().length).toBe(antes);
    fireEvent.click(screen.getByRole("button", { name: "Aplicar" }));
    await waitFor(() => expect(urls("evolucao-por-mes")).toHaveLength(2));
    expect(urlsDados().slice(antes).sort()).toEqual(
      [
        "/api/inadimplencia/list?competencia=2026-10",
        "/api/inadimplencia/atual?competencia=2026-10",
        "/api/inadimplencia/comparar-periodo?competencia=2026-10",
        "/api/inadimplencia/evolucao?days=all&competencia=2026-10",
        "/api/inadimplencia/reincidencia",
        "/api/inadimplencia/evolucao-por-mes?competencia=2026-10",
      ].sort(),
    );
  });

  it("comparação por período: primeiro e último upload e a variação em pp; indicadores sem a variação vs 7d", async () => {
    abrir();
    await carregado();
    escolher("Competência", "2026-10");
    fireEvent.click(screen.getByRole("button", { name: "Aplicar" }));
    const comp = await screen.findByText("3 snapshots no período");
    const card = within(comp.closest("section") as HTMLElement);
    expect(card.getByText("Outubro/2026")).toBeTruthy();
    expect(card.getByText("Primeiro upload — 26/10/2026")).toBeTruthy();
    expect(card.getByText("15,00%")).toBeTruthy();
    expect(card.getByText("Último upload — 27/11/2026")).toBeTruthy();
    expect(card.getByText("-2,66 pp")).toBeTruthy();
    expect(card.getByText("pontos percentuais")).toBeTruthy();
    expect(cartao("Taxa atual").queryByText(/vs 7d/)).toBeNull();
  });

  it("comparação por período com menos de dois uploads avisa", async () => {
    abrir({ periodo: () => json({ insuficiente: true, snapshots_count: 1 }) });
    await carregado();
    escolher("Competência", "2026-10");
    fireEvent.click(screen.getByRole("button", { name: "Aplicar" }));
    expect(
      await screen.findByText("É necessário ter pelo menos dois uploads no período para comparar a evolução."),
    ).toBeTruthy();
    expect(screen.getByText("(1 snapshot encontrado para a competência selecionada)")).toBeTruthy();
  });

  it("datas valem sobre competência e meses: trocar data limpa a competência, e recent_months não vai", async () => {
    abrir();
    await carregado();
    escolher("Competência", "2026-10");
    escolher("Data inicial", "2026-11-01");
    expect((screen.getByLabelText("Competência") as HTMLSelectElement).value).toBe("");
    escolher("Data final", "2026-11-27");
    const antes = urlsDados().length;
    fireEvent.click(screen.getByRole("button", { name: "Aplicar" }));
    await waitFor(() => expect(urls("evolucao-por-mes")).toHaveLength(2));
    expect(urlsDados().slice(antes).sort()).toEqual(
      [
        "/api/inadimplencia/list?date_a=2026-11-01&date_b=2026-11-27",
        "/api/inadimplencia/atual?date_a=2026-11-01&date_b=2026-11-27",
        "/api/inadimplencia/comparar",
        "/api/inadimplencia/evolucao?days=all&date_a=2026-11-01&date_b=2026-11-27",
        "/api/inadimplencia/reincidencia?date_a=2026-11-01&date_b=2026-11-27",
        "/api/inadimplencia/evolucao-por-mes?date_a=2026-11-01&date_b=2026-11-27",
      ].sort(),
    );
  });

  it("trocar competência limpa as datas; trocar últimos meses limpa competência e datas", async () => {
    abrir();
    await carregado();
    escolher("Data inicial", "2026-11-01");
    escolher("Competência", "2026-10");
    expect((screen.getByLabelText("Data inicial") as HTMLInputElement).value).toBe("");
    escolher("Data final", "2026-11-27");
    escolher("Últimos meses", "6");
    expect((screen.getByLabelText("Competência") as HTMLSelectElement).value).toBe("");
    expect((screen.getByLabelText("Data final") as HTMLInputElement).value).toBe("");
    fireEvent.click(screen.getByRole("button", { name: "Aplicar" }));
    await waitFor(() => expect(urls("list")).toContain("/api/inadimplencia/list?recent_months=6"));
    expect(urls("reincidencia")).toContain("/api/inadimplencia/reincidencia?recent_months=6");
    expect(urls("evolucao")).toContain("/api/inadimplencia/evolucao?days=all&recent_months=6");
  });

  it("Aplicar de novo com os mesmos filtros busca de novo; competências não", async () => {
    abrir();
    await carregado();
    fireEvent.click(screen.getByRole("button", { name: "Aplicar" }));
    await waitFor(() => expect(urls("evolucao-por-mes")).toHaveLength(2));
    expect(urls("list")).toEqual([URLS_PADRAO[0], URLS_PADRAO[0]]);
    expect(urls("competencias")).toHaveLength(1);
  });

  it("Limpar volta ao padrão (Recentes, 3 meses) e busca", async () => {
    abrir();
    await carregado();
    escolher("Competência", "2026-10");
    fireEvent.click(screen.getByRole("button", { name: "Aplicar" }));
    await waitFor(() => expect(urls("evolucao-por-mes")).toHaveLength(2));
    escolher("Últimos meses", "12");
    fireEvent.click(screen.getByRole("button", { name: "Limpar" }));
    expect((screen.getByLabelText("Últimos meses") as HTMLSelectElement).value).toBe("3");
    expect((screen.getByLabelText("Competência") as HTMLSelectElement).value).toBe("");
    await waitFor(() => expect(urls("evolucao-por-mes")).toHaveLength(3));
    expect(urls("list").slice(-1)[0]).toBe("/api/inadimplencia/list?recent_months=3");
    expect(urls("comparar").slice(-1)[0]).toBe("/api/inadimplencia/comparar");
  });
});

describe("Inadimplência: comparar 2 datas", () => {
  it("mostra o /comparar da abertura, com o aviso de data sem upload", async () => {
    abrir();
    await carregado();
    const card = secao("Comparar 2 datas");
    expect(card.getByText("De — 19/11/2026")).toBeTruthy();
    expect(card.getByText("Sem upload em 21/11/2026 — exibindo dados de 19/11/2026 (2 dias antes)")).toBeTruthy();
    expect(card.getByText("11,50%")).toBeTruthy();
    expect(card.getByText("1.150 inadimplentes")).toBeTruthy();
    expect(card.getByText("Até — 27/11/2026")).toBeTruthy();
    expect(card.getByText("+0,84%")).toBeTruthy();
    expect(card.getByText("+84 alunos")).toBeTruthy();
  });

  it("Comparar usa as datas do cartão (padrão: 7 dias atrás e hoje, como a antiga)", async () => {
    abrir();
    await carregado();
    const card = secao("Comparar 2 datas");
    expect((card.getByLabelText("Comparar de") as HTMLInputElement).value).toBe("2026-11-21");
    expect((card.getByLabelText("Comparar até") as HTMLInputElement).value).toBe("2026-11-28");
    fireEvent.change(card.getByLabelText("Comparar de"), { target: { value: "2026-11-01" } });
    fireEvent.click(card.getByRole("button", { name: "Comparar" }));
    await waitFor(() =>
      expect(urls("comparar")).toContain("/api/inadimplencia/comparar?date_a=2026-11-01&date_b=2026-11-28"),
    );
  });

  it("sem snapshot nas duas datas avisa", async () => {
    abrir({ comparar: () => json({ ...COMPARAR, a: lado({ snapshot_id: null }), b: lado({ snapshot_id: null }) }) });
    await carregado();
    expect(secao("Comparar 2 datas").getByText("Nenhum snapshot encontrado para as datas selecionadas.")).toBeTruthy();
  });
});

describe("Inadimplência: reincidência", () => {
  it("mostra as quatro faixas e o resumo", async () => {
    abrir();
    await carregado();
    expect(cartao("1 competência").getByText("1.500")).toBeTruthy();
    expect(cartao("2 competências").getByText("300")).toBeTruthy();
    expect(cartao("3 competências").getByText("40")).toBeTruthy();
    expect(cartao("4+ competências").getByText("5")).toBeTruthy();
    expect(
      screen.getByText("1.845 RGMs únicos analisados · 2 competências consideradas: 2026-09, 2026-10"),
    ).toBeTruthy();
  });

  it("sem competência com dados", async () => {
    abrir({ reinc: () => json({ ...REINC, competencias_usadas: [], rgms_analisados: 0 }) });
    await carregado();
    expect(screen.getByText("Nenhuma competência com dados disponível.")).toBeTruthy();
  });

  it("erro do servidor aparece no resumo", async () => {
    abrir({ reinc: () => json({ error: "falhou o banco" }, 500) });
    await carregado();
    expect(await screen.findByText("Erro: falhou o banco")).toBeTruthy();
    expect(cartao("1 competência").getByText("—")).toBeTruthy();
  });
});

describe("Inadimplência: evolução e comparativo por competência", () => {
  it("gráfico de linha com as datas dd/mm e as taxas", async () => {
    abrir();
    await carregado();
    const g = grafico("line");
    expect(g.config.data.labels).toEqual(["20/11", "27/11"]);
    expect(g.config.data.datasets[0].data).toEqual([11, 12.34]);
    const tooltip = g.config.options.plugins.tooltip.callbacks;
    expect(tooltip.label({ parsed: { y: 12.34 } })).toBe(" Taxa: 12,34%");
    expect(tooltip.afterBody([{ dataIndex: 1 }])).toEqual([" Inadimplentes: 1.234", " Em curso: 10.000"]);
  });

  it("botões 7d/30d/90d/Tudo só buscam a evolução e ignoram o filtro de cima; Aplicar desliga o botão", async () => {
    abrir();
    await carregado();
    const antes = urlsDados().length;
    fireEvent.click(screen.getByRole("button", { name: "30d" }));
    await waitFor(() => expect(urlsDados().length).toBe(antes + 1));
    expect(urlsDados().slice(-1)[0]).toBe("/api/inadimplencia/evolucao?days=30&recent_months=all");
    expect(screen.getByRole("button", { name: "30d" }).getAttribute("aria-pressed")).toBe("true");
    fireEvent.click(screen.getByRole("button", { name: "Tudo" }));
    await waitFor(() => expect(urlsDados().slice(-1)[0]).toBe("/api/inadimplencia/evolucao?days=all&recent_months=all"));
    fireEvent.click(screen.getByRole("button", { name: "Aplicar" }));
    await waitFor(() => expect(urls("evolucao-por-mes")).toHaveLength(2));
    expect(screen.getByRole("button", { name: "Tudo" }).getAttribute("aria-pressed")).toBe("false");
    expect(urls("evolucao").filter((u) => !u.includes("por-mes")).slice(-1)[0]).toBe(
      "/api/inadimplencia/evolucao?days=all&recent_months=3",
    );
  });

  it("evolução sem pontos avisa", async () => {
    abrir({ evolucao: () => json({ points: [] }) });
    await carregado();
    expect(screen.getByText("Sem snapshots no período selecionado.")).toBeTruthy();
    expect(grafico("line")).toBeUndefined();
  });

  it("comparativo por competência: dia mais recente, barras por competência e '(últ.)' quando falta o dia", async () => {
    abrir();
    await carregado();
    const dia = screen.getByLabelText("Dia") as HTMLInputElement;
    expect(dia.value).toBe("2026-11-27");
    expect(dia.min).toBe("2026-11-20");
    expect(dia.max).toBe("2026-11-27");
    let g = grafico("bar");
    expect(g.config.data.labels).toEqual(["Setembro/2026", "Outubro/2026"]);
    expect(g.config.data.datasets[0].data).toEqual([9.5, 12.34]);
    const tick = g.config.options.scales.x.ticks.callback;
    const ctxTick = { getLabelForValue: (i: number) => g.config.data.labels[i] };
    expect(tick.call(ctxTick, 0, 0)).toEqual(["Setembro/2026", "20/11 (últ.)"]);
    expect(tick.call(ctxTick, 1, 1)).toEqual(["Outubro/2026", "27/11"]);
    const after = g.config.options.plugins.tooltip.callbacks.afterBody;
    expect(after([{ dataIndex: 0, dataset: g.config.data.datasets[0] }])).toEqual([
      " Sem snapshot em 27/11/2026 — exibindo último disponível",
      " Atualizado em: 20/11/2026",
      " Inadimplentes: 950",
      " Em curso: 10.000",
    ]);

    // Trocar o dia redesenha sem buscar de novo.
    const antes = urlsDados().length;
    fireEvent.change(dia, { target: { value: "2026-11-20" } });
    await waitFor(() => expect(grafico("bar").config.data.datasets[0].data).toEqual([9.5, 11]));
    g = grafico("bar");
    expect(g.config.options.scales.x.ticks.callback.call(ctxTick, 1, 1)).toEqual(["Outubro/2026", "20/11"]);
    expect(urlsDados().length).toBe(antes);
  });

  it("comparativo sem competências avisa", async () => {
    abrir({ porMes: () => json({ competencias: [] }) });
    await screen.findByText("inad 27.11.2026.xlsx");
    expect(await screen.findByText("Sem snapshots para comparar entre competências.")).toBeTruthy();
  });
});

describe("Inadimplência: histórico e apagar", () => {
  it("tabela com data, arquivo, competência, inadimplentes, em curso e taxa", async () => {
    abrir();
    await carregado();
    const tabela = within(screen.getByRole("table"));
    const linhas = tabela.getAllByRole("row");
    expect(linhas).toHaveLength(3);
    const l1 = within(linhas[1]);
    expect(l1.getByText("27/11/2026")).toBeTruthy();
    expect(l1.getByText("Outubro/2026")).toBeTruthy();
    expect(l1.getByText("1.234")).toBeTruthy();
    expect(l1.getByText("10.000")).toBeTruthy();
    expect(l1.getByText("12,34%")).toBeTruthy();
    const l2 = within(linhas[2]);
    expect(l2.getByText("antigo.xlsx")).toBeTruthy();
    expect(l2.getByText("—")).toBeTruthy();
    expect(l2.getByText("11,00%")).toBeTruthy();
  });

  it("Apagar pede a mesma confirmação; cancelar não apaga", async () => {
    abrir();
    await carregado();
    const confirmar = vi.spyOn(window, "confirm").mockReturnValue(false);
    const linha = screen.getByText("antigo.xlsx").closest("tr") as HTMLElement;
    fireEvent.click(within(linha).getByRole("button", { name: "Apagar" }));
    expect(confirmar).toHaveBeenCalledWith("Apagar snapshot antigo.xlsx?\nEsta ação não pode ser desfeita.");
    expect(chamadas.some((c) => c.metodo === "DELETE")).toBe(false);
  });

  it("Apagar confirmado faz DELETE e recarrega a tela, competências inclusive", async () => {
    abrir();
    await carregado();
    vi.spyOn(window, "confirm").mockReturnValue(true);
    const linha = screen.getByText("antigo.xlsx").closest("tr") as HTMLElement;
    fireEvent.click(within(linha).getByRole("button", { name: "Apagar" }));
    await waitFor(() => expect(urls("evolucao-por-mes")).toHaveLength(2));
    expect(chamadas.filter((c) => c.metodo === "DELETE").map((c) => c.url)).toEqual([
      "/api/inadimplencia/snapshot/10",
    ]);
    expect(urls("list")).toHaveLength(2);
    expect(urls("reincidencia")).toHaveLength(2);
    await waitFor(() => expect(urls("competencias")).toHaveLength(2));
  });

  it("erro ao apagar mostra o alerta com a mensagem do servidor", async () => {
    abrir({ apagar: () => json({ error: "Snapshot não encontrado" }, 404) });
    await carregado();
    vi.spyOn(window, "confirm").mockReturnValue(true);
    const alerta = vi.spyOn(window, "alert").mockImplementation(() => {});
    const linha = screen.getByText("antigo.xlsx").closest("tr") as HTMLElement;
    fireEvent.click(within(linha).getByRole("button", { name: "Apagar" }));
    await waitFor(() => expect(alerta).toHaveBeenCalledWith("Erro: Snapshot não encontrado"));
    expect(urls("list")).toHaveLength(1);
  });
});

describe("Inadimplência: estados", () => {
  it("sem snapshots: mensagem de vazio e sem os cartões", async () => {
    abrir({ lista: () => json({ snapshots: [] }), atual: () => json({ error: "Nenhum snapshot de inadimplentes encontrado." }, 404) });
    expect(await screen.findByText("Nenhum dado de inadimplência")).toBeTruthy();
    expect(screen.getByText(/Nenhum snapshot de inadimplência foi feito ainda\./)).toBeTruthy();
    expect(screen.queryByRole("table")).toBeNull();
    expect(cartao("Taxa atual").getByText("—")).toBeTruthy();
    // como a antiga, o comparativo por competência só busca quando há snapshots
    expect(urls("evolucao-por-mes")).toEqual([]);
  });

  it("sem snapshots na competência escolhida", async () => {
    abrir({ lista: (u) => json(u.includes("competencia=") ? { snapshots: [] } : LISTA) });
    await carregado();
    escolher("Competência", "2026-09");
    fireEvent.click(screen.getByRole("button", { name: "Aplicar" }));
    expect(await screen.findByText("Sem dados para Setembro/2026")).toBeTruthy();
    expect(screen.getByText(/Nenhum snapshot de inadimplência para esta competência\./)).toBeTruthy();
  });

  it("erro do servidor na lista aparece na tela", async () => {
    abrir({ lista: () => json({ error: "banco fora do ar" }, 500) });
    expect(await screen.findByText("Não foi possível carregar: banco fora do ar")).toBeTruthy();
  });

  it("403 mostra o aviso de permissão", async () => {
    abrir({ lista: () => json({ error: "x" }, 403) });
    expect(await screen.findByText(SEM_PERMISSAO)).toBeTruthy();
  });

  it("usuário comum com a página vê a mesma tela, Apagar inclusive (a antiga não tinha modo admin)", async () => {
    abrir({ admin: false });
    await carregado();
    expect(screen.getAllByRole("button", { name: "Apagar" })).toHaveLength(2);
    expect(urlsDados().slice(0, 5).sort()).toEqual([...URLS_PADRAO].sort());
  });

  it("quem não tem a página não abre a tela nem chama as APIs", async () => {
    abrir({ temPagina: false });
    expect(await screen.findByText("Tela não encontrada")).toBeTruthy();
    expect(chamadas.some((c) => c.url.startsWith(PREFIXO))).toBe(false);
  });
});
