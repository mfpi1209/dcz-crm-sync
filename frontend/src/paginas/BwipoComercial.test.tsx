import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi, type MockInstance } from "vitest";
import { tentarDeNovo } from "../api";
import { App, FUTURO_ROUTER } from "../App";
import type { Me } from "../me";
import { registrarAcesso } from "../rastreio";
import { csvAgente, linhasRanking, nivelRanking, periodoMaisRecente } from "./bwipo-comercial/formato";
import type { Conflitos, ConsultaRgm, Painel } from "./bwipo-comercial/tipos";

vi.mock("../rastreio", () => ({ registrarAcesso: vi.fn() }));

const PAINEL: Painel = {
  ok: true,
  kpis: {
    matriculas: 3,
    em_curso: 2,
    evasao: 1,
    fora_padrao: 1,
    media: 1.5,
    dias: 2,
    ticket: 1234.5,
    prefixo: "50",
    vendas_6m: 2,
    vendas_1a: null,
    pct_6m: 50,
    pct_1a: null,
    delta_6m: 1,
    delta_1a: null,
    compare_6m_period: "01/04/2026 a 30/04/2026",
    compare_1a_period: "01/10/2025 a 31/10/2025",
  },
  linhas: [
    { rgm: "50000001", nome: "Aluno Um", situacao: "EM CURSO", data: "2026-10-05", polo: "Polo A", nivel: "Graduação", tipo: "NOVA MATRICULA", curso: "ADM", agente: "Ana Souza", fonte: "kommo", conta: true, fora: false },
    { rgm: "50000002", nome: "Aluno Dois", situacao: "EM CURSO", data: "2026-10-06", polo: "Polo B", nivel: "Pós-Graduação", tipo: "RETORNO", curso: "MBA", agente: "Ana Souza", fonte: "bwipo", conta: true, fora: false },
    { rgm: "50000003", nome: "Aluno Três", situacao: "CANCELADO", data: "2026-10-06", polo: "Polo A", nivel: "Graduação", tipo: "", curso: "DIR", agente: "Bruno Lima", fonte: "manual", conta: false, fora: false },
    { rgm: "40000004", nome: "Aluno Quatro", situacao: "EM CURSO", data: "2026-10-05", polo: "Polo A", nivel: "Graduação", tipo: "", curso: "PED", agente: "Bruno Lima", fonte: "sem_dono", conta: false, fora: true },
  ],
  prefixos: [{ pfx: "40", n: 1 }],
  ranking: [
    { agente: "Ana Souza", matriculas: 2, aberto: 5, perdido: 1, evasao: 0, pos: 1, meta: 2, intermediaria: 1, supermeta: 4, conv: 12.5, user_id: 111 },
    { agente: "Bruno Lima", matriculas: 0, aberto: 3, perdido: 0, evasao: 1, pos: 0, meta: 0, intermediaria: 0, supermeta: 0, conv: 0, user_id: null },
    { agente: "Zé Zero", matriculas: 0, aberto: 0, perdido: 0, evasao: 0, pos: 0, meta: 0, intermediaria: 0, supermeta: 0, conv: 0, user_id: null },
  ],
  polos: [
    { polo: "Polo A", total: 2 },
    { polo: "Polo B", total: 1 },
  ],
  evolucao: [
    { dia: "2026-10-05", total: 1 },
    { dia: "2026-10-06", total: 1 },
  ],
  opcoes: { polos: ["Polo A", "Polo B"], agentes: ["Admin Sistema", "Ana Souza", "Bruno Lima"] },
};

const CONFLITOS: Conflitos = {
  ok: true,
  total: 2,
  total_nao_resolvidos: 1,
  conflitos: [
    {
      rgm: "50000001",
      resolucao: null,
      candidatos: [
        { origem: "bwipo", id: "d1", number: 11, user_id: 111, agente: "Ana Souza", etapa: "Ganho", ganho: true, data: "2026-10-05" },
        { origem: "bwipo", id: "d2", number: 12, user_id: 222, agente: "Carla Dias", etapa: "Aberto", ganho: false, data: "2026-10-05" },
      ],
    },
    {
      rgm: "50000002",
      resolucao: { user_id: 222, user_name: "Carla Dias" },
      candidatos: [
        { origem: "bwipo", id: "d3", number: 13, user_id: 111, agente: "Ana Souza", etapa: "Ganho", ganho: true, data: "2026-10-06" },
        { origem: "bwipo", id: "d4", number: 14, user_id: 222, agente: "Carla Dias", etapa: "Ganho", ganho: true, data: "2026-10-06" },
      ],
    },
  ],
};

const RGM: ConsultaRgm = {
  ok: true,
  rgm: "50000001",
  agente: "Ana Souza",
  user_id: 111,
  fonte: "kommo",
  resolucao: null,
  contar_venda: false,
  candidatos: [
    { origem: "kommo", id: 9001, user_id: 111, agente: "Ana Souza", etapa: "Venda ganha", data: "2026-10-05" },
    { origem: "bwipo", id: "d2", number: 12, user_id: 222, agente: "Carla Dias", etapa: "Aberto" },
    { origem: "bwipo", id: "d5", number: 15, user_id: null, agente: "Fulano", etapa: "Aberto" },
  ],
};

function me(admin = true): Me {
  return {
    user_id: admin ? 1 : 5,
    username: admin ? "adm" : "ana",
    role: admin ? "admin" : "viewer",
    categoria: null,
    display_name: admin ? "Adm" : "Ana Souza",
    nav: {
      is_admin: admin,
      is_comercial: !admin,
      is_suporte_comercial: false,
      pagina_inicial: "dashboard",
      paginas: ["dashboard", "bwipo_comercial"],
      secoes: { comercial: true },
      grupos: { comercial: ["bwipo_comercial"] },
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

function abrir(
  opcoes: {
    admin?: boolean;
    painel?: Rota;
    conflitos?: Rota;
    metas?: Rota;
    campanhas?: Rota;
    rotas?: Record<string, Rota>;
    /** QueryClient com a regra de novas tentativas de produção (main.tsx). */
    producao?: boolean;
  } = {},
) {
  chamadas = [];
  const admin = opcoes.admin ?? true;
  vi.spyOn(globalThis, "fetch").mockImplementation(async (entrada, init) => {
    const url = String(entrada);
    const metodo = (init?.method || "GET").toUpperCase();
    chamadas.push({ url, metodo, corpo: init?.body as string | undefined });
    const chave = `${metodo} ${url.split("?")[0]}`;
    if (opcoes.rotas?.[chave]) return opcoes.rotas[chave](url, init);
    if (url === "/api/me") return json(me(admin));
    if (url === "/api/me/fixadas") return json({ fixadas: [], mais_usadas: [] });
    if (url === "/api/avisos/nao-lidos") return json({ count: 0 });
    if (url === "/api/comercial-rgm/metas?categoria=matriculas")
      return (opcoes.metas ?? (() => json({ ok: true, metas: [{ dt_inicio: "2026-09-01", dt_fim: "2026-09-30" }] })))(url, init);
    if (url === "/api/premiacao/campanhas-periodos")
      return (opcoes.campanhas ?? (() => json({ ok: true, campanhas: [{ dt_inicio: "2026-10-01T00:00:00", dt_fim: "2026-10-31" }] })))(url, init);
    if (url === "/api/comercial-rgm/ciclos")
      return json({
        ok: true,
        ciclos: [
          { id: 7, nome: "26.2", descricao: "Ciclo 26.2", ativo: true, dt_inicio: "2026-08-01", dt_fim: "2027-01-31" },
          { id: 6, nome: "26.1", descricao: null, ativo: false, dt_inicio: "2026-06-30", dt_fim: "2026-02-01" },
        ],
      });
    if (url.startsWith("/api/comercial-rgm/turmas"))
      return json({
        ok: true,
        turmas: url.includes("ciclo_id=7")
          ? [{ id: 31, nome: "Novembro", nivel: "Pós-Graduação", dt_inicio: "2026-11-01", dt_fim: "2026-11-30" }]
          : [
              { id: 21, nome: "Outubro", nivel: "Graduação", dt_inicio: "2026-10-01", dt_fim: "2026-10-31" },
              { id: 31, nome: "Novembro", nivel: "Pós-Graduação", dt_inicio: "2026-11-01", dt_fim: "2026-11-30" },
            ],
      });
    if (url === "/api/comercial-rgm/snapshot-info")
      return json({ ok: true, total: 12345, min_date: "2026-01-02", max_date: "2026-11-27", mm_inscritos: 10, mm_matriculados: 4 });
    if (url.startsWith("/api/bwipo/painel/extras")) return json({ ok: true, ytd: 1500 });
    if (url === "/api/bwipo/painel/conflitos" && metodo === "POST") return (opcoes.conflitos ?? (() => json(CONFLITOS)))(url, init);
    if (url === "/api/bwipo/painel/conflitos/resolver") return json({ ok: true });
    if (url === "/api/bwipo/painel/contar-venda") return json({ ok: true });
    if (url.startsWith("/api/bwipo/painel/rgm")) return json(RGM);
    if (url === "/api/bwipo/painel/contatos")
      return json({ ok: true, contatos: { "50000001": { cpf: "111.222.333-44", telefone: "11999990000" } } });
    if (url.startsWith("/api/bwipo/painel?")) return (opcoes.painel ?? (() => json(PAINEL)))(url, init);
    return json({}, 404);
  });
  const cliente = new QueryClient({
    defaultOptions: { queries: opcoes.producao ? { retry: tentarDeNovo, retryDelay: 0 } : { retry: false } },
  });
  render(
    <QueryClientProvider client={cliente}>
      <MemoryRouter initialEntries={["/bwipo_comercial"]} future={FUTURO_ROUTER}>
        <App />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

function urls(prefixo: string, metodo = "GET") {
  return chamadas.filter((c) => c.metodo === metodo && c.url.startsWith(prefixo)).map((c) => c.url);
}
const corpos = (url: string, metodo = "POST") =>
  chamadas.filter((c) => c.metodo === metodo && c.url === url).map((c) => JSON.parse(String(c.corpo)));

const PAINEL_INICIAL = "/api/bwipo/painel?dt_ini=2026-10-01&dt_fim=2026-10-31";

const kpi = (nome: string) => within(screen.getByRole("group", { name: nome }));
const regiao = (nome: string) => within(screen.getByRole("region", { name: nome }));

async function esperarPainel() {
  await screen.findByRole("button", { name: "Ana Souza" });
}

const pausa = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Depois de gravar: o painel é buscado exatamente mais uma vez, e extras e conflitos o seguem. */
async function esperarDepoisDeGravar() {
  await waitFor(() => expect(urls("/api/bwipo/painel?")).toHaveLength(2));
  await waitFor(() => expect(urls("/api/bwipo/painel/extras")).toHaveLength(2));
  await waitFor(() => expect(corpos("/api/bwipo/painel/conflitos")).toHaveLength(2));
  await pausa(50);
  expect(urls("/api/bwipo/painel?")).toHaveLength(2);
  expect(urls("/api/bwipo/painel/extras")).toHaveLength(2);
  expect(corpos("/api/bwipo/painel/conflitos")).toHaveLength(2);
}

let confirmar: MockInstance<(mensagem?: string) => boolean>;

beforeEach(() => {
  vi.mocked(registrarAcesso).mockClear();
  confirmar = vi.spyOn(window, "confirm").mockReturnValue(true);
});

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  vi.useRealTimers();
});

describe("formato", () => {
  it("período mais recente: maior início, depois maior fim", () => {
    expect(
      periodoMaisRecente([
        { dt_inicio: "2026-09-01", dt_fim: "2026-09-30" },
        { dt_inicio: "2026-10-01T00:00:00", dt_fim: "2026-10-15" },
        { dt_inicio: "2026-10-01", dt_fim: "2026-10-31" },
        { dt_inicio: null, dt_fim: "2027-01-01" },
      ]),
    ).toEqual({ dt_inicio: "2026-10-01", dt_fim: "2026-10-31" });
    expect(periodoMaisRecente([])).toBeNull();
  });

  it("nível do ranking como na antiga", () => {
    const base = PAINEL.ranking[0];
    expect(nivelRanking({ ...base, matriculas: 4 })).toBe("Super");
    expect(nivelRanking({ ...base, matriculas: 2 })).toBe("Meta");
    expect(nivelRanking({ ...base, matriculas: 1 })).toBe("Interm.");
    expect(nivelRanking({ ...base, matriculas: 0 })).toBe("Abaixo");
    expect(nivelRanking({ ...base, meta: 0, intermediaria: 0, supermeta: 0, pos: 0 })).toBe("Grad.");
    expect(nivelRanking({ ...base, meta: 0, intermediaria: 0, supermeta: 0, pos: 2, matriculas: 2 })).toBe("Pós");
    expect(nivelRanking({ ...base, meta: 0, intermediaria: 0, supermeta: 0, pos: 1, matriculas: 2 })).toBe("Misto");
  });

  it("ranking sem dia esconde quem está zerado; com dia recalcula pelas linhas", () => {
    expect(linhasRanking(PAINEL, null).map((r) => r.agente)).toEqual(["Ana Souza", "Bruno Lima"]);
    const dia = linhasRanking(PAINEL, "2026-10-06");
    expect(dia.map((r) => [r.agente, r.matriculas, r.evasao, r.pos, r.conv])).toEqual([
      ["Ana Souza", 1, 0, 1, 100],
      ["Bruno Lima", 0, 1, 0, 0],
    ]);
  });

  it("CSV do agente com BOM, ponto e vírgula e aspas", () => {
    const csv = csvAgente([{ ...PAINEL.linhas[0], nome: 'Aluno "Um"', cpf: "1", telefone: "2" }]);
    expect(csv).toBe(
      '﻿rgm;nome;cpf;telefone;tipo;polo;nivel;data;curso;situacao;fonte\n' +
        '"50000001";"Aluno ""Um""";"1";"2";"NOVA MATRICULA";"Polo A";"Graduação";"2026-10-05";"ADM";"EM CURSO";"kommo"',
    );
  });
});

describe("Dashboard Comercial Bwipo: carga e números", () => {
  it("abre no período da campanha mais recente e chama o painel uma vez", async () => {
    abrir();
    await esperarPainel();
    expect(urls("/api/bwipo/painel?")).toEqual([PAINEL_INICIAL]);
    expect((screen.getByLabelText("De") as HTMLInputElement).value).toBe("2026-10-01");
    expect((screen.getByLabelText("Até") as HTMLInputElement).value).toBe("2026-10-31");
    expect(registrarAcesso).toHaveBeenCalledWith("bwipo_comercial");
    // extras com os mesmos parâmetros
    await waitFor(() => expect(urls("/api/bwipo/painel/extras")).toEqual(["/api/bwipo/painel/extras?dt_ini=2026-10-01&dt_fim=2026-10-31"]));
  });

  it("sem metas nem campanhas usa do dia 1 do mês até hoje", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(new Date(2026, 10, 28, 10, 0, 0));
    abrir({ metas: () => json({ error: "x" }, 500), campanhas: () => json({ ok: true, campanhas: [] }) });
    await esperarPainel();
    expect(urls("/api/bwipo/painel?")).toEqual(["/api/bwipo/painel?dt_ini=2026-11-01&dt_fim=2026-11-28"]);
  });

  it("mostra os números do servidor", async () => {
    abrir();
    await esperarPainel();
    const hero = kpi("Matrículas no período");
    expect(hero.getByText("3")).toBeTruthy();
    expect(hero.getByText("2")).toBeTruthy(); // em curso
    expect(await kpi("YTD").findByText("1.500")).toBeTruthy();
    expect(kpi("Média diária").getByText("1,5")).toBeTruthy();
    expect(kpi("Média diária").getByText("2 dias no período")).toBeTruthy();
    expect(kpi("Ticket médio (30%)").getByText("R$ 1.234,50")).toBeTruthy();
    const m6 = kpi("vs 6 meses");
    expect(m6.getByText("2")).toBeTruthy();
    expect(m6.getByText("↑ 50%")).toBeTruthy();
    expect(m6.getByText("+1 matrículas").getAttribute("title")).toBe("01/04/2026 a 30/04/2026");
    const a1 = kpi("vs 1 ano");
    expect(a1.getByText("—")).toBeTruthy();
    expect(a1.getByText("sem histórico no período")).toBeTruthy();
    expect(screen.getByText(/12\.345 registros CSV \| 2026-01-02 a 2026-11-27 \| M&M: 10 insc\. \/ 4 matr\./)).toBeTruthy();
  });

  it("não repete o título da tela", async () => {
    abrir();
    await esperarPainel();
    // o título fica só na barra de cima (h1); o conteúdo da tela não o repete
    expect(screen.getByRole("heading", { level: 1, name: "Dashboard Comercial Bwipo" })).toBeTruthy();
    expect(within(screen.getByRole("main")).queryByText("Dashboard Comercial Bwipo")).toBeNull();
  });

  it("erro do painel mostra a mensagem do servidor", async () => {
    abrir({ painel: () => json({ ok: false, error: "base em reconstrução" }, 500) });
    expect(await screen.findByText("base em reconstrução")).toBeTruthy();
  });

  it("403 mostra o aviso de sem permissão", async () => {
    abrir({ painel: () => json({ error: "não" }, 403) });
    expect(await screen.findByText("Você não tem permissão para esta tela.")).toBeTruthy();
  });

  it("ranking vazio e sem polo", async () => {
    abrir({ painel: () => json({ ...PAINEL, ranking: [], polos: [], linhas: [], evolucao: [] }) });
    expect(await screen.findByText("Nenhuma matrícula neste período.")).toBeTruthy();
    expect(screen.getByText("Sem polo.")).toBeTruthy();
  });
});

describe("filtros", () => {
  it("mexer nos filtros não busca; Atualizar busca com os parâmetros na ordem da antiga", async () => {
    abrir();
    await esperarPainel();
    fireEvent.change(screen.getByLabelText("Polo"), { target: { value: "Polo B" } });
    fireEvent.change(screen.getByLabelText("Nível"), { target: { value: "Pós-Graduação" } });
    fireEvent.change(screen.getByLabelText("Agente"), { target: { value: "Ana Souza" } });
    fireEvent.change(screen.getByLabelText("De"), { target: { value: "2026-10-02" } });
    expect(urls("/api/bwipo/painel?")).toHaveLength(1);
    fireEvent.click(screen.getByRole("button", { name: "Atualizar" }));
    await waitFor(() => expect(urls("/api/bwipo/painel?")).toHaveLength(2));
    expect(urls("/api/bwipo/painel?")[1]).toBe(
      "/api/bwipo/painel?polo=Polo+B&nivel=P%C3%B3s-Gradua%C3%A7%C3%A3o&owner=Ana+Souza&dt_ini=2026-10-02&dt_fim=2026-10-31",
    );
  });

  it("Atualizar sem mudar nada busca o painel de novo (e extras e conflitos depois)", async () => {
    abrir();
    await esperarPainel();
    await waitFor(() => expect(urls("/api/bwipo/painel/conflitos", "POST")).toHaveLength(1));
    fireEvent.click(screen.getByRole("button", { name: "Atualizar" }));
    await waitFor(() => expect(urls("/api/bwipo/painel?")).toEqual([PAINEL_INICIAL, PAINEL_INICIAL]));
    await waitFor(() => expect(urls("/api/bwipo/painel/conflitos", "POST")).toHaveLength(2));
    expect(urls("/api/bwipo/painel/extras")).toHaveLength(2);
    // filtros e ciclos não são buscados de novo
    expect(urls("/api/comercial-rgm/ciclos")).toHaveLength(1);
    expect(urls("/api/comercial-rgm/metas")).toHaveLength(1);
  });

  it("ciclo preenche as datas (trocando De/Até invertidos), recarrega turmas e já busca", async () => {
    abrir();
    await esperarPainel();
    const ciclo = screen.getByLabelText("Ciclo") as HTMLSelectElement;
    expect(ciclo.value).toBe("");
    expect(within(ciclo).getByRole("option", { name: "Ciclo 26.2 (ativo)" })).toBeTruthy();
    fireEvent.change(ciclo, { target: { value: "6" } });
    await waitFor(() => expect(urls("/api/bwipo/painel?")).toHaveLength(2));
    expect(urls("/api/bwipo/painel?")[1]).toBe("/api/bwipo/painel?dt_ini=2026-02-01&dt_fim=2026-06-30");
    fireEvent.change(ciclo, { target: { value: "7" } });
    await waitFor(() => expect(urls("/api/comercial-rgm/turmas?ciclo_id=7")).toHaveLength(1));
    await waitFor(() => expect(urls("/api/bwipo/painel?")[2]).toBe("/api/bwipo/painel?dt_ini=2026-08-01&dt_fim=2027-01-31"));
  });

  it("turma preenche datas e nível e já busca", async () => {
    abrir();
    await esperarPainel();
    const turma = screen.getByLabelText("Turma") as HTMLSelectElement;
    await waitFor(() => expect(within(turma).getByRole("option", { name: "Novembro" })).toBeTruthy());
    fireEvent.change(turma, { target: { value: "31" } });
    await waitFor(() => expect(urls("/api/bwipo/painel?")).toHaveLength(2));
    expect(urls("/api/bwipo/painel?")[1]).toBe(
      "/api/bwipo/painel?nivel=P%C3%B3s-Gradua%C3%A7%C3%A3o&dt_ini=2026-11-01&dt_fim=2026-11-30",
    );
    expect((screen.getByLabelText("Nível") as HTMLSelectElement).value).toBe("Pós-Graduação");
  });
});

describe("ranking, dias e polos", () => {
  it("ranking com metas, nível, carteira e conversão", async () => {
    abrir();
    await esperarPainel();
    const r = regiao("Ranking de agentes comerciais");
    expect(r.getByText("2 agentes")).toBeTruthy();
    const linhas = r.getAllByRole("row").slice(1);
    expect(linhas).toHaveLength(2);
    expect(within(linhas[0]).getAllByRole("cell").map((c) => c.textContent)).toEqual([
      "1", "Ana Souza", "2", "1", "2", "4", "Meta", "5", "1", "0", "12,5%",
    ]);
    expect(within(linhas[1]).getAllByRole("cell").map((c) => c.textContent)).toEqual([
      "2", "Bruno Lima", "0", "—", "—", "—", "Grad.", "3", "0", "1", "0%",
    ]);
    expect(r.queryByText("Zé Zero")).toBeNull();
  });

  it("clicar num dia filtra o ranking e a lista de evasão; limpar volta", async () => {
    abrir();
    await esperarPainel();
    fireEvent.click(screen.getByRole("button", { name: "Filtrar o ranking pelo dia 06/10 (1)" }));
    expect(screen.getByText(/Dia 06\/10 filtrando o ranking\./)).toBeTruthy();
    const r = regiao("Ranking de agentes comerciais");
    const linhas = r.getAllByRole("row").slice(1);
    expect(within(linhas[0]).getAllByRole("cell").map((c) => c.textContent)).toEqual([
      "1", "Ana Souza", "1", "1", "2", "4", "Interm.", "5", "1", "0", "100%",
    ]);
    fireEvent.click(screen.getByRole("button", { name: "limpar" }));
    expect(screen.queryByText(/filtrando o ranking/)).toBeNull();
    expect(within(r.getAllByRole("row")[1]).getAllByRole("cell")[2].textContent).toBe("2");
    expect(urls("/api/bwipo/painel?")).toHaveLength(1);
  });

  it("polos com barra proporcional", async () => {
    abrir();
    await esperarPainel();
    const p = regiao("Matrículas por polo");
    const linhas = p.getAllByRole("row").slice(1);
    expect(within(linhas[0]).getAllByRole("cell").slice(0, 3).map((c) => c.textContent)).toEqual(["1", "Polo A", "2"]);
    expect((linhas[1].querySelector("[data-barra]") as HTMLElement).style.width).toBe("50%");
  });
});

describe("evasão, fora do padrão e conflitos", () => {
  it("evasão com chips e lista ao clicar", async () => {
    abrir();
    await esperarPainel();
    const botao = screen.getByRole("button", { name: /Evasão no período/ });
    expect(within(botao).getByText("1")).toBeTruthy();
    expect(within(botao).getByText("Cancelado: 1")).toBeTruthy();
    expect(screen.queryByRole("region", { name: "Lista de evasão" })).toBeNull();
    fireEvent.click(botao);
    const lista = regiao("Lista de evasão");
    expect(lista.getByText("Aluno Três")).toBeTruthy();
    expect(lista.getByText("Bruno Lima · decisão manual")).toBeTruthy();
    expect(lista.getByText("06/10/2026")).toBeTruthy();
    expect(lista.queryByText("Aluno Um")).toBeNull();
  });

  it("fora do padrão: prefixos, aviso no destaque e Contar venda só para admin", async () => {
    abrir();
    await esperarPainel();
    expect(screen.getByRole("button", { name: /1 RGM fora do padrão/ })).toBeTruthy();
    expect(screen.getByText("1 alunos em curso — ver lista")).toBeTruthy();
    const botao = screen.getByRole("button", { name: /Fora do padrão RGM/ });
    expect(within(botao).getByText("40 · 1")).toBeTruthy();
    fireEvent.click(botao);
    const lista = regiao("Lista fora do padrão");
    expect(lista.getByText("Aluno Quatro")).toBeTruthy();
    fireEvent.click(lista.getByRole("button", { name: "Contar venda" }));
    expect(confirmar).toHaveBeenCalledWith("RGM 40000004: passar a contar como venda?");
    await waitFor(() => expect(corpos("/api/bwipo/painel/contar-venda")).toEqual([{ rgm: "40000004" }]));
    expect(await screen.findByText("RGM 40000004 passa a contar.")).toBeTruthy();
    await esperarDepoisDeGravar();
  });

  it("usuário comum não vê Contar venda nem Sync agentes e não chama rotas de admin", async () => {
    abrir({ admin: false });
    await esperarPainel();
    fireEvent.click(screen.getByRole("button", { name: /Fora do padrão RGM/ }));
    expect(regiao("Lista fora do padrão").queryByRole("button", { name: "Contar venda" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Sync agentes" })).toBeNull();
    fireEvent.change(screen.getByLabelText("Consultar RGM"), { target: { value: "50000001" } });
    fireEvent.click(screen.getByRole("button", { name: "Buscar" }));
    const modal = within(await screen.findByRole("dialog", { name: "RGM 50000001" }));
    expect(modal.queryByRole("button", { name: /Contar venda/ })).toBeNull();
    const admin = ["/api/bwipo/painel/sync-agentes", "/api/bwipo/painel/consultores", "/api/bwipo/painel/meta-agente", "/api/bwipo/painel/contar-venda"];
    expect(chamadas.filter((c) => admin.some((a) => c.url.startsWith(a)))).toEqual([]);
  });

  it("conflitos: corpo com os RGMs e o período; contagem e cartões", async () => {
    abrir();
    await esperarPainel();
    await waitFor(() =>
      expect(corpos("/api/bwipo/painel/conflitos")).toEqual([
        { rgms: ["50000001", "50000002", "50000003", "40000004"], dt_ini: "2026-10-01", dt_fim: "2026-10-31" },
      ]),
    );
    const botao = screen.getByRole("button", { name: /Vendas em conflito/ });
    expect(await within(botao).findByText("2")).toBeTruthy();
    expect(within(botao).getByText("· 1 sem decisão")).toBeTruthy();
    fireEvent.click(botao);
    const lista = regiao("Lista de conflitos");
    expect(lista.getByText("Aluno Um")).toBeTruthy();
    expect(lista.getByText("Salvo")).toBeTruthy();
    const sel = lista.getByLabelText("Creditar venda do RGM 50000002") as HTMLSelectElement;
    expect(sel.value).toBe("222");
  });

  it("fixar venda pelo seletor pede confirmação e grava", async () => {
    abrir();
    await esperarPainel();
    fireEvent.click(screen.getByRole("button", { name: /Vendas em conflito/ }));
    const lista = regiao("Lista de conflitos");
    const sel = await lista.findByLabelText("Creditar venda do RGM 50000001");
    confirmar.mockReturnValueOnce(false);
    fireEvent.change(sel, { target: { value: "222" } });
    expect(confirmar).toHaveBeenLastCalledWith("Fixar a venda do RGM 50000001 em Carla Dias?");
    expect(corpos("/api/bwipo/painel/conflitos/resolver")).toEqual([]);
    expect((sel as HTMLSelectElement).value).toBe("");
    fireEvent.change(sel, { target: { value: "222" } });
    await waitFor(() =>
      expect(corpos("/api/bwipo/painel/conflitos/resolver")).toEqual([{ rgm: "50000001", user_id: 222, user_name: "Carla Dias" }]),
    );
    expect(await screen.findByText("Venda do RGM 50000001 fixada em Carla Dias.")).toBeTruthy();
    await esperarDepoisDeGravar();
  });

  it("desfazer decisão pede confirmação e manda DELETE", async () => {
    abrir();
    await esperarPainel();
    fireEvent.click(screen.getByRole("button", { name: /Vendas em conflito/ }));
    fireEvent.click(await regiao("Lista de conflitos").findByRole("button", { name: "Desfazer" }));
    expect(confirmar).toHaveBeenCalledWith("Tirar a decisão manual do RGM 50000002? A venda volta para a regra automática.");
    await waitFor(() => expect(corpos("/api/bwipo/painel/conflitos/resolver", "DELETE")).toEqual([{ rgm: "50000002" }]));
    expect(await screen.findByText("RGM 50000002 voltou para a regra automática.")).toBeTruthy();
  });

  it("erro dos conflitos aparece na lista", async () => {
    abrir({ conflitos: () => json({ ok: false, error: "falhou o banco" }, 500) });
    await esperarPainel();
    fireEvent.click(screen.getByRole("button", { name: /Vendas em conflito/ }));
    expect(await regiao("Lista de conflitos").findByText("falhou o banco")).toBeTruthy();
  });
});

describe("consultar RGM e atualizar lead", () => {
  it("RGM inválido avisa sem chamar a API", async () => {
    abrir();
    await esperarPainel();
    fireEvent.change(screen.getByLabelText("Consultar RGM"), { target: { value: "123" } });
    fireEvent.click(screen.getByRole("button", { name: "Buscar" }));
    expect(await screen.findByText("Informe um RGM com 8 dígitos.")).toBeTruthy();
    expect(urls("/api/bwipo/painel/rgm")).toEqual([]);
  });

  it("consulta mostra o dono, os candidatos e fixa a venda", async () => {
    abrir();
    await esperarPainel();
    fireEvent.change(screen.getByLabelText("Consultar RGM"), { target: { value: "5000-0001" } });
    fireEvent.keyDown(screen.getByLabelText("Consultar RGM"), { key: "Enter" });
    const modal = within(await screen.findByRole("dialog", { name: "RGM 50000001" }));
    expect(urls("/api/bwipo/painel/rgm")).toEqual(["/api/bwipo/painel/rgm?rgm=50000001"]);
    expect(modal.getByText("Aluno Um · EM CURSO · 2026-10-05")).toBeTruthy();
    expect(modal.getByText("pela regra: Kommo")).toBeTruthy();
    expect(modal.getByText("sem depara")).toBeTruthy();
    expect(modal.getByRole("button", { name: "Contar venda mesmo fora do padrão" })).toBeTruthy();
    const fixar = modal.getAllByRole("button", { name: "Fixar venda aqui" });
    expect(fixar).toHaveLength(2);
    fireEvent.click(fixar[1]);
    expect(confirmar).toHaveBeenCalledWith("Fixar a venda do RGM 50000001 em Carla Dias?");
    await waitFor(() =>
      expect(corpos("/api/bwipo/painel/conflitos/resolver")).toEqual([{ rgm: "50000001", user_id: 222, user_name: "Carla Dias" }]),
    );
    // modal aberto: consulta de novo e atualiza o painel
    await waitFor(() => expect(urls("/api/bwipo/painel/rgm")).toHaveLength(2));
    await esperarDepoisDeGravar();
  });

  it("Atualizar do candidato do Bwipo chama o mini-sync com o deal_id", async () => {
    abrir({ rotas: { "POST /api/bwipo/painel/mini-sync": () => json({ ok: true, msg: "Negócio #12 atualizado." }) } });
    await esperarPainel();
    fireEvent.change(screen.getByLabelText("Consultar RGM"), { target: { value: "50000001" } });
    fireEvent.click(screen.getByRole("button", { name: "Buscar" }));
    const modal = within(await screen.findByRole("dialog", { name: "RGM 50000001" }));
    fireEvent.click(modal.getAllByRole("button", { name: "Atualizar" })[0]);
    await waitFor(() => expect(corpos("/api/bwipo/painel/mini-sync")).toEqual([{ deal_id: "d2" }]));
    expect(await screen.findByText("Negócio #12 atualizado.")).toBeTruthy();
  });

  it("atualizar lead: valida, manda número ou RGM, mostra a escolha e atualiza se fixou", async () => {
    let n = 0;
    abrir({
      rotas: {
        "POST /api/bwipo/painel/mini-sync": (_u, init) => {
          n += 1;
          const corpo = JSON.parse(String(init?.body));
          if (corpo.rgm)
            return json({
              ok: true,
              rgm: corpo.rgm,
              msg: "2 negócios com o RGM 48411612. Escolha qual atualizar.",
              pick: [
                { id: "a1", number: 4997, title: "Maria", etapa: "Ganho", agente: "Ana" },
                { id: "a2", number: 4998, title: "Maria 2", etapa: "Aberto", agente: "Ana" },
              ],
            });
          if (corpo.number) return json({ ok: false, error: "Número do card inválido." }, 400);
          return json({ ok: true, pinned: true, msg: `Negócio #4997 atualizado. Venda do RGM 48411612 fixada em Ana. (${n})` });
        },
      },
    });
    await esperarPainel();
    fireEvent.click(screen.getByRole("button", { name: "Atualizar lead" }));
    const painel = regiao("Atualizar negócio na base Bwipo");
    fireEvent.click(painel.getByRole("button", { name: "Buscar e atualizar" }));
    expect(await screen.findByText("Informe o número do card ou um RGM de 8 dígitos.")).toBeTruthy();
    expect(corpos("/api/bwipo/painel/mini-sync")).toEqual([]);

    fireEvent.change(painel.getByLabelText("Número do card (Bwipo)"), { target: { value: "#49a" } });
    fireEvent.click(painel.getByRole("button", { name: "Buscar e atualizar" }));
    await waitFor(() => expect(corpos("/api/bwipo/painel/mini-sync")).toEqual([{ number: "49" }]));
    expect(await painel.findByText("Número do card inválido.")).toBeTruthy();

    fireEvent.change(painel.getByLabelText("Número do card (Bwipo)"), { target: { value: "" } });
    fireEvent.change(painel.getByLabelText("RGM (8 dígitos)"), { target: { value: "48411612" } });
    fireEvent.click(painel.getByRole("button", { name: "Buscar e atualizar" }));
    const escolha = await painel.findByRole("button", { name: "#4997 Maria · Ganho" });
    expect(corpos("/api/bwipo/painel/mini-sync")[1]).toEqual({ rgm: "48411612" });
    expect(urls("/api/bwipo/painel?")).toHaveLength(1);
    fireEvent.click(escolha);
    await waitFor(() => expect(corpos("/api/bwipo/painel/mini-sync")[2]).toEqual({ deal_id: "a1" }));
    expect(await painel.findByText(/Venda do RGM 48411612 fixada em Ana/)).toBeTruthy();
    await esperarDepoisDeGravar();
  });
});

describe("detalhe do agente", () => {
  it("abre com as matrículas, busca CPF e telefone, filtra e mostra a contagem", async () => {
    abrir();
    await esperarPainel();
    fireEvent.click(screen.getByRole("button", { name: "Bruno Lima" }));
    const modal = within(await screen.findByRole("dialog", { name: /Bruno Lima/ }));
    expect(modal.getByText(/0 venda\(s\) · 2 matrícula\(s\) — período 01\/10\/2026 a 31\/10\/2026/)).toBeTruthy();
    expect(modal.getByText("⚠ 1 RGM fora do padrão")).toBeTruthy();
    expect(document.activeElement).toBe(modal.getByLabelText("Buscar RGM ou nome")); // foco na busca ao abrir
    await waitFor(() => expect(corpos("/api/bwipo/painel/contatos")).toEqual([{ rgms: ["50000003", "40000004"] }]));
    expect(modal.getByRole("button", { name: "Contar venda" })).toBeTruthy();
    fireEvent.change(modal.getByLabelText("Buscar RGM ou nome"), { target: { value: "quatro" } });
    expect(modal.queryByText("Aluno Três")).toBeNull();
    expect(modal.getByText("Aluno Quatro")).toBeTruthy();
    fireEvent.change(modal.getByLabelText("Buscar RGM ou nome"), { target: { value: "zzz" } });
    expect(modal.getByText("Nenhum RGM nesse filtro.")).toBeTruthy();
    fireEvent.click(modal.getByRole("button", { name: "Fechar" }));
    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it("CPF e telefone chegam depois; Ver Performance só com id do Kommo", async () => {
    abrir({ admin: false });
    await esperarPainel();
    fireEvent.click(screen.getByRole("button", { name: "Ana Souza" }));
    const modal = within(await screen.findByRole("dialog", { name: /Ana Souza/ }));
    expect(await modal.findByText("111.222.333-44")).toBeTruthy();
    expect(modal.getByText("11999990000")).toBeTruthy();
    expect(modal.getByText("Todos os RGMs dentro do padrão do ciclo")).toBeTruthy();
    expect(modal.getByRole("link", { name: "Ver Performance" }).getAttribute("href")).toBe("/#minha_performance");
    expect(modal.getByText("NOVA")).toBeTruthy();
  });
});

describe("ferramentas", () => {
  it("duplicatas manda os RGMs do recorte e lista os negócios", async () => {
    abrir({
      rotas: {
        "POST /api/bwipo/painel/duplicatas": () =>
          json({
            ok: true,
            total: 1,
            duplicatas: [
              { rgm: "50000001", count: 2, negocios: [{ id: "d1", number: 11, title: "Aluno Um", agente: "Ana", etapa: "Ganho", ganho: true, perdido: false }] },
            ],
          }),
      },
    });
    await esperarPainel();
    fireEvent.click(screen.getByRole("button", { name: "Duplicatas" }));
    const modal = within(await screen.findByRole("dialog", { name: "Duplicatas no Bwipo" }));
    expect(await modal.findByText("1 RGMs do recorte com mais de um negócio no Pipeline Principal")).toBeTruthy();
    expect(corpos("/api/bwipo/painel/duplicatas")).toEqual([{ rgms: ["50000001", "50000002", "50000003", "40000004"] }]);
    expect(modal.getByText("#11 Aluno Um")).toBeTruthy();
  });

  it("sem data lista as duas seções", async () => {
    abrir({
      rotas: {
        "POST /api/bwipo/painel/sem-data": () =>
          json({ ok: true, total: 1, sem_data: [{ rgm: "50000002", number: 7, agente: "Ana", etapa: "Ganho" }], sem_negocio: ["50000003"] }),
      },
    });
    await esperarPainel();
    fireEvent.click(screen.getByRole("button", { name: "Sem data" }));
    const modal = within(await screen.findByRole("dialog", { name: "Matrículas sem data no Bwipo" }));
    expect(await modal.findByText("Negócio no Bwipo sem data de matrícula (1)")).toBeTruthy();
    expect(modal.getByText("Matrícula sem negócio no Bwipo (1)")).toBeTruthy();
    expect(modal.getByText("#7 · Ana · Ganho")).toBeTruthy();
    expect(modal.getByText("Bruno Lima · decisão manual")).toBeTruthy();
  });

  it("diagnóstico mostra fontes do recorte e números do espelho", async () => {
    abrir({
      rotas: {
        "GET /api/bwipo/painel/diagnostico": () =>
          json({
            ok: true,
            numeros: { negocios: 1200, negocios_principal: 1000, decisoes_manuais: 3 },
            depara_por_tipo: { telefone: 5 },
            donos_sem_depara: [],
            syncs: [{ entidade: "deals", em: null, status: "ok", erro: null }],
          }),
      },
    });
    await esperarPainel();
    fireEvent.click(screen.getByRole("button", { name: "Diagnóstico" }));
    const modal = within(await screen.findByRole("dialog", { name: "Diagnóstico Kommo ↔ Bwipo" }));
    expect(await modal.findByText("Quem ficou com as vendas deste recorte (4)")).toBeTruthy();
    expect(modal.getByText("1.200")).toBeTruthy();
    expect(modal.getByText("· por telefone")).toBeTruthy();
    expect(modal.getByText("Nenhum.")).toBeTruthy();
  });

  it("sync agentes (admin) avisa o resultado e atualiza", async () => {
    abrir({ rotas: { "POST /api/bwipo/painel/sync-agentes": () => json({ ok: true, kommo_users: null, depara: 8, bwipo_sem_depara: 2 }) } });
    await esperarPainel();
    fireEvent.click(screen.getByRole("button", { name: "Sync agentes" }));
    expect(await screen.findByText("Kommo: — usuários · Bwipo ligados: 8 · sem depara: 2")).toBeTruthy();
    await esperarDepoisDeGravar();
  });
});

describe("sem novas tentativas (regra de produção do QueryClient)", () => {
  it("erro 500 no diagnóstico aparece no modal e a rota é chamada uma vez só", async () => {
    abrir({ producao: true, rotas: { "GET /api/bwipo/painel/diagnostico": () => json({ ok: false, error: "espelho fora do ar" }, 500) } });
    await esperarPainel();
    fireEvent.click(screen.getByRole("button", { name: "Diagnóstico" }));
    const modal = within(await screen.findByRole("dialog", { name: "Diagnóstico Kommo ↔ Bwipo" }));
    expect(await modal.findByText("espelho fora do ar")).toBeTruthy();
    expect(modal.getByText("Erro")).toBeTruthy();
    await pausa(50);
    expect(urls("/api/bwipo/painel/diagnostico")).toHaveLength(1);
  });

  it("erro 500 em duplicatas, sem data, contatos, ciclos e turmas: uma chamada cada", async () => {
    const erro = () => json({ ok: false, error: "falhou" }, 500);
    abrir({
      producao: true,
      rotas: {
        "POST /api/bwipo/painel/duplicatas": erro,
        "POST /api/bwipo/painel/sem-data": erro,
        "POST /api/bwipo/painel/contatos": erro,
        "GET /api/comercial-rgm/ciclos": erro,
        "GET /api/comercial-rgm/turmas": erro,
      },
    });
    await esperarPainel();
    fireEvent.click(screen.getByRole("button", { name: "Duplicatas" }));
    expect(await within(await screen.findByRole("dialog", { name: "Duplicatas no Bwipo" })).findByText("falhou")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Fechar" }));
    fireEvent.click(screen.getByRole("button", { name: "Sem data" }));
    expect(await within(await screen.findByRole("dialog", { name: "Matrículas sem data no Bwipo" })).findByText("falhou")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Fechar" }));
    fireEvent.click(screen.getByRole("button", { name: "Ana Souza" }));
    await screen.findByRole("dialog", { name: /Ana Souza/ });
    await waitFor(() => expect(urls("/api/bwipo/painel/contatos", "POST")).toHaveLength(1));
    await pausa(50);
    expect(urls("/api/bwipo/painel/duplicatas", "POST")).toHaveLength(1);
    expect(urls("/api/bwipo/painel/sem-data", "POST")).toHaveLength(1);
    expect(urls("/api/bwipo/painel/contatos", "POST")).toHaveLength(1);
    expect(urls("/api/comercial-rgm/ciclos")).toHaveLength(1);
    expect(urls("/api/comercial-rgm/turmas")).toHaveLength(1);
  });
});
